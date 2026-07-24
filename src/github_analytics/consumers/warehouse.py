"""Kafka-to-PostgreSQL warehouse writer with ordered durable boundaries."""

import json
import logging
from collections.abc import Callable
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Protocol, cast

from confluent_kafka import Consumer, KafkaException, Message, TopicPartition
from psycopg_pool import ConnectionPool
from pydantic import TypeAdapter, ValidationError

from github_analytics.consumers.config import WarehouseSettings
from github_analytics.consumers.dlq import (
    DeadLetterRecord,
    create_dlq_publisher,
)
from github_analytics.consumers.storage import (
    InsertOutcome,
    PostgresRawEventStorage,
    WebhookRawEvent,
)
from github_analytics.webhook.models import GitHubEventEnvelope, GitHubEventName

logger = logging.getLogger(__name__)
_AWARE_DATETIME = TypeAdapter(datetime)


class ConsumerClient(Protocol):
    """Subset of the Confluent consumer used by the worker."""

    def subscribe(self, topics: list[str]) -> None: ...

    def poll(self, timeout: float) -> Message | None: ...

    def commit(
        self,
        *,
        message: Message,
        asynchronous: bool,
    ) -> list[TopicPartition] | None: ...

    def close(self) -> None: ...


class RawEventStorage(Protocol):
    """Durable raw-event insert boundary."""

    def insert(self, event: WebhookRawEvent) -> InsertOutcome: ...


class DlqPublisher(Protocol):
    """Durable poison-record publish boundary."""

    def publish(self, record: DeadLetterRecord) -> None: ...


class ProcessingOutcome(StrEnum):
    """Observable result after the source offset is committed."""

    INSERTED = "inserted"
    DUPLICATE = "duplicate"
    DLQ = "dlq"


class WarehouseWriter:
    """Process records without advancing offsets ahead of durable effects."""

    def __init__(
        self,
        consumer: ConsumerClient,
        storage: RawEventStorage,
        dlq_publisher: DlqPublisher,
        *,
        raw_topic: str,
        poll_timeout_seconds: float,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._consumer = consumer
        self._storage = storage
        self._dlq_publisher = dlq_publisher
        self._raw_topic = raw_topic
        self._poll_timeout_seconds = poll_timeout_seconds
        self._clock = clock

    def run_forever(self) -> None:
        """Poll until interrupted; processing failures stop the worker."""

        self._consumer.subscribe([self._raw_topic])
        while True:
            message = self._consumer.poll(self._poll_timeout_seconds)
            if message is None:
                continue
            outcome = self.process(message)
            topic, partition, offset = _message_lineage(message)
            logger.info(
                json.dumps(
                    {
                        "event": "warehouse_record_processed",
                        "outcome": outcome.value,
                        "topic": topic,
                        "partition": partition,
                        "offset": offset,
                    },
                    separators=(",", ":"),
                    sort_keys=True,
                )
            )

    def process(
        self,
        message: Message,
        *,
        after_database_commit: Callable[[], None] | None = None,
    ) -> ProcessingOutcome:
        """Produce one durable effect, then synchronously commit its source offset."""

        message_error = message.error()
        if message_error is not None:
            raise KafkaException(message_error)
        _, partition, offset = _message_lineage(message)

        try:
            envelope = _validate_envelope(message.value())
        except (TypeError, ValidationError):
            dead_letter = DeadLetterRecord.from_message(
                message,
                failed_at=self._clock(),
            )
            self._dlq_publisher.publish(dead_letter)
            self._commit_source_offset(message)
            return ProcessingOutcome.DLQ

        insert_outcome = self._storage.insert(
            WebhookRawEvent(
                envelope=envelope,
                kafka_partition=partition,
                kafka_offset=offset,
                occurred_at=_extract_occurred_at(envelope),
            )
        )
        if after_database_commit is not None:
            after_database_commit()
        self._commit_source_offset(message)
        return ProcessingOutcome(insert_outcome.value)

    def _commit_source_offset(self, message: Message) -> None:
        committed = self._consumer.commit(message=message, asynchronous=False)
        for partition in committed or []:
            if partition.error is not None:
                raise KafkaException(partition.error)


def create_consumer(settings: WarehouseSettings) -> ConsumerClient:
    """Create a manual-commit consumer in the warehouse-writer group."""

    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "client.id": "warehouse-writer",
            "group.id": settings.kafka_group_id,
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "auto.offset.reset": "earliest",
        }
    )
    return cast(ConsumerClient, consumer)


def create_pool(settings: WarehouseSettings) -> ConnectionPool[Any]:
    """Create the small synchronous pool used by the single-threaded worker."""

    return ConnectionPool(
        settings.database_url.get_secret_value(),
        min_size=1,
        max_size=4,
        timeout=settings.database_pool_timeout_seconds,
        open=True,
    )


def main() -> int:
    """Run the warehouse writer until an orderly keyboard interrupt."""

    settings = WarehouseSettings()
    with create_pool(settings) as pool:
        pool.wait(timeout=settings.database_pool_timeout_seconds)
        consumer = create_consumer(settings)
        dlq_publisher = create_dlq_publisher(settings)
        writer = WarehouseWriter(
            consumer,
            PostgresRawEventStorage(pool),
            dlq_publisher,
            raw_topic=settings.kafka_raw_topic,
            poll_timeout_seconds=settings.kafka_poll_timeout_seconds,
        )
        try:
            writer.run_forever()
        except KeyboardInterrupt:
            return 0
        finally:
            consumer.close()
            dlq_publisher.close()
    return 0


def _validate_envelope(value: bytes | None) -> GitHubEventEnvelope:
    if value is None:
        raise TypeError("Kafka tombstones are not GitHub event envelopes")
    return GitHubEventEnvelope.model_validate_json(value)


def _message_lineage(message: Message) -> tuple[str, int, int]:
    topic = message.topic()
    partition = message.partition()
    offset = message.offset()
    if topic is None or partition is None or offset is None:
        raise ValueError("consumed message is missing source lineage")
    return topic, partition, offset


def _extract_occurred_at(envelope: GitHubEventEnvelope) -> datetime | None:
    paths: dict[GitHubEventName, tuple[str, str]] = {
        GitHubEventName.PULL_REQUEST: ("pull_request", "updated_at"),
        GitHubEventName.PULL_REQUEST_REVIEW: ("review", "submitted_at"),
        GitHubEventName.WORKFLOW_RUN: ("workflow_run", "updated_at"),
        GitHubEventName.DEPLOYMENT: ("deployment", "created_at"),
        GitHubEventName.DEPLOYMENT_STATUS: ("deployment_status", "created_at"),
    }
    object_name, timestamp_name = paths[envelope.event_name]
    source_object = envelope.payload.get(object_name)
    if not isinstance(source_object, dict):
        return None
    value = source_object.get(timestamp_name)
    try:
        occurred_at = _AWARE_DATETIME.validate_python(value)
    except ValidationError:
        return None
    if occurred_at.tzinfo is None:
        return None
    return occurred_at
