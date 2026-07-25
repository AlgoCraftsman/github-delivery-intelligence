"""Acknowledged Kafka dead-letter publishing for poison source records."""

import base64
import json
from collections.abc import Callable
from datetime import datetime
from threading import Event
from time import monotonic
from typing import Any, Literal, Protocol, cast

from confluent_kafka import KafkaError, KafkaException, Message, Producer
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

DlqDeliveryCallback = Callable[[KafkaError | None, Message], None]
DlqFailureReason = Literal[
    "invalid_github_event_envelope",
    "invalid_pr_monitor_event",
]


class DlqSettings(Protocol):
    """Configuration shared by consumers that publish poison records."""

    kafka_bootstrap_servers: str
    kafka_dlq_topic: str
    kafka_dlq_publish_timeout_seconds: float


class SourceMessage(Protocol):
    """Consumed-message fields retained in a dead-letter record."""

    def topic(self) -> str | None: ...

    def partition(self) -> int | None: ...

    def offset(self) -> int | None: ...

    def key(self) -> bytes | None: ...

    def value(self) -> bytes | None: ...


class DeadLetterRecord(BaseModel):
    """Versioned poison-record contract with complete source bytes and lineage."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1] = 1
    failed_at: AwareDatetime
    failure_reason: DlqFailureReason
    source_topic: str = Field(min_length=1)
    source_partition: int = Field(ge=0)
    source_offset: int = Field(ge=0)
    delivery_id: str | None = Field(default=None, min_length=1)
    source_key_base64: str | None
    source_value_base64: str | None

    @classmethod
    def from_message(
        cls,
        message: SourceMessage,
        *,
        failed_at: datetime,
        failure_reason: DlqFailureReason = "invalid_github_event_envelope",
    ) -> "DeadLetterRecord":
        """Retain source bytes and use a real delivery ID only when recoverable."""

        key = message.key()
        value = message.value()
        topic = message.topic()
        partition = message.partition()
        offset = message.offset()
        if topic is None or partition is None or offset is None:
            raise ValueError("consumed message is missing source lineage")
        return cls(
            failed_at=failed_at,
            failure_reason=failure_reason,
            source_topic=topic,
            source_partition=partition,
            source_offset=offset,
            delivery_id=_extract_delivery_id(value),
            source_key_base64=_encode_optional_bytes(key),
            source_value_base64=_encode_optional_bytes(value),
        )

    def kafka_key(self) -> bytes | None:
        """Key by the real delivery ID when the poison bytes expose one."""

        if self.delivery_id is None:
            return None
        return self.delivery_id.encode("utf-8")


class DlqPublishError(RuntimeError):
    """Raised when a poison record is not acknowledged by Kafka."""


class DlqProducerClient(Protocol):
    """Subset of the Confluent producer required by the DLQ boundary."""

    def produce(
        self,
        topic: str,
        value: bytes,
        key: bytes | None,
        on_delivery: DlqDeliveryCallback,
    ) -> None: ...

    def poll(self, timeout: float) -> int: ...

    def flush(self, timeout: float) -> int: ...


class KafkaDlqPublisher:
    """Publish a dead letter and wait for its delivery callback."""

    _MAX_POLL_SECONDS = 0.1

    def __init__(
        self,
        producer: DlqProducerClient,
        *,
        topic: str,
        publish_timeout_seconds: float,
    ) -> None:
        self._producer = producer
        self._topic = topic
        self._publish_timeout_seconds = publish_timeout_seconds

    def publish(self, record: DeadLetterRecord) -> None:
        completed = Event()
        errors: list[KafkaError] = []

        def on_delivery(error: KafkaError | None, message: Message) -> None:
            del message
            if error is not None:
                errors.append(error)
            completed.set()

        try:
            self._producer.produce(
                self._topic,
                value=record.model_dump_json().encode("utf-8"),
                key=record.kafka_key(),
                on_delivery=on_delivery,
            )
        except (BufferError, KafkaException, RuntimeError) as error:
            raise DlqPublishError("poison record could not be queued for the DLQ") from error

        deadline = monotonic() + self._publish_timeout_seconds
        while not completed.is_set():
            remaining = deadline - monotonic()
            if remaining <= 0:
                raise DlqPublishError("DLQ delivery acknowledgement timed out")
            try:
                self._producer.poll(min(remaining, self._MAX_POLL_SECONDS))
            except (KafkaException, RuntimeError) as error:
                raise DlqPublishError("DLQ delivery acknowledgement failed") from error

        if errors:
            raise DlqPublishError("Kafka rejected the poison record") from None

    def close(self) -> None:
        """Serve any final callbacks during orderly shutdown."""

        self._producer.flush(self._publish_timeout_seconds)


def create_dlq_publisher(
    settings: DlqSettings,
    *,
    client_id: str = "warehouse-writer-dlq",
) -> KafkaDlqPublisher:
    """Create the DLQ producer with the same durable local boundary as webhooks."""

    message_timeout_ms = max(
        1,
        round(settings.kafka_dlq_publish_timeout_seconds * 900),
    )
    producer = Producer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "client.id": client_id,
            "acks": "all",
            "enable.idempotence": True,
            "message.timeout.ms": message_timeout_ms,
        }
    )
    return KafkaDlqPublisher(
        cast(DlqProducerClient, producer),
        topic=settings.kafka_dlq_topic,
        publish_timeout_seconds=settings.kafka_dlq_publish_timeout_seconds,
    )


def _extract_delivery_id(value: bytes | None) -> str | None:
    if value is None:
        return None
    try:
        decoded: Any = json.loads(value)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None
    if not isinstance(decoded, dict):
        return None
    delivery_id = decoded.get("delivery_id")
    if not isinstance(delivery_id, str) or not delivery_id.strip():
        return None
    return delivery_id.strip()


def _encode_optional_bytes(value: bytes | None) -> str | None:
    if value is None:
        return None
    return base64.b64encode(value).decode("ascii")
