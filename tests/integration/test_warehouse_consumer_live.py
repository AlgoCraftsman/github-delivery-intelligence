"""Opt-in duplicate, crash-window, and poison-record integration evidence."""

import asyncio
import base64
import json
import os
from collections.abc import Callable
from datetime import UTC, datetime
from time import monotonic
from typing import Any, cast
from uuid import uuid4

import pytest
from confluent_kafka import Consumer, KafkaException, Message, Producer, TopicPartition
from psycopg_pool import ConnectionPool
from pydantic import SecretStr

from github_analytics.consumers.config import WarehouseSettings
from github_analytics.consumers.dlq import create_dlq_publisher
from github_analytics.consumers.storage import PostgresRawEventStorage
from github_analytics.consumers.warehouse import (
    ProcessingOutcome,
    WarehouseWriter,
    create_consumer,
)
from github_analytics.webhook.config import WebhookSettings
from github_analytics.webhook.kafka import create_kafka_publisher
from github_analytics.webhook.models import GitHubEventEnvelope, GitHubEventName

pytestmark = pytest.mark.skipif(
    os.getenv("RUN_WAREHOUSE_INTEGRATION") != "1",
    reason="requires RUN_WAREHOUSE_INTEGRATION=1 and migrated local services",
)

DATABASE_URL = "postgresql://github_analytics:local_only_change_me@localhost:55432/github_analytics"
RAW_TOPIC = "github.events.raw.v1"
DLQ_TOPIC = "github.events.dlq.v1"


def test_crash_after_database_commit_replays_to_one_durable_effect() -> None:
    delivery_id = f"crash-window-{uuid4()}"
    group_id = f"warehouse-crash-test-{uuid4()}"
    envelope = _envelope(delivery_id)
    source_value = envelope.model_dump_json().encode()
    _publish_envelope(envelope)
    settings = _warehouse_settings(group_id)

    with ConnectionPool[Any](
        DATABASE_URL,
        min_size=1,
        max_size=1,
        open=True,
    ) as pool:
        storage = PostgresRawEventStorage(pool)
        first_consumer = create_consumer(settings)
        first_dlq = create_dlq_publisher(settings)
        first_writer = WarehouseWriter(
            first_consumer,
            storage,
            first_dlq,
            raw_topic=RAW_TOPIC,
            poll_timeout_seconds=0.25,
        )
        first_consumer.subscribe([RAW_TOPIC])
        first_message = _poll_for(
            first_consumer,
            lambda message: message.value() == source_value,
        )

        def simulate_crash() -> None:
            raise RuntimeError("simulated crash after database commit")

        with pytest.raises(RuntimeError, match="simulated crash"):
            first_writer.process(
                first_message,
                after_database_commit=simulate_crash,
            )
        first_consumer.close()
        first_dlq.close()

        replay_consumer = create_consumer(settings)
        replay_dlq = create_dlq_publisher(settings)
        replay_writer = WarehouseWriter(
            replay_consumer,
            storage,
            replay_dlq,
            raw_topic=RAW_TOPIC,
            poll_timeout_seconds=0.25,
        )
        replay_consumer.subscribe([RAW_TOPIC])
        replay_message = _poll_for(
            replay_consumer,
            lambda message: message.value() == source_value,
        )

        assert replay_message.partition() == first_message.partition()
        assert replay_message.offset() == first_message.offset()
        assert replay_writer.process(replay_message) is ProcessingOutcome.DUPLICATE
        replay_consumer.close()
        replay_dlq.close()

        with pool.connection() as connection:
            count = connection.execute(
                """
                SELECT count(*)
                FROM raw.github_events
                WHERE source = 'webhook' AND source_record_key = %s
                """,
                (delivery_id,),
            ).fetchone()

    assert count == (1,)


def test_poison_record_reaches_dlq_before_source_offset_advances() -> None:
    delivery_id = f"poison-{uuid4()}"
    source_value = f'{{"delivery_id":"{delivery_id}","schema_version":999}}'.encode()
    _publish_bytes(RAW_TOPIC, source_value, key=b"20001")
    settings = _warehouse_settings(f"warehouse-poison-test-{uuid4()}")
    source_consumer = create_consumer(settings)
    dlq_publisher = create_dlq_publisher(settings)
    with ConnectionPool[Any](
        DATABASE_URL,
        min_size=1,
        max_size=1,
        open=True,
    ) as pool:
        writer = WarehouseWriter(
            source_consumer,
            PostgresRawEventStorage(pool),
            dlq_publisher,
            raw_topic=RAW_TOPIC,
            poll_timeout_seconds=0.25,
        )
        source_consumer.subscribe([RAW_TOPIC])
        source_message = _poll_for(
            source_consumer,
            lambda message: message.value() == source_value,
        )

        assert writer.process(source_message) is ProcessingOutcome.DLQ
        partition = source_message.partition()
        offset = source_message.offset()
        assert partition is not None
        assert offset is not None
        committed = cast(Consumer, source_consumer).committed(
            [TopicPartition(RAW_TOPIC, partition)],
            timeout=5,
        )
        assert committed[0].offset == offset + 1
        source_consumer.close()
        dlq_publisher.close()

    inspector = Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": f"dlq-inspector-{uuid4()}",
            "auto.offset.reset": "earliest",
        }
    )
    inspector.subscribe([DLQ_TOPIC])
    dlq_message = _poll_for(
        inspector,
        lambda message: message.key() == delivery_id.encode(),
        commit_skipped=False,
    )
    inspector.close()

    assert dlq_message.key() == delivery_id.encode()
    dlq_value = dlq_message.value()
    assert dlq_value is not None
    dlq_record = json.loads(dlq_value)
    assert base64.b64decode(dlq_record["source_value_base64"]) == source_value


def _envelope(delivery_id: str) -> GitHubEventEnvelope:
    return GitHubEventEnvelope.from_webhook(
        delivery_id=delivery_id,
        event_name=GitHubEventName.PULL_REQUEST,
        received_at=datetime.now(UTC),
        payload={
            "action": "opened",
            "installation": {"id": 10001},
            "repository": {
                "id": 20001,
                "full_name": "example-org/delivery-demo",
            },
            "pull_request": {
                "id": 30001,
                "updated_at": "2026-07-24T14:00:00Z",
            },
        },
    )


def _warehouse_settings(group_id: str) -> WarehouseSettings:
    return WarehouseSettings(
        database_url=SecretStr(DATABASE_URL),
        kafka_bootstrap_servers="localhost:9092",
        kafka_raw_topic=RAW_TOPIC,
        kafka_dlq_topic=DLQ_TOPIC,
        kafka_group_id=group_id,
        kafka_poll_timeout_seconds=0.25,
        kafka_dlq_publish_timeout_seconds=5,
    )


def _publish_envelope(envelope: GitHubEventEnvelope) -> None:
    settings = WebhookSettings(
        webhook_secret=SecretStr("integration-only-secret"),
        kafka_bootstrap_servers="localhost:9092",
        kafka_raw_topic=RAW_TOPIC,
        kafka_publish_timeout_seconds=5,
    )
    asyncio.run(create_kafka_publisher(settings).publish(envelope))


def _publish_bytes(topic: str, value: bytes, *, key: bytes) -> None:
    producer = Producer(
        {
            "bootstrap.servers": "localhost:9092",
            "acks": "all",
            "enable.idempotence": True,
            "message.timeout.ms": 4500,
        }
    )
    producer.produce(topic, value=value, key=key)
    assert producer.flush(5) == 0


def _poll_for(
    consumer: Any,
    predicate: Callable[[Message], bool],
    *,
    commit_skipped: bool = True,
) -> Message:
    deadline = monotonic() + 15
    while monotonic() < deadline:
        message = consumer.poll(0.25)
        if message is None:
            continue
        error = message.error()
        if error is not None:
            raise KafkaException(error)
        if predicate(message):
            return cast(Message, message)
        if commit_skipped:
            consumer.commit(message=message, asynchronous=False)
    raise AssertionError("timed out waiting for the integration-test record")
