"""Tests for acknowledged poison-record publishing."""

import base64
import json
from datetime import UTC, datetime
from typing import Any, cast

import pytest
from confluent_kafka import KafkaError, KafkaException, Message

from github_analytics.consumers.config import WarehouseSettings
from github_analytics.consumers.dlq import (
    DeadLetterRecord,
    DlqDeliveryCallback,
    DlqPublishError,
    KafkaDlqPublisher,
    SourceMessage,
    create_dlq_publisher,
)


class FakeMessage:
    def __init__(
        self,
        *,
        value: bytes | None,
        key: bytes | None = b"repository-key",
        topic: str | None = "github.events.raw.v1",
        partition: int | None = 2,
        offset: int | None = 41,
    ) -> None:
        self._value = value
        self._key = key
        self._topic = topic
        self._partition = partition
        self._offset = offset

    def topic(self) -> str | None:
        return self._topic

    def partition(self) -> int | None:
        return self._partition

    def offset(self) -> int | None:
        return self._offset

    def key(self) -> bytes | None:
        return self._key

    def value(self) -> bytes | None:
        return self._value


class FakeProducer:
    def __init__(
        self,
        *,
        delivery_error: KafkaError | None = None,
        enqueue_error: Exception | None = None,
        poll_error: Exception | None = None,
        invoke_callback: bool = True,
    ) -> None:
        self.delivery_error = delivery_error
        self.enqueue_error = enqueue_error
        self.poll_error = poll_error
        self.invoke_callback = invoke_callback
        self.callback: DlqDeliveryCallback | None = None
        self.topic: str | None = None
        self.value: bytes | None = None
        self.key: bytes | None = None
        self.flush_timeout: float | None = None

    def produce(
        self,
        topic: str,
        value: bytes,
        key: bytes | None,
        on_delivery: DlqDeliveryCallback,
    ) -> None:
        if self.enqueue_error is not None:
            raise self.enqueue_error
        self.topic = topic
        self.value = value
        self.key = key
        self.callback = on_delivery

    def poll(self, timeout: float) -> int:
        del timeout
        if self.poll_error is not None:
            raise self.poll_error
        if self.invoke_callback and self.callback is not None:
            callback = self.callback
            self.callback = None
            callback(self.delivery_error, cast(Message, object()))
            return 1
        return 0

    def flush(self, timeout: float) -> int:
        self.flush_timeout = timeout
        return 0


def _record(value: bytes | None = None) -> DeadLetterRecord:
    if value is None:
        value = b'{"delivery_id":"real-delivery","not":"an envelope"}'
    return DeadLetterRecord.from_message(
        cast(SourceMessage, FakeMessage(value=value)),
        failed_at=datetime(2026, 7, 24, 14, 0, tzinfo=UTC),
    )


def test_dead_letter_retains_source_bytes_and_real_delivery_identity() -> None:
    value = b'{"delivery_id":"  real-delivery  ","unknown":true}'
    message = FakeMessage(value=value)

    record = DeadLetterRecord.from_message(
        cast(SourceMessage, message),
        failed_at=datetime(2026, 7, 24, 14, 0, tzinfo=UTC),
    )

    assert record.delivery_id == "real-delivery"
    assert record.kafka_key() == b"real-delivery"
    assert record.source_key_base64 == base64.b64encode(b"repository-key").decode()
    assert record.source_value_base64 == base64.b64encode(value).decode()
    assert record.source_topic == "github.events.raw.v1"
    assert record.source_partition == 2
    assert record.source_offset == 41


@pytest.mark.parametrize(
    "value",
    [
        None,
        b"\xff",
        b"[]",
        b'{"delivery_id":123}',
        b'{"delivery_id":"  "}',
    ],
)
def test_dead_letter_does_not_fabricate_unavailable_delivery_identity(
    value: bytes | None,
) -> None:
    message = FakeMessage(value=value, key=None)

    record = DeadLetterRecord.from_message(
        cast(SourceMessage, message),
        failed_at=datetime(2026, 7, 24, 14, 0, tzinfo=UTC),
    )

    assert record.delivery_id is None
    assert record.kafka_key() is None
    assert record.source_key_base64 is None
    assert record.source_value_base64 == (
        None if value is None else base64.b64encode(value).decode()
    )


@pytest.mark.parametrize(
    ("topic", "partition", "offset"),
    [
        (None, 0, 0),
        ("topic", None, 0),
        ("topic", 0, None),
    ],
)
def test_dead_letter_requires_real_source_lineage(
    topic: str | None,
    partition: int | None,
    offset: int | None,
) -> None:
    message = FakeMessage(
        value=b"{}",
        topic=topic,
        partition=partition,
        offset=offset,
    )

    with pytest.raises(ValueError, match="source lineage"):
        DeadLetterRecord.from_message(
            cast(SourceMessage, message),
            failed_at=datetime.now(UTC),
        )


def test_dlq_publish_waits_for_successful_acknowledgement_and_closes() -> None:
    producer = FakeProducer()
    publisher = KafkaDlqPublisher(
        producer,
        topic="github.events.dlq.v1",
        publish_timeout_seconds=0.1,
    )

    publisher.publish(_record())
    publisher.close()

    assert producer.topic == "github.events.dlq.v1"
    assert producer.key == b"real-delivery"
    assert producer.value is not None
    assert json.loads(producer.value)["source_offset"] == 41
    assert producer.flush_timeout == 0.1


def test_dlq_delivery_callback_error_fails_publish() -> None:
    producer = FakeProducer(delivery_error=KafkaError(KafkaError._MSG_TIMED_OUT))
    publisher = KafkaDlqPublisher(
        producer,
        topic="github.events.dlq.v1",
        publish_timeout_seconds=0.1,
    )

    with pytest.raises(DlqPublishError, match="rejected"):
        publisher.publish(_record())


@pytest.mark.parametrize(
    "error",
    [
        BufferError("queue full"),
        KafkaException(KafkaError(KafkaError._TRANSPORT)),
        RuntimeError("producer closed"),
    ],
)
def test_dlq_enqueue_errors_fail_publish(error: Exception) -> None:
    publisher = KafkaDlqPublisher(
        FakeProducer(enqueue_error=error),
        topic="github.events.dlq.v1",
        publish_timeout_seconds=0.1,
    )

    with pytest.raises(DlqPublishError, match="could not be queued"):
        publisher.publish(_record())


@pytest.mark.parametrize(
    "error",
    [
        KafkaException(KafkaError(KafkaError._TRANSPORT)),
        RuntimeError("producer closed"),
    ],
)
def test_dlq_poll_errors_fail_publish(error: Exception) -> None:
    publisher = KafkaDlqPublisher(
        FakeProducer(poll_error=error),
        topic="github.events.dlq.v1",
        publish_timeout_seconds=0.1,
    )

    with pytest.raises(DlqPublishError, match="acknowledgement failed"):
        publisher.publish(_record())


def test_dlq_missing_callback_times_out() -> None:
    publisher = KafkaDlqPublisher(
        FakeProducer(invoke_callback=False),
        topic="github.events.dlq.v1",
        publish_timeout_seconds=0.001,
    )

    with pytest.raises(DlqPublishError, match="timed out"):
        publisher.publish(_record())


def test_dlq_factory_applies_durable_bounded_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}
    producer = FakeProducer()

    def producer_factory(config: dict[str, Any]) -> FakeProducer:
        captured.update(config)
        return producer

    monkeypatch.setattr("github_analytics.consumers.dlq.Producer", producer_factory)
    settings = WarehouseSettings(
        kafka_bootstrap_servers="kafka.example:9092",
        kafka_dlq_topic="custom.dlq.v1",
        kafka_dlq_publish_timeout_seconds=2.5,
    )

    publisher = create_dlq_publisher(settings)
    publisher.publish(_record())

    assert producer.topic == "custom.dlq.v1"
    assert captured == {
        "bootstrap.servers": "kafka.example:9092",
        "client.id": "warehouse-writer-dlq",
        "acks": "all",
        "enable.idempotence": True,
        "message.timeout.ms": 2250,
    }
