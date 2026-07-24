"""Tests for the bounded Kafka delivery-acknowledgement boundary."""

import asyncio
import json
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast

import pytest
from confluent_kafka import KafkaError, KafkaException, Message
from pydantic import SecretStr

from github_analytics.webhook.config import WebhookSettings
from github_analytics.webhook.kafka import (
    DeliveryCallback,
    KafkaEnvelopePublisher,
    create_kafka_publisher,
)
from github_analytics.webhook.models import GitHubEventEnvelope, GitHubEventName
from github_analytics.webhook.publishing import PublishError


class FakeProducer:
    def __init__(
        self,
        *,
        delivery_error: KafkaError | None = None,
        enqueue_error: Exception | None = None,
        invoke_callback: bool = True,
        ready: bool = True,
        metadata_error: bool = False,
    ) -> None:
        self.delivery_error = delivery_error
        self.enqueue_error = enqueue_error
        self.invoke_callback = invoke_callback
        self.ready = ready
        self.metadata_error = metadata_error
        self.callback: DeliveryCallback | None = None
        self.topic: str | None = None
        self.value: bytes | None = None
        self.key: bytes | None = None

    def produce(
        self,
        topic: str,
        value: bytes,
        key: bytes,
        on_delivery: DeliveryCallback,
    ) -> None:
        if self.enqueue_error is not None:
            raise self.enqueue_error
        self.topic = topic
        self.value = value
        self.key = key
        self.callback = on_delivery

    def poll(self, timeout: float) -> int:
        del timeout
        if self.invoke_callback and self.callback is not None:
            callback = self.callback
            self.callback = None
            callback(self.delivery_error, cast(Message, object()))
            return 1
        return 0

    def list_topics(self, *, timeout: float) -> Any:
        del timeout
        if self.metadata_error:
            raise KafkaException(KafkaError(KafkaError._TRANSPORT))
        return SimpleNamespace(brokers={1: object()} if self.ready else {})


def _envelope() -> GitHubEventEnvelope:
    payload: dict[str, Any] = {
        "action": "opened",
        "installation": {"id": 10001},
        "repository": {
            "id": 20001,
            "full_name": "example-org/delivery-demo",
        },
        "fixture_extension": {"safe_to_ignore": True},
    }
    return GitHubEventEnvelope.from_webhook(
        delivery_id="delivery-example-1",
        event_name=GitHubEventName.PULL_REQUEST,
        received_at=datetime(2026, 7, 23, tzinfo=UTC),
        payload=payload,
    )


def _publisher(
    producer: FakeProducer,
    *,
    timeout: float = 0.1,
) -> KafkaEnvelopePublisher:
    return KafkaEnvelopePublisher(
        producer,
        topic="github.events.raw.v1",
        publish_timeout_seconds=timeout,
        readiness_timeout_seconds=0.1,
    )


def test_publish_waits_for_successful_delivery_callback() -> None:
    producer = FakeProducer()

    asyncio.run(_publisher(producer).publish(_envelope()))

    assert producer.topic == "github.events.raw.v1"
    assert producer.key == b"20001"
    assert producer.value is not None
    assert json.loads(producer.value)["delivery_id"] == "delivery-example-1"


def test_delivery_callback_error_fails_publish() -> None:
    producer = FakeProducer(delivery_error=KafkaError(KafkaError._MSG_TIMED_OUT))

    with pytest.raises(PublishError, match="Kafka rejected"):
        asyncio.run(_publisher(producer).publish(_envelope()))


@pytest.mark.parametrize(
    "error",
    [
        BufferError("queue full"),
        KafkaException(KafkaError(KafkaError._TRANSPORT)),
        RuntimeError("producer closed"),
    ],
)
def test_enqueue_errors_fail_publish(error: Exception) -> None:
    producer = FakeProducer(enqueue_error=error)

    with pytest.raises(PublishError, match="could not be queued"):
        asyncio.run(_publisher(producer).publish(_envelope()))


def test_missing_delivery_callback_times_out() -> None:
    producer = FakeProducer(invoke_callback=False)

    with pytest.raises(PublishError, match="acknowledgement timed out"):
        asyncio.run(_publisher(producer, timeout=0.001).publish(_envelope()))


@pytest.mark.parametrize(
    ("producer", "expected"),
    [
        (FakeProducer(ready=True), True),
        (FakeProducer(ready=False), False),
        (FakeProducer(metadata_error=True), False),
    ],
)
def test_readiness_reflects_cluster_metadata(
    producer: FakeProducer,
    expected: bool,
) -> None:
    assert asyncio.run(_publisher(producer).is_ready()) is expected


def test_factory_applies_durable_bounded_producer_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}
    producer = FakeProducer()

    def producer_factory(config: dict[str, Any]) -> FakeProducer:
        captured.update(config)
        return producer

    monkeypatch.setattr("github_analytics.webhook.kafka.Producer", producer_factory)
    settings = WebhookSettings(
        webhook_secret=SecretStr("test-secret"),
        kafka_bootstrap_servers="kafka.example:9092",
        kafka_raw_topic="github.events.raw.v1",
        kafka_publish_timeout_seconds=2.5,
        kafka_readiness_timeout_seconds=0.5,
    )

    publisher = create_kafka_publisher(settings)
    asyncio.run(publisher.publish(_envelope()))

    assert captured == {
        "bootstrap.servers": "kafka.example:9092",
        "client.id": "github-webhook-receiver",
        "acks": "all",
        "enable.idempotence": True,
        "message.timeout.ms": 2250,
    }
