"""Kafka publisher that waits for a broker delivery report before returning."""

import asyncio
from collections.abc import Callable
from threading import Event
from time import monotonic
from typing import Any, Protocol, cast

from confluent_kafka import KafkaError, KafkaException, Message, Producer

from github_analytics.webhook.config import WebhookSettings
from github_analytics.webhook.models import GitHubEventEnvelope
from github_analytics.webhook.publishing import PublishError

DeliveryCallback = Callable[[KafkaError | None, Message], None]


class ProducerClient(Protocol):
    """Subset of the Confluent producer used by the HTTP boundary."""

    def produce(
        self,
        topic: str,
        value: bytes,
        key: bytes,
        on_delivery: DeliveryCallback,
    ) -> None:
        """Enqueue one record for asynchronous delivery."""

    def poll(self, timeout: float) -> int:
        """Serve producer events and delivery callbacks."""

    def list_topics(self, *, timeout: float) -> Any:
        """Fetch cluster metadata or raise when the broker is unavailable."""


class KafkaEnvelopePublisher:
    """Publish envelopes without claiming success before broker acknowledgement."""

    _MAX_POLL_SECONDS = 0.1

    def __init__(
        self,
        producer: ProducerClient,
        *,
        topic: str,
        publish_timeout_seconds: float,
        readiness_timeout_seconds: float,
    ) -> None:
        self._producer = producer
        self._topic = topic
        self._publish_timeout_seconds = publish_timeout_seconds
        self._readiness_timeout_seconds = readiness_timeout_seconds

    async def publish(self, envelope: GitHubEventEnvelope) -> None:
        await asyncio.to_thread(self._publish_sync, envelope)

    def _publish_sync(self, envelope: GitHubEventEnvelope) -> None:
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
                value=envelope.model_dump_json().encode("utf-8"),
                key=str(envelope.repository_id).encode("ascii"),
                on_delivery=on_delivery,
            )
        except (BufferError, KafkaException, RuntimeError) as error:
            raise PublishError("event could not be queued for Kafka") from error

        deadline = monotonic() + self._publish_timeout_seconds
        while not completed.is_set():
            remaining = deadline - monotonic()
            if remaining <= 0:
                raise PublishError("Kafka delivery acknowledgement timed out")
            self._producer.poll(min(remaining, self._MAX_POLL_SECONDS))

        if errors:
            raise PublishError("Kafka rejected the event") from None

    async def is_ready(self) -> bool:
        return await asyncio.to_thread(self._is_ready_sync)

    def _is_ready_sync(self) -> bool:
        try:
            metadata = self._producer.list_topics(timeout=self._readiness_timeout_seconds)
        except KafkaException:
            return False
        return bool(metadata.brokers)


def create_kafka_publisher(settings: WebhookSettings) -> KafkaEnvelopePublisher:
    """Create the production publisher with explicit durability and timeout settings."""

    message_timeout_ms = max(
        1,
        round(settings.kafka_publish_timeout_seconds * 900),
    )
    producer = Producer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "client.id": "github-webhook-receiver",
            "acks": "all",
            "enable.idempotence": True,
            "message.timeout.ms": message_timeout_ms,
        }
    )
    return KafkaEnvelopePublisher(
        cast(ProducerClient, producer),
        topic=settings.kafka_raw_topic,
        publish_timeout_seconds=settings.kafka_publish_timeout_seconds,
        readiness_timeout_seconds=settings.kafka_readiness_timeout_seconds,
    )
