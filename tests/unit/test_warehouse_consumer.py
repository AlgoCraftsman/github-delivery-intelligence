"""Tests for database-before-offset ordering in the warehouse writer."""

import json
from contextlib import AbstractContextManager
from datetime import UTC, datetime
from types import SimpleNamespace, TracebackType
from typing import Any, Self, cast

import pytest
from confluent_kafka import KafkaError, KafkaException, Message, TopicPartition
from psycopg_pool import ConnectionPool
from pydantic import SecretStr

from github_analytics.consumers.config import WarehouseSettings
from github_analytics.consumers.dlq import DeadLetterRecord
from github_analytics.consumers.storage import InsertOutcome, WebhookRawEvent
from github_analytics.consumers.warehouse import (
    ConsumerClient,
    ProcessingOutcome,
    WarehouseWriter,
    _extract_occurred_at,
    create_consumer,
    create_pool,
    main,
)
from github_analytics.webhook.models import GitHubEventEnvelope, GitHubEventName


class FakeMessage:
    def __init__(
        self,
        *,
        value: bytes | None,
        topic: str | None = "github.events.raw.v1",
        partition: int | None = 1,
        offset: int | None = 7,
        error: KafkaError | None = None,
    ) -> None:
        self._value = value
        self._topic = topic
        self._partition = partition
        self._offset = offset
        self._error = error

    def topic(self) -> str | None:
        return self._topic

    def partition(self) -> int | None:
        return self._partition

    def offset(self) -> int | None:
        return self._offset

    def key(self) -> bytes | None:
        return b"20001"

    def value(self) -> bytes | None:
        return self._value

    def error(self) -> KafkaError | None:
        return self._error


class FakeConsumer:
    def __init__(self) -> None:
        self.actions: list[str] = []
        self.messages: list[Message | None | BaseException] = []
        self.commit_result: list[TopicPartition] | None = [
            TopicPartition("github.events.raw.v1", 1, 8)
        ]
        self.commit_error: Exception | None = None
        self.closed = False
        self.subscribed: list[str] | None = None

    def subscribe(self, topics: list[str]) -> None:
        self.subscribed = topics

    def poll(self, timeout: float) -> Message | None:
        self.actions.append(f"poll:{timeout}")
        next_item = self.messages.pop(0)
        if isinstance(next_item, BaseException):
            raise next_item
        return next_item

    def commit(
        self,
        *,
        message: Message,
        asynchronous: bool,
    ) -> list[TopicPartition] | None:
        del message
        assert asynchronous is False
        self.actions.append("offset_commit")
        if self.commit_error is not None:
            raise self.commit_error
        return self.commit_result

    def close(self) -> None:
        self.closed = True


class FakeStorage:
    def __init__(
        self,
        actions: list[str],
        *,
        outcome: InsertOutcome = InsertOutcome.INSERTED,
        error: Exception | None = None,
    ) -> None:
        self.actions = actions
        self.outcome = outcome
        self.error = error
        self.event: WebhookRawEvent | None = None

    def insert(self, event: WebhookRawEvent) -> InsertOutcome:
        self.actions.append("database_commit")
        if self.error is not None:
            raise self.error
        self.event = event
        return self.outcome


class FakeDlq:
    def __init__(
        self,
        actions: list[str],
        *,
        error: Exception | None = None,
    ) -> None:
        self.actions = actions
        self.error = error
        self.record: DeadLetterRecord | None = None
        self.closed = False

    def publish(self, record: DeadLetterRecord) -> None:
        self.actions.append("dlq_ack")
        if self.error is not None:
            raise self.error
        self.record = record

    def close(self) -> None:
        self.closed = True


def _envelope_bytes(
    *,
    event_name: GitHubEventName = GitHubEventName.PULL_REQUEST,
    event_object: dict[str, Any] | None = None,
) -> bytes:
    object_names = {
        GitHubEventName.PULL_REQUEST: "pull_request",
        GitHubEventName.PULL_REQUEST_REVIEW: "review",
        GitHubEventName.WORKFLOW_RUN: "workflow_run",
        GitHubEventName.DEPLOYMENT: "deployment",
        GitHubEventName.DEPLOYMENT_STATUS: "deployment_status",
    }
    payload: dict[str, Any] = {
        "action": "opened",
        "installation": {"id": 10001},
        "repository": {
            "id": 20001,
            "full_name": "example-org/delivery-demo",
        },
        object_names[event_name]: event_object or {},
    }
    envelope = GitHubEventEnvelope.from_webhook(
        delivery_id="delivery-example",
        event_name=event_name,
        received_at=datetime(2026, 7, 24, 14, 0, tzinfo=UTC),
        payload=payload,
    )
    return envelope.model_dump_json().encode()


def _writer(
    consumer: FakeConsumer,
    storage: FakeStorage,
    dlq: FakeDlq,
) -> WarehouseWriter:
    return WarehouseWriter(
        cast(ConsumerClient, consumer),
        storage,
        dlq,
        raw_topic="github.events.raw.v1",
        poll_timeout_seconds=0.25,
        clock=lambda: datetime(2026, 7, 24, 15, 0, tzinfo=UTC),
    )


@pytest.mark.parametrize(
    ("insert_outcome", "processing_outcome"),
    [
        (InsertOutcome.INSERTED, ProcessingOutcome.INSERTED),
        (InsertOutcome.DUPLICATE, ProcessingOutcome.DUPLICATE),
    ],
)
def test_database_commit_precedes_synchronous_source_offset_commit(
    insert_outcome: InsertOutcome,
    processing_outcome: ProcessingOutcome,
) -> None:
    actions: list[str] = []
    consumer = FakeConsumer()
    consumer.actions = actions
    storage = FakeStorage(actions, outcome=insert_outcome)
    dlq = FakeDlq(actions)
    message = cast(Message, FakeMessage(value=_envelope_bytes()))

    outcome = _writer(consumer, storage, dlq).process(message)

    assert outcome is processing_outcome
    assert actions == ["database_commit", "offset_commit"]
    assert storage.event is not None
    assert storage.event.envelope.delivery_id == "delivery-example"
    assert storage.event.kafka_partition == 1
    assert storage.event.kafka_offset == 7


def test_crash_after_database_commit_leaves_source_offset_uncommitted() -> None:
    actions: list[str] = []
    consumer = FakeConsumer()
    consumer.actions = actions
    storage = FakeStorage(actions)
    writer = _writer(consumer, storage, FakeDlq(actions))

    def simulate_crash() -> None:
        actions.append("crash")
        raise RuntimeError("simulated crash window")

    with pytest.raises(RuntimeError, match="crash window"):
        writer.process(
            cast(Message, FakeMessage(value=_envelope_bytes())),
            after_database_commit=simulate_crash,
        )

    assert actions == ["database_commit", "crash"]


def test_storage_failure_does_not_commit_source_offset() -> None:
    actions: list[str] = []
    consumer = FakeConsumer()
    consumer.actions = actions
    writer = _writer(
        consumer,
        FakeStorage(actions, error=RuntimeError("postgres unavailable")),
        FakeDlq(actions),
    )

    with pytest.raises(RuntimeError, match="postgres unavailable"):
        writer.process(cast(Message, FakeMessage(value=_envelope_bytes())))

    assert actions == ["database_commit"]


@pytest.mark.parametrize("value", [b"not-json", None])
def test_poison_record_dlq_ack_precedes_source_offset_commit(
    value: bytes | None,
) -> None:
    actions: list[str] = []
    consumer = FakeConsumer()
    consumer.actions = actions
    dlq = FakeDlq(actions)

    outcome = _writer(consumer, FakeStorage(actions), dlq).process(
        cast(Message, FakeMessage(value=value))
    )

    assert outcome is ProcessingOutcome.DLQ
    assert actions == ["dlq_ack", "offset_commit"]
    assert dlq.record is not None
    assert dlq.record.source_offset == 7


def test_dlq_failure_does_not_commit_source_offset() -> None:
    actions: list[str] = []
    consumer = FakeConsumer()
    consumer.actions = actions
    writer = _writer(
        consumer,
        FakeStorage(actions),
        FakeDlq(actions, error=RuntimeError("dlq unavailable")),
    )

    with pytest.raises(RuntimeError, match="dlq unavailable"):
        writer.process(cast(Message, FakeMessage(value=b"not-json")))

    assert actions == ["dlq_ack"]


def test_source_commit_failure_stops_processing() -> None:
    actions: list[str] = []
    consumer = FakeConsumer()
    consumer.actions = actions
    consumer.commit_error = KafkaException(KafkaError(KafkaError._TRANSPORT))
    writer = _writer(consumer, FakeStorage(actions), FakeDlq(actions))

    with pytest.raises(KafkaException):
        writer.process(cast(Message, FakeMessage(value=_envelope_bytes())))

    assert actions == ["database_commit", "offset_commit"]


def test_partition_commit_error_stops_processing() -> None:
    actions: list[str] = []
    consumer = FakeConsumer()
    consumer.actions = actions
    consumer.commit_result = cast(
        list[TopicPartition],
        [SimpleNamespace(error=KafkaError(KafkaError._TRANSPORT))],
    )

    with pytest.raises(KafkaException):
        _writer(consumer, FakeStorage(actions), FakeDlq(actions)).process(
            cast(Message, FakeMessage(value=_envelope_bytes()))
        )


def test_message_error_is_not_treated_as_a_poison_record() -> None:
    actions: list[str] = []
    error = KafkaError(KafkaError._TRANSPORT)
    writer = _writer(FakeConsumer(), FakeStorage(actions), FakeDlq(actions))

    with pytest.raises(KafkaException):
        writer.process(cast(Message, FakeMessage(value=b"{}", error=error)))

    assert actions == []


@pytest.mark.parametrize(
    ("topic", "partition", "offset"),
    [
        (None, 0, 0),
        ("topic", None, 0),
        ("topic", 0, None),
    ],
)
def test_consumed_message_requires_source_lineage(
    topic: str | None,
    partition: int | None,
    offset: int | None,
) -> None:
    writer = _writer(FakeConsumer(), FakeStorage([]), FakeDlq([]))

    with pytest.raises(ValueError, match="source lineage"):
        writer.process(
            cast(
                Message,
                FakeMessage(
                    value=_envelope_bytes(),
                    topic=topic,
                    partition=partition,
                    offset=offset,
                ),
            )
        )


def test_run_loop_subscribes_polls_and_logs_structured_outcome(
    caplog: pytest.LogCaptureFixture,
) -> None:
    actions: list[str] = []
    consumer = FakeConsumer()
    consumer.actions = actions
    consumer.messages = [
        None,
        cast(Message, FakeMessage(value=_envelope_bytes())),
        KeyboardInterrupt(),
    ]
    writer = _writer(consumer, FakeStorage(actions), FakeDlq(actions))

    with caplog.at_level("INFO"), pytest.raises(KeyboardInterrupt):
        writer.run_forever()

    assert consumer.subscribed == ["github.events.raw.v1"]
    log_record = json.loads(caplog.messages[0])
    assert log_record == {
        "event": "warehouse_record_processed",
        "offset": 7,
        "outcome": "inserted",
        "partition": 1,
        "topic": "github.events.raw.v1",
    }


@pytest.mark.parametrize(
    ("event_name", "field_name"),
    [
        (GitHubEventName.PULL_REQUEST, "updated_at"),
        (GitHubEventName.PULL_REQUEST_REVIEW, "submitted_at"),
        (GitHubEventName.WORKFLOW_RUN, "updated_at"),
        (GitHubEventName.DEPLOYMENT, "created_at"),
        (GitHubEventName.DEPLOYMENT_STATUS, "created_at"),
    ],
)
def test_occurred_at_uses_event_family_source_timestamp(
    event_name: GitHubEventName,
    field_name: str,
) -> None:
    envelope = GitHubEventEnvelope.model_validate_json(
        _envelope_bytes(
            event_name=event_name,
            event_object={field_name: "2026-07-24T13:45:00Z"},
        )
    )

    assert _extract_occurred_at(envelope) == datetime(
        2026,
        7,
        24,
        13,
        45,
        tzinfo=UTC,
    )


@pytest.mark.parametrize(
    "event_object",
    [
        None,
        {"updated_at": "not-a-time"},
        {"updated_at": "2026-07-24T13:45:00"},
    ],
)
def test_occurred_at_is_null_without_trustworthy_aware_timestamp(
    event_object: dict[str, Any] | None,
) -> None:
    envelope = GitHubEventEnvelope.model_validate_json(_envelope_bytes(event_object=event_object))
    if event_object is None:
        envelope.payload["pull_request"] = "not-an-object"

    assert _extract_occurred_at(envelope) is None


def test_consumer_factory_disables_automatic_offset_advancement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}
    consumer = FakeConsumer()

    def consumer_factory(config: dict[str, Any]) -> FakeConsumer:
        captured.update(config)
        return consumer

    monkeypatch.setattr(
        "github_analytics.consumers.warehouse.Consumer",
        consumer_factory,
    )
    settings = WarehouseSettings(
        kafka_bootstrap_servers="kafka.example:9092",
        kafka_group_id="warehouse-writer-test",
    )

    assert create_consumer(settings) is consumer
    assert captured == {
        "bootstrap.servers": "kafka.example:9092",
        "client.id": "warehouse-writer",
        "group.id": "warehouse-writer-test",
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "auto.offset.reset": "earliest",
    }


def test_pool_factory_uses_secret_connection_string_and_bounded_pool(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}
    pool = object()

    def pool_factory(conninfo: str, **kwargs: Any) -> object:
        captured["conninfo"] = conninfo
        captured.update(kwargs)
        return pool

    monkeypatch.setattr(
        "github_analytics.consumers.warehouse.ConnectionPool",
        pool_factory,
    )
    settings = WarehouseSettings(
        database_url=SecretStr("postgresql://example.invalid/database"),
        database_pool_timeout_seconds=2.5,
    )

    assert create_pool(settings) is pool
    assert captured == {
        "conninfo": "postgresql://example.invalid/database",
        "min_size": 1,
        "max_size": 4,
        "timeout": 2.5,
        "open": True,
    }


class FakeRuntimePool(AbstractContextManager["FakeRuntimePool"]):
    def __init__(self) -> None:
        self.wait_timeout: float | None = None

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exc_type, exc_value, traceback

    def wait(self, timeout: float) -> None:
        self.wait_timeout = timeout


@pytest.mark.parametrize("interrupt", [True, False])
def test_main_closes_clients_on_interrupt_or_worker_return(
    monkeypatch: pytest.MonkeyPatch,
    interrupt: bool,
) -> None:
    pool = FakeRuntimePool()
    consumer = FakeConsumer()
    dlq = FakeDlq([])

    class InterruptingWriter:
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            pass

        def run_forever(self) -> None:
            if interrupt:
                raise KeyboardInterrupt

    monkeypatch.setattr(
        "github_analytics.consumers.warehouse.create_pool",
        lambda settings: cast(ConnectionPool[Any], pool),
    )
    monkeypatch.setattr(
        "github_analytics.consumers.warehouse.create_consumer",
        lambda settings: cast(ConsumerClient, consumer),
    )
    monkeypatch.setattr(
        "github_analytics.consumers.warehouse.create_dlq_publisher",
        lambda settings: dlq,
    )
    monkeypatch.setattr(
        "github_analytics.consumers.warehouse.WarehouseWriter",
        InterruptingWriter,
    )

    assert main() == 0
    assert pool.wait_timeout == 5.0
    assert consumer.closed
    assert dlq.closed
