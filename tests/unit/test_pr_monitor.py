"""Tests for the independent pull-request monitor consumer."""

from contextlib import AbstractContextManager
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace, TracebackType
from typing import Any, Self, cast

import pytest
from confluent_kafka import KafkaError, KafkaException, Message, TopicPartition
from psycopg_pool import ConnectionPool
from pydantic import SecretStr

from github_analytics.consumers.dlq import DeadLetterRecord
from github_analytics.consumers.pr_monitor import (
    ConsumerClient,
    MonitorOutcome,
    PrMonitor,
    _message_lineage,
    create_consumer,
    create_pool,
    main,
)
from github_analytics.consumers.pr_monitor_config import PrMonitorSettings
from github_analytics.consumers.pr_storage import (
    ProjectionOutcome,
    PullRequestSnapshot,
    ReviewOutcome,
    SubmittedReview,
)
from github_analytics.webhook.models import GitHubEventEnvelope, GitHubEventName

NOW = datetime(2026, 7, 25, 12, tzinfo=UTC)


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
    def __init__(self, actions: list[str]) -> None:
        self.actions = actions
        self.projection_outcome = ProjectionOutcome.APPLIED
        self.review_outcome = ReviewOutcome.RECORDED
        self.snapshot: PullRequestSnapshot | None = None
        self.review: SubmittedReview | None = None
        self.sweep_count = 0
        self.sweep_arguments: tuple[datetime, datetime] | None = None

    def apply_pull_request(self, snapshot: PullRequestSnapshot) -> ProjectionOutcome:
        self.actions.append("database_commit")
        self.snapshot = snapshot
        return self.projection_outcome

    def apply_review(self, review: SubmittedReview) -> ReviewOutcome:
        self.actions.append("database_commit")
        self.review = review
        return self.review_outcome

    def create_stale_alerts(
        self,
        *,
        stale_cutoff: datetime,
        created_at: datetime,
    ) -> int:
        self.actions.append("stale_sweep")
        self.sweep_arguments = (stale_cutoff, created_at)
        return self.sweep_count


class FakeDlq:
    def __init__(self, actions: list[str]) -> None:
        self.actions = actions
        self.record: DeadLetterRecord | None = None
        self.closed = False

    def publish(self, record: DeadLetterRecord) -> None:
        self.actions.append("dlq_ack")
        self.record = record

    def close(self) -> None:
        self.closed = True


def _pull_request_payload(
    *,
    action: str = "opened",
    state: str = "open",
    author_id: int = 40001,
) -> dict[str, Any]:
    return {
        "action": action,
        "installation": {"id": 10001},
        "repository": {
            "id": 20001,
            "full_name": "example-org/delivery-demo",
        },
        "pull_request": {
            "id": 30001,
            "number": 17,
            "state": state,
            "title": "Add deterministic PR monitoring",
            "user": {"id": author_id, "login": "example-author"},
            "draft": False,
            "created_at": "2026-07-24T10:00:00Z",
            "updated_at": "2026-07-25T11:00:00Z",
        },
    }


def _envelope_bytes(
    *,
    event_name: GitHubEventName = GitHubEventName.PULL_REQUEST,
    action: str = "opened",
    payload: dict[str, Any] | None = None,
) -> bytes:
    event_payload = payload or _pull_request_payload(action=action)
    event_payload["action"] = action
    envelope = GitHubEventEnvelope.from_webhook(
        delivery_id="delivery-pr-monitor",
        event_name=event_name,
        received_at=NOW,
        payload=event_payload,
    )
    return envelope.model_dump_json().encode()


def _review_bytes(
    *,
    action: str = "submitted",
    reviewer_id: int = 40002,
    state: str = "approved",
) -> bytes:
    payload = _pull_request_payload(action=action)
    payload["review"] = {
        "id": 50001,
        "state": state,
        "user": {"id": reviewer_id, "login": "example-reviewer"},
        "submitted_at": "2026-07-25T11:30:00Z",
    }
    return _envelope_bytes(
        event_name=GitHubEventName.PULL_REQUEST_REVIEW,
        action=action,
        payload=payload,
    )


def _monitor(
    consumer: FakeConsumer,
    storage: FakeStorage,
    dlq: FakeDlq,
    *,
    clock: Any = lambda: NOW,
) -> PrMonitor:
    return PrMonitor(
        cast(ConsumerClient, consumer),
        storage,
        dlq,
        raw_topic="github.events.raw.v1",
        poll_timeout_seconds=0.25,
        stale_after=timedelta(hours=24),
        stale_sweep_interval=timedelta(minutes=1),
        clock=clock,
    )


@pytest.mark.parametrize(
    ("projection_outcome", "expected"),
    [
        (ProjectionOutcome.APPLIED, MonitorOutcome.PROJECTED),
        (ProjectionOutcome.STALE, MonitorOutcome.STALE),
    ],
)
def test_pull_request_snapshot_commits_database_before_source_offset(
    projection_outcome: ProjectionOutcome,
    expected: MonitorOutcome,
) -> None:
    consumer = FakeConsumer()
    storage = FakeStorage(consumer.actions)
    storage.projection_outcome = projection_outcome
    monitor = _monitor(consumer, storage, FakeDlq(consumer.actions))

    outcome = monitor.process(cast(Message, FakeMessage(value=_envelope_bytes())))

    assert outcome is expected
    assert consumer.actions == ["database_commit", "offset_commit"]
    assert storage.snapshot is not None
    assert storage.snapshot.pull_request_id == 30001
    assert storage.snapshot.repository_full_name == "example-org/delivery-demo"
    assert storage.snapshot.updated_at == datetime(
        2026,
        7,
        25,
        11,
        tzinfo=UTC,
    )


@pytest.mark.parametrize(
    ("review_outcome", "expected"),
    [
        (ReviewOutcome.RECORDED, MonitorOutcome.REVIEW_RECORDED),
        (ReviewOutcome.INELIGIBLE, MonitorOutcome.REVIEW_INELIGIBLE),
        (ReviewOutcome.UNCHANGED, MonitorOutcome.REVIEW_UNCHANGED),
    ],
)
def test_submitted_review_outcomes_commit_after_database(
    review_outcome: ReviewOutcome,
    expected: MonitorOutcome,
) -> None:
    consumer = FakeConsumer()
    storage = FakeStorage(consumer.actions)
    storage.review_outcome = review_outcome
    monitor = _monitor(consumer, storage, FakeDlq(consumer.actions))

    outcome = monitor.process(cast(Message, FakeMessage(value=_review_bytes())))

    assert outcome is expected
    assert consumer.actions == ["database_commit", "offset_commit"]
    assert storage.review is not None
    assert storage.review.review_id == 50001
    assert storage.review.reviewer_id == 40002
    assert storage.review.submitted_at == datetime(
        2026,
        7,
        25,
        11,
        30,
        tzinfo=UTC,
    )


@pytest.mark.parametrize(
    "value",
    [
        None,
        b'{"schema_version":999}',
    ],
)
def test_invalid_envelope_reaches_dlq_before_offset_commit(
    value: bytes | None,
) -> None:
    consumer = FakeConsumer()
    storage = FakeStorage(consumer.actions)
    dlq = FakeDlq(consumer.actions)
    monitor = _monitor(consumer, storage, dlq)

    assert monitor.process(cast(Message, FakeMessage(value=value))) is MonitorOutcome.DLQ
    assert consumer.actions == ["dlq_ack", "offset_commit"]
    assert dlq.record is not None
    assert dlq.record.failure_reason == "invalid_github_event_envelope"
    assert storage.snapshot is None


@pytest.mark.parametrize(
    "value",
    [
        _envelope_bytes(payload=_pull_request_payload() | {"pull_request": {"id": 1}}),
        _review_bytes(state="pending"),
    ],
)
def test_unprojectable_pr_event_reaches_dlq_before_offset_commit(
    value: bytes,
) -> None:
    consumer = FakeConsumer()
    dlq = FakeDlq(consumer.actions)
    monitor = _monitor(consumer, FakeStorage(consumer.actions), dlq)

    assert monitor.process(cast(Message, FakeMessage(value=value))) is MonitorOutcome.DLQ
    assert consumer.actions == ["dlq_ack", "offset_commit"]
    assert dlq.record is not None
    assert dlq.record.failure_reason == "invalid_pr_monitor_event"


@pytest.mark.parametrize(
    "value",
    [
        _envelope_bytes(
            event_name=GitHubEventName.WORKFLOW_RUN,
            payload={
                "action": "completed",
                "installation": {"id": 10001},
                "repository": {
                    "id": 20001,
                    "full_name": "example-org/delivery-demo",
                },
            },
        ),
        _review_bytes(action="dismissed"),
    ],
)
def test_irrelevant_event_commits_without_database_effect(value: bytes) -> None:
    consumer = FakeConsumer()
    storage = FakeStorage(consumer.actions)
    monitor = _monitor(consumer, storage, FakeDlq(consumer.actions))

    assert monitor.process(cast(Message, FakeMessage(value=value))) is MonitorOutcome.IGNORED
    assert consumer.actions == ["offset_commit"]


def test_crash_hook_after_database_commit_prevents_offset_commit() -> None:
    consumer = FakeConsumer()
    storage = FakeStorage(consumer.actions)
    monitor = _monitor(consumer, storage, FakeDlq(consumer.actions))

    def simulate_crash() -> None:
        consumer.actions.append("crash")
        raise RuntimeError("simulated crash")

    with pytest.raises(RuntimeError, match="simulated crash"):
        monitor.process(
            cast(Message, FakeMessage(value=_envelope_bytes())),
            after_database_commit=simulate_crash,
        )
    assert consumer.actions == ["database_commit", "crash"]


def test_consumed_message_error_stops_processing() -> None:
    error = KafkaError(KafkaError._TRANSPORT)
    monitor = _monitor(FakeConsumer(), FakeStorage([]), FakeDlq([]))

    with pytest.raises(KafkaException):
        monitor.process(cast(Message, FakeMessage(value=b"{}", error=error)))


def test_commit_call_failure_stops_processing() -> None:
    consumer = FakeConsumer()
    consumer.commit_error = KafkaException(KafkaError(KafkaError._TRANSPORT))
    monitor = _monitor(consumer, FakeStorage(consumer.actions), FakeDlq([]))

    with pytest.raises(KafkaException):
        monitor.process(cast(Message, FakeMessage(value=_envelope_bytes())))


def test_commit_partition_error_stops_processing() -> None:
    consumer = FakeConsumer()
    consumer.commit_result = cast(
        list[TopicPartition],
        [SimpleNamespace(error=KafkaError(KafkaError._TRANSPORT))],
    )
    monitor = _monitor(consumer, FakeStorage(consumer.actions), FakeDlq([]))

    with pytest.raises(KafkaException):
        monitor.process(cast(Message, FakeMessage(value=_envelope_bytes())))


def test_none_commit_result_is_successful() -> None:
    consumer = FakeConsumer()
    consumer.commit_result = None
    monitor = _monitor(consumer, FakeStorage(consumer.actions), FakeDlq([]))

    assert (
        monitor.process(cast(Message, FakeMessage(value=_envelope_bytes())))
        is MonitorOutcome.PROJECTED
    )


def test_sweep_uses_configured_stale_window() -> None:
    storage = FakeStorage([])
    storage.sweep_count = 3
    monitor = _monitor(FakeConsumer(), storage, FakeDlq([]))

    assert monitor.sweep_stale(now=NOW) == 3
    assert storage.sweep_arguments == (NOW - timedelta(hours=24), NOW)


def test_run_loop_subscribes_sweeps_skips_empty_poll_and_logs_record() -> None:
    consumer = FakeConsumer()
    consumer.messages = [
        None,
        cast(Message, FakeMessage(value=_envelope_bytes())),
        KeyboardInterrupt(),
    ]
    storage = FakeStorage(consumer.actions)
    storage.sweep_count = 1
    clock_values = iter(
        [
            NOW,
            NOW,
            NOW + timedelta(seconds=10),
            NOW + timedelta(seconds=20),
        ]
    )
    monitor = _monitor(
        consumer,
        storage,
        FakeDlq(consumer.actions),
        clock=lambda: next(clock_values),
    )

    with pytest.raises(KeyboardInterrupt):
        monitor.run_forever()

    assert consumer.subscribed == ["github.events.raw.v1"]
    assert consumer.actions == [
        "stale_sweep",
        "poll:0.25",
        "poll:0.25",
        "database_commit",
        "offset_commit",
        "poll:0.25",
    ]


@pytest.mark.parametrize(
    "message",
    [
        FakeMessage(value=b"{}", topic=None),
        FakeMessage(value=b"{}", partition=None),
        FakeMessage(value=b"{}", offset=None),
    ],
)
def test_missing_message_lineage_is_rejected(message: FakeMessage) -> None:
    with pytest.raises(ValueError, match="source lineage"):
        _message_lineage(cast(Message, message))


def test_consumer_factory_uses_independent_manual_commit_group(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}
    consumer = FakeConsumer()

    def consumer_factory(config: dict[str, Any]) -> FakeConsumer:
        captured.update(config)
        return consumer

    monkeypatch.setattr(
        "github_analytics.consumers.pr_monitor.Consumer",
        consumer_factory,
    )
    settings = PrMonitorSettings(
        kafka_bootstrap_servers="kafka.example:9092",
        kafka_group_id="pr-monitor-test",
    )

    assert create_consumer(settings) is consumer
    assert captured == {
        "bootstrap.servers": "kafka.example:9092",
        "client.id": "pr-monitor",
        "group.id": "pr-monitor-test",
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
        "github_analytics.consumers.pr_monitor.ConnectionPool",
        pool_factory,
    )
    settings = PrMonitorSettings(
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
    captured: dict[str, Any] = {}

    class InterruptingMonitor:
        def __init__(self, *_args: Any, **kwargs: Any) -> None:
            captured.update(kwargs)

        def run_forever(self) -> None:
            if interrupt:
                raise KeyboardInterrupt

    def dlq_factory(settings: Any, *, client_id: str) -> FakeDlq:
        del settings
        captured["dlq_client_id"] = client_id
        return dlq

    monkeypatch.setattr(
        "github_analytics.consumers.pr_monitor.create_pool",
        lambda settings: cast(ConnectionPool[Any], pool),
    )
    monkeypatch.setattr(
        "github_analytics.consumers.pr_monitor.create_consumer",
        lambda settings: cast(ConsumerClient, consumer),
    )
    monkeypatch.setattr(
        "github_analytics.consumers.pr_monitor.create_dlq_publisher",
        dlq_factory,
    )
    monkeypatch.setattr(
        "github_analytics.consumers.pr_monitor.PrMonitor",
        InterruptingMonitor,
    )

    assert main() == 0
    assert pool.wait_timeout == 5.0
    assert captured["dlq_client_id"] == "pr-monitor-dlq"
    assert captured["raw_topic"] == "github.events.raw.v1"
    assert captured["stale_after"] == timedelta(hours=24)
    assert captured["stale_sweep_interval"] == timedelta(seconds=60)
    assert consumer.closed
    assert dlq.closed
