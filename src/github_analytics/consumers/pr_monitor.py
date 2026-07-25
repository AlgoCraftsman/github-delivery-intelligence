"""Independent Kafka consumer for pull-request state and stale alerts."""

import json
import logging
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Annotated, Any, Protocol, cast

from confluent_kafka import Consumer, KafkaException, Message, TopicPartition
from psycopg_pool import ConnectionPool
from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    ValidationError,
)

from github_analytics.consumers.dlq import (
    DeadLetterRecord,
    DlqFailureReason,
    create_dlq_publisher,
)
from github_analytics.consumers.pr_monitor_config import PrMonitorSettings
from github_analytics.consumers.pr_storage import (
    PostgresPullRequestStorage,
    ProjectionOutcome,
    PullRequestSnapshot,
    PullRequestState,
    ReviewOutcome,
    SubmittedReview,
)
from github_analytics.webhook.models import GitHubEventEnvelope, GitHubEventName

logger = logging.getLogger(__name__)
_NonEmptyString = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


class ConsumerClient(Protocol):
    """Subset of the Confluent consumer used by the PR monitor."""

    def subscribe(self, topics: list[str]) -> None: ...

    def poll(self, timeout: float) -> Message | None: ...

    def commit(
        self,
        *,
        message: Message,
        asynchronous: bool,
    ) -> list[TopicPartition] | None: ...

    def close(self) -> None: ...


class ProjectionStorage(Protocol):
    """Transactional projection and outbox boundary."""

    def apply_pull_request(self, snapshot: PullRequestSnapshot) -> ProjectionOutcome: ...

    def apply_review(self, review: SubmittedReview) -> ReviewOutcome: ...

    def create_stale_alerts(
        self,
        *,
        stale_cutoff: datetime,
        created_at: datetime,
    ) -> int: ...


class DlqPublisher(Protocol):
    """Acknowledged dead-letter boundary."""

    def publish(self, record: DeadLetterRecord) -> None: ...


class MonitorOutcome(StrEnum):
    """Observable result after the source offset is committed."""

    PROJECTED = "projected"
    STALE = "stale"
    REVIEW_RECORDED = "review_recorded"
    REVIEW_INELIGIBLE = "review_ineligible"
    REVIEW_UNCHANGED = "review_unchanged"
    IGNORED = "ignored"
    DLQ = "dlq"


class _Actor(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: int = Field(gt=0)
    login: _NonEmptyString


class _PullRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: int = Field(gt=0)
    number: int = Field(gt=0)
    state: PullRequestState
    title: _NonEmptyString
    user: _Actor
    draft: bool
    created_at: AwareDatetime
    updated_at: AwareDatetime


class _PullRequestPayload(BaseModel):
    model_config = ConfigDict(extra="ignore")

    pull_request: _PullRequest


class _ReviewState(StrEnum):
    APPROVED = "approved"
    CHANGES_REQUESTED = "changes_requested"
    COMMENTED = "commented"


class _Review(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: int = Field(gt=0)
    state: _ReviewState
    user: _Actor
    submitted_at: AwareDatetime


class _SubmittedReviewPayload(_PullRequestPayload):
    review: _Review


type ProjectionCommand = PullRequestSnapshot | SubmittedReview


class PrMonitor:
    """Apply PR effects before synchronously committing source offsets."""

    def __init__(
        self,
        consumer: ConsumerClient,
        storage: ProjectionStorage,
        dlq_publisher: DlqPublisher,
        *,
        raw_topic: str,
        poll_timeout_seconds: float,
        stale_after: timedelta,
        stale_sweep_interval: timedelta,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._consumer = consumer
        self._storage = storage
        self._dlq_publisher = dlq_publisher
        self._raw_topic = raw_topic
        self._poll_timeout_seconds = poll_timeout_seconds
        self._stale_after = stale_after
        self._stale_sweep_interval = stale_sweep_interval
        self._clock = clock

    def run_forever(self) -> None:
        """Poll and sweep until interrupted; durable-boundary failures stop the worker."""

        self._consumer.subscribe([self._raw_topic])
        next_sweep_at = self._clock()
        while True:
            now = self._clock()
            if now >= next_sweep_at:
                inserted = self.sweep_stale(now=now)
                logger.info(
                    json.dumps(
                        {
                            "event": "pr_monitor_stale_sweep",
                            "alerts_created": inserted,
                            "stale_cutoff": (now - self._stale_after).isoformat(),
                        },
                        separators=(",", ":"),
                        sort_keys=True,
                    )
                )
                next_sweep_at = now + self._stale_sweep_interval
            message = self._consumer.poll(self._poll_timeout_seconds)
            if message is None:
                continue
            outcome = self.process(message)
            topic, partition, offset = _message_lineage(message)
            logger.info(
                json.dumps(
                    {
                        "event": "pr_monitor_record_processed",
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
    ) -> MonitorOutcome:
        """Produce one durable effect, then commit that message's source offset."""

        message_error = message.error()
        if message_error is not None:
            raise KafkaException(message_error)
        try:
            envelope = _validate_envelope(message.value())
        except (TypeError, ValidationError):
            return self._dead_letter(
                message,
                failure_reason="invalid_github_event_envelope",
            )
        try:
            command = _projection_command(envelope)
        except ValidationError:
            return self._dead_letter(
                message,
                failure_reason="invalid_pr_monitor_event",
            )
        if command is None:
            self._commit_source_offset(message)
            return MonitorOutcome.IGNORED

        if isinstance(command, PullRequestSnapshot):
            storage_outcome = self._storage.apply_pull_request(command)
            outcome = {
                ProjectionOutcome.APPLIED: MonitorOutcome.PROJECTED,
                ProjectionOutcome.STALE: MonitorOutcome.STALE,
            }[storage_outcome]
        else:
            review_outcome = self._storage.apply_review(command)
            outcome = {
                ReviewOutcome.RECORDED: MonitorOutcome.REVIEW_RECORDED,
                ReviewOutcome.INELIGIBLE: MonitorOutcome.REVIEW_INELIGIBLE,
                ReviewOutcome.UNCHANGED: MonitorOutcome.REVIEW_UNCHANGED,
            }[review_outcome]
        if after_database_commit is not None:
            after_database_commit()
        self._commit_source_offset(message)
        return outcome

    def sweep_stale(self, *, now: datetime) -> int:
        """Create idempotent stale-alert intents for eligible open PRs."""

        return self._storage.create_stale_alerts(
            stale_cutoff=now - self._stale_after,
            created_at=now,
        )

    def _dead_letter(
        self,
        message: Message,
        *,
        failure_reason: DlqFailureReason,
    ) -> MonitorOutcome:
        dead_letter = DeadLetterRecord.from_message(
            message,
            failed_at=self._clock(),
            failure_reason=failure_reason,
        )
        self._dlq_publisher.publish(dead_letter)
        self._commit_source_offset(message)
        return MonitorOutcome.DLQ

    def _commit_source_offset(self, message: Message) -> None:
        committed = self._consumer.commit(message=message, asynchronous=False)
        for partition in committed or []:
            if partition.error is not None:
                raise KafkaException(partition.error)


def create_consumer(settings: PrMonitorSettings) -> ConsumerClient:
    """Create a manual-commit consumer in the independent pr-monitor group."""

    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "client.id": "pr-monitor",
            "group.id": settings.kafka_group_id,
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "auto.offset.reset": "earliest",
        }
    )
    return cast(ConsumerClient, consumer)


def create_pool(settings: PrMonitorSettings) -> ConnectionPool[Any]:
    """Create the synchronous PostgreSQL pool used by the monitor."""

    return ConnectionPool(
        settings.database_url.get_secret_value(),
        min_size=1,
        max_size=4,
        timeout=settings.database_pool_timeout_seconds,
        open=True,
    )


def main() -> int:
    """Run the PR monitor until an orderly keyboard interrupt."""

    settings = PrMonitorSettings()
    with create_pool(settings) as pool:
        pool.wait(timeout=settings.database_pool_timeout_seconds)
        consumer = create_consumer(settings)
        dlq_publisher = create_dlq_publisher(
            settings,
            client_id="pr-monitor-dlq",
        )
        monitor = PrMonitor(
            consumer,
            PostgresPullRequestStorage(pool),
            dlq_publisher,
            raw_topic=settings.kafka_raw_topic,
            poll_timeout_seconds=settings.kafka_poll_timeout_seconds,
            stale_after=timedelta(hours=settings.stale_after_hours),
            stale_sweep_interval=timedelta(seconds=settings.stale_sweep_interval_seconds),
        )
        try:
            monitor.run_forever()
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


def _projection_command(
    envelope: GitHubEventEnvelope,
) -> ProjectionCommand | None:
    if envelope.event_name is GitHubEventName.PULL_REQUEST:
        payload = _PullRequestPayload.model_validate(envelope.payload)
        return _snapshot(envelope, payload.pull_request)
    if envelope.event_name is not GitHubEventName.PULL_REQUEST_REVIEW:
        return None
    if envelope.action != "submitted":
        return None
    payload = _SubmittedReviewPayload.model_validate(envelope.payload)
    return SubmittedReview(
        pull_request=_snapshot(envelope, payload.pull_request),
        review_id=payload.review.id,
        reviewer_id=payload.review.user.id,
        reviewer_login=payload.review.user.login,
        submitted_at=payload.review.submitted_at,
    )


def _snapshot(
    envelope: GitHubEventEnvelope,
    pull_request: _PullRequest,
) -> PullRequestSnapshot:
    return PullRequestSnapshot(
        delivery_id=envelope.delivery_id,
        repository_id=envelope.repository_id,
        repository_full_name=envelope.repository_full_name,
        pull_request_id=pull_request.id,
        pull_request_number=pull_request.number,
        state=pull_request.state,
        title=pull_request.title,
        author_id=pull_request.user.id,
        author_login=pull_request.user.login,
        is_draft=pull_request.draft,
        opened_at=pull_request.created_at,
        updated_at=pull_request.updated_at,
    )


def _message_lineage(message: Message) -> tuple[str, int, int]:
    topic = message.topic()
    partition = message.partition()
    offset = message.offset()
    if topic is None or partition is None or offset is None:
        raise ValueError("consumed message is missing source lineage")
    return topic, partition, offset
