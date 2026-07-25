"""Opt-in live evidence for independent groups, reviews, and stale alerts."""

import asyncio
import os
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from time import monotonic
from typing import Any, cast
from uuid import uuid4

import pytest
from confluent_kafka import Consumer, KafkaException, Message, TopicPartition
from psycopg_pool import ConnectionPool
from pydantic import SecretStr

from github_analytics.consumers.config import WarehouseSettings
from github_analytics.consumers.dlq import create_dlq_publisher
from github_analytics.consumers.pr_monitor import (
    MonitorOutcome,
    PrMonitor,
)
from github_analytics.consumers.pr_monitor import (
    create_consumer as create_pr_monitor_consumer,
)
from github_analytics.consumers.pr_monitor_config import PrMonitorSettings
from github_analytics.consumers.pr_storage import PostgresPullRequestStorage
from github_analytics.consumers.storage import PostgresRawEventStorage
from github_analytics.consumers.warehouse import (
    ProcessingOutcome,
    WarehouseWriter,
)
from github_analytics.consumers.warehouse import (
    create_consumer as create_warehouse_consumer,
)
from github_analytics.webhook.config import WebhookSettings
from github_analytics.webhook.kafka import create_kafka_publisher
from github_analytics.webhook.models import GitHubEventEnvelope, GitHubEventName

pytestmark = pytest.mark.skipif(
    os.getenv("RUN_PR_MONITOR_INTEGRATION") != "1",
    reason="requires RUN_PR_MONITOR_INTEGRATION=1 and migrated local services",
)

DATABASE_URL = "postgresql://github_analytics:local_only_change_me@localhost:55432/github_analytics"
RAW_TOPIC = "github.events.raw.v1"


def test_independent_groups_first_review_and_idempotent_stale_alert() -> None:
    unique = uuid4().int
    repository_id = 700_000_000 + unique % 100_000_000
    reviewed_pr_id = 800_000_000 + unique % 100_000_000
    stale_pr_id = 900_000_000 + unique % 100_000_000
    reviewed_pr_number = 10_000 + unique % 10_000
    stale_pr_number = reviewed_pr_number + 20_000
    warehouse_group = f"warehouse-independence-{uuid4()}"
    monitor_group = f"pr-monitor-independence-{uuid4()}"
    opened_at = datetime(2026, 7, 23, 10, tzinfo=UTC)
    open_event = _pull_request_envelope(
        repository_id=repository_id,
        pull_request_id=reviewed_pr_id,
        pull_request_number=reviewed_pr_number,
        delivery_id=f"pr-open-{uuid4()}",
        state="open",
        action="opened",
        opened_at=opened_at,
        updated_at=opened_at,
    )
    source_value = open_event.model_dump_json().encode()
    _publish(open_event)

    warehouse_settings = _warehouse_settings(warehouse_group)
    monitor_settings = _monitor_settings(monitor_group)
    with ConnectionPool[Any](
        DATABASE_URL,
        min_size=1,
        max_size=1,
        open=True,
    ) as pool:
        warehouse_consumer = create_warehouse_consumer(warehouse_settings)
        warehouse_dlq = create_dlq_publisher(warehouse_settings)
        warehouse = WarehouseWriter(
            warehouse_consumer,
            PostgresRawEventStorage(pool),
            warehouse_dlq,
            raw_topic=RAW_TOPIC,
            poll_timeout_seconds=0.25,
        )
        monitor_consumer = create_pr_monitor_consumer(monitor_settings)
        monitor_dlq = create_dlq_publisher(
            monitor_settings,
            client_id="pr-monitor-integration-dlq",
        )
        monitor = PrMonitor(
            monitor_consumer,
            PostgresPullRequestStorage(pool),
            monitor_dlq,
            raw_topic=RAW_TOPIC,
            poll_timeout_seconds=0.25,
            stale_after=timedelta(hours=24),
            stale_sweep_interval=timedelta(minutes=1),
        )
        warehouse_consumer.subscribe([RAW_TOPIC])
        monitor_consumer.subscribe([RAW_TOPIC])
        warehouse_message = _poll_for(
            warehouse_consumer,
            lambda message: message.value() == source_value,
        )
        monitor_message = _poll_for(
            monitor_consumer,
            lambda message: message.value() == source_value,
        )

        assert warehouse_message.partition() == monitor_message.partition()
        assert warehouse_message.offset() == monitor_message.offset()
        assert warehouse.process(warehouse_message) is ProcessingOutcome.INSERTED
        assert monitor.process(monitor_message) is MonitorOutcome.PROJECTED
        _assert_group_committed_message(
            cast(Consumer, warehouse_consumer),
            warehouse_message,
        )
        _assert_group_committed_message(
            cast(Consumer, monitor_consumer),
            monitor_message,
        )

        author_review = _review_envelope(
            repository_id=repository_id,
            pull_request_id=reviewed_pr_id,
            pull_request_number=reviewed_pr_number,
            delivery_id=f"author-review-{uuid4()}",
            opened_at=opened_at,
            updated_at=opened_at + timedelta(hours=1),
            review_id=unique % 100_000_000 + 1,
            reviewer_id=40001,
            submitted_at=opened_at + timedelta(hours=1),
        )
        later_review = _review_envelope(
            repository_id=repository_id,
            pull_request_id=reviewed_pr_id,
            pull_request_number=reviewed_pr_number,
            delivery_id=f"later-review-{uuid4()}",
            opened_at=opened_at,
            updated_at=opened_at + timedelta(hours=3),
            review_id=unique % 100_000_000 + 2,
            reviewer_id=40002,
            submitted_at=opened_at + timedelta(hours=3),
        )
        earlier_review = _review_envelope(
            repository_id=repository_id,
            pull_request_id=reviewed_pr_id,
            pull_request_number=reviewed_pr_number,
            delivery_id=f"earlier-review-{uuid4()}",
            opened_at=opened_at,
            updated_at=opened_at + timedelta(hours=2),
            review_id=unique % 100_000_000 + 3,
            reviewer_id=40003,
            submitted_at=opened_at + timedelta(hours=2),
        )
        assert (
            _publish_poll_process(monitor, monitor_consumer, author_review)
            is MonitorOutcome.REVIEW_INELIGIBLE
        )
        assert (
            _publish_poll_process(monitor, monitor_consumer, later_review)
            is MonitorOutcome.REVIEW_RECORDED
        )
        assert (
            _publish_poll_process(monitor, monitor_consumer, earlier_review)
            is MonitorOutcome.REVIEW_RECORDED
        )
        reviewed_close = _pull_request_envelope(
            repository_id=repository_id,
            pull_request_id=reviewed_pr_id,
            pull_request_number=reviewed_pr_number,
            delivery_id=f"reviewed-close-{uuid4()}",
            state="closed",
            action="closed",
            opened_at=opened_at,
            updated_at=opened_at + timedelta(hours=4),
        )
        reviewed_reopen = _pull_request_envelope(
            repository_id=repository_id,
            pull_request_id=reviewed_pr_id,
            pull_request_number=reviewed_pr_number,
            delivery_id=f"reviewed-reopen-{uuid4()}",
            state="open",
            action="reopened",
            opened_at=opened_at,
            updated_at=opened_at + timedelta(hours=5),
        )
        assert (
            _publish_poll_process(monitor, monitor_consumer, reviewed_close)
            is MonitorOutcome.PROJECTED
        )
        assert (
            _publish_poll_process(monitor, monitor_consumer, reviewed_reopen)
            is MonitorOutcome.PROJECTED
        )

        stale_open = _pull_request_envelope(
            repository_id=repository_id,
            pull_request_id=stale_pr_id,
            pull_request_number=stale_pr_number,
            delivery_id=f"stale-open-{uuid4()}",
            state="open",
            action="opened",
            opened_at=opened_at,
            updated_at=opened_at,
        )
        assert (
            _publish_poll_process(monitor, monitor_consumer, stale_open) is MonitorOutcome.PROJECTED
        )
        sweep_at = opened_at + timedelta(days=2)
        assert monitor.sweep_stale(now=sweep_at) == 1
        assert monitor.sweep_stale(now=sweep_at) == 0

        stale_close = _pull_request_envelope(
            repository_id=repository_id,
            pull_request_id=stale_pr_id,
            pull_request_number=stale_pr_number,
            delivery_id=f"stale-close-{uuid4()}",
            state="closed",
            action="closed",
            opened_at=opened_at,
            updated_at=sweep_at + timedelta(minutes=1),
        )
        assert (
            _publish_poll_process(monitor, monitor_consumer, stale_close)
            is MonitorOutcome.PROJECTED
        )

        with pool.connection() as connection:
            raw_count = connection.execute(
                """
                SELECT count(*)
                FROM raw.github_events
                WHERE source = 'webhook' AND source_record_key = %s
                """,
                (open_event.delivery_id,),
            ).fetchone()
            reviewed_projection = connection.execute(
                """
                SELECT first_eligible_review_at, first_eligible_reviewer_id
                FROM serving.open_pull_requests
                WHERE repository_id = %s AND pull_request_id = %s
                """,
                (repository_id, reviewed_pr_id),
            ).fetchone()
            stale_projection_count = connection.execute(
                """
                SELECT count(*)
                FROM serving.open_pull_requests
                WHERE repository_id = %s AND pull_request_id = %s
                """,
                (repository_id, stale_pr_id),
            ).fetchone()
            alert_rows = connection.execute(
                """
                SELECT count(*), min(status)
                FROM ops.alert_outbox
                WHERE alert_key = %s
                """,
                (f"stale-pull-request:{repository_id}:{stale_pr_id}",),
            ).fetchone()

        warehouse_consumer.close()
        warehouse_dlq.close()
        monitor_consumer.close()
        monitor_dlq.close()

    assert raw_count == (1,)
    assert reviewed_projection == (opened_at + timedelta(hours=2), 40003)
    assert stale_projection_count == (0,)
    assert alert_rows == (1, "cancelled")


def _pull_request_envelope(
    *,
    repository_id: int,
    pull_request_id: int,
    pull_request_number: int,
    delivery_id: str,
    state: str,
    action: str,
    opened_at: datetime,
    updated_at: datetime,
) -> GitHubEventEnvelope:
    return GitHubEventEnvelope.from_webhook(
        delivery_id=delivery_id,
        event_name=GitHubEventName.PULL_REQUEST,
        received_at=datetime.now(UTC),
        payload=_payload(
            repository_id=repository_id,
            pull_request_id=pull_request_id,
            pull_request_number=pull_request_number,
            state=state,
            action=action,
            opened_at=opened_at,
            updated_at=updated_at,
        ),
    )


def _review_envelope(
    *,
    repository_id: int,
    pull_request_id: int,
    pull_request_number: int,
    delivery_id: str,
    opened_at: datetime,
    updated_at: datetime,
    review_id: int,
    reviewer_id: int,
    submitted_at: datetime,
) -> GitHubEventEnvelope:
    payload = _payload(
        repository_id=repository_id,
        pull_request_id=pull_request_id,
        pull_request_number=pull_request_number,
        state="open",
        action="submitted",
        opened_at=opened_at,
        updated_at=updated_at,
    )
    payload["review"] = {
        "id": review_id,
        "state": "approved",
        "user": {
            "id": reviewer_id,
            "login": f"reviewer-{reviewer_id}",
        },
        "submitted_at": submitted_at.isoformat(),
    }
    return GitHubEventEnvelope.from_webhook(
        delivery_id=delivery_id,
        event_name=GitHubEventName.PULL_REQUEST_REVIEW,
        received_at=datetime.now(UTC),
        payload=payload,
    )


def _payload(
    *,
    repository_id: int,
    pull_request_id: int,
    pull_request_number: int,
    state: str,
    action: str,
    opened_at: datetime,
    updated_at: datetime,
) -> dict[str, Any]:
    return {
        "action": action,
        "installation": {"id": 10001},
        "repository": {
            "id": repository_id,
            "full_name": "example-org/delivery-demo",
        },
        "pull_request": {
            "id": pull_request_id,
            "number": pull_request_number,
            "state": state,
            "title": f"Synthetic PR {pull_request_number}",
            "user": {"id": 40001, "login": "example-author"},
            "draft": False,
            "created_at": opened_at.isoformat(),
            "updated_at": updated_at.isoformat(),
        },
    }


def _warehouse_settings(group_id: str) -> WarehouseSettings:
    return WarehouseSettings(
        database_url=SecretStr(DATABASE_URL),
        kafka_bootstrap_servers="localhost:9092",
        kafka_raw_topic=RAW_TOPIC,
        kafka_group_id=group_id,
        kafka_poll_timeout_seconds=0.25,
    )


def _monitor_settings(group_id: str) -> PrMonitorSettings:
    return PrMonitorSettings(
        database_url=SecretStr(DATABASE_URL),
        kafka_bootstrap_servers="localhost:9092",
        kafka_raw_topic=RAW_TOPIC,
        kafka_group_id=group_id,
        kafka_poll_timeout_seconds=0.25,
    )


def _publish(envelope: GitHubEventEnvelope) -> None:
    settings = WebhookSettings(
        webhook_secret=SecretStr("integration-only-secret"),
        kafka_bootstrap_servers="localhost:9092",
        kafka_raw_topic=RAW_TOPIC,
        kafka_publish_timeout_seconds=5,
    )
    asyncio.run(create_kafka_publisher(settings).publish(envelope))


def _publish_poll_process(
    monitor: PrMonitor,
    consumer: Any,
    envelope: GitHubEventEnvelope,
) -> MonitorOutcome:
    value = envelope.model_dump_json().encode()
    _publish(envelope)
    message = _poll_for(consumer, lambda candidate: candidate.value() == value)
    return monitor.process(message)


def _poll_for(
    consumer: Any,
    predicate: Callable[[Message], bool],
) -> Message:
    deadline = monotonic() + 20
    while monotonic() < deadline:
        message = consumer.poll(0.25)
        if message is None:
            continue
        error = message.error()
        if error is not None:
            raise KafkaException(error)
        if predicate(message):
            return cast(Message, message)
        consumer.commit(message=message, asynchronous=False)
    raise AssertionError("timed out waiting for the integration-test record")


def _assert_group_committed_message(
    consumer: Consumer,
    message: Message,
) -> None:
    assert consumer.memberid() is not None
    partition = message.partition()
    offset = message.offset()
    assert partition is not None
    assert offset is not None
    committed = consumer.committed(
        [TopicPartition(RAW_TOPIC, partition)],
        timeout=5,
    )
    assert committed[0].offset == offset + 1
