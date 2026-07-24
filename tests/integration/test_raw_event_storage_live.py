"""Opt-in integration test for the real PostgreSQL idempotency constraint."""

import os
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import pytest
from psycopg_pool import ConnectionPool

from github_analytics.consumers.storage import (
    InsertOutcome,
    PostgresRawEventStorage,
    WebhookRawEvent,
)
from github_analytics.webhook.models import GitHubEventEnvelope, GitHubEventName

pytestmark = pytest.mark.skipif(
    os.getenv("RUN_POSTGRES_INTEGRATION") != "1",
    reason="requires RUN_POSTGRES_INTEGRATION=1 and migrated local PostgreSQL",
)

DATABASE_URL = "postgresql://github_analytics:local_only_change_me@localhost:55432/github_analytics"


def test_duplicate_delivery_creates_one_durable_raw_row() -> None:
    delivery_id = f"storage-integration-{uuid4()}"
    envelope = GitHubEventEnvelope.from_webhook(
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
        },
    )
    event = WebhookRawEvent(
        envelope=envelope,
        kafka_partition=0,
        kafka_offset=1,
    )

    with ConnectionPool[Any](
        DATABASE_URL,
        min_size=1,
        max_size=1,
        open=True,
    ) as pool:
        storage = PostgresRawEventStorage(pool)
        assert storage.insert(event) is InsertOutcome.INSERTED
        assert storage.insert(event) is InsertOutcome.DUPLICATE
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
