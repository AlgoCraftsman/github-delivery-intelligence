"""Opt-in PostgreSQL evidence for atomic backfill pages and duplicate replay."""

import os
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import pytest
from psycopg_pool import ConnectionPool

from github_analytics.backfill.models import BackfillRecord, BackfillRunKey
from github_analytics.backfill.storage import PostgresBackfillStorage

pytestmark = pytest.mark.skipif(
    os.getenv("RUN_POSTGRES_INTEGRATION") != "1",
    reason="requires RUN_POSTGRES_INTEGRATION=1 and migrated local PostgreSQL",
)

DATABASE_URL = "postgresql://github_analytics:local_only_change_me@localhost:55432/github_analytics"


def test_backfill_page_and_checkpoint_replay_are_durable_and_idempotent() -> None:
    unique = str(uuid4())
    window_start = datetime.now(UTC) - timedelta(days=1)
    window_end = datetime.now(UTC)
    key = BackfillRunKey(
        repository_id=20001,
        resource=f"integration_{unique}",
        scope="repository",
        window_start=window_start,
        window_end=window_end,
    )
    record = BackfillRecord(
        source_record_key=f"github_graphql:integration:{unique}",
        event_name="pull_request",
        action="open",
        repository_id=20001,
        installation_id=10001,
        occurred_at=window_start,
        payload={"id": unique, "synthetic": True},
    )

    with ConnectionPool[Any](
        DATABASE_URL,
        min_size=1,
        max_size=1,
        open=True,
    ) as pool:
        storage = PostgresBackfillStorage(pool)
        first = storage.persist_page(
            key,
            [record],
            next_cursor=None,
            completed=True,
        )
        replay = storage.persist_page(
            key,
            [record],
            next_cursor=None,
            completed=True,
        )
        checkpoint = storage.load_checkpoint(key)
        with pool.connection() as connection:
            count = connection.execute(
                """
                SELECT count(*)
                FROM raw.github_events
                WHERE source = 'backfill' AND source_record_key = %s
                """,
                (record.source_record_key,),
            ).fetchone()

    assert first.inserted == 1
    assert replay.duplicates == 1
    assert count == (1,)
    assert checkpoint is not None
    assert checkpoint.pages_completed == 2
    assert checkpoint.records_inserted == 1
