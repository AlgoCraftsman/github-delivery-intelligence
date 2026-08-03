"""Opt-in PostgreSQL evidence for the idempotent analytics refresh ledger."""

import os
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import pytest
from psycopg_pool import ConnectionPool

from github_analytics.analytics_refresh.models import (
    DbtResultSummary,
    RefreshRunIdentity,
    SourceWatermark,
)
from github_analytics.analytics_refresh.storage import PostgresAnalyticsRefreshStorage

pytestmark = pytest.mark.skipif(
    os.getenv("RUN_POSTGRES_INTEGRATION") != "1",
    reason="requires RUN_POSTGRES_INTEGRATION=1 and migrated local PostgreSQL",
)

DATABASE_URL = "postgresql://github_analytics:local_only_change_me@localhost:55432/github_analytics"


def test_success_and_failure_rows_are_unique_and_terminal() -> None:
    now = datetime.now(UTC)
    unique = str(uuid4())
    success = RefreshRunIdentity(
        "analytics_refresh", f"integration-success-{unique}", now, now, now + timedelta(hours=1)
    )
    failure = RefreshRunIdentity(
        "analytics_refresh", f"integration-failure-{unique}", now, now, now + timedelta(hours=1)
    )
    summary = DbtResultSummary("integration-invocation", succeeded=2, artifact={"safe": True})

    with ConnectionPool[Any](DATABASE_URL, min_size=1, max_size=1, open=True) as pool:
        storage = PostgresAnalyticsRefreshStorage(pool)
        storage.begin_run(success, started_at=now, source_relation="raw.github_events_fixture")
        storage.begin_run(success, started_at=now, source_relation="raw.github_events_fixture")
        storage.record_watermark(success.run_id, SourceWatermark(now, 0))
        storage.record_source(success.run_id, summary)
        storage.resume_build(success.run_id)
        storage.record_build(success.run_id, summary)
        storage.finish_success(success.run_id, finished_at=now + timedelta(seconds=1))

        storage.begin_run(failure, started_at=now, source_relation="raw.github_events_fixture")
        storage.finish_failure(
            failure.run_id,
            finished_at=now + timedelta(seconds=1),
            category="simulated_failure",
            error_summary="bounded deterministic failure",
        )
        with pool.connection() as connection:
            rows = connection.execute(
                """
                SELECT dag_run_id, status, finished_at IS NOT NULL, dbt_succeeded_count
                FROM ops.analytics_refresh_runs
                WHERE dag_run_id IN (%s, %s)
                ORDER BY dag_run_id
                """,
                (success.dag_run_id, failure.dag_run_id),
            ).fetchall()

    assert len(rows) == 2
    assert {row[1] for row in rows} == {"succeeded", "failed"}
    assert all(row[2] for row in rows)
    assert sum(row[3] for row in rows) == 2
