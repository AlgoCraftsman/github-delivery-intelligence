"""Transactional PostgreSQL storage for analytics refresh run state."""

from datetime import datetime
from typing import Any, cast

from psycopg import sql
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

from github_analytics.analytics_refresh.models import (
    DbtResultSummary,
    RefreshRunIdentity,
    SourceWatermark,
)

_BEGIN_RUN = """
INSERT INTO ops.analytics_refresh_runs (
    run_id, dag_id, dag_run_id, logical_date, data_interval_start, data_interval_end,
    started_at, finished_at, status, source_relation, source_max_ingested_at,
    source_delay_seconds, source_freshness_summary, dbt_invocation_id,
    dbt_result_summary, dbt_succeeded_count, dbt_failed_count, dbt_skipped_count,
    dbt_warning_count, dbt_error_count, error_category, error_summary
) VALUES (
    %(run_id)s, %(dag_id)s, %(dag_run_id)s, %(logical_date)s,
    %(data_interval_start)s, %(data_interval_end)s, %(started_at)s, NULL, 'running',
    %(source_relation)s, NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, NULL
)
ON CONFLICT (dag_id, dag_run_id) DO UPDATE SET
    logical_date = EXCLUDED.logical_date,
    data_interval_start = EXCLUDED.data_interval_start,
    data_interval_end = EXCLUDED.data_interval_end,
    started_at = EXCLUDED.started_at,
    finished_at = NULL,
    status = 'running',
    source_relation = EXCLUDED.source_relation,
    source_max_ingested_at = NULL,
    source_delay_seconds = NULL,
    source_freshness_summary = NULL,
    dbt_invocation_id = NULL,
    dbt_result_summary = NULL,
    dbt_succeeded_count = 0,
    dbt_failed_count = 0,
    dbt_skipped_count = 0,
    dbt_warning_count = 0,
    dbt_error_count = 0,
    error_category = NULL,
    error_summary = NULL,
    updated_at = clock_timestamp()
WHERE ops.analytics_refresh_runs.status IN ('running', 'failed')
RETURNING run_id
"""

_RECORD_SOURCE = """
UPDATE ops.analytics_refresh_runs
SET source_freshness_summary = %(summary)s,
    dbt_invocation_id = COALESCE(%(invocation_id)s, dbt_invocation_id),
    updated_at = clock_timestamp()
WHERE run_id = %(run_id)s AND status = 'running'
RETURNING run_id
"""

_RECORD_WATERMARK = """
UPDATE ops.analytics_refresh_runs
SET source_max_ingested_at = %(maximum_ingested_at)s,
    source_delay_seconds = %(delay_seconds)s,
    updated_at = clock_timestamp()
WHERE run_id = %(run_id)s AND status = 'running'
RETURNING run_id
"""

_RECORD_BUILD = """
UPDATE ops.analytics_refresh_runs
SET dbt_invocation_id = %(invocation_id)s,
    dbt_result_summary = %(summary)s,
    dbt_succeeded_count = %(succeeded)s,
    dbt_failed_count = %(failed)s,
    dbt_skipped_count = %(skipped)s,
    dbt_warning_count = %(warnings)s,
    dbt_error_count = %(errors)s,
    updated_at = clock_timestamp()
WHERE run_id = %(run_id)s AND status = 'running'
RETURNING run_id
"""

_RESUME_BUILD = """
UPDATE ops.analytics_refresh_runs
SET status = 'running',
    finished_at = NULL,
    error_category = NULL,
    error_summary = NULL,
    updated_at = clock_timestamp()
WHERE run_id = %(run_id)s AND status IN ('running', 'failed')
RETURNING run_id
"""

_FINISH_SUCCESS = """
UPDATE ops.analytics_refresh_runs
SET status = 'succeeded', finished_at = %(finished_at)s, updated_at = clock_timestamp()
WHERE run_id = %(run_id)s AND status IN ('running', 'succeeded')
RETURNING run_id
"""

_FINISH_FAILURE = """
UPDATE ops.analytics_refresh_runs
SET status = 'failed',
    finished_at = GREATEST(%(finished_at)s, started_at),
    dbt_invocation_id = COALESCE(%(invocation_id)s, dbt_invocation_id),
    dbt_result_summary = COALESCE(%(summary)s, dbt_result_summary),
    dbt_succeeded_count = COALESCE(%(succeeded)s, dbt_succeeded_count),
    dbt_failed_count = COALESCE(%(failed)s, dbt_failed_count),
    dbt_skipped_count = COALESCE(%(skipped)s, dbt_skipped_count),
    dbt_warning_count = COALESCE(%(warnings)s, dbt_warning_count),
    dbt_error_count = COALESCE(%(errors)s, dbt_error_count),
    error_category = COALESCE(error_category, %(category)s),
    error_summary = COALESCE(error_summary, %(error_summary)s),
    updated_at = clock_timestamp()
WHERE run_id = %(run_id)s AND status IN ('running', 'failed')
RETURNING run_id
"""


class PostgresAnalyticsRefreshStorage:
    """Persist explicit, idempotent analytics refresh state transitions."""

    def __init__(self, pool: ConnectionPool[Any]) -> None:
        self._pool = pool

    def begin_run(
        self,
        identity: RefreshRunIdentity,
        *,
        started_at: datetime,
        source_relation: str,
    ) -> str:
        """Insert or restart one non-successful Airflow run ledger row."""

        row = self._execute_one(
            _BEGIN_RUN,
            {
                "run_id": identity.run_id,
                "dag_id": identity.dag_id,
                "dag_run_id": identity.dag_run_id,
                "logical_date": identity.logical_date,
                "data_interval_start": identity.data_interval_start,
                "data_interval_end": identity.data_interval_end,
                "started_at": started_at,
                "source_relation": source_relation,
            },
        )
        if row is None:
            raise ValueError("a succeeded analytics refresh run cannot be restarted")
        return str(row[0])

    def read_source_watermark(
        self,
        *,
        schema: str,
        identifier: str,
        observed_at: datetime,
    ) -> SourceWatermark:
        """Read the newest ingestion timestamp through identifier-safe SQL."""

        statement = sql.SQL("SELECT max(ingested_at) FROM {}.{}").format(
            sql.Identifier(schema), sql.Identifier(identifier)
        )
        with self._pool.connection() as connection:
            row = connection.execute(statement).fetchone()
        if row is None:
            raise RuntimeError("source watermark query returned no aggregate row")
        maximum = row[0]
        delay = None if maximum is None else max(0, int((observed_at - maximum).total_seconds()))
        return SourceWatermark(maximum, delay)

    def record_source(
        self,
        run_id: str,
        summary: DbtResultSummary,
    ) -> None:
        """Attach the bounded dbt freshness summary to a running row."""

        self._require_write(
            _RECORD_SOURCE,
            {
                "run_id": run_id,
                "summary": Jsonb(summary.artifact),
                "invocation_id": summary.invocation_id,
            },
        )

    def record_watermark(self, run_id: str, watermark: SourceWatermark) -> None:
        """Persist the source observation before invoking dbt freshness."""

        self._require_write(
            _RECORD_WATERMARK,
            {
                "run_id": run_id,
                "maximum_ingested_at": watermark.maximum_ingested_at,
                "delay_seconds": watermark.delay_seconds,
            },
        )

    def record_build(self, run_id: str, summary: DbtResultSummary) -> None:
        """Attach a bounded dbt build result to a running row."""

        parameters = _summary_parameters(summary)
        parameters["run_id"] = run_id
        self._require_write(_RECORD_BUILD, parameters)

    def resume_build(self, run_id: str) -> None:
        """Re-enter running for a dbt task retry while retaining source evidence."""

        self._require_write(_RESUME_BUILD, {"run_id": run_id})

    def finish_success(self, run_id: str, *, finished_at: datetime) -> None:
        """Persist a terminal successful state idempotently."""

        self._require_write(
            _FINISH_SUCCESS,
            {"run_id": run_id, "finished_at": finished_at},
        )

    def finish_failure(
        self,
        run_id: str,
        *,
        finished_at: datetime,
        category: str,
        error_summary: str,
        summary: DbtResultSummary | None = None,
    ) -> None:
        """Persist a terminal failed state without overwriting successful runs."""

        parameters = _summary_parameters(summary)
        parameters.update(
            {
                "run_id": run_id,
                "finished_at": finished_at,
                "category": category,
                "error_summary": error_summary,
            }
        )
        self._require_write(_FINISH_FAILURE, parameters)

    def _execute_one(self, statement: str, parameters: dict[str, Any]) -> tuple[Any, ...] | None:
        with (
            self._pool.connection() as connection,
            connection.transaction(),
        ):
            return cast(
                tuple[Any, ...] | None, connection.execute(statement, parameters).fetchone()
            )

    def _require_write(self, statement: str, parameters: dict[str, Any]) -> None:
        if self._execute_one(statement, parameters) is None:
            raise ValueError("analytics refresh state transition was rejected")


def _summary_parameters(summary: DbtResultSummary | None) -> dict[str, Any]:
    if summary is None:
        return {
            "invocation_id": None,
            "summary": None,
            "succeeded": None,
            "failed": None,
            "skipped": None,
            "warnings": None,
            "errors": None,
        }
    return {
        "invocation_id": summary.invocation_id,
        "summary": Jsonb(summary.artifact),
        "succeeded": summary.succeeded,
        "failed": summary.failed,
        "skipped": summary.skipped,
        "warnings": summary.warnings,
        "errors": summary.errors,
    }
