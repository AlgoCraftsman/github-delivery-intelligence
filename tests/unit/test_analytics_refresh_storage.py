"""Tests for transactional analytics refresh ledger transitions."""

from contextlib import AbstractContextManager
from datetime import UTC, datetime, timedelta
from types import TracebackType
from typing import Any, cast

import pytest
from psycopg_pool import ConnectionPool

from github_analytics.analytics_refresh.models import (
    DbtResultSummary,
    RefreshRunIdentity,
    SourceWatermark,
)
from github_analytics.analytics_refresh.storage import PostgresAnalyticsRefreshStorage

NOW = datetime(2026, 8, 2, 12, tzinfo=UTC)


class FakeResult:
    def __init__(self, row: tuple[Any, ...] | None) -> None:
        self.row = row

    def fetchone(self) -> tuple[Any, ...] | None:
        return self.row


class FakeTransaction(AbstractContextManager[None]):
    def __enter__(self) -> None:
        return None

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exc_type, exc_value, traceback


class FakeConnection:
    def __init__(self, rows: list[tuple[Any, ...] | None]) -> None:
        self.rows = rows
        self.calls: list[tuple[Any, dict[str, Any] | None]] = []

    def transaction(self) -> FakeTransaction:
        return FakeTransaction()

    def execute(self, statement: Any, parameters: dict[str, Any] | None = None) -> FakeResult:
        self.calls.append((statement, parameters))
        return FakeResult(self.rows.pop(0))


class FakeConnectionContext(AbstractContextManager[FakeConnection]):
    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection

    def __enter__(self) -> FakeConnection:
        return self.connection

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exc_type, exc_value, traceback


class FakePool:
    def __init__(self, connection: FakeConnection) -> None:
        self.connection_object = connection

    def connection(self) -> FakeConnectionContext:
        return FakeConnectionContext(self.connection_object)


def _storage(connection: FakeConnection) -> PostgresAnalyticsRefreshStorage:
    return PostgresAnalyticsRefreshStorage(cast(ConnectionPool[Any], FakePool(connection)))


def _identity() -> RefreshRunIdentity:
    return RefreshRunIdentity(
        "analytics_refresh", "scheduled__one", NOW, NOW, NOW + timedelta(hours=1)
    )


def test_begin_run_returns_stable_id_and_rejects_succeeded_restart() -> None:
    connection = FakeConnection([(_identity().run_id,), None])
    storage = _storage(connection)

    assert (
        storage.begin_run(_identity(), started_at=NOW, source_relation="raw.github_events")
        == _identity().run_id
    )
    parameters = connection.calls[0][1]
    assert parameters is not None
    assert parameters["dag_run_id"] == "scheduled__one"

    with pytest.raises(ValueError, match="cannot be restarted"):
        storage.begin_run(_identity(), started_at=NOW, source_relation="raw.github_events")


def test_watermark_query_handles_empty_current_and_future_sources() -> None:
    connection = FakeConnection(
        [(None,), (NOW - timedelta(seconds=61),), (NOW + timedelta(seconds=1),)]
    )
    storage = _storage(connection)

    assert storage.read_source_watermark(
        schema="raw", identifier="events", observed_at=NOW
    ) == SourceWatermark(None, None)
    assert (
        storage.read_source_watermark(
            schema="raw", identifier="events", observed_at=NOW
        ).delay_seconds
        == 61
    )
    assert (
        storage.read_source_watermark(
            schema="raw", identifier="events", observed_at=NOW
        ).delay_seconds
        == 0
    )
    assert connection.calls[0][1] is None


def test_watermark_query_requires_an_aggregate_row() -> None:
    with pytest.raises(RuntimeError, match="no aggregate row"):
        _storage(FakeConnection([None])).read_source_watermark(
            schema="raw", identifier="events", observed_at=NOW
        )


def test_running_updates_and_terminal_transitions_write_safe_parameters() -> None:
    connection = FakeConnection([("run",)] * 7)
    storage = _storage(connection)
    watermark = SourceWatermark(NOW, 0)
    summary = DbtResultSummary(
        "inv",
        succeeded=2,
        failed=1,
        skipped=3,
        warnings=4,
        errors=5,
        artifact={"safe": True},
    )

    storage.record_watermark("run", watermark)
    storage.record_source("run", summary)
    storage.resume_build("run")
    storage.record_build("run", summary)
    storage.finish_success("run", finished_at=NOW)
    storage.finish_failure(
        "run",
        finished_at=NOW,
        category="dbt_build_failed",
        error_summary="safe",
        summary=summary,
    )
    storage.finish_failure(
        "run",
        finished_at=NOW,
        category="unexpected_error",
        error_summary="safe",
    )

    build_parameters = connection.calls[3][1]
    assert build_parameters is not None
    assert build_parameters["succeeded"] == 2
    assert build_parameters["summary"].obj == {"safe": True}
    no_summary_parameters = connection.calls[-1][1]
    assert no_summary_parameters is not None
    assert no_summary_parameters["summary"] is None


def test_running_update_rejects_invalid_state() -> None:
    storage = _storage(FakeConnection([None]))

    with pytest.raises(ValueError, match="transition was rejected"):
        storage.record_watermark("missing", SourceWatermark(None, None))
