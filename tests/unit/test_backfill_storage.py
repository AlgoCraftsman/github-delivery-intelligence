"""Tests for atomic backfill raw inserts and checkpoint advancement."""

from contextlib import AbstractContextManager
from datetime import UTC, datetime
from types import TracebackType
from typing import Any, cast

import pytest
from psycopg_pool import ConnectionPool

from github_analytics.backfill.models import (
    BackfillRecord,
    BackfillRunKey,
    CheckpointStatus,
)
from github_analytics.backfill.storage import PostgresBackfillStorage

START = datetime(2026, 1, 1, tzinfo=UTC)
END = datetime(2026, 2, 1, tzinfo=UTC)


class FakeResult:
    def __init__(self, row: tuple[Any, ...] | None) -> None:
        self.row = row

    def fetchone(self) -> tuple[Any, ...] | None:
        return self.row


class FakeTransaction(AbstractContextManager[None]):
    def __init__(self, connection: "FakeConnection") -> None:
        self.connection = connection

    def __enter__(self) -> None:
        self.connection.in_transaction = True

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exc_type, exc_value, traceback
        self.connection.in_transaction = False
        self.connection.transaction_exited = True


class FakeConnection:
    def __init__(
        self,
        *,
        checkpoint_load: tuple[Any, ...] | None = None,
        insert_rows: list[tuple[Any, ...] | None] | None = None,
        checkpoint_write: tuple[Any, ...] | None = (None, "completed", 1, 1),
    ) -> None:
        self.checkpoint_load = checkpoint_load
        self.insert_rows = insert_rows or []
        self.checkpoint_write = checkpoint_write
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.in_transaction = False
        self.transaction_exited = False

    def transaction(self) -> FakeTransaction:
        return FakeTransaction(self)

    def execute(self, statement: str, parameters: dict[str, Any]) -> FakeResult:
        self.calls.append((statement, parameters))
        if statement.lstrip().startswith("SELECT"):
            return FakeResult(self.checkpoint_load)
        assert self.in_transaction
        if "INSERT INTO raw.github_events" in statement:
            return FakeResult(self.insert_rows.pop(0))
        return FakeResult(self.checkpoint_write)


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


def _storage(connection: FakeConnection) -> PostgresBackfillStorage:
    return PostgresBackfillStorage(cast(ConnectionPool[Any], FakePool(connection)))


def _key() -> BackfillRunKey:
    return BackfillRunKey(10, "pull_requests", "repository", START, END)


def _record(key: str = "source-key", repository_id: int = 10) -> BackfillRecord:
    return BackfillRecord(
        source_record_key=key,
        event_name="pull_request",
        action="open",
        repository_id=repository_id,
        installation_id=20,
        occurred_at=START,
        payload={"unknown_additive_field": True},
    )


@pytest.mark.parametrize(
    ("row", "expected"),
    [
        (None, None),
        (
            ("cursor-1", "in_progress", 2, 3),
            ("cursor-1", CheckpointStatus.IN_PROGRESS, 2, 3),
        ),
    ],
)
def test_load_checkpoint_returns_restart_state(
    row: tuple[Any, ...] | None,
    expected: tuple[Any, ...] | None,
) -> None:
    connection = FakeConnection(checkpoint_load=row)

    checkpoint = _storage(connection).load_checkpoint(_key())

    if expected is None:
        assert checkpoint is None
    else:
        assert checkpoint is not None
        assert (
            checkpoint.cursor,
            checkpoint.status,
            checkpoint.pages_completed,
            checkpoint.records_inserted,
        ) == expected
    assert connection.calls[0][1]["scope"] == "repository"


def test_persist_page_absorbs_duplicates_and_advances_cursor_atomically() -> None:
    connection = FakeConnection(
        insert_rows=[("event-id",), None],
        checkpoint_write=("next", "in_progress", 3, 8),
    )

    outcome = _storage(connection).persist_page(
        _key(),
        [_record("one"), _record("two")],
        next_cursor="next",
        completed=False,
    )

    assert outcome.inserted == 1
    assert outcome.duplicates == 1
    assert outcome.checkpoint.pages_completed == 3
    assert connection.transaction_exited
    first_parameters = connection.calls[0][1]
    assert first_parameters["payload"].obj == {"unknown_additive_field": True}
    assert connection.calls[-1][1]["status"] == "in_progress"
    assert connection.calls[-1][1]["inserted"] == 1


def test_persist_completed_empty_page_clears_cursor() -> None:
    connection = FakeConnection(
        checkpoint_write=(None, "completed", 1, 0),
    )

    outcome = _storage(connection).persist_page(
        _key(),
        [],
        next_cursor=None,
        completed=True,
    )

    assert outcome.checkpoint.status is CheckpointStatus.COMPLETED
    assert outcome.inserted == outcome.duplicates == 0
    assert connection.calls[-1][1]["status"] == "completed"


@pytest.mark.parametrize(
    ("records", "next_cursor", "completed", "message"),
    [
        ([], "cursor", True, "cannot retain"),
        ([], None, False, "requires"),
        ([_record(repository_id=11)], None, True, "does not match"),
    ],
)
def test_persist_page_rejects_invalid_checkpoint_transition(
    records: list[BackfillRecord],
    next_cursor: str | None,
    completed: bool,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        _storage(FakeConnection()).persist_page(
            _key(),
            records,
            next_cursor=next_cursor,
            completed=completed,
        )


def test_persist_page_fails_closed_when_checkpoint_returning_is_empty() -> None:
    connection = FakeConnection(checkpoint_write=None)

    with pytest.raises(RuntimeError, match="no durable state"):
        _storage(connection).persist_page(
            _key(),
            [],
            next_cursor=None,
            completed=True,
        )
