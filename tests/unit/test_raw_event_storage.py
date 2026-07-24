"""Tests for the PostgreSQL raw-event idempotency boundary."""

from contextlib import AbstractContextManager
from datetime import UTC, datetime
from types import TracebackType
from typing import Any, cast
from uuid import UUID

import pytest
from psycopg_pool import ConnectionPool

from github_analytics.consumers.storage import (
    InsertOutcome,
    PostgresRawEventStorage,
    WebhookRawEvent,
)
from github_analytics.webhook.models import GitHubEventEnvelope, GitHubEventName


class FakeResult:
    def __init__(self, row: tuple[UUID] | None) -> None:
        self.row = row

    def fetchone(self) -> tuple[UUID] | None:
        return self.row


class FakeTransaction(AbstractContextManager[None]):
    def __init__(self, connection: "FakeConnection") -> None:
        self.connection = connection

    def __enter__(self) -> None:
        self.connection.transaction_entered = True

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exc_type, exc_value, traceback
        self.connection.transaction_exited = True


class FakeConnection:
    def __init__(self, row: tuple[UUID] | None) -> None:
        self.row = row
        self.transaction_entered = False
        self.transaction_exited = False
        self.statement: str | None = None
        self.parameters: dict[str, Any] | None = None

    def transaction(self) -> FakeTransaction:
        return FakeTransaction(self)

    def execute(self, statement: str, parameters: dict[str, Any]) -> FakeResult:
        assert self.transaction_entered
        self.statement = statement
        self.parameters = parameters
        return FakeResult(self.row)


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
        self._connection = connection

    def connection(self) -> FakeConnectionContext:
        return FakeConnectionContext(self._connection)


def _envelope() -> GitHubEventEnvelope:
    return GitHubEventEnvelope.from_webhook(
        delivery_id="real-delivery-identity",
        event_name=GitHubEventName.PULL_REQUEST,
        received_at=datetime(2026, 7, 24, 12, 30, tzinfo=UTC),
        payload={
            "action": "opened",
            "installation": {"id": 10001},
            "repository": {
                "id": 20001,
                "full_name": "example-org/delivery-demo",
            },
            "unknown_additive_field": {"retained": True},
        },
    )


@pytest.mark.parametrize(
    ("returned_row", "expected"),
    [
        ((UUID("11111111-1111-1111-1111-111111111111"),), InsertOutcome.INSERTED),
        (None, InsertOutcome.DUPLICATE),
    ],
)
def test_insert_uses_real_delivery_identity_and_reports_outcome(
    returned_row: tuple[UUID] | None,
    expected: InsertOutcome,
) -> None:
    connection = FakeConnection(returned_row)
    storage = PostgresRawEventStorage(
        cast(ConnectionPool[Any], FakePool(connection)),
    )
    event = WebhookRawEvent(
        envelope=_envelope(),
        kafka_partition=2,
        kafka_offset=41,
    )

    assert storage.insert(event) is expected

    assert connection.transaction_exited
    assert connection.statement is not None
    assert "ON CONFLICT (source, source_record_key) DO NOTHING" in connection.statement
    assert connection.parameters is not None
    assert connection.parameters["source_record_key"] == "real-delivery-identity"
    assert connection.parameters["delivery_id"] == "real-delivery-identity"
    assert connection.parameters["event_name"] == "pull_request"
    assert connection.parameters["repository_id"] == 20001
    assert connection.parameters["installation_id"] == 10001
    assert connection.parameters["occurred_at"] is None
    assert connection.parameters["received_at"] == datetime(
        2026,
        7,
        24,
        12,
        30,
        tzinfo=UTC,
    )
    assert connection.parameters["payload"].obj["unknown_additive_field"] == {"retained": True}
    assert connection.parameters["kafka_partition"] == 2
    assert connection.parameters["kafka_offset"] == 41


@pytest.mark.parametrize(
    ("partition", "offset", "message"),
    [
        (-1, 0, "partition"),
        (0, -1, "offset"),
    ],
)
def test_raw_event_rejects_negative_kafka_lineage(
    partition: int,
    offset: int,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        WebhookRawEvent(
            envelope=_envelope(),
            kafka_partition=partition,
            kafka_offset=offset,
        )
