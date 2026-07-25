"""Tests for the monotonic PR projection and idempotent alert boundary."""

from contextlib import AbstractContextManager
from datetime import UTC, datetime
from types import TracebackType
from typing import Any, cast

import pytest
from psycopg_pool import ConnectionPool

from github_analytics.consumers.pr_storage import (
    PostgresPullRequestStorage,
    ProjectionOutcome,
    PullRequestSnapshot,
    PullRequestState,
    ReviewOutcome,
    SubmittedReview,
)

NOW = datetime(2026, 7, 25, 12, tzinfo=UTC)


class FakeResult:
    def __init__(
        self,
        *,
        row: tuple[Any, ...] | None = None,
        rows: list[tuple[Any, ...]] | None = None,
    ) -> None:
        self.row = row
        self.rows = rows or []

    def fetchone(self) -> tuple[Any, ...] | None:
        return self.row

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self.rows


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
    def __init__(self, results: list[FakeResult]) -> None:
        self.results = results
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def transaction(self) -> FakeTransaction:
        return FakeTransaction()

    def execute(self, statement: str, parameters: dict[str, Any]) -> FakeResult:
        self.calls.append((statement, parameters))
        if self.results:
            return self.results.pop(0)
        return FakeResult()


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
        self.connection_value = connection

    def connection(self) -> FakeConnectionContext:
        return FakeConnectionContext(self.connection_value)


def _snapshot(
    *,
    state: PullRequestState = PullRequestState.OPEN,
    author_id: int = 40001,
) -> PullRequestSnapshot:
    return PullRequestSnapshot(
        delivery_id="delivery-pr-17",
        repository_id=20001,
        repository_full_name="example-org/delivery-demo",
        pull_request_id=30001,
        pull_request_number=17,
        state=state,
        title="Add deterministic PR monitoring",
        author_id=author_id,
        author_login="example-author",
        is_draft=False,
        opened_at=datetime(2026, 7, 24, 12, tzinfo=UTC),
        updated_at=NOW,
    )


def _storage(
    results: list[FakeResult],
) -> tuple[PostgresPullRequestStorage, FakeConnection]:
    connection = FakeConnection(results)
    storage = PostgresPullRequestStorage(
        cast(ConnectionPool[Any], FakePool(connection)),
    )
    return storage, connection


def test_apply_open_pull_request_advances_watermark_and_upserts_projection() -> None:
    storage, connection = _storage([FakeResult(row=(NOW,))])

    assert storage.apply_pull_request(_snapshot()) is ProjectionOutcome.APPLIED

    assert len(connection.calls) == 2
    assert "pull_request_projection_watermarks" in connection.calls[0][0]
    assert "INSERT INTO serving.open_pull_requests" in connection.calls[1][0]
    assert connection.calls[0][1]["delivery_id"] == "delivery-pr-17"
    assert connection.calls[1][1]["pull_request_number"] == 17


def test_apply_closed_pull_request_removes_projection_and_cancels_pending_alert() -> None:
    storage, connection = _storage([FakeResult(row=(NOW,))])

    assert (
        storage.apply_pull_request(_snapshot(state=PullRequestState.CLOSED))
        is ProjectionOutcome.APPLIED
    )

    assert len(connection.calls) == 3
    assert "DELETE FROM serving.open_pull_requests" in connection.calls[1][0]
    assert "UPDATE ops.alert_outbox" in connection.calls[2][0]
    assert connection.calls[2][1]["alert_key"] == "stale-pull-request:20001:30001"


def test_stale_pull_request_snapshot_does_not_change_projection() -> None:
    storage, connection = _storage([FakeResult(row=None)])

    assert storage.apply_pull_request(_snapshot()) is ProjectionOutcome.STALE
    assert len(connection.calls) == 1


def test_author_review_is_ineligible_after_projecting_newer_snapshot() -> None:
    snapshot = _snapshot(author_id=40001)
    review = SubmittedReview(
        pull_request=snapshot,
        review_id=50001,
        reviewer_id=40001,
        reviewer_login="example-author",
        submitted_at=NOW,
    )
    storage, connection = _storage([FakeResult(row=(NOW,))])

    assert storage.apply_review(review) is ReviewOutcome.INELIGIBLE
    assert len(connection.calls) == 2
    assert "INSERT INTO serving.open_pull_requests" in connection.calls[1][0]


def test_earliest_non_author_review_is_recorded_even_for_stale_pr_snapshot() -> None:
    review = SubmittedReview(
        pull_request=_snapshot(),
        review_id=50001,
        reviewer_id=40002,
        reviewer_login="example-reviewer",
        submitted_at=NOW,
    )
    storage, connection = _storage(
        [
            FakeResult(row=None),
            FakeResult(row=(50001,)),
        ]
    )

    assert storage.apply_review(review) is ReviewOutcome.RECORDED
    assert len(connection.calls) == 2
    assert "first_eligible_review_at" in connection.calls[1][0]
    assert connection.calls[1][1]["reviewer_id"] == 40002


def test_later_or_replayed_non_author_review_leaves_first_review_unchanged() -> None:
    review = SubmittedReview(
        pull_request=_snapshot(),
        review_id=50002,
        reviewer_id=40003,
        reviewer_login="later-reviewer",
        submitted_at=NOW,
    )
    storage, _ = _storage([FakeResult(row=None), FakeResult(row=None)])

    assert storage.apply_review(review) is ReviewOutcome.UNCHANGED


def test_review_with_closed_newer_snapshot_removes_open_projection() -> None:
    review = SubmittedReview(
        pull_request=_snapshot(state=PullRequestState.CLOSED),
        review_id=50001,
        reviewer_id=40002,
        reviewer_login="example-reviewer",
        submitted_at=NOW,
    )
    storage, connection = _storage(
        [
            FakeResult(row=(NOW,)),
            FakeResult(),
            FakeResult(),
            FakeResult(row=None),
        ]
    )

    assert storage.apply_review(review) is ReviewOutcome.UNCHANGED
    assert "DELETE FROM serving.open_pull_requests" in connection.calls[1][0]
    assert "UPDATE ops.alert_outbox" in connection.calls[2][0]


def test_stale_sweep_returns_new_alert_count_and_uses_supplied_times() -> None:
    storage, connection = _storage(
        [
            FakeResult(
                rows=[
                    ("11111111-1111-1111-1111-111111111111",),
                    ("22222222-2222-2222-2222-222222222222",),
                ]
            )
        ]
    )
    cutoff = datetime(2026, 7, 24, 12, tzinfo=UTC)

    assert storage.create_stale_alerts(stale_cutoff=cutoff, created_at=NOW) == 2
    assert "ON CONFLICT (alert_key) DO NOTHING" in connection.calls[0][0]
    assert connection.calls[0][1] == {
        "stale_cutoff": cutoff,
        "created_at": NOW,
    }


@pytest.mark.parametrize(
    ("stale_cutoff", "created_at"),
    [
        (datetime(2026, 7, 24, 12), NOW),
        (NOW, datetime(2026, 7, 25, 12)),
    ],
)
def test_stale_sweep_rejects_naive_timestamps(
    stale_cutoff: datetime,
    created_at: datetime,
) -> None:
    storage, _ = _storage([])

    with pytest.raises(ValueError, match="timezone-aware"):
        storage.create_stale_alerts(
            stale_cutoff=stale_cutoff,
            created_at=created_at,
        )


@pytest.mark.parametrize(
    "updates",
    [
        {"repository_id": 0},
        {"pull_request_id": 0},
        {"pull_request_number": 0},
        {"author_id": 0},
    ],
)
def test_pull_request_snapshot_rejects_nonpositive_identity(
    updates: dict[str, int],
) -> None:
    values = {
        "delivery_id": "delivery-pr-17",
        "repository_id": 20001,
        "repository_full_name": "example-org/delivery-demo",
        "pull_request_id": 30001,
        "pull_request_number": 17,
        "state": PullRequestState.OPEN,
        "title": "Projection",
        "author_id": 40001,
        "author_login": "author",
        "is_draft": False,
        "opened_at": NOW,
        "updated_at": NOW,
        **updates,
    }
    with pytest.raises(ValueError, match="identities"):
        PullRequestSnapshot(**values)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "updates",
    [
        {"delivery_id": " "},
        {"repository_full_name": ""},
        {"title": "\t"},
        {"author_login": ""},
    ],
)
def test_pull_request_snapshot_rejects_empty_text(
    updates: dict[str, str],
) -> None:
    values = {
        "delivery_id": "delivery-pr-17",
        "repository_id": 20001,
        "repository_full_name": "example-org/delivery-demo",
        "pull_request_id": 30001,
        "pull_request_number": 17,
        "state": PullRequestState.OPEN,
        "title": "Projection",
        "author_id": 40001,
        "author_login": "author",
        "is_draft": False,
        "opened_at": NOW,
        "updated_at": NOW,
        **updates,
    }
    with pytest.raises(ValueError, match="text fields"):
        PullRequestSnapshot(**values)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "updates",
    [
        {"opened_at": datetime(2026, 7, 25, 12)},
        {"updated_at": datetime(2026, 7, 25, 12)},
    ],
)
def test_pull_request_snapshot_rejects_naive_timestamp(
    updates: dict[str, datetime],
) -> None:
    values = {
        "delivery_id": "delivery-pr-17",
        "repository_id": 20001,
        "repository_full_name": "example-org/delivery-demo",
        "pull_request_id": 30001,
        "pull_request_number": 17,
        "state": PullRequestState.OPEN,
        "title": "Projection",
        "author_id": 40001,
        "author_login": "author",
        "is_draft": False,
        "opened_at": NOW,
        "updated_at": NOW,
        **updates,
    }
    with pytest.raises(ValueError, match="timestamps"):
        PullRequestSnapshot(**values)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("review_id", "reviewer_id"),
    [(0, 40002), (50001, 0)],
)
def test_submitted_review_rejects_nonpositive_identity(
    review_id: int,
    reviewer_id: int,
) -> None:
    with pytest.raises(ValueError, match="review identities"):
        SubmittedReview(
            pull_request=_snapshot(),
            review_id=review_id,
            reviewer_id=reviewer_id,
            reviewer_login="reviewer",
            submitted_at=NOW,
        )


def test_submitted_review_rejects_empty_login() -> None:
    with pytest.raises(ValueError, match="reviewer login"):
        SubmittedReview(
            pull_request=_snapshot(),
            review_id=50001,
            reviewer_id=40002,
            reviewer_login=" ",
            submitted_at=NOW,
        )


def test_submitted_review_rejects_naive_timestamp() -> None:
    with pytest.raises(ValueError, match="review timestamp"):
        SubmittedReview(
            pull_request=_snapshot(),
            review_id=50001,
            reviewer_id=40002,
            reviewer_login="reviewer",
            submitted_at=datetime(2026, 7, 25, 12),
        )
