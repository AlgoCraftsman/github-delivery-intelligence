"""Durable pull-request projection and idempotent stale-alert outbox."""

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any

from psycopg_pool import ConnectionPool


class PullRequestState(StrEnum):
    """Source states relevant to the open-pull-request projection."""

    OPEN = "open"
    CLOSED = "closed"


class ProjectionOutcome(StrEnum):
    """Result of applying a pull-request source snapshot."""

    APPLIED = "applied"
    STALE = "stale"


class ReviewOutcome(StrEnum):
    """Result of considering one submitted review."""

    RECORDED = "recorded"
    INELIGIBLE = "ineligible"
    UNCHANGED = "unchanged"


@dataclass(frozen=True, slots=True)
class PullRequestSnapshot:
    """Complete source snapshot required by the PR monitor."""

    delivery_id: str
    repository_id: int
    repository_full_name: str
    pull_request_id: int
    pull_request_number: int
    state: PullRequestState
    title: str
    author_id: int
    author_login: str
    is_draft: bool
    opened_at: datetime
    updated_at: datetime

    def __post_init__(self) -> None:
        numeric_fields = (
            self.repository_id,
            self.pull_request_id,
            self.pull_request_number,
            self.author_id,
        )
        if any(value <= 0 for value in numeric_fields):
            raise ValueError("pull-request identities must be positive")
        text_fields = (
            self.delivery_id,
            self.repository_full_name,
            self.title,
            self.author_login,
        )
        if any(not value.strip() for value in text_fields):
            raise ValueError("pull-request text fields must be nonempty")
        if self.opened_at.tzinfo is None or self.updated_at.tzinfo is None:
            raise ValueError("pull-request timestamps must be timezone-aware")


@dataclass(frozen=True, slots=True)
class SubmittedReview:
    """Submitted review paired with the PR snapshot carried by its webhook."""

    pull_request: PullRequestSnapshot
    review_id: int
    reviewer_id: int
    reviewer_login: str
    submitted_at: datetime

    def __post_init__(self) -> None:
        if self.review_id <= 0 or self.reviewer_id <= 0:
            raise ValueError("review identities must be positive")
        if not self.reviewer_login.strip():
            raise ValueError("reviewer login must be nonempty")
        if self.submitted_at.tzinfo is None:
            raise ValueError("review timestamp must be timezone-aware")


_ADVANCE_WATERMARK = """
INSERT INTO serving.pull_request_projection_watermarks (
    repository_id,
    pull_request_id,
    last_event_at,
    last_delivery_id
) VALUES (
    %(repository_id)s,
    %(pull_request_id)s,
    %(updated_at)s,
    %(delivery_id)s
)
ON CONFLICT (repository_id, pull_request_id) DO UPDATE SET
    last_event_at = EXCLUDED.last_event_at,
    last_delivery_id = EXCLUDED.last_delivery_id,
    updated_at = clock_timestamp()
WHERE EXCLUDED.last_event_at
    > serving.pull_request_projection_watermarks.last_event_at
RETURNING last_event_at
"""

_UPSERT_OPEN_PULL_REQUEST = """
INSERT INTO serving.open_pull_requests (
    repository_id,
    pull_request_id,
    pull_request_number,
    repository_full_name,
    title,
    author_id,
    author_login,
    is_draft,
    opened_at,
    last_source_updated_at
) VALUES (
    %(repository_id)s,
    %(pull_request_id)s,
    %(pull_request_number)s,
    %(repository_full_name)s,
    %(title)s,
    %(author_id)s,
    %(author_login)s,
    %(is_draft)s,
    %(opened_at)s,
    %(updated_at)s
)
ON CONFLICT (repository_id, pull_request_id) DO UPDATE SET
    pull_request_number = EXCLUDED.pull_request_number,
    repository_full_name = EXCLUDED.repository_full_name,
    title = EXCLUDED.title,
    author_id = EXCLUDED.author_id,
    author_login = EXCLUDED.author_login,
    is_draft = EXCLUDED.is_draft,
    opened_at = EXCLUDED.opened_at,
    last_source_updated_at = EXCLUDED.last_source_updated_at,
    projected_at = clock_timestamp()
"""

_DELETE_CLOSED_PULL_REQUEST = """
DELETE FROM serving.open_pull_requests
WHERE repository_id = %(repository_id)s
  AND pull_request_id = %(pull_request_id)s
"""

_CANCEL_PENDING_ALERT = """
UPDATE ops.alert_outbox
SET status = 'cancelled'
WHERE alert_key = %(alert_key)s
  AND status = 'pending'
"""

_RECORD_FIRST_ELIGIBLE_REVIEW = """
UPDATE serving.open_pull_requests
SET
    first_eligible_review_at = %(submitted_at)s,
    first_eligible_review_id = %(review_id)s,
    first_eligible_reviewer_id = %(reviewer_id)s,
    first_eligible_reviewer_login = %(reviewer_login)s,
    projected_at = clock_timestamp()
WHERE repository_id = %(repository_id)s
  AND pull_request_id = %(pull_request_id)s
  AND (
      first_eligible_review_at IS NULL
      OR %(submitted_at)s < first_eligible_review_at
  )
RETURNING first_eligible_review_id
"""

_INSERT_STALE_ALERTS = """
INSERT INTO ops.alert_outbox (
    alert_key,
    alert_type,
    repository_id,
    pull_request_id,
    pull_request_number,
    created_at,
    payload
)
SELECT
    'stale-pull-request:' || repository_id || ':' || pull_request_id,
    'stale_pull_request',
    repository_id,
    pull_request_id,
    pull_request_number,
    %(created_at)s,
    jsonb_build_object(
        'repository_id', repository_id,
        'repository_full_name', repository_full_name,
        'pull_request_id', pull_request_id,
        'pull_request_number', pull_request_number,
        'title', title,
        'author_login', author_login,
        'opened_at', opened_at,
        'stale_cutoff', %(stale_cutoff)s
    )
FROM serving.open_pull_requests
WHERE opened_at <= %(stale_cutoff)s
  AND first_eligible_review_at IS NULL
  AND NOT is_draft
ON CONFLICT (alert_key) DO NOTHING
RETURNING alert_id
"""


class PostgresPullRequestStorage:
    """Apply PR snapshots and reviews in PostgreSQL transactions."""

    def __init__(self, pool: ConnectionPool[Any]) -> None:
        self._pool = pool

    def apply_pull_request(self, snapshot: PullRequestSnapshot) -> ProjectionOutcome:
        """Apply a newer source snapshot without allowing delayed state regression."""

        parameters = _snapshot_parameters(snapshot)
        with (
            self._pool.connection() as connection,
            connection.transaction(),
        ):
            advanced = connection.execute(_ADVANCE_WATERMARK, parameters).fetchone()
            if advanced is None:
                return ProjectionOutcome.STALE
            _apply_snapshot_state(connection, snapshot, parameters)
        return ProjectionOutcome.APPLIED

    def apply_review(self, review: SubmittedReview) -> ReviewOutcome:
        """Project its PR snapshot and retain only the first non-author review."""

        snapshot = review.pull_request
        parameters = _snapshot_parameters(snapshot)
        review_parameters = {
            **parameters,
            "review_id": review.review_id,
            "reviewer_id": review.reviewer_id,
            "reviewer_login": review.reviewer_login,
            "submitted_at": review.submitted_at,
        }
        with (
            self._pool.connection() as connection,
            connection.transaction(),
        ):
            advanced = connection.execute(_ADVANCE_WATERMARK, parameters).fetchone()
            if advanced is not None:
                _apply_snapshot_state(connection, snapshot, parameters)
            if review.reviewer_id == snapshot.author_id:
                return ReviewOutcome.INELIGIBLE
            recorded = connection.execute(
                _RECORD_FIRST_ELIGIBLE_REVIEW,
                review_parameters,
            ).fetchone()
        if recorded is None:
            return ReviewOutcome.UNCHANGED
        return ReviewOutcome.RECORDED

    def create_stale_alerts(
        self,
        *,
        stale_cutoff: datetime,
        created_at: datetime,
    ) -> int:
        """Create at most one durable alert intent for every eligible stale PR."""

        if stale_cutoff.tzinfo is None or created_at.tzinfo is None:
            raise ValueError("stale-alert timestamps must be timezone-aware")
        with (
            self._pool.connection() as connection,
            connection.transaction(),
        ):
            inserted = connection.execute(
                _INSERT_STALE_ALERTS,
                {
                    "stale_cutoff": stale_cutoff,
                    "created_at": created_at,
                },
            ).fetchall()
        return len(inserted)


def _snapshot_parameters(snapshot: PullRequestSnapshot) -> dict[str, Any]:
    return {
        "delivery_id": snapshot.delivery_id,
        "repository_id": snapshot.repository_id,
        "repository_full_name": snapshot.repository_full_name,
        "pull_request_id": snapshot.pull_request_id,
        "pull_request_number": snapshot.pull_request_number,
        "title": snapshot.title,
        "author_id": snapshot.author_id,
        "author_login": snapshot.author_login,
        "is_draft": snapshot.is_draft,
        "opened_at": snapshot.opened_at,
        "updated_at": snapshot.updated_at,
        "alert_key": (f"stale-pull-request:{snapshot.repository_id}:{snapshot.pull_request_id}"),
    }


def _apply_snapshot_state(
    connection: Any,
    snapshot: PullRequestSnapshot,
    parameters: dict[str, Any],
) -> None:
    if snapshot.state is PullRequestState.OPEN:
        connection.execute(_UPSERT_OPEN_PULL_REQUEST, parameters)
        return
    connection.execute(_DELETE_CLOSED_PULL_REQUEST, parameters)
    connection.execute(_CANCEL_PENDING_ALERT, parameters)
