"""Stable identities and restart state shared by backfill adapters."""

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any


class CheckpointStatus(StrEnum):
    """Lifecycle of one bounded resource traversal."""

    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"


@dataclass(frozen=True, slots=True)
class BackfillRunKey:
    """Identity of one repository resource traversal and time window."""

    repository_id: int
    resource: str
    scope: str
    window_start: datetime
    window_end: datetime

    def __post_init__(self) -> None:
        if self.repository_id <= 0:
            raise ValueError("repository_id must be positive")
        if not self.resource.strip() or not self.scope.strip():
            raise ValueError("resource and scope must be nonempty")
        if self.window_start.tzinfo is None or self.window_end.tzinfo is None:
            raise ValueError("backfill window timestamps must be timezone-aware")
        if self.window_start >= self.window_end:
            raise ValueError("backfill window_start must precede window_end")


@dataclass(frozen=True, slots=True)
class BackfillCheckpoint:
    """Durable cursor and progress counters for one run key."""

    cursor: str | None
    status: CheckpointStatus
    pages_completed: int
    records_inserted: int


@dataclass(frozen=True, slots=True)
class BackfillRecord:
    """One append-only GitHub resource snapshot ready for raw storage."""

    source_record_key: str
    event_name: str
    action: str
    repository_id: int
    installation_id: int
    occurred_at: datetime | None
    payload: dict[str, Any]

    def __post_init__(self) -> None:
        if self.repository_id <= 0 or self.installation_id <= 0:
            raise ValueError("GitHub repository and installation identities must be positive")
        if any(
            not value.strip() for value in (self.source_record_key, self.event_name, self.action)
        ):
            raise ValueError("backfill record identity and routing fields must be nonempty")
        if self.occurred_at is not None and self.occurred_at.tzinfo is None:
            raise ValueError("occurred_at must be timezone-aware when supplied")


@dataclass(frozen=True, slots=True)
class PageWriteOutcome:
    """Durable result of inserting one API page and advancing its cursor."""

    inserted: int
    duplicates: int
    checkpoint: BackfillCheckpoint
