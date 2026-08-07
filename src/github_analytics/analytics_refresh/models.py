"""Typed identities, states, and bounded dbt summaries."""

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any


class RunStatus(StrEnum):
    """Durable analytics refresh states."""

    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


_ALLOWED_TRANSITIONS = {
    RunStatus.RUNNING: {RunStatus.RUNNING, RunStatus.SUCCEEDED, RunStatus.FAILED},
    RunStatus.FAILED: {RunStatus.RUNNING, RunStatus.FAILED},
    RunStatus.SUCCEEDED: {RunStatus.SUCCEEDED},
}


def validate_transition(current: RunStatus, target: RunStatus) -> None:
    """Reject state regressions while allowing retry and idempotent writes."""

    if target not in _ALLOWED_TRANSITIONS[current]:
        raise ValueError(f"invalid analytics refresh transition: {current} -> {target}")


@dataclass(frozen=True, slots=True)
class RefreshRunIdentity:
    """Stable orchestration identity and UTC-aware data interval."""

    dag_id: str
    dag_run_id: str
    logical_date: datetime
    data_interval_start: datetime
    data_interval_end: datetime

    def __post_init__(self) -> None:
        values = (self.logical_date, self.data_interval_start, self.data_interval_end)
        if not self.dag_id.strip() or not self.dag_run_id.strip():
            raise ValueError("DAG identity values must be nonempty")
        if any(value.tzinfo is None or value.utcoffset() is None for value in values):
            raise ValueError("refresh timestamps must be timezone-aware")
        if self.data_interval_start > self.data_interval_end:
            raise ValueError("data interval start cannot follow its end")

    @property
    def run_id(self) -> str:
        """Return a deterministic, collision-resistant application run identity."""

        import hashlib

        digest = hashlib.sha256(f"{self.dag_id}\0{self.dag_run_id}".encode()).hexdigest()
        return f"analytics-refresh-{digest}"


@dataclass(frozen=True, slots=True)
class SourceWatermark:
    """Newest raw ingestion timestamp and its age at refresh start."""

    maximum_ingested_at: datetime | None
    delay_seconds: int | None

    def __post_init__(self) -> None:
        if self.delay_seconds is not None and self.delay_seconds < 0:
            raise ValueError("source delay cannot be negative")


@dataclass(frozen=True, slots=True)
class DbtResultSummary:
    """Sanitized bounded dbt artifact summary safe for the ops ledger."""

    invocation_id: str | None
    succeeded: int = 0
    failed: int = 0
    skipped: int = 0
    warnings: int = 0
    errors: int = 0
    artifact: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if min(self.succeeded, self.failed, self.skipped, self.warnings, self.errors) < 0:
            raise ValueError("dbt result counts cannot be negative")


class RefreshError(RuntimeError):
    """A categorized refresh error carrying an optional safe artifact summary."""

    def __init__(
        self,
        category: str,
        message: str,
        *,
        summary: DbtResultSummary | None = None,
    ) -> None:
        super().__init__(message)
        self.category = category
        self.summary = summary
