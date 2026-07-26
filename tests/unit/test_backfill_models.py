"""Tests for backfill identity and checkpoint value objects."""

from datetime import UTC, datetime

import pytest

from github_analytics.backfill.models import BackfillRecord, BackfillRunKey

START = datetime(2026, 1, 1, tzinfo=UTC)
END = datetime(2026, 2, 1, tzinfo=UTC)


def test_run_key_accepts_a_bounded_aware_window() -> None:
    key = BackfillRunKey(1, "pull_requests", "repository", START, END)

    assert key.repository_id == 1


@pytest.mark.parametrize(
    ("arguments", "message"),
    [
        ((0, "pull_requests", "repository", START, END), "repository_id"),
        ((1, "", "repository", START, END), "resource"),
        ((1, "pull_requests", " ", START, END), "resource"),
        ((1, "pull_requests", "repository", START.replace(tzinfo=None), END), "timezone"),
        ((1, "pull_requests", "repository", START, END.replace(tzinfo=None)), "timezone"),
        ((1, "pull_requests", "repository", END, START), "precede"),
    ],
)
def test_run_key_rejects_invalid_identity_or_window(
    arguments: tuple[object, ...],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        BackfillRunKey(*arguments)  # type: ignore[arg-type]


def test_backfill_record_accepts_real_source_identity() -> None:
    record = BackfillRecord(
        source_record_key="github_graphql:pull_request:PR_1:2026-01-01T00:00:00+00:00",
        event_name="pull_request",
        action="open",
        repository_id=10,
        installation_id=20,
        occurred_at=START,
        payload={"id": "PR_1"},
    )

    assert record.payload == {"id": "PR_1"}


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"repository_id": 0}, "positive"),
        ({"installation_id": 0}, "positive"),
        ({"source_record_key": ""}, "nonempty"),
        ({"event_name": " "}, "nonempty"),
        ({"action": ""}, "nonempty"),
        ({"occurred_at": START.replace(tzinfo=None)}, "timezone"),
    ],
)
def test_backfill_record_rejects_invalid_fields(
    changes: dict[str, object],
    message: str,
) -> None:
    arguments: dict[str, object] = {
        "source_record_key": "key",
        "event_name": "pull_request",
        "action": "open",
        "repository_id": 10,
        "installation_id": 20,
        "occurred_at": None,
        "payload": {},
    }
    arguments.update(changes)

    with pytest.raises(ValueError, match=message):
        BackfillRecord(**arguments)  # type: ignore[arg-type]
