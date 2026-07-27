"""Tests for restartable workflow-run and deployment REST adapters."""

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any

import pytest

from github_analytics.backfill.github import BackfillSummary
from github_analytics.backfill.models import (
    BackfillCheckpoint,
    BackfillRecord,
    BackfillRunKey,
    CheckpointStatus,
    PageWriteOutcome,
)
from github_analytics.backfill.rest_backfill import (
    GitHubRestBackfill,
    RestDataError,
    _aware_datetime,
    _github_timestamp,
    _next_page,
    _required_array_payload,
    _required_nonnegative_int,
    _required_object,
    _required_object_list,
    _required_object_payload,
    _required_positive_int,
    _required_string,
)

START = datetime(2026, 1, 1, tzinfo=UTC)
END = datetime(2026, 2, 1, tzinfo=UTC)


class FakeRestClient:
    def __init__(self, responses: list[dict[str, Any] | list[Any]]) -> None:
        self.responses = responses
        self.requests: list[tuple[str, Mapping[str, object]]] = []

    def get(
        self,
        path: str,
        params: Mapping[str, object],
    ) -> dict[str, Any] | list[Any]:
        self.requests.append((path, params))
        return self.responses.pop(0)


class MemoryStorage:
    def __init__(
        self,
        checkpoints: dict[tuple[str, str], BackfillCheckpoint] | None = None,
        existing_keys: set[str] | None = None,
    ) -> None:
        self.checkpoints = checkpoints or {}
        self.source_keys = existing_keys or set()
        self.records: list[BackfillRecord] = []
        self.persisted: list[tuple[BackfillRunKey, str | None, bool]] = []

    def load_checkpoint(self, key: BackfillRunKey) -> BackfillCheckpoint | None:
        return self.checkpoints.get((key.resource, key.scope))

    def persist_page(
        self,
        key: BackfillRunKey,
        records: Sequence[BackfillRecord],
        *,
        next_cursor: str | None,
        completed: bool,
    ) -> PageWriteOutcome:
        inserted = 0
        for record in records:
            if record.source_record_key in self.source_keys:
                continue
            self.source_keys.add(record.source_record_key)
            self.records.append(record)
            inserted += 1
        prior = self.checkpoints.get((key.resource, key.scope))
        checkpoint = BackfillCheckpoint(
            cursor=next_cursor,
            status=(CheckpointStatus.COMPLETED if completed else CheckpointStatus.IN_PROGRESS),
            pages_completed=(prior.pages_completed if prior else 0) + 1,
            records_inserted=(prior.records_inserted if prior else 0) + inserted,
        )
        self.checkpoints[(key.resource, key.scope)] = checkpoint
        self.persisted.append((key, next_cursor, completed))
        return PageWriteOutcome(
            inserted=inserted,
            duplicates=len(records) - inserted,
            checkpoint=checkpoint,
        )


def _workflow_run(
    run_id: int,
    created_at: str,
    *,
    updated_at: str = "2026-01-12T00:00:00Z",
    status: str = "completed",
    attempt: int = 1,
    repository_id: int = 10,
    repository_name: str = "example/repo",
) -> dict[str, Any]:
    return {
        "id": run_id,
        "run_attempt": attempt,
        "created_at": created_at,
        "updated_at": updated_at,
        "status": status,
        "conclusion": "success" if status == "completed" else None,
        "repository": {"id": repository_id, "full_name": repository_name},
    }


def _deployment(deployment_id: int, created_at: str) -> dict[str, Any]:
    return {
        "id": deployment_id,
        "created_at": created_at,
        "updated_at": created_at,
        "environment": "production",
        "sha": "a" * 40,
    }


def _status(status_id: int, state: str = "success") -> dict[str, Any]:
    return {
        "id": status_id,
        "state": state,
        "created_at": "2026-02-02T00:00:00Z",
        "updated_at": "2026-02-02T00:00:00Z",
    }


def _backfill(
    responses: list[dict[str, Any] | list[Any]],
    storage: MemoryStorage | None = None,
    **kwargs: object,
) -> tuple[GitHubRestBackfill, FakeRestClient, MemoryStorage]:
    client = FakeRestClient(responses)
    actual_storage = storage or MemoryStorage()
    arguments: dict[str, object] = {
        "client": client,
        "storage": actual_storage,
        "repository": "example/repo",
        "repository_id": 10,
        "installation_id": 20,
        "page_size": 2,
    }
    arguments.update(kwargs)
    return (
        GitHubRestBackfill(**arguments),  # type: ignore[arg-type]
        client,
        actual_storage,
    )


def test_run_paginates_filters_and_persists_all_rest_resources() -> None:
    backfill, client, storage = _backfill(
        [
            {
                "total_count": 3,
                "workflow_runs": [
                    _workflow_run(1, "2026-01-10T00:00:00Z"),
                    _workflow_run(2, "2026-02-01T00:00:00Z"),
                ],
            },
            {
                "total_count": 3,
                "workflow_runs": [
                    _workflow_run(
                        3,
                        "2026-01-20T00:00:00Z",
                        status="in_progress",
                    )
                ],
            },
            [
                _deployment(4, "2026-01-15T00:00:00Z"),
                _deployment(5, "2025-12-15T00:00:00Z"),
            ],
            [_status(6), _status(7, "failure")],
            [_status(8, "inactive")],
            [],
        ]
    )

    assert backfill.run(window_start=START, window_end=END) == BackfillSummary(6, 6, 0)
    assert [record.event_name for record in storage.records] == [
        "workflow_run",
        "workflow_run",
        "deployment_status",
        "deployment_status",
        "deployment_status",
        "deployment",
    ]
    assert [record.action for record in storage.records] == [
        "completed",
        "in_progress",
        "success",
        "failure",
        "inactive",
        "created",
    ]
    assert storage.records[0].source_record_key == (
        "github_rest:workflow_run:1:1:2026-01-12T00:00:00+00:00"
    )
    assert storage.records[2].source_record_key == "github_rest:deployment_status:6"
    assert storage.records[-1].source_record_key == "github_rest:deployment:4"
    assert storage.records[2].payload["deployment_id"] == 4
    assert client.requests[0][0] == "repos/example/repo/actions/runs"
    assert client.requests[0][1]["page"] == 1
    assert client.requests[1][1]["page"] == 2
    assert client.requests[2][0] == "repos/example/repo/deployments"
    assert client.requests[3][0].endswith("/deployments/4/statuses")
    assert storage.checkpoints[("workflow_runs", "repository")].status is (
        CheckpointStatus.COMPLETED
    )
    assert storage.checkpoints[("deployment_statuses", "4")].pages_completed == 2

    request_count = len(client.requests)
    assert backfill.run(window_start=START, window_end=END) == BackfillSummary(0, 0, 0)
    assert len(client.requests) == request_count


def test_run_resumes_pages_and_absorbs_duplicate_source_keys() -> None:
    storage = MemoryStorage(
        {
            ("workflow_runs", "repository"): BackfillCheckpoint(
                "2", CheckpointStatus.IN_PROGRESS, 1, 1
            ),
            ("deployments", "repository"): BackfillCheckpoint(
                None, CheckpointStatus.COMPLETED, 1, 0
            ),
        },
        existing_keys={
            "github_rest:workflow_run:3:1:2026-01-12T00:00:00+00:00",
        },
    )
    backfill, client, _ = _backfill(
        [
            {
                "total_count": 3,
                "workflow_runs": [_workflow_run(3, "2026-01-20T00:00:00Z")],
            }
        ],
        storage,
    )

    assert backfill.run(window_start=START, window_end=END) == BackfillSummary(1, 0, 1)
    assert client.requests[0][1]["page"] == 2


def test_replayed_deployment_page_skips_already_completed_status_traversal() -> None:
    storage = MemoryStorage(
        {
            ("workflow_runs", "repository"): BackfillCheckpoint(
                None, CheckpointStatus.COMPLETED, 1, 0
            ),
            ("deployment_statuses", "4"): BackfillCheckpoint(
                None, CheckpointStatus.COMPLETED, 1, 1
            ),
        }
    )
    backfill, client, _ = _backfill(
        [[_deployment(4, "2026-01-15T00:00:00Z")]],
        storage,
    )

    assert backfill.run(window_start=START, window_end=END) == BackfillSummary(1, 1, 0)
    assert len(client.requests) == 1
    assert client.requests[0][0].endswith("/deployments")


def test_workflow_window_over_api_cap_fails_without_advancing_checkpoint() -> None:
    backfill, _, storage = _backfill([{"total_count": 1_001, "workflow_runs": []}])

    with pytest.raises(RestDataError, match="1,000-result"):
        backfill.run(window_start=START, window_end=END)
    assert storage.persisted == []


def test_workflow_pagination_cannot_end_before_reported_total() -> None:
    backfill, _, _ = _backfill(
        [{"total_count": 3, "workflow_runs": []}],
    )

    with pytest.raises(RestDataError, match="ended before"):
        backfill.run(window_start=START, window_end=END)


@pytest.mark.parametrize(
    ("arguments", "message"),
    [
        ({"repository": "invalid"}, "owner/name"),
        ({"repository": "/repo"}, "owner/name"),
        ({"repository": "owner/repo/extra"}, "owner/name"),
        ({"repository_id": 0}, "positive"),
        ({"installation_id": 0}, "positive"),
        ({"page_size": 0}, "page_size"),
        ({"page_size": 101}, "page_size"),
    ],
)
def test_constructor_rejects_invalid_configuration(
    arguments: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        _backfill([], **arguments)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("start", "end", "message"),
    [
        (datetime(2026, 1, 1), END, "timezone-aware"),
        (END, START, "precede"),
    ],
)
def test_run_rejects_invalid_windows(
    start: datetime,
    end: datetime,
    message: str,
) -> None:
    backfill, _, _ = _backfill([])

    with pytest.raises(ValueError, match=message):
        backfill.run(window_start=start, window_end=end)


@pytest.mark.parametrize(
    ("checkpoint", "message"),
    [
        (
            BackfillCheckpoint(None, CheckpointStatus.IN_PROGRESS, 1, 0),
            "missing",
        ),
        (
            BackfillCheckpoint("cursor", CheckpointStatus.IN_PROGRESS, 1, 0),
            "not a page",
        ),
        (
            BackfillCheckpoint("0", CheckpointStatus.IN_PROGRESS, 1, 0),
            "positive",
        ),
    ],
)
def test_invalid_rest_checkpoint_cursors_fail_closed(
    checkpoint: BackfillCheckpoint,
    message: str,
) -> None:
    with pytest.raises(RestDataError, match=message):
        _next_page(checkpoint)


@pytest.mark.parametrize(
    ("repository_id", "repository_name", "message"),
    [
        (11, "example/repo", "id"),
        (10, "other/repo", "name"),
    ],
)
def test_workflow_repository_identity_must_match_configuration(
    repository_id: int,
    repository_name: str,
    message: str,
) -> None:
    backfill, _, _ = _backfill(
        [
            {
                "total_count": 1,
                "workflow_runs": [
                    _workflow_run(
                        1,
                        "2026-01-10T00:00:00Z",
                        repository_id=repository_id,
                        repository_name=repository_name,
                    )
                ],
            }
        ]
    )

    with pytest.raises(RestDataError, match=message):
        backfill.run(window_start=START, window_end=END)


@pytest.mark.parametrize(
    ("callable_name", "arguments", "message"),
    [
        ("object_payload", ([], "response"), "object"),
        ("array_payload", ({}, "response"), "array"),
        ("array_payload", ([None], "response"), "array"),
        ("object_list", ({"items": None}, "items"), "invalid"),
        ("object_list", ({"items": [None]}, "items"), "invalid"),
        ("object", ({}, "item"), "missing"),
        ("string", ({"item": ""}, "item"), "missing"),
        ("nonnegative", ({"item": True}, "item"), "invalid"),
        ("nonnegative", ({"item": -1}, "item"), "invalid"),
        ("positive", ({"item": 0}, "item"), "invalid"),
        ("datetime", ({"item": "bad"}, "item"), "invalid"),
    ],
)
def test_rest_shape_helpers_reject_invalid_values(
    callable_name: str,
    arguments: tuple[object, str],
    message: str,
) -> None:
    helpers = {
        "object_payload": _required_object_payload,
        "array_payload": _required_array_payload,
        "object_list": _required_object_list,
        "object": _required_object,
        "string": _required_string,
        "nonnegative": _required_nonnegative_int,
        "positive": _required_positive_int,
        "datetime": _aware_datetime,
    }

    with pytest.raises(RestDataError, match=message):
        helpers[callable_name](*arguments)  # type: ignore[arg-type]


def test_rest_shape_helpers_accept_valid_values_and_format_utc() -> None:
    item = {"id": 1}

    assert _required_object_payload(item, "response") == item
    assert _required_array_payload([item], "response") == [item]
    assert _required_object_list({"items": [item]}, "items") == [item]
    assert _required_object({"item": item}, "item") == item
    assert _required_string({"item": "value"}, "item") == "value"
    assert _required_nonnegative_int({"item": 0}, "item") == 0
    assert _required_positive_int({"item": 1}, "item") == 1
    assert _aware_datetime({"item": "2026-01-01T00:00:00Z"}, "item") == START
    assert _github_timestamp(START) == "2026-01-01T00:00:00Z"
