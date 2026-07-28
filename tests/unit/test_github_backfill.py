"""Tests for restartable pull-request, review, and commit adapters."""

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any

import pytest

from github_analytics.backfill.github import (
    BackfillSummary,
    GitHubPullRequestBackfill,
    GraphQLDataError,
    _aware_datetime,
    _connection_page,
    _optional_aware_datetime,
    _required_object,
    _required_string,
)
from github_analytics.backfill.models import (
    BackfillCheckpoint,
    BackfillRecord,
    BackfillRunKey,
    CheckpointStatus,
    PageWriteOutcome,
)

START = datetime(2026, 1, 1, tzinfo=UTC)
END = datetime(2026, 2, 1, tzinfo=UTC)


class FakeGraphQLClient:
    def __init__(self, responses: list[dict[str, Any]]) -> None:
        self.responses = responses
        self.requests: list[tuple[str, Mapping[str, object]]] = []

    def execute(
        self,
        query: str,
        variables: Mapping[str, object],
    ) -> dict[str, Any]:
        self.requests.append((query, variables))
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


def _page(
    nodes: list[dict[str, Any]],
    *,
    has_next: bool = False,
    cursor: str | None = None,
) -> dict[str, Any]:
    return {
        "nodes": nodes,
        "pageInfo": {"hasNextPage": has_next, "endCursor": cursor},
    }


def _repository_page(
    nodes: list[dict[str, Any]],
    *,
    has_next: bool = False,
    cursor: str | None = None,
    repository_id: int = 10,
    repository_name: str = "example/repo",
) -> dict[str, Any]:
    return {
        "repository": {
            "databaseId": repository_id,
            "nameWithOwner": repository_name,
            "pullRequests": _page(nodes, has_next=has_next, cursor=cursor),
        }
    }


def _nested_page(
    connection_name: str,
    nodes: list[dict[str, Any]],
    *,
    has_next: bool = False,
    cursor: str | None = None,
    typename: str = "PullRequest",
) -> dict[str, Any]:
    return {
        "node": {
            "__typename": typename,
            connection_name: _page(nodes, has_next=has_next, cursor=cursor),
        }
    }


def _pull_request(
    node_id: str,
    created_at: str,
    *,
    updated_at: str = "2026-01-12T00:00:00Z",
    state: str = "MERGED",
) -> dict[str, Any]:
    return {
        "id": node_id,
        "number": 7,
        "state": state,
        "title": "Synthetic PR",
        "createdAt": created_at,
        "updatedAt": updated_at,
    }


def _review(
    node_id: str,
    *,
    state: str = "COMMENTED",
    submitted_at: str | None = None,
) -> dict[str, Any]:
    return {
        "id": node_id,
        "state": state,
        "submittedAt": submitted_at,
        "updatedAt": "2026-01-15T00:00:00Z",
    }


def _commit(node_id: str) -> dict[str, Any]:
    return {
        "id": node_id,
        "commit": {
            "id": f"COMMIT_{node_id}",
            "oid": "a" * 40,
            "committedDate": "2026-01-14T00:00:00Z",
        },
    }


def _backfill(
    responses: list[dict[str, Any]],
    storage: MemoryStorage | None = None,
) -> tuple[GitHubPullRequestBackfill, FakeGraphQLClient, MemoryStorage]:
    client = FakeGraphQLClient(responses)
    actual_storage = storage or MemoryStorage()
    return (
        GitHubPullRequestBackfill(
            client,
            actual_storage,
            repository="example/repo",
            repository_id=10,
            installation_id=20,
            page_size=2,
        ),
        client,
        actual_storage,
    )


def test_run_paginates_all_resources_filters_window_and_persists_stable_keys() -> None:
    backfill, client, storage = _backfill(
        [
            _repository_page(
                [
                    _pull_request("PR_OLD", "2025-12-01T00:00:00Z"),
                    _pull_request("PR_1", "2026-01-10T00:00:00Z"),
                ],
                has_next=True,
                cursor="pr-page-2",
            ),
            _nested_page(
                "reviews",
                [_review("REVIEW_1")],
                has_next=True,
                cursor="review-page-2",
            ),
            _nested_page(
                "reviews",
                [_review("REVIEW_2", state="APPROVED", submitted_at="2026-01-16T00:00:00Z")],
            ),
            _nested_page("commits", [_commit("PR_COMMIT_1")]),
            _repository_page([_pull_request("PR_AFTER", "2026-02-01T00:00:00Z")]),
        ]
    )

    summary = backfill.run(window_start=START, window_end=END)

    assert summary == BackfillSummary(
        pages_persisted=5,
        records_inserted=4,
        duplicates_absorbed=0,
    )
    assert len(client.requests) == 5
    assert client.requests[0][1]["cursor"] is None
    assert client.requests[1][1]["pullRequestId"] == "PR_1"
    assert client.requests[2][1]["cursor"] == "review-page-2"
    assert client.requests[4][1]["cursor"] == "pr-page-2"
    assert storage.checkpoints[("pull_requests", "repository")].status is (
        CheckpointStatus.COMPLETED
    )

    records = {record.event_name: record for record in storage.records}
    assert records["pull_request"].source_record_key == (
        "github_graphql:pull_request:PR_1:2026-01-12T00:00:00+00:00"
    )
    assert records["pull_request"].action == "merged"
    assert records["pull_request_review"].source_record_key.endswith("REVIEW_2:approved")
    assert records["pull_request_review"].payload["pull_request_id"] == "PR_1"
    assert records["pull_request_commit"].source_record_key.endswith("PR_COMMIT_1")
    assert records["pull_request_commit"].payload["pull_request_id"] == "PR_1"
    assert records["pull_request_commit"].payload["repository"] == {
        "id": 10,
        "full_name": "example/repo",
    }
    commented = next(record for record in storage.records if record.action == "commented")
    assert commented.occurred_at == datetime(2026, 1, 15, tzinfo=UTC)


def test_completed_outer_checkpoint_skips_every_api_call() -> None:
    storage = MemoryStorage(
        {
            ("pull_requests", "repository"): BackfillCheckpoint(
                None,
                CheckpointStatus.COMPLETED,
                3,
                8,
            )
        }
    )
    backfill, client, _ = _backfill([], storage)

    assert backfill.run(window_start=START, window_end=END) == BackfillSummary(0, 0, 0)
    assert client.requests == []


def test_in_progress_checkpoints_resume_outer_and_nested_cursors() -> None:
    storage = MemoryStorage(
        {
            ("pull_requests", "repository"): BackfillCheckpoint(
                "resume-pr",
                CheckpointStatus.IN_PROGRESS,
                1,
                0,
            ),
            ("pull_request_reviews", "PR_1"): BackfillCheckpoint(
                "resume-review",
                CheckpointStatus.IN_PROGRESS,
                1,
                0,
            ),
            ("pull_request_commits", "PR_1"): BackfillCheckpoint(
                None,
                CheckpointStatus.COMPLETED,
                1,
                1,
            ),
        }
    )
    backfill, client, _ = _backfill(
        [
            _repository_page([_pull_request("PR_1", "2026-01-10T00:00:00Z")]),
            _nested_page("reviews", [_review("REVIEW_1")]),
        ],
        storage,
    )

    assert backfill.run(window_start=START, window_end=END).pages_persisted == 2
    assert client.requests[0][1]["cursor"] == "resume-pr"
    assert client.requests[1][1]["cursor"] == "resume-review"


def test_duplicate_source_keys_are_absorbed_by_storage_boundary() -> None:
    existing = {
        "github_graphql:pull_request:PR_1:2026-01-12T00:00:00+00:00",
        "github_graphql:pull_request_review:REVIEW_1:commented",
        "github_graphql:pull_request_commit:PR_COMMIT_1",
    }
    storage = MemoryStorage(existing_keys=existing)
    backfill, _, _ = _backfill(
        [
            _repository_page([_pull_request("PR_1", "2026-01-10T00:00:00Z")]),
            _nested_page("reviews", [_review("REVIEW_1")]),
            _nested_page("commits", [_commit("PR_COMMIT_1")]),
        ],
        storage,
    )

    assert backfill.run(window_start=START, window_end=END) == BackfillSummary(3, 0, 3)


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
    values: dict[str, object] = {
        "client": FakeGraphQLClient([]),
        "storage": MemoryStorage(),
        "repository": "example/repo",
        "repository_id": 10,
        "installation_id": 20,
        "page_size": 50,
    }
    values.update(arguments)
    with pytest.raises(ValueError, match=message):
        GitHubPullRequestBackfill(**values)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("repository_id", "repository_name", "message"),
    [
        (11, "example/repo", "databaseId"),
        (10, "other/repo", "name"),
    ],
)
def test_run_rejects_repository_identity_mismatch(
    repository_id: int,
    repository_name: str,
    message: str,
) -> None:
    backfill, _, _ = _backfill(
        [
            _repository_page(
                [],
                repository_id=repository_id,
                repository_name=repository_name,
            )
        ]
    )

    with pytest.raises(GraphQLDataError, match=message):
        backfill.run(window_start=START, window_end=END)


def test_nested_node_must_still_be_a_pull_request() -> None:
    backfill, _, _ = _backfill(
        [
            _repository_page([_pull_request("PR_1", "2026-01-10T00:00:00Z")]),
            _nested_page("reviews", [], typename="Issue"),
        ]
    )

    with pytest.raises(GraphQLDataError, match="not a PullRequest"):
        backfill.run(window_start=START, window_end=END)


@pytest.mark.parametrize(
    ("container", "name", "message"),
    [
        ({"items": {"nodes": None, "pageInfo": {}}}, "items", "object nodes"),
        ({"items": {"nodes": [None], "pageInfo": {}}}, "items", "object nodes"),
        ({"items": {"nodes": [], "pageInfo": None}}, "items", "pageInfo"),
        (
            {"items": {"nodes": [], "pageInfo": {"hasNextPage": "yes"}}},
            "items",
            "hasNextPage",
        ),
        (
            {
                "items": {
                    "nodes": [],
                    "pageInfo": {"hasNextPage": False, "endCursor": 1},
                }
            },
            "items",
            "endCursor",
        ),
        (
            {
                "items": {
                    "nodes": [],
                    "pageInfo": {"hasNextPage": True, "endCursor": None},
                }
            },
            "items",
            "no endCursor",
        ),
    ],
)
def test_connection_page_rejects_malformed_shapes(
    container: Mapping[str, Any],
    name: str,
    message: str,
) -> None:
    with pytest.raises(GraphQLDataError, match=message):
        _connection_page(container, name)


def test_graphql_scalar_helpers_reject_missing_or_invalid_values() -> None:
    with pytest.raises(GraphQLDataError, match="missing value"):
        _required_object({}, "value")
    with pytest.raises(GraphQLDataError, match="missing value"):
        _required_string({"value": ""}, "value")
    with pytest.raises(GraphQLDataError, match="invalid createdAt"):
        _aware_datetime({"createdAt": "not-a-date"}, "createdAt")
    assert _optional_aware_datetime({"submittedAt": None}, "submittedAt") is None
    assert _required_object({"value": {"id": 1}}, "value") == {"id": 1}
    assert _required_string({"value": "id"}, "value") == "id"
