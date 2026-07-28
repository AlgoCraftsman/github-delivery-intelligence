"""Restartable GitHub GraphQL backfill for pull requests, reviews, and commits."""

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from functools import partial
from typing import Any, Protocol, cast

from pydantic import AwareDatetime, TypeAdapter, ValidationError

from github_analytics.backfill.models import (
    BackfillCheckpoint,
    BackfillRecord,
    BackfillRunKey,
    CheckpointStatus,
    PageWriteOutcome,
)

_AWARE_DATETIME = TypeAdapter(AwareDatetime)

_PULL_REQUESTS_QUERY = """
query PullRequests($owner: String!, $name: String!, $pageSize: Int!, $cursor: String) {
  repository(owner: $owner, name: $name) {
    databaseId
    nameWithOwner
    pullRequests(
      first: $pageSize
      after: $cursor
      orderBy: {field: CREATED_AT, direction: ASC}
    ) {
      nodes {
        id
        number
        state
        title
        body
        isDraft
        createdAt
        updatedAt
        closedAt
        mergedAt
        baseRefName
        headRefName
        additions
        deletions
        changedFiles
        author { id login }
        mergeCommit { id oid }
      }
      pageInfo { hasNextPage endCursor }
    }
  }
}
"""

_REVIEWS_QUERY = """
query PullRequestReviews($pullRequestId: ID!, $pageSize: Int!, $cursor: String) {
  node(id: $pullRequestId) {
    __typename
    ... on PullRequest {
      reviews(first: $pageSize, after: $cursor) {
        nodes {
          id
          fullDatabaseId
          state
          body
          submittedAt
          updatedAt
          url
          author { id login }
          commit { id oid }
        }
        pageInfo { hasNextPage endCursor }
      }
    }
  }
}
"""

_COMMITS_QUERY = """
query PullRequestCommits($pullRequestId: ID!, $pageSize: Int!, $cursor: String) {
  node(id: $pullRequestId) {
    __typename
    ... on PullRequest {
      commits(first: $pageSize, after: $cursor) {
        nodes {
          id
          commit {
            id
            oid
            authoredDate
            committedDate
            message
            additions
            deletions
            changedFilesIfAvailable
            author { name email user { id login } }
          }
        }
        pageInfo { hasNextPage endCursor }
      }
    }
  }
}
"""


class GraphQLDataError(RuntimeError):
    """A successful GraphQL call did not satisfy the backfill data contract."""


class GraphQLExecutor(Protocol):
    """Query surface used by the resource adapters."""

    def execute(
        self,
        query: str,
        variables: Mapping[str, object],
    ) -> dict[str, Any]: ...


class BackfillStorage(Protocol):
    """Checkpoint and transactional page-write boundary."""

    def load_checkpoint(self, key: BackfillRunKey) -> BackfillCheckpoint | None: ...

    def persist_page(
        self,
        key: BackfillRunKey,
        records: Sequence[BackfillRecord],
        *,
        next_cursor: str | None,
        completed: bool,
    ) -> PageWriteOutcome: ...


@dataclass(frozen=True, slots=True)
class BackfillSummary:
    """Work performed by one invocation."""

    pages_persisted: int
    records_inserted: int
    duplicates_absorbed: int

    def plus(self, other: "BackfillSummary") -> "BackfillSummary":
        """Combine nested and repository traversal evidence."""

        return BackfillSummary(
            pages_persisted=self.pages_persisted + other.pages_persisted,
            records_inserted=self.records_inserted + other.records_inserted,
            duplicates_absorbed=self.duplicates_absorbed + other.duplicates_absorbed,
        )


class GitHubPullRequestBackfill:
    """Backfill PR snapshots and every review/commit connection for each selected PR."""

    def __init__(
        self,
        client: GraphQLExecutor,
        storage: BackfillStorage,
        *,
        repository: str,
        repository_id: int,
        installation_id: int,
        page_size: int = 50,
    ) -> None:
        try:
            owner, name = repository.split("/", maxsplit=1)
        except ValueError as error:
            raise ValueError("repository must use owner/name form") from error
        if not owner or not name or "/" in name:
            raise ValueError("repository must use owner/name form")
        if repository_id <= 0 or installation_id <= 0:
            raise ValueError("repository and installation identities must be positive")
        if not 1 <= page_size <= 100:
            raise ValueError("GitHub GraphQL page_size must be between 1 and 100")
        self._client = client
        self._storage = storage
        self._owner = owner
        self._name = name
        self._repository = repository
        self._repository_id = repository_id
        self._installation_id = installation_id
        self._page_size = page_size

    def run(self, *, window_start: datetime, window_end: datetime) -> BackfillSummary:
        """Traverse a bounded PR creation window, resuming every stored cursor."""

        key = BackfillRunKey(
            repository_id=self._repository_id,
            resource="pull_requests",
            scope="repository",
            window_start=window_start,
            window_end=window_end,
        )
        checkpoint = self._storage.load_checkpoint(key)
        if checkpoint is not None and checkpoint.status is CheckpointStatus.COMPLETED:
            return BackfillSummary(0, 0, 0)
        cursor = checkpoint.cursor if checkpoint is not None else None
        summary = BackfillSummary(0, 0, 0)

        while True:
            data = self._client.execute(
                _PULL_REQUESTS_QUERY,
                {
                    "owner": self._owner,
                    "name": self._name,
                    "pageSize": self._page_size,
                    "cursor": cursor,
                },
            )
            repository = _required_object(data, "repository")
            self._validate_repository(repository)
            nodes, end_cursor, has_next_page = _connection_page(
                repository,
                "pullRequests",
            )
            records: list[BackfillRecord] = []
            reached_window_end = False
            for node in nodes:
                created_at = _aware_datetime(node, "createdAt")
                if created_at >= window_end:
                    reached_window_end = True
                    break
                if created_at < window_start:
                    continue
                pull_request_id = _required_string(node, "id")
                summary = summary.plus(
                    self._backfill_nested(
                        pull_request_id,
                        key,
                        resource="pull_request_reviews",
                        connection_name="reviews",
                        query=_REVIEWS_QUERY,
                        record_factory=partial(self._review_record, pull_request_id),
                    )
                )
                summary = summary.plus(
                    self._backfill_nested(
                        pull_request_id,
                        key,
                        resource="pull_request_commits",
                        connection_name="commits",
                        query=_COMMITS_QUERY,
                        record_factory=partial(self._commit_record, pull_request_id),
                    )
                )
                records.append(self._pull_request_record(node, created_at))

            completed = reached_window_end or not has_next_page
            outcome = self._storage.persist_page(
                key,
                records,
                next_cursor=None if completed else end_cursor,
                completed=completed,
            )
            summary = summary.plus(_summary(outcome))
            if completed:
                return summary
            cursor = end_cursor

    def _backfill_nested(
        self,
        pull_request_id: str,
        parent_key: BackfillRunKey,
        *,
        resource: str,
        connection_name: str,
        query: str,
        record_factory: Callable[[dict[str, Any]], BackfillRecord],
    ) -> BackfillSummary:
        key = BackfillRunKey(
            repository_id=self._repository_id,
            resource=resource,
            scope=pull_request_id,
            window_start=parent_key.window_start,
            window_end=parent_key.window_end,
        )
        checkpoint = self._storage.load_checkpoint(key)
        if checkpoint is not None and checkpoint.status is CheckpointStatus.COMPLETED:
            return BackfillSummary(0, 0, 0)
        cursor = checkpoint.cursor if checkpoint is not None else None
        summary = BackfillSummary(0, 0, 0)
        while True:
            data = self._client.execute(
                query,
                {
                    "pullRequestId": pull_request_id,
                    "pageSize": self._page_size,
                    "cursor": cursor,
                },
            )
            node = _required_object(data, "node")
            if node.get("__typename") != "PullRequest":
                raise GraphQLDataError(f"GitHub node {pull_request_id!r} is not a PullRequest")
            items, end_cursor, has_next_page = _connection_page(node, connection_name)
            records = [record_factory(item) for item in items]
            outcome = self._storage.persist_page(
                key,
                records,
                next_cursor=end_cursor if has_next_page else None,
                completed=not has_next_page,
            )
            summary = summary.plus(_summary(outcome))
            if not has_next_page:
                return summary
            cursor = end_cursor

    def _validate_repository(self, repository: Mapping[str, Any]) -> None:
        if repository.get("databaseId") != self._repository_id:
            raise GraphQLDataError("GitHub repository databaseId does not match configuration")
        if repository.get("nameWithOwner") != self._repository:
            raise GraphQLDataError("GitHub repository name does not match configuration")

    def _pull_request_record(
        self,
        node: dict[str, Any],
        created_at: datetime,
    ) -> BackfillRecord:
        node_id = _required_string(node, "id")
        updated_at = _aware_datetime(node, "updatedAt")
        state = _required_string(node, "state").lower()
        return BackfillRecord(
            source_record_key=f"github_graphql:pull_request:{node_id}:{updated_at.isoformat()}",
            event_name="pull_request",
            action=state,
            repository_id=self._repository_id,
            installation_id=self._installation_id,
            occurred_at=created_at,
            payload=self._payload("pull_request", node),
        )

    def _review_record(
        self,
        pull_request_id: str,
        node: dict[str, Any],
    ) -> BackfillRecord:
        node_id = _required_string(node, "id")
        state = _required_string(node, "state").lower()
        submitted_at = _optional_aware_datetime(node, "submittedAt")
        occurred_at = submitted_at or _aware_datetime(node, "updatedAt")
        return BackfillRecord(
            source_record_key=f"github_graphql:pull_request_review:{node_id}:{state}",
            event_name="pull_request_review",
            action=state,
            repository_id=self._repository_id,
            installation_id=self._installation_id,
            occurred_at=occurred_at,
            payload={
                **self._payload("pull_request_review", node),
                "pull_request_id": pull_request_id,
            },
        )

    def _commit_record(
        self,
        pull_request_id: str,
        node: dict[str, Any],
    ) -> BackfillRecord:
        association_id = _required_string(node, "id")
        commit = _required_object(node, "commit")
        occurred_at = _aware_datetime(commit, "committedDate")
        return BackfillRecord(
            source_record_key=f"github_graphql:pull_request_commit:{association_id}",
            event_name="pull_request_commit",
            action="associated",
            repository_id=self._repository_id,
            installation_id=self._installation_id,
            occurred_at=occurred_at,
            payload={
                **self._payload("pull_request_commit", node),
                "pull_request_id": pull_request_id,
            },
        )

    def _payload(self, resource: str, node: dict[str, Any]) -> dict[str, Any]:
        return {
            "resource": resource,
            "repository": {
                "id": self._repository_id,
                "full_name": self._repository,
            },
            resource: node,
        }


def _summary(outcome: PageWriteOutcome) -> BackfillSummary:
    return BackfillSummary(
        pages_persisted=1,
        records_inserted=outcome.inserted,
        duplicates_absorbed=outcome.duplicates,
    )


def _connection_page(
    container: Mapping[str, Any],
    name: str,
) -> tuple[list[dict[str, Any]], str | None, bool]:
    connection = _required_object(container, name)
    raw_nodes = connection.get("nodes")
    if not isinstance(raw_nodes, list) or any(not isinstance(node, dict) for node in raw_nodes):
        raise GraphQLDataError(f"{name} connection is missing object nodes")
    page_info = _required_object(connection, "pageInfo")
    has_next_page = page_info.get("hasNextPage")
    if not isinstance(has_next_page, bool):
        raise GraphQLDataError(f"{name} pageInfo is missing hasNextPage")
    end_cursor = page_info.get("endCursor")
    if end_cursor is not None and not isinstance(end_cursor, str):
        raise GraphQLDataError(f"{name} pageInfo endCursor must be a string or null")
    if has_next_page and not end_cursor:
        raise GraphQLDataError(f"{name} has another page but no endCursor")
    return cast(list[dict[str, Any]], raw_nodes), end_cursor, has_next_page


def _required_object(container: Mapping[str, Any], name: str) -> dict[str, Any]:
    value = container.get(name)
    if not isinstance(value, dict):
        raise GraphQLDataError(f"GitHub GraphQL response is missing {name}")
    return cast(dict[str, Any], value)


def _required_string(container: Mapping[str, Any], name: str) -> str:
    value = container.get(name)
    if not isinstance(value, str) or not value:
        raise GraphQLDataError(f"GitHub GraphQL response is missing {name}")
    return value


def _aware_datetime(container: Mapping[str, Any], name: str) -> datetime:
    value = container.get(name)
    try:
        return _AWARE_DATETIME.validate_python(value)
    except ValidationError as error:
        raise GraphQLDataError(f"GitHub GraphQL response has invalid {name}") from error


def _optional_aware_datetime(
    container: Mapping[str, Any],
    name: str,
) -> datetime | None:
    if container.get(name) is None:
        return None
    return _aware_datetime(container, name)
