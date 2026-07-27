"""Restartable GitHub REST backfill for workflow runs and deployments."""

from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any, Protocol, cast

from pydantic import AwareDatetime, TypeAdapter, ValidationError

from github_analytics.backfill.github import BackfillStorage, BackfillSummary
from github_analytics.backfill.models import (
    BackfillCheckpoint,
    BackfillRecord,
    BackfillRunKey,
    CheckpointStatus,
    PageWriteOutcome,
)

_AWARE_DATETIME = TypeAdapter(AwareDatetime)
_MAX_FILTERED_WORKFLOW_RUNS = 1_000


class RestDataError(RuntimeError):
    """A successful REST call did not satisfy the backfill data contract."""


class RestExecutor(Protocol):
    """GET surface used by the REST resource adapters."""

    def get(
        self,
        path: str,
        params: Mapping[str, object],
    ) -> dict[str, Any] | list[Any]: ...


class GitHubRestBackfill:
    """Backfill bounded workflow runs and deployments with their statuses."""

    def __init__(
        self,
        client: RestExecutor,
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
            raise ValueError("GitHub REST page_size must be between 1 and 100")
        self._client = client
        self._storage = storage
        self._repository = repository
        self._repository_path = f"repos/{owner}/{name}"
        self._repository_id = repository_id
        self._installation_id = installation_id
        self._page_size = page_size

    def run(self, *, window_start: datetime, window_end: datetime) -> BackfillSummary:
        """Backfill both REST resource families for one half-open time window."""

        return self._run_workflow_runs(
            window_start=window_start,
            window_end=window_end,
        ).plus(
            self._run_deployments(
                window_start=window_start,
                window_end=window_end,
            )
        )

    def _run_workflow_runs(
        self,
        *,
        window_start: datetime,
        window_end: datetime,
    ) -> BackfillSummary:
        key = self._run_key("workflow_runs", "repository", window_start, window_end)
        checkpoint = self._storage.load_checkpoint(key)
        if _completed(checkpoint):
            return BackfillSummary(0, 0, 0)
        page = _next_page(checkpoint)
        summary = BackfillSummary(0, 0, 0)

        while True:
            payload = self._client.get(
                f"{self._repository_path}/actions/runs",
                {
                    "created": (
                        f"{_github_timestamp(window_start)}..{_github_timestamp(window_end)}"
                    ),
                    "exclude_pull_requests": False,
                    "per_page": self._page_size,
                    "page": page,
                },
            )
            response = _required_object_payload(payload, "workflow-run response")
            total_count = _required_nonnegative_int(response, "total_count")
            if total_count > _MAX_FILTERED_WORKFLOW_RUNS:
                raise RestDataError(
                    "workflow-run window exceeds GitHub's 1,000-result filtered-search cap; "
                    "use a smaller time window"
                )
            nodes = _required_object_list(response, "workflow_runs")
            records = [
                self._workflow_run_record(node)
                for node in nodes
                if _in_window(_aware_datetime(node, "created_at"), window_start, window_end)
            ]
            completed = page * self._page_size >= total_count
            outcome = self._persist_page(key, records, page=page, completed=completed)
            summary = summary.plus(_summary(outcome))
            if completed:
                return summary
            if not nodes:
                raise RestDataError("workflow-run pagination ended before total_count")
            page += 1

    def _run_deployments(
        self,
        *,
        window_start: datetime,
        window_end: datetime,
    ) -> BackfillSummary:
        key = self._run_key("deployments", "repository", window_start, window_end)
        checkpoint = self._storage.load_checkpoint(key)
        if _completed(checkpoint):
            return BackfillSummary(0, 0, 0)
        page = _next_page(checkpoint)
        summary = BackfillSummary(0, 0, 0)

        while True:
            payload = self._client.get(
                f"{self._repository_path}/deployments",
                {"per_page": self._page_size, "page": page},
            )
            nodes = _required_array_payload(payload, "deployments response")
            records: list[BackfillRecord] = []
            for node in nodes:
                created_at = _aware_datetime(node, "created_at")
                if not _in_window(created_at, window_start, window_end):
                    continue
                deployment_id = _required_positive_int(node, "id")
                summary = summary.plus(
                    self._run_deployment_statuses(
                        deployment_id,
                        window_start=window_start,
                        window_end=window_end,
                    )
                )
                records.append(self._deployment_record(node, created_at))

            completed = len(nodes) < self._page_size
            outcome = self._persist_page(key, records, page=page, completed=completed)
            summary = summary.plus(_summary(outcome))
            if completed:
                return summary
            page += 1

    def _run_deployment_statuses(
        self,
        deployment_id: int,
        *,
        window_start: datetime,
        window_end: datetime,
    ) -> BackfillSummary:
        key = self._run_key(
            "deployment_statuses",
            str(deployment_id),
            window_start,
            window_end,
        )
        checkpoint = self._storage.load_checkpoint(key)
        if _completed(checkpoint):
            return BackfillSummary(0, 0, 0)
        page = _next_page(checkpoint)
        summary = BackfillSummary(0, 0, 0)

        while True:
            payload = self._client.get(
                f"{self._repository_path}/deployments/{deployment_id}/statuses",
                {"per_page": self._page_size, "page": page},
            )
            nodes = _required_array_payload(payload, "deployment-status response")
            records = [self._deployment_status_record(deployment_id, node) for node in nodes]
            completed = len(nodes) < self._page_size
            outcome = self._persist_page(key, records, page=page, completed=completed)
            summary = summary.plus(_summary(outcome))
            if completed:
                return summary
            page += 1

    def _persist_page(
        self,
        key: BackfillRunKey,
        records: Sequence[BackfillRecord],
        *,
        page: int,
        completed: bool,
    ) -> PageWriteOutcome:
        return self._storage.persist_page(
            key,
            records,
            next_cursor=None if completed else str(page + 1),
            completed=completed,
        )

    def _workflow_run_record(self, node: dict[str, Any]) -> BackfillRecord:
        run_id = _required_positive_int(node, "id")
        attempt = _required_positive_int(node, "run_attempt")
        created_at = _aware_datetime(node, "created_at")
        updated_at = _aware_datetime(node, "updated_at")
        status = _required_string(node, "status").lower()
        self._validate_workflow_repository(node)
        action = "completed" if status == "completed" else status
        return BackfillRecord(
            source_record_key=(
                f"github_rest:workflow_run:{run_id}:{attempt}:{updated_at.isoformat()}"
            ),
            event_name="workflow_run",
            action=action,
            repository_id=self._repository_id,
            installation_id=self._installation_id,
            occurred_at=created_at,
            payload=self._payload("workflow_run", node),
        )

    def _deployment_record(
        self,
        node: dict[str, Any],
        created_at: datetime,
    ) -> BackfillRecord:
        deployment_id = _required_positive_int(node, "id")
        return BackfillRecord(
            source_record_key=f"github_rest:deployment:{deployment_id}",
            event_name="deployment",
            action="created",
            repository_id=self._repository_id,
            installation_id=self._installation_id,
            occurred_at=created_at,
            payload=self._payload("deployment", node),
        )

    def _deployment_status_record(
        self,
        deployment_id: int,
        node: dict[str, Any],
    ) -> BackfillRecord:
        status_id = _required_positive_int(node, "id")
        state = _required_string(node, "state").lower()
        occurred_at = _aware_datetime(node, "created_at")
        return BackfillRecord(
            source_record_key=f"github_rest:deployment_status:{status_id}",
            event_name="deployment_status",
            action=state,
            repository_id=self._repository_id,
            installation_id=self._installation_id,
            occurred_at=occurred_at,
            payload={
                **self._payload("deployment_status", node),
                "deployment_id": deployment_id,
            },
        )

    def _validate_workflow_repository(self, node: Mapping[str, Any]) -> None:
        repository = _required_object(node, "repository")
        if repository.get("id") != self._repository_id:
            raise RestDataError("workflow-run repository id does not match configuration")
        if repository.get("full_name") != self._repository:
            raise RestDataError("workflow-run repository name does not match configuration")

    def _payload(self, resource: str, node: dict[str, Any]) -> dict[str, Any]:
        return {
            "resource": resource,
            "repository": {
                "id": self._repository_id,
                "full_name": self._repository,
            },
            resource: node,
        }

    def _run_key(
        self,
        resource: str,
        scope: str,
        window_start: datetime,
        window_end: datetime,
    ) -> BackfillRunKey:
        return BackfillRunKey(
            repository_id=self._repository_id,
            resource=resource,
            scope=scope,
            window_start=window_start,
            window_end=window_end,
        )


def _summary(outcome: PageWriteOutcome) -> BackfillSummary:
    return BackfillSummary(
        pages_persisted=1,
        records_inserted=outcome.inserted,
        duplicates_absorbed=outcome.duplicates,
    )


def _completed(checkpoint: BackfillCheckpoint | None) -> bool:
    return checkpoint is not None and checkpoint.status is CheckpointStatus.COMPLETED


def _next_page(checkpoint: BackfillCheckpoint | None) -> int:
    if checkpoint is None:
        return 1
    if checkpoint.cursor is None:
        raise RestDataError("in-progress REST checkpoint is missing its next page")
    try:
        page = int(checkpoint.cursor)
    except ValueError as error:
        raise RestDataError("REST checkpoint cursor is not a page number") from error
    if page <= 0:
        raise RestDataError("REST checkpoint page must be positive")
    return page


def _required_object_payload(
    payload: dict[str, Any] | list[Any],
    name: str,
) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise RestDataError(f"{name} must be a JSON object")
    return payload


def _required_array_payload(
    payload: dict[str, Any] | list[Any],
    name: str,
) -> list[dict[str, Any]]:
    if not isinstance(payload, list) or any(not isinstance(node, dict) for node in payload):
        raise RestDataError(f"{name} must be an array of objects")
    return cast(list[dict[str, Any]], payload)


def _required_object_list(container: Mapping[str, Any], name: str) -> list[dict[str, Any]]:
    value = container.get(name)
    if not isinstance(value, list) or any(not isinstance(node, dict) for node in value):
        raise RestDataError(f"GitHub REST response has invalid {name}")
    return cast(list[dict[str, Any]], value)


def _required_object(container: Mapping[str, Any], name: str) -> dict[str, Any]:
    value = container.get(name)
    if not isinstance(value, dict):
        raise RestDataError(f"GitHub REST response is missing {name}")
    return cast(dict[str, Any], value)


def _required_string(container: Mapping[str, Any], name: str) -> str:
    value = container.get(name)
    if not isinstance(value, str) or not value:
        raise RestDataError(f"GitHub REST response is missing {name}")
    return value


def _required_nonnegative_int(container: Mapping[str, Any], name: str) -> int:
    value = container.get(name)
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise RestDataError(f"GitHub REST response has invalid {name}")
    return value


def _required_positive_int(container: Mapping[str, Any], name: str) -> int:
    value = _required_nonnegative_int(container, name)
    if value == 0:
        raise RestDataError(f"GitHub REST response has invalid {name}")
    return value


def _aware_datetime(container: Mapping[str, Any], name: str) -> datetime:
    value = container.get(name)
    try:
        return _AWARE_DATETIME.validate_python(value)
    except ValidationError as error:
        raise RestDataError(f"GitHub REST response has invalid {name}") from error


def _in_window(value: datetime, start: datetime, end: datetime) -> bool:
    return start <= value < end


def _github_timestamp(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")
