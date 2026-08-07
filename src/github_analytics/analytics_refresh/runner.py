"""Safe dbt subprocess execution and bounded artifact parsing."""

import json
import re
import subprocess
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from github_analytics.analytics_refresh.config import AnalyticsRefreshSettings
from github_analytics.analytics_refresh.models import DbtResultSummary, RefreshError

CommandExecutor = Callable[[Sequence[str], float], subprocess.CompletedProcess[str]]

_SECRET_PATTERN = re.compile(
    r"(?i)(password|token|secret|signature|authorization)(\s*[=:]\s*)([^\s,;]+)"
)
_URI_PASSWORD_PATTERN = re.compile(r"(?i)([a-z][a-z0-9+.-]*://[^:/\s]+:)([^@\s]+)(@)")


@dataclass(frozen=True, slots=True)
class DbtRunner:
    """Build dbt argument lists and summarize explicit target artifacts."""

    settings: AnalyticsRefreshSettings
    executor: CommandExecutor

    def freshness_command(self) -> list[str]:
        """Return the explicit dbt source-freshness argument list."""

        return self._base_command("source", "freshness")

    def build_command(self) -> list[str]:
        """Return the explicit contracted dbt build argument list."""

        variables: dict[str, Any] = {
            "github_events_identifier": self.settings.source_identifier,
        }
        if self.settings.source_identifier == "github_events_fixture":
            variables["fixture_validation"] = True
        return self._base_command("build", variables=variables)

    def run_freshness(self) -> DbtResultSummary:
        """Run source freshness and require a valid sources.json artifact."""

        return self._run(
            self.freshness_command(),
            artifact_name="sources.json",
            category="source_freshness_failed",
        )

    def run_build(self) -> DbtResultSummary:
        """Run dbt build and require a valid run_results.json artifact."""

        return self._run(
            self.build_command(),
            artifact_name="run_results.json",
            category="dbt_build_failed",
        )

    def _base_command(
        self,
        *subcommand: str,
        variables: dict[str, Any] | None = None,
    ) -> list[str]:
        dbt_variables = variables or {
            "github_events_identifier": self.settings.source_identifier,
        }
        return [
            self.settings.dbt_executable,
            *subcommand,
            "--project-dir",
            str(self.settings.dbt_project_dir),
            "--profiles-dir",
            str(self.settings.dbt_profiles_dir),
            "--target-path",
            str(self.settings.dbt_target_dir),
            "--vars",
            json.dumps(dbt_variables, sort_keys=True, separators=(",", ":")),
        ]

    def _run(
        self,
        command: list[str],
        *,
        artifact_name: str,
        category: str,
    ) -> DbtResultSummary:
        artifact_path = self.settings.dbt_target_dir / artifact_name
        artifact_path.unlink(missing_ok=True)
        try:
            result = self.executor(command, self.settings.dbt_command_timeout_seconds)
        except (OSError, subprocess.SubprocessError) as error:
            raise RefreshError(category, sanitize_error(str(error))) from error

        summary: DbtResultSummary | None = None
        try:
            summary = parse_dbt_artifact(
                artifact_path,
                max_failures=self.settings.artifact_max_failures,
                max_message_chars=self.settings.artifact_message_max_chars,
            )
        except RefreshError:
            if result.returncode == 0:
                raise

        if result.returncode != 0:
            message = sanitize_error(
                result.stderr or result.stdout or f"dbt exited {result.returncode}"
            )
            raise RefreshError(category, message, summary=summary)
        return cast(DbtResultSummary, summary)


def default_executor(
    command: Sequence[str], timeout_seconds: float
) -> subprocess.CompletedProcess[str]:
    """Execute one argument-list command without a shell or inherited stdin."""

    return subprocess.run(
        list(command),
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
    )


def parse_dbt_artifact(
    path: Path,
    *,
    max_failures: int,
    max_message_chars: int,
) -> DbtResultSummary:
    """Parse only bounded metadata, counts, and sanitized failing-node messages."""

    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as error:
        raise RefreshError("artifact_missing", f"dbt artifact is missing: {path.name}") from error
    except (OSError, json.JSONDecodeError) as error:
        raise RefreshError("artifact_invalid", f"dbt artifact is invalid: {path.name}") from error
    if not isinstance(document, dict) or not isinstance(document.get("results"), list):
        raise RefreshError("artifact_invalid", f"dbt artifact has no result list: {path.name}")

    metadata = document.get("metadata")
    invocation_id = metadata.get("invocation_id") if isinstance(metadata, dict) else None
    if invocation_id is not None and not isinstance(invocation_id, str):
        invocation_id = None

    counts = {"succeeded": 0, "failed": 0, "skipped": 0, "warnings": 0, "errors": 0}
    failures: list[dict[str, str]] = []
    for raw_result in document["results"]:
        if not isinstance(raw_result, dict):
            raise RefreshError("artifact_invalid", "dbt artifact contains a non-object result")
        status = str(raw_result.get("status", "error")).lower()
        bucket = _status_bucket(status)
        counts[bucket] += 1
        if bucket in {"failed", "warnings", "errors"} and len(failures) < max_failures:
            unique_id = str(raw_result.get("unique_id", "unknown"))[:200]
            message = sanitize_error(str(raw_result.get("message") or status))[:max_message_chars]
            failures.append({"unique_id": unique_id, "status": status[:50], "message": message})

    artifact = {
        "artifact": path.name,
        "invocation_id": invocation_id,
        "counts": counts,
        "failures": failures,
        "results_truncated": len(failures) >= max_failures,
    }
    return DbtResultSummary(invocation_id=invocation_id, artifact=artifact, **counts)


def sanitize_error(message: str, *, max_chars: int = 1000) -> str:
    """Redact common secret assignments, flatten whitespace, and bound storage."""

    redacted = _URI_PASSWORD_PATTERN.sub(r"\1[REDACTED]\3", message)
    redacted = _SECRET_PATTERN.sub(r"\1\2[REDACTED]", redacted)
    flattened = " ".join(redacted.split())
    return (flattened or "unspecified refresh failure")[:max_chars]


def _status_bucket(status: str) -> str:
    if status in {"success", "pass"}:
        return "succeeded"
    if status in {"fail", "failed"}:
        return "failed"
    if status in {"skip", "skipped"}:
        return "skipped"
    if status in {"warn", "warning"}:
        return "warnings"
    return "errors"
