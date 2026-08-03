"""Tests for shell-free dbt commands and bounded artifact parsing."""

import json
import subprocess
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pytest

from github_analytics.analytics_refresh.config import AnalyticsRefreshSettings
from github_analytics.analytics_refresh.models import RefreshError
from github_analytics.analytics_refresh.runner import (
    DbtRunner,
    default_executor,
    parse_dbt_artifact,
    sanitize_error,
)


def _settings(tmp_path: Path, *, fixture: bool = False) -> AnalyticsRefreshSettings:
    project = tmp_path / "project"
    profiles = tmp_path / "profiles"
    target = tmp_path / "target"
    project.mkdir()
    profiles.mkdir()
    target.mkdir()
    return AnalyticsRefreshSettings(
        source_identifier="github_events_fixture" if fixture else "github_events",
        dbt_project_dir=project,
        dbt_profiles_dir=profiles,
        dbt_target_dir=target,
    )


def _artifact(invocation_id: Any = "inv-1") -> dict[str, Any]:
    return {
        "metadata": {"invocation_id": invocation_id},
        "results": [
            {"unique_id": "model.one", "status": "success"},
            {"unique_id": "test.two", "status": "pass"},
            {"unique_id": "test.three", "status": "warn", "message": "token=visible"},
            {"unique_id": "test.four", "status": "fail", "message": "bad row"},
            {"unique_id": "model.five", "status": "skipped"},
            {"unique_id": "model.six", "status": "runtime error"},
        ],
    }


def test_command_construction_is_explicit_and_fixture_aware(tmp_path: Path) -> None:
    settings = _settings(tmp_path, fixture=True)
    runner = DbtRunner(settings, lambda command, timeout: subprocess.CompletedProcess(command, 0))

    freshness = runner.freshness_command()
    build = runner.build_command()

    assert freshness[:3] == ["dbt", "source", "freshness"]
    assert build[:2] == ["dbt", "build"]
    assert "--project-dir" in build and "--profiles-dir" in build and "--target-path" in build
    variables = json.loads(build[build.index("--vars") + 1])
    assert variables == {
        "fixture_validation": True,
        "github_events_identifier": "github_events_fixture",
    }
    assert all(isinstance(argument, str) for argument in build)


@pytest.mark.parametrize(
    ("method", "artifact_name"),
    [("run_freshness", "sources.json"), ("run_build", "run_results.json")],
)
def test_successful_execution_parses_bounded_artifact(
    tmp_path: Path, method: str, artifact_name: str
) -> None:
    settings = _settings(tmp_path)
    calls: list[tuple[list[str], float]] = []

    def executor(command: Sequence[str], timeout: float) -> subprocess.CompletedProcess[str]:
        calls.append((list(command), timeout))
        (settings.dbt_target_dir / artifact_name).write_text(
            json.dumps(_artifact()), encoding="utf-8"
        )
        return subprocess.CompletedProcess(list(command), 0, stdout="safe", stderr="")

    summary = getattr(DbtRunner(settings, executor), method)()

    assert calls[0][1] == 900
    assert summary.invocation_id == "inv-1"
    assert (
        summary.succeeded,
        summary.failed,
        summary.skipped,
        summary.warnings,
        summary.errors,
    ) == (
        2,
        1,
        1,
        1,
        1,
    )
    assert summary.artifact["failures"][0]["message"] == "token=[REDACTED]"


def test_nonzero_dbt_exit_retains_artifact_summary_and_sanitizes_output(tmp_path: Path) -> None:
    settings = _settings(tmp_path)

    def executor(command: Sequence[str], timeout: float) -> subprocess.CompletedProcess[str]:
        del timeout
        (settings.dbt_target_dir / "run_results.json").write_text(
            json.dumps(_artifact()), encoding="utf-8"
        )
        return subprocess.CompletedProcess(
            list(command), 1, stdout="", stderr="password=hunter2 failed"
        )

    with pytest.raises(RefreshError, match=r"password=\[REDACTED\]") as captured:
        DbtRunner(settings, executor).run_build()

    assert captured.value.category == "dbt_build_failed"
    assert captured.value.summary is not None


@pytest.mark.parametrize("payload", [None, "not-json", {"metadata": {}}])
def test_success_requires_a_valid_artifact(tmp_path: Path, payload: Any) -> None:
    settings = _settings(tmp_path)

    def executor(command: Sequence[str], timeout: float) -> subprocess.CompletedProcess[str]:
        del timeout
        if payload is not None:
            content = payload if isinstance(payload, str) else json.dumps(payload)
            (settings.dbt_target_dir / "sources.json").write_text(content, encoding="utf-8")
        return subprocess.CompletedProcess(list(command), 0)

    with pytest.raises(RefreshError) as captured:
        DbtRunner(settings, executor).run_freshness()

    assert captured.value.category in {"artifact_missing", "artifact_invalid"}


def test_nonzero_exit_without_artifact_uses_process_failure(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    runner = DbtRunner(
        settings,
        lambda command, timeout: subprocess.CompletedProcess(command, 2, stdout="dbt stopped"),
    )

    with pytest.raises(RefreshError, match="dbt stopped") as captured:
        runner.run_freshness()

    assert captured.value.category == "source_freshness_failed"


def test_executor_exception_is_categorized(tmp_path: Path) -> None:
    settings = _settings(tmp_path)

    def executor(command: Sequence[str], timeout: float) -> subprocess.CompletedProcess[str]:
        del command, timeout
        raise subprocess.TimeoutExpired("dbt", 1)

    with pytest.raises(RefreshError, match="timed out"):
        DbtRunner(settings, executor).run_build()


def test_parser_rejects_non_object_results_and_handles_metadata(tmp_path: Path) -> None:
    path = tmp_path / "run_results.json"
    path.write_text(
        json.dumps({"metadata": {"invocation_id": 4}, "results": [None]}), encoding="utf-8"
    )

    with pytest.raises(RefreshError, match="non-object"):
        parse_dbt_artifact(path, max_failures=2, max_message_chars=100)

    path.write_text(json.dumps({"metadata": {"invocation_id": 4}, "results": []}), encoding="utf-8")
    summary = parse_dbt_artifact(path, max_failures=2, max_message_chars=100)
    assert summary.invocation_id is None


def test_parser_bounds_failure_details_and_sanitizer_flattens_messages(tmp_path: Path) -> None:
    path = tmp_path / "run_results.json"
    document = {"results": [{"unique_id": "x" * 300, "status": "error"}] * 3}
    path.write_text(json.dumps(document), encoding="utf-8")

    summary = parse_dbt_artifact(path, max_failures=2, max_message_chars=100)

    assert len(summary.artifact["failures"]) == 2
    assert len(summary.artifact["failures"][0]["unique_id"]) == 200
    assert summary.artifact["results_truncated"] is True
    assert sanitize_error("\n token : abc \n") == "token : [REDACTED]"
    assert (
        sanitize_error("postgresql://reader:plaintext@database:5432/analytics")
        == "postgresql://reader:[REDACTED]@database:5432/analytics"
    )
    assert sanitize_error("   ") == "unspecified refresh failure"
    assert len(sanitize_error("x" * 2000)) == 1000


def test_default_executor_uses_argument_list_without_shell(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def fake_run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        captured.update({"command": command, **kwargs})
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(subprocess, "run", fake_run)

    default_executor(["dbt", "build"], 30)

    assert captured == {
        "command": ["dbt", "build"],
        "check": False,
        "capture_output": True,
        "text": True,
        "timeout": 30,
    }
    assert "shell" not in captured
