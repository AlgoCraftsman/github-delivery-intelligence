"""Tests for deterministic reviewer demo orchestration."""

import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from github_analytics import demo


class RecordingRunner:
    def __init__(self) -> None:
        self.calls: list[tuple[list[str], Path, str | None]] = []

    def __call__(
        self,
        command: Sequence[str],
        *,
        cwd: Path,
        stdin: str | None = None,
    ) -> None:
        self.calls.append((list(command), cwd, stdin))


def test_run_demo_uses_isolated_fixture_and_explicit_commands(tmp_path: Path) -> None:
    fixture = tmp_path / "dbt/github_analytics/fixtures/load_github_events.sql"
    fixture.parent.mkdir(parents=True)
    fixture.write_text("SELECT 'synthetic fixture';\n", encoding="utf-8")
    runner = RecordingRunner()

    demo.run_demo(tmp_path, runner=runner)

    assert len(runner.calls) == 5
    load_command, load_root, load_input = runner.calls[0]
    assert load_command[:7] == [
        "docker",
        "compose",
        "-f",
        "infra/docker-compose.yml",
        "exec",
        "-T",
        "postgres",
    ]
    assert load_root == tmp_path
    assert load_input == "SELECT 'synthetic fixture';\n"
    assert runner.calls[1][0] == [
        "dbt",
        "source",
        "freshness",
        "--project-dir",
        "dbt/github_analytics",
        "--profiles-dir",
        "dbt/github_analytics",
        "--vars",
        '{"github_events_identifier":"github_events_fixture"}',
    ]
    assert runner.calls[2][0][-2:] == [
        "--vars",
        '{"github_events_identifier":"github_events_fixture","fixture_validation":true}',
    ]
    assert runner.calls[3][0] == [sys.executable, "tools/validate_dashboard_sql.py"]
    assert "analytics_marts.fct_delivery_performance_daily" in runner.calls[4][0][-1]
    assert all(call[1] == tmp_path for call in runner.calls)


def test_run_command_checks_the_subprocess(monkeypatch: Any, tmp_path: Path) -> None:
    observed: dict[str, Any] = {}

    def fake_run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        observed.update(command=command, **kwargs)
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(subprocess, "run", fake_run)

    demo._run_command(["example", "argument"], cwd=tmp_path, stdin="fixture")

    assert observed == {
        "command": ["example", "argument"],
        "cwd": tmp_path,
        "input": "fixture",
        "text": True,
        "check": True,
    }


def test_main_reports_completion(monkeypatch: Any, capsys: Any) -> None:
    observed: list[Path] = []
    monkeypatch.setattr(demo, "run_demo", observed.append)

    assert demo.main() == 0
    assert observed == [Path.cwd()]
    assert capsys.readouterr().out == "Deterministic demo completed successfully.\n"
