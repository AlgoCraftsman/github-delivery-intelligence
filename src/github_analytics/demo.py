"""Deterministic local reviewer demo orchestration."""

import json
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Protocol

_COMPOSE_FILE = "infra/docker-compose.yml"
_DBT_PROJECT = "dbt/github_analytics"
_FIXTURE_PATH = "dbt/github_analytics/fixtures/load_github_events.sql"
_SUMMARY_QUERY = """
WITH latest_reporting_date AS (
    SELECT MAX(date_day) AS date_day
    FROM analytics_marts.fct_delivery_performance_daily
)
SELECT
    metrics.repository_full_name,
    metrics.date_day,
    metrics.metric_name,
    metrics.metric_value,
    metrics.measurement_status,
    metrics.coverage_numerator,
    metrics.coverage_denominator,
    metrics.coverage_ratio,
    COALESCE(metrics.exclusion_reason, '') AS exclusion_reason
FROM analytics_marts.fct_delivery_performance_daily AS metrics
INNER JOIN latest_reporting_date USING (date_day)
ORDER BY metrics.repository_full_name, metrics.metric_name;
""".strip()


class CommandRunner(Protocol):
    """Execute one checked command, optionally providing standard input."""

    def __call__(
        self,
        command: Sequence[str],
        *,
        cwd: Path,
        stdin: str | None = None,
    ) -> None: ...


def _run_command(
    command: Sequence[str],
    *,
    cwd: Path,
    stdin: str | None = None,
) -> None:
    subprocess.run(
        command,
        cwd=cwd,
        input=stdin,
        text=True,
        check=True,
    )


def run_demo(root: Path, *, runner: CommandRunner = _run_command) -> None:
    """Load isolated fixtures, prove dbt contracts, and print metric evidence."""

    fixture_sql = (root / _FIXTURE_PATH).read_text(encoding="utf-8")
    dbt_common = [
        "--project-dir",
        _DBT_PROJECT,
        "--profiles-dir",
        _DBT_PROJECT,
    ]
    fixture_source = json.dumps(
        {"github_events_identifier": "github_events_fixture"},
        separators=(",", ":"),
    )
    fixture_build = json.dumps(
        {
            "github_events_identifier": "github_events_fixture",
            "fixture_validation": True,
        },
        separators=(",", ":"),
    )

    runner(
        [
            "docker",
            "compose",
            "-f",
            _COMPOSE_FILE,
            "exec",
            "-T",
            "postgres",
            "psql",
            "--username",
            "github_analytics",
            "--dbname",
            "github_analytics",
            "--set",
            "ON_ERROR_STOP=1",
        ],
        cwd=root,
        stdin=fixture_sql,
    )
    runner(
        ["dbt", "source", "freshness", *dbt_common, "--vars", fixture_source],
        cwd=root,
    )
    runner(
        ["dbt", "build", *dbt_common, "--vars", fixture_build],
        cwd=root,
    )
    runner(
        [sys.executable, "tools/validate_dashboard_sql.py"],
        cwd=root,
    )
    runner(
        [
            "docker",
            "compose",
            "-f",
            _COMPOSE_FILE,
            "exec",
            "-T",
            "postgres",
            "psql",
            "--username",
            "github_analytics",
            "--dbname",
            "github_analytics",
            "--set",
            "ON_ERROR_STOP=1",
            "--command",
            _SUMMARY_QUERY,
        ],
        cwd=root,
    )


def main() -> int:
    """Run the deterministic demo from the repository root."""

    run_demo(Path.cwd())
    print("Deterministic demo completed successfully.")
    return 0


if __name__ == "__main__":  # pragma: no cover - exercised through ``main``
    raise SystemExit(main())
