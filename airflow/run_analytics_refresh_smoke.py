"""Execute the real Airflow 3.3 test path for the fixture-backed refresh DAG."""

import os
import subprocess


def main() -> int:
    """Migrate ephemeral metadata and execute one supported `dags test` run."""

    subprocess.run(["airflow", "db", "migrate"], check=True)
    subprocess.run(["airflow", "dags", "reserialize"], check=True)
    logical_date = os.environ.get("ANALYTICS_REFRESH_SMOKE_LOGICAL_DATE", "2026-01-14T13:00:00Z")
    subprocess.run(
        ["airflow", "dags", "test", "analytics_refresh", logical_date],
        check=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
