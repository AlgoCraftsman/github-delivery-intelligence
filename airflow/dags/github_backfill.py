"""Manually triggered, restartable GitHub history backfill."""

from datetime import UTC, datetime, timedelta

from airflow.sdk import DAG, Param, task

with DAG(
    dag_id="github_backfill",
    description="Backfill one bounded GitHub history window with durable checkpoints",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=1),
    },
    params={
        "window_start": Param(
            type="string",
            format="date-time",
            title="Window start",
            description="Inclusive ISO 8601 timestamp with a UTC offset.",
        ),
        "window_end": Param(
            type="string",
            format="date-time",
            title="Window end",
            description="Exclusive ISO 8601 timestamp with a UTC offset.",
        ),
    },
    tags=["github", "backfill"],
) as dag:

    @task(task_id="backfill_history")
    def backfill_history(window_start: str, window_end: str) -> None:
        """Invoke the tested package; retries resume its PostgreSQL checkpoints."""

        from github_analytics.backfill.cli import main

        exit_code = main(["--start", window_start, "--end", window_end])
        if exit_code != 0:
            raise RuntimeError(f"github-backfill exited with status {exit_code}")

    backfill_history(
        "{{ params.window_start }}",
        "{{ params.window_end }}",
    )
