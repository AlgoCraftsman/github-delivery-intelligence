"""Hourly analytics refresh with durable application-owned run state."""

from datetime import UTC, datetime, timedelta
from typing import Any

from airflow.sdk import DAG, get_current_context, task


def _identity_from_context(context: dict[str, Any]) -> Any:
    """Build the package identity without embedding refresh behavior in the DAG."""

    from github_analytics.analytics_refresh.models import RefreshRunIdentity

    logical_date = context["logical_date"]
    return RefreshRunIdentity(
        dag_id=context["dag"].dag_id,
        dag_run_id=context["run_id"],
        logical_date=logical_date,
        data_interval_start=context.get("data_interval_start") or logical_date,
        data_interval_end=context.get("data_interval_end") or logical_date,
    )


def _persist_dag_failure(context: dict[str, Any]) -> None:
    """Ensure an exhausted task/DAG failure leaves a terminal durable row."""

    from github_analytics.analytics_refresh.service import run_failure_step

    error = context.get("exception")
    run_failure_step(
        _identity_from_context(context),
        error if isinstance(error, BaseException) else RuntimeError("Airflow DAG run failed"),
    )


with DAG(
    dag_id="analytics_refresh",
    description="Check raw freshness, build contracted marts, and persist run health",
    schedule="@hourly",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    },
    on_failure_callback=_persist_dag_failure,
    tags=["github", "analytics", "dbt", "pipeline-health"],
) as dag:

    @task(task_id="record_and_check_source")
    def record_and_check_source() -> None:
        """Delegate durable start, source watermark, and freshness to the package."""

        from github_analytics.analytics_refresh.service import run_source_step

        context = get_current_context()
        run_source_step(_identity_from_context(context))

    @task(task_id="dbt_build")
    def dbt_build() -> None:
        """Delegate the contracted build and artifact capture to the package."""

        from github_analytics.analytics_refresh.service import run_build_step

        context = get_current_context()
        run_build_step(_identity_from_context(context))

    @task(task_id="persist_success")
    def persist_success() -> None:
        """Persist terminal success only after the build task completes."""

        from github_analytics.analytics_refresh.service import run_success_step

        context = get_current_context()
        run_success_step(_identity_from_context(context))

    source_check = record_and_check_source()
    build = dbt_build()
    success = persist_success()
    source_check >> build >> success
