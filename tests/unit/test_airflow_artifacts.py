"""Static checks for the pinned Airflow image and thin orchestration DAG."""

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_backfill_dag_is_valid_python_and_delegates_to_the_package() -> None:
    dag_path = ROOT / "airflow" / "dags" / "github_backfill.py"
    source = dag_path.read_text(encoding="utf-8")

    ast.parse(source, filename=str(dag_path))
    assert "from airflow.sdk import DAG, Param, task" in source
    assert 'dag_id="github_backfill"' in source
    assert "schedule=None" in source
    assert "max_active_runs=1" in source
    assert '"retries": 3' in source
    assert "github_analytics.backfill.cli import main" in source
    assert "GitHubGraphQLClient" not in source
    assert "GitHubRestClient" not in source
    assert "PostgresBackfillStorage" not in source


def test_airflow_image_is_pinned_and_checks_its_dependency_set() -> None:
    dockerfile = (ROOT / "airflow" / "Dockerfile").read_text(encoding="utf-8")

    assert "ARG AIRFLOW_VERSION=3.3.0" in dockerfile
    assert "ARG PYTHON_VERSION=3.12" in dockerfile
    assert "FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}" in dockerfile
    assert '"apache-airflow==${AIRFLOW_VERSION}"' in dockerfile
    assert "--requirement /opt/airflow/requirements.txt" in dockerfile
    assert "ENV PYTHONPATH=/opt/airflow/application/src" in dockerfile
    assert "pip check" in dockerfile
    assert "airflow/check_dag.py /opt/airflow/check_dag.py" in dockerfile
    assert "COPY --chown=airflow:root airflow/dags/" in dockerfile
    assert "dbt/github_analytics/ /opt/airflow/dbt/github_analytics/" in dockerfile
    assert "run_analytics_refresh_smoke.py" in dockerfile

    requirements = (ROOT / "airflow" / "requirements.txt").read_text(encoding="utf-8")
    assert "fastapi" not in requirements
    assert "confluent-kafka" not in requirements
    assert "psycopg[binary,pool]==3.3.4" in requirements
    assert "dbt-core==1.12.0" in requirements
    assert "dbt-postgres==1.11.0" in requirements


def test_build_context_excludes_secrets_and_local_artifacts() -> None:
    ignored = set((ROOT / ".dockerignore").read_text(encoding="utf-8").splitlines())

    assert {".git", ".env", ".venv", ".artifacts", "tests"} <= ignored
    assert "dbt" not in ignored
    assert "dbt/**/target" in ignored


def test_dag_check_uses_an_ephemeral_metadata_database_without_examples() -> None:
    makefile = (ROOT / "Makefile").read_text(encoding="utf-8")
    check_path = ROOT / "airflow" / "check_dag.py"
    check_source = check_path.read_text(encoding="utf-8")

    assert "AIRFLOW__CORE__LOAD_EXAMPLES=False" in makefile
    assert "python /opt/airflow/check_dag.py" in makefile
    ast.parse(check_source, filename=str(check_path))
    assert '["airflow", "db", "migrate"]' in check_source
    assert '["airflow", "dags", "reserialize"]' in check_source
    assert '"list-import-errors"' in check_source
    assert '["analytics_refresh", "github_backfill"]' in check_source


def test_analytics_refresh_dag_is_hourly_ordered_and_delegates() -> None:
    dag_path = ROOT / "airflow" / "dags" / "analytics_refresh.py"
    source = dag_path.read_text(encoding="utf-8")

    ast.parse(source, filename=str(dag_path))
    assert "from airflow.sdk import DAG, get_current_context, task" in source
    assert 'dag_id="analytics_refresh"' in source
    assert 'schedule="@hourly"' in source
    assert "catchup=False" in source
    assert "max_active_runs=1" in source
    assert '"retries": 2' in source
    assert "source_check >> build >> success" in source
    assert "on_failure_callback=_persist_dag_failure" in source
    assert 'dag_run_id=context["run_id"]' in source
    assert "github_analytics.analytics_refresh.service" in source
    assert "subprocess" not in source
    assert "psycopg" not in source


def test_analytics_smoke_script_uses_supported_argument_list_cli() -> None:
    path = ROOT / "airflow" / "run_analytics_refresh_smoke.py"
    source = path.read_text(encoding="utf-8")

    ast.parse(source, filename=str(path))
    assert '["airflow", "dags", "test", "analytics_refresh", logical_date]' in source
    assert "shell=True" not in source
