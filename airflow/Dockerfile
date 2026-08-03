ARG AIRFLOW_VERSION=3.3.0
ARG PYTHON_VERSION=3.12

FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

ARG AIRFLOW_VERSION

USER airflow
WORKDIR /opt/airflow

COPY --chown=airflow:root airflow/requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir \
        "apache-airflow==${AIRFLOW_VERSION}" \
        --requirement /opt/airflow/requirements.txt \
    && pip check

COPY --chown=airflow:root src/ /opt/airflow/application/src/
COPY --chown=airflow:root dbt/github_analytics/ /opt/airflow/dbt/github_analytics/

ENV PYTHONPATH=/opt/airflow/application/src

COPY --chown=airflow:root airflow/check_dag.py /opt/airflow/check_dag.py
COPY --chown=airflow:root airflow/run_analytics_refresh_smoke.py /opt/airflow/run_analytics_refresh_smoke.py
COPY --chown=airflow:root airflow/dags/ /opt/airflow/dags/
