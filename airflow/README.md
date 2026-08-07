# Airflow

The pinned Apache Airflow 3.3.0 / Python 3.12 image contains two thin DAGs:

- `github_backfill`, a manual bounded history workflow;
- `analytics_refresh`, an hourly UTC source-freshness and contracted dbt build.

Business logic remains in tested application packages rather than the DAG files.

Build the image and ask Airflow to report DAG import errors:

```bash
make airflow-image
make airflow-dag-check
```

The image copies local package source and the dbt project to
`/opt/airflow/dbt/github_analytics`, installs the pinned backfill and dbt runtime beside
the exact base-image Airflow version, then runs `pip check`. Webhook and Kafka consumer
dependencies stay outside the orchestration image. CI checks imports and executes the
fixture-backed refresh through Airflow's real `dags test` command.

## Runtime contract

Backfill runs inject the `BACKFILL_*` variables documented in `.env.example`. Use a
short-lived GitHub App installation token with repository
contents/metadata, Actions read, and Deployments read permissions. Do not bake tokens
or database credentials into the image.

When Airflow runs in a container while PostgreSQL runs through the repository's Docker
Compose stack, `localhost` refers to the Airflow container. Put both services on a
shared Docker network and use the PostgreSQL service name, or use
`host.docker.internal:55432` for an intentional Docker Desktop host connection.

Analytics refresh runs inject `ANALYTICS_REFRESH_DATABASE_URL`, source schema and
identifier, command timeout, and explicit dbt project/profile/target directories.
`DBT_POSTGRES_*` configures the dbt profile. Production defaults read
`raw.github_events`; deterministic validation overrides only the identifier to
`github_events_fixture`. The image paths are:

- project and profiles: `/opt/airflow/dbt/github_analytics`;
- artifacts: `/opt/airflow/dbt/github_analytics/target/analytics_refresh`.

Trigger `github_backfill` manually from the Airflow UI and provide:

- `window_start`: inclusive, offset-aware ISO 8601 timestamp.
- `window_end`: exclusive, offset-aware ISO 8601 timestamp.

Only one DAG run is active at a time. The task retries three times at one-minute
intervals. Every retry invokes the same application command; the PostgreSQL
`raw.backfill_checkpoints` rows resume GraphQL cursors and REST page numbers after the
last committed API page.

## Hourly analytics refresh

`analytics_refresh` has `schedule="@hourly"`, a fixed UTC start date,
`catchup=False`, `max_active_runs=1`, and two retries at two-minute intervals. Its
explicit dependency order is:

```text
record/check source -> dbt build -> persist terminal success
```

Airflow 3.3's default cron-trigger timetable may report equal data-interval start and
end values. The ledger accepts that supported zero-width trigger interval but rejects
reversed intervals.

The first task upserts `ops.analytics_refresh_runs` to `running`, reads maximum
`ingested_at`, records its nonnegative age at that wall-clock instant, and runs dbt
source freshness. The second task resumes the same row if an Airflow retry follows a
failed attempt, then runs `dbt build`. The final task marks success. Step exceptions and
the DAG failure callback persist bounded sanitized failure state and re-raise.

The ledger stores summarized `sources.json` and `run_results.json` content: invocation
ID, result counts, and at most ten bounded failing-node messages. It never stores
environment dumps, credentials, GitHub payloads, signatures, tokens, complete logs, or
unbounded traces. A successful retry uses the same `(dag_id, dag_run_id)` row.

Apply migration 006 to the existing volume without deleting it, load the fixture, and
execute a real Airflow 3.3 test run:

```bash
docker compose -f infra/docker-compose.yml up -d --wait postgres
make migrate
make airflow-analytics-check
```

Inspect the application ledger with:

```sql
select dag_run_id, status, source_max_ingested_at, source_delay_seconds,
       dbt_invocation_id, dbt_succeeded_count, dbt_failed_count, finished_at
from ops.analytics_refresh_runs
order by logical_date desc;
```

This directory does not define a production Airflow topology. Scheduler, API
server, metadata database, executor selection, authentication, and secret management
remain deployment concerns and are intentionally outside the Day 7 image artifact.
