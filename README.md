# GitHub Delivery Intelligence

Event-driven engineering analytics using GitHub App webhooks, Kafka, Airflow, dbt,
PostgreSQL, and Metabase to produce replayable PR-flow, evidence-aware software
delivery metrics, versioned dashboard queries, and deterministic visual evidence.

The project is being built from the reviewed [15-day build plan](BUILD_PLAN.md). The
current Day 12 checkpoint adds an hourly, durable analytics refresh and a sanitized
pipeline-health mart to the signed webhook, idempotent raw warehouse, independent PR
monitor, restartable history backfill, dashboards, and contracted dbt analytics paths.

## Prerequisites

- Python 3.12.13
- uv 0.11.29
- Docker Desktop with Docker Compose v2
- GNU Make (optional; the underlying commands also work directly)

## Foundation quickstart

```bash
uv sync --frozen
make check
make up
make migrate
make ps
make dashboard-up
make dashboard-sql-check
```

`make up` starts Kafka on `localhost:9092` and PostgreSQL on `localhost:55432`, then waits
for both health checks and creates the raw and DLQ topics if absent. `make migrate`
applies the idempotent raw-table migration to an existing named volume. The credentials
in `.env.example` are local-only defaults.

Stop services without deleting data:

```bash
make down
```

Stop only the optional Metabase service while preserving its application data:

```bash
make dashboard-down
```

## Local dashboard demo

`make dashboard-up` starts PostgreSQL, reapplies the idempotent read-only analytics
role, and starts the optional Compose `dashboards` profile. Open
<http://localhost:3000> and sign in with `demo@example.invalid` /
`local_only_metabase_demo1`. The Metabase database user is `metabase_reader` /
`local_only_read_only`. All of these passwords are local-only defaults and are not
production-safe.

The demo uses checked-in SQL plus manual Metabase configuration; it does not claim
paid serialization as an OSS feature. Rebuild the isolated fixture marts before
regenerating screenshots:

```powershell
$env:PGPASSWORD='local_only_change_me'
psql --host localhost --port 55432 --username github_analytics `
  --dbname github_analytics --set ON_ERROR_STOP=1 `
  --file dbt/github_analytics/fixtures/load_github_events.sql

uv run dbt source freshness `
  --project-dir dbt/github_analytics --profiles-dir dbt/github_analytics `
  --vars '{"github_events_identifier": "github_events_fixture"}'

uv run dbt build `
  --project-dir dbt/github_analytics --profiles-dir dbt/github_analytics `
  --vars '{"github_events_identifier": "github_events_fixture", "fixture_validation": true}'

make dashboard-sql-check
```

The loader truncates only `raw.github_events_fixture`. Dashboard definitions, metric
status and coverage semantics, manual card configuration, and screenshot QA are in
[`dashboards/README.md`](dashboards/README.md).

![Delivery performance dashboard](dashboards/screenshots/delivery-performance.png)

![Pull-request flow dashboard](dashboards/screenshots/pull-request-flow.png)

## Webhook contract

The receiver supports `pull_request`, `pull_request_review`, `workflow_run`,
`deployment`, and `deployment_status`. It rejects invalid signatures, missing delivery
headers, unsupported events, malformed payloads, and bodies larger than
`GITHUB_WEBHOOK_MAX_BODY_BYTES`.

The outer envelope is strict and versioned. The original JSON payload remains intact and
accepts unknown fields. Synthetic fixtures for all five event families live under
`tests/fixtures`.

The application fails closed with `503` when no publisher is configured. Day 3
implements the publisher with `acks=all`, producer idempotence, repository-keyed
records, and a bounded delivery callback. Enqueuing a record locally is not treated as
success: the receiver returns `202` only after Kafka reports delivery and returns `503`
on queue, callback, or timeout failure.

Run the local receiver after copying `.env.example` to `.env`, changing the webhook
secret, and starting the core services:

```bash
cp .env.example .env
make up
make webhook
```

The receiver listens on `http://127.0.0.1:8000` by default:

- `POST /webhooks/github` validates and durably publishes supported events.
- `GET /health/live` reports process liveness.
- `GET /health/ready` checks Kafka metadata availability.
- `GET /metrics` exposes request, publish-result, and publish-latency metrics in
  Prometheus text format.

## Warehouse writer

The `warehouse-writer` consumer group reads `github.events.raw.v1`, validates the
versioned envelope, and inserts the complete original payload into
`raw.github_events`. The durable uniqueness boundary is `(source, source_record_key)`;
webhook rows use the real GitHub delivery ID as the source record key.

Run it after copying `.env.example` to `.env`, starting the services, and applying the
migration:

```bash
make up
make migrate
make warehouse
```

Automatic Kafka commits and automatic offset storage are disabled. For a valid record,
the writer commits PostgreSQL before synchronously committing that message's Kafka
offset. A replay after a crash in between those operations reaches the uniqueness
constraint and creates no second raw row.

Unprocessable records are published to `github.events.dlq.v1` with their original bytes
base64 encoded and their source topic, partition, and offset retained. The writer waits
for the DLQ delivery callback before committing the source offset. If PostgreSQL, DLQ
publishing, or offset commit fails, the worker stops without advancing past an
unconfirmed durable boundary.

This is at-least-once processing with idempotent database effects. It is not end-to-end
exactly-once delivery.

## Pull-request monitor

The independent `pr-monitor` consumer group reads the same raw topic without sharing
progress with `warehouse-writer`. It maintains `serving.open_pull_requests`, selects the
earliest submitted review from someone other than the PR author, and retains a source
watermark so delayed snapshots cannot reopen or regress newer PR state.

Run it after applying the migrations:

```bash
make up
make migrate
make pr-monitor
```

The default stale threshold is 24 hours and the sweep interval is 60 seconds. Both are
configurable through `PR_MONITOR_STALE_AFTER_HOURS` and
`PR_MONITOR_STALE_SWEEP_INTERVAL_SECONDS`. Draft PRs and PRs with an eligible review are
excluded from the sweep.

Each eligible stale PR creates an `ops.alert_outbox` row with a unique key derived from
the real repository and pull-request identities. Repeated sweeps create no duplicate
effect. Closing a PR removes it from the open projection and cancels its still-pending
alert. External Slack dispatch remains deferred; the outbox is the durable Day 5
boundary.

## Historical GitHub backfill

The `github-backfill` command reads pull requests through GitHub GraphQL, then traverses
the complete review and commit connections for each PR created in an explicit half-open
window (`start <= createdAt < end`). It also reads workflow runs, deployments, and
deployment statuses through GitHub's versioned REST API. GraphQL uses opaque forward
cursors; REST checkpoints store the next positive page number. Both use a configured
page size from 1 through GitHub's maximum of 100.

Copy `.env.example` to `.env` and replace the example values with a short-lived token
and the real repository and GitHub App installation identities. Backfill source records
do not fabricate webhook delivery IDs. Apply the checkpoint migration and run a bounded
window:

```bash
make migrate
make backfill \
  BACKFILL_START=2026-01-01T00:00:00Z \
  BACKFILL_END=2026-02-01T00:00:00Z
```

Each API page inserts its selected GraphQL resource objects into
`raw.github_events` and advances `raw.backfill_checkpoints` in the same PostgreSQL
transaction. The source keys use GitHub global node IDs plus the resource version or
state where it can change. Repeating a page or restarting from a stored opaque cursor
therefore produces no duplicate raw effect.

The client reads GitHub's returned rate-limit headers rather than assuming a universal
allowance. Primary exhaustion waits until `x-ratelimit-reset`; secondary limits honor
`retry-after` or use bounded exponential waits beginning at the documented one-minute
minimum. Retries stop after the configured budget.

The repository pull-request connection and deployment endpoint have no direct
creation-time range argument. This MVP filters their complete paginated responses
locally; very large repositories may therefore require scanning older pages. GitHub
caps a filtered workflow-run search at 1,000 results, so the command fails without
advancing that checkpoint when the requested window is too large and asks for a
smaller window. GitHub retains historical deployment statuses for only 90 days, so an
older backfill may have an explicit coverage gap even when deployment records remain.

The token needs read access to repository contents and metadata for GraphQL, Actions
read access for workflow runs, and Deployments read access for deployments and their
statuses.

## Airflow orchestration

`airflow/Dockerfile` extends the pinned Apache Airflow 3.3.0 Python 3.12 image, copies
the tested application package and dbt project, installs pinned backfill plus
`dbt-core==1.12.0` / `dbt-postgres==1.11.0` dependencies, pins Airflow to the base-image
version, and runs `pip check`. Webhook and Kafka dependencies stay out of this image.
Build it and verify that both expected DAGs import without errors:

```bash
make airflow-image
make airflow-dag-check
```

The `github_backfill` DAG is manual-only and allows one active run. Its trigger form
requires an inclusive `window_start` and exclusive `window_end`, both as offset-aware
ISO 8601 timestamps. The task invokes the tested `github-backfill` package and has
three bounded retries. Because each API page and checkpoint commit in one PostgreSQL
transaction, an Airflow retry resumes from durable progress and duplicate source keys
remain harmless.

The `analytics_refresh` DAG runs hourly in UTC with `catchup=False` and one active run.
Its ordered tasks persist `running` and check the raw watermark/freshness, execute the
contracted dbt build, then persist `succeeded`. Exhausted failures persist `failed` and
re-raise so Airflow agrees with the durable ledger. `(dag_id, dag_run_id)` makes task
retries update one row. Bounded summaries come from explicit `sources.json` and
`run_results.json` target artifacts; command output, credentials, payloads, and full
tracebacks are not stored.

After starting PostgreSQL, applying migrations, and loading the isolated fixture, run
the supported Airflow 3.3 test path inside the image:

```bash
make airflow-analytics-check
```

This is still an image and DAG artifact, not a production scheduler/API/executor
topology. Streaming consumers remain long-running services outside Airflow. See
`airflow/README.md` for runtime variables, container paths, and inspection queries.

## dbt analytics models

The dbt project pins `dbt-core==1.12.0` and `dbt-postgres==1.11.0`. The adapter
and Core use independent version numbers; this is the compatible stable pair released
for the reviewed baseline. Six contracted staging views normalize webhook payloads and
GraphQL/REST backfill wrappers:

- pull requests
- pull-request reviews
- pull-request commit associations
- workflow runs
- deployments
- deployment statuses

Each view retains `event_id`, source identity, repository and installation lineage,
warehouse load time, and the raw JSON payload. Entity keys normalize both ingestion
paths without pretending that a GraphQL node ID is a REST database ID.

Four contracted intermediate views collapse duplicate snapshots and expose reusable
stateful logic:

- resolved pull-request lifecycle
- first eligible non-author review
- production deployments with resolved status history
- exact-SHA change-to-successful-deployment linkage

The change linkage prefers merge-commit evidence and falls back to a directly matched
pull-request commit. It does not infer Git ancestry or claim coverage for an unmatched
change.

The contracted marts expose the core analytics vertical slice and run health:

- repository evidence configuration and a bounded date spine
- pull-request lifecycle and exact-SHA deployment linkage
- measured deployment statuses and explicitly configured workflow proxies
- one repository/date/metric row with status, coverage, definition version, and
  exclusion reason
- one `analytics_refresh` run with watermark age, dbt result counts, bounded failure
  classification, and latest-success state

Deployment frequency is measured only after repository configuration. Change lead
time is calculated only for successfully linked changes and publishes linkage
coverage. Failed deployment recovery time, change failure rate, and deployment
rework rate remain unavailable until defensible intervention evidence is configured
and modeled; CI failures are not treated as production failures.

Against the live `raw.github_events` table:

```bash
make dbt-debug
make dbt-freshness
make dbt-build
```

Deterministic validation uses the dedicated `raw.github_events_fixture` table. The
loader transactionally reloads only that fixture table, gives resource timestamps
fixed synthetic values, and sets warehouse load time relative to execution so
freshness does not expire. CI loads it, runs `dbt source freshness`, and runs `dbt
build` with fixture assertions enabled. Exact local commands, resource counts, and
manually calculated Day 9 and Day 10 outcomes are documented in
`dbt/github_analytics/README.md`.

## Current scope

Day 12 establishes the scheduled source-to-mart refresh boundary and durable evidence
for last refresh status, last successful Airflow/dbt run, source watermark and delay,
dbt result counts, and sanitized failures. Day 11 dashboard SQL and screenshots remain
unchanged. Duplicate-delivery health, DLQ incidents and last-failure evidence, failure
drills, throughput, and latency benchmarks are intentionally unavailable until they are
actually instrumented or measured in Day 13. The local single-broker Kafka deployment
and Airflow test container demonstrate semantics; neither is a production availability
topology.
