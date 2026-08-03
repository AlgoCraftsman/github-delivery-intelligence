# Local infrastructure

The Day 1 stack contains one Kafka KRaft broker and one PostgreSQL instance. It is a
local semantics and integration-test environment, not a production topology.

Copy `.env.example` to `.env` if you need to override the development defaults, then
run `make up`. Compose waits for both services to report healthy before returning.
PostgreSQL uses host port `55432` by default to avoid colliding with a conventional local
installation on `5432`; containers still connect to it on `postgres:5432`.

`make up` also creates `github.events.raw.v1` with three partitions and
`github.events.dlq.v1` with one partition if they do not already exist. `make topics`
can be rerun safely; it never replaces an existing topic. Broker-side automatic topic
creation is disabled so a misspelled or prematurely used topic cannot silently inherit
the three-partition broker default.

The PostgreSQL initialization scripts create the `raw`, `serving`, `ops`, and
`analytics` schemas, the append-only `raw.github_events` table, the PR-monitor
projection tables, `ops.alert_outbox`, `raw.backfill_checkpoints`, and the
`ops.analytics_refresh_runs` ledger. Initialization scripts run only when the named
volume is empty.

To apply a newly added idempotent schema script to an existing local named volume
without deleting data, start the services and run:

```bash
make migrate
```

`make migrate` includes idempotent `006_create_analytics_refresh_runs.sql`. It creates
or verifies the Day 12 ledger and indexes in place; never delete `postgres_data` to
apply it. The ledger constrains statuses, terminal timestamps, intervals, nonnegative
source delay/result counts, bounded error text, and `(dag_id, dag_run_id)` uniqueness.

The raw table uses `(source, source_record_key)` as its durable uniqueness boundary.
Webhook rows use the real GitHub delivery ID as both `source_record_key` and
`delivery_id`; backfill rows must not fabricate a webhook delivery ID.

`raw.backfill_checkpoints` keys progress by the real repository identity, resource,
scope, and bounded time window. Its cursor is an opaque value returned by GitHub
GraphQL. The backfill storage writes raw records and advances this cursor in one
transaction so a restart either sees both effects or neither.

`serving.pull_request_projection_watermarks` retains the newest applied source timestamp
even after a PR closes. `serving.pull_request_first_reviews` preserves the earliest
eligible review across close/reopen transitions. `serving.open_pull_requests` therefore
cannot be recreated by a delayed older event or lose its review history when reopened.
`ops.alert_outbox.alert_key` is unique, so repeated stale-PR sweeps produce one durable
intent.

Host processes connect to Kafka at `localhost:9092`. Future Compose services use the
internal listener at `kafka:29092`, avoiding metadata that incorrectly redirects a
container client back to itself.

## Optional dashboards profile

Run `make dashboard-up` to start PostgreSQL, apply
`infra/init/005_create_metabase_reader.sql`, and start the pinned Metabase service in
the Compose `dashboards` profile. The UI is available at <http://localhost:3000> and
persists its local application database in the `metabase_data` named volume. Run
`make dashboard-down` to stop only Metabase without deleting that volume.

The initialized demo account is `demo@example.invalid` with password
`local_only_metabase_demo1`. Configure the **GitHub Delivery Analytics** database with
host `postgres`, port `5432`, database `github_analytics`, user `metabase_reader`, and
password `local_only_read_only`. These credentials are local-only examples and are not
production-safe.

The reader role has `CONNECT`, analytics-schema `USAGE`, and `SELECT` on the staging,
intermediate, and mart relations, including future tables created by
`github_analytics`. It is explicitly non-superuser and cannot create databases or
roles, inherit another role, replicate, or access `raw`, `serving`, or `ops`.

If an existing PostgreSQL volume predates the role, `make dashboard-up`,
`make metabase-access`, or `make migrate` applies the idempotent script without volume
deletion. Metabase OSS content is then configured manually from the versioned SQL in
`dashboards/sql`; the local application database is not presented as portable paid
serialization.

Day 12 exposes refresh health through `analytics_marts.fct_pipeline_health_runs`.
`metabase_reader` can select that sanitized view but retains no `ops` or `raw` schema
access. The mart does not expose stored artifact JSON, raw logs, payloads, or secrets.
