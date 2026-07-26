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
projection tables, `ops.alert_outbox`, and `raw.backfill_checkpoints`. Initialization
scripts run only when the named volume is empty.

To apply a newly added idempotent schema script to an existing local named volume
without deleting data, start the services and run:

```bash
make migrate
```

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
