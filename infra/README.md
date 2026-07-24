# Local infrastructure

The Day 1 stack contains one Kafka KRaft broker and one PostgreSQL instance. It is a
local semantics and integration-test environment, not a production topology.

Copy `.env.example` to `.env` if you need to override the development defaults, then
run `make up`. Compose waits for both services to report healthy before returning.
PostgreSQL uses host port `55432` by default to avoid colliding with a conventional local
installation on `5432`; containers still connect to it on `postgres:5432`.

The PostgreSQL initialization scripts create the `raw`, `serving`, `ops`, and
`analytics` schemas plus the append-only `raw.github_events` table. Initialization
scripts run only when the named volume is empty.

To apply a newly added idempotent schema script to an existing local named volume
without deleting data, start the services and run:

```bash
make migrate
```

The raw table uses `(source, source_record_key)` as its durable uniqueness boundary.
Webhook rows use the real GitHub delivery ID as both `source_record_key` and
`delivery_id`; backfill rows must not fabricate a webhook delivery ID.

Host processes connect to Kafka at `localhost:9092`. Future Compose services use the
internal listener at `kafka:29092`, avoiding metadata that incorrectly redirects a
container client back to itself.
