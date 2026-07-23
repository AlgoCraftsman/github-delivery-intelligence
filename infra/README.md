# Local infrastructure

The Day 1 stack contains one Kafka KRaft broker and one PostgreSQL instance. It is a
local semantics and integration-test environment, not a production topology.

Copy `.env.example` to `.env` if you need to override the development defaults, then
run `make up`. Compose waits for both services to report healthy before returning.
PostgreSQL uses host port `55432` by default to avoid colliding with a conventional local
installation on `5432`; containers still connect to it on `postgres:5432`.

The PostgreSQL initialization script creates the `raw`, `serving`, `ops`, and
`analytics` schemas. Initialization scripts run only when the named volume is empty.

Host processes connect to Kafka at `localhost:9092`. Future Compose services use the
internal listener at `kafka:29092`, avoiding metadata that incorrectly redirects a
container client back to itself.
