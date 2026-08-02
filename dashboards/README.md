# Day 11 dashboard demo

This directory contains the versioned SQL and fixture-backed screenshots for two
local Metabase dashboards: **Delivery performance** and **Pull-request flow**. The
portable OSS contract is Compose, checked-in SQL, documented manual configuration,
and screenshots. Metabase application state remains in the local `metabase_data`
volume; this project does not claim paid serialization as part of the workflow.

## Start and stop the demo

From the repository root, sync the locked environment and start PostgreSQL plus the
optional `dashboards` Compose profile:

```bash
uv sync --frozen
make dashboard-up
```

Metabase is available at <http://localhost:3000>. The local demo administrator is
`demo@example.invalid` with password `local_only_metabase_demo1`. These credentials
and every password below are local-only examples and are not production-safe.

Stop Metabase without deleting either named volume:

```bash
make dashboard-down
```

`dashboard-down` stops the Metabase container; it does not delete PostgreSQL or
Metabase data. Do not use a volume-removing Compose command when preserving the demo.

## Deterministic data and SQL validation

Reload only the isolated fixture table; this never truncates `raw.github_events`:

```bash
PGPASSWORD=local_only_change_me psql \
  --host localhost --port 55432 \
  --username github_analytics --dbname github_analytics \
  --set ON_ERROR_STOP=1 \
  --file dbt/github_analytics/fixtures/load_github_events.sql

uv run dbt source freshness \
  --project-dir dbt/github_analytics \
  --profiles-dir dbt/github_analytics \
  --vars '{"github_events_identifier": "github_events_fixture"}'

uv run dbt build \
  --project-dir dbt/github_analytics \
  --profiles-dir dbt/github_analytics \
  --vars '{"github_events_identifier": "github_events_fixture", "fixture_validation": true}'

make dashboard-sql-check
```

The validator executes every file under `dashboards/sql` twice, verifies its grain,
required fields and fixture values, and compares the ordered result to the checked-in
SHA-256 contract in `query_contracts.json`.

## Metabase database and content setup

`make dashboard-up` applies the idempotent reader-role script before starting
Metabase. For manual database setup, add a PostgreSQL database named **GitHub Delivery
Analytics** with:

| Setting | Value |
|---|---|
| Host | `postgres` from Metabase in Compose; `localhost` from a host client |
| Port | `5432` in Compose; `55432` from the host |
| Database | `github_analytics` |
| Username | `metabase_reader` |
| Password | `local_only_read_only` |

The reader can connect, use `analytics_staging`, `analytics_intermediate`, and
`analytics_marts`, and select their relations. It has no superuser, database-create,
role-create, inheritance, or replication capability and has no access to `raw`,
`serving`, or `ops`.

Create a collection named **Day 11 Dashboard Demo** and add these native-query cards
using the corresponding checked-in SQL:

- **Delivery performance**: deployment frequency as a repository-split line chart;
  change lead time as a line chart with `date_day` on X, `series_name` as the breakout,
  and `lead_time_hours` on Y; metric status as a table focused on status, coverage,
  and exclusions.
- **Pull-request flow**: review-latency distribution as an ordered bar chart;
  review-latency P50/P90 as a time-series line chart; cycle time by size band as a
  grouped chart; review rework as an ordered bar chart; open aging and WIP as a table.

The recommended dashboard layout is documented with every query in
[`sql/README.md`](sql/README.md). The local screenshots are evidence of that manual
configuration, not a portable Metabase export.

## Metric interpretation

`measurement_status` separates evidence quality from the number shown:

- `measured`: the configured production evidence is directly observed.
- `configured_proxy`: an explicitly configured workflow signal is used as a proxy.
- `unavailable`: the evidence needed for a defensible value is absent; the metric
  value remains null and `exclusion_reason` explains why.

`coverage_numerator` is the eligible evidence included in a metric,
`coverage_denominator` is all eligible evidence considered, and `coverage_ratio` is
their quotient when the denominator is nonzero. Failed deployment recovery time,
change failure rate, and deployment rework rate remain unavailable because operational
incident, intervention, and unplanned-rework evidence is not modeled. CI failure is
not treated as a production failure.

Change lead-time P50/P90 is calculated directly from linked pull-request facts, not
from daily averages. Size bands use additions plus deletions: `XS <=50`, `S 51-200`,
`M 201-500`, and `L >500`. One review-rework cycle is counted for each distinct
reviewed revision whose resolved eligible non-author review state is
`changes_requested`; the resolved review identity is used when commit SHA is absent.
The open-aging demo is anchored at `2026-01-14 12:00 UTC`, so its screenshots do not
depend on the current clock.

Dashboard 3 operational evidence is intentionally deferred to Days 12/13 because the
required operational events are not modeled yet.

## Screenshots

After rebuilding the fixture marts and confirming that every card has finished
loading, regenerate and visually inspect:

- `screenshots/delivery-performance.png`
- `screenshots/pull-request-flow.png`

The complete capture checklist is in [`screenshots/README.md`](screenshots/README.md).
