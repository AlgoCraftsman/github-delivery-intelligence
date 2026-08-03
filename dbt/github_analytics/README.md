# GitHub analytics dbt project

This project turns append-only `raw.github_events` rows into typed staging views,
state-resolved intermediate views, and evidence-aware analytics marts while
preserving source lineage. It requires Python 3.12, `dbt-core==1.12.0`, and
`dbt-postgres==1.11.0`; all three are installed by the repository's locked uv
environment.

Apply `infra/init/006_create_analytics_refresh_runs.sql` (normally `make migrate`)
before parsing or building Day 12 models because the pipeline-health mart sources the
application-owned ops ledger.

## Configure PostgreSQL

`profiles.yml` reads the following environment variables and supplies local Compose
defaults:

| Variable | Default |
|---|---|
| `DBT_POSTGRES_HOST` | `localhost` |
| `DBT_POSTGRES_PORT` | `55432` |
| `DBT_POSTGRES_DB` | `github_analytics` |
| `DBT_POSTGRES_USER` | `github_analytics` |
| `DBT_POSTGRES_PASSWORD` | `local_only_change_me` |
| `DBT_POSTGRES_SCHEMA` | `analytics` |

Keep real credentials in the environment or an ignored `.env` file. Do not commit
them.

## Validate live raw data

From the repository root:

```bash
uv sync --frozen
make dbt-debug
make dbt-parse
make dbt-freshness
make dbt-build
```

The source resolves to `raw.github_events`. Freshness warns after one hour without a
new `ingested_at` watermark and errors after six hours. A quiet source can therefore
be structurally valid while intentionally failing freshness; investigate ingestion
before bypassing that signal.

## Run deterministic fixture validation

Use a disposable PostgreSQL database. The loader creates and transactionally reloads
only `raw.github_events_fixture`, inserts 32 synthetic rows, and gives them a current
warehouse load watermark. It never truncates or mutates `raw.github_events`.

```bash
psql --set ON_ERROR_STOP=1 \
  --file dbt/github_analytics/fixtures/load_github_events.sql

uv run dbt source freshness \
  --project-dir dbt/github_analytics \
  --profiles-dir dbt/github_analytics \
  --vars '{"github_events_identifier": "github_events_fixture"}'

uv run dbt build \
  --project-dir dbt/github_analytics \
  --profiles-dir dbt/github_analytics \
  --vars '{"github_events_identifier": "github_events_fixture", "fixture_validation": true}'
```

The expected staging output is 12 pull-request snapshots, 10 review snapshots, one
pull-request commit association, three workflow-run snapshots, three deployment
snapshots, and three deployment-status snapshots. The original webhook/backfill pairs
must still normalize to the same entity keys.

The paired snapshots collapse into one row in each Day 9 path. Their exact manually
calculated outcomes are:

| Intermediate model | Fixture outcome |
|---|---|
| `int_pr_lifecycle` | PR `20001:17` merged after 180,000 seconds |
| `int_first_eligible_review` | first non-author review after 82,800 seconds |
| `int_production_deployments` | deployment `20001:70001` succeeded after 300 seconds |
| `int_change_to_deployment` | merge commit linked to production after 93,900 seconds |

The fixture assertion also proves that each webhook/backfill pair contributes two
snapshots from two ingestion paths without producing duplicate intermediate rows.

Day 10 fixture cases prove these mart outcomes:

| Evidence case | Expected outcome |
|---|---|
| configured deployment status | `measured`; deployment frequency `1` on 2026-01-13 |
| two eligible changes, one linked | change lead time `93,900` seconds with `0.5` coverage |
| configured workflow named `Release production` | `configured_proxy` with exact-SHA linkage |
| fully observed unconfigured repository | `unavailable` with `missing_repository_configuration` |
| dates without deployments | measured deployment frequency `0`; null coverage denominator |
| instability metrics | `unavailable` with metric-specific missing-evidence reasons |

Temporal contract tests reject negative lifecycle, review, deployment-success, and
change-lead durations. The date dimension is deterministically bounded from
2026-01-10 through 2026-01-13 for the isolated fixture.

Day 11 assertions add seven exact PR-flow lifecycle/review/rework outcomes. Dashboard
query validation then checks deterministic ordered snapshots for deployment frequency,
direct-from-linked-PR change lead-time P50/P90, status and coverage, review latency,
explicit `XS <=50` / `S 51-200` / `M 201-500` / `L >500` size bands, review rework,
and open aging anchored at `2026-01-14T12:00:00Z`.

Generated artifacts live under `target/` and are ignored.

## Pipeline-health run mart

`analytics_marts.fct_pipeline_health_runs` has one row per hourly
`analytics_refresh` run. It exposes Airflow logical/data-interval times, terminal
duration and status, source watermark/delay captured at run start, dbt invocation and
result counts, bounded sanitized failure fields, and latest-success state. The source
artifact JSON remains in `ops`; raw payloads, credentials, logs, and traces never enter
the mart.

The scheduled image uses explicit project/profile and target paths. Production reads
`raw.github_events`; fixture validation sets
`ANALYTICS_REFRESH_SOURCE_IDENTIFIER=github_events_fixture` without changing model SQL.
Duplicate/DLQ health and benchmark evidence are not represented by this mart and remain
unavailable until actual instrumentation or Day 13 validation exists.
