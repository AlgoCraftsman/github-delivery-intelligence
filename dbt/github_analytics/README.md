# GitHub analytics dbt project

This project turns append-only `raw.github_events` rows into typed staging views while
preserving source lineage. It requires Python 3.12, `dbt-core==1.12.0`, and
`dbt-postgres==1.11.0`; all three are installed by the repository's locked uv
environment.

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
only `raw.github_events_fixture`, inserts eleven synthetic rows, and gives them a
current warehouse load watermark. It never truncates or mutates
`raw.github_events`.

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

The expected fixture output is two rows each for pull requests, reviews, workflow
runs, deployments, and deployment statuses, plus one pull-request commit
association. The paired resources must normalize webhook and backfill payloads to the
same entity keys.

Generated artifacts live under `target/` and are ignored.
