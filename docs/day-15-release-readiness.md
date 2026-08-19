# Day 15 release-readiness review

Review date: 2026-08-18 America/New_York (some tool timestamps are 2026-08-19
UTC)

Base commit: `ad7df7bb69b53f12407386950ceb785ef12e1471`

Validated implementation commit:
`d5be06f7501240a3e40d3e810066ff8194aa7a1b`

## Decision

The implementation hardening and most acceptance evidence are complete, but the
portfolio release candidate is **not signed off**. Current blockers are:

- the Airflow overlay still resolves transitive packages during its image build, so
  the broad dependency-pinning criterion is not yet fully proved;
- the vulnerability audits found unresolved fixable high and critical findings;
- the changed commit has local CI-equivalent evidence but no remote GitHub Actions
  run because nothing was pushed;
- the checked-in screenshots passed pixel inspection and their SQL contracts are
  current, but a browser-runtime failure prevented fresh inspection of the real
  Metabase UI;
- no final implementation pull request exists, and creating one was outside this
  review's authorization.

This is a readiness report, not a production-infrastructure, vulnerability-free, or
end-to-end exactly-once claim.

## Acceptance evidence

| Criterion | Status | Evidence and limitation |
|---|---|---|
| Fresh clone starts the core stack with documented commands | Proved | A complete Git bundle of `d5be06f` was cloned to a new ignored path, pinned uv 0.11.29 created a clone-local environment, and an isolated `make demo` completed successfully on Kafka port 19094 and PostgreSQL port 55435. |
| No implementation work was committed directly to `main` | Proved | Every first-parent `main` commit after the parentless initial commit has two parents and a pull-request merge subject. The most recent merge is PR #24 at `ad7df7b`. |
| All images and dependencies use pinned versions or digests | Incomplete | Application packages are exact in `pyproject.toml` and resolved by `uv.lock`; GitHub Actions use commit SHAs; all four container images now use version plus manifest digest. Airflow's added requirements are exact direct pins, but their transitives are still selected by `pip` during image construction. |
| Webhook signature validation and broker acknowledgement are tested | Proved | `tests/unit/test_webhook_security.py`, `tests/unit/test_webhook_app.py`, `tests/unit/test_webhook_kafka.py`, and `tests/integration/test_webhook_kafka_live.py` cover the HMAC boundary and acknowledgement-before-success behavior. The live integration test remained opt-in during this review. |
| Raw writes and alert effects are idempotent under replay | Proved | Storage and monitor unit/integration tests cover duplicate source identities, replay, monotonic projections, and unique outbox effects. `docs/day-13-evidence.md` records the prior observed replay drill. |
| Kafka offsets advance only after durable effects or DLQ acknowledgement | Proved | `tests/unit/test_warehouse_consumer.py`, `tests/unit/test_warehouse_dlq.py`, and `tests/unit/test_pr_monitor.py` cover database commit, crash windows, DLQ acknowledgement, and disabled automatic offset advancement. |
| Backfill is restartable and rate-limit aware | Proved | Backfill model, client, storage, CLI, and DAG tests cover persisted cursors/pages, replay absorption, primary/secondary rate limits, bounded backoff, and invalid checkpoint rejection. |
| dbt models have documented grains, contracts, tests, and lineage | Proved | Model YAML, `dbt/github_analytics/models/README.md`, fixture assertions, source freshness, and the 322-node fixture build provide current evidence. |
| DORA labels match available evidence and show coverage | Proved | The fixture-backed marts and eight dashboard contracts preserve `measured`, `configured_proxy`, and `unavailable`, null unavailable values, exclusion reasons, and coverage numerator/denominator/ratio semantics. |
| Individual contribution leaderboards are absent | Proved | Dashboard SQL and screenshots contain repository/service flow metrics and no contributor grouping or ranking. `docs/metric-definitions.md` explicitly prohibits individual-performance use. |
| CI, failure drills, and actual benchmark evidence are green | Incomplete | The post-merge `main` run at `ad7df7b` passed all four jobs, and local equivalents pass at `d5be06f`. Day 13 contains observed drill/benchmark evidence. There is no remote CI run for the unpushed Day 15 commit. |
| README includes architecture, quickstart, screenshots, limitations, and demo steps | Proved | All five sections are present and link to detailed guides and observed validation. |
| Security and operations runbooks are complete | Proved | The runbooks cover trust boundaries, local-only credentials, reader isolation, replay, DLQ, checkpoints, recovery, rotation, and safe volume-preserving operation. Completion does not mean the audit was clean. |
| Final implementation PR has a reviewer-oriented summary | Pending | No Day 15 PR exists. No PR or GitHub object was created or changed in this review. |

## Reproducibility hardening

Registry inspection with `docker buildx imagetools inspect` resolved and the
implementation pins these multi-platform manifest digests:

| Component | Reference |
|---|---|
| Kafka | `apache/kafka:4.3.1@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837` |
| PostgreSQL | `postgres:17.10-bookworm@sha256:9b18b78397054fce88a9552e9d5a3ad5bb7fd258c5b3cc1c5028e46373d6ea8f` |
| Metabase | `metabase/metabase:v0.63.2@sha256:252f8c9bd56dd21158005675b55876cf9fb838e0a0e0541581af859eafe1f32e` |
| Airflow base | `apache/airflow:3.3.0-python3.12@sha256:96e99f25815f533b298a4d53f283adf5c84c27334ea16ef232777cb800bddf10` |

The PostgreSQL 17.10 tag had previously resolved locally to
`sha256:4f736ae292687621d4dbe0d499ffd024a36bd2ee7d8ca6f2ccd4c800f047b394`,
while the registry returned `sha256:9b18...` during this review. That observed tag
movement is why version-only image references were insufficient.

All CI jobs now select the supported `ubuntu-24.04` runner label instead of the
moving `ubuntu-latest` label. Action implementations remain commit-SHA pinned.

## Dependency and vulnerability audit

### Python lock audit

Pinned `pip-audit==2.10.1` audited a frozen export of all uv lock groups with
`--disable-pip --strict`. It reported four advisories in `sqlparse==0.5.5`:

- `CVE-2026-71491`
- `CVE-2026-59894`
- `CVE-2026-59893`
- `CVE-2026-54284`

Each report identifies `sqlparse==0.6.0` as the fix. The package arrives through
`dbt-core==1.12.0`, a local analytics/development dependency rather than the webhook
receiver runtime. The current stable `dbt-core==1.12.2` metadata still requires
`sqlparse>=0.5.5,<0.6.0`, so this review did not force an unsupported override or
adopt a dbt 2.0 prerelease. Repository-controlled analytics SQL constrains exposure,
but the advisories remain unresolved and the audit is not clean.

### Container image audit

Pinned `aquasec/trivy:0.70.0` used a vulnerability database downloaded around
2026-08-19 01:29 UTC. The scope was vulnerability scanning only, severities HIGH and
CRITICAL, with `--ignore-unfixed`. Counts therefore cover only fixable high/critical
occurrences and are not an all-severity, configuration, secret, reachability, or
all-unfixed assessment.

| Image | High occurrences | Critical occurrences | Unique vulnerability IDs |
|---|---:|---:|---:|
| Kafka 4.3.1 | 10 | 0 | 9 |
| PostgreSQL 17.10 Bookworm | 21 | 1 | 22 |
| Metabase 0.63.2 | 6 | 0 | 6 |
| Airflow 3.3.0 Python 3.12 base | 47 | 3 | 48 |
| Locally built Airflow image | 47 | 3 | 48 |

The PostgreSQL critical occurrence is `CVE-2025-68121` in Go `stdlib` v1.24.6.
The Airflow critical occurrences are `CVE-2026-35030`, `CVE-2026-42208`, and
`CVE-2026-49468` in `litellm==1.82.6`; the scanner lists later fixed versions.
Occurrence counts are not distinct exploit paths, and reachability was not proved.
They must not be dismissed on that basis, so image-audit signoff remains open.

### Update decisions

- Dependabot PR #19 (`uv-build` 0.11.32 to 0.12.3) predates Days 13 and 14. It had
  historical green checks, but no security justification was identified and a newer
  0.12.x already existed. It was not adopted merely for freshness.
- PR #21 (PostgreSQL 17.10 to 18.0) is a major upgrade and had an unstable state plus
  a historical Airflow failure. PostgreSQL requires dump/restore, `pg_upgrade`, or
  logical replication for a major upgrade. It was not pointed at the retained 17
  volume and was not adopted without a separate migration/compatibility plan.
- PR #22 (Metabase 0.63.2 to 0.63.5.2) had historical green checks but predates Days
  13 and 14. Metabase recommends backing up application data and testing upgrades;
  rollback can require restoring that data. It was not adopted without preserved-
  volume UI and screenshot validation.
- Kafka 4.3.1 remains the official release assessed in this review. Airflow 3.3.0
  supports Python 3.12 and PostgreSQL 17. Version upgrades were not used as a
  substitute for compatibility and release evidence.

No Dependabot PR was modified or merged.

## Secret and access review

Current tracked files and full Git history were searched without printing candidate
values for GitHub token prefixes, AWS access-key prefixes, private-key headers, and
Slack token prefixes. No match was found. The only tracked sensitive-looking
extension is `.env.example`; real `.env` files, artifacts, environments, caches,
logs, and local databases are ignored.

A Git archive of tracked `HEAD` was scanned with pinned Trivy 0.70.0's secret scanner.
The report parsed successfully with zero secret findings. Scanning an archive bounded
the input to tracked content and excluded `.git`, `.venv`, and prior artifacts by
construction.

After migrations, `metabase_reader` successfully selected from one relation in each
of `analytics_staging`, `analytics_intermediate`, and `analytics_marts`. Direct reads
from `raw`, `serving`, and `ops` each failed with schema-permission denial. Example
passwords remain local-only and are not production-safe.

## Dashboard and screenshot review

The deterministic fixture workflow produced 322 successful/pass dbt results. All
eight SQL contracts passed with 40, 20, 25, 7, 3, 5, 4, and 4 rows respectively.

Pixel inspection of `delivery-performance.png` and `pull-request-flow.png` confirmed
legible P50/P90 labels, the measurement status and coverage columns, exclusion
evidence, the four size bands, numerically ordered rework cycles, the fixed
2026-01-14 12:00 aging anchor, draft/review/WIP fields, and no contributor ranking.

The images were not regenerated. The installed in-app browser runtime rejected its
own bundled browser service at a trusted-path check before tab discovery, so the real
Metabase dashboards could not be freshly opened and compared. Pixel and SQL evidence
are valid, but screenshot currency remains incomplete rather than being inferred.

## Validation results

### Local checkout

- `uv lock --check`: resolved 91 packages and exited successfully.
- `make UV=.venv/Scripts/uv.exe check`: Ruff format passed for 74 files, Ruff lint
  passed, strict mypy passed for 69 source files, 328 tests passed, 7 opt-in live
  integration tests skipped, and measured branch coverage was 100%.
- Base and dashboard-profile Compose configuration passed.
- `make demo`: source freshness completed, dbt recorded 322/322 pass/success results,
  all eight dashboard contracts passed, 25 evidence-aware metric rows printed, and
  the deterministic completion marker was observed.
- `make migrate` twice: both invocations exited zero against the retained volume.
- Airflow image build: the `FROM` line resolved the pinned digest and `pip check`
  reported no broken requirements.
- DAG check: returned `analytics_refresh` and `github_backfill` with no import errors.
- Airflow analytics smoke: the durable ledger row is `succeeded` for
  `raw.github_events_fixture`, with 322 succeeded and zero failed, skipped, warning,
  or error results.
- `git diff --check`: passed before the implementation commit.

`make day13-evidence` was intentionally not rerun. The checked-in Day 13 evidence
already contains the observed failure drills and benchmark, while rerunning it would
append new synthetic live-raw rows and perform outages.

### Fresh clone of the implementation commit

`git bundle verify` reported complete history and the expected
`hardening/release-readiness` ref. The bundle was cloned into a new ignored path at
`d5be06f`. Pinned uv 0.11.29 used CPython 3.12.13 and installed the frozen 90-package
environment into the clone's own `.venv` and cache.

The clone used:

- `COMPOSE_PROJECT_NAME=github-delivery-intelligence-day15-fresh`
- `KAFKA_PORT=19094`
- `POSTGRES_PORT=55435`
- `DBT_POSTGRES_PORT=55435`

The fresh demo observed healthy isolated Kafka and PostgreSQL services, completed all
322 dbt nodes, passed all eight dashboard contracts, printed 25 metric rows, and
printed `Deterministic demo completed successfully.` Ordinary `make down` stopped the
isolated containers and retained their PostgreSQL volume. It did not truncate or
mutate append-only `raw.github_events`.

### Remote CI boundary

The post-merge `main` run for base commit `ad7df7b` passed Python quality, Compose,
dbt, and Airflow jobs:

<https://github.com/AlgoCraftsman/github-delivery-intelligence/actions/runs/32203307418>

That run is prior evidence, not a current run for `d5be06f`. Nothing was pushed, so
the changed implementation has no remote CI result.

## Service and volume state

At the end of validation, the original Kafka service was healthy on port 9092 and
PostgreSQL was healthy on port 55432. Metabase remained stopped with its preserved
application volume. The fresh-clone containers were stopped.

All seven project volumes were retained:

- `github-delivery-intelligence_postgres_data`
- `github-delivery-intelligence_metabase_data`
- `github-delivery-intelligence-day14-fresh_postgres_data`
- `github-delivery-intelligence-day14-fresh-default_postgres_data`
- `github-delivery-intelligence-day14-port-fix_postgres_data`
- `github-delivery-intelligence-day14-fresh-port-fix_postgres_data`
- `github-delivery-intelligence-day15-fresh_postgres_data`

No volume was deleted, recreated for recovery, or used with PostgreSQL 18.

## GitHub and audit limitations

- Dependabot alerts could not be used because the API returned HTTP 403 with alerts
  disabled and a token-scope limitation. This is not a passed alert audit.
- The private repository's branch-protection endpoint returned HTTP 403 because the
  required GitHub plan or public visibility was unavailable. Branch protection is
  not claimed.
- No release or tag exists.
- No push, PR/issue modification, release/tag creation, merge, or repository-setting
  change occurred during this review.

## Authoritative references

- [GitHub-hosted runner labels](https://docs.github.com/en/actions/reference/runners/github-hosted-runners)
- [Apache Kafka 4.3.1 release announcement](https://kafka.apache.org/blog/2026/06/25/apache-kafka-4.3.1-release-announcement/)
- [Airflow 3.3.0 prerequisites](https://airflow.apache.org/docs/apache-airflow/3.3.0/installation/prerequisites.html)
- [Airflow reproducible constraints guidance](https://airflow.apache.org/docs/apache-airflow/3.3.0/installation/installing-from-pypi.html)
- [PostgreSQL major-version upgrade guidance](https://www.postgresql.org/docs/18/upgrading.html)
- [PostgreSQL `pg_upgrade`](https://www.postgresql.org/docs/18/pgupgrade.html)
- [Metabase upgrade guidance](https://www.metabase.com/docs/latest/installation-and-operation/upgrading-metabase)
- [`dbt-core` 1.12.2 package metadata](https://pypi.org/pypi/dbt-core/1.12.2/json)
- [`sqlparse` 0.6.0 package metadata](https://pypi.org/pypi/sqlparse/0.6.0/json)
