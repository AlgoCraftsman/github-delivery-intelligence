# Publication-readiness review

Review date: 2026-08-19 America/New_York

Base commit: `b4600a9fc36b18291d70c3c28241cd1a42448b10`

Review branch: `maintenance/public-release-readiness`

## Decision

The implementation is approved for public portfolio publication with the limitations
in this report. This is a source-publication decision, not a claim that the local stack
is production-ready, vulnerability-free, or fully dependency-reproducible. Publishing
the private repository, protecting `main`, and creating any tag or release remain
separate maintainer actions.

The delivery model remains at-least-once processing with idempotent durable effects.
The single-broker Kafka, local PostgreSQL, local Airflow, and manually configured
Metabase topology demonstrate behavior; they are not production infrastructure.

## Publication changes

- The repository uses the [MIT License](../LICENSE) with copyright held by
  AlgoCraftsman.
- [CONTRIBUTING.md](../CONTRIBUTING.md) records review, data-safety, metric-integrity,
  and validation expectations.
- [SECURITY.md](../SECURITY.md) directs sensitive reports to GitHub private
  vulnerability reporting when available and makes no production-support or response-
  time guarantee.
- CI pins uv 0.12.5 exactly. The project accepts `>=0.12.1,<0.13`, which admits the
  observed Dependabot uv 0.12.1 updater and reviewed 0.12 patch releases without
  admitting a later minor release. Both uv 0.12.1 and 0.12.5 completed
  `uv lock --check` against the unchanged lock, and a frozen uv 0.12.5 sync completed.
- Historical Day 13 through Day 15 reports retain uv 0.11.29 because that is the
  toolchain that produced those observations.

## Pinned container inputs

The local stack currently references these versioned images and immutable manifest
digests:

| Component | Exact reference |
|---|---|
| Kafka | `apache/kafka:4.3.1@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837` |
| PostgreSQL | `postgres:17.11-bookworm@sha256:84560e3b9c6874893fc4e2854f5dc3e7c1a37bc9d1dfd7a8c641310ae22ba5ad` |
| Metabase | `metabase/metabase:v0.63.13@sha256:6e188e7068c6e9cf7b24480ada80f335bca9135765764ee827245f44ffa9eace` |
| Airflow base | `apache/airflow:3.3.0-python3.12@sha256:96e99f25815f533b298a4d53f283adf5c84c27334ea16ef232777cb800bddf10` |

The application dependency graph is locked. Airflow's overlay requirements are exact
direct pins and install on the official Airflow base, but `pip` still selects their
transitive packages during image construction. The broad release criterion that all
dependencies are pinned therefore remains incomplete.

## Security audit

### Python dependencies

Pinned `pip-audit` 2.10.1 ran in strict, no-pip mode against an export of every lock
group, with hashes and without the local project. It returned exit code 1 because it
found four advisories in `sqlparse==0.5.5`:

| Advisory | Scanner-listed fix |
|---|---|
| `PYSEC-2026-3698` | `0.6.0` |
| `PYSEC-2026-3697` | `0.6.0` |
| `PYSEC-2026-3699` | `0.6.0` |
| `PYSEC-2026-3696` | `0.6.0` |

`dbt-core==1.12.0` requires `sqlparse>=0.5.5,<0.6.0`. This review did not force an
unsupported override. The package is in the local analytics/development graph, not a
direct webhook-receiver runtime dependency, and analytics SQL is repository-
controlled. Those exposure limits do not resolve the advisories or make the audit
clean.

### Container images

Trivy 0.74.0, pulled as
`aquasec/trivy@sha256:62b1e65e8869bc4b4c6aa4fa2b21595256c7c2f6018a9d9ad61caf87187c1969`,
scanned the exact images above plus the freshly built local Airflow image. The
vulnerability database was updated at `2026-08-19T13:00:00.616726228Z` and downloaded
at `2026-08-19T19:47:34.861832228Z`.

The scan selected the vulnerability scanner, HIGH and CRITICAL severities, and
`--ignore-unfixed`. Counts are package occurrences, not distinct exploit paths or a
reachability assessment.

| Image | HIGH occurrences | CRITICAL occurrences | Unique IDs |
|---|---:|---:|---:|
| Kafka 4.3.1 | 10 | 0 | 9 |
| PostgreSQL 17.11 Bookworm | 21 | 1 | 22 |
| Metabase 0.63.13 | 1 | 0 | 1 |
| Airflow 3.3.0 Python 3.12 base | 47 | 3 | 48 |
| Fresh local Airflow image | 47 | 3 | 48 |

The PostgreSQL CRITICAL is `CVE-2025-68121` in Go `stdlib` v1.24.6; Trivy lists
1.24.13, 1.25.7, and 1.26.0-rc.3 as fixes. The three Airflow CRITICAL occurrences are
`CVE-2026-35030`, `CVE-2026-42208`, and `CVE-2026-49468` in `litellm==1.82.6`, with
scanner-listed fixes beginning at 1.83.0, 1.83.7, and 1.84.0 respectively. The
Metabase HIGH is `CVE-2025-59250` in the bundled Microsoft SQL Server JDBC driver
13.2.1. No unsupported in-image package overrides were applied.

This is not an all-severity, configuration, secret, or production-risk assessment.
Public source visibility does not deploy the affected local demo images.

### Source secret scan

The same pinned Trivy scanner inspected an explicit snapshot of all tracked and
untracked, non-ignored working-tree files with only the secret scanner enabled. The
final scan returned exit code 0 with zero secret findings. `.git`, `.venv`,
`.artifacts`, and other ignored local files were outside that source-publication scope.
The temporary snapshot was removed; its JSON report remains under ignored
`.artifacts`.

An earlier history review of the unchanged `origin/main` found no common secret
prefixes, private keys, sensitive historical paths, large historical blobs, Actions
secrets, Dependabot secrets, or Actions artifacts. That earlier review supplements
but does not enlarge the current working-tree scan scope.

## Metabase validation context

Metabase 0.63.13 was previously run from the exact digest above against an isolated
copy of the Metabase application-data volume. The live UI and checked-in dashboards
were inspected in that isolated upgrade context, and the original named volume was
retained. The current SQL contracts remain the portable open-source source of truth;
paid Metabase serialization is not claimed.

## Acceptance status and limitations

The publication decision accepts these explicit limitations:

- The all-images-and-dependencies-pinned checklist item remains incomplete because the
  Airflow overlay's transitive resolution has not been locked and proved.
- Python and container vulnerability audits have unresolved fixable findings; neither
  audit is clean.
- The demo has no live GitHub App credentials or completed live pull-request lifecycle.
- Failed GitHub delivery discovery/redelivery and external alert dispatch are deferred.
- Kafka is a plaintext, single-broker local topology with replication factor one.
- Airflow validation covers image, DAG, and local analytics behavior rather than a
  production scheduler or executor topology.
- Metabase setup is manual, local, and based on checked-in SQL and screenshots.
- Exact-SHA linkage does not infer Git ancestry, and API history limits can produce
  explicit backfill coverage gaps.

Unavailable metrics remain null with exclusion reasons. CI failures are not treated as
production failures, and repository/service aggregates are not contributor rankings.

## Final working-tree validation

Validation used the standalone uv 0.12.5 executable under ignored `.artifacts` so the
running tool did not need to replace itself inside `.venv`.

| Command | Observed result |
|---|---|
| `uv lock --check` | Passed; resolved 91 packages without changing `uv.lock`. |
| `make UV=.artifacts/uv-bin/uv.exe check` | Passed: 74 files were already formatted, Ruff lint was clean, strict mypy was clean across 69 source files, and 336 tests were collected. The result was 329 passed and 7 opt-in live tests skipped, with measured 100% statement and branch coverage. Core Compose configuration also passed. |
| `uv build --out-dir .artifacts/publication-build-20260819` | Passed; built the sdist and wheel, proving the MIT project metadata is accepted by the build backend. |
| Core and dashboard-profile `docker compose ... config --quiet` | Both passed. |
| `make UV=.artifacts/uv-bin/uv.exe dashboard-sql-check` | Passed all eight SQL contracts; row counts were 40, 20, 25, 7, 3, 5, 4, and 4. |
| `make UV=.artifacts/uv-bin/uv.exe airflow-image` | Passed from the exact Airflow base digest. The cached install layer includes `pip check`. Docker warned that Git safe-directory handling prevented capture of current commit provenance; the build itself passed. |
| `make UV=.artifacts/uv-bin/uv.exe airflow-dag-check` | Passed; loaded `analytics_refresh` and `github_backfill` with zero import errors. |
| First `make UV=.artifacts/uv-bin/uv.exe airflow-analytics-check` | Failed because dbt freshness correctly classified the fixture watermark as stale. This attempt is not counted as passed. |
| Transactional fixture reload | Passed; reloaded only `raw.github_events_fixture` with 32 synthetic rows and committed. It did not truncate or mutate `raw.github_events`. |
| Retried `make UV=.artifacts/uv-bin/uv.exe airflow-analytics-check` | Passed: source freshness, the contracted dbt build, and success persistence all completed, and the Airflow DAG run finished successfully. |
| Pinned Trivy 0.74.0 working-tree secret scan | Passed with zero findings across all tracked and untracked, non-ignored source files. |

The live dashboard and Airflow checks used the healthy preserved PostgreSQL container
already running locally. That container still reports the earlier 17.10 image; the
checked-in 17.11 reference was configuration-validated and vulnerability-scanned but
was not substituted at runtime during this review. No PostgreSQL or Metabase named
volume was deleted or recreated.
