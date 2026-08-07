# Instructions for coding agents working in this repository

These instructions govern coding agents working in this repository. Use AI assistance as part of a reviewable engineering workflow: inspect the current state, make scoped changes, verify claims with evidence, and preserve human accountability for architecture, data safety, and metric interpretation.

## Purpose and engineering judgment

- Inspect the current Git state, relevant code, documentation, tests, migrations, and checked-in contracts before changing anything.
- AI assistance supports engineering judgment; it does not replace source review, validation, or accountable decisions.
- If uncertain, say so rather than fabricating. Never invent APIs, versions, validation evidence, benchmarks, screenshots, citations, statistics, or quotes.
- Use authoritative, current sources when accuracy depends on changing external information.

## Sources of truth and scope

- Treat `BUILD_PLAN.md`, current repository documentation, tests, migrations, and checked-in contracts as the sources of truth for intended behavior.
- Follow the requested scope. Do not silently start later build-plan milestones or redo completed milestone work unless the request requires it.
- Preserve established architecture and naming conventions unless a change is justified and tested.
- Distinguish diagnosis, implementation, and review requests. Do not broaden the requested action without permission.

## Git and worktree discipline

- Never implement directly on `main`. Use an appropriately named feature branch.
- Run `git status --short --branch` before editing. Preserve unrelated changes and stop if edits unexpectedly overlap.
- Commit only when the task authorizes commits, and keep commits intentional and reviewable.
- Do not create or modify GitHub issues or pull requests unless explicitly requested.
- Never discard user work or use destructive Git operations such as `git reset --hard`.
- Run `git diff --check` and review the final diff before handoff.

## Data and infrastructure safety

- Never delete or recreate PostgreSQL or Metabase named volumes merely to apply a migration or fix a test. Apply idempotent migrations with `make migrate`.
- Never reset production-like or shared data without explicit authorization.
- Fixture validation may transactionally reload only `raw.github_events_fixture`. It must not truncate or mutate `raw.github_events`.
- Preserve append-only raw-event semantics.
- Treat example passwords as local-demo values only; never describe them as production-safe.
- Never commit secrets, tokens, private keys, signatures, real payloads, or environment dumps.
- Re-prove reader isolation when analytics grants or schemas change. The Metabase reader must not gain direct access to `raw`, `serving`, or `ops`.

## Architecture and claim integrity

- Describe delivery as at-least-once processing with idempotent durable effects, not end-to-end exactly-once processing.
- Establish Kafka delivery acknowledgement before reporting webhook success.
- Advance Kafka offsets only after durable database effects or acknowledged dead-letter-queue handling.
- Keep streaming consumers as long-running services outside Airflow. Airflow must not poll Kafka or orchestrate individual events.
- Keep orchestration logic thin and application behavior in tested packages.
- Do not describe the local single-broker or local Airflow test topology as production infrastructure.
- Do not claim paid Metabase serialization is part of the open-source workflow.

## Metric and analytics integrity

- Do not create contributor rankings or individual-performance reporting.
- Do not describe CI failures as production failures.
- Do not infer incidents, rollbacks, intervention, dead-letter-queue evidence, or deployment rework without modeled evidence.
- Preserve the meanings of `measured`, `configured_proxy`, and `unavailable`. Keep unavailable values null and provide an explicit exclusion reason.
- Preserve coverage numerator, denominator, and ratio semantics.
- Do not fabricate duplicate-delivery counts, dead-letter-queue incidents, failure-drill results, throughput numbers, or latency benchmarks.
- Report only validation evidence actually observed. Do not compare unrelated repositories or services as though their metrics were interchangeable.

## Implementation conventions

- Target Python 3.12 and use the locked `uv` environment.
- Maintain strict typing, Ruff formatting and linting, and the repository's measured 100% Python coverage standard.
- Pin runtime dependencies and container images deliberately.
- In application code, execute subprocesses with argument lists; never construct shell command strings from runtime values.
- Validate configuration and source identifiers before using them in SQL or commands.
- Store only bounded, sanitized operational summaries, never unbounded logs or traces.
- Keep database migrations idempotent and safe for existing volumes.
- Add tests proportional to the risk of the change.
- Keep DAG files thin and delegate behavior to tested application packages.

## Verification

Run checks proportional to the change. A documentation-only edit does not require Docker-heavy validation, but milestone-completion claims require the full relevant evidence.

Baseline checks:

```powershell
& '.\.venv\Scripts\uv.exe' lock --check
make UV=.venv/Scripts/uv.exe check
docker compose -f infra/docker-compose.yml config --quiet
docker compose -f infra/docker-compose.yml --profile dashboards config --quiet
git diff --check
```

Add focused validation for each affected area:

- Database schema: start PostgreSQL without deleting volumes, run `make migrate`, and verify migration idempotency.
- dbt: run source freshness and the contracted fixture-backed build with the `github_events_fixture` source override for deterministic checks.
- Dashboards: run `make UV=.venv/Scripts/uv.exe dashboard-sql-check`.
- Airflow: run `make UV=.venv/Scripts/uv.exe airflow-image` and `make UV=.venv/Scripts/uv.exe airflow-dag-check`; run the relevant real smoke target when runtime behavior changes.
- Reader or security changes: prove permitted mart reads and expected `ops` and `raw` denials.

Never claim a check passed if it was skipped, failed, or only statically inspected.

## Documentation and handoff

- Update public documentation when commands, behavior, schemas, metrics, or limitations change.
- Update milestone or checkpoint language only after implementation and evidence are complete.
- Distinguish CI failures from production failures and observed results from planned acceptance targets.
- Report files changed, commands run, actual results, limitations, and remaining work.
