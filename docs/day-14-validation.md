# Day 14 fresh-clone validation

Validation date: 2026-08-18

Validated commit: `d94781cedff8b0867d91e0cd516869133819d752`

Result: **pass**. A new isolated clone followed the README quickstart successfully.

## Isolation and prerequisites

The unpushed feature branch was exported as a verified local Git bundle, then cloned
into a new ignored `.artifacts` directory. The clone resolved to the validated commit
above and created its own `.venv`.

The pinned uv 0.11.29 executable was placed on `PATH` as the declared prerequisite;
no source package or virtual environment was reused. The fresh clone created and
populated its own `.venv` from `uv.lock`.

The existing Compose stack was stopped with `make down`, which preserved its named
volumes. The validation clone used the distinct Compose project
`github-delivery-intelligence-day14-fresh-default` on the documented default ports.
After validation, its containers were stopped without `-v`, its PostgreSQL volume was
retained, and the original stack was restored.

## Commands executed from the fresh clone

These are the README commands, in order:

```bash
uv sync --frozen
make demo
make ps
```

No `.env`, GitHub token, webhook secret, private key, real repository, or private
payload was supplied.

## Observed outcomes

- `uv sync --frozen` used CPython 3.12.13, created a new `.venv`, and installed the
  locked 90 packages in 3.056 seconds.
- `make demo` completed in 84.814 seconds.
- Kafka and PostgreSQL reached healthy state.
- Both Kafka topics were created in the isolated project.
- All idempotent migrations applied to the new PostgreSQL volume.
- The fixture loader inserted 32 synthetic rows after replacing only
  `raw.github_events_fixture`.
- dbt source freshness passed.
- dbt completed 322 nodes: 322 passed, 0 warnings, 0 errors, and 0 skips.
- All eight versioned dashboard SQL contracts passed with their expected row counts.
- The demo printed 25 latest-reporting-date metric rows, including measured,
  configured-proxy, unavailable, coverage, and exclusion evidence.
- The demo printed `Deterministic demo completed successfully.` and exited zero.
- `make ps` showed isolated Kafka healthy on port 9092 and PostgreSQL healthy on port
  55432.

## Service and volume state after validation

The isolated containers and network were removed with ordinary `make down`; the
isolated PostgreSQL named volume was retained. The original Kafka and PostgreSQL
containers were recreated from their preserved project state and both were observed
healthy. The original PostgreSQL and Metabase volumes remained present. No volume was
deleted or recreated as a migration or recovery step, and append-only
`raw.github_events` was not truncated or mutated by the demo.

## Limitation found during isolation planning

An earlier isolated attempt set `KAFKA_PORT=19092` so it could run concurrently with
the original stack. The broker started, but its in-container health command followed
the host-advertised `localhost:19092` address and timed out because the broker listens
on container port 9092. That attempt was stopped without deleting its PostgreSQL
volume. The supported documented quickstart uses the default Kafka port; validation
therefore preserved and stopped the original stack, used a distinct Compose project on
the default ports, and restored the original stack afterward.

This validation proves the deterministic local reviewer path. It does not add live
GitHub App lifecycle evidence or production infrastructure, availability, security,
capacity, or latency evidence.
