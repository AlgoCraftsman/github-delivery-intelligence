# GitHub Delivery Intelligence

Event-driven engineering analytics using GitHub App webhooks, Kafka, Airflow, dbt,
PostgreSQL, and Metabase to produce replayable PR-flow and evidence-aware software
delivery metrics.

The project is being built from the reviewed [15-day build plan](BUILD_PLAN.md). The
current Day 5 checkpoint provides the signed webhook and idempotent raw warehouse path
plus an independent PR monitor with a durable open-PR projection and alert outbox.

## Prerequisites

- Python 3.12.13
- uv 0.11.29
- Docker Desktop with Docker Compose v2
- GNU Make (optional; the underlying commands also work directly)

## Foundation quickstart

```bash
uv sync --frozen
make check
make up
make migrate
make ps
```

`make up` starts Kafka on `localhost:9092` and PostgreSQL on `localhost:55432`, then waits
for both health checks and creates the raw and DLQ topics if absent. `make migrate`
applies the idempotent raw-table migration to an existing named volume. The credentials
in `.env.example` are local-only defaults.

Stop services without deleting data:

```bash
make down
```

## Webhook contract

The receiver supports `pull_request`, `pull_request_review`, `workflow_run`,
`deployment`, and `deployment_status`. It rejects invalid signatures, missing delivery
headers, unsupported events, malformed payloads, and bodies larger than
`GITHUB_WEBHOOK_MAX_BODY_BYTES`.

The outer envelope is strict and versioned. The original JSON payload remains intact and
accepts unknown fields. Synthetic fixtures for all five event families live under
`tests/fixtures`.

The application fails closed with `503` when no publisher is configured. Day 3
implements the publisher with `acks=all`, producer idempotence, repository-keyed
records, and a bounded delivery callback. Enqueuing a record locally is not treated as
success: the receiver returns `202` only after Kafka reports delivery and returns `503`
on queue, callback, or timeout failure.

Run the local receiver after copying `.env.example` to `.env`, changing the webhook
secret, and starting the core services:

```bash
cp .env.example .env
make up
make webhook
```

The receiver listens on `http://127.0.0.1:8000` by default:

- `POST /webhooks/github` validates and durably publishes supported events.
- `GET /health/live` reports process liveness.
- `GET /health/ready` checks Kafka metadata availability.
- `GET /metrics` exposes request, publish-result, and publish-latency metrics in
  Prometheus text format.

## Warehouse writer

The `warehouse-writer` consumer group reads `github.events.raw.v1`, validates the
versioned envelope, and inserts the complete original payload into
`raw.github_events`. The durable uniqueness boundary is `(source, source_record_key)`;
webhook rows use the real GitHub delivery ID as the source record key.

Run it after copying `.env.example` to `.env`, starting the services, and applying the
migration:

```bash
make up
make migrate
make warehouse
```

Automatic Kafka commits and automatic offset storage are disabled. For a valid record,
the writer commits PostgreSQL before synchronously committing that message's Kafka
offset. A replay after a crash in between those operations reaches the uniqueness
constraint and creates no second raw row.

Unprocessable records are published to `github.events.dlq.v1` with their original bytes
base64 encoded and their source topic, partition, and offset retained. The writer waits
for the DLQ delivery callback before committing the source offset. If PostgreSQL, DLQ
publishing, or offset commit fails, the worker stops without advancing past an
unconfirmed durable boundary.

This is at-least-once processing with idempotent database effects. It is not end-to-end
exactly-once delivery.

## Pull-request monitor

The independent `pr-monitor` consumer group reads the same raw topic without sharing
progress with `warehouse-writer`. It maintains `serving.open_pull_requests`, selects the
earliest submitted review from someone other than the PR author, and retains a source
watermark so delayed snapshots cannot reopen or regress newer PR state.

Run it after applying the migrations:

```bash
make up
make migrate
make pr-monitor
```

The default stale threshold is 24 hours and the sweep interval is 60 seconds. Both are
configurable through `PR_MONITOR_STALE_AFTER_HOURS` and
`PR_MONITOR_STALE_SWEEP_INTERVAL_SECONDS`. Draft PRs and PRs with an eligible review are
excluded from the sweep.

Each eligible stale PR creates an `ops.alert_outbox` row with a unique key derived from
the real repository and pull-request identities. Repeated sweeps create no duplicate
effect. Closing a PR removes it from the open projection and cancels its still-pending
alert. External Slack dispatch remains deferred; the outbox is the durable Day 5
boundary.

## Current scope

Day 5 adds independent consumer progress, monotonic open-PR state, first eligible review
selection, and duplicate-safe stale alert intents to the Day 4 raw ingestion boundary.
Backfill, analytics models, orchestration, dashboards, external alert delivery, and
broader failure drills follow in the order documented by the build plan. The local
single-broker Kafka deployment demonstrates client semantics; it is not a production
availability topology.
