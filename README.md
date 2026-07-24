# GitHub Delivery Intelligence

Event-driven engineering analytics using GitHub App webhooks, Kafka, Airflow, dbt,
PostgreSQL, and Metabase to produce replayable PR-flow and evidence-aware software
delivery metrics.

The project is being built from the reviewed [15-day build plan](BUILD_PLAN.md). The
current Day 4 checkpoint provides a signed webhook-to-Kafka acknowledgement path and a
manual-commit warehouse consumer that lands versioned events idempotently in PostgreSQL.

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

## Current scope

Day 4 establishes raw event storage, duplicate absorption, database-before-offset
ordering, crash-window replay safety, and acknowledged poison-record DLQ handling.
The independent PR monitor, backfill, analytics models, orchestration, dashboards, and
broader failure drills follow in the order documented by the build plan. The local
single-broker Kafka deployment demonstrates client semantics; it is not a production
availability topology.
