# GitHub Delivery Intelligence

Event-driven engineering analytics using GitHub App webhooks, Kafka, Airflow, dbt,
PostgreSQL, and Metabase to produce replayable PR-flow and evidence-aware software
delivery metrics.

The project is being built from the reviewed [15-day build plan](BUILD_PLAN.md). The
current Day 3 checkpoint provides a signed webhook-to-Kafka acknowledgement path,
versioned event contract, sanitized fixtures, and service health and metrics endpoints
on the Day 1 platform foundation.

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
make ps
```

`make up` starts Kafka on `localhost:9092` and PostgreSQL on `localhost:55432`, then waits
for both health checks. The credentials in `.env.example` are local-only defaults.

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

## Current scope

Day 3 establishes request validation, the event contract, and durable Kafka
acknowledgement at the HTTP boundary. Consumers, backfill, analytics models,
orchestration, dashboards, and broader failure drills follow in the order documented by
the build plan. The local single-broker Kafka deployment demonstrates client semantics;
it is not a production availability topology.
