# GitHub Delivery Intelligence

Event-driven engineering analytics using GitHub App webhooks, Kafka, Airflow, dbt,
PostgreSQL, and Metabase to produce replayable PR-flow and evidence-aware software
delivery metrics.

The project is being built from the reviewed [15-day build plan](BUILD_PLAN.md). The
current Day 2 checkpoint adds a versioned event envelope, checked-in JSON Schema,
sanitized webhook fixtures, and a raw-body HMAC receiver to the Day 1 platform
foundation.

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

Day 2 deliberately does not include the Kafka producer. The application fails closed
with `503` when no publisher is configured; a `2xx` response is possible only after an
injected publisher returns successfully. Day 3 will implement that interface with a
bounded Kafka delivery acknowledgement.

## Current scope

Day 2 establishes request validation and the event contract. Durable Kafka publishing,
consumers, backfill, analytics models, orchestration, dashboards, and failure drills
follow in the order documented by the build plan. The local single-broker Kafka
deployment demonstrates client semantics; it is not a production availability topology.
