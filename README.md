# GitHub Delivery Intelligence

Event-driven engineering analytics using GitHub App webhooks, Kafka, Airflow, dbt,
PostgreSQL, and Metabase to produce replayable PR-flow and evidence-aware software
delivery metrics.

The project is being built from the reviewed [15-day build plan](BUILD_PLAN.md). The Day 1
foundation provides a typed Python package, deterministic dependency management, local
Kafka and PostgreSQL services, CI quality gates, and architecture decision records.

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

## Current scope

Day 1 establishes the repository and runtime foundation. Webhook ingestion, consumers,
backfill, analytics models, orchestration, dashboards, and failure drills follow in the
order documented by the build plan. The local single-broker Kafka deployment demonstrates
client semantics; it is not a production availability topology.
