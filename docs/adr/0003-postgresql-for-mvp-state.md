# ADR 0003: PostgreSQL for MVP durable state

- Status: Accepted
- Date: 2026-07-22

## Context

The MVP needs an append-only raw store, consumer projections, an alert outbox, and
analytical marts. Adding Redis or object storage would create more failure modes without
a demonstrated workload that needs them.

## Decision

PostgreSQL 17 stores raw events, serving projections, operational state, and analytics
marts in separate schemas. Kafka remains the replayable streaming boundary.

## Consequences

The local stack is small and durable across process restarts. Redis and object storage
are scale-out options, not implied components of the MVP.
