# ADR 0001: At-least-once delivery with idempotent effects

- Status: Accepted
- Date: 2026-07-22

## Context

GitHub can redeliver webhooks, Kafka consumers can replay records, and a consumer can
crash after committing a database transaction but before committing its Kafka offset.
Claiming end-to-end exactly-once delivery would therefore be misleading.

## Decision

The platform provides at-least-once processing. Consumers commit database transactions
before Kafka offsets, and durable effects use stable source identities and uniqueness
constraints so replay is harmless. Poison records must be acknowledged by the DLQ before
the source offset advances.

## Consequences

Duplicate delivery is expected and measurable. Every new side effect must define an
idempotency key and a crash-window test.
