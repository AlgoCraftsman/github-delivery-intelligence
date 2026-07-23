# ADR 0002: One versioned raw Kafka topic

- Status: Accepted
- Date: 2026-07-22

## Context

Event-specific topics and an external schema registry add operational surface before the
MVP has independently deployed producers and consumers.

## Decision

All supported webhook families use `github.events.raw.v1`, keyed by repository ID. A
checked-in JSON Schema and Pydantic model define the versioned envelope. Unprocessable
records use `github.events.dlq.v1`.

## Consequences

Consumers route by envelope fields and must tolerate unknown payload fields. Schema
Registry and Avro remain deferred until organizational or contract-evolution pressure
justifies them.
