# ADR 0004: uv for application packaging

- Status: Accepted
- Date: 2026-07-22

## Context

Application services need deterministic Python environments across local development and
CI. Airflow has its own supported constraints-based installation process.

## Decision

Use Python 3.12 and uv with a committed `uv.lock` for application code. Pin direct
dependencies exactly and install with `uv sync --frozen` in CI. Build the later Airflow
image using Apache Airflow's official constraints instead of the application lock.

## Consequences

Dependency updates are explicit, reviewable changes. Contributors need uv 0.11.29, and
the project rejects incompatible uv versions.
