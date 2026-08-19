# ADR 0004: uv for application packaging

- Status: Accepted
- Date: 2026-07-22
- Amended: 2026-08-19

## Context

Application services need deterministic Python environments across local development and
CI. Airflow has its own supported constraints-based installation process.

## Decision

Use Python 3.12 and uv with a committed `uv.lock` for application code. Pin direct
dependencies exactly and install with `uv sync --frozen` in CI. Build the later Airflow
image using Apache Airflow's official constraints instead of the application lock.

## Consequences

Dependency updates are explicit, reviewable changes. CI pins uv 0.12.5 exactly. The
project accepts uv versions from 0.12.1 through the 0.12 release line so GitHub's
Dependabot updater and reviewed patch releases can operate without admitting a later
minor release. Earlier Day 13 through Day 15 evidence remains tied to the uv 0.11.29
toolchain that actually produced it.
