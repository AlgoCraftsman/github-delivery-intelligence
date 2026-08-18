# Security

This document separates implemented MVP controls from production guidance. Values in
`.env.example`, Compose, migrations, and dashboard setup are local-demo defaults only;
they are not production-safe credentials.

## Trust boundaries and data flow

1. Untrusted internet input crosses the FastAPI webhook boundary.
2. Validated envelopes cross into the Kafka raw topic only after signature, header,
   event-type, body-size, JSON, and model validation.
3. Independent consumers cross from Kafka into PostgreSQL or the Kafka DLQ.
4. Backfill crosses from authenticated GitHub APIs directly into append-only raw
   storage through transactional page writes.
5. dbt transforms raw JSON into analytics schemas.
6. Metabase crosses only into analytics schemas through a restricted database role.

The local Compose network and host ports are a development convenience, not a trusted
production perimeter.

## Implemented webhook controls

The receiver computes HMAC-SHA-256 over the original request bytes and compares the
expected `sha256=` value with `hmac.compare_digest`. This follows GitHub's guidance to
validate `X-Hub-Signature-256` before processing and use a timing-safe comparison:
[Validating webhook deliveries](https://docs.github.com/en/webhooks/using-webhooks/validating-webhook-deliveries).

The receiver also:

- enforces `GITHUB_WEBHOOK_MAX_BODY_BYTES` while reading the stream and rejects an
  oversized body before parsing it;
- requires non-empty `X-Hub-Signature-256`, `X-GitHub-Event`, and
  `X-GitHub-Delivery` headers;
- accepts only `pull_request`, `pull_request_review`, `workflow_run`, `deployment`,
  and `deployment_status`;
- validates a strict versioned outer envelope and the required repository and
  installation identities while preserving the original JSON payload;
- returns generic client errors rather than exposing secrets or payload contents; and
- returns `202` only after Kafka acknowledges delivery. Queue, callback, and timeout
  failures return bounded `503` responses.

Kafka producer idempotence reduces duplicate production attempts, but the durable
contract is still at-least-once processing plus idempotent database effects.

## GitHub App permissions

Request the minimum repository permissions needed for the selected events and
backfill:

- Pull requests: read, for `pull_request`, `pull_request_review`, and GraphQL PR,
  review, and commit history.
- Actions: read, for `workflow_run` events and workflow-run REST backfill.
- Deployments: read, for `deployment`, `deployment_status`, and deployment REST
  backfill.
- Contents and Metadata: read, for repository and commit metadata used by the
  backfill client.

GitHub documents the event permission mapping in
[Webhook events and payloads](https://docs.github.com/en/webhooks/webhook-events-and-payloads)
and recommends selecting only required permissions in
[Choosing permissions for a GitHub App](https://docs.github.com/en/apps/creating-github-apps/registering-a-github-app/choosing-permissions-for-a-github-app).
The deterministic demo needs none of these permissions because it never contacts
GitHub.

## Secrets and fixtures

Runtime secrets must be injected through the process environment or a production
secret manager. Do not commit personal access tokens, installation tokens, GitHub App
private keys, webhook secrets, signatures, real webhook payloads, connection strings
containing real credentials, or environment dumps. `.env` files are ignored; only the
deliberately fake `.env.example` is tracked.

The checked-in fixtures use synthetic repositories, users, installations, delivery
IDs, commits, and payload extensions. New fixtures must be sanitized and must not be
copied from a real delivery unless every identifying and secret-bearing field is
reviewed and replaced. The deterministic loader truncates and reloads only
`raw.github_events_fixture`; it never truncates or mutates `raw.github_events`.

The application emits structured, bounded operational summaries. Webhook errors are
generic; consumer success logs contain outcome and Kafka lineage, not raw payloads;
analytics refresh persistence uses bounded sanitized artifact summaries rather than
command output, full logs, credentials, payloads, or tracebacks. DLQ records retain
base64-encoded source bytes by design and must therefore be access-controlled and
handled as potentially sensitive data.

## PostgreSQL authorization

The local application owner `github_analytics` creates and writes the schemas. The
`metabase_reader` migration creates a non-superuser login with no create-database,
create-role, inheritance, or replication privileges. It:

- has `CONNECT` to the database;
- has `USAGE` and `SELECT` only on `analytics_staging`,
  `analytics_intermediate`, and `analytics_marts`; and
- is explicitly revoked from `raw`, `serving`, `ops`, and `analytics` schemas.

Default privileges grant future analytics tables to the reader. Any grant or schema
change must re-prove that mart reads succeed and direct access to `raw`, `serving`, and
`ops` is denied. The checked-in password `local_only_read_only` and application
password `local_only_change_me` are examples for an isolated workstation only.

## Local limitations

- Kafka uses plaintext listeners without TLS or SASL, one broker, replication factor
  one, and host port exposure.
- PostgreSQL and Metabase use local example passwords and host ports without a
  production network policy.
- Compose volumes are durable on one workstation but are not a backup strategy.
- There is no automated secret rotation or dual-webhook-secret overlap.
- There is no automated failed-webhook reconciliation or redelivery service.
- DLQ payload encryption, retention, and access policy are not modeled locally.
- The Airflow artifact is an image and DAG smoke path, not a hardened scheduler,
  executor, API, or identity deployment.

## Production hardening guidance

Before production use:

- terminate TLS at a controlled ingress, restrict request methods and paths, apply
  rate limits, and retain the raw-byte HMAC verification at the application boundary;
- inject versioned secrets from a secret manager, audit access, and perform coordinated
  rotations without writing secrets to logs or images;
- use encrypted and authenticated Kafka listeners, authorization per topic, at least
  three brokers, appropriate replication/minimum in-sync replicas, quotas, and tested
  backup/disaster procedures for dependent state;
- use managed or hardened PostgreSQL with TLS, separate least-privilege application,
  migration, analytics, and dashboard roles, network policy, backups, restore drills,
  and audit logging;
- run receiver and consumers under independent supervisors with bounded retries,
  health checks, resource limits, and sanitized centralized telemetry;
- restrict and monitor access to raw and DLQ payloads, define retention/deletion policy
  consistent with legal requirements, and encrypt sensitive storage;
- deploy a real Airflow control plane with authenticated access and isolated secrets;
  and
- implement reconciliation for failed GitHub deliveries, recording only bounded
  delivery metadata and preserving idempotent delivery IDs.

Rotation and recovery procedures are in the [operations runbook](operations-runbook.md).
