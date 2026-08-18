# Operations runbook

This runbook covers the implemented local MVP. Commands assume the repository root,
the local defaults, and a locked Python environment. Never use a volume-removing
Compose command for normal migration, restart, or recovery.

## Start, inspect, and stop

Install the locked environment once, start the core services, create topics, and apply
all idempotent migrations:

```bash
uv sync --frozen
make up
make migrate
make ps
```

`make up` waits for Kafka and PostgreSQL health checks and creates
`github.events.raw.v1` and `github.events.dlq.v1` if absent. `make migrate` is safe to
repeat against an existing named PostgreSQL volume.

Start or stop the optional Metabase profile:

```bash
make dashboard-up
make dashboard-down
```

Stop the core containers while preserving PostgreSQL and Metabase named volumes:

```bash
make down
```

Inspect status and bounded recent logs before changing anything:

```bash
make ps
docker compose -f infra/docker-compose.yml logs --tail 100 kafka postgres
```

## Health and readiness

- Kafka and PostgreSQL must show `healthy` in `make ps`.
- After starting the receiver, `GET /health/live` proves process liveness and
  `GET /health/ready` performs a bounded Kafka metadata check.
- `GET /metrics` exposes receiver request, publish result, and publish-latency metrics.
- Metabase health is available at `http://localhost:3000/api/health` when its profile
  is running.

Example receiver checks:

```bash
curl --fail http://127.0.0.1:8000/health/live
curl --fail http://127.0.0.1:8000/health/ready
curl --fail http://127.0.0.1:8000/metrics
```

## Webhook receiver and consumers

Copy `.env.example` to ignored `.env`, replace the webhook secret for any real GitHub
connection, and use separate supervised terminals or service managers:

```bash
make webhook
make warehouse
make pr-monitor
```

The receiver must acknowledge Kafka before returning `202`. `warehouse-writer` and
`pr-monitor` are independent long-running consumer groups. Airflow must not be used to
poll Kafka or invoke one task per event.

On an unexpected worker exit, inspect the terminal or sanitized service logs, confirm
Kafka and PostgreSQL health, correct the dependency or configuration problem, and
restart that worker. Manual offsets commit only after a durable database effect or an
acknowledged DLQ effect, so an uncommitted record is replayed.

## Deterministic reviewer demo and replay

Run the ordinary, volume-preserving demo:

```bash
make demo
```

It starts Kafka and PostgreSQL, reapplies topics and migrations, transactionally
reloads only `raw.github_events_fixture`, runs fixture-backed dbt freshness and build
assertions, validates dashboard SQL snapshots, and prints the modeled metric rows. It
does not contact GitHub, mutate `raw.github_events`, start Metabase, run Airflow, or
perform outage drills.

Repeating `make demo` is deterministic: the fixture loader replaces only the dedicated
fixture relation. For full webhook duplicate, crash-window, DLQ, and dependency-outage
evidence, use `make day13-evidence` only when those destructive-but-volume-preserving
drills are intentionally required. That workflow appends uniquely identified synthetic
rows to the live raw table and temporarily stops dependencies; it is not the ordinary
reviewer demo.

## Duplicate handling

Duplicate webhook deliveries are expected in an at-least-once system. The warehouse
writer uses unique `(source, source_record_key)` identity; a duplicate returns the
internal `duplicate` outcome without another raw row. The PR projection uses source
watermarks, and stale-alert intents use stable unique alert keys.

Do not delete duplicate evidence or reset offsets to hide it. If duplicate effects are
suspected:

1. Preserve the delivery ID, source topic/partition/offset, consumer group, and bounded
   error summary.
2. Query by the exact delivery ID without changing data:

   ```bash
   docker compose -f infra/docker-compose.yml exec -T postgres psql \
     --username github_analytics --dbname github_analytics \
     --command "SELECT source, source_record_key, kafka_partition, kafka_offset, ingested_at FROM raw.github_events WHERE delivery_id = 'REPLACE_WITH_DELIVERY_ID';"
   ```

3. Confirm that at most one raw row exists and inspect the relevant consumer-group
   offset. Do not infer duplicate durable effects from repeated HTTP attempts alone.

## DLQ inspection

Describe the DLQ and inspect only the bounded number of records needed for diagnosis:

```bash
docker compose -f infra/docker-compose.yml exec -T kafka \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --describe --topic github.events.dlq.v1

docker compose -f infra/docker-compose.yml exec -T kafka \
  /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic github.events.dlq.v1 --from-beginning --max-messages 1
```

DLQ values contain base64-encoded original bytes and lineage and may be sensitive.
Restrict their output, do not paste them into issues or logs, and stop the consumer if
no record is available. A DLQ entry proves a poison-record outcome; it does not prove a
production incident.

## Offset-reset safeguards

No routine offset-reset target is provided. Before any reset:

1. Obtain explicit authorization for the named consumer group, topic, partitions, and
   target offsets.
2. Stop only that consumer and verify no second instance is running.
3. Record current offsets and lag:

   ```bash
   docker compose -f infra/docker-compose.yml exec -T kafka \
     /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
     --describe --group warehouse-writer
   ```

4. Explain expected replay effects and confirm database uniqueness/projection
   idempotency for the affected records.
5. Prefer proving the reset with a new isolated test group. Never reset both production
   groups merely to make lag disappear.
6. After an authorized reset, restart one consumer, watch bounded logs and lag, and
   verify durable counts before proceeding.

An offset reset cannot reconstruct source events already removed by Kafka retention.

## Kafka outage recovery

Symptoms include receiver readiness failure, bounded webhook `503`, producer callback
failure, and stopped or stalled consumers.

1. Run `make ps` and inspect `docker compose -f infra/docker-compose.yml logs --tail 100 kafka`.
2. Start the existing broker container without deleting volumes:

   ```bash
   docker compose -f infra/docker-compose.yml up -d --wait kafka
   make topics
   ```

3. Confirm receiver readiness, then restart any exited consumers.
4. Check both consumer groups independently. Uncommitted messages replay; database and
   projection identities must absorb duplicates.
5. Reconcile GitHub failures separately. The repository has no automatic redelivery
   worker.

GitHub states that failed webhook deliveries are not automatically redelivered. App
owners or managers can use the GitHub UI for recent deliveries, or an explicitly
authenticated implementation can use the documented API. Do not improvise an endpoint
or token type; follow [Redelivering webhooks](https://docs.github.com/en/webhooks/testing-and-troubleshooting-webhooks/redelivering-webhooks)
and [GitHub App webhook endpoints](https://docs.github.com/en/rest/apps/webhooks).

## PostgreSQL outage recovery

Symptoms include consumer storage errors, stopped consumers, dbt connection failure,
and Metabase query errors. The receiver can continue acknowledging Kafka while broker
retention and capacity remain adequate.

1. Inspect status and bounded PostgreSQL logs.
2. Start the existing database container and wait for health:

   ```bash
   docker compose -f infra/docker-compose.yml up -d --wait postgres
   make migrate
   ```

3. Restart exited consumers. Records whose database effect was not committed replay;
   records committed before an offset failure are absorbed as duplicates.
4. Run `make dbt-debug` before resuming analytics refreshes.
5. Do not delete or recreate the volume to apply a migration or clear a failure.

## Backfill checkpoints and resume

Inspect checkpoints without changing them:

```bash
docker compose -f infra/docker-compose.yml exec -T postgres psql \
  --username github_analytics --dbname github_analytics \
  --command "SELECT repository_id, resource, scope, window_start, window_end, cursor, status, pages_completed, records_inserted, updated_at FROM raw.backfill_checkpoints ORDER BY updated_at DESC LIMIT 25;"
```

Re-run the same repository and half-open time window with `make backfill`. The command
loads its opaque GraphQL cursor or next REST page from PostgreSQL. Each source page and
checkpoint update share one transaction, so a failed page does not advance the cursor.
Never edit an opaque cursor by hand. A completed checkpoint deliberately has a null
cursor.

GitHub API retention and result limits can leave explicit history gaps. Narrow the
window when the client reports the workflow-run cap; do not mark a checkpoint complete
by hand.

## Analytics refresh and Airflow

For direct local analytics checks:

```bash
make dbt-debug
make dbt-freshness
make dbt-build
make dashboard-sql-check
```

Build the Airflow image, validate both DAG imports, and execute the supported
fixture-backed analytics smoke path:

```bash
make airflow-image
make airflow-dag-check
make airflow-analytics-check
```

The `analytics_refresh` DAG is hourly, non-catchup, and single-active-run. Inspect
`ops.analytics_refresh_runs` or `analytics_marts.fct_pipeline_health_runs` for bounded
run state. The local commands validate an image and DAG behavior; they do not start a
production Airflow scheduler or executor.

## Secret rotation

The receiver supports one active webhook secret, not overlapping old/new secrets.
Coordinate rotation to minimize rejection:

1. Generate a new secret in an approved secret manager; never write it to the
   repository, shell history, logs, or an image.
2. Schedule a short receiver maintenance window or deploy a version that can read the
   new secret.
3. Update the GitHub App webhook secret and the receiver's injected
   `GITHUB_WEBHOOK_SECRET` as one coordinated change, then restart the receiver.
4. Send an authorized test delivery and verify Kafka acknowledgement plus downstream
   durable landing.
5. Revoke the old secret and investigate deliveries rejected during the cutover.

Rotate database, Kafka, GitHub API, and Metabase credentials through their owning
systems with separate least-privilege identities. The checked-in local migrations
hard-code demo credentials and are not a production rotation mechanism.

## Destructive local-data reset — explicit authorization required

This is not migration or recovery. It permanently removes the local PostgreSQL raw,
serving, operations, checkpoint, and analytics data, and/or the local Metabase
application configuration. It cannot be recovered unless an external backup exists.

Before acting, obtain explicit user authorization naming each exact volume. Stop the
project with `make down`, then inspect the exact targets:

```bash
docker volume inspect github-delivery-intelligence_postgres_data
docker volume inspect github-delivery-intelligence_metabase_data
```

Only after that inspection and explicit authorization, remove only the approved exact
volume name with `docker volume rm <exact-approved-name>`. Do not use globs, computed
paths, `docker compose down -v`, or a broad prune command. Starting services afterward
creates empty local state and re-runs initialization migrations.

The deterministic fixture can be reset safely without deleting a volume by running
`make demo`; only `raw.github_events_fixture` is replaced.
