# Integration tests

`test_webhook_kafka_outage.py` always runs against a deliberately closed local port and
proves a broker outage produces a bounded non-`2xx` HTTP response.

`test_webhook_kafka_live.py` is opt-in because it requires the local Compose Kafka
broker. Start the services and run it with:

```bash
RUN_KAFKA_INTEGRATION=1 uv run pytest --no-cov tests/integration/test_webhook_kafka_live.py
```

The raw-storage test is also opt-in. Apply the checked-in migration, then run:

```bash
RUN_POSTGRES_INTEGRATION=1 uv run pytest --no-cov tests/integration/test_raw_event_storage_live.py
```

The end-to-end warehouse test uses both local services. It proves database-commit /
offset-commit crash replay and acknowledged poison-record DLQ handling:

```bash
RUN_WAREHOUSE_INTEGRATION=1 uv run pytest --no-cov tests/integration/test_warehouse_consumer_live.py
```

The PR-monitor test uses both services and the Day 5 migration. It proves the
`warehouse-writer` and `pr-monitor` groups can consume and commit the same record
independently, selects the earliest eligible non-author review, deduplicates repeated
stale sweeps, and cancels a pending alert when its PR closes:

```bash
RUN_PR_MONITOR_INTEGRATION=1 uv run pytest --no-cov tests/integration/test_pr_monitor_live.py
```

Run the complete suite with coverage and live integrations by setting the relevant
environment variables before `make check`.
