# End-to-end replay and failure evidence

Run the deterministic Day 13 workflow from the repository root:

```powershell
make UV=.venv/Scripts/uv.exe day13-evidence
```

The workflow starts the existing Kafka and PostgreSQL Compose services without
deleting containers or named volumes, applies idempotent migrations, and then records:

- a 500-request signed fixture burst through the real Kafka acknowledgement boundary;
- a duplicate replay and crash-window replay with one durable raw effect per delivery;
- acknowledged poison-record handling through the DLQ;
- Kafka outage/receiver recovery and PostgreSQL outage/consumer restart;
- durable backfill checkpoint resume across new database pools; and
- an explicit `unavailable` result for the live GitHub App lifecycle unless it was
  actually observed.

Machine-readable results are written to `.artifacts/day-13-evidence.json`; the matching
review artifact is `docs/day-13-evidence.md`. The runner uses sanitized checked-in
fixtures and unique source identities. It appends evidence rows to `raw.github_events`
and never truncates or mutates that append-only table.
