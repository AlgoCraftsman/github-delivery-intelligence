# Day 13 replay, failure-drill, and benchmark evidence

Generated at `2026-08-17T22:38:39.540973+00:00` from commit `2c1440656e570235f09d04a32998017d45d9cdaf` with a dirty working tree.

This report records a local single-broker Docker Desktop run. It demonstrates
at-least-once processing with idempotent durable effects; it is not a production
capacity or high-availability claim.

## Workload

500 signed copies of tests/fixtures/pull_request.opened.json sent with 25 concurrent in-process HTTP requests; real Kafka acknowledgements, Kafka consumer groups, and PostgreSQL durable effects.

## Environment

| Field | Observed value |
|---|---|
| Name | Local Windows Docker Desktop WSL2 workstation |
| Operating system | Windows 11 10.0.26200 |
| Architecture | AMD64 |
| Logical CPUs available to Docker | 16 |
| Memory available to Docker | 7.62 GiB |
| Python | 3.12.13 |
| uv | 0.11.29 (901092ee1 2026-07-15 x86_64-pc-windows-msvc) |
| Docker Desktop | Docker Desktop 4.84.0 (234817) |
| Docker Engine | 29.6.2 |
| Docker Compose | 5.3.1 |
| Kafka image | apache/kafka:4.3.1 |
| PostgreSQL image | postgres:17.10-bookworm |

## Evidence

| Check | Status | Acceptance condition | Observed result | Measurements |
|---|---|---|---|---|
| 500-event receiver burst and durable landing | passed | Zero lost acknowledged events and webhook acknowledgement below the 10-second receiver failure window. | All 500 requests returned 202 after Kafka acknowledgement; the warehouse inserted 500 unique append-only rows. | accepted_requests=500; lost_acknowledged_events=0; raw_rows_inserted=500; warehouse_p95_milliseconds=7.465; warehouse_throughput_per_second=89.807; timing_scope="receiver_http_to_kafka_acknowledgement"; samples=500; wall=2.779s; throughput=179.892/s; p50=120.050ms; p95=194.072ms; max=225.366ms |
| Duplicate replay | passed | Repeated deliveries create zero duplicate durable raw effects. | A second acknowledged replay of 500 delivery IDs produced 500 duplicate outcomes and left the raw row count unchanged. | duplicate_durable_effects=0; duplicates_absorbed=500; receiver_replay_throughput_per_second=166.314; replayed_deliveries=500; timing_scope="warehouse_duplicate_processing_to_offset_commit"; samples=500; wall=1.225s; throughput=408.005/s; p50=2.156ms; p95=2.804ms; max=4.020ms |
| Crash after database commit before offset commit | passed | Restart replays the source record and retains exactly one durable effect. | The injected post-database crash prevented the source offset commit; a new consumer with the same group replayed the same Kafka record as a duplicate. | durable_raw_rows=1; replay_outcome="duplicate" |
| Poison record DLQ acknowledgement | passed | The DLQ record is acknowledged before the source offset advances. | The invalid envelope reached the DLQ with its original bytes and lineage; only then did the warehouse group commit the next source offset. | committed_offset=4010; dlq_records_observed=1; failure_reason="invalid_github_event_envelope"; source_offset=4009 |
| Kafka outage and recovery | passed | Broker outage returns a bounded non-2xx; recovery accepts and durably lands a retry. | With Kafka stopped, the signed request returned 503 within the configured boundary. After the same container restarted, the retry returned 202 and produced one raw row. | durable_raw_rows=1; outage_response_milliseconds=2017.634; outage_status=503; recovery_status=202 |
| PostgreSQL outage and consumer restart | passed | Database failure leaves the source offset uncommitted and restart replays it. | Processing failed while PostgreSQL was stopped and the source offset did not advance. A new consumer in the same group inserted the replay after PostgreSQL recovered. | committed_offset_during_outage=4011; durable_raw_rows=1; failure_class="OperationalError"; restart_outcome="inserted"; source_offset=4011 |
| Backfill interruption and cursor resume | passed | A new process resumes from the durable cursor without losing prior pages. | The first process committed page 1 and cursor page-2. A new pool loaded that cursor, committed page 2, and completed the checkpoint with both raw records present. | durable_raw_rows=2; final_status="completed"; pages_completed=2; records_inserted=2 |
| Live GitHub App PR lifecycle | unavailable | Observe a real PR open, review, merge, and configured deployment. | Not run: this local workflow has no configured public GitHub App delivery path, so synthetic evidence is not presented as a live lifecycle. | none |

## Interpretation limits

- Timings use nearest-rank p50/p95 over the named scope. Receiver timing covers
  local in-process HTTP transport through real Kafka acknowledgement; warehouse
  timing covers consumer processing through the durable effect and offset commit.
- Failure drills stop and restart existing Compose services without deleting
  containers or named volumes.
- `unavailable` means the evidence was not observed; it must not be inferred
  from tests, CI failures, or synthetic data.
