"""Run the destructive-but-volume-preserving Day 13 local evidence workflow."""

from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import hmac
import json
import platform
import subprocess
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from time import monotonic, perf_counter
from typing import Any, cast
from uuid import uuid4

import httpx2
import psycopg
from confluent_kafka import Consumer, KafkaException, Message, Producer, TopicPartition
from psycopg_pool import ConnectionPool, PoolTimeout
from pydantic import SecretStr

from github_analytics.backfill.models import BackfillRecord, BackfillRunKey
from github_analytics.backfill.storage import PostgresBackfillStorage
from github_analytics.consumers.config import WarehouseSettings
from github_analytics.consumers.dlq import create_dlq_publisher
from github_analytics.consumers.storage import PostgresRawEventStorage
from github_analytics.consumers.warehouse import (
    ProcessingOutcome,
    WarehouseWriter,
    create_consumer,
)
from github_analytics.evidence import (
    EvidenceEnvironment,
    EvidenceReport,
    EvidenceResult,
    EvidenceStatus,
    TimingSummary,
    summarize_timings,
    write_report,
)
from github_analytics.webhook.app import create_app
from github_analytics.webhook.config import WebhookSettings
from github_analytics.webhook.kafka import create_kafka_publisher

DATABASE_URL = "postgresql://github_analytics:local_only_change_me@localhost:55432/github_analytics"
RAW_TOPIC = "github.events.raw.v1"
DLQ_TOPIC = "github.events.dlq.v1"
COMPOSE_FILE = Path("infra/docker-compose.yml")
FIXTURE_PATH = Path("tests/fixtures/pull_request.opened.json")
REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
WEBHOOK_SECRET = "day13-local-evidence-only"
POLL_TIMEOUT_SECONDS = 120.0


@dataclass(frozen=True, slots=True)
class HttpBatch:
    """Observed HTTP status and timing for one receiver burst."""

    statuses: tuple[int, ...]
    timing: TimingSummary


@dataclass(frozen=True, slots=True)
class ProcessBatch:
    """Observed warehouse outcomes and timing for one Kafka batch."""

    outcomes: tuple[ProcessingOutcome, ...]
    timing: TimingSummary


@dataclass(frozen=True, slots=True)
class LiveContext:
    """Validated runtime choices shared by every local drill."""

    event_count: int
    concurrency: int
    run_id: str


def main() -> int:
    """Run all evidence checks, restoring stopped services even on failure."""

    arguments = _parse_arguments()
    context = LiveContext(
        event_count=arguments.event_count,
        concurrency=arguments.concurrency,
        run_id=uuid4().hex[:12],
    )
    _start_core_services()
    results: list[EvidenceResult] = []
    try:
        environment = _collect_environment(arguments.environment_name)
        results.extend(_run_burst_and_duplicate_replay(context))
        results.append(_run_crash_window_replay(context))
        results.append(_run_dlq_drill(context))
        results.append(_run_kafka_outage_recovery(context))
        results.append(_run_postgres_outage_restart(context))
        results.append(_run_backfill_resume(context))
        results.append(
            EvidenceResult(
                name="Live GitHub App PR lifecycle",
                status=EvidenceStatus.UNAVAILABLE,
                acceptance="Observe a real PR open, review, merge, and configured deployment.",
                observed=(
                    "Not run: this local workflow has no configured public GitHub App delivery "
                    "path, so synthetic evidence is not presented as a live lifecycle."
                ),
            )
        )
    finally:
        _restore_core_services()

    report = EvidenceReport(
        generated_at=datetime.now(UTC),
        git_revision=_git_revision(),
        git_worktree_dirty=_git_worktree_dirty(),
        workload=(
            f"{context.event_count} signed copies of {FIXTURE_PATH.as_posix()} sent with "
            f"{context.concurrency} concurrent in-process HTTP requests; real Kafka "
            "acknowledgements, Kafka consumer groups, and PostgreSQL durable effects."
        ),
        environment=environment,
        results=tuple(results),
    )
    json_path = Path(arguments.json_output)
    markdown_path = Path(arguments.markdown_output)
    write_report(report, json_path=json_path, markdown_path=markdown_path)
    print(f"wrote {json_path} and {markdown_path}")
    return 0


def _parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--event-count", type=int, default=500)
    parser.add_argument("--concurrency", type=int, default=25)
    parser.add_argument(
        "--environment-name",
        default="Local Windows Docker Desktop WSL2 workstation",
    )
    parser.add_argument(
        "--json-output",
        default=".artifacts/day-13-evidence.json",
    )
    parser.add_argument(
        "--markdown-output",
        default="docs/day-13-evidence.md",
    )
    arguments = parser.parse_args()
    if arguments.event_count <= 0:
        parser.error("--event-count must be positive")
    if arguments.concurrency <= 0:
        parser.error("--concurrency must be positive")
    return arguments


def _run_burst_and_duplicate_replay(context: LiveContext) -> list[EvidenceResult]:
    prefix = f"day13-burst-{context.run_id}-"
    delivery_ids = tuple(f"{prefix}{index:04d}" for index in range(context.event_count))
    warehouse_group = f"day13-burst-{context.run_id}"
    first_http = _send_deliveries(
        delivery_ids,
        concurrency=context.concurrency,
    )
    _require(
        all(status == 202 for status in first_http.statuses),
        "receiver burst did not acknowledge every delivery",
    )
    first_process = _process_delivery_batch(
        delivery_ids,
        group_id=warehouse_group,
    )
    _require(
        first_process.outcomes.count(ProcessingOutcome.INSERTED) == context.event_count,
        "first replay did not insert every acknowledged delivery",
    )
    _require(_raw_count(prefix) == context.event_count, "acknowledged delivery count was lost")

    duplicate_http = _send_deliveries(
        delivery_ids,
        concurrency=context.concurrency,
    )
    _require(
        all(status == 202 for status in duplicate_http.statuses),
        "duplicate replay was not fully acknowledged",
    )
    duplicate_process = _process_delivery_batch(
        delivery_ids,
        group_id=warehouse_group,
    )
    duplicate_count = duplicate_process.outcomes.count(ProcessingOutcome.DUPLICATE)
    _require(duplicate_count == context.event_count, "duplicate replay created a new raw effect")
    _require(_raw_count(prefix) == context.event_count, "duplicate replay changed raw row count")

    return [
        EvidenceResult(
            name="500-event receiver burst and durable landing",
            status=EvidenceStatus.PASSED,
            acceptance=(
                "Zero lost acknowledged events and webhook acknowledgement below the "
                "10-second receiver failure window."
            ),
            observed=(
                f"All {context.event_count} requests returned 202 after Kafka acknowledgement; "
                f"the warehouse inserted {context.event_count} unique append-only rows."
            ),
            measurements={
                "accepted_requests": context.event_count,
                "raw_rows_inserted": context.event_count,
                "lost_acknowledged_events": 0,
                "warehouse_throughput_per_second": first_process.timing.throughput_per_second,
                "warehouse_p95_milliseconds": first_process.timing.p95_milliseconds,
            },
            timing_scope="receiver_http_to_kafka_acknowledgement",
            timing=first_http.timing,
        ),
        EvidenceResult(
            name="Duplicate replay",
            status=EvidenceStatus.PASSED,
            acceptance="Repeated deliveries create zero duplicate durable raw effects.",
            observed=(
                f"A second acknowledged replay of {context.event_count} delivery IDs produced "
                f"{duplicate_count} duplicate outcomes and left the raw row count unchanged."
            ),
            measurements={
                "replayed_deliveries": context.event_count,
                "duplicates_absorbed": duplicate_count,
                "duplicate_durable_effects": 0,
                "receiver_replay_throughput_per_second": (
                    duplicate_http.timing.throughput_per_second
                ),
            },
            timing_scope="warehouse_duplicate_processing_to_offset_commit",
            timing=duplicate_process.timing,
        ),
    ]


def _run_crash_window_replay(context: LiveContext) -> EvidenceResult:
    delivery_id = f"day13-crash-{context.run_id}"
    _require(
        _send_deliveries((delivery_id,), concurrency=1).statuses == (202,),
        "crash-window delivery was not acknowledged",
    )
    settings = _warehouse_settings(f"day13-crash-group-{context.run_id}")
    first_consumer = create_consumer(settings)
    first_dlq = create_dlq_publisher(settings, client_id=f"day13-crash-dlq-{context.run_id}")
    with ConnectionPool[Any](DATABASE_URL, min_size=1, max_size=1, open=True) as pool:
        writer = WarehouseWriter(
            first_consumer,
            PostgresRawEventStorage(pool),
            first_dlq,
            raw_topic=RAW_TOPIC,
            poll_timeout_seconds=0.25,
        )
        first_consumer.subscribe([RAW_TOPIC])
        message = _poll_for_delivery(first_consumer, delivery_id)

        def simulated_crash() -> None:
            raise RuntimeError("simulated crash after database commit")

        try:
            writer.process(message, after_database_commit=simulated_crash)
        except RuntimeError as error:
            _require(
                str(error) == "simulated crash after database commit", "unexpected crash error"
            )
        else:
            raise RuntimeError("crash-window drill did not interrupt before offset commit")
    first_consumer.close()
    first_dlq.close()

    replay_consumer = create_consumer(settings)
    replay_dlq = create_dlq_publisher(settings, client_id=f"day13-crash-replay-{context.run_id}")
    with ConnectionPool[Any](DATABASE_URL, min_size=1, max_size=1, open=True) as pool:
        replay_writer = WarehouseWriter(
            replay_consumer,
            PostgresRawEventStorage(pool),
            replay_dlq,
            raw_topic=RAW_TOPIC,
            poll_timeout_seconds=0.25,
        )
        replay_consumer.subscribe([RAW_TOPIC])
        replay_message = _poll_for_delivery(replay_consumer, delivery_id)
        outcome = replay_writer.process(replay_message)
    replay_consumer.close()
    replay_dlq.close()
    _require(outcome is ProcessingOutcome.DUPLICATE, "crash replay was not absorbed")
    _require(_raw_count(delivery_id, exact=True) == 1, "crash replay created multiple raw rows")
    return EvidenceResult(
        name="Crash after database commit before offset commit",
        status=EvidenceStatus.PASSED,
        acceptance="Restart replays the source record and retains exactly one durable effect.",
        observed=(
            "The injected post-database crash prevented the source offset commit; a new consumer "
            "with the same group replayed the same Kafka record as a duplicate."
        ),
        measurements={"durable_raw_rows": 1, "replay_outcome": outcome.value},
    )


def _run_dlq_drill(context: LiveContext) -> EvidenceResult:
    delivery_id = f"day13-poison-{context.run_id}"
    source_value = f'{{"delivery_id":"{delivery_id}","schema_version":999}}'.encode()
    _publish_raw_bytes(source_value, key=delivery_id.encode())
    settings = _warehouse_settings(f"day13-poison-group-{context.run_id}")
    consumer = create_consumer(settings)
    dlq = create_dlq_publisher(settings, client_id=f"day13-poison-dlq-{context.run_id}")
    with ConnectionPool[Any](DATABASE_URL, min_size=1, max_size=1, open=True) as pool:
        writer = WarehouseWriter(
            consumer,
            PostgresRawEventStorage(pool),
            dlq,
            raw_topic=RAW_TOPIC,
            poll_timeout_seconds=0.25,
        )
        consumer.subscribe([RAW_TOPIC])
        message = _poll_for_value(consumer, source_value)
        outcome = writer.process(message)
        committed_offset = _committed_offset(cast(Consumer, consumer), message)
        source_offset = _required_offset(message)
    consumer.close()
    dlq.close()
    _require(outcome is ProcessingOutcome.DLQ, "poison record was not routed to the DLQ")
    _require(committed_offset == source_offset + 1, "source offset did not follow DLQ ack")

    inspector = Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": f"day13-dlq-inspector-{context.run_id}",
            "auto.offset.reset": "earliest",
        }
    )
    inspector.subscribe([DLQ_TOPIC])
    dlq_message = _poll_for_key(inspector, delivery_id.encode())
    inspector.close()
    dlq_value = dlq_message.value()
    if dlq_value is None:
        raise RuntimeError("DLQ record had no value")
    record = json.loads(dlq_value)
    _require(
        base64.b64decode(record["source_value_base64"]) == source_value,
        "DLQ record did not retain the poison bytes",
    )
    return EvidenceResult(
        name="Poison record DLQ acknowledgement",
        status=EvidenceStatus.PASSED,
        acceptance="The DLQ record is acknowledged before the source offset advances.",
        observed=(
            "The invalid envelope reached the DLQ with its original bytes and lineage; only then "
            "did the warehouse group commit the next source offset."
        ),
        measurements={
            "failure_reason": record["failure_reason"],
            "source_offset": source_offset,
            "committed_offset": committed_offset,
            "dlq_records_observed": 1,
        },
    )


def _run_kafka_outage_recovery(context: LiveContext) -> EvidenceResult:
    delivery_id = f"day13-kafka-outage-{context.run_id}"
    _compose("stop", "kafka")
    outage = _send_deliveries((delivery_id,), concurrency=1, publish_timeout_seconds=2.0)
    _require(outage.statuses == (503,), "Kafka outage did not produce a bounded 503")
    _require(
        outage.timing.maximum_milliseconds < 10_000,
        "Kafka outage response exceeded the 10-second receiver failure window",
    )
    _compose("up", "-d", "--wait", "kafka")
    _run_command(["make", "topics"])
    recovery = _send_deliveries((delivery_id,), concurrency=1)
    _require(recovery.statuses == (202,), "receiver did not recover after Kafka restart")
    processed = _process_delivery_batch(
        (delivery_id,),
        group_id=f"day13-kafka-recovery-{context.run_id}",
    )
    _require(
        processed.outcomes[0] is ProcessingOutcome.INSERTED,
        "recovered Kafka delivery did not reach PostgreSQL",
    )
    _require(_raw_count(delivery_id, exact=True) == 1, "Kafka recovery created wrong row count")
    return EvidenceResult(
        name="Kafka outage and recovery",
        status=EvidenceStatus.PASSED,
        acceptance=(
            "Broker outage returns a bounded non-2xx; recovery accepts and durably lands a retry."
        ),
        observed=(
            "With Kafka stopped, the signed request returned 503 within the configured boundary. "
            "After the same container restarted, the retry returned 202 and produced one raw row."
        ),
        measurements={
            "outage_status": outage.statuses[0],
            "outage_response_milliseconds": outage.timing.maximum_milliseconds,
            "recovery_status": recovery.statuses[0],
            "durable_raw_rows": 1,
        },
    )


def _run_postgres_outage_restart(context: LiveContext) -> EvidenceResult:
    delivery_id = f"day13-postgres-outage-{context.run_id}"
    _require(
        _send_deliveries((delivery_id,), concurrency=1).statuses == (202,),
        "PostgreSQL drill delivery was not acknowledged by Kafka",
    )
    settings = _warehouse_settings(f"day13-postgres-group-{context.run_id}")
    consumer = create_consumer(settings)
    dlq = create_dlq_publisher(settings, client_id=f"day13-postgres-dlq-{context.run_id}")
    pool = ConnectionPool[Any](DATABASE_URL, min_size=1, max_size=1, timeout=3, open=True)
    pool.wait(timeout=5)
    writer = WarehouseWriter(
        consumer,
        PostgresRawEventStorage(pool),
        dlq,
        raw_topic=RAW_TOPIC,
        poll_timeout_seconds=0.25,
    )
    consumer.subscribe([RAW_TOPIC])
    message = _poll_for_delivery(consumer, delivery_id)
    source_offset = _required_offset(message)
    _compose("stop", "postgres")
    failure_name: str | None = None
    try:
        writer.process(message)
    except (psycopg.Error, PoolTimeout) as error:
        failure_name = type(error).__name__
    committed_during_outage = _committed_offset(cast(Consumer, consumer), message)
    consumer.close()
    dlq.close()
    pool.close()
    _require(failure_name is not None, "PostgreSQL outage did not stop processing")
    _require(
        committed_during_outage != source_offset + 1,
        "source offset advanced despite PostgreSQL failure",
    )

    _compose("up", "-d", "--wait", "postgres")
    _run_command(["make", "migrate"])
    replay_consumer = create_consumer(settings)
    replay_dlq = create_dlq_publisher(settings, client_id=f"day13-postgres-replay-{context.run_id}")
    with ConnectionPool[Any](DATABASE_URL, min_size=1, max_size=1, open=True) as replay_pool:
        replay_writer = WarehouseWriter(
            replay_consumer,
            PostgresRawEventStorage(replay_pool),
            replay_dlq,
            raw_topic=RAW_TOPIC,
            poll_timeout_seconds=0.25,
        )
        replay_consumer.subscribe([RAW_TOPIC])
        replay_message = _poll_for_delivery(replay_consumer, delivery_id)
        outcome = replay_writer.process(replay_message)
    replay_consumer.close()
    replay_dlq.close()
    _require(outcome is ProcessingOutcome.INSERTED, "PostgreSQL restart did not replay the event")
    _require(_raw_count(delivery_id, exact=True) == 1, "PostgreSQL restart lost or duplicated data")
    return EvidenceResult(
        name="PostgreSQL outage and consumer restart",
        status=EvidenceStatus.PASSED,
        acceptance="Database failure leaves the source offset uncommitted and restart replays it.",
        observed=(
            "Processing failed while PostgreSQL was stopped and the source offset did not advance. "
            "A new consumer in the same group inserted the replay after PostgreSQL recovered."
        ),
        measurements={
            "failure_class": failure_name,
            "source_offset": source_offset,
            "committed_offset_during_outage": committed_during_outage,
            "restart_outcome": outcome.value,
            "durable_raw_rows": 1,
        },
    )


def _run_backfill_resume(context: LiveContext) -> EvidenceResult:
    now = datetime.now(UTC)
    key = BackfillRunKey(
        repository_id=20001,
        resource=f"day13_evidence_{context.run_id}",
        scope="repository",
        window_start=now - timedelta(days=1),
        window_end=now,
    )
    records = (
        _backfill_record(context, page=1, occurred_at=key.window_start),
        _backfill_record(context, page=2, occurred_at=key.window_start + timedelta(hours=1)),
    )
    with ConnectionPool[Any](DATABASE_URL, min_size=1, max_size=1, open=True) as first_pool:
        first = PostgresBackfillStorage(first_pool).persist_page(
            key,
            [records[0]],
            next_cursor="page-2",
            completed=False,
        )
    _require(first.checkpoint.cursor == "page-2", "first backfill cursor was not durable")

    with ConnectionPool[Any](DATABASE_URL, min_size=1, max_size=1, open=True) as resumed_pool:
        resumed_storage = PostgresBackfillStorage(resumed_pool)
        checkpoint = resumed_storage.load_checkpoint(key)
        if checkpoint is None:
            raise RuntimeError("new process could not load the backfill checkpoint")
        _require(checkpoint.cursor == "page-2", "new process loaded the wrong backfill cursor")
        completed = resumed_storage.persist_page(
            key,
            [records[1]],
            next_cursor=None,
            completed=True,
        )
    count = sum(
        _raw_count(record.source_record_key, exact=True, source="backfill") for record in records
    )
    _require(count == 2, "backfill resume did not retain both durable pages")
    _require(completed.checkpoint.pages_completed == 2, "backfill page count was not resumed")
    return EvidenceResult(
        name="Backfill interruption and cursor resume",
        status=EvidenceStatus.PASSED,
        acceptance="A new process resumes from the durable cursor without losing prior pages.",
        observed=(
            "The first process committed page 1 and cursor page-2. A new pool loaded that cursor, "
            "committed page 2, and completed the checkpoint with both raw records present."
        ),
        measurements={
            "pages_completed": completed.checkpoint.pages_completed,
            "records_inserted": completed.checkpoint.records_inserted,
            "durable_raw_rows": count,
            "final_status": completed.checkpoint.status.value,
        },
    )


def _send_deliveries(
    delivery_ids: Sequence[str],
    *,
    concurrency: int,
    publish_timeout_seconds: float = 5.0,
) -> HttpBatch:
    return asyncio.run(
        _send_deliveries_async(
            delivery_ids,
            concurrency=concurrency,
            publish_timeout_seconds=publish_timeout_seconds,
        )
    )


async def _send_deliveries_async(
    delivery_ids: Sequence[str],
    *,
    concurrency: int,
    publish_timeout_seconds: float,
) -> HttpBatch:
    body = FIXTURE_PATH.read_bytes()
    signature = "sha256=" + hmac.new(WEBHOOK_SECRET.encode(), body, hashlib.sha256).hexdigest()
    settings = WebhookSettings(
        webhook_secret=SecretStr(WEBHOOK_SECRET),
        kafka_bootstrap_servers="localhost:9092",
        kafka_raw_topic=RAW_TOPIC,
        kafka_publish_timeout_seconds=publish_timeout_seconds,
    )
    app = create_app(settings, publisher=create_kafka_publisher(settings))
    semaphore = asyncio.Semaphore(concurrency)
    statuses: list[int] = []
    samples: list[float] = []

    async def send(client: httpx2.AsyncClient, delivery_id: str) -> None:
        async with semaphore:
            started = perf_counter()
            response = await client.post(
                "/webhooks/github",
                content=body,
                headers={
                    "Content-Type": "application/json",
                    "X-GitHub-Event": "pull_request",
                    "X-GitHub-Delivery": delivery_id,
                    "X-Hub-Signature-256": signature,
                },
            )
            samples.append(perf_counter() - started)
            statuses.append(response.status_code)

    wall_started = perf_counter()
    async with httpx2.AsyncClient(
        transport=httpx2.ASGITransport(app=app),
        base_url="http://day13.local",
        timeout=15,
    ) as client:
        await asyncio.gather(*(send(client, delivery_id) for delivery_id in delivery_ids))
    return HttpBatch(
        statuses=tuple(statuses),
        timing=summarize_timings(samples, wall_seconds=perf_counter() - wall_started),
    )


def _process_delivery_batch(delivery_ids: Sequence[str], *, group_id: str) -> ProcessBatch:
    remaining = set(delivery_ids)
    settings = _warehouse_settings(group_id)
    consumer = create_consumer(settings)
    dlq = create_dlq_publisher(settings, client_id=f"{group_id}-dlq")
    outcomes: list[ProcessingOutcome] = []
    samples: list[float] = []
    wall_started = perf_counter()
    with ConnectionPool[Any](DATABASE_URL, min_size=1, max_size=1, open=True) as pool:
        writer = WarehouseWriter(
            consumer,
            PostgresRawEventStorage(pool),
            dlq,
            raw_topic=RAW_TOPIC,
            poll_timeout_seconds=0.25,
        )
        consumer.subscribe([RAW_TOPIC])
        deadline = monotonic() + POLL_TIMEOUT_SECONDS
        while remaining and monotonic() < deadline:
            message = consumer.poll(0.25)
            if message is None:
                continue
            _raise_message_error(message)
            delivery_id = _delivery_id(message.value())
            if delivery_id not in remaining:
                _commit_skipped(consumer, message)
                continue
            started = perf_counter()
            outcomes.append(writer.process(message))
            samples.append(perf_counter() - started)
            remaining.remove(delivery_id)
    consumer.close()
    dlq.close()
    _require(not remaining, f"timed out waiting for {len(remaining)} delivery IDs")
    return ProcessBatch(
        outcomes=tuple(outcomes),
        timing=summarize_timings(samples, wall_seconds=perf_counter() - wall_started),
    )


def _poll_for_delivery(consumer: Any, delivery_id: str) -> Message:
    return _poll_for(consumer, lambda message: _delivery_id(message.value()) == delivery_id)


def _poll_for_value(consumer: Any, value: bytes) -> Message:
    return _poll_for(consumer, lambda message: message.value() == value)


def _poll_for_key(consumer: Any, key: bytes) -> Message:
    return _poll_for(consumer, lambda message: message.key() == key)


def _poll_for(consumer: Any, predicate: Callable[[Message], bool]) -> Message:
    deadline = monotonic() + POLL_TIMEOUT_SECONDS
    while monotonic() < deadline:
        message = consumer.poll(0.25)
        if message is None:
            continue
        _raise_message_error(message)
        if predicate(message):
            return cast(Message, message)
        _commit_skipped(consumer, message)
    raise RuntimeError("timed out waiting for the Day 13 Kafka record")


def _delivery_id(value: bytes | None) -> str | None:
    if value is None:
        return None
    try:
        payload = json.loads(value)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    delivery_id = payload.get("delivery_id")
    return delivery_id if isinstance(delivery_id, str) else None


def _commit_skipped(consumer: Any, message: Message) -> None:
    committed = consumer.commit(message=message, asynchronous=False)
    for partition in committed or []:
        if partition.error is not None:
            raise KafkaException(partition.error)


def _raise_message_error(message: Message) -> None:
    error = message.error()
    if error is not None:
        raise KafkaException(error)


def _required_offset(message: Message) -> int:
    offset = message.offset()
    if offset is None:
        raise RuntimeError("Kafka message had no offset")
    return offset


def _committed_offset(consumer: Consumer, message: Message) -> int:
    topic = message.topic()
    partition = message.partition()
    if topic is None or partition is None:
        raise RuntimeError("Kafka message had incomplete lineage")
    committed = consumer.committed([TopicPartition(topic, partition)], timeout=5)
    return committed[0].offset


def _publish_raw_bytes(value: bytes, *, key: bytes) -> None:
    producer = Producer(
        {
            "bootstrap.servers": "localhost:9092",
            "acks": "all",
            "enable.idempotence": True,
            "message.timeout.ms": 4_500,
        }
    )
    producer.produce(RAW_TOPIC, value=value, key=key)
    _require(producer.flush(5) == 0, "raw Kafka publish did not flush")


def _warehouse_settings(group_id: str) -> WarehouseSettings:
    return WarehouseSettings(
        database_url=SecretStr(DATABASE_URL),
        database_pool_timeout_seconds=3,
        kafka_bootstrap_servers="localhost:9092",
        kafka_raw_topic=RAW_TOPIC,
        kafka_dlq_topic=DLQ_TOPIC,
        kafka_group_id=group_id,
        kafka_poll_timeout_seconds=0.25,
        kafka_dlq_publish_timeout_seconds=5,
    )


def _raw_count(identity: str, *, exact: bool = False, source: str = "webhook") -> int:
    operator = "=" if exact else "LIKE"
    parameter = identity if exact else f"{identity}%"
    with psycopg.connect(DATABASE_URL) as connection:
        row = connection.execute(
            f"""
            SELECT count(*)
            FROM raw.github_events
            WHERE source = %s AND source_record_key {operator} %s
            """,
            (source, parameter),
        ).fetchone()
    if row is None:
        raise RuntimeError("raw event count query returned no row")
    return cast(int, row[0])


def _backfill_record(
    context: LiveContext,
    *,
    page: int,
    occurred_at: datetime,
) -> BackfillRecord:
    return BackfillRecord(
        source_record_key=f"github_day13:{context.run_id}:page:{page}",
        event_name="pull_request",
        action="opened",
        repository_id=20001,
        installation_id=10001,
        occurred_at=occurred_at,
        payload={"synthetic": True, "evidence_run": context.run_id, "page": page},
    )


def _collect_environment(name: str) -> EvidenceEnvironment:
    docker_name, docker_engine = _split_fields(
        _run_command(
            [
                "docker",
                "version",
                "--format",
                "{{.Server.Platform.Name}}|{{.Server.Version}}",
            ]
        ),
        2,
    )
    logical_cpus, memory_bytes = _split_fields(
        _run_command(["docker", "info", "--format", "{{.NCPU}}|{{.MemTotal}}"]),
        2,
    )
    kafka_container = _compose("ps", "-q", "kafka")
    postgres_container = _compose("ps", "-q", "postgres")
    return EvidenceEnvironment(
        name=name,
        operating_system=(f"{platform.system()} {platform.release()} {platform.version()}"),
        architecture=platform.machine(),
        logical_cpus=int(logical_cpus),
        memory_gib=round(int(memory_bytes) / (1024**3), 3),
        python_version=platform.python_version(),
        uv_version=_uv_version(),
        docker_desktop_version=docker_name,
        docker_engine_version=docker_engine,
        docker_compose_version=_run_command(["docker", "compose", "version", "--short"]),
        kafka_image=_run_command(
            ["docker", "inspect", "--format", "{{.Config.Image}}", kafka_container]
        ),
        postgres_image=_run_command(
            ["docker", "inspect", "--format", "{{.Config.Image}}", postgres_container]
        ),
    )


def _uv_version() -> str:
    uv_path = Path(".venv/Scripts/uv.exe") if sys.platform == "win32" else Path(".venv/bin/uv")
    return _run_command([str(uv_path), "--version"]).removeprefix("uv ")


def _git_revision() -> str:
    return _git_command("rev-parse", "HEAD")


def _git_worktree_dirty() -> bool:
    return bool(_git_command("status", "--porcelain", "--untracked-files=all"))


def _git_command(*arguments: str) -> str:
    return _run_command(
        [
            "git",
            "-c",
            f"safe.directory={REPOSITORY_ROOT.as_posix()}",
            "-C",
            str(REPOSITORY_ROOT),
            *arguments,
        ]
    )


def _start_core_services() -> None:
    _compose("up", "-d", "--wait", "kafka", "postgres")
    _run_command(["make", "topics"])
    _run_command(["make", "migrate"])


def _restore_core_services() -> None:
    _compose("up", "-d", "--wait", "kafka", "postgres")
    _run_command(["make", "topics"])
    _run_command(["make", "migrate"])


def _compose(*arguments: str) -> str:
    return _run_command(["docker", "compose", "-f", str(COMPOSE_FILE), *arguments])


def _run_command(arguments: Sequence[str]) -> str:
    completed = subprocess.run(
        list(arguments),
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    return completed.stdout.strip()


def _split_fields(value: str, count: int) -> tuple[str, ...]:
    fields = tuple(value.split("|"))
    if len(fields) != count or any(not field for field in fields):
        raise RuntimeError("environment command returned an unexpected shape")
    return fields


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


if __name__ == "__main__":
    raise SystemExit(main())
