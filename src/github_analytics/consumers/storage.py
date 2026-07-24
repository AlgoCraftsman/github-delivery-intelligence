"""Idempotent PostgreSQL landing for raw GitHub webhook events."""

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any

from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

from github_analytics.webhook.models import GitHubEventEnvelope

_INSERT_RAW_EVENT = """
INSERT INTO raw.github_events (
    source,
    source_record_key,
    delivery_id,
    event_name,
    action,
    repository_id,
    installation_id,
    occurred_at,
    received_at,
    payload,
    kafka_partition,
    kafka_offset
) VALUES (
    'webhook',
    %(source_record_key)s,
    %(delivery_id)s,
    %(event_name)s,
    %(action)s,
    %(repository_id)s,
    %(installation_id)s,
    %(occurred_at)s,
    %(received_at)s,
    %(payload)s,
    %(kafka_partition)s,
    %(kafka_offset)s
)
ON CONFLICT (source, source_record_key) DO NOTHING
RETURNING event_id
"""


class InsertOutcome(StrEnum):
    """Durable effect produced by one raw-event insert attempt."""

    INSERTED = "inserted"
    DUPLICATE = "duplicate"


@dataclass(frozen=True, slots=True)
class WebhookRawEvent:
    """Validated envelope plus its Kafka lineage."""

    envelope: GitHubEventEnvelope
    kafka_partition: int
    kafka_offset: int
    occurred_at: datetime | None = None

    def __post_init__(self) -> None:
        if self.kafka_partition < 0:
            raise ValueError("Kafka partition must be nonnegative")
        if self.kafka_offset < 0:
            raise ValueError("Kafka offset must be nonnegative")


class PostgresRawEventStorage:
    """Insert raw events transactionally and absorb source-identity replays."""

    def __init__(self, pool: ConnectionPool[Any]) -> None:
        self._pool = pool

    def insert(self, event: WebhookRawEvent) -> InsertOutcome:
        """Commit one insert before returning its inserted-or-duplicate outcome."""

        envelope = event.envelope
        parameters = {
            "source_record_key": envelope.delivery_id,
            "delivery_id": envelope.delivery_id,
            "event_name": envelope.event_name.value,
            "action": envelope.action,
            "repository_id": envelope.repository_id,
            "installation_id": envelope.installation_id,
            "occurred_at": event.occurred_at,
            "received_at": envelope.received_at,
            "payload": Jsonb(envelope.payload),
            "kafka_partition": event.kafka_partition,
            "kafka_offset": event.kafka_offset,
        }
        with (
            self._pool.connection() as connection,
            connection.transaction(),
        ):
            result = connection.execute(_INSERT_RAW_EVENT, parameters).fetchone()
        if result is None:
            return InsertOutcome.DUPLICATE
        return InsertOutcome.INSERTED
