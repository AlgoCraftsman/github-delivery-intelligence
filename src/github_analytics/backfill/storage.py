"""Transactional raw inserts and cursor advancement for GitHub backfills."""

from collections.abc import Sequence
from typing import Any

from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

from github_analytics.backfill.models import (
    BackfillCheckpoint,
    BackfillRecord,
    BackfillRunKey,
    CheckpointStatus,
    PageWriteOutcome,
)

_LOAD_CHECKPOINT = """
SELECT cursor, status, pages_completed, records_inserted
FROM raw.backfill_checkpoints
WHERE repository_id = %(repository_id)s
  AND resource = %(resource)s
  AND scope = %(scope)s
  AND window_start = %(window_start)s
  AND window_end = %(window_end)s
"""

_INSERT_RECORD = """
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
    'backfill',
    %(source_record_key)s,
    NULL,
    %(event_name)s,
    %(action)s,
    %(repository_id)s,
    %(installation_id)s,
    %(occurred_at)s,
    NULL,
    %(payload)s,
    NULL,
    NULL
)
ON CONFLICT (source, source_record_key) DO NOTHING
RETURNING event_id
"""

_ADVANCE_CHECKPOINT = """
INSERT INTO raw.backfill_checkpoints (
    repository_id,
    resource,
    scope,
    window_start,
    window_end,
    cursor,
    status,
    pages_completed,
    records_inserted
) VALUES (
    %(repository_id)s,
    %(resource)s,
    %(scope)s,
    %(window_start)s,
    %(window_end)s,
    %(cursor)s,
    %(status)s,
    1,
    %(inserted)s
)
ON CONFLICT (repository_id, resource, scope, window_start, window_end)
DO UPDATE SET
    cursor = EXCLUDED.cursor,
    status = EXCLUDED.status,
    pages_completed = raw.backfill_checkpoints.pages_completed + 1,
    records_inserted = raw.backfill_checkpoints.records_inserted + EXCLUDED.records_inserted,
    updated_at = clock_timestamp()
RETURNING cursor, status, pages_completed, records_inserted
"""


class PostgresBackfillStorage:
    """Persist a raw page and its next cursor in one PostgreSQL transaction."""

    def __init__(self, pool: ConnectionPool[Any]) -> None:
        self._pool = pool

    def load_checkpoint(self, key: BackfillRunKey) -> BackfillCheckpoint | None:
        """Return prior restart state without mutating it."""

        with self._pool.connection() as connection:
            row = connection.execute(_LOAD_CHECKPOINT, _key_parameters(key)).fetchone()
        if row is None:
            return None
        return _checkpoint_from_row(row)

    def persist_page(
        self,
        key: BackfillRunKey,
        records: Sequence[BackfillRecord],
        *,
        next_cursor: str | None,
        completed: bool,
    ) -> PageWriteOutcome:
        """Commit idempotent records and checkpoint progress atomically."""

        if completed and next_cursor is not None:
            raise ValueError("completed checkpoint cannot retain a cursor")
        if not completed and not next_cursor:
            raise ValueError("in-progress checkpoint requires a next cursor")
        if any(record.repository_id != key.repository_id for record in records):
            raise ValueError("record repository does not match checkpoint repository")

        inserted = 0
        with (
            self._pool.connection() as connection,
            connection.transaction(),
        ):
            for record in records:
                row = connection.execute(
                    _INSERT_RECORD,
                    {
                        "source_record_key": record.source_record_key,
                        "event_name": record.event_name,
                        "action": record.action,
                        "repository_id": record.repository_id,
                        "installation_id": record.installation_id,
                        "occurred_at": record.occurred_at,
                        "payload": Jsonb(record.payload),
                    },
                ).fetchone()
                inserted += row is not None
            parameters = _key_parameters(key)
            parameters.update(
                {
                    "cursor": next_cursor,
                    "status": (
                        CheckpointStatus.COMPLETED.value
                        if completed
                        else CheckpointStatus.IN_PROGRESS.value
                    ),
                    "inserted": inserted,
                }
            )
            checkpoint_row = connection.execute(
                _ADVANCE_CHECKPOINT,
                parameters,
            ).fetchone()
        if checkpoint_row is None:
            raise RuntimeError("checkpoint upsert returned no durable state")
        return PageWriteOutcome(
            inserted=inserted,
            duplicates=len(records) - inserted,
            checkpoint=_checkpoint_from_row(checkpoint_row),
        )


def _key_parameters(key: BackfillRunKey) -> dict[str, Any]:
    return {
        "repository_id": key.repository_id,
        "resource": key.resource,
        "scope": key.scope,
        "window_start": key.window_start,
        "window_end": key.window_end,
    }


def _checkpoint_from_row(row: tuple[Any, ...]) -> BackfillCheckpoint:
    cursor, status, pages_completed, records_inserted = row
    return BackfillCheckpoint(
        cursor=cursor,
        status=CheckpointStatus(status),
        pages_completed=pages_completed,
        records_inserted=records_inserted,
    )
