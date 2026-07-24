CREATE TABLE IF NOT EXISTS raw.github_events (
    event_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    source text NOT NULL,
    source_record_key text NOT NULL,
    delivery_id text,
    event_name text NOT NULL,
    action text NOT NULL,
    repository_id bigint NOT NULL,
    installation_id bigint NOT NULL,
    occurred_at timestamptz,
    received_at timestamptz,
    ingested_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    payload jsonb NOT NULL,
    kafka_partition integer,
    kafka_offset bigint,
    CONSTRAINT github_events_source_record_key_unique
        UNIQUE (source, source_record_key),
    CONSTRAINT github_events_source_allowed
        CHECK (source IN ('webhook', 'backfill')),
    CONSTRAINT github_events_source_record_key_nonempty
        CHECK (btrim(source_record_key) <> ''),
    CONSTRAINT github_events_delivery_id_nonempty
        CHECK (delivery_id IS NULL OR btrim(delivery_id) <> ''),
    CONSTRAINT github_events_event_name_nonempty
        CHECK (btrim(event_name) <> ''),
    CONSTRAINT github_events_action_nonempty
        CHECK (btrim(action) <> ''),
    CONSTRAINT github_events_repository_id_positive
        CHECK (repository_id > 0),
    CONSTRAINT github_events_installation_id_positive
        CHECK (installation_id > 0),
    CONSTRAINT github_events_kafka_partition_nonnegative
        CHECK (kafka_partition IS NULL OR kafka_partition >= 0),
    CONSTRAINT github_events_kafka_offset_nonnegative
        CHECK (kafka_offset IS NULL OR kafka_offset >= 0),
    CONSTRAINT github_events_webhook_lineage_complete
        CHECK (
            source <> 'webhook'
            OR (
                delivery_id IS NOT NULL
                AND received_at IS NOT NULL
                AND kafka_partition IS NOT NULL
                AND kafka_offset IS NOT NULL
            )
        ),
    CONSTRAINT github_events_backfill_has_no_webhook_identity
        CHECK (source <> 'backfill' OR delivery_id IS NULL)
);

COMMENT ON TABLE raw.github_events IS
    'Append-only GitHub source events with stable source identity and ingestion lineage.';
COMMENT ON COLUMN raw.github_events.source_record_key IS
    'Stable identity within source; the real GitHub delivery ID for webhook rows.';
COMMENT ON COLUMN raw.github_events.occurred_at IS
    'Source event time when the payload supplies a trustworthy timestamp; otherwise null.';

CREATE OR REPLACE FUNCTION raw.reject_github_event_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'raw.github_events is append-only';
END;
$$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_trigger
        WHERE tgname = 'github_events_reject_mutation'
          AND tgrelid = 'raw.github_events'::regclass
          AND NOT tgisinternal
    ) THEN
        CREATE TRIGGER github_events_reject_mutation
        BEFORE UPDATE OR DELETE ON raw.github_events
        FOR EACH ROW
        EXECUTE FUNCTION raw.reject_github_event_mutation();
    END IF;
END;
$$;
