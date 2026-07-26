CREATE TABLE IF NOT EXISTS raw.backfill_checkpoints (
    repository_id bigint NOT NULL,
    resource text NOT NULL,
    scope text NOT NULL,
    window_start timestamptz NOT NULL,
    window_end timestamptz NOT NULL,
    cursor text,
    status text NOT NULL DEFAULT 'in_progress',
    pages_completed integer NOT NULL DEFAULT 0,
    records_inserted bigint NOT NULL DEFAULT 0,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (repository_id, resource, scope, window_start, window_end),
    CONSTRAINT backfill_checkpoints_repository_id_positive
        CHECK (repository_id > 0),
    CONSTRAINT backfill_checkpoints_resource_nonempty
        CHECK (btrim(resource) <> ''),
    CONSTRAINT backfill_checkpoints_scope_nonempty
        CHECK (btrim(scope) <> ''),
    CONSTRAINT backfill_checkpoints_window_valid
        CHECK (window_start < window_end),
    CONSTRAINT backfill_checkpoints_status_allowed
        CHECK (status IN ('in_progress', 'completed')),
    CONSTRAINT backfill_checkpoints_pages_nonnegative
        CHECK (pages_completed >= 0),
    CONSTRAINT backfill_checkpoints_records_nonnegative
        CHECK (records_inserted >= 0),
    CONSTRAINT backfill_checkpoints_completion_has_no_cursor
        CHECK (status <> 'completed' OR cursor IS NULL)
);

COMMENT ON TABLE raw.backfill_checkpoints IS
    'Restart cursors for bounded GitHub API backfill resources and nested scopes.';
COMMENT ON COLUMN raw.backfill_checkpoints.scope IS
    'repository for pull-request discovery or the real GitHub pull-request node ID.';
COMMENT ON COLUMN raw.backfill_checkpoints.cursor IS
    'Opaque GitHub GraphQL end cursor; never parsed or synthesized by the application.';
