CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.analytics_refresh_runs (
    run_id text PRIMARY KEY,
    dag_id text NOT NULL,
    dag_run_id text NOT NULL,
    logical_date timestamptz NOT NULL,
    data_interval_start timestamptz NOT NULL,
    data_interval_end timestamptz NOT NULL,
    started_at timestamptz NOT NULL,
    finished_at timestamptz,
    status text NOT NULL,
    source_relation text NOT NULL,
    source_max_ingested_at timestamptz,
    source_delay_seconds bigint,
    source_freshness_summary jsonb,
    dbt_invocation_id text,
    dbt_result_summary jsonb,
    dbt_succeeded_count integer NOT NULL DEFAULT 0,
    dbt_failed_count integer NOT NULL DEFAULT 0,
    dbt_skipped_count integer NOT NULL DEFAULT 0,
    dbt_warning_count integer NOT NULL DEFAULT 0,
    dbt_error_count integer NOT NULL DEFAULT 0,
    error_category text,
    error_summary varchar(1000),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT analytics_refresh_runs_orchestration_identity_unique
        UNIQUE (dag_id, dag_run_id),
    CONSTRAINT analytics_refresh_runs_run_id_nonempty CHECK (btrim(run_id) <> ''),
    CONSTRAINT analytics_refresh_runs_dag_id_nonempty CHECK (btrim(dag_id) <> ''),
    CONSTRAINT analytics_refresh_runs_dag_run_id_nonempty CHECK (btrim(dag_run_id) <> ''),
    CONSTRAINT analytics_refresh_runs_status_allowed
        CHECK (status IN ('running', 'succeeded', 'failed')),
    CONSTRAINT analytics_refresh_runs_source_relation_nonempty
        CHECK (btrim(source_relation) <> ''),
    CONSTRAINT analytics_refresh_runs_interval_valid
        CHECK (data_interval_start <= data_interval_end),
    CONSTRAINT analytics_refresh_runs_terminal_timestamp_valid
        CHECK (
            (status = 'running' AND finished_at IS NULL)
            OR (status IN ('succeeded', 'failed') AND finished_at IS NOT NULL)
        ),
    CONSTRAINT analytics_refresh_runs_timestamp_order_valid
        CHECK (finished_at IS NULL OR finished_at >= started_at),
    CONSTRAINT analytics_refresh_runs_source_delay_nonnegative
        CHECK (source_delay_seconds IS NULL OR source_delay_seconds >= 0),
    CONSTRAINT analytics_refresh_runs_counts_nonnegative
        CHECK (
            dbt_succeeded_count >= 0
            AND dbt_failed_count >= 0
            AND dbt_skipped_count >= 0
            AND dbt_warning_count >= 0
            AND dbt_error_count >= 0
        ),
    CONSTRAINT analytics_refresh_runs_error_state_valid
        CHECK (
            status = 'failed'
            OR (error_category IS NULL AND error_summary IS NULL)
        )
);

ALTER TABLE ops.analytics_refresh_runs
    DROP CONSTRAINT IF EXISTS analytics_refresh_runs_interval_valid;
ALTER TABLE ops.analytics_refresh_runs
    ADD CONSTRAINT analytics_refresh_runs_interval_valid
    CHECK (data_interval_start <= data_interval_end);

CREATE INDEX IF NOT EXISTS analytics_refresh_runs_logical_date_idx
    ON ops.analytics_refresh_runs (logical_date DESC);

CREATE INDEX IF NOT EXISTS analytics_refresh_runs_latest_success_idx
    ON ops.analytics_refresh_runs (finished_at DESC)
    WHERE status = 'succeeded';

COMMENT ON TABLE ops.analytics_refresh_runs IS
    'Idempotent run ledger for the scheduled analytics_refresh Airflow DAG.';
COMMENT ON COLUMN ops.analytics_refresh_runs.run_id IS
    'Stable application identity derived from DAG ID and Airflow dag_run_id.';
COMMENT ON COLUMN ops.analytics_refresh_runs.source_delay_seconds IS
    'Nonnegative age of the newest source ingested_at value at run start.';
COMMENT ON COLUMN ops.analytics_refresh_runs.source_freshness_summary IS
    'Bounded sanitized summary derived from dbt sources.json; never raw logs.';
COMMENT ON COLUMN ops.analytics_refresh_runs.dbt_result_summary IS
    'Bounded sanitized summary derived from dbt run_results.json; never raw logs.';
