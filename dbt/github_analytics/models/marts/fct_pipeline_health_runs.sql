with runs as (
    select * from {{ source('analytics_ops', 'analytics_refresh_runs') }}
),

latest_success as (
    select max(finished_at) as latest_success_finished_at
    from runs
    where status = 'succeeded'
)

select
    runs.run_id,
    runs.dag_run_id,
    runs.logical_date,
    runs.data_interval_start,
    runs.data_interval_end,
    runs.started_at,
    runs.finished_at,
    case
        when runs.finished_at is null then null
        else floor(extract(epoch from (runs.finished_at - runs.started_at)))::bigint
    end as duration_seconds,
    runs.status as run_status,
    runs.source_relation,
    runs.source_max_ingested_at as source_watermark,
    runs.source_delay_seconds,
    runs.dbt_invocation_id,
    runs.dbt_succeeded_count,
    runs.dbt_failed_count,
    runs.dbt_skipped_count,
    runs.dbt_warning_count,
    runs.dbt_error_count,
    runs.error_category,
    runs.error_summary,
    (
        runs.status = 'succeeded'
        and runs.finished_at = latest_success.latest_success_finished_at
    ) as is_latest_successful_run
from runs
cross join latest_success
