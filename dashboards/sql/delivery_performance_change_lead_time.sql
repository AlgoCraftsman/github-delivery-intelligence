-- Grain: one repository, repository-local merge date, and percentile.
-- Visualization: line chart split by series_name, with coverage in the tooltip.
-- Filters: repository_full_name and date_day.
-- Assumption: percentiles are calculated from linked PR rows, not the daily average mart value.
with eligible_changes as (
    select
        pull_requests.repository_id,
        pull_requests.repository_full_name,
        (pull_requests.merged_at at time zone repositories.repository_timezone)::date as date_day,
        pull_requests.lead_time_seconds,
        pull_requests.is_change_linked
    from analytics_marts.fct_pull_requests as pull_requests
    inner join analytics_marts.dim_repository as repositories using (repository_id)
    where pull_requests.is_eligible_change
),
daily_percentiles as (
    select
        repository_id,
        repository_full_name,
        date_day,
        count(*) filter (where is_change_linked)::bigint as linked_change_count,
        percentile_cont(0.5) within group (order by lead_time_seconds)
            filter (where is_change_linked) as p50_lead_time_seconds,
        percentile_cont(0.9) within group (order by lead_time_seconds)
            filter (where is_change_linked) as p90_lead_time_seconds
    from eligible_changes
    group by repository_id, repository_full_name, date_day
)
select
    metrics.repository_full_name,
    metrics.date_day,
    values_by_percentile.percentile_name,
    metrics.repository_full_name || ' · ' || upper(values_by_percentile.percentile_name)
        as series_name,
    round((values_by_percentile.lead_time_seconds / 3600.0)::numeric, 2)
        as lead_time_hours,
    coalesce(percentiles.linked_change_count, 0) as linked_change_count,
    metrics.measurement_status,
    metrics.coverage_numerator,
    metrics.coverage_denominator,
    metrics.coverage_ratio,
    metrics.definition_version,
    metrics.exclusion_reason
from analytics_marts.fct_delivery_performance_daily as metrics
left join daily_percentiles as percentiles
    on metrics.repository_id = percentiles.repository_id
    and metrics.date_day = percentiles.date_day
cross join lateral (
    values
        ('p50'::text, percentiles.p50_lead_time_seconds),
        ('p90'::text, percentiles.p90_lead_time_seconds)
) as values_by_percentile(percentile_name, lead_time_seconds)
where metrics.metric_name = 'change_lead_time'
order by metrics.repository_full_name, metrics.date_day, values_by_percentile.percentile_name;
