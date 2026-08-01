-- Grain: one repository, repository-local merge date, and documented size band.
-- Visualization: grouped line/bar chart for p50 cycle time by size_band.
-- Filters: repository_full_name, date_day, and size_band.
-- Size bands use additions + deletions: XS <=50, S 51-200, M 201-500, L >500.
with merged_pull_requests as (
    select
        facts.repository_full_name,
        (facts.merged_at at time zone repositories.repository_timezone)::date as date_day,
        facts.lifecycle_seconds,
        case
            when facts.additions is null and facts.deletions is null then 'Unknown'
            when coalesce(facts.additions, 0) + coalesce(facts.deletions, 0) <= 50 then 'XS (≤50)'
            when coalesce(facts.additions, 0) + coalesce(facts.deletions, 0) <= 200 then 'S (51–200)'
            when coalesce(facts.additions, 0) + coalesce(facts.deletions, 0) <= 500 then 'M (201–500)'
            else 'L (>500)'
        end as size_band,
        case
            when facts.additions is null and facts.deletions is null then 5
            when coalesce(facts.additions, 0) + coalesce(facts.deletions, 0) <= 50 then 1
            when coalesce(facts.additions, 0) + coalesce(facts.deletions, 0) <= 200 then 2
            when coalesce(facts.additions, 0) + coalesce(facts.deletions, 0) <= 500 then 3
            else 4
        end as size_band_order
    from analytics_marts.fct_pull_requests as facts
    inner join analytics_marts.dim_repository as repositories using (repository_id)
    where facts.pull_request_state = 'merged'
      and facts.lifecycle_seconds is not null
)
select
    repository_full_name,
    date_day,
    size_band,
    round((percentile_cont(0.5) within group (order by lifecycle_seconds) / 3600.0)::numeric, 2)
        as p50_cycle_time_hours,
    round((percentile_cont(0.9) within group (order by lifecycle_seconds) / 3600.0)::numeric, 2)
        as p90_cycle_time_hours,
    count(*)::bigint as merged_pr_count
from merged_pull_requests
group by repository_full_name, date_day, size_band, size_band_order
order by repository_full_name, date_day, size_band_order;
