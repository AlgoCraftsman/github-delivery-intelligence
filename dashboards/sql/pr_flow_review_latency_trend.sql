-- Grain: one repository and repository-local PR creation date.
-- Visualization: line chart for p50 and p90; include review coverage in tooltips.
-- Filters: repository_full_name and date_day.
-- Assumption: non-draft PRs form the denominator; unresolved PRs remain eligible.
with pull_requests as (
    select
        facts.repository_id,
        facts.repository_full_name,
        (facts.created_at at time zone repositories.repository_timezone)::date as date_day,
        facts.seconds_to_first_review
    from analytics_marts.fct_pull_requests as facts
    inner join analytics_marts.dim_repository as repositories using (repository_id)
    where not facts.is_draft
)
select
    repository_full_name,
    date_day,
    round((
        percentile_cont(0.5) within group (order by seconds_to_first_review)
            filter (where seconds_to_first_review is not null) / 3600.0
    )::numeric, 2) as p50_review_latency_hours,
    round((
        percentile_cont(0.9) within group (order by seconds_to_first_review)
            filter (where seconds_to_first_review is not null) / 3600.0
    )::numeric, 2) as p90_review_latency_hours,
    count(*) filter (where seconds_to_first_review is not null)::bigint as reviewed_pr_count,
    count(*)::bigint as eligible_pr_count,
    round(
        count(*) filter (where seconds_to_first_review is not null)::numeric
        / nullif(count(*), 0),
        4
    ) as review_coverage_ratio
from pull_requests
group by repository_full_name, date_day
order by repository_full_name, date_day;
