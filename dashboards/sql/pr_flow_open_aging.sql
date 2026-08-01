-- Grain: one open pull request at a deterministic as_of timestamp.
-- Visualization: detail table plus WIP count and aging-bucket summary cards.
-- Filters: repository_full_name, is_draft, and aging_bucket.
-- Demo anchor: update only this CTE for a live view; screenshots use 2026-01-14 12:00 UTC.
with params as (
    select '2026-01-14T12:00:00Z'::timestamptz as as_of
),
open_pull_requests as (
    select
        facts.repository_full_name,
        facts.pull_request_number,
        facts.title,
        facts.is_draft,
        facts.created_at,
        params.as_of,
        extract(epoch from (params.as_of - facts.created_at)) / 3600.0 as age_hours,
        facts.seconds_to_first_review is not null as has_eligible_review,
        facts.review_rework_cycle_count
    from analytics_marts.fct_pull_requests as facts
    cross join params
    where facts.pull_request_state = 'open'
      and facts.created_at <= params.as_of
)
select
    repository_full_name,
    pull_request_number,
    title,
    is_draft,
    created_at,
    as_of,
    round(age_hours::numeric, 1) as age_hours,
    case
        when age_hours < 24 then 'Under 1d'
        when age_hours < 72 then '1–3d'
        when age_hours < 168 then '3–7d'
        else '7d+'
    end as aging_bucket,
    has_eligible_review,
    review_rework_cycle_count,
    count(*) over (partition by repository_full_name)::bigint as repository_wip
from open_pull_requests
order by repository_full_name, age_hours desc, pull_request_number;
