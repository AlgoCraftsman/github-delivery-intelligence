-- Grain: one repository and fixed review-latency bucket.
-- Visualization: ordered bar chart.
-- Filters: repository_full_name.
-- Assumption: drafts and PRs without an eligible review are excluded from this distribution.
with reviewed_pull_requests as (
    select
        repository_full_name,
        seconds_to_first_review / 3600.0 as review_latency_hours
    from analytics_marts.fct_pull_requests
    where not is_draft
      and seconds_to_first_review is not null
),
bucketed as (
    select
        repository_full_name,
        case
            when review_latency_hours < 4 then 'Under 4h'
            when review_latency_hours < 12 then '4–12h'
            when review_latency_hours < 24 then '12–24h'
            when review_latency_hours < 48 then '24–48h'
            else '48h+'
        end as latency_bucket,
        case
            when review_latency_hours < 4 then 1
            when review_latency_hours < 12 then 2
            when review_latency_hours < 24 then 3
            when review_latency_hours < 48 then 4
            else 5
        end as bucket_order
    from reviewed_pull_requests
)
select
    repository_full_name,
    latency_bucket,
    count(*)::bigint as pull_request_count
from bucketed
group by repository_full_name, latency_bucket, bucket_order
order by repository_full_name, bucket_order;
