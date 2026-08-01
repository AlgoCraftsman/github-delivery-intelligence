-- Grain: one repository and review-rework cycle count.
-- Visualization: ordered bar chart showing the PR distribution by rework cycles.
-- Filters: repository_full_name.
-- Definition: one cycle per distinct reviewed revision with resolved CHANGES_REQUESTED state.
with reviewed_pull_requests as (
    select
        repository_full_name,
        review_rework_cycle_count
    from analytics_marts.fct_pull_requests
    where not is_draft
      and eligible_review_count > 0
),
distribution as (
    select
        repository_full_name,
        review_rework_cycle_count,
        count(*)::bigint as pull_request_count
    from reviewed_pull_requests
    group by repository_full_name, review_rework_cycle_count
)
select
    repository_full_name,
    review_rework_cycle_count,
    pull_request_count,
    round(
        pull_request_count::numeric
        / sum(pull_request_count) over (partition by repository_full_name),
        4
    ) as repository_share
from distribution
order by repository_full_name, review_rework_cycle_count;
