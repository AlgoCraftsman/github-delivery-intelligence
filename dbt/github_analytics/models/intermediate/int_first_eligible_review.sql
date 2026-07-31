with review_snapshots as (
    select
        *,
        row_number() over (
            partition by review_key
            order by
                coalesce(updated_at, submitted_at, occurred_at, ingested_at) desc,
                case when source = 'backfill' then 1 else 0 end desc,
                ingested_at desc,
                event_id desc
        ) as snapshot_rank,
        count(*) over (partition by review_key)::integer as snapshot_count
    from {{ ref('stg_github__reviews') }}
),
review_source_rollup as (
    select
        review_key,
        count(distinct source)::integer as source_count
    from review_snapshots
    group by review_key
),
resolved_reviews as (
    select
        snapshots.*,
        rollup.source_count
    from review_snapshots as snapshots
    inner join review_source_rollup as rollup using (review_key)
    where snapshots.snapshot_rank = 1
),
eligible_reviews as (
    select
        pull_requests.repository_id,
        pull_requests.repository_full_name,
        pull_requests.pull_request_key,
        pull_requests.pull_request_number,
        reviews.review_key,
        reviews.review_id,
        reviews.review_node_id,
        reviews.review_state,
        reviews.reviewer_id,
        reviews.reviewer_node_id,
        reviews.reviewer_login,
        reviews.commit_sha,
        reviews.submitted_at,
        reviews.snapshot_count,
        reviews.source_count,
        extract(epoch from (reviews.submitted_at - pull_requests.created_at))::bigint
            as seconds_to_first_review,
        (
            pull_requests.merged_at is null
            or reviews.submitted_at <= pull_requests.merged_at
        ) as is_pre_merge_review,
        row_number() over (
            partition by pull_requests.pull_request_key
            order by reviews.submitted_at, reviews.review_key
        ) as review_rank
    from resolved_reviews as reviews
    inner join {{ ref('int_pr_lifecycle') }} as pull_requests
        on reviews.repository_id = pull_requests.repository_id
        and (
            (
                reviews.pull_request_node_id is not null
                and reviews.pull_request_node_id = pull_requests.pull_request_node_id
            )
            or (
                reviews.pull_request_number is not null
                and reviews.pull_request_number = pull_requests.pull_request_number
            )
        )
    where reviews.review_state != 'pending'
      and lower(reviews.reviewer_login) != lower(pull_requests.author_login)
      and reviews.submitted_at >= pull_requests.created_at
)
select
    repository_id,
    repository_full_name,
    pull_request_key,
    pull_request_number,
    review_key,
    review_id,
    review_node_id,
    review_state,
    reviewer_id,
    reviewer_node_id,
    reviewer_login,
    commit_sha,
    submitted_at as first_review_at,
    seconds_to_first_review,
    is_pre_merge_review,
    snapshot_count as review_snapshot_count,
    source_count as review_source_count
from eligible_reviews
where review_rank = 1
