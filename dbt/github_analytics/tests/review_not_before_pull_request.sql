with pull_requests as (
    select
        repository_id,
        pull_request_node_id,
        pull_request_number,
        min(created_at) as created_at
    from {{ ref('stg_github__pull_requests') }}
    group by repository_id, pull_request_node_id, pull_request_number
),
matched_reviews as (
    select
        reviews.event_id,
        reviews.submitted_at,
        pull_requests.created_at
    from {{ ref('stg_github__reviews') }} as reviews
    inner join pull_requests
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
)
select *
from matched_reviews
where submitted_at < created_at
