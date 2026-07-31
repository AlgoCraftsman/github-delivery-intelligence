{% if var('fixture_validation', false) %}

with actual as (
    select 'pull_requests' as resource, count(*) as row_count
    from {{ ref('stg_github__pull_requests') }}
    union all
    select 'reviews', count(*) from {{ ref('stg_github__reviews') }}
    union all
    select 'pull_request_commits', count(*) from {{ ref('stg_github__pull_request_commits') }}
    union all
    select 'workflow_runs', count(*) from {{ ref('stg_github__workflow_runs') }}
    union all
    select 'deployments', count(*) from {{ ref('stg_github__deployments') }}
    union all
    select 'deployment_statuses', count(*) from {{ ref('stg_github__deployment_statuses') }}
),
expected(resource, row_count) as (
    values
        ('pull_requests', 5::bigint),
        ('reviews', 2::bigint),
        ('pull_request_commits', 1::bigint),
        ('workflow_runs', 3::bigint),
        ('deployments', 3::bigint),
        ('deployment_statuses', 3::bigint)
)
select
    expected.resource,
    expected.row_count as expected_count,
    actual.row_count as actual_count
from expected
left join actual using (resource)
where actual.row_count is distinct from expected.row_count

{% else %}

select 1 as fixture_validation_disabled
where false

{% endif %}
