{% if var('fixture_validation', false) %}

with paired_keys as (
    select 'pull_request' as resource, pull_request_key as resource_key, source
    from {{ ref('stg_github__pull_requests') }}
    union all
    select 'review', review_key, source
    from {{ ref('stg_github__reviews') }}
    union all
    select 'workflow_run', workflow_run_key, source
    from {{ ref('stg_github__workflow_runs') }}
    union all
    select 'deployment', deployment_key, source
    from {{ ref('stg_github__deployments') }}
    union all
    select 'deployment_status', deployment_status_key, source
    from {{ ref('stg_github__deployment_statuses') }}
),
key_counts as (
    select
        resource,
        resource_key,
        count(*) as snapshot_count,
        count(distinct source) as source_count
    from paired_keys
    group by resource, resource_key
),
unpaired as (
    select resource, resource_key
    from key_counts
    where snapshot_count > 1
      and (snapshot_count != 2 or source_count != 2)
),
missing_review_parent as (
    select 'review_parent' as resource, review_key as resource_key
    from {{ ref('stg_github__reviews') }}
    where pull_request_node_id != 'PR_NODE_17'
),
incorrect_commit as (
    select 'pull_request_commit' as resource, pull_request_commit_key as resource_key
    from {{ ref('stg_github__pull_request_commits') }}
    where pull_request_node_id != 'PR_NODE_17'
       or commit_sha != 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'
)
select * from unpaired
union all
select * from missing_review_parent
union all
select * from incorrect_commit

{% else %}

select
    cast(null as text) as resource,
    cast(null as text) as resource_key
where false

{% endif %}
