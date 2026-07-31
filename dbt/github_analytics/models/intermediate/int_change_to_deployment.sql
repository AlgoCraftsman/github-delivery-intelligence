with merge_changes as (
    select
        repository_id,
        repository_full_name,
        pull_request_key,
        pull_request_number,
        pull_request_node_id,
        merge_commit_sha as change_sha,
        'merge_commit'::text as linkage_type,
        merged_at as change_completed_at,
        1 as linkage_priority
    from {{ ref('int_pr_lifecycle') }}
    where is_merged
      and merge_commit_sha is not null
      and merged_at is not null
),
pull_request_commit_changes as (
    select
        pull_requests.repository_id,
        pull_requests.repository_full_name,
        pull_requests.pull_request_key,
        pull_requests.pull_request_number,
        pull_requests.pull_request_node_id,
        commits.commit_sha as change_sha,
        'pull_request_commit'::text as linkage_type,
        commits.committed_at as change_completed_at,
        2 as linkage_priority
    from {{ ref('stg_github__pull_request_commits') }} as commits
    inner join {{ ref('int_pr_lifecycle') }} as pull_requests
        on commits.repository_id = pull_requests.repository_id
        and commits.pull_request_node_id = pull_requests.pull_request_node_id
    where commits.commit_sha is not null
      and commits.committed_at is not null
),
changes as (
    select * from merge_changes
    union all
    select * from pull_request_commit_changes
),
candidate_links as (
    select
        changes.repository_id,
        changes.repository_full_name,
        changes.pull_request_key,
        changes.pull_request_number,
        changes.pull_request_node_id,
        deployments.deployment_key,
        deployments.deployment_id,
        deployments.environment,
        changes.change_sha,
        changes.linkage_type,
        changes.change_completed_at,
        deployments.successful_at as deployed_at,
        extract(
            epoch from (deployments.successful_at - changes.change_completed_at)
        )::bigint as lead_time_seconds,
        row_number() over (
            partition by changes.pull_request_key, deployments.deployment_key
            order by changes.linkage_priority, changes.change_completed_at desc
        ) as linkage_rank
    from changes
    inner join {{ ref('int_production_deployments') }} as deployments
        on changes.repository_id = deployments.repository_id
        and changes.change_sha = deployments.deployment_sha
    where deployments.is_successful
      and deployments.successful_at >= changes.change_completed_at
)
select
    pull_request_key || '->' || deployment_key as change_deployment_key,
    repository_id,
    repository_full_name,
    pull_request_key,
    pull_request_number,
    pull_request_node_id,
    deployment_key,
    deployment_id,
    environment,
    change_sha,
    linkage_type,
    change_completed_at,
    deployed_at,
    lead_time_seconds
from candidate_links
where linkage_rank = 1
