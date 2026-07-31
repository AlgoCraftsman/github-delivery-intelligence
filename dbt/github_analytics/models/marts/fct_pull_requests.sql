with deployment_links as (
    select
        links.pull_request_key,
        deployments.deployment_key,
        links.linkage_type,
        deployments.evidence_type as deployment_evidence_type,
        links.change_completed_at,
        deployments.successful_at as deployed_at,
        extract(
            epoch from (deployments.successful_at - links.change_completed_at)
        )::bigint as lead_time_seconds,
        deployments.measurement_status,
        deployments.definition_version
    from {{ ref('int_change_to_deployment') }} as links
    inner join {{ ref('fct_deployments') }} as deployments
        on links.deployment_key = deployments.deployment_key
        and deployments.measurement_status in ('measured', 'configured_proxy')
),
workflow_links as (
    select
        pull_requests.pull_request_key,
        deployments.deployment_key,
        'merge_commit'::text as linkage_type,
        deployments.evidence_type as deployment_evidence_type,
        pull_requests.merged_at as change_completed_at,
        deployments.successful_at as deployed_at,
        extract(
            epoch from (deployments.successful_at - pull_requests.merged_at)
        )::bigint as lead_time_seconds,
        deployments.measurement_status,
        deployments.definition_version
    from {{ ref('int_pr_lifecycle') }} as pull_requests
    inner join {{ ref('fct_deployments') }} as deployments
        on pull_requests.repository_id = deployments.repository_id
        and pull_requests.merge_commit_sha = deployments.deployment_sha
        and deployments.successful_at >= pull_requests.merged_at
        and deployments.measurement_status = 'configured_proxy'
        and deployments.evidence_type = 'workflow_run'
    where pull_requests.is_merged
      and pull_requests.merged_at is not null
      and pull_requests.merge_commit_sha is not null
),
candidate_links as (
    select * from deployment_links
    union all
    select
        pull_request_key,
        deployment_key,
        linkage_type,
        deployment_evidence_type,
        change_completed_at,
        deployed_at,
        lead_time_seconds,
        measurement_status,
        definition_version
    from workflow_links
),
ranked_links as (
    select
        candidate_links.*,
        row_number() over (
            partition by pull_request_key
            order by deployed_at, deployment_key
        ) as link_rank
    from candidate_links
),
selected_links as (
    select *
    from ranked_links
    where link_rank = 1
)
select
    pull_requests.repository_id,
    pull_requests.repository_full_name,
    pull_requests.pull_request_key,
    pull_requests.pull_request_number,
    pull_requests.pull_request_state,
    pull_requests.title,
    pull_requests.is_draft,
    pull_requests.created_at,
    reviews.first_review_at,
    pull_requests.closed_at,
    pull_requests.merged_at,
    pull_requests.resolved_at,
    pull_requests.lifecycle_seconds,
    reviews.seconds_to_first_review,
    case
        when reviews.first_review_at is not null and pull_requests.merged_at >= reviews.first_review_at
            then extract(epoch from (pull_requests.merged_at - reviews.first_review_at))::bigint
    end as seconds_from_first_review_to_merge,
    pull_requests.additions,
    pull_requests.deletions,
    pull_requests.changed_files,
    pull_requests.snapshot_count,
    pull_requests.source_count,
    pull_requests.is_merged
        and pull_requests.merged_at is not null
        and pull_requests.merged_at >= pull_requests.created_at as is_eligible_change,
    links.deployment_key,
    links.linkage_type,
    links.deployment_evidence_type,
    links.deployed_at,
    links.lead_time_seconds,
    links.deployment_key is not null as is_change_linked,
    case
        when not pull_requests.is_merged then 'unavailable'
        when pull_requests.merged_at is null then 'unavailable'
        when pull_requests.merged_at < pull_requests.created_at then 'unavailable'
        when not repositories.is_metric_configured then 'unavailable'
        when pull_requests.merge_commit_sha is null then 'unavailable'
        when links.deployment_key is null then 'unavailable'
        else links.measurement_status
    end as measurement_status,
    coalesce(links.definition_version, repositories.definition_version, 'v1.0.0')
        as definition_version,
    case
        when not pull_requests.is_merged then 'pull_request_not_merged'
        when pull_requests.merged_at is null then 'missing_merge_timestamp'
        when pull_requests.merged_at < pull_requests.created_at then 'impossible_merge_timestamp'
        when not repositories.is_metric_configured then 'missing_repository_configuration'
        when pull_requests.merge_commit_sha is null then 'missing_change_sha'
        when links.deployment_key is null then 'missing_exact_sha_linkage'
    end as exclusion_reason
from {{ ref('int_pr_lifecycle') }} as pull_requests
left join {{ ref('int_first_eligible_review') }} as reviews
    on pull_requests.pull_request_key = reviews.pull_request_key
inner join {{ ref('dim_repository') }} as repositories
    on pull_requests.repository_id = repositories.repository_id
left join selected_links as links
    on pull_requests.pull_request_key = links.pull_request_key
