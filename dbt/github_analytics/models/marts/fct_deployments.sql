with deployment_evidence as (
    select
        deployments.deployment_key,
        deployments.repository_id,
        deployments.repository_full_name,
        'deployment_status'::text as evidence_type,
        deployments.deployment_id,
        null::bigint as workflow_run_id,
        deployments.environment,
        null::text as workflow_name,
        deployments.deployment_sha,
        deployments.created_at,
        deployments.successful_at,
        deployments.latest_deployment_state as latest_state,
        deployments.is_successful,
        deployments.seconds_to_success,
        deployments.deployment_snapshot_count as snapshot_count,
        deployments.deployment_source_count as source_count
    from {{ ref('int_production_deployments') }} as deployments
),
workflow_snapshots as (
    select
        workflows.*,
        row_number() over (
            partition by workflows.workflow_run_key
            order by
                coalesce(workflows.updated_at, workflows.created_at, workflows.ingested_at) desc,
                case when workflows.source = 'backfill' then 1 else 0 end desc,
                workflows.ingested_at desc,
                workflows.event_id desc
        ) as snapshot_rank,
        count(*) over (partition by workflows.workflow_run_key)::integer as snapshot_count
    from {{ ref('stg_github__workflow_runs') }} as workflows
),
workflow_source_counts as (
    select
        workflow_run_key,
        count(distinct source)::integer as source_count
    from workflow_snapshots
    group by workflow_run_key
),
workflow_evidence as (
    select
        workflows.workflow_run_key as deployment_key,
        workflows.repository_id,
        workflows.repository_full_name,
        'workflow_run'::text as evidence_type,
        null::bigint as deployment_id,
        workflows.workflow_run_id,
        null::text as environment,
        workflows.workflow_name,
        workflows.head_sha as deployment_sha,
        workflows.created_at,
        case
            when workflows.workflow_status = 'completed'
             and workflows.conclusion = 'success'
                then workflows.updated_at
        end as successful_at,
        coalesce(workflows.conclusion, workflows.workflow_status) as latest_state,
        workflows.workflow_status = 'completed'
            and workflows.conclusion = 'success' as is_successful,
        case
            when workflows.workflow_status = 'completed'
             and workflows.conclusion = 'success'
             and workflows.updated_at >= workflows.created_at
                then extract(epoch from (workflows.updated_at - workflows.created_at))::bigint
        end as seconds_to_success,
        workflows.snapshot_count,
        source_counts.source_count
    from workflow_snapshots as workflows
    inner join workflow_source_counts as source_counts using (workflow_run_key)
    inner join {{ ref('dim_repository') }} as repositories
        on workflows.repository_id = repositories.repository_id
        and repositories.production_signal = 'workflow_run'
        and lower(workflows.workflow_name) = lower(repositories.production_workflow_name)
    where workflows.snapshot_rank = 1
),
all_evidence as (
    select * from deployment_evidence
    union all
    select * from workflow_evidence
),
classified as (
    select
        evidence.*,
        repositories.production_signal,
        repositories.production_environment_names,
        repositories.definition_version,
        repositories.is_metric_configured,
        case
            when not repositories.is_metric_configured then false
            when evidence.evidence_type != repositories.production_signal then false
            when evidence.evidence_type = 'deployment_status'
                then lower(evidence.environment) = any(
                    string_to_array(lower(repositories.production_environment_names), '|')
                )
            else true
        end as is_primary_evidence
    from all_evidence as evidence
    inner join {{ ref('dim_repository') }} as repositories using (repository_id)
)
select
    deployment_key,
    repository_id,
    repository_full_name,
    evidence_type,
    deployment_id,
    workflow_run_id,
    environment,
    workflow_name,
    deployment_sha,
    created_at,
    successful_at,
    latest_state,
    is_successful,
    seconds_to_success,
    snapshot_count,
    source_count,
    is_primary_evidence,
    case
        when not is_metric_configured then 'unavailable'
        when not is_primary_evidence then 'unavailable'
        when successful_at is not null and successful_at < created_at then 'unavailable'
        when not is_successful or successful_at is null then 'unavailable'
        when evidence_type = 'workflow_run' then 'configured_proxy'
        else 'measured'
    end as measurement_status,
    coalesce(definition_version, 'v1.0.0') as definition_version,
    case
        when not is_metric_configured then 'missing_repository_configuration'
        when evidence_type != production_signal then 'non_primary_production_signal'
        when not is_primary_evidence then 'production_environment_not_configured'
        when successful_at is not null and successful_at < created_at
            then 'impossible_success_timestamp'
        when not is_successful or successful_at is null then 'production_evidence_not_successful'
    end as exclusion_reason
from classified
