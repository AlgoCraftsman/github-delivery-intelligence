with deployment_snapshots as (
    select
        *,
        row_number() over (
            partition by deployment_key
            order by
                coalesce(updated_at, created_at, occurred_at, ingested_at) desc,
                case when source = 'backfill' then 1 else 0 end desc,
                ingested_at desc,
                event_id desc
        ) as snapshot_rank
    from {{ ref('stg_github__deployments') }}
),
deployment_rollup as (
    select
        deployment_key,
        min(created_at) as created_at,
        max(updated_at) as updated_at,
        count(*)::integer as snapshot_count,
        count(distinct source)::integer as source_count,
        min(ingested_at) as first_ingested_at,
        max(ingested_at) as last_ingested_at
    from deployment_snapshots
    group by deployment_key
),
resolved_deployments as (
    select
        latest.repository_id,
        latest.repository_full_name,
        latest.installation_id,
        latest.deployment_key,
        latest.deployment_id,
        latest.environment,
        latest.deployment_sha,
        latest.git_ref,
        latest.deployment_task,
        latest.is_transient_environment,
        latest.is_production_environment,
        rollup.created_at,
        rollup.updated_at,
        rollup.snapshot_count,
        rollup.source_count,
        rollup.first_ingested_at,
        rollup.last_ingested_at
    from deployment_snapshots as latest
    inner join deployment_rollup as rollup using (deployment_key)
    where latest.snapshot_rank = 1
),
status_snapshots as (
    select
        *,
        row_number() over (
            partition by deployment_status_key
            order by
                coalesce(updated_at, created_at, occurred_at, ingested_at) desc,
                case when source = 'backfill' then 1 else 0 end desc,
                ingested_at desc,
                event_id desc
        ) as snapshot_rank,
        count(*) over (partition by deployment_status_key)::integer as snapshot_count
    from {{ ref('stg_github__deployment_statuses') }}
),
resolved_statuses as (
    select *
    from status_snapshots
    where snapshot_rank = 1
),
ranked_statuses as (
    select
        *,
        row_number() over (
            partition by repository_id, deployment_id
            order by created_at desc, deployment_status_id desc
        ) as deployment_status_rank
    from resolved_statuses
),
status_rollup as (
    select
        repository_id,
        deployment_id,
        min(created_at) filter (where deployment_state = 'success') as successful_at,
        count(*)::integer as status_record_count,
        sum(snapshot_count)::integer as status_snapshot_count
    from resolved_statuses
    group by repository_id, deployment_id
),
latest_status as (
    select
        repository_id,
        deployment_id,
        deployment_status_key as latest_deployment_status_key,
        deployment_status_id as latest_deployment_status_id,
        deployment_state as latest_deployment_state,
        created_at as latest_status_at
    from ranked_statuses
    where deployment_status_rank = 1
)
select
    deployments.repository_id,
    deployments.repository_full_name,
    deployments.installation_id,
    deployments.deployment_key,
    deployments.deployment_id,
    deployments.environment,
    deployments.deployment_sha,
    deployments.git_ref,
    deployments.deployment_task,
    deployments.is_transient_environment,
    deployments.created_at,
    deployments.updated_at,
    latest_status.latest_deployment_status_key,
    latest_status.latest_deployment_status_id,
    latest_status.latest_deployment_state,
    latest_status.latest_status_at,
    status_rollup.successful_at,
    status_rollup.successful_at is not null as is_successful,
    case
        when status_rollup.successful_at is not null
            then extract(epoch from (status_rollup.successful_at - deployments.created_at))::bigint
    end as seconds_to_success,
    deployments.snapshot_count as deployment_snapshot_count,
    deployments.source_count as deployment_source_count,
    coalesce(status_rollup.status_record_count, 0)::integer as status_record_count,
    coalesce(status_rollup.status_snapshot_count, 0)::integer as status_snapshot_count,
    deployments.first_ingested_at,
    deployments.last_ingested_at
from resolved_deployments as deployments
left join status_rollup
    on deployments.repository_id = status_rollup.repository_id
    and deployments.deployment_id = status_rollup.deployment_id
left join latest_status
    on deployments.repository_id = latest_status.repository_id
    and deployments.deployment_id = latest_status.deployment_id
where deployments.is_production_environment
