with observed_repositories as (
    select
        repository_id,
        max(repository_full_name) as repository_full_name,
        max(installation_id) as installation_id
    from {{ ref('int_pr_lifecycle') }}
    group by repository_id

    union all

    select
        repository_id,
        max(repository_full_name) as repository_full_name,
        max(installation_id) as installation_id
    from {{ ref('int_production_deployments') }}
    group by repository_id
),
observed_rollup as (
    select
        repository_id,
        max(repository_full_name) as repository_full_name,
        max(installation_id) as installation_id
    from observed_repositories
    group by repository_id
),
repository_ids as (
    select repository_id from observed_rollup
    union
    select repository_id from {{ ref('repository_metric_config') }}
)
select
    repository_ids.repository_id,
    coalesce(config.repository_full_name, observed.repository_full_name) as repository_full_name,
    observed.installation_id,
    config.default_branch,
    config.repository_timezone,
    config.production_signal,
    config.production_environment_names,
    config.production_workflow_name,
    config.incident_label,
    config.rollback_label,
    config.hotfix_label,
    config.unplanned_rework_label,
    config.definition_version,
    config.repository_id is not null as is_metric_configured
from repository_ids
left join observed_rollup as observed using (repository_id)
left join {{ ref('repository_metric_config') }} as config using (repository_id)
