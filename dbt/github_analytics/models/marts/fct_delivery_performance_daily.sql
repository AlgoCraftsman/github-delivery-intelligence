with repository_dates as (
    select
        repositories.repository_id,
        repositories.repository_full_name,
        repositories.repository_timezone,
        repositories.production_signal,
        repositories.definition_version,
        repositories.is_metric_configured,
        dates.date_key,
        dates.date_day
    from {{ ref('dim_repository') }} as repositories
    cross join {{ ref('dim_date') }} as dates
),
deployment_daily as (
    select
        deployments.repository_id,
        (
            coalesce(deployments.successful_at, deployments.created_at)
            at time zone coalesce(repositories.repository_timezone, 'UTC')
        )::date as date_day,
        count(*)::bigint as candidate_evidence_count,
        count(*) filter (where deployments.is_primary_evidence)::bigint
            as configured_evidence_count,
        count(*) filter (
            where deployments.measurement_status in ('measured', 'configured_proxy')
        )::bigint as successful_deployment_count
    from {{ ref('fct_deployments') }} as deployments
    inner join {{ ref('dim_repository') }} as repositories using (repository_id)
    group by deployments.repository_id, date_day
),
change_daily as (
    select
        pull_requests.repository_id,
        (
            pull_requests.merged_at
            at time zone coalesce(repositories.repository_timezone, 'UTC')
        )::date as date_day,
        count(*) filter (where pull_requests.is_eligible_change)::bigint
            as eligible_change_count,
        count(*) filter (
            where pull_requests.is_eligible_change and pull_requests.is_change_linked
        )::bigint as linked_change_count,
        avg(pull_requests.lead_time_seconds) filter (
            where pull_requests.is_eligible_change and pull_requests.is_change_linked
        )::numeric as average_lead_time_seconds
    from {{ ref('fct_pull_requests') }} as pull_requests
    inner join {{ ref('dim_repository') }} as repositories using (repository_id)
    where pull_requests.merged_at is not null
    group by pull_requests.repository_id, date_day
),
daily_inputs as (
    select
        repository_dates.*,
        coalesce(deployments.candidate_evidence_count, 0)::bigint
            as candidate_evidence_count,
        coalesce(deployments.configured_evidence_count, 0)::bigint
            as configured_evidence_count,
        coalesce(deployments.successful_deployment_count, 0)::bigint
            as successful_deployment_count,
        coalesce(changes.eligible_change_count, 0)::bigint as eligible_change_count,
        coalesce(changes.linked_change_count, 0)::bigint as linked_change_count,
        changes.average_lead_time_seconds
    from repository_dates
    left join deployment_daily as deployments using (repository_id, date_day)
    left join change_daily as changes using (repository_id, date_day)
),
metric_names(metric_name, metric_unit) as (
    values
        ('deployment_frequency'::text, 'deployments'::text),
        ('change_lead_time'::text, 'seconds'::text),
        ('failed_deployment_recovery_time'::text, 'seconds'::text),
        ('change_failure_rate'::text, 'ratio'::text),
        ('deployment_rework_rate'::text, 'ratio'::text)
)
select
    inputs.repository_id::text || ':' || inputs.date_key::text || ':' || metrics.metric_name
        as delivery_performance_daily_key,
    inputs.repository_id,
    inputs.repository_full_name,
    inputs.date_key,
    inputs.date_day,
    metrics.metric_name,
    metrics.metric_unit,
    (case
        when metrics.metric_name = 'deployment_frequency'
         and inputs.is_metric_configured
            then inputs.successful_deployment_count::numeric
        when metrics.metric_name = 'change_lead_time'
         and inputs.is_metric_configured
         and inputs.linked_change_count > 0
            then inputs.average_lead_time_seconds
    end)::numeric(20, 6) as metric_value,
    case
        when metrics.metric_name = 'deployment_frequency'
            then inputs.configured_evidence_count
        when metrics.metric_name = 'change_lead_time' then inputs.linked_change_count
        else 0::bigint
    end as coverage_numerator,
    case
        when metrics.metric_name = 'deployment_frequency'
            then inputs.candidate_evidence_count
        when metrics.metric_name = 'change_lead_time' then inputs.eligible_change_count
        else inputs.successful_deployment_count
    end as coverage_denominator,
    (case
        when metrics.metric_name = 'deployment_frequency'
         and inputs.candidate_evidence_count > 0
            then inputs.configured_evidence_count::numeric
                / inputs.candidate_evidence_count::numeric
        when metrics.metric_name = 'change_lead_time'
         and inputs.eligible_change_count > 0
            then inputs.linked_change_count::numeric / inputs.eligible_change_count::numeric
        when metrics.metric_name in (
            'failed_deployment_recovery_time',
            'change_failure_rate',
            'deployment_rework_rate'
        ) and inputs.successful_deployment_count > 0 then 0::numeric
    end)::numeric(12, 6) as coverage_ratio,
    case
        when metrics.metric_name in (
            'failed_deployment_recovery_time',
            'change_failure_rate',
            'deployment_rework_rate'
        ) then 'unavailable'
        when not inputs.is_metric_configured then 'unavailable'
        when metrics.metric_name = 'change_lead_time'
         and inputs.eligible_change_count = 0 then 'unavailable'
        when metrics.metric_name = 'change_lead_time'
         and inputs.linked_change_count = 0 then 'unavailable'
        when inputs.production_signal = 'workflow_run' then 'configured_proxy'
        else 'measured'
    end as measurement_status,
    coalesce(inputs.definition_version, 'v1.0.0') as definition_version,
    case
        when metrics.metric_name = 'failed_deployment_recovery_time'
            then 'required_intervention_recovery_evidence_not_configured'
        when metrics.metric_name = 'change_failure_rate'
            then 'required_change_failure_evidence_not_configured'
        when metrics.metric_name = 'deployment_rework_rate'
            then 'required_unplanned_rework_evidence_not_configured'
        when not inputs.is_metric_configured then 'missing_repository_configuration'
        when metrics.metric_name = 'change_lead_time'
         and inputs.eligible_change_count = 0 then 'no_eligible_changes'
        when metrics.metric_name = 'change_lead_time'
         and inputs.linked_change_count = 0 then 'missing_change_deployment_linkage'
    end as exclusion_reason
from daily_inputs as inputs
cross join metric_names as metrics
