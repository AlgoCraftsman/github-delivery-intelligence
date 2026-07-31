{% if var('fixture_validation', false) %}

with actual as (
    select
        'dim_repository'::text as model_name,
        repository_id::text as record_key,
        jsonb_build_object(
            'is_metric_configured', is_metric_configured,
            'production_signal', production_signal
        ) as outcome
    from {{ ref('dim_repository') }}
    where repository_id in (20001, 20002, 20003)

    union all

    select
        'fct_pull_requests',
        pull_request_key,
        jsonb_build_object(
            'is_eligible_change', is_eligible_change,
            'is_change_linked', is_change_linked,
            'lead_time_seconds', lead_time_seconds,
            'measurement_status', measurement_status,
            'exclusion_reason', exclusion_reason
        )
    from {{ ref('fct_pull_requests') }}

    union all

    select
        'fct_deployments',
        deployment_key,
        jsonb_build_object(
            'is_primary_evidence', is_primary_evidence,
            'measurement_status', measurement_status,
            'exclusion_reason', exclusion_reason
        )
    from {{ ref('fct_deployments') }}

    union all

    select
        'fct_delivery_performance_daily',
        repository_id::text || ':' || date_day::text || ':' || metric_name,
        jsonb_build_object(
            'metric_value', metric_value,
            'coverage_numerator', coverage_numerator,
            'coverage_denominator', coverage_denominator,
            'coverage_ratio', coverage_ratio,
            'measurement_status', measurement_status,
            'exclusion_reason', exclusion_reason
        )
    from {{ ref('fct_delivery_performance_daily') }}
    where (repository_id, date_day, metric_name) in (
        (20001::bigint, '2026-01-11'::date, 'deployment_frequency'::text),
        (20001::bigint, '2026-01-12'::date, 'change_lead_time'::text),
        (20001::bigint, '2026-01-13'::date, 'deployment_frequency'::text),
        (20002::bigint, '2026-01-13'::date, 'deployment_frequency'::text),
        (20003::bigint, '2026-01-12'::date, 'change_lead_time'::text),
        (20003::bigint, '2026-01-13'::date, 'deployment_frequency'::text)
    )
),
expected(model_name, record_key, outcome) as (
    values
        (
            'dim_repository',
            '20001',
            '{"is_metric_configured": true, "production_signal": "deployment_status"}'::jsonb
        ),
        (
            'dim_repository',
            '20002',
            '{"is_metric_configured": false, "production_signal": null}'::jsonb
        ),
        (
            'dim_repository',
            '20003',
            '{"is_metric_configured": true, "production_signal": "workflow_run"}'::jsonb
        ),
        (
            'fct_pull_requests',
            '20001:17',
            '{
              "is_eligible_change": true,
              "is_change_linked": true,
              "lead_time_seconds": 93900,
              "measurement_status": "measured",
              "exclusion_reason": null
            }'::jsonb
        ),
        (
            'fct_pull_requests',
            '20001:18',
            '{
              "is_eligible_change": true,
              "is_change_linked": false,
              "lead_time_seconds": null,
              "measurement_status": "unavailable",
              "exclusion_reason": "missing_exact_sha_linkage"
            }'::jsonb
        ),
        (
            'fct_pull_requests',
            '20002:1',
            '{
              "is_eligible_change": true,
              "is_change_linked": false,
              "lead_time_seconds": null,
              "measurement_status": "unavailable",
              "exclusion_reason": "missing_repository_configuration"
            }'::jsonb
        ),
        (
            'fct_pull_requests',
            '20003:1',
            '{
              "is_eligible_change": true,
              "is_change_linked": true,
              "lead_time_seconds": 105600,
              "measurement_status": "configured_proxy",
              "exclusion_reason": null
            }'::jsonb
        ),
        (
            'fct_deployments',
            '20001:70001',
            '{
              "is_primary_evidence": true,
              "measurement_status": "measured",
              "exclusion_reason": null
            }'::jsonb
        ),
        (
            'fct_deployments',
            '20002:71001',
            '{
              "is_primary_evidence": false,
              "measurement_status": "unavailable",
              "exclusion_reason": "missing_repository_configuration"
            }'::jsonb
        ),
        (
            'fct_deployments',
            '20003:62001:1',
            '{
              "is_primary_evidence": true,
              "measurement_status": "configured_proxy",
              "exclusion_reason": null
            }'::jsonb
        ),
        (
            'fct_delivery_performance_daily',
            '20001:2026-01-11:deployment_frequency',
            '{
              "metric_value": 0,
              "coverage_numerator": 0,
              "coverage_denominator": 0,
              "coverage_ratio": null,
              "measurement_status": "measured",
              "exclusion_reason": null
            }'::jsonb
        ),
        (
            'fct_delivery_performance_daily',
            '20001:2026-01-12:change_lead_time',
            '{
              "metric_value": 93900,
              "coverage_numerator": 1,
              "coverage_denominator": 2,
              "coverage_ratio": 0.5,
              "measurement_status": "measured",
              "exclusion_reason": null
            }'::jsonb
        ),
        (
            'fct_delivery_performance_daily',
            '20001:2026-01-13:deployment_frequency',
            '{
              "metric_value": 1,
              "coverage_numerator": 1,
              "coverage_denominator": 1,
              "coverage_ratio": 1,
              "measurement_status": "measured",
              "exclusion_reason": null
            }'::jsonb
        ),
        (
            'fct_delivery_performance_daily',
            '20002:2026-01-13:deployment_frequency',
            '{
              "metric_value": null,
              "coverage_numerator": 0,
              "coverage_denominator": 1,
              "coverage_ratio": 0,
              "measurement_status": "unavailable",
              "exclusion_reason": "missing_repository_configuration"
            }'::jsonb
        ),
        (
            'fct_delivery_performance_daily',
            '20003:2026-01-12:change_lead_time',
            '{
              "metric_value": 105600,
              "coverage_numerator": 1,
              "coverage_denominator": 1,
              "coverage_ratio": 1,
              "measurement_status": "configured_proxy",
              "exclusion_reason": null
            }'::jsonb
        ),
        (
            'fct_delivery_performance_daily',
            '20003:2026-01-13:deployment_frequency',
            '{
              "metric_value": 1,
              "coverage_numerator": 1,
              "coverage_denominator": 1,
              "coverage_ratio": 1,
              "measurement_status": "configured_proxy",
              "exclusion_reason": null
            }'::jsonb
        )
),
mismatches as (
    select
        coalesce(actual.model_name, expected.model_name) as model_name,
        coalesce(actual.record_key, expected.record_key) as record_key,
        expected.outcome as expected_outcome,
        actual.outcome as actual_outcome
    from actual
    full outer join expected using (model_name, record_key)
    where actual.outcome is distinct from expected.outcome
)
select * from mismatches

{% else %}

select
    cast(null as text) as model_name,
    cast(null as text) as record_key,
    cast(null as jsonb) as expected_outcome,
    cast(null as jsonb) as actual_outcome
where false

{% endif %}
