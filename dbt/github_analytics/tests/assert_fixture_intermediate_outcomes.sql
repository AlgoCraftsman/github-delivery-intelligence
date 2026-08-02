{% if var('fixture_validation', false) %}

with actual as (
    select
        'int_pr_lifecycle'::text as model_name,
        pull_request_key as record_key,
        jsonb_build_object(
            'state', pull_request_state,
            'is_merged', is_merged,
            'lifecycle_seconds', lifecycle_seconds,
            'snapshot_count', snapshot_count,
            'source_count', source_count
        ) as outcome
    from {{ ref('int_pr_lifecycle') }}
    where repository_id != 20004
    union all
    select
        'int_first_eligible_review',
        pull_request_key,
        jsonb_build_object(
            'review_state', review_state,
            'seconds_to_first_review', seconds_to_first_review,
            'is_pre_merge_review', is_pre_merge_review,
            'review_snapshot_count', review_snapshot_count,
            'review_source_count', review_source_count
        )
    from {{ ref('int_first_eligible_review') }}
    where repository_id != 20004
    union all
    select
        'int_production_deployments',
        deployment_key,
        jsonb_build_object(
            'latest_state', latest_deployment_state,
            'is_successful', is_successful,
            'seconds_to_success', seconds_to_success,
            'deployment_snapshot_count', deployment_snapshot_count,
            'deployment_source_count', deployment_source_count,
            'status_record_count', status_record_count,
            'status_snapshot_count', status_snapshot_count
        )
    from {{ ref('int_production_deployments') }}
    union all
    select
        'int_change_to_deployment',
        change_deployment_key,
        jsonb_build_object(
            'linkage_type', linkage_type,
            'lead_time_seconds', lead_time_seconds
        )
    from {{ ref('int_change_to_deployment') }}
),
expected(model_name, record_key, outcome) as (
    values
        (
            'int_pr_lifecycle',
            '20001:17',
            '{
              "state": "merged",
              "is_merged": true,
              "lifecycle_seconds": 180000,
              "snapshot_count": 2,
              "source_count": 2
            }'::jsonb
        ),
        (
            'int_first_eligible_review',
            '20001:17',
            '{
              "review_state": "approved",
              "seconds_to_first_review": 82800,
              "is_pre_merge_review": true,
              "review_snapshot_count": 2,
              "review_source_count": 2
            }'::jsonb
        ),
        (
            'int_production_deployments',
            '20001:70001',
            '{
              "latest_state": "success",
              "is_successful": true,
              "seconds_to_success": 300,
              "deployment_snapshot_count": 2,
              "deployment_source_count": 2,
              "status_record_count": 1,
              "status_snapshot_count": 2
            }'::jsonb
        ),
        (
            'int_change_to_deployment',
            '20001:17->20001:70001',
            '{
              "linkage_type": "merge_commit",
              "lead_time_seconds": 93900
            }'::jsonb
        ),
        (
            'int_pr_lifecycle',
            '20001:18',
            '{
              "state": "merged",
              "is_merged": true,
              "lifecycle_seconds": 180000,
              "snapshot_count": 1,
              "source_count": 1
            }'::jsonb
        ),
        (
            'int_pr_lifecycle',
            '20002:1',
            '{
              "state": "merged",
              "is_merged": true,
              "lifecycle_seconds": 165600,
              "snapshot_count": 1,
              "source_count": 1
            }'::jsonb
        ),
        (
            'int_production_deployments',
            '20002:71001',
            '{
              "latest_state": "success",
              "is_successful": true,
              "seconds_to_success": 600,
              "deployment_snapshot_count": 1,
              "deployment_source_count": 1,
              "status_record_count": 1,
              "status_snapshot_count": 1
            }'::jsonb
        ),
        (
            'int_change_to_deployment',
            '20002:1->20002:71001',
            '{
              "linkage_type": "merge_commit",
              "lead_time_seconds": 105000
            }'::jsonb
        ),
        (
            'int_pr_lifecycle',
            '20003:1',
            '{
              "state": "merged",
              "is_merged": true,
              "lifecycle_seconds": 165600,
              "snapshot_count": 1,
              "source_count": 1
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
