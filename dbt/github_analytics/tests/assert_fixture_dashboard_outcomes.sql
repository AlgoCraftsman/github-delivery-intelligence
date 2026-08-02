{% if var('fixture_validation', false) %}

with actual as (
    select
        pull_request_key,
        jsonb_build_object(
            'state', pull_request_state,
            'is_draft', is_draft,
            'lifecycle_seconds', lifecycle_seconds,
            'seconds_to_first_review', seconds_to_first_review,
            'eligible_review_count', eligible_review_count,
            'review_rework_cycle_count', review_rework_cycle_count
        ) as outcome
    from {{ ref('fct_pull_requests') }}
    where repository_id = 20004
),
expected(pull_request_key, outcome) as (
    values
        (
            '20004:101',
            '{
              "state": "merged", "is_draft": false, "lifecycle_seconds": 14400,
              "seconds_to_first_review": 7200, "eligible_review_count": 1,
              "review_rework_cycle_count": 0
            }'::jsonb
        ),
        (
            '20004:102',
            '{
              "state": "merged", "is_draft": false, "lifecycle_seconds": 97200,
              "seconds_to_first_review": 32400, "eligible_review_count": 2,
              "review_rework_cycle_count": 1
            }'::jsonb
        ),
        (
            '20004:103',
            '{
              "state": "merged", "is_draft": false, "lifecycle_seconds": 259200,
              "seconds_to_first_review": 86400, "eligible_review_count": 3,
              "review_rework_cycle_count": 2
            }'::jsonb
        ),
        (
            '20004:104',
            '{
              "state": "merged", "is_draft": false, "lifecycle_seconds": 302400,
              "seconds_to_first_review": 172800, "eligible_review_count": 1,
              "review_rework_cycle_count": 0
            }'::jsonb
        ),
        (
            '20004:105',
            '{
              "state": "open", "is_draft": false, "lifecycle_seconds": null,
              "seconds_to_first_review": 86400, "eligible_review_count": 1,
              "review_rework_cycle_count": 1
            }'::jsonb
        ),
        (
            '20004:106',
            '{
              "state": "open", "is_draft": false, "lifecycle_seconds": null,
              "seconds_to_first_review": null, "eligible_review_count": 0,
              "review_rework_cycle_count": 0
            }'::jsonb
        ),
        (
            '20004:107',
            '{
              "state": "open", "is_draft": true, "lifecycle_seconds": null,
              "seconds_to_first_review": null, "eligible_review_count": 0,
              "review_rework_cycle_count": 0
            }'::jsonb
        )
),
mismatches as (
    select
        coalesce(actual.pull_request_key, expected.pull_request_key) as pull_request_key,
        expected.outcome as expected_outcome,
        actual.outcome as actual_outcome
    from actual
    full outer join expected using (pull_request_key)
    where actual.outcome is distinct from expected.outcome
)
select * from mismatches

{% else %}

select
    cast(null as text) as pull_request_key,
    cast(null as jsonb) as expected_outcome,
    cast(null as jsonb) as actual_outcome
where false

{% endif %}
