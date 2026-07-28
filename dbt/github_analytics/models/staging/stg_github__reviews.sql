with events as (
    select *
    from {{ ref('stg_github__events') }}
    where event_name = 'pull_request_review'
),
typed as (
    select
        event_id,
        source,
        source_record_key,
        delivery_id,
        action,
        repository_id,
        repository_full_name,
        installation_id,
        repository_id::text || ':' || coalesce(
            payload #>> '{review,id}',
            payload #>> '{pull_request_review,fullDatabaseId}',
            payload #>> '{pull_request_review,id}'
        ) as review_key,
        {{ github_bigint(
            "coalesce(payload #>> '{review,id}', payload #>> '{pull_request_review,fullDatabaseId}')"
        ) }} as review_id,
        coalesce(
            payload #>> '{review,node_id}',
            case
                when (payload #>> '{pull_request_review,id}') !~ '^[0-9]+$'
                    then payload #>> '{pull_request_review,id}'
            end
        ) as review_node_id,
        coalesce(
            payload #>> '{pull_request,node_id}',
            payload #>> '{pull_request_id}'
        ) as pull_request_node_id,
        (payload #>> '{pull_request,number}')::integer as pull_request_number,
        lower(coalesce(
            payload #>> '{review,state}',
            payload #>> '{pull_request_review,state}'
        )) as review_state,
        coalesce(
            {{ github_bigint("payload #>> '{review,user,id}'") }},
            {{ github_bigint("payload #>> '{pull_request_review,author,id}'") }}
        ) as reviewer_id,
        coalesce(
            payload #>> '{review,user,node_id}',
            case
                when (payload #>> '{pull_request_review,author,id}') !~ '^[0-9]+$'
                    then payload #>> '{pull_request_review,author,id}'
            end
        ) as reviewer_node_id,
        coalesce(
            payload #>> '{review,user,login}',
            payload #>> '{pull_request_review,author,login}'
        ) as reviewer_login,
        coalesce(
            payload #>> '{review,body}',
            payload #>> '{pull_request_review,body}'
        ) as body,
        coalesce(
            payload #>> '{review,commit_id}',
            payload #>> '{pull_request_review,commit,oid}'
        ) as commit_sha,
        coalesce(
            (payload #>> '{review,submitted_at}')::timestamptz,
            (payload #>> '{pull_request_review,submittedAt}')::timestamptz,
            occurred_at
        ) as submitted_at,
        coalesce(
            (payload #>> '{review,updated_at}')::timestamptz,
            (payload #>> '{pull_request_review,updatedAt}')::timestamptz
        ) as updated_at,
        occurred_at,
        received_at,
        ingested_at,
        kafka_partition,
        kafka_offset,
        payload as raw_payload
    from events
)
select * from typed
