with events as (
    select *
    from {{ ref('stg_github__events') }}
    where event_name = 'pull_request'
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
        repository_id::text || ':' || (payload #>> '{pull_request,number}') as pull_request_key,
        {{ github_bigint("payload #>> '{pull_request,id}'") }} as pull_request_id,
        coalesce(
            payload #>> '{pull_request,node_id}',
            case
                when (payload #>> '{pull_request,id}') !~ '^[0-9]+$'
                    then payload #>> '{pull_request,id}'
            end
        ) as pull_request_node_id,
        (payload #>> '{pull_request,number}')::integer as pull_request_number,
        lower(payload #>> '{pull_request,state}') as pull_request_state,
        payload #>> '{pull_request,title}' as title,
        payload #>> '{pull_request,body}' as body,
        coalesce(
            (payload #>> '{pull_request,draft}')::boolean,
            (payload #>> '{pull_request,isDraft}')::boolean,
            false
        ) as is_draft,
        {{ github_bigint("payload #>> '{pull_request,user,id}'") }} as author_id,
        coalesce(
            payload #>> '{pull_request,user,node_id}',
            case
                when (payload #>> '{pull_request,author,id}') !~ '^[0-9]+$'
                    then payload #>> '{pull_request,author,id}'
            end
        ) as author_node_id,
        coalesce(
            payload #>> '{pull_request,user,login}',
            payload #>> '{pull_request,author,login}'
        ) as author_login,
        coalesce(
            (payload #>> '{pull_request,created_at}')::timestamptz,
            (payload #>> '{pull_request,createdAt}')::timestamptz,
            occurred_at
        ) as created_at,
        coalesce(
            (payload #>> '{pull_request,updated_at}')::timestamptz,
            (payload #>> '{pull_request,updatedAt}')::timestamptz
        ) as updated_at,
        coalesce(
            (payload #>> '{pull_request,closed_at}')::timestamptz,
            (payload #>> '{pull_request,closedAt}')::timestamptz
        ) as closed_at,
        coalesce(
            (payload #>> '{pull_request,merged_at}')::timestamptz,
            (payload #>> '{pull_request,mergedAt}')::timestamptz
        ) as merged_at,
        coalesce(
            payload #>> '{pull_request,base,ref}',
            payload #>> '{pull_request,baseRefName}'
        ) as base_ref_name,
        coalesce(
            payload #>> '{pull_request,head,ref}',
            payload #>> '{pull_request,headRefName}'
        ) as head_ref_name,
        coalesce(
            payload #>> '{pull_request,merge_commit_sha}',
            payload #>> '{pull_request,mergeCommit,oid}'
        ) as merge_commit_sha,
        (payload #>> '{pull_request,additions}')::integer as additions,
        (payload #>> '{pull_request,deletions}')::integer as deletions,
        coalesce(
            (payload #>> '{pull_request,changed_files}')::integer,
            (payload #>> '{pull_request,changedFiles}')::integer
        ) as changed_files,
        occurred_at,
        received_at,
        ingested_at,
        kafka_partition,
        kafka_offset,
        payload as raw_payload
    from events
)
select * from typed
