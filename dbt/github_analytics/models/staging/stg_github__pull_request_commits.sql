with events as (
    select *
    from {{ ref('stg_github__events') }}
    where event_name = 'pull_request_commit'
),
typed as (
    select
        event_id,
        source,
        source_record_key,
        action,
        repository_id,
        repository_full_name,
        installation_id,
        repository_id::text || ':' || (payload #>> '{pull_request_commit,id}')
            as pull_request_commit_key,
        payload #>> '{pull_request_commit,id}' as association_node_id,
        payload #>> '{pull_request_id}' as pull_request_node_id,
        payload #>> '{pull_request_commit,commit,id}' as commit_node_id,
        payload #>> '{pull_request_commit,commit,oid}' as commit_sha,
        payload #>> '{pull_request_commit,commit,message}' as commit_message,
        (payload #>> '{pull_request_commit,commit,authoredDate}')::timestamptz as authored_at,
        coalesce(
            (payload #>> '{pull_request_commit,commit,committedDate}')::timestamptz,
            occurred_at
        ) as committed_at,
        payload #>> '{pull_request_commit,commit,author,name}' as author_name,
        payload #>> '{pull_request_commit,commit,author,email}' as author_email,
        payload #>> '{pull_request_commit,commit,author,user,id}' as author_node_id,
        payload #>> '{pull_request_commit,commit,author,user,login}' as author_login,
        (payload #>> '{pull_request_commit,commit,additions}')::integer as additions,
        (payload #>> '{pull_request_commit,commit,deletions}')::integer as deletions,
        (payload #>> '{pull_request_commit,commit,changedFilesIfAvailable}')::integer
            as changed_files,
        occurred_at,
        ingested_at,
        payload as raw_payload
    from events
)
select * from typed
