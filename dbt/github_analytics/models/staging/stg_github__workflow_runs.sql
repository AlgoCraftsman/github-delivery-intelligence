with events as (
    select *
    from {{ ref('stg_github__events') }}
    where event_name = 'workflow_run'
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
        repository_id::text || ':' || (payload #>> '{workflow_run,id}') || ':' || coalesce(
            payload #>> '{workflow_run,run_attempt}', '1'
        ) as workflow_run_key,
        (payload #>> '{workflow_run,id}')::bigint as workflow_run_id,
        coalesce((payload #>> '{workflow_run,run_attempt}')::integer, 1) as run_attempt,
        (payload #>> '{workflow_run,workflow_id}')::bigint as workflow_id,
        payload #>> '{workflow_run,name}' as workflow_name,
        payload #>> '{workflow_run,event}' as trigger_event,
        lower(payload #>> '{workflow_run,status}') as workflow_status,
        lower(payload #>> '{workflow_run,conclusion}') as conclusion,
        payload #>> '{workflow_run,head_branch}' as head_branch,
        payload #>> '{workflow_run,head_sha}' as head_sha,
        coalesce(
            (payload #>> '{workflow_run,created_at}')::timestamptz,
            occurred_at
        ) as created_at,
        (payload #>> '{workflow_run,updated_at}')::timestamptz as updated_at,
        (payload #>> '{workflow_run,run_started_at}')::timestamptz as run_started_at,
        occurred_at,
        received_at,
        ingested_at,
        kafka_partition,
        kafka_offset,
        payload as raw_payload
    from events
)
select * from typed
