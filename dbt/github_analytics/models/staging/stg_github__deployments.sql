with events as (
    select *
    from {{ ref('stg_github__events') }}
    where event_name = 'deployment'
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
        repository_id::text || ':' || (payload #>> '{deployment,id}') as deployment_key,
        (payload #>> '{deployment,id}')::bigint as deployment_id,
        payload #>> '{deployment,environment}' as environment,
        payload #>> '{deployment,sha}' as deployment_sha,
        payload #>> '{deployment,ref}' as git_ref,
        payload #>> '{deployment,task}' as deployment_task,
        coalesce((payload #>> '{deployment,transient_environment}')::boolean, false)
            as is_transient_environment,
        coalesce((payload #>> '{deployment,production_environment}')::boolean, false)
            as is_production_environment,
        coalesce(
            (payload #>> '{deployment,created_at}')::timestamptz,
            occurred_at
        ) as created_at,
        (payload #>> '{deployment,updated_at}')::timestamptz as updated_at,
        occurred_at,
        received_at,
        ingested_at,
        kafka_partition,
        kafka_offset,
        payload as raw_payload
    from events
)
select * from typed
