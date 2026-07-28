with events as (
    select *
    from {{ ref('stg_github__events') }}
    where event_name = 'deployment_status'
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
        repository_id::text || ':' || (payload #>> '{deployment_status,id}')
            as deployment_status_key,
        (payload #>> '{deployment_status,id}')::bigint as deployment_status_id,
        coalesce(
            (payload #>> '{deployment,id}')::bigint,
            (payload #>> '{deployment_id}')::bigint
        ) as deployment_id,
        lower(payload #>> '{deployment_status,state}') as deployment_state,
        payload #>> '{deployment_status,environment}' as environment,
        payload #>> '{deployment_status,description}' as description,
        payload #>> '{deployment_status,environment_url}' as environment_url,
        payload #>> '{deployment_status,log_url}' as log_url,
        {{ github_bigint("payload #>> '{deployment_status,creator,id}'") }} as creator_id,
        payload #>> '{deployment_status,creator,login}' as creator_login,
        coalesce(
            (payload #>> '{deployment_status,created_at}')::timestamptz,
            occurred_at
        ) as created_at,
        (payload #>> '{deployment_status,updated_at}')::timestamptz as updated_at,
        occurred_at,
        received_at,
        ingested_at,
        kafka_partition,
        kafka_offset,
        payload as raw_payload
    from events
)
select * from typed
