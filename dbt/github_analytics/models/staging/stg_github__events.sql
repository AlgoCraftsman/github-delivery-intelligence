{{ config(materialized='ephemeral') }}

select
    event_id,
    source,
    source_record_key,
    delivery_id,
    event_name,
    action,
    repository_id,
    installation_id,
    occurred_at,
    received_at,
    ingested_at,
    kafka_partition,
    kafka_offset,
    payload,
    payload #>> '{repository,full_name}' as repository_full_name
from {{ source('github_raw', 'github_events') }}
