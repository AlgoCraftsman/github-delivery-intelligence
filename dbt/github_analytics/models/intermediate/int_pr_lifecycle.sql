with snapshots as (
    select
        *,
        row_number() over (
            partition by pull_request_key
            order by
                coalesce(updated_at, merged_at, closed_at, occurred_at, ingested_at) desc,
                case when source = 'backfill' then 1 else 0 end desc,
                ingested_at desc,
                event_id desc
        ) as snapshot_rank
    from {{ ref('stg_github__pull_requests') }}
),
snapshot_rollup as (
    select
        pull_request_key,
        max(pull_request_id) as pull_request_id,
        max(pull_request_node_id) as pull_request_node_id,
        min(created_at) as created_at,
        max(updated_at) as updated_at,
        max(closed_at) as observed_closed_at,
        max(merged_at) as observed_merged_at,
        count(*)::integer as snapshot_count,
        count(distinct source)::integer as source_count,
        min(ingested_at) as first_ingested_at,
        max(ingested_at) as last_ingested_at
    from snapshots
    group by pull_request_key
),
resolved as (
    select
        latest.repository_id,
        latest.repository_full_name,
        latest.installation_id,
        latest.pull_request_key,
        rollup.pull_request_id,
        rollup.pull_request_node_id,
        latest.pull_request_number,
        case
            when coalesce(latest.merged_at, rollup.observed_merged_at) is not null then 'merged'
            else latest.pull_request_state
        end as pull_request_state,
        latest.title,
        latest.is_draft,
        latest.author_id,
        latest.author_node_id,
        latest.author_login,
        rollup.created_at,
        rollup.updated_at,
        case
            when latest.pull_request_state in ('closed', 'merged')
                then coalesce(latest.closed_at, rollup.observed_closed_at)
        end as closed_at,
        coalesce(latest.merged_at, rollup.observed_merged_at) as merged_at,
        latest.base_ref_name,
        latest.head_ref_name,
        latest.merge_commit_sha,
        latest.additions,
        latest.deletions,
        latest.changed_files,
        rollup.snapshot_count,
        rollup.source_count,
        rollup.first_ingested_at,
        rollup.last_ingested_at
    from snapshots as latest
    inner join snapshot_rollup as rollup using (pull_request_key)
    where latest.snapshot_rank = 1
)
select
    repository_id,
    repository_full_name,
    installation_id,
    pull_request_key,
    pull_request_id,
    pull_request_node_id,
    pull_request_number,
    pull_request_state,
    title,
    is_draft,
    author_id,
    author_node_id,
    author_login,
    created_at,
    updated_at,
    closed_at,
    merged_at,
    case
        when pull_request_state = 'merged' then merged_at
        when pull_request_state = 'closed' then closed_at
    end as resolved_at,
    pull_request_state = 'merged' as is_merged,
    pull_request_state in ('closed', 'merged') as is_closed,
    case
        when pull_request_state = 'merged' and merged_at is not null
            then extract(epoch from (merged_at - created_at))::bigint
        when pull_request_state = 'closed' and closed_at is not null
            then extract(epoch from (closed_at - created_at))::bigint
    end as lifecycle_seconds,
    base_ref_name,
    head_ref_name,
    merge_commit_sha,
    additions,
    deletions,
    changed_files,
    snapshot_count,
    source_count,
    first_ingested_at,
    last_ingested_at
from resolved
