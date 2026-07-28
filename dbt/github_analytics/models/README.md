# Models

## Staging

All Day 8 staging models are views in `<target_schema>_staging` and have enforced
column contracts.

| Model | Grain | Cross-source identity |
|---|---|---|
| `stg_github__pull_requests` | one raw `event_id` snapshot | `repository_id:pull_request_number` |
| `stg_github__reviews` | one raw `event_id` review state | repository plus review database/node ID |
| `stg_github__pull_request_commits` | one raw PR/commit association | repository plus GraphQL association node |
| `stg_github__workflow_runs` | one raw run-attempt snapshot | repository, run ID, and attempt |
| `stg_github__deployments` | one raw deployment record | repository plus deployment ID |
| `stg_github__deployment_statuses` | one raw deployment status | repository plus deployment-status ID |

These models type and rename JSON fields; they do not choose a latest snapshot or
collapse webhook/backfill duplicates. Stateful resolution belongs in Day 9
intermediate models.

The ephemeral `stg_github__events` model centralizes raw lineage and source selection.
Production reads `raw.github_events`; tests may override the identifier without
changing model SQL.
