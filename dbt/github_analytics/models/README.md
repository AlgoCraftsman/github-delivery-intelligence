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
collapse webhook/backfill duplicates.

The ephemeral `stg_github__events` model centralizes raw lineage and source selection.
Production reads `raw.github_events`; tests may override the identifier without
changing model SQL.

## Intermediate

Day 9 models are contracted views in `<target_schema>_intermediate`.

| Model | Grain | Resolution |
|---|---|---|
| `int_pr_lifecycle` | one repository-scoped pull request | latest state plus first/last snapshot lineage |
| `int_first_eligible_review` | one reviewed pull request | first submitted non-author review |
| `int_production_deployments` | one production deployment | latest status plus first successful status |
| `int_change_to_deployment` | one PR/deployment pair | exact merge or PR-commit SHA linkage |

Intermediate models do not infer commit ancestry. Unmatched changes remain unmatched
so Day 10 coverage calculations can distinguish measured values from evidence gaps.
