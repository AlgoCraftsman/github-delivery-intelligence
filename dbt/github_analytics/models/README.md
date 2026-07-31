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

## Marts

Day 10 models are contracted views in `<target_schema>_marts`.

| Model | Grain | Responsibility |
|---|---|---|
| `dim_repository` | one repository | observed identity plus checked-in evidence conventions |
| `dim_date` | one calendar date | contiguous, observed-event-bounded reporting spine |
| `fct_pull_requests` | one repository-scoped PR | lifecycle, review timing, and first exact-SHA production link |
| `fct_deployments` | one deployment or configured workflow run | classify measured signals, proxies, and exclusions |
| `fct_delivery_performance_daily` | one repository/date/metric | metric value, status, coverage, version, and exclusion reason |

`fct_delivery_performance_daily` is long-form so every metric result has the same
integrity contract. Reporting dates use each repository's configured timezone.
Deployment frequency keeps empty dates as measured zeroes for configured
repositories. Change lead time is averaged by the repository-local merge date and
its coverage is linked eligible changes divided by eligible merged changes.

The allowed statuses are `measured`, `configured_proxy`, and `unavailable`.
Unavailable rows have a null metric value and a machine-readable exclusion reason.
No mart infers Git ancestry or turns CI failures into production failures.
