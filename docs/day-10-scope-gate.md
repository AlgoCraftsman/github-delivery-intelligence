# Day 10 analytics scope gate

Decision date: 2026-07-31

Decision: **pass the analytics vertical-slice gate; keep Day 11 work separate.**

The core source-to-mart path is executable in SQL with repository evidence
configuration, explicit measurement status, coverage, definition version, and
machine-readable exclusion reasons. Day 11 dashboard work may begin only after these
Day 10 changes are reviewed and merged; it is not part of this branch.

## Evidence at the gate

- `repository_metric_config` selects one primary signal per repository.
- `dim_repository` retains observed repositories even when configuration is missing.
- `dim_date` supplies contiguous empty dates within observed event bounds.
- `fct_pull_requests` preserves PR flow metrics and exact-SHA linkage evidence.
- `fct_deployments` distinguishes measured deployment statuses from explicitly
  configured workflow proxies.
- `fct_delivery_performance_daily` exposes all five delivery metrics at one
  repository/local-date/metric grain.
- The isolated 17-row fixture proves duplicate collapse, configured success,
  configured proxy evidence, missing configuration, one unmatched change, 50%
  linkage coverage, empty dates, and non-negative temporal outcomes.
- Fixture-backed source freshness and the full dbt build completed with 296 passes,
  zero warnings, zero errors, and zero skips.

## Metric decisions

| Metric | Day 10 decision | Coverage interpretation |
|---|---|---|
| Deployment frequency | measured for configured deployment statuses; `configured_proxy` for an exact named production workflow | classified primary evidence / candidate production evidence |
| Change lead time | measured only for exact-SHA links to successful configured production evidence | linked eligible merged changes / eligible merged changes |
| Failed deployment recovery time | unavailable | intervention and recovery evidence is not configured or modeled |
| Change failure rate | unavailable | rollback, hotfix, incident, or equivalent immediate-intervention evidence is not defensible yet |
| Deployment rework rate | unavailable | unplanned-rework evidence is not configured or modeled |

CI workflow failures are not production failures. Unmatched commits remain unmatched,
and no Git ancestry is inferred. Results are repository/service aggregates and are not
individual-performance scores.

## Scope consequence

The Day 10 core path is green, so no immediate MVP feature cut is required. If this
path regresses before release, defer Slack delivery, automated webhook reconciliation,
and the third dashboard before reducing model contracts, evidence classification,
coverage tests, or documentation.
