# Dashboard SQL contracts

Every native query targets contracted `analytics_marts` relations, declares its grain
and visualization intent in its header, and has a deterministic fixture contract in
`query_contracts.json`. Run `make dashboard-sql-check` after the fixture-backed dbt
build. A snapshot change must be reviewed and the new hash checked in intentionally;
do not update hashes merely to silence a failure.

| Query | Result grain | Intended visualization |
|---|---|---|
| `delivery_performance_deployment_frequency.sql` | repository and repository-local calendar date | line chart split by repository |
| `delivery_performance_change_lead_time.sql` | repository, repository-local merge date, and percentile | line chart split by `series_name`; `lead_time_hours` on Y |
| `delivery_performance_metric_status.sql` | repository and metric on the latest fixture date | evidence table showing status, coverage, and exclusion reason |
| `pr_flow_review_latency_distribution.sql` | repository and fixed latency bucket | ordered bar chart |
| `pr_flow_review_latency_trend.sql` | repository and repository-local PR creation date | P50/P90 time-series line chart with coverage in tooltips |
| `pr_flow_cycle_time.sql` | repository, repository-local merge date, and size band | grouped P50/P90 cycle-time chart with merged-PR count |
| `pr_flow_review_rework.sql` | repository and rework-cycle count | ordered distribution bar chart |
| `pr_flow_open_aging.sql` | open pull request at a fixed `as_of` | detail table with draft, age, bucket, review state, and repository WIP |

## Definitions encoded by the queries

- Change lead-time P50/P90 is calculated from individual linked PR facts. It is not a
  percentile of a daily average.
- Review latency excludes drafts and PRs without an eligible non-author review from
  the latency distribution; the trend retains non-draft PRs in its coverage
  denominator.
- Size is additions plus deletions: `XS <=50`, `S 51-200`, `M 201-500`, `L >500`.
- One rework cycle is one distinct reviewed revision whose resolved eligible
  non-author review state is `changes_requested`. When commit SHA is absent, the
  resolved review identity is the fallback distinct key.
- Open aging uses the fixed `2026-01-14T12:00:00Z` anchor for reproducible evidence.
  Change only the `params` CTE when deliberately creating a live, current-time view.
- `unavailable` values remain null. The status query never converts missing evidence
  into a measured zero.

The delivery dashboard uses three full-width cards stacked vertically. The PR-flow
dashboard uses review distribution and trend side by side, cycle time and rework side
by side, then a full-width aging/WIP table. Native-query cards and their layout are
configured manually in Metabase OSS; the SQL files are the versioned analytics
contract, not a claim of application-database serialization.
