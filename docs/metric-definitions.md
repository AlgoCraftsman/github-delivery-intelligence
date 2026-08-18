# Metric definitions

These definitions describe the contracted dbt models at definition version `v1.0.0`.
The source of truth is the checked-in repository configuration, model SQL, contracts,
and fixture assertions. Values are repository/service aggregates, never contributor
rankings or individual-performance scores.

## Status and coverage contract

Every daily delivery-performance row has one status:

- `measured`: direct configured deployment-status evidence supports the result.
- `configured_proxy`: an exact configured production workflow supplies proxy evidence.
- `unavailable`: the required configuration or evidence is absent. `metric_value`
  remains null and `exclusion_reason` explains why.

Coverage is evidence completeness, not metric quality. `coverage_ratio` is
`coverage_numerator / coverage_denominator` when the denominator is positive; an empty
denominator normally yields null. Numerators never exceed denominators. Coverage and
status must accompany a value when comparing repositories or time periods.

## Delivery performance

### Deployment frequency

- **Business question:** How many configured successful production releases occurred
  on each repository-local calendar day?
- **Grain:** One repository, local date, and `deployment_frequency` metric row.
- **Source evidence:** `fct_deployments`, using successful deployment-status records
  for configured production environment names, or successful runs of one exact
  configured workflow.
- **Value:** Count of rows whose status is `measured` or `configured_proxy`. A configured
  day with no successful evidence is a measured zero.
- **Coverage:** Numerator = candidate deployment/workflow evidence matching the
  repository's configured primary signal. Denominator = all candidate
  deployment/workflow evidence observed that day.
- **Inclusions:** Only the sole primary signal selected in
  `repository_metric_config`; the event is dated at `successful_at`, falling back to
  `created_at`, in the configured repository timezone.
- **Exclusions:** Non-primary environments or workflow names and unsuccessful evidence
  do not contribute to the value. Missing repository configuration makes the value
  unavailable with `missing_repository_configuration`.
- **Status and caution:** Deployment-status evidence is `measured`; named workflow
  evidence is `configured_proxy`. A successful CI workflow is production evidence only
  when explicitly configured as that repository's proxy. It is not interchangeable
  with direct deployment evidence.

### Change lead time

- **Business question:** How long did an eligible merged change take to reach the first
  successful configured production signal?
- **Grain:** The daily mart stores one repository/local merge date average; dashboard
  percentiles are calculated from the linked PR rows for that same grain.
- **Source evidence:** Merged PRs in `fct_pull_requests`, linked by exact commit SHA to
  successful primary evidence in `fct_deployments`.
- **Value:** Mean seconds from `merged_at` to the first linked `successful_at` in the
  daily mart. Dashboard queries publish p50 and p90 from the underlying linked rows.
- **Coverage:** Numerator = eligible merged PRs with a successful exact-SHA link.
  Denominator = eligible merged PRs. The ratio is linked / eligible.
- **Inclusions:** A temporally valid merged PR with a merge-commit match, or a directly
  observed PR-commit match when the merge commit does not match. Only non-negative
  time to the first production success is retained.
- **Exclusions:** Open, closed-unmerged, or temporally invalid PRs are ineligible.
  Eligible but unmatched PRs stay in the denominator. No eligible changes yields
  `no_eligible_changes`; no linked changes yields
  `missing_change_deployment_linkage`.
- **Status and caution:** Status follows the repository's production signal:
  `measured` for deployment status and `configured_proxy` for workflow evidence. The
  model does not infer Git ancestry; low coverage can materially bias the linked
  distribution.

### Failed deployment recovery time

- **Business question:** After a failed production deployment requiring intervention,
  how long until production recovered?
- **Grain:** One repository, local date, and metric row.
- **Source evidence:** Required failure, intervention, and recovery linkage is not
  configured or modeled.
- **Value and coverage:** Value is null. Coverage numerator is zero; denominator is the
  day's successful configured deployment count, and the ratio is zero only when that
  denominator is positive.
- **Status:** Always `unavailable` with
  `required_intervention_recovery_evidence_not_configured`.
- **Caution:** A failed workflow or deployment-status snapshot alone does not prove a
  production incident, human intervention, or recovery sequence.

### Change failure rate

- **Business question:** What share of production changes caused a production failure
  requiring remediation?
- **Grain:** One repository, local date, and metric row.
- **Source evidence:** Rollback, hotfix, incident, or equivalent immediate-intervention
  evidence is not yet defensibly linked to changes.
- **Value and coverage:** Value is null. Coverage numerator is zero; denominator is the
  day's successful configured deployment count, and the ratio is zero only when that
  denominator is positive.
- **Status:** Always `unavailable` with
  `required_change_failure_evidence_not_configured`.
- **Caution:** CI failures are not production failures. Do not substitute a workflow
  failure rate for change failure rate.

### Deployment rework rate

- **Business question:** What share of production deployments required unplanned
  deployment rework?
- **Grain:** One repository, local date, and metric row.
- **Source evidence:** An unplanned-rework convention is not configured or modeled.
- **Value and coverage:** Value is null. Coverage numerator is zero; denominator is the
  day's successful configured deployment count, and the ratio is zero only when that
  denominator is positive.
- **Status:** Always `unavailable` with
  `required_unplanned_rework_evidence_not_configured`.
- **Caution:** Multiple deployments, later commits, or repeated workflow attempts do
  not by themselves prove rework.

## Pull-request flow

PR-flow fields are measured from resolved GitHub PR and review evidence in
`fct_pull_requests`. They do not use the daily delivery metric status column, but the
same evidence and coverage cautions apply.

### PR cycle time

- **Business question:** How long do merged PRs take from creation to merge?
- **Grain:** One merged PR; the dashboard groups by repository, repository-local merge
  date, and size band.
- **Value:** `merged_at - created_at` in seconds. The dashboard reports p50 and p90.
- **Inclusions/exclusions:** Only merged PRs with a non-null, non-negative lifecycle.
  Size uses additions + deletions: XS <= 50, S 51–200, M 201–500, L > 500, or Unknown.
- **Coverage/caution:** The dashboard includes `merged_pr_count`; no independent
  coverage ratio is modeled. Compare like repositories and size bands, not unrelated
  services as if their work were interchangeable.

### Time to first eligible review

- **Business question:** How long do non-draft PRs wait for a review from someone other
  than the author?
- **Grain:** One PR; trends group by repository and repository-local creation date.
- **Value:** Seconds from PR creation to the earliest submitted non-author review;
  dashboard trends show p50 and p90 and the distribution uses fixed hour buckets.
- **Coverage:** Numerator = non-draft PRs with an eligible review. Denominator = all
  non-draft PRs, including unresolved PRs. Ratio = reviewed / eligible.
- **Exclusions/caution:** Drafts are excluded. Self-reviews are ineligible. A null value
  means no eligible review was observed, not zero latency.

### Review rework cycles

- **Business question:** How many reviewed revisions ended in a changes-requested state
  before resolution?
- **Grain:** One PR, displayed as a repository distribution by cycle count.
- **Value:** Count of distinct reviewed commit SHAs whose latest resolved review state
  is `CHANGES_REQUESTED`; when a review lacks a commit SHA, review identity is the
  fallback.
- **Inclusions/exclusions:** The dashboard includes non-draft PRs with at least one
  eligible review. It does not infer that a cycle caused deployment rework.
- **Coverage/caution:** `pull_request_count` and repository share describe the visible
  reviewed population; no separate coverage ratio is modeled.

### Open PR aging and work in progress

- **Business question:** How old are currently open PRs, and how many are open per
  repository at the selected observation time?
- **Grain:** One open PR at an explicit `as_of` timestamp.
- **Value:** Age hours = `as_of - created_at`; repository WIP is the count of open PR
  rows. Buckets are under 1 day, 1–3 days, 3–7 days, and 7+ days.
- **Inclusions/exclusions:** Only PRs resolved as open and created no later than
  `as_of`. Draft and review state are dimensions, not silent exclusions.
- **Coverage/caution:** The checked-in screenshot query uses a fixed synthetic
  `2026-01-14 12:00 UTC` anchor for reproducibility. Change the anchor explicitly for a
  live view; never present the fixture anchor as current operational state.

## Interpretation rules

- Keep unavailable values null and show the exclusion reason; never coerce them to
  zero.
- A coverage ratio describes modeled linkage/classification, not organizational
  performance.
- Do not infer incidents, rollback, intervention, deployment rework, or DLQ activity
  without the corresponding modeled evidence.
- Do not compare unrelated repositories or services without accounting for different
  production signals, coverage, timezones, work types, and operating contexts.
- Do not use these models for contributor ranking or individual-performance reporting.

See the [Day 10 scope gate](day-10-scope-gate.md), dbt
[model contracts](../dbt/github_analytics/models/README.md), and
[dashboard query definitions](../dashboards/README.md) for executable evidence.
