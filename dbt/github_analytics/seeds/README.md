# Seeds

`repository_metric_config.csv` is the checked-in evidence contract at one row per
GitHub repository database identity. Each row selects exactly one primary production
signal:

- `deployment_status` requires pipe-delimited exact production environment names;
- `workflow_run` requires one exact workflow name and is labeled
  `configured_proxy` in metric results.

The seed also records the default branch, IANA reporting timezone, optional incident,
rollback, hotfix, and unplanned-rework conventions, and a metric definition version.
Do not populate both deployment and workflow conventions for one repository. A seed
test enforces that exclusivity.

The `example-org/*` rows exist only to make deterministic fixture behavior executable;
replace or extend the operational repository rows deliberately. Optional instability
labels do not by themselves enable instability metrics—the necessary event
relationships must also be implemented and tested.

Raw event fixtures remain SQL-loaded from `../fixtures/` instead of dbt seeds because
their JSONB columns require native PostgreSQL types and their `ingested_at` watermark
must be fresh at execution time.
