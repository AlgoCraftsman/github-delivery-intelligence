# Singular data tests

Schema YAML owns column contracts, unique/not-null checks, and accepted-value checks.
This directory owns cross-model and fixture-specific invariants:

- reviews cannot precede the matched pull request's creation;
- deterministic fixture resource counts must remain exact;
- webhook and backfill variants must normalize to the same entity keys;
- backfill reviews and commit associations must retain their parent PR node ID;
- resolved intermediate rows must match manually calculated lifecycle, review,
  deployment, and lead-time outcomes;
- the fixture date spine must retain empty dates at exact deterministic bounds;
- configured deployment signals, configured workflow proxies, missing repository
  configuration, unmatched changes, and partial linkage coverage must produce exact
  mart outcomes;
- dashboard fixture outcomes must preserve exact PR lifecycle, first-review latency,
  eligible-review counts, and review-rework cycle counts for the seven PR-flow cases;
- rework cycles count distinct resolved `changes_requested` revisions, using the
  resolved review identity only when commit SHA is absent;
- dashboard SQL contracts must retain deterministic order, unique declared grains,
  required status values, expected row counts, and reviewed fixture snapshots;
- resolved timestamps and durations cannot run backward.
- pipeline-health run identities remain unique, statuses stay accepted, terminal
  durations are nonnegative, and finish timestamps cannot precede starts.

Fixture-only assertions compile to empty result sets unless
`fixture_validation: true` is passed. This keeps production builds independent of
fixture counts. Dashboard query assertions are run separately by
`make dashboard-sql-check` after the fixture-backed dbt build so a presentation query
cannot silently drift from its checked-in ordered result hash.
