# Singular data tests

Schema YAML owns column contracts, unique/not-null checks, and accepted-value checks.
This directory owns cross-model and fixture-specific invariants:

- reviews cannot precede the matched pull request's creation;
- deterministic fixture resource counts must remain exact;
- webhook and backfill variants must normalize to the same entity keys;
- backfill reviews and commit associations must retain their parent PR node ID.

Fixture-only assertions compile to empty result sets unless
`fixture_validation: true` is passed. This keeps production builds independent of
fixture counts.
