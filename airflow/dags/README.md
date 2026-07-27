# DAGs

`github_backfill.py` defines the Day 7 manual backfill DAG. It declares only scheduling,
validated trigger parameters, retries, and the call into
`github_analytics.backfill.cli`; API traversal, identities, transactions, and
checkpoints remain in the application package.

The half-open window parameters are required and have no defaults, so a manual trigger
cannot silently run an arbitrary history range. `max_active_runs=1` also prevents two
operators from unintentionally running overlapping windows concurrently.
