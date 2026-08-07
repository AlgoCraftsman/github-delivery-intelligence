# DAGs

`github_backfill.py` defines the manual backfill DAG. It declares scheduling, validated
trigger parameters, retries, and a call into `github_analytics.backfill.cli`; API
traversal, identities, transactions, and checkpoints remain in the package.

The half-open window parameters are required and have no defaults, so a manual trigger
cannot silently run an arbitrary history range. `max_active_runs=1` also prevents two
operators from unintentionally running overlapping windows concurrently.

`analytics_refresh.py` defines the hourly UTC refresh DAG. Its three ordered tasks call
`github_analytics.analytics_refresh.service` for durable start/source freshness, dbt
build, and terminal success. A DAG failure callback delegates terminal failure to the
same package. The DAG contains no PostgreSQL SQL, subprocess construction, artifact
parsing, Kafka polling, or per-event orchestration.
