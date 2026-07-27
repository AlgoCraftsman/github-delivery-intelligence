# Airflow

Day 7 provides a pinned Apache Airflow 3.3.0 image on Python 3.12 and the manually
triggered `github_backfill` DAG. Business logic remains in the tested application
package rather than in the DAG.

Build the image and ask Airflow to report DAG import errors:

```bash
make airflow-image
make airflow-dag-check
```

The image copies the local package source and installs the pinned backfill-only
requirements alongside the exact Airflow version from the base image, then runs
`pip check`. The webhook and Kafka consumer dependencies intentionally stay outside
the orchestration image because Airflow 3.3.0 and the webhook service have different
compatible FastAPI ranges. CI repeats both image checks.

## Runtime contract

An Airflow deployment must inject the `BACKFILL_*` variables documented in
`.env.example`. Use a short-lived GitHub App installation token with repository
contents/metadata, Actions read, and Deployments read permissions. Do not bake tokens
or database credentials into the image.

When Airflow runs in a container while PostgreSQL runs through the repository's Docker
Compose stack, `localhost` refers to the Airflow container. Put both services on a
shared Docker network and use the PostgreSQL service name, or use
`host.docker.internal:55432` for an intentional Docker Desktop host connection.

Trigger `github_backfill` manually from the Airflow UI and provide:

- `window_start`: inclusive, offset-aware ISO 8601 timestamp.
- `window_end`: exclusive, offset-aware ISO 8601 timestamp.

Only one DAG run is active at a time. The task retries three times at one-minute
intervals. Every retry invokes the same application command; the PostgreSQL
`raw.backfill_checkpoints` rows resume GraphQL cursors and REST page numbers after the
last committed API page.

This directory does not yet define a production Airflow topology. Scheduler, API
server, metadata database, executor selection, authentication, and secret management
remain deployment concerns and are intentionally outside the Day 7 image artifact.
