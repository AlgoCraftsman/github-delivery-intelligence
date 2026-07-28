# Seeds

Repository metric configuration will be added with the metric layers. Raw event
fixtures are intentionally SQL-loaded from `../fixtures/` instead of dbt seeds because
their JSONB columns require native PostgreSQL types and their `ingested_at` watermark
must be fresh at execution time.
