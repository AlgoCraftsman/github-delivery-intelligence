# Contributing

Thank you for considering an improvement to GitHub Delivery Intelligence.

## Before changing code

- Review `AGENTS.md`, `BUILD_PLAN.md`, the architecture decisions, and the relevant
  checked-in contracts.
- Open an issue before a large architectural, schema, metric, or infrastructure change.
- Keep changes scoped and use a branch rather than committing directly to `main`.
- Never commit secrets, signatures, real webhook payloads, private keys, environment
  dumps, or production-like credentials.

## Local validation

The project targets Python 3.12 and the locked uv environment. Start with:

```bash
uv sync --frozen
make check
docker compose -f infra/docker-compose.yml --profile dashboards config --quiet
git diff --check
```

Run the focused database, dbt, dashboard, Airflow, or reader-isolation checks described
in `AGENTS.md` when those areas change. Never delete named PostgreSQL or Metabase
volumes to make a check pass.

## Pull requests

Explain the problem, user-visible outcome, architecture or contract decisions, checks
actually run, and known limitations. Keep unavailable metrics null with an explicit
reason, do not turn CI failures into production-failure claims, and do not add
individual-contributor rankings.

By contributing, you agree that your contribution is licensed under the repository's
[MIT License](LICENSE).
