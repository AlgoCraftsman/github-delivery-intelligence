"""Command-line boundary for bounded pull-request history backfills."""

import argparse
import json
from datetime import datetime
from typing import Any

from psycopg_pool import ConnectionPool

from github_analytics.backfill.config import BackfillSettings
from github_analytics.backfill.github import GitHubPullRequestBackfill
from github_analytics.backfill.graphql import GitHubGraphQLClient
from github_analytics.backfill.storage import PostgresBackfillStorage


def main(arguments: list[str] | None = None) -> int:
    """Run one explicit half-open PR creation window."""

    parser = argparse.ArgumentParser(description="Backfill GitHub pull-request history")
    parser.add_argument("--start", required=True, type=_aware_datetime)
    parser.add_argument("--end", required=True, type=_aware_datetime)
    parsed = parser.parse_args(arguments)
    settings = BackfillSettings()  # type: ignore[call-arg]
    client = GitHubGraphQLClient(
        settings.github_token.get_secret_value(),
        url=settings.github_graphql_url,
        request_timeout_seconds=settings.github_request_timeout_seconds,
        max_rate_limit_retries=settings.github_max_rate_limit_retries,
        secondary_backoff_seconds=settings.github_secondary_backoff_seconds,
    )
    try:
        with ConnectionPool[Any](
            settings.database_url.get_secret_value(),
            min_size=1,
            max_size=4,
            timeout=settings.database_pool_timeout_seconds,
            open=True,
        ) as pool:
            pool.wait(timeout=settings.database_pool_timeout_seconds)
            summary = GitHubPullRequestBackfill(
                client,
                PostgresBackfillStorage(pool),
                repository=settings.github_repository,
                repository_id=settings.github_repository_id,
                installation_id=settings.github_installation_id,
                page_size=settings.github_page_size,
            ).run(window_start=parsed.start, window_end=parsed.end)
    finally:
        client.close()
    print(
        json.dumps(
            {
                "event": "github_backfill_completed",
                "window_start": parsed.start.isoformat(),
                "window_end": parsed.end.isoformat(),
                "pages_persisted": summary.pages_persisted,
                "records_inserted": summary.records_inserted,
                "duplicates_absorbed": summary.duplicates_absorbed,
            },
            separators=(",", ":"),
            sort_keys=True,
        )
    )
    return 0


def _aware_datetime(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("timestamp must be ISO 8601") from error
    if parsed.tzinfo is None:
        raise argparse.ArgumentTypeError("timestamp must include a UTC offset")
    return parsed
