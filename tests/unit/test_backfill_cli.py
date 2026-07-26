"""Tests for the explicit backfill command boundary."""

import argparse
import json
from contextlib import AbstractContextManager
from datetime import UTC, datetime
from types import TracebackType
from typing import ClassVar

import pytest
from pydantic import SecretStr

from github_analytics.backfill import cli
from github_analytics.backfill.config import BackfillSettings
from github_analytics.backfill.github import BackfillSummary


class FakeSettings:
    database_url = SecretStr("postgresql://example")
    database_pool_timeout_seconds = 5.0
    github_token = SecretStr("token")
    github_repository = "example/repo"
    github_repository_id = 10
    github_installation_id = 20
    github_graphql_url = "https://example.test/graphql"
    github_page_size = 25
    github_request_timeout_seconds = 12.0
    github_max_rate_limit_retries = 2
    github_secondary_backoff_seconds = 60.0


class FakePool(AbstractContextManager["FakePool"]):
    created: ClassVar[list["FakePool"]] = []

    def __init__(self, *args: object, **kwargs: object) -> None:
        self.args = args
        self.kwargs = kwargs
        self.waited: float | None = None
        self.__class__.created.append(self)

    def __enter__(self) -> "FakePool":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exc_type, exc_value, traceback

    def wait(self, *, timeout: float) -> None:
        self.waited = timeout


class FakeClient:
    created: ClassVar[list["FakeClient"]] = []

    def __init__(self, token: str, **kwargs: object) -> None:
        self.token = token
        self.kwargs = kwargs
        self.closed = False
        self.__class__.created.append(self)

    def close(self) -> None:
        self.closed = True


class FakeStorage:
    def __init__(self, pool: FakePool) -> None:
        self.pool = pool


class FakeBackfill:
    created: ClassVar[list["FakeBackfill"]] = []

    def __init__(
        self,
        client: FakeClient,
        storage: FakeStorage,
        **kwargs: object,
    ) -> None:
        self.client = client
        self.storage = storage
        self.kwargs = kwargs
        self.window: tuple[datetime, datetime] | None = None
        self.__class__.created.append(self)

    def run(
        self,
        *,
        window_start: datetime,
        window_end: datetime,
    ) -> BackfillSummary:
        self.window = (window_start, window_end)
        return BackfillSummary(3, 4, 1)


def test_main_wires_settings_runs_window_and_emits_structured_summary(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    FakePool.created.clear()
    FakeClient.created.clear()
    FakeBackfill.created.clear()
    monkeypatch.setattr(cli, "BackfillSettings", FakeSettings)
    monkeypatch.setattr(cli, "ConnectionPool", FakePool)
    monkeypatch.setattr(cli, "GitHubGraphQLClient", FakeClient)
    monkeypatch.setattr(cli, "PostgresBackfillStorage", FakeStorage)
    monkeypatch.setattr(cli, "GitHubPullRequestBackfill", FakeBackfill)

    assert (
        cli.main(
            [
                "--start",
                "2026-01-01T00:00:00Z",
                "--end",
                "2026-02-01T00:00:00+00:00",
            ]
        )
        == 0
    )

    assert FakePool.created[0].args == ("postgresql://example",)
    assert FakePool.created[0].waited == 5.0
    assert FakeClient.created[0].token == "token"
    assert FakeClient.created[0].closed
    assert FakeBackfill.created[0].kwargs == {
        "repository": "example/repo",
        "repository_id": 10,
        "installation_id": 20,
        "page_size": 25,
    }
    assert FakeBackfill.created[0].window == (
        datetime(2026, 1, 1, tzinfo=UTC),
        datetime(2026, 2, 1, tzinfo=UTC),
    )
    output = json.loads(capsys.readouterr().out)
    assert output["event"] == "github_backfill_completed"
    assert output["records_inserted"] == 4
    assert output["duplicates_absorbed"] == 1


@pytest.mark.parametrize(
    ("value", "message"),
    [
        ("not-a-timestamp", "ISO 8601"),
        ("2026-01-01T00:00:00", "UTC offset"),
    ],
)
def test_cli_timestamp_requires_valid_aware_iso8601(value: str, message: str) -> None:
    with pytest.raises(argparse.ArgumentTypeError, match=message):
        cli._aware_datetime(value)


def test_settings_load_required_source_identities_from_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("BACKFILL_GITHUB_TOKEN", "secret")
    monkeypatch.setenv("BACKFILL_GITHUB_REPOSITORY", "example/repo")
    monkeypatch.setenv("BACKFILL_GITHUB_REPOSITORY_ID", "10")
    monkeypatch.setenv("BACKFILL_GITHUB_INSTALLATION_ID", "20")

    settings = BackfillSettings()  # type: ignore[call-arg]

    assert settings.github_token.get_secret_value() == "secret"
    assert settings.github_page_size == 50
