"""Tests for analytics refresh configuration and typed state contracts."""

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError

from github_analytics.analytics_refresh.config import AnalyticsRefreshSettings
from github_analytics.analytics_refresh.models import (
    DbtResultSummary,
    RefreshError,
    RefreshRunIdentity,
    RunStatus,
    SourceWatermark,
    validate_transition,
)

NOW = datetime(2026, 8, 2, 12, tzinfo=UTC)


def test_settings_defaults_and_fixture_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ANALYTICS_REFRESH_SOURCE_IDENTIFIER", "github_events_fixture")

    settings = AnalyticsRefreshSettings()

    assert settings.source_relation == "raw.github_events_fixture"
    assert settings.database_url.get_secret_value().startswith("postgresql://")
    assert settings.dbt_project_dir == Path("dbt/github_analytics")


@pytest.mark.parametrize(
    ("environment", "message"),
    [
        ({"ANALYTICS_REFRESH_SOURCE_SCHEMA": "raw;drop"}, "source_schema"),
        ({"ANALYTICS_REFRESH_SOURCE_IDENTIFIER": "Raw.Events"}, "source_identifier"),
        ({"ANALYTICS_REFRESH_DBT_PROJECT_DIR": "."}, "explicit directory"),
    ],
)
def test_settings_reject_unsafe_values(
    monkeypatch: pytest.MonkeyPatch,
    environment: dict[str, str],
    message: str,
) -> None:
    for key, value in environment.items():
        monkeypatch.setenv(key, value)

    with pytest.raises(ValidationError, match=message):
        AnalyticsRefreshSettings()


def _identity() -> RefreshRunIdentity:
    return RefreshRunIdentity(
        "analytics_refresh",
        "scheduled__2026-08-02T12:00:00+00:00",
        NOW,
        NOW,
        NOW + timedelta(hours=1),
    )


def test_refresh_identity_is_stable_and_validated() -> None:
    identity = _identity()

    assert identity.run_id == _identity().run_id
    assert identity.run_id.startswith("analytics-refresh-")

    with pytest.raises(ValueError, match="nonempty"):
        RefreshRunIdentity("", "run", NOW, NOW, NOW + timedelta(hours=1))
    with pytest.raises(ValueError, match="timezone-aware"):
        RefreshRunIdentity("dag", "run", NOW.replace(tzinfo=None), NOW, NOW + timedelta(hours=1))
    assert RefreshRunIdentity("dag", "run", NOW, NOW, NOW).data_interval_end == NOW
    with pytest.raises(ValueError, match="cannot follow"):
        RefreshRunIdentity("dag", "run", NOW, NOW, NOW - timedelta(seconds=1))


def test_state_transitions_allow_retries_and_reject_success_regression() -> None:
    for current, target in [
        (RunStatus.RUNNING, RunStatus.RUNNING),
        (RunStatus.RUNNING, RunStatus.SUCCEEDED),
        (RunStatus.RUNNING, RunStatus.FAILED),
        (RunStatus.FAILED, RunStatus.RUNNING),
        (RunStatus.FAILED, RunStatus.FAILED),
        (RunStatus.SUCCEEDED, RunStatus.SUCCEEDED),
    ]:
        validate_transition(current, target)

    with pytest.raises(ValueError, match="invalid analytics refresh transition"):
        validate_transition(RunStatus.SUCCEEDED, RunStatus.RUNNING)


def test_watermark_summary_and_error_validation() -> None:
    assert SourceWatermark(NOW, 0).delay_seconds == 0
    with pytest.raises(ValueError, match="cannot be negative"):
        SourceWatermark(NOW, -1)
    with pytest.raises(ValueError, match="cannot be negative"):
        DbtResultSummary(None, failed=-1)

    summary = DbtResultSummary("invocation", succeeded=1, artifact={"safe": True})
    error = RefreshError("dbt_build_failed", "failed", summary=summary)
    assert error.category == "dbt_build_failed"
    assert error.summary is summary
