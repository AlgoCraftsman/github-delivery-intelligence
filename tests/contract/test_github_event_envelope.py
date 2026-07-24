"""Compatibility tests for the versioned GitHub event envelope."""

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import pytest
from jsonschema import Draft202012Validator, FormatChecker
from pydantic import ValidationError

from github_analytics.webhook.models import GitHubEventEnvelope, GitHubEventName

ROOT = Path(__file__).parents[2]
FIXTURES = ROOT / "tests" / "fixtures"
SCHEMA_PATH = ROOT / "schemas" / "github-event-envelope-v1.json"
RECEIVED_AT = datetime(2026, 7, 23, 18, 0, tzinfo=UTC)

EVENT_FIXTURES = {
    GitHubEventName.PULL_REQUEST: "pull_request.opened.json",
    GitHubEventName.PULL_REQUEST_REVIEW: "pull_request_review.submitted.json",
    GitHubEventName.WORKFLOW_RUN: "workflow_run.completed.json",
    GitHubEventName.DEPLOYMENT: "deployment.created.json",
    GitHubEventName.DEPLOYMENT_STATUS: "deployment_status.created.json",
}


def _json_object(path: Path) -> dict[str, Any]:
    return cast(dict[str, Any], json.loads(path.read_text(encoding="utf-8")))


def _schema() -> dict[str, Any]:
    return _json_object(SCHEMA_PATH)


def _envelope(event_name: GitHubEventName, fixture_name: str) -> GitHubEventEnvelope:
    return GitHubEventEnvelope.from_webhook(
        delivery_id=f"delivery-{event_name}",
        event_name=event_name,
        received_at=RECEIVED_AT,
        payload=_json_object(FIXTURES / fixture_name),
    )


def test_checked_in_schema_is_valid_draft_2020_12() -> None:
    Draft202012Validator.check_schema(_schema())


@pytest.mark.parametrize(("event_name", "fixture_name"), EVENT_FIXTURES.items())
def test_sanitized_fixture_matches_pydantic_and_json_schema(
    event_name: GitHubEventName,
    fixture_name: str,
) -> None:
    envelope = _envelope(event_name, fixture_name)
    serialized = envelope.model_dump(mode="json")

    Draft202012Validator(
        _schema(),
        format_checker=FormatChecker(),
    ).validate(serialized)
    assert serialized["payload"]["fixture_extension"] == {"safe_to_ignore": True}


@pytest.mark.parametrize(
    "required_field",
    [
        "schema_version",
        "delivery_id",
        "event_name",
        "action",
        "installation_id",
        "repository_id",
        "repository_full_name",
        "received_at",
        "payload",
    ],
)
def test_schema_rejects_missing_required_envelope_fields(required_field: str) -> None:
    serialized = _envelope(
        GitHubEventName.PULL_REQUEST,
        EVENT_FIXTURES[GitHubEventName.PULL_REQUEST],
    ).model_dump(mode="json")
    del serialized[required_field]

    assert list(Draft202012Validator(_schema()).iter_errors(serialized))


def test_schema_and_pydantic_reject_unknown_versions() -> None:
    serialized = _envelope(
        GitHubEventName.PULL_REQUEST,
        EVENT_FIXTURES[GitHubEventName.PULL_REQUEST],
    ).model_dump(mode="json")
    serialized["schema_version"] = 2

    assert list(Draft202012Validator(_schema()).iter_errors(serialized))
    with pytest.raises(ValidationError):
        GitHubEventEnvelope.model_validate(serialized)


def test_schema_and_pydantic_reject_unknown_outer_fields() -> None:
    serialized = _envelope(
        GitHubEventName.PULL_REQUEST,
        EVENT_FIXTURES[GitHubEventName.PULL_REQUEST],
    ).model_dump(mode="json")
    serialized["unexpected"] = True

    assert list(Draft202012Validator(_schema()).iter_errors(serialized))
    with pytest.raises(ValidationError):
        GitHubEventEnvelope.model_validate(serialized)
