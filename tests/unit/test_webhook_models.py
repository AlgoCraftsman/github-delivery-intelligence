"""Tests for versioned GitHub event-envelope models."""

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import pytest
from pydantic import ValidationError

from github_analytics.webhook.models import GitHubEventEnvelope, GitHubEventName

FIXTURES = Path(__file__).parents[1] / "fixtures"


def _payload() -> dict[str, Any]:
    return cast(
        dict[str, Any],
        json.loads((FIXTURES / "pull_request.opened.json").read_text(encoding="utf-8")),
    )


def test_envelope_extracts_routing_fields_and_preserves_payload() -> None:
    payload = _payload()
    received_at = datetime(2026, 7, 23, 16, 30, tzinfo=UTC)

    envelope = GitHubEventEnvelope.from_webhook(
        delivery_id="delivery-example-1",
        event_name=GitHubEventName.PULL_REQUEST,
        received_at=received_at,
        payload=payload,
    )

    assert envelope.schema_version == 1
    assert envelope.action == "opened"
    assert envelope.installation_id == 10001
    assert envelope.repository_id == 20001
    assert envelope.repository_full_name == "example-org/delivery-demo"
    assert envelope.received_at == received_at
    assert envelope.payload == payload
    assert envelope.payload["fixture_extension"] == {"safe_to_ignore": True}


def test_envelope_rejects_missing_common_payload_fields() -> None:
    payload = _payload()
    del payload["installation"]

    with pytest.raises(ValidationError):
        GitHubEventEnvelope.from_webhook(
            delivery_id="delivery-example-1",
            event_name=GitHubEventName.PULL_REQUEST,
            received_at=datetime(2026, 7, 23, tzinfo=UTC),
            payload=payload,
        )


def test_envelope_rejects_naive_receiver_timestamp() -> None:
    with pytest.raises(ValidationError):
        GitHubEventEnvelope.from_webhook(
            delivery_id="delivery-example-1",
            event_name=GitHubEventName.PULL_REQUEST,
            received_at=datetime(2026, 7, 23),
            payload=_payload(),
        )
