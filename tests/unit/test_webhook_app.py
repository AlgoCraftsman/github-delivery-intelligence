"""HTTP-boundary tests for the GitHub webhook receiver."""

import hashlib
import hmac
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr

from github_analytics.webhook.app import create_app, create_runtime_app, utc_now
from github_analytics.webhook.config import WebhookSettings
from github_analytics.webhook.models import GitHubEventEnvelope
from github_analytics.webhook.publishing import PublishError

FIXTURE = Path(__file__).parents[1] / "fixtures" / "pull_request.opened.json"
SECRET = "unit-test-secret"
RECEIVED_AT = datetime(2026, 7, 23, 17, 0, tzinfo=UTC)


class RecordingPublisher:
    def __init__(self) -> None:
        self.envelopes: list[GitHubEventEnvelope] = []

    async def publish(self, envelope: GitHubEventEnvelope) -> None:
        self.envelopes.append(envelope)

    async def is_ready(self) -> bool:
        return True


class FailingPublisher:
    async def publish(self, envelope: GitHubEventEnvelope) -> None:
        del envelope
        raise PublishError("test publish failure")

    async def is_ready(self) -> bool:
        return True


def _body() -> bytes:
    return FIXTURE.read_bytes()


def _signature(body: bytes, *, secret: str = SECRET) -> str:
    digest = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return f"sha256={digest}"


def _headers(body: bytes) -> dict[str, str]:
    return {
        "Content-Type": "application/json",
        "X-GitHub-Delivery": "delivery-example-1",
        "X-GitHub-Event": "pull_request",
        "X-Hub-Signature-256": _signature(body),
    }


def _client(
    *,
    maximum_bytes: int = 1_048_576,
    publisher: RecordingPublisher | FailingPublisher | None = None,
) -> TestClient:
    settings = WebhookSettings(
        webhook_secret=SecretStr(SECRET),
        webhook_max_body_bytes=maximum_bytes,
    )
    resolved_publisher = publisher or RecordingPublisher()
    return TestClient(
        create_app(
            settings,
            publisher=resolved_publisher,
            clock=lambda: RECEIVED_AT,
        )
    )


def test_signed_supported_request_is_validated() -> None:
    body = _body()
    publisher = RecordingPublisher()

    response = _client(publisher=publisher).post(
        "/webhooks/github",
        content=body,
        headers=_headers(body),
    )

    assert response.status_code == 202
    assert response.json() == {
        "status": "accepted",
        "delivery_id": "delivery-example-1",
    }
    assert len(publisher.envelopes) == 1
    assert publisher.envelopes[0].payload["pull_request"]["number"] == 17


@pytest.mark.parametrize("signature", [None, "sha256=invalid"])
def test_missing_or_invalid_signature_is_rejected(signature: str | None) -> None:
    body = _body()
    headers = _headers(body)
    if signature is None:
        del headers["X-Hub-Signature-256"]
    else:
        headers["X-Hub-Signature-256"] = signature

    response = _client().post("/webhooks/github", content=body, headers=headers)

    assert response.status_code == 401
    assert response.json() == {"detail": "invalid webhook signature"}


@pytest.mark.parametrize(
    ("header", "value", "expected_detail"),
    [
        ("X-GitHub-Event", None, "missing required header: X-GitHub-Event"),
        ("X-GitHub-Event", " ", "missing required header: X-GitHub-Event"),
        ("X-GitHub-Delivery", None, "missing required header: X-GitHub-Delivery"),
    ],
)
def test_missing_required_delivery_headers_are_rejected(
    header: str,
    value: str | None,
    expected_detail: str,
) -> None:
    body = _body()
    headers = _headers(body)
    if value is None:
        del headers[header]
    else:
        headers[header] = value

    response = _client().post("/webhooks/github", content=body, headers=headers)

    assert response.status_code == 400
    assert response.json() == {"detail": expected_detail}


def test_unsupported_event_is_rejected() -> None:
    body = _body()
    headers = _headers(body)
    headers["X-GitHub-Event"] = "issues"

    response = _client().post("/webhooks/github", content=body, headers=headers)

    assert response.status_code == 422
    assert response.json() == {"detail": "unsupported webhook event"}


def test_oversized_request_is_rejected_before_processing() -> None:
    body = _body()

    response = _client(maximum_bytes=len(body) - 1).post(
        "/webhooks/github",
        content=body,
        headers=_headers(body),
    )

    assert response.status_code == 413
    assert response.json() == {"detail": "webhook request body is too large"}


@pytest.mark.parametrize(
    ("body", "expected_detail"),
    [
        (b"{not-json", "invalid JSON webhook payload"),
        (b"[]", "webhook payload must be a JSON object"),
    ],
)
def test_invalid_json_shapes_are_rejected(body: bytes, expected_detail: str) -> None:
    response = _client().post(
        "/webhooks/github",
        content=body,
        headers=_headers(body),
    )

    assert response.status_code == 422
    assert response.json() == {"detail": expected_detail}


def test_payload_missing_required_contract_fields_is_rejected() -> None:
    payload = json.loads(_body())
    del payload["repository"]
    body = json.dumps(payload).encode()

    response = _client().post("/webhooks/github", content=body, headers=_headers(body))

    assert response.status_code == 422
    assert response.json() == {"detail": "invalid webhook payload"}


def test_publish_failure_returns_non_2xx() -> None:
    body = _body()

    response = _client(publisher=FailingPublisher()).post(
        "/webhooks/github",
        content=body,
        headers=_headers(body),
    )

    assert response.status_code == 503
    assert response.json() == {"detail": "webhook event could not be published"}


def test_unconfigured_publisher_fails_closed() -> None:
    body = _body()
    settings = WebhookSettings(webhook_secret=SecretStr(SECRET))

    response = TestClient(
        create_app(settings, clock=lambda: RECEIVED_AT),
    ).post("/webhooks/github", content=body, headers=_headers(body))

    assert response.status_code == 503


def test_health_endpoints_report_process_and_dependency_state() -> None:
    ready_client = _client()
    settings = WebhookSettings(webhook_secret=SecretStr(SECRET))
    unready_client = TestClient(
        create_app(settings, clock=lambda: RECEIVED_AT),
    )

    assert ready_client.get("/health/live").json() == {"status": "ok"}
    ready_response = ready_client.get("/health/ready")
    unready_response = unready_client.get("/health/ready")

    assert ready_response.status_code == 200
    assert ready_response.json() == {"status": "ok"}
    assert unready_response.status_code == 503
    assert unready_response.json() == {"status": "not_ready"}


def test_metrics_report_accepted_and_rejected_requests() -> None:
    client = _client()
    body = _body()
    accepted = client.post("/webhooks/github", content=body, headers=_headers(body))
    rejected = client.post("/webhooks/github", content=body)

    metrics = client.get("/metrics")

    assert accepted.status_code == 202
    assert rejected.status_code == 401
    assert metrics.status_code == 200
    assert metrics.headers["content-type"].startswith("text/plain")
    assert 'github_webhook_requests_total{outcome="accepted"} 1.0' in metrics.text
    assert 'github_webhook_requests_total{outcome="rejected"} 1.0' in metrics.text
    assert 'github_webhook_publish_total{outcome="success"} 1.0' in metrics.text
    assert "github_webhook_publish_duration_seconds_count 1.0" in metrics.text


def test_app_loads_settings_from_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GITHUB_WEBHOOK_SECRET", SECRET)
    monkeypatch.setenv("GITHUB_WEBHOOK_MAX_BODY_BYTES", "1048576")
    body = _body()

    response = TestClient(
        create_app(
            publisher=RecordingPublisher(),
            clock=lambda: RECEIVED_AT,
        )
    ).post(
        "/webhooks/github",
        content=body,
        headers=_headers(body),
    )

    assert response.status_code == 202


def test_runtime_app_wires_environment_settings_to_kafka_publisher(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    publisher = RecordingPublisher()
    monkeypatch.setenv("GITHUB_WEBHOOK_SECRET", SECRET)
    monkeypatch.setattr(
        "github_analytics.webhook.app.create_kafka_publisher",
        lambda settings: publisher,
    )

    response = TestClient(create_runtime_app()).get("/health/ready")

    assert response.status_code == 200


def test_default_clock_is_timezone_aware_utc() -> None:
    assert utc_now().tzinfo is UTC
