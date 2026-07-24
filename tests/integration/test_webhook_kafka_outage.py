"""Failure-boundary test using the real Kafka client against a closed port."""

import hashlib
import hmac
from pathlib import Path
from time import monotonic

from fastapi.testclient import TestClient
from pydantic import SecretStr

from github_analytics.webhook.app import create_app
from github_analytics.webhook.config import WebhookSettings
from github_analytics.webhook.kafka import create_kafka_publisher

FIXTURE = Path(__file__).parents[1] / "fixtures" / "pull_request.opened.json"
SECRET = "integration-test-secret"


def test_broker_outage_returns_bounded_non_2xx_response() -> None:
    body = FIXTURE.read_bytes()
    signature = hmac.new(SECRET.encode(), body, hashlib.sha256).hexdigest()
    settings = WebhookSettings(
        webhook_secret=SecretStr(SECRET),
        kafka_bootstrap_servers="127.0.0.1:1",
        kafka_publish_timeout_seconds=0.2,
        kafka_readiness_timeout_seconds=0.1,
    )
    client = TestClient(
        create_app(
            settings,
            publisher=create_kafka_publisher(settings),
        )
    )

    started_at = monotonic()
    response = client.post(
        "/webhooks/github",
        content=body,
        headers={
            "Content-Type": "application/json",
            "X-GitHub-Delivery": "outage-delivery-example",
            "X-GitHub-Event": "pull_request",
            "X-Hub-Signature-256": f"sha256={signature}",
        },
    )
    elapsed = monotonic() - started_at

    assert response.status_code == 503
    assert response.json() == {"detail": "webhook event could not be published"}
    assert elapsed < 1.0
