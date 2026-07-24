"""Opt-in positive-path test against the local Compose Kafka broker."""

import hashlib
import hmac
import os
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr

from github_analytics.webhook.app import create_app
from github_analytics.webhook.config import WebhookSettings
from github_analytics.webhook.kafka import create_kafka_publisher

pytestmark = pytest.mark.skipif(
    os.getenv("RUN_KAFKA_INTEGRATION") != "1",
    reason="requires RUN_KAFKA_INTEGRATION=1 and the local Kafka broker",
)

FIXTURE = Path(__file__).parents[1] / "fixtures" / "pull_request.opened.json"
SECRET = "live-integration-test-secret"


def test_signed_webhook_returns_2xx_after_real_broker_acknowledgement() -> None:
    body = FIXTURE.read_bytes()
    signature = hmac.new(SECRET.encode(), body, hashlib.sha256).hexdigest()
    settings = WebhookSettings(
        webhook_secret=SecretStr(SECRET),
        kafka_bootstrap_servers="localhost:9092",
        kafka_publish_timeout_seconds=5,
    )
    client = TestClient(
        create_app(
            settings,
            publisher=create_kafka_publisher(settings),
        )
    )

    response = client.post(
        "/webhooks/github",
        content=body,
        headers={
            "Content-Type": "application/json",
            "X-GitHub-Delivery": "live-delivery-example",
            "X-GitHub-Event": "pull_request",
            "X-Hub-Signature-256": f"sha256={signature}",
        },
    )

    assert response.status_code == 202
