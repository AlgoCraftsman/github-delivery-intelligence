"""FastAPI application for the GitHub webhook validation boundary."""

import json
from collections.abc import Callable
from datetime import UTC, datetime
from time import perf_counter

from fastapi import FastAPI, HTTPException, Request, Response, status
from fastapi.responses import JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST
from pydantic import ValidationError

from github_analytics.webhook.config import WebhookSettings
from github_analytics.webhook.kafka import create_kafka_publisher
from github_analytics.webhook.metrics import WebhookMetrics
from github_analytics.webhook.models import (
    GitHubEventEnvelope,
    GitHubEventName,
    WebhookReceipt,
)
from github_analytics.webhook.publishing import (
    EnvelopePublisher,
    PublishError,
    UnconfiguredPublisher,
)
from github_analytics.webhook.security import has_valid_signature

Clock = Callable[[], datetime]


def utc_now() -> datetime:
    """Return a timezone-aware receiver timestamp."""

    return datetime.now(UTC)


async def _read_limited_body(request: Request, *, maximum_bytes: int) -> bytes:
    body = bytearray()
    async for chunk in request.stream():
        body.extend(chunk)
        if len(body) > maximum_bytes:
            raise HTTPException(
                status_code=status.HTTP_413_CONTENT_TOO_LARGE,
                detail="webhook request body is too large",
            )
    return bytes(body)


def _required_header(request: Request, name: str) -> str:
    value = request.headers.get(name)
    if value is None or not value.strip():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"missing required header: {name}",
        )
    return value


def _parse_payload(body: bytes) -> dict[str, object]:
    try:
        payload = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="invalid JSON webhook payload",
        ) from error

    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="webhook payload must be a JSON object",
        )
    return payload


def create_app(
    settings: WebhookSettings | None = None,
    *,
    publisher: EnvelopePublisher | None = None,
    clock: Clock = utc_now,
) -> FastAPI:
    """Create a receiver app with injectable configuration and time."""

    resolved_settings = settings or WebhookSettings()  # type: ignore[call-arg]
    resolved_publisher = publisher or UnconfiguredPublisher()
    metrics = WebhookMetrics()
    app = FastAPI(title="GitHub Delivery Intelligence Webhook Receiver")

    @app.exception_handler(HTTPException)
    async def handle_http_error(
        request: Request,
        error: HTTPException,
    ) -> JSONResponse:
        del request
        metrics.requests.labels(outcome="rejected").inc()
        return JSONResponse(
            status_code=error.status_code,
            content={"detail": error.detail},
            headers=error.headers,
        )

    @app.get("/health/live")
    async def liveness() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/health/ready")
    async def readiness() -> JSONResponse:
        ready = await resolved_publisher.is_ready()
        return JSONResponse(
            status_code=status.HTTP_200_OK if ready else status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"status": "ok" if ready else "not_ready"},
        )

    @app.get("/metrics")
    async def prometheus_metrics() -> Response:
        return Response(
            content=metrics.render(),
            headers={"Content-Type": CONTENT_TYPE_LATEST},
        )

    @app.post(
        "/webhooks/github",
        response_model=WebhookReceipt,
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def receive_github_webhook(request: Request) -> WebhookReceipt:
        body = await _read_limited_body(
            request,
            maximum_bytes=resolved_settings.webhook_max_body_bytes,
        )

        signature = request.headers.get("X-Hub-Signature-256")
        if signature is None or not has_valid_signature(
            body=body,
            secret=resolved_settings.webhook_secret.get_secret_value(),
            signature=signature,
        ):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="invalid webhook signature",
            )

        event_header = _required_header(request, "X-GitHub-Event")
        delivery_id = _required_header(request, "X-GitHub-Delivery")
        try:
            event_name = GitHubEventName(event_header)
        except ValueError as error:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                detail="unsupported webhook event",
            ) from error

        payload = _parse_payload(body)
        try:
            envelope = GitHubEventEnvelope.from_webhook(
                delivery_id=delivery_id,
                event_name=event_name,
                received_at=clock(),
                payload=payload,
            )
        except ValidationError as error:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                detail="invalid webhook payload",
            ) from error

        publish_started_at = perf_counter()
        try:
            await resolved_publisher.publish(envelope)
        except PublishError as error:
            metrics.publishes.labels(outcome="failure").inc()
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="webhook event could not be published",
            ) from error
        finally:
            metrics.publish_duration.observe(perf_counter() - publish_started_at)

        metrics.publishes.labels(outcome="success").inc()
        metrics.requests.labels(outcome="accepted").inc()
        return WebhookReceipt(delivery_id=delivery_id)

    return app


def create_runtime_app() -> FastAPI:
    """Create the environment-configured receiver used by the service process."""

    settings = WebhookSettings()  # type: ignore[call-arg]
    return create_app(settings, publisher=create_kafka_publisher(settings))
