"""Tests for GitHub REST response and rate-limit handling."""

from collections.abc import Mapping
from typing import Any

import pytest

from github_analytics.backfill.rest import (
    GitHubRestClient,
    RestRateLimitError,
    RestRequestError,
    _float_header,
)


class FakeResponse:
    def __init__(
        self,
        payload: object,
        *,
        status_code: int = 200,
        headers: Mapping[str, str] | None = None,
        invalid_json: bool = False,
    ) -> None:
        self.payload = payload
        self.status_code = status_code
        self.headers = headers or {}
        self.invalid_json = invalid_json

    def json(self) -> object:
        if self.invalid_json:
            raise ValueError("bad json")
        return self.payload


class FakeHttpClient:
    def __init__(self, responses: list[FakeResponse]) -> None:
        self.responses = responses
        self.requests: list[dict[str, Any]] = []
        self.closed = False

    def get(
        self,
        url: str,
        *,
        headers: Mapping[str, str],
        params: Mapping[str, object],
        timeout: float,
    ) -> FakeResponse:
        self.requests.append({"url": url, "headers": headers, "params": params, "timeout": timeout})
        return self.responses.pop(0)

    def close(self) -> None:
        self.closed = True


def _client(
    responses: list[FakeResponse],
    *,
    sleeps: list[float] | None = None,
    max_retries: int = 3,
    epoch: float = 100.0,
) -> tuple[GitHubRestClient, FakeHttpClient]:
    http = FakeHttpClient(responses)
    sleep_values = sleeps if sleeps is not None else []
    return (
        GitHubRestClient(
            "secret",
            base_url="https://example.test/",
            api_version="2026-03-10",
            request_timeout_seconds=12,
            max_rate_limit_retries=max_retries,
            http_client=http,
            sleep=sleep_values.append,
            epoch_time=lambda: epoch,
            jitter=lambda _upper: 0.5,
        ),
        http,
    )


def test_get_returns_payload_and_sends_versioned_auth_headers() -> None:
    client, http = _client([FakeResponse([{"id": 1}])])

    assert client.get("/repos/example/repo/deployments", {"page": 2}) == [{"id": 1}]
    request = http.requests[0]
    assert request["url"] == "https://example.test/repos/example/repo/deployments"
    assert request["headers"]["Authorization"] == "Bearer secret"
    assert request["headers"]["X-GitHub-Api-Version"] == "2026-03-10"
    assert request["params"] == {"page": 2}
    assert request["timeout"] == 12
    client.close()
    assert not http.closed


def test_close_closes_internally_created_http_client() -> None:
    client = GitHubRestClient("secret")

    client.close()


@pytest.mark.parametrize(
    ("keyword", "value", "message"),
    [
        ("token", "", "token"),
        ("base_url", "", "base URL"),
        ("api_version", "", "version"),
        ("request_timeout_seconds", 0, "timeout"),
        ("max_rate_limit_retries", -1, "retries"),
        ("secondary_backoff_seconds", 59, "backoff"),
    ],
)
def test_client_rejects_unsafe_configuration(
    keyword: str,
    value: object,
    message: str,
) -> None:
    arguments: dict[str, object] = {"token": "secret"}
    arguments[keyword] = value
    with pytest.raises(ValueError, match=message):
        GitHubRestClient(**arguments)  # type: ignore[arg-type]


def test_get_rejects_empty_path() -> None:
    client, _ = _client([])

    with pytest.raises(ValueError, match="path"):
        client.get("", {})


def test_primary_limit_waits_until_reset_then_retries() -> None:
    sleeps: list[float] = []
    client, _ = _client(
        [
            FakeResponse(
                {"message": "rate limited"},
                status_code=403,
                headers={"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "110"},
            ),
            FakeResponse({"ok": True}),
        ],
        sleeps=sleeps,
    )

    assert client.get("resource", {}) == {"ok": True}
    assert sleeps == [10.5]


def test_primary_limit_requires_reset_and_honors_retry_budget() -> None:
    missing_reset, _ = _client(
        [
            FakeResponse(
                {},
                status_code=403,
                headers={"x-ratelimit-remaining": "0"},
            )
        ]
    )
    with pytest.raises(RestRateLimitError, match="omitted"):
        missing_reset.get("resource", {})

    exhausted, _ = _client(
        [
            FakeResponse(
                {},
                status_code=429,
                headers={"x-ratelimit-remaining": "0", "x-ratelimit-reset": "100"},
            )
        ],
        max_retries=0,
    )
    with pytest.raises(RestRateLimitError, match="primary"):
        exhausted.get("resource", {})


@pytest.mark.parametrize("status_code", [403, 429])
def test_secondary_limit_honors_retry_after(status_code: int) -> None:
    sleeps: list[float] = []
    client, _ = _client(
        [
            FakeResponse(
                {"message": "secondary rate limit"},
                status_code=status_code,
                headers={"Retry-After": "2.5"},
            ),
            FakeResponse({}),
        ],
        sleeps=sleeps,
    )

    assert client.get("resource", {}) == {}
    assert sleeps == [3.0]


@pytest.mark.parametrize(
    "message",
    ["You have exceeded a secondary rate limit", "Abuse detection mechanism"],
)
def test_secondary_message_uses_exponential_default(message: str) -> None:
    sleeps: list[float] = []
    client, _ = _client(
        [
            FakeResponse({"message": message}, status_code=403),
            FakeResponse(
                {"message": message},
                status_code=403,
                headers={"retry-after": "not-a-number"},
            ),
            FakeResponse([]),
        ],
        sleeps=sleeps,
    )

    assert client.get("resource", {}) == []
    assert sleeps == [60.5, 120.5]


def test_secondary_limit_raises_after_retry_budget() -> None:
    client, _ = _client([FakeResponse({}, status_code=429)], max_retries=0)

    with pytest.raises(RestRateLimitError, match="secondary"):
        client.get("resource", {})


def test_plain_forbidden_is_not_misclassified_as_rate_limit() -> None:
    client, _ = _client([FakeResponse({"message": 3}, status_code=403)])

    with pytest.raises(RestRequestError, match="HTTP 403"):
        client.get("resource", {})


@pytest.mark.parametrize(
    ("response", "message"),
    [
        (FakeResponse({}, status_code=500), "HTTP 500"),
        (FakeResponse("text"), "neither"),
        (FakeResponse({}, invalid_json=True), "invalid JSON"),
    ],
)
def test_get_rejects_http_and_shape_failures(
    response: FakeResponse,
    message: str,
) -> None:
    client, _ = _client([response])

    with pytest.raises(RestRequestError, match=message):
        client.get("resource", {})


def test_header_parsers_ignore_malformed_and_negative_values() -> None:
    client, _ = _client(
        [
            FakeResponse(
                {},
                headers={"x-ratelimit-remaining": "bad"},
            )
        ]
    )

    assert client.get("resource", {}) == {}
    assert _float_header({"retry-after": "-1"}, "retry-after") is None
    assert _float_header({}, "retry-after") is None
