"""Tests for GitHub GraphQL response and rate-limit handling."""

from collections.abc import Mapping
from typing import Any

import pytest

from github_analytics.backfill.graphql import (
    GitHubGraphQLClient,
    GraphQLRequestError,
    RateLimitError,
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

    def post(
        self,
        url: str,
        *,
        headers: Mapping[str, str],
        json: Mapping[str, object],
        timeout: float,
    ) -> FakeResponse:
        self.requests.append({"url": url, "headers": headers, "json": json, "timeout": timeout})
        return self.responses.pop(0)

    def close(self) -> None:
        self.closed = True


def _client(
    responses: list[FakeResponse],
    *,
    sleeps: list[float] | None = None,
    max_retries: int = 3,
    epoch: float = 100.0,
) -> tuple[GitHubGraphQLClient, FakeHttpClient]:
    http = FakeHttpClient(responses)
    sleep_values = sleeps if sleeps is not None else []
    return (
        GitHubGraphQLClient(
            "secret",
            url="https://example.test/graphql",
            request_timeout_seconds=12,
            max_rate_limit_retries=max_retries,
            http_client=http,
            sleep=sleep_values.append,
            epoch_time=lambda: epoch,
            jitter=lambda _upper: 0.5,
        ),
        http,
    )


def test_execute_returns_data_and_sends_auth_without_closing_injected_client() -> None:
    client, http = _client([FakeResponse({"data": {"viewer": {"login": "octocat"}}})])

    assert client.execute("query Viewer {}", {"a": 1}) == {"viewer": {"login": "octocat"}}
    request = http.requests[0]
    assert request["url"] == "https://example.test/graphql"
    assert request["headers"]["Authorization"] == "Bearer secret"
    assert request["json"] == {
        "query": "query Viewer {}",
        "variables": {"a": 1},
    }
    assert request["timeout"] == 12
    client.close()
    assert not http.closed


def test_close_closes_internally_created_http_client() -> None:
    client = GitHubGraphQLClient("secret")

    client.close()


@pytest.mark.parametrize(
    ("keyword", "value", "message"),
    [
        ("token", "", "token"),
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
        GitHubGraphQLClient(**arguments)  # type: ignore[arg-type]


def test_primary_limit_sleeps_until_reset_then_retries() -> None:
    sleeps: list[float] = []
    client, _ = _client(
        [
            FakeResponse(
                {"errors": [{"message": "rate limited"}]},
                headers={"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "110"},
            ),
            FakeResponse({"data": {"ok": True}}),
        ],
        sleeps=sleeps,
    )

    assert client.execute("query {}", {}) == {"ok": True}
    assert sleeps == [10.5]


def test_primary_limit_clamps_past_reset_to_jitter() -> None:
    sleeps: list[float] = []
    client, _ = _client(
        [
            FakeResponse(
                {"errors": [{"message": "rate limited"}]},
                headers={"x-ratelimit-remaining": "0", "x-ratelimit-reset": "bad"},
            )
        ],
        sleeps=sleeps,
    )
    with pytest.raises(RateLimitError, match="omitted"):
        client.execute("query {}", {})
    assert sleeps == []

    retrying_client, _ = _client(
        [
            FakeResponse(
                {"errors": [{"message": "rate limited"}]},
                headers={"x-ratelimit-remaining": "0", "x-ratelimit-reset": "90"},
            ),
            FakeResponse({"data": {}}),
        ],
        sleeps=sleeps,
    )
    assert retrying_client.execute("query {}", {}) == {}
    assert sleeps == [0.5]


def test_primary_limit_raises_after_retry_budget() -> None:
    client, _ = _client(
        [
            FakeResponse(
                {"errors": [{"message": "rate limited"}]},
                headers={"x-ratelimit-remaining": "0", "x-ratelimit-reset": "100"},
            )
        ],
        max_retries=0,
    )

    with pytest.raises(RateLimitError, match="primary"):
        client.execute("query {}", {})


@pytest.mark.parametrize("status_code", [403, 429])
def test_secondary_limit_honors_retry_after(status_code: int) -> None:
    sleeps: list[float] = []
    client, _ = _client(
        [
            FakeResponse(
                {"message": "slow down"},
                status_code=status_code,
                headers={"Retry-After": "2.5"},
            ),
            FakeResponse({"data": {"ok": True}}),
        ],
        sleeps=sleeps,
    )

    assert client.execute("query {}", {}) == {"ok": True}
    assert sleeps == [3.0]


def test_plain_forbidden_response_is_not_misclassified_as_rate_limit() -> None:
    client, _ = _client([FakeResponse({}, status_code=403)])

    with pytest.raises(GraphQLRequestError, match="HTTP 403"):
        client.execute("query {}", {})


@pytest.mark.parametrize(
    "message",
    ["You have exceeded a secondary rate limit", "Abuse detection mechanism"],
)
def test_secondary_graphql_error_uses_exponential_default(message: str) -> None:
    sleeps: list[float] = []
    client, _ = _client(
        [
            FakeResponse({"errors": [{"message": message}]}),
            FakeResponse(
                {"errors": [{"message": message}]},
                headers={"retry-after": "not-a-number"},
            ),
            FakeResponse({"data": {}}),
        ],
        sleeps=sleeps,
    )

    assert client.execute("query {}", {}) == {}
    assert sleeps == [60.5, 120.5]


def test_secondary_limit_raises_after_retry_budget() -> None:
    client, _ = _client(
        [FakeResponse({}, status_code=429)],
        max_retries=0,
    )

    with pytest.raises(RateLimitError, match="secondary"):
        client.execute("query {}", {})


@pytest.mark.parametrize(
    ("response", "message"),
    [
        (FakeResponse({}, status_code=500), "HTTP 500"),
        (FakeResponse({"errors": [{"message": "bad query"}]}), "bad query"),
        (FakeResponse({"data": None}), "data object"),
        (FakeResponse([], status_code=200), "non-object"),
        (FakeResponse({}, invalid_json=True), "invalid JSON"),
    ],
)
def test_execute_rejects_http_graphql_and_shape_failures(
    response: FakeResponse,
    message: str,
) -> None:
    client, _ = _client([response])

    with pytest.raises(GraphQLRequestError, match=message):
        client.execute("query {}", {})


def test_malformed_error_entries_and_headers_do_not_hide_success() -> None:
    client, _ = _client(
        [
            FakeResponse(
                {"errors": [None, {"other": "value"}], "data": {"ok": True}},
                headers={"x-ratelimit-remaining": "not-an-int"},
            )
        ]
    )

    assert client.execute("query {}", {}) == {"ok": True}


def test_retry_after_parser_rejects_negative_values() -> None:
    assert _float_header({"retry-after": "-1"}, "retry-after") is None
    assert _float_header({"retry-after": "2"}, "retry-after") == 2.0
