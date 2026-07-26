"""Small GitHub GraphQL client with explicit rate-limit behavior."""

import random
import time
from collections.abc import Callable, Mapping
from typing import Any, Protocol, cast

import httpx2


class GraphQLClientError(RuntimeError):
    """Base error for failed or malformed GitHub GraphQL calls."""


class GraphQLRequestError(GraphQLClientError):
    """GitHub rejected a request or returned GraphQL errors."""


class RateLimitError(GraphQLClientError):
    """A primary or secondary rate limit could not be cleared safely."""


class HttpResponse(Protocol):
    """Response surface used by the client and its deterministic tests."""

    status_code: int
    headers: Mapping[str, str]

    def json(self) -> object: ...


class HttpClient(Protocol):
    """Synchronous HTTP surface required for GraphQL POST calls."""

    def post(
        self,
        url: str,
        *,
        headers: Mapping[str, str],
        json: Mapping[str, object],
        timeout: float,
    ) -> HttpResponse: ...

    def close(self) -> None: ...


class GitHubGraphQLClient:
    """Execute authenticated GraphQL queries and honor GitHub rate-limit signals."""

    def __init__(
        self,
        token: str,
        *,
        url: str = "https://api.github.com/graphql",
        request_timeout_seconds: float = 20.0,
        max_rate_limit_retries: int = 3,
        secondary_backoff_seconds: float = 60.0,
        http_client: HttpClient | None = None,
        sleep: Callable[[float], None] = time.sleep,
        epoch_time: Callable[[], float] = time.time,
        jitter: Callable[[float], float] = lambda upper: random.uniform(0, upper),
    ) -> None:
        if not token:
            raise ValueError("GitHub token must be nonempty")
        if request_timeout_seconds <= 0:
            raise ValueError("request timeout must be positive")
        if max_rate_limit_retries < 0:
            raise ValueError("max rate-limit retries must be nonnegative")
        if secondary_backoff_seconds < 60:
            raise ValueError("secondary rate-limit backoff must be at least 60 seconds")
        self._url = url
        self._timeout = request_timeout_seconds
        self._max_retries = max_rate_limit_retries
        self._secondary_backoff = secondary_backoff_seconds
        self._http = http_client or cast(HttpClient, httpx2.Client())
        self._owns_http_client = http_client is None
        self._sleep = sleep
        self._epoch_time = epoch_time
        self._jitter = jitter
        self._headers = {
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "User-Agent": "github-delivery-intelligence-backfill",
        }

    def execute(
        self,
        query: str,
        variables: Mapping[str, object],
    ) -> dict[str, Any]:
        """Return the GraphQL data object after bounded rate-limit retries."""

        for attempt in range(self._max_retries + 1):
            response = self._http.post(
                self._url,
                headers=self._headers,
                json={"query": query, "variables": dict(variables)},
                timeout=self._timeout,
            )
            payload = _json_object(response)
            errors = _error_messages(payload)
            remaining = _integer_header(response.headers, "x-ratelimit-remaining")

            if errors and remaining == 0:
                self._retry_primary(response.headers, attempt)
                continue
            if _is_secondary_limit(response.status_code, response.headers, errors):
                self._retry_secondary(response.headers, attempt)
                continue
            if response.status_code != 200:
                raise GraphQLRequestError(f"GitHub GraphQL returned HTTP {response.status_code}")
            if errors:
                raise GraphQLRequestError("GitHub GraphQL errors: " + "; ".join(errors))
            data = payload.get("data")
            if not isinstance(data, dict):
                raise GraphQLRequestError("GitHub GraphQL response is missing a data object")
            return cast(dict[str, Any], data)
        raise AssertionError(  # pragma: no cover - loop returns or raises on every iteration
            "rate-limit retry loop exhausted without returning"
        )

    def close(self) -> None:
        """Close only the internally created HTTP client."""

        if self._owns_http_client:
            self._http.close()

    def _retry_primary(self, headers: Mapping[str, str], attempt: int) -> None:
        if attempt >= self._max_retries:
            raise RateLimitError("GitHub GraphQL primary rate limit remained exhausted")
        reset_at = _integer_header(headers, "x-ratelimit-reset")
        if reset_at is None:
            raise RateLimitError("GitHub omitted x-ratelimit-reset for an exhausted limit")
        delay = max(0.0, reset_at - self._epoch_time()) + self._jitter(1.0)
        self._sleep(delay)

    def _retry_secondary(self, headers: Mapping[str, str], attempt: int) -> None:
        if attempt >= self._max_retries:
            raise RateLimitError("GitHub GraphQL secondary rate limit persisted")
        retry_after = _float_header(headers, "retry-after")
        base_delay = (
            retry_after if retry_after is not None else self._secondary_backoff * (2**attempt)
        )
        self._sleep(base_delay + self._jitter(min(5.0, base_delay * 0.1)))


def _json_object(response: HttpResponse) -> dict[str, Any]:
    try:
        payload = response.json()
    except ValueError as error:
        raise GraphQLRequestError("GitHub GraphQL returned invalid JSON") from error
    if not isinstance(payload, dict):
        raise GraphQLRequestError("GitHub GraphQL returned a non-object response")
    return cast(dict[str, Any], payload)


def _error_messages(payload: Mapping[str, Any]) -> list[str]:
    errors = payload.get("errors")
    if not isinstance(errors, list):
        return []
    messages: list[str] = []
    for error in errors:
        if isinstance(error, dict) and isinstance(error.get("message"), str):
            messages.append(error["message"])
    return messages


def _is_secondary_limit(
    status_code: int,
    headers: Mapping[str, str],
    errors: list[str],
) -> bool:
    if status_code == 429 or _header(headers, "retry-after") is not None:
        return True
    return any(
        "secondary rate limit" in message.lower() or "abuse detection" in message.lower()
        for message in errors
    )


def _header(headers: Mapping[str, str], name: str) -> str | None:
    expected = name.lower()
    for key, value in headers.items():
        if key.lower() == expected:
            return value
    return None


def _integer_header(headers: Mapping[str, str], name: str) -> int | None:
    value = _header(headers, name)
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _float_header(headers: Mapping[str, str], name: str) -> float | None:
    value = _header(headers, name)
    if value is None:
        return None
    try:
        parsed = float(value)
    except ValueError:
        return None
    return parsed if parsed >= 0 else None
