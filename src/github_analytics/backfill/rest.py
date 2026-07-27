"""Small GitHub REST client with explicit, bounded rate-limit behavior."""

import random
import time
from collections.abc import Callable, Mapping
from typing import Any, Protocol, cast

import httpx2


class RestClientError(RuntimeError):
    """Base error for failed or malformed GitHub REST calls."""


class RestRequestError(RestClientError):
    """GitHub rejected a REST request or returned an invalid response."""


class RestRateLimitError(RestClientError):
    """A primary or secondary REST rate limit could not be cleared safely."""


class HttpResponse(Protocol):
    """Response surface used by the client and deterministic tests."""

    status_code: int
    headers: Mapping[str, str]

    def json(self) -> object: ...


class HttpClient(Protocol):
    """Synchronous HTTP surface required for REST GET calls."""

    def get(
        self,
        url: str,
        *,
        headers: Mapping[str, str],
        params: Mapping[str, object],
        timeout: float,
    ) -> HttpResponse: ...

    def close(self) -> None: ...


class GitHubRestClient:
    """Execute authenticated GitHub REST GET requests with bounded retries."""

    def __init__(
        self,
        token: str,
        *,
        base_url: str = "https://api.github.com",
        api_version: str = "2026-03-10",
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
        if not base_url:
            raise ValueError("GitHub REST base URL must be nonempty")
        if not api_version:
            raise ValueError("GitHub REST API version must be nonempty")
        if request_timeout_seconds <= 0:
            raise ValueError("request timeout must be positive")
        if max_rate_limit_retries < 0:
            raise ValueError("max rate-limit retries must be nonnegative")
        if secondary_backoff_seconds < 60:
            raise ValueError("secondary rate-limit backoff must be at least 60 seconds")
        self._base_url = base_url.rstrip("/")
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
            "X-GitHub-Api-Version": api_version,
        }

    def get(
        self,
        path: str,
        params: Mapping[str, object],
    ) -> dict[str, Any] | list[Any]:
        """Return a JSON object or array after bounded rate-limit retries."""

        if not path:
            raise ValueError("GitHub REST path must be nonempty")
        url = f"{self._base_url}/{path.lstrip('/')}"
        for attempt in range(self._max_retries + 1):
            response = self._http.get(
                url,
                headers=self._headers,
                params=params,
                timeout=self._timeout,
            )
            payload = _json_payload(response)
            message = _message(payload)
            remaining = _integer_header(response.headers, "x-ratelimit-remaining")

            if response.status_code in (403, 429) and remaining == 0:
                self._retry_primary(response.headers, attempt)
                continue
            if _is_secondary_limit(response.status_code, response.headers, message):
                self._retry_secondary(response.headers, attempt)
                continue
            if response.status_code != 200:
                raise RestRequestError(f"GitHub REST returned HTTP {response.status_code}")
            return payload
        raise AssertionError(  # pragma: no cover - loop returns or raises on every iteration
            "rate-limit retry loop exhausted without returning"
        )

    def close(self) -> None:
        """Close only the internally created HTTP client."""

        if self._owns_http_client:
            self._http.close()

    def _retry_primary(self, headers: Mapping[str, str], attempt: int) -> None:
        if attempt >= self._max_retries:
            raise RestRateLimitError("GitHub REST primary rate limit remained exhausted")
        reset_at = _integer_header(headers, "x-ratelimit-reset")
        if reset_at is None:
            raise RestRateLimitError("GitHub omitted x-ratelimit-reset for an exhausted limit")
        delay = max(0.0, reset_at - self._epoch_time()) + self._jitter(1.0)
        self._sleep(delay)

    def _retry_secondary(self, headers: Mapping[str, str], attempt: int) -> None:
        if attempt >= self._max_retries:
            raise RestRateLimitError("GitHub REST secondary rate limit persisted")
        retry_after = _float_header(headers, "retry-after")
        base_delay = (
            retry_after if retry_after is not None else self._secondary_backoff * (2**attempt)
        )
        self._sleep(base_delay + self._jitter(min(5.0, base_delay * 0.1)))


def _json_payload(response: HttpResponse) -> dict[str, Any] | list[Any]:
    try:
        payload = response.json()
    except ValueError as error:
        raise RestRequestError("GitHub REST returned invalid JSON") from error
    if not isinstance(payload, (dict, list)):
        raise RestRequestError("GitHub REST returned neither an object nor an array")
    return cast(dict[str, Any] | list[Any], payload)


def _message(payload: dict[str, Any] | list[Any]) -> str:
    if isinstance(payload, dict) and isinstance(payload.get("message"), str):
        return cast(str, payload["message"])
    return ""


def _is_secondary_limit(
    status_code: int,
    headers: Mapping[str, str],
    message: str,
) -> bool:
    if status_code == 429 or (status_code == 403 and _header(headers, "retry-after") is not None):
        return True
    lowered = message.lower()
    return status_code == 403 and (
        "secondary rate limit" in lowered or "abuse detection" in lowered
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
