"""HTTP client for SEC EDGAR that follows the SEC fair-access policy.

Rules (https://www.sec.gov/os/accessing-edgar-data):
  * every request carries a User-Agent that identifies the app and a contact email
  * at most 10 requests per second -> we default to 6 to keep a safety margin
  * back off when the server says "too many requests" (429) or has a 5xx hiccup
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable

import requests

log = logging.getLogger(__name__)

RETRY_STATUS = {429, 500, 502, 503, 504}


def is_missing_object(response: requests.Response) -> bool:
    """SEC's file store answers 'no such file' (e.g. no daily index on a weekend) with an
    S3-style 403 XML body <Code>AccessDenied</Code>, not a 404. SEC's real rate-limit block
    is also a 403, but an HTML page, so we only treat the S3 XML form as 'not found'."""
    return response.status_code == 403 and "<Code>AccessDenied</Code>" in response.text[:500]


class RateLimiter:
    """Simple 'minimum gap between requests' limiter. Thread-safe."""

    def __init__(
        self,
        max_per_second: float,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if max_per_second <= 0:
            raise ValueError("max_per_second must be positive")
        self.min_interval = 1.0 / max_per_second
        self._clock = clock
        self._sleep = sleep
        self._next_allowed = 0.0
        self._lock = threading.Lock()

    def wait(self) -> None:
        with self._lock:
            now = self._clock()
            if now < self._next_allowed:
                self._sleep(self._next_allowed - now)
                now = self._next_allowed
            self._next_allowed = now + self.min_interval


class SecClientError(RuntimeError):
    pass


class SecClient:
    def __init__(
        self,
        user_agent: str,
        max_requests_per_second: float = 6.0,
        max_retries: int = 5,
        backoff_base: float = 1.0,
        timeout: float = 30.0,
        session: requests.Session | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if not user_agent or "@" not in user_agent:
            raise ValueError("SEC requires a User-Agent with a contact email, e.g. 'App you@x.com'")
        if max_requests_per_second >= 10:
            raise ValueError("SEC allows fewer than 10 requests/second")
        self.session = session or requests.Session()
        self.session.headers.update({"User-Agent": user_agent, "Accept-Encoding": "gzip, deflate"})
        self.limiter = RateLimiter(max_requests_per_second, sleep=sleep)
        self.max_retries = max_retries
        self.backoff_base = backoff_base
        self.timeout = timeout
        self._sleep = sleep
        self.request_count = 0

    def _backoff_seconds(self, attempt: int, response: requests.Response | None) -> float:
        if response is not None:
            retry_after = response.headers.get("Retry-After", "")
            if retry_after.isdigit():
                return float(retry_after)
        # exponential: 1s, 2s, 4s, 8s ... capped at 60s
        return min(self.backoff_base * (2**attempt), 60.0)

    def get(self, url: str) -> requests.Response | None:
        """GET a URL. Returns None for 404 (e.g. no index on a holiday). Raises after retries."""
        last_error = ""
        for attempt in range(self.max_retries + 1):
            self.limiter.wait()
            self.request_count += 1
            response: requests.Response | None = None
            try:
                response = self.session.get(url, timeout=self.timeout)
            except requests.RequestException as exc:  # timeouts, connection resets
                last_error = f"{type(exc).__name__}: {exc}"
            else:
                if response.status_code == 200:
                    return response
                if response.status_code == 404 or is_missing_object(response):
                    return None
                last_error = f"HTTP {response.status_code}"
                if response.status_code not in RETRY_STATUS:
                    break  # 403 etc: retrying will not help
            if attempt < self.max_retries:
                wait = self._backoff_seconds(attempt, response)
                log.warning(
                    "GET %s failed (%s); retry %d in %.1fs", url, last_error, attempt + 1, wait
                )
                self._sleep(wait)
        raise SecClientError(f"GET {url} failed: {last_error}")
