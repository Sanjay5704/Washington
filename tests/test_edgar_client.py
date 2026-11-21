from __future__ import annotations

import pytest
import requests

from washington.edgar.client import RateLimiter, SecClient, SecClientError


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def clock(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


def test_rate_limiter_spaces_requests() -> None:
    fake = FakeClock()
    limiter = RateLimiter(5, clock=fake.clock, sleep=fake.sleep)
    for _ in range(6):
        limiter.wait()
    # 6 calls at 5/s: first is free, then 5 gaps of 0.2s
    assert fake.now == pytest.approx(1.0)
    assert len(fake.sleeps) == 5


class FakeResponse:
    def __init__(self, status: int, text: str = "", headers: dict | None = None) -> None:
        self.status_code = status
        self.text = text
        self.headers = headers or {}


class FakeSession:
    def __init__(self, responses: list) -> None:
        self.responses = list(responses)
        self.headers: dict[str, str] = {}
        self.calls = 0

    def get(self, url: str, timeout: float) -> FakeResponse:
        self.calls += 1
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def make_client(responses: list, sleeps: list[float]) -> tuple[SecClient, FakeSession]:
    session = FakeSession(responses)
    client = SecClient(
        "Washington test@example.com",
        max_requests_per_second=1000 / 999,  # effectively no rate limit wait in tests
        session=session,  # type: ignore[arg-type]
        sleep=sleeps.append,
    )
    client.limiter = RateLimiter(1000, sleep=lambda s: None)
    return client, session


def test_user_agent_is_sent() -> None:
    client, session = make_client([FakeResponse(200, "ok")], [])
    assert session.headers["User-Agent"] == "Washington test@example.com"
    assert client.get("https://www.sec.gov/x").text == "ok"


def test_user_agent_must_contain_email() -> None:
    with pytest.raises(ValueError):
        SecClient("Washington")


def test_refuses_rate_above_sec_limit() -> None:
    with pytest.raises(ValueError):
        SecClient("Washington a@b.com", max_requests_per_second=10)


def test_retries_429_then_succeeds_with_backoff() -> None:
    sleeps: list[float] = []
    client, session = make_client(
        [FakeResponse(429), FakeResponse(503), FakeResponse(200, "done")], sleeps
    )
    assert client.get("u").text == "done"
    assert session.calls == 3
    assert sleeps == [1.0, 2.0]  # exponential backoff


def test_retry_after_header_is_respected() -> None:
    sleeps: list[float] = []
    client, _ = make_client(
        [FakeResponse(429, headers={"Retry-After": "7"}), FakeResponse(200)], sleeps
    )
    client.get("u")
    assert sleeps == [7.0]


def test_connection_errors_are_retried() -> None:
    sleeps: list[float] = []
    client, _ = make_client([requests.ConnectionError("reset"), FakeResponse(200, "ok")], sleeps)
    assert client.get("u").text == "ok"


def test_404_returns_none() -> None:
    client, _ = make_client([FakeResponse(404)], [])
    assert client.get("u") is None


def test_gives_up_after_max_retries() -> None:
    sleeps: list[float] = []
    client, session = make_client([FakeResponse(500)] * 6, sleeps)
    with pytest.raises(SecClientError):
        client.get("u")
    assert session.calls == 6


def test_403_is_not_retried() -> None:
    client, session = make_client([FakeResponse(403)], [])
    with pytest.raises(SecClientError):
        client.get("u")
    assert session.calls == 1
