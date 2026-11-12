"""Rate limits, client addresses and API keys."""

from types import SimpleNamespace
from typing import Any, List

from src.v2.service.access import (
    RateLimiter,
    client_ip,
    hash_key,
    load_key_hashes,
    valid_api_key,
)


def test_rate_limiter_window() -> None:
    now: List[float] = [0.0]
    limiter = RateLimiter(limit=2, window_seconds=60, clock=lambda: now[0])
    assert limiter.allow("a") == (True, 0)
    assert limiter.allow("a") == (True, 0)
    allowed, retry = limiter.allow("a")
    assert not allowed and 0 < retry <= 61
    assert limiter.allow("b") == (True, 0)
    now[0] = 61.0
    assert limiter.allow("a") == (True, 0)


def test_anonymous_limit_is_ten_per_72_hours() -> None:
    from src.v2.service.api import get_rate_limiter

    limiter = get_rate_limiter()
    assert (limiter.limit, limiter.window) == (10, 72 * 3600)


def request(headers: dict, host: str = "10.0.0.1") -> Any:
    return SimpleNamespace(headers=headers, client=SimpleNamespace(host=host))


def test_client_ip_uses_the_last_forwarded_entry() -> None:
    assert (
        client_ip(request({"x-forwarded-for": "6.6.6.6, 203.0.113.9"})) == "203.0.113.9"
    )
    assert client_ip(request({})) == "10.0.0.1"


def test_api_keys_are_checked_by_hash() -> None:
    hashes = load_key_hashes(
        {"SEVERITY_API_KEY_HASHES": f" {hash_key('k1').upper()} ,{hash_key('k2')}"}
    )
    assert valid_api_key("k1", hashes)
    assert valid_api_key("k2", hashes)
    assert not valid_api_key("k3", hashes)
    assert not valid_api_key(None, hashes)
    assert load_key_hashes({}) == set()
