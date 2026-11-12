"""Who may start new work: per-IP rate limits and API keys (design §6).

Anonymous submissions that would start a job are limited per client IP, in
memory. There is one API instance, so the count is exact, and resetting on
deploy is fine. An API key lifts the limit for programmatic clients. Cache
hits and joining a running job cost nothing and are never limited.
"""

import hashlib
import hmac
import os
import threading
import time
from collections import defaultdict, deque
from typing import Callable, Deque, Dict, Optional, Set, Tuple

from fastapi import Request

ENV_API_KEY_HASHES = "SEVERITY_API_KEY_HASHES"


class RateLimiter:
    """At most ``limit`` events per key in any ``window_seconds``."""

    def __init__(
        self,
        limit: int,
        window_seconds: float,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.limit = limit
        self.window = window_seconds
        self._clock = clock
        self._events: Dict[str, Deque[float]] = defaultdict(deque)
        self._lock = threading.Lock()

    def allow(self, key: str) -> Tuple[bool, int]:
        """Record an event if allowed; return (allowed, seconds until one is)."""
        now = self._clock()
        with self._lock:
            events = self._events[key]
            while events and events[0] <= now - self.window:
                events.popleft()
            if len(events) >= self.limit:
                return False, max(1, int(events[0] + self.window - now) + 1)
            events.append(now)
            return True, 0


def client_ip(request: Request) -> str:
    """The caller's address.

    Behind Cloud Run the last X-Forwarded-For entry is the one Google's front
    end appended; earlier entries come from the client and can be forged.
    """
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[-1].strip()
    return request.client.host if request.client else "unknown"


def hash_key(key: str) -> str:
    return hashlib.sha256(key.encode()).hexdigest()


def load_key_hashes(env: Optional[Dict[str, str]] = None) -> Set[str]:
    """SHA-256 hex digests of the valid keys, comma-separated (from Secret Manager)."""
    raw = (os.environ if env is None else env).get(ENV_API_KEY_HASHES, "")
    return {part.strip().lower() for part in raw.split(",") if part.strip()}


def valid_api_key(key: Optional[str], hashes: Set[str]) -> bool:
    if not key:
        return False
    digest = hash_key(key)
    return any(hmac.compare_digest(digest, known) for known in hashes)
