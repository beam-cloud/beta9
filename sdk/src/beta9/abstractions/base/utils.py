import os
import threading
import time
from contextlib import contextmanager
from typing import Any, Callable, Dict, Iterator, Optional, Tuple

from ... import terminal

TRUTHY_ENV_VALUES = {"1", "true", "yes", "on"}


def env_enabled(name: str) -> bool:
    return os.getenv(name, "").lower() in TRUTHY_ENV_VALUES


@contextmanager
def sdk_timing(label: str) -> Iterator[None]:
    if not env_enabled("BETA9_SDK_TIMINGS"):
        yield
        return

    start_ns = time.monotonic_ns()
    try:
        yield
    finally:
        elapsed_ms = (time.monotonic_ns() - start_ns) / 1_000_000
        terminal.detail(f"SDK timing {label}: {elapsed_ms:.1f}ms")


@contextmanager
def timed_lock(lock: threading.Lock, label: str) -> Iterator[None]:
    with sdk_timing(label):
        lock.acquire()
    try:
        yield
    finally:
        lock.release()


class TTLCache:
    def __init__(
        self,
        *,
        default_ttl_seconds: float,
        ttl_env_var: str,
        disabled: Optional[Callable[[], bool]] = None,
    ) -> None:
        self.default_ttl_seconds = default_ttl_seconds
        self.ttl_env_var = ttl_env_var
        self.disabled = disabled or (lambda: False)
        self._lock = threading.Lock()
        self._items: Dict[str, Tuple[Any, float]] = {}

    def get(self, key: str) -> Optional[Any]:
        if self.disabled():
            return None

        now = time.monotonic()
        with self._lock:
            entry = self._items.get(key)
            if entry is None:
                return None

            value, expires_at = entry
            if expires_at <= now:
                self._items.pop(key, None)
                return None

            return value

    def set(self, key: str, value: Any, *, aliases: Tuple[str, ...] = ()) -> None:
        expires_at = time.monotonic() + self._ttl_seconds()
        with self._lock:
            for cache_key in (key, *aliases):
                self._items[cache_key] = (value, expires_at)

    def _ttl_seconds(self) -> float:
        value = os.getenv(self.ttl_env_var)
        if value is None:
            return self.default_ttl_seconds

        try:
            return max(0.0, float(value))
        except ValueError:
            return self.default_ttl_seconds
