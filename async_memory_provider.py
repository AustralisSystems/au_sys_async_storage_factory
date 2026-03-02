from __future__ import annotations

"""
Async Memory Provider - Transient High-Speed Cache.

Implements IStorageProvider using an in-memory dictionary.
Intended for transient data, caching, and testing.
"""

import re
import time
from datetime import datetime, UTC
from typing import Any, Optional

from .interfaces.storage import IStorageProvider
from .interfaces.health import IHealthCheck, HealthMonitor


class AsyncMemoryProvider(IStorageProvider, IHealthCheck):
    """
    Volatile in-memory storage provider.
    """

    def __init__(self) -> None:
        self._store: dict[str, Any] = {}
        self._expires: dict[str, float] = {}
        self._health_monitor = HealthMonitor()

    async def get_async(self, key: str) -> Optional[Any]:
        if key in self._expires and time.time() > self._expires[key]:
            await self.delete_async(key)
            return None
        return self._store.get(key)

    async def set_async(self, key: str, value: Any) -> bool:
        self._store[key] = value
        if key in self._expires:
            del self._expires[key]
        return True

    async def delete_async(self, key: str) -> bool:
        if key in self._store:
            del self._store[key]
            if key in self._expires:
                del self._expires[key]
            return True
        return False

    async def exists_async(self, key: str) -> bool:
        if key in self._expires and time.time() > self._expires[key]:
            await self.delete_async(key)
            return False
        return key in self._store

    async def list_keys_async(self, pattern: Optional[str] = None) -> list[str]:
        keys = list(self._store.keys())
        if pattern:
            regex = re.compile(pattern, re.IGNORECASE)
            keys = [k for k in keys if regex.search(k)]
        return keys

    async def find_async(self, query: dict[str, Any]) -> list[Any]:
        results = []
        for val in self._store.values():
            if isinstance(val, dict):
                match = True
                for qk, qv in query.items():
                    if val.get(qk) != qv:
                        match = False
                        break
                if match:
                    results.append(val)
        return results

    async def clear_async(self) -> int:
        count = len(self._store)
        self._store.clear()
        self._expires.clear()
        return count

    def supports_ttl(self) -> bool:
        return True

    async def set_with_ttl_async(self, key: str, value: Any, ttl: int) -> bool:
        self._store[key] = value
        self._expires[key] = time.time() + ttl
        return True

    # --- IHealthCheck Implementation ---

    def is_healthy(self) -> bool:
        return self._health_monitor.is_healthy()

    def get_health_status(self) -> dict[str, Any]:
        return self._health_monitor.get_health_status()

    def get_last_health_check(self) -> datetime:
        return self._health_monitor.get_last_health_check()

    async def perform_deep_health_check(self) -> bool:
        # Memory is always healthy if the object exists
        self._health_monitor.update_health(True)
        return True
