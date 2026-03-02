from __future__ import annotations

"""
Health Check Interface - Single Responsibility Principle (Async First).

Defines health monitoring capabilities for storage providers.
"""

from abc import ABC, abstractmethod
from datetime import datetime, UTC
from typing import Any, Optional


class IHealthCheck(ABC):
    """
    Interface for storage health monitoring (Async First).
    """

    @abstractmethod
    def is_healthy(self) -> bool:
        """
        Check if the storage provider is currently healthy (Cached/Quick check).
        """

    @abstractmethod
    def get_health_status(self) -> dict[str, Any]:
        """
        Get detailed health information.
        """

    @abstractmethod
    def get_last_health_check(self) -> datetime:
        """
        Get the timestamp of the last health check.
        """

    @abstractmethod
    async def perform_deep_health_check(self) -> bool:
        """
        Perform a deep health check (write/read/delete) asynchronously.
        """


class HealthMonitor:
    """
    Simple implementation of health monitoring.
    """

    def __init__(self) -> None:
        self._last_check = datetime.now(UTC)
        self._is_healthy = True
        self._failure_count = 0
        self._health_details: dict[str, Any] = {}

    def update_health(self, is_healthy: bool, details: Optional[dict[str, Any]] = None) -> None:
        """Update health status with details."""
        self._last_check = datetime.now(UTC)
        self._is_healthy = is_healthy
        self._health_details = details or {}

        if not is_healthy:
            self._failure_count += 1
        else:
            self._failure_count = 0

    def is_healthy(self) -> bool:
        return self._is_healthy

    def get_health_status(self) -> dict[str, Any]:
        return {
            "healthy": self._is_healthy,
            "last_check": self._last_check.isoformat(),
            "failure_count": self._failure_count,
            "details": self._health_details,
        }

    def get_last_health_check(self) -> datetime:
        return self._last_check
