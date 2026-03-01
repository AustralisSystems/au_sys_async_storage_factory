"""
Health Check Interface - Single Responsibility Principle.

Defines health monitoring capabilities for storage providers.
Separated from core storage operations for better testability and composability.

Scaffolded from rest-api-orchestrator `src/services/storage/interfaces/health.py` (commit a5f0cc2a4f33f312e3d340c06f9aba4688ae9f01).
"""

from abc import ABC, abstractmethod
from datetime import datetime, UTC
from typing import Any, Optional


class IHealthCheck(ABC):
    """
    Interface for storage health monitoring.

    This interface is responsible only for health checking and monitoring.
    It can be implemented by storage providers or used as a separate service.
    """

    @abstractmethod
    def is_healthy(self) -> bool:
        """
        Check if the storage provider is currently healthy.

        Returns:
            True if healthy, False otherwise
        """

    @abstractmethod
    def get_health_status(self) -> dict[str, Any]:
        """
        Get detailed health information.

        Returns:
            Dictionary containing health metrics and status details
        """

    @abstractmethod
    def get_last_health_check(self) -> datetime:
        """
        Get the timestamp of the last health check.

        Returns:
            Datetime of the last health check
        """

    @abstractmethod
    def perform_deep_health_check(self) -> bool:
        """
        Perform a deep health check (write/read/delete).
        This method should verify that the storage is actually functional,
        not just that the connection is open.

        Returns:
            True if deep check passed, False otherwise
        """

    async def perform_deep_health_check_async(self) -> bool:
        """
        Asynchronous deep health check.
        Defaults to running the sync version in a thread.

        Returns:
            True if deep check passed, False otherwise
        """
        import asyncio

        return await asyncio.to_thread(self.perform_deep_health_check)


class HealthMonitor:
    """
    Simple implementation of health monitoring.

    This can be used as a base class or composed with storage providers.
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
        """Check if currently healthy."""
        return self._is_healthy

    def get_health_status(self) -> dict[str, Any]:
        """Get detailed health status."""
        return {
            "healthy": self._is_healthy,
            "last_check": self._last_check.isoformat(),
            "failure_count": self._failure_count,
            "details": self._health_details,
        }

    def get_last_health_check(self) -> datetime:
        """Get last health check time."""
        return self._last_check
