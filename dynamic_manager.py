from __future__ import annotations
"""
Dynamic storage manager.

Provides lightweight health/error tracking so StorageFactory can expose
actionable telemetry and recommend failover when the active backend
experiences repeated failures.
"""


from dataclasses import dataclass, field
from datetime import datetime, UTC
from enum import Enum


class OperationType(str, Enum):
    READ = "read"
    WRITE = "write"
    DELETE = "delete"
    LIST = "list"
    HEALTH = "health"


@dataclass
class BackendMetrics:
    backend: str
    consecutive_failures: int = 0
    last_error: str | None = None
    last_failure_at: datetime | None = None
    last_success_at: datetime | None = None
    last_operation: OperationType | None = None
    total_failures: int = 0
    total_success: int = 0
    recent_error_operation: OperationType | None = None
    recent_error_detail: str | None = None
    extra: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, object]:
        return {
            "backend": self.backend,
            "consecutive_failures": self.consecutive_failures,
            "total_failures": self.total_failures,
            "total_success": self.total_success,
            "last_operation": self.last_operation.value if self.last_operation else None,
            "last_success_at": self.last_success_at.isoformat() if self.last_success_at else None,
            "last_failure_at": self.last_failure_at.isoformat() if self.last_failure_at else None,
            "last_error": self.last_error,
            "recent_error_operation": (self.recent_error_operation.value if self.recent_error_operation else None),
            "recent_error_detail": self.recent_error_detail,
            "extra": self.extra,
        }


class DynamicStorageManager:
    """Tracks backend health and recommends failover/failback actions."""

    def __init__(
        self,
        active_backend: str,
        *,
        fallback_backend: str = "tinydb",
        failure_threshold: int = 5,
    ) -> None:
        self._active_backend = active_backend
        self._fallback_backend = fallback_backend
        self._failure_threshold = max(1, failure_threshold)
        self._metrics: dict[str, BackendMetrics] = {}

    def _ensure_backend(self, backend: str) -> BackendMetrics:
        if backend not in self._metrics:
            self._metrics[backend] = BackendMetrics(backend=backend)
        return self._metrics[backend]

    def record_success(self, backend: str, operation: OperationType) -> None:
        metrics = self._ensure_backend(backend)
        metrics.consecutive_failures = 0
        metrics.last_operation = operation
        metrics.last_success_at = datetime.now(UTC)
        metrics.total_success += 1

    def record_failure(self, backend: str, operation: OperationType, error_detail: str) -> None:
        metrics = self._ensure_backend(backend)
        metrics.consecutive_failures += 1
        metrics.total_failures += 1
        metrics.last_operation = operation
        metrics.last_failure_at = datetime.now(UTC)
        metrics.last_error = error_detail
        metrics.recent_error_operation = operation
        metrics.recent_error_detail = error_detail

    def should_failover(self) -> tuple[bool, str]:
        """Return whether failover is recommended along with a reason."""
        active_metrics = self._metrics.get(self._active_backend)
        if not active_metrics:
            return False, "No health data collected yet"

        if self._active_backend == self._fallback_backend:
            return False, "Already operating on fallback backend"

        if active_metrics.consecutive_failures >= self._failure_threshold:
            return (
                True,
                f"{active_metrics.consecutive_failures} consecutive failures detected for {self._active_backend}",
            )

        return False, "Failure threshold not exceeded"

    def set_active_backend(self, backend: str) -> None:
        """Update the active backend (e.g., after manual failover)."""
        self._active_backend = backend
        self._ensure_backend(backend).consecutive_failures = 0

    @property
    def active_backend(self) -> str:
        return self._active_backend

    @property
    def fallback_backend(self) -> str:
        return self._fallback_backend

    def get_status(self) -> dict[str, object]:
        """Return structured dynamic manager status."""
        should_failover, reason = self.should_failover()
        return {
            "active_backend": self._active_backend,
            "fallback_backend": self._fallback_backend,
            "failure_threshold": self._failure_threshold,
            "should_failover": should_failover,
            "reason": reason,
            "backends": {name: metrics.to_dict() for name, metrics in self._metrics.items()},
        }
