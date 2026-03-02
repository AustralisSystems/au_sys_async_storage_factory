from __future__ import annotations

"""
Sync Interface - Bidirectional Data Synchronization (Async First).

Defines the contract for synchronizing data between storage backends.
This interface enables seamless failover with no data loss.
"""

from abc import ABC, abstractmethod
from datetime import datetime, UTC
from enum import Enum
from typing import Any, Optional, Tuple

from .storage import IStorageProvider


class SyncDirection(Enum):
    """Direction of synchronization."""

    TO_TARGET = "to_target"
    FROM_TARGET = "from_target"
    BIDIRECTIONAL = "bidirectional"


class SyncConflictResolution(Enum):
    """How to resolve conflicts during sync."""

    SOURCE_WINS = "source_wins"
    TARGET_WINS = "target_wins"
    NEWEST_WINS = "newest_wins"
    MANUAL = "manual"


class SyncResult:
    """Result of a synchronization operation."""

    def __init__(self) -> None:
        self.success = False
        self.items_synced = 0
        self.conflicts_found = 0
        self.conflicts_resolved = 0
        self.errors: list[str] = []
        self.sync_duration_seconds = 0.0
        self.sync_direction: Optional[SyncDirection] = None
        self.timestamp = datetime.now(UTC)
        self.source_count = 0
        self.target_count = 0
        self.details: dict[str, Any] = {}

    def to_dict(self) -> dict[str, Any]:
        return {
            "success": self.success,
            "items_synced": self.items_synced,
            "conflicts_found": self.conflicts_found,
            "conflicts_resolved": self.conflicts_resolved,
            "errors": self.errors,
            "sync_duration_seconds": self.sync_duration_seconds,
            "sync_direction": self.sync_direction.value if self.sync_direction else None,
            "timestamp": self.timestamp.isoformat(),
            "source_count": self.source_count,
            "target_count": self.target_count,
            "details": self.details,
        }


class ISyncProvider(IStorageProvider, ABC):
    """
    Interface for bidirectional data synchronization (Async First).
    """

    @abstractmethod
    async def sync_to(
        self,
        target_provider: IStorageProvider,
        direction: SyncDirection = SyncDirection.TO_TARGET,
        conflict_resolution: SyncConflictResolution = SyncConflictResolution.NEWEST_WINS,
        dry_run: bool = False,
    ) -> SyncResult:
        """Synchronize data with another storage provider asynchronously."""

    @abstractmethod
    def get_sync_metadata(self) -> dict[str, Any]:
        """Get metadata about sync capabilities."""

    @abstractmethod
    async def prepare_for_sync(self) -> bool:
        """Prepare for sync asynchronously."""

    @abstractmethod
    async def cleanup_after_sync(self, sync_result: SyncResult) -> None:
        """Cleanup after sync asynchronously."""

    @abstractmethod
    async def get_data_for_sync(self, last_sync_timestamp: Optional[datetime] = None) -> list[dict[str, Any]]:
        """Get data items for sync asynchronously."""

    @abstractmethod
    async def apply_sync_data(
        self,
        sync_data: list[dict[str, Any]],
        conflict_resolution: SyncConflictResolution = SyncConflictResolution.NEWEST_WINS,
    ) -> Tuple[int, list[dict[str, Any]]]:
        """Apply sync data asynchronously."""
