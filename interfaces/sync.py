"""
Sync Interface - Bidirectional Data Synchronization.

Defines the contract for synchronizing data between storage backends.
This interface enables seamless failover with no data loss.
"""

from abc import ABC, abstractmethod
from datetime import datetime, timezone, UTC
from enum import Enum
from typing import Any, Optional

from .storage import IStorageProvider


class SyncDirection(Enum):
    """Direction of synchronization."""

    TO_TARGET = "to_target"  # Sync from source to target
    FROM_TARGET = "from_target"  # Sync from target to source
    BIDIRECTIONAL = "bidirectional"  # Full bidirectional sync


class SyncConflictResolution(Enum):
    """How to resolve conflicts during sync."""

    SOURCE_WINS = "source_wins"  # Source data overwrites target
    TARGET_WINS = "target_wins"  # Target data overwrites source
    NEWEST_WINS = "newest_wins"  # Most recently updated data wins
    MANUAL = "manual"  # Require manual resolution


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
        """Convert sync result to dictionary."""
        return {
            "success": self.success,
            "items_synced": self.items_synced,
            "conflicts_found": self.conflicts_found,
            "conflicts_resolved": self.conflicts_resolved,
            "errors": self.errors,
            "sync_duration_seconds": self.sync_duration_seconds,
            "sync_direction": (self.sync_direction.value if self.sync_direction else None),
            "timestamp": self.timestamp.isoformat(),
            "source_count": self.source_count,
            "target_count": self.target_count,
            "details": self.details,
        }


class ISyncProvider(IStorageProvider, ABC):
    """
    Interface for bidirectional data synchronization between storage backends.

    This interface follows the Single Responsibility Principle by focusing
    solely on data synchronization operations.
    """

    @abstractmethod
    def sync_to(
        self,
        target_provider: "IStorageProvider",
        direction: SyncDirection = SyncDirection.TO_TARGET,
        conflict_resolution: SyncConflictResolution = SyncConflictResolution.NEWEST_WINS,
        dry_run: bool = False,
    ) -> SyncResult:
        """
        Synchronize data with another storage provider.

        Args:
            target_provider: The target storage provider to sync with
            direction: Direction of synchronization
            conflict_resolution: How to resolve data conflicts
            dry_run: If True, only simulate the sync without making changes

        Returns:
            SyncResult with detailed information about the sync operation
        """

    @abstractmethod
    def get_sync_metadata(self) -> dict[str, Any]:
        """
        Get metadata about this provider's sync capabilities and state.

        Returns:
            Dictionary containing sync metadata
        """

    @abstractmethod
    def prepare_for_sync(self) -> bool:
        """
        Prepare the storage provider for synchronization.

        This might include creating snapshots, locking resources, etc.

        Returns:
            True if preparation was successful
        """

    @abstractmethod
    def cleanup_after_sync(self, sync_result: SyncResult) -> None:
        """
        Cleanup after synchronization operation.

        This might include removing temporary data, unlocking resources, etc.

        Args:
            sync_result: The result of the sync operation
        """

    @abstractmethod
    def get_data_for_sync(self, last_sync_timestamp: Optional[datetime] = None) -> list[dict[str, Any]]:
        """
        Get all data that needs to be synchronized.

        Args:
            last_sync_timestamp: Optional timestamp to get only changes since then

        Returns:
            List of data items with their metadata
        """

    @abstractmethod
    def apply_sync_data(
        self,
        sync_data: list[dict[str, Any]],
        conflict_resolution: SyncConflictResolution = SyncConflictResolution.NEWEST_WINS,
    ) -> tuple[int, list[dict[str, Any]]]:
        """
        Apply synchronized data to this storage provider.

        Args:
            sync_data: List of data items to apply
            conflict_resolution: How to resolve conflicts

        Returns:
            Tuple of (items_applied, conflicts_found)
        """


class SyncManager:
    """
    Manager for coordinating synchronization between storage providers.

    This provides common sync functionality that can be used with any
    storage providers that implement ISyncProvider.
    """

    def __init__(self) -> None:
        """Initialize sync manager."""
        self.sync_history = []
        self.active_syncs = {}

    def sync_providers(
        self,
        source_provider: ISyncProvider,
        target_provider: ISyncProvider,
        direction: SyncDirection = SyncDirection.BIDIRECTIONAL,
        conflict_resolution: SyncConflictResolution = SyncConflictResolution.NEWEST_WINS,
        dry_run: bool = False,
    ) -> SyncResult:
        """
        Synchronize data between two storage providers.

        Args:
            source_provider: Source storage provider
            target_provider: Target storage provider
            direction: Direction of synchronization
            conflict_resolution: How to resolve conflicts
            dry_run: If True, only simulate the sync

        Returns:
            SyncResult with detailed sync information
        """
        sync_id = f"sync_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}"

        try:
            # Record active sync
            self.active_syncs[sync_id] = {
                "started_at": datetime.now(UTC),
                "direction": direction,
                "dry_run": dry_run,
            }

            # Prepare both providers
            source_ready = source_provider.prepare_for_sync()
            target_ready = target_provider.prepare_for_sync()

            if not source_ready or not target_ready:
                result = SyncResult()
                result.success = False
                result.errors.append("Failed to prepare providers for sync")
                return result

            # Perform the sync
            if direction == SyncDirection.BIDIRECTIONAL:
                # Perform bidirectional sync
                result = self._sync_bidirectional(source_provider, target_provider, conflict_resolution, dry_run)
            else:
                # Perform unidirectional sync
                result = source_provider.sync_to(target_provider, direction, conflict_resolution, dry_run)

            # Cleanup
            source_provider.cleanup_after_sync(result)
            target_provider.cleanup_after_sync(result)

            # Record sync in history
            self.sync_history.append(
                {
                    "sync_id": sync_id,
                    "result": result.to_dict(),
                    "completed_at": datetime.now(UTC),
                }
            )

            return result

        except Exception as e:
            result = SyncResult()
            result.success = False
            result.errors.append(f"Sync failed: {str(e)}")
            return result

        finally:
            # Remove from active syncs
            self.active_syncs.pop(sync_id, None)

    def _sync_bidirectional(
        self,
        provider_a: ISyncProvider,
        provider_b: ISyncProvider,
        conflict_resolution: SyncConflictResolution,
        dry_run: bool,
    ) -> SyncResult:
        """Perform bidirectional synchronization."""
        result = SyncResult()
        result.sync_direction = SyncDirection.BIDIRECTIONAL
        start_time = datetime.now(UTC)

        try:
            # Get data from both providers
            data_a = provider_a.get_data_for_sync()
            data_b = provider_b.get_data_for_sync()

            result.source_count = len(data_a)
            result.target_count = len(data_b)

            # Apply data from A to B
            if not dry_run:
                items_to_b, conflicts_b = provider_b.apply_sync_data(data_a, conflict_resolution)
                result.items_synced += items_to_b
                result.conflicts_found += len(conflicts_b)

            # Apply data from B to A
            if not dry_run:
                items_to_a, conflicts_a = provider_a.apply_sync_data(data_b, conflict_resolution)
                result.items_synced += items_to_a
                result.conflicts_found += len(conflicts_a)

            result.success = True

        except Exception as e:
            result.errors.append(f"Bidirectional sync error: {str(e)}")

        finally:
            end_time = datetime.now(UTC)
            result.sync_duration_seconds = (end_time - start_time).total_seconds()

        return result

    def get_sync_history(self, limit: int = 10) -> list[dict[str, Any]]:
        """Get recent sync history."""
        return self.sync_history[-limit:]

    def get_active_syncs(self) -> dict[str, Any]:
        """Get currently active sync operations."""
        return self.active_syncs.copy()
