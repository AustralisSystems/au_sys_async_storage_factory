from __future__ import annotations

"""
Graph Failover Provider.

Orchestrates failover and synchronization between a primary and secondary 
Graph Storage Provider.
"""

import asyncio
import logging
from datetime import datetime
from typing import Any, Optional

from ..interfaces.backup import IBackupProvider
from ..interfaces.base_graph_provider import IGraphProvider
from ..interfaces.health import HealthMonitor, IHealthCheck
from ..interfaces.storage import StorageError
from ..interfaces.sync import ISyncProvider, SyncConflictResolution, SyncDirection, SyncResult

logger = logging.getLogger(__name__)


class GraphFailoverProvider(IGraphProvider, ISyncProvider, IHealthCheck, IBackupProvider):
    """
    Orchestrator for failover and data synchronization between two graph providers.
    """

    def __init__(
        self,
        primary: IGraphProvider,
        secondary: IGraphProvider,
        failover_threshold: int = 3,
        failback_delay: int = 60,
    ):
        self.primary = primary
        self.secondary = secondary
        self.failover_threshold = failover_threshold
        self.failback_delay = failback_delay

        self._active_provider = primary
        self._is_failing_over = False
        self._primary_failures = 0
        self._last_primary_recovery = None
        self._health_monitor = HealthMonitor()

    async def initialize(self) -> None:
        await asyncio.gather(self.primary.initialize(), self.secondary.initialize())
        self._health_monitor.update_health(True)

    async def _handle_primary_failure(self, error: Exception) -> None:
        self._primary_failures += 1
        logger.warning(f"Primary graph provider failure: {error}")
        if self._primary_failures >= self.failover_threshold and not self._is_failing_over:
            logger.error("Failing over to secondary graph provider.")
            self._active_provider = self.secondary
            self._is_failing_over = True

    # --- IGraphProvider Implementation ---

    async def create_node(self, labels: list[str], properties: dict[str, Any]) -> str:
        try:
            return await self._active_provider.create_node(labels, properties)
        except Exception as e:
            if self._active_provider == self.primary:
                await self._handle_primary_failure(e)
                return await self.secondary.create_node(labels, properties)
            raise StorageError(f"Graph node creation failed on both: {e}")

    async def create_relationship(self, from_id: str, to_id: str, rel_type: str, properties: dict[str, Any]) -> str:
        try:
            return await self._active_provider.create_relationship(from_id, to_id, rel_type, properties)
        except Exception as e:
            if self._active_provider == self.primary:
                await self._handle_primary_failure(e)
                return await self.secondary.create_relationship(from_id, to_id, rel_type, properties)
            raise StorageError(f"Graph relationship creation failed on both: {e}")

    async def execute_query(self, cypher: str, params: Optional[dict[str, Any]] = None) -> list[dict[str, Any]]:
        try:
            return await self._active_provider.execute_query(cypher, params)
        except Exception as e:
            if self._active_provider == self.primary:
                await self._handle_primary_failure(e)
                return await self.secondary.execute_query(cypher, params)
            raise StorageError(f"Graph query execution failed on both: {e}")

    async def delete_node(self, node_id: str) -> bool:
        try:
            return await self._active_provider.delete_node(node_id)
        except Exception as e:
            if self._active_provider == self.primary:
                await self._handle_primary_failure(e)
                return await self.secondary.delete_node(node_id)
            raise StorageError(f"Graph node delete failed on both: {e}")

    # --- IStorageProvider Implementation ---
    async def get_async(self, key: str) -> Optional[Any]:
        return await self._active_provider.get_async(key)

    async def set_async(self, key: str, value: Any) -> bool:
        return await self._active_provider.set_async(key, value)

    async def delete_async(self, key: str) -> bool:
        return await self._active_provider.delete_async(key)

    async def exists_async(self, key: str) -> bool:
        return await self._active_provider.exists_async(key)

    async def list_keys_async(self, pattern: Optional[str] = None) -> list[str]:
        return await self._active_provider.list_keys_async(pattern)

    async def find_async(self, query: dict[str, Any]) -> list[Any]:
        return await self._active_provider.find_async(query)

    async def clear_async(self) -> int:
        return await self._active_provider.clear_async()

    # --- ISyncProvider Implementation ---
    async def sync_to(
        self,
        target_provider: Any,
        direction: SyncDirection = SyncDirection.TO_TARGET,
        conflict_resolution: SyncConflictResolution = SyncConflictResolution.NEWEST_WINS,
        dry_run: bool = False,
    ) -> SyncResult:
        if isinstance(self._active_provider, ISyncProvider):
            return await self._active_provider.sync_to(target_provider, direction, conflict_resolution, dry_run)

        result = SyncResult()
        result.success = False
        result.errors.append("Sync not supported.")
        return result

    def get_sync_metadata(self) -> dict[str, Any]:
        return {}

    async def prepare_for_sync(self) -> bool:
        return True

    async def cleanup_after_sync(self, sync_result: SyncResult) -> None:
        pass

    async def get_data_for_sync(self, last_sync_timestamp: Optional[datetime] = None) -> list[dict[str, Any]]:
        return []

    async def apply_sync_data(
        self,
        sync_data: list[dict[str, Any]],
        conflict_resolution: SyncConflictResolution = SyncConflictResolution.NEWEST_WINS,
    ) -> tuple[int, list[dict[str, Any]]]:
        return 0, []

    # --- IHealthCheck Implementation ---
    def is_healthy(self) -> bool:
        return self._health_monitor.is_healthy()

    async def perform_deep_health_check(self) -> bool:
        primary_ok = False
        secondary_ok = False
        try:
            primary_ok = await self.primary.perform_deep_health_check()
        except Exception as e:
            logger.warning("Primary graph deep health check failed: %s", e)
        try:
            secondary_ok = await self.secondary.perform_deep_health_check()
        except Exception as e:
            logger.warning("Secondary graph deep health check failed: %s", e)
        healthy = primary_ok or secondary_ok
        self._health_monitor.update_health(healthy)
        return healthy

    def get_health_status(self) -> dict[str, Any]:
        return self._health_monitor.get_health_status()

    def get_last_health_check(self) -> datetime:
        return self._health_monitor.get_last_health_check()

    # --- IBackupProvider Implementation ---
    async def create_backup(self, backup_path: str, metadata: Optional[dict[str, Any]] = None) -> bool:
        """Delegate backup creation to the active provider."""
        if isinstance(self._active_provider, IBackupProvider):
            return await self._active_provider.create_backup(backup_path, metadata)
        logger.error("Active graph provider does not support IBackupProvider.create_backup")
        return False

    async def restore_backup(self, backup_path: str, clear_existing: bool = False) -> bool:
        """Delegate backup restore to the active provider."""
        if isinstance(self._active_provider, IBackupProvider):
            return await self._active_provider.restore_backup(backup_path, clear_existing)
        logger.error("Active graph provider does not support IBackupProvider.restore_backup")
        return False

    async def list_backups(self, backup_dir: str) -> dict[str, dict[str, Any]]:
        """Delegate backup listing to the active provider."""
        if isinstance(self._active_provider, IBackupProvider):
            return await self._active_provider.list_backups(backup_dir)
        logger.error("Active graph provider does not support IBackupProvider.list_backups")
        return {}

    async def validate_backup(self, backup_path: str) -> dict[str, Any]:
        """Delegate backup validation to the active provider."""
        if isinstance(self._active_provider, IBackupProvider):
            return await self._active_provider.validate_backup(backup_path)
        logger.error("Active graph provider does not support IBackupProvider.validate_backup")
        return {"valid": False, "error": "Active provider does not implement IBackupProvider"}
