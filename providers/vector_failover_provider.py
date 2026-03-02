from __future__ import annotations

"""
Vector Failover Provider.

Orchestrates failover and synchronization between a primary and secondary 
Vector Storage Provider.
"""

import asyncio
import logging
from datetime import datetime, UTC
from typing import Any, Dict, List, Optional, Union, Tuple

from ..interfaces.base_vector_provider import IVectorProvider
from ..interfaces.sync import ISyncProvider, SyncResult, SyncDirection, SyncConflictResolution
from ..interfaces.health import IHealthCheck, HealthMonitor
from ..interfaces.storage import StorageError

logger = logging.getLogger(__name__)


class VectorFailoverProvider(IVectorProvider, ISyncProvider, IHealthCheck):
    """
    Orchestrator for failover and data synchronization between two vector providers.
    """

    def __init__(
        self,
        primary: IVectorProvider,
        secondary: IVectorProvider,
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
        self._last_primary_recovery: Optional[datetime] = None
        self._health_monitor = HealthMonitor()

    async def initialize(self) -> None:
        await asyncio.gather(self.primary.initialize(), self.secondary.initialize())
        self._health_monitor.update_health(True)

    async def _handle_primary_failure(self, error: Exception) -> None:
        self._primary_failures += 1
        logger.warning(f"Primary vector provider failure: {error}")
        if self._primary_failures >= self.failover_threshold and not self._is_failing_over:
            logger.error("Failing over to secondary vector provider.")
            self._active_provider = self.secondary
            self._is_failing_over = True

    # --- IVectorProvider Implementation ---

    async def create_index(self, name: str, dimension: int, distance_metric: str = "cosine", **kwargs: Any) -> bool:
        try:
            return await self._active_provider.create_index(name, dimension, distance_metric, **kwargs)
        except Exception as e:
            if self._active_provider == self.primary:
                await self._handle_primary_failure(e)
                return await self.secondary.create_index(name, dimension, distance_metric, **kwargs)
            raise StorageError(f"Vector index creation failed on both: {e}")

    async def upsert(
        self,
        index_name: str,
        vectors: list[list[float]],
        metadata: list[dict[str, Any]],
        ids: Optional[list[str]] = None,
    ) -> bool:
        try:
            return await self._active_provider.upsert(index_name, vectors, metadata, ids)
        except Exception as e:
            if self._active_provider == self.primary:
                await self._handle_primary_failure(e)
                return await self.secondary.upsert(index_name, vectors, metadata, ids)
            raise StorageError(f"Vector upsert failed on both: {e}")

    async def search(
        self,
        index_name: str,
        query_vector: list[float],
        limit: int = 10,
        filters: Optional[dict[str, Any]] = None,
    ) -> list[dict[str, Any]]:
        try:
            return await self._active_provider.search(index_name, query_vector, limit, filters)
        except Exception as e:
            if self._active_provider == self.primary:
                await self._handle_primary_failure(e)
                return await self.secondary.search(index_name, query_vector, limit, filters)
            raise StorageError(f"Vector search failed on both: {e}")

    async def delete(self, index_name: str, ids: list[str]) -> bool:
        try:
            return await self._active_provider.delete(index_name, ids)
        except Exception as e:
            if self._active_provider == self.primary:
                await self._handle_primary_failure(e)
                return await self.secondary.delete(index_name, ids)
            raise StorageError(f"Vector delete failed on both: {e}")

    async def get_index_stats(self, index_name: str) -> dict[str, Any]:
        try:
            return await self._active_provider.get_index_stats(index_name)
        except Exception as e:
            if self._active_provider == self.primary:
                await self._handle_primary_failure(e)
                return await self.secondary.get_index_stats(index_name)
            raise StorageError(f"Vector get_index_stats failed on both: {e}")

    async def drop_index(self, index_name: str) -> bool:
        try:
            return await self._active_provider.drop_index(index_name)
        except Exception as e:
            if self._active_provider == self.primary:
                await self._handle_primary_failure(e)
                return await self.secondary.drop_index(index_name)
            raise StorageError(f"Vector drop_index failed on both: {e}")

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
        raise NotImplementedError("Sync not supported.")

    def get_sync_metadata(self) -> dict[str, Any]:
        if isinstance(self._active_provider, ISyncProvider):
            return self._active_provider.get_sync_metadata()
        return {}

    async def prepare_for_sync(self) -> bool:
        if isinstance(self._active_provider, ISyncProvider):
            return await self._active_provider.prepare_for_sync()
        return True

    async def cleanup_after_sync(self, sync_result: SyncResult) -> None:
        if isinstance(self._active_provider, ISyncProvider):
            await self._active_provider.cleanup_after_sync(sync_result)

    async def get_data_for_sync(self, last_sync_timestamp: Optional[datetime] = None) -> list[dict[str, Any]]:
        if isinstance(self._active_provider, ISyncProvider):
            return await self._active_provider.get_data_for_sync(last_sync_timestamp)
        return []

    async def apply_sync_data(
        self,
        sync_data: list[dict[str, Any]],
        conflict_resolution: SyncConflictResolution = SyncConflictResolution.NEWEST_WINS,
    ) -> tuple[int, list[dict[str, Any]]]:
        if isinstance(self._active_provider, ISyncProvider):
            return await self._active_provider.apply_sync_data(sync_data, conflict_resolution)
        return 0, []

    # --- IHealthCheck Implementation ---
    def is_healthy(self) -> bool:
        return self._health_monitor.is_healthy()

    async def perform_deep_health_check(self) -> bool:
        p_health = False
        try:
            p_health = await self.primary.perform_deep_health_check()
        except Exception as exc:
            logger.warning("Primary vector health check failed: %s", exc)

        s_health = False
        try:
            s_health = await self.secondary.perform_deep_health_check()
        except Exception as exc:
            logger.warning("Secondary vector health check failed: %s", exc)

        healthy = p_health or s_health
        self._health_monitor.update_health(healthy)
        return healthy

    def get_health_status(self) -> dict[str, Any]:
        return self._health_monitor.get_health_status()

    def get_last_health_check(self) -> datetime:
        return self._health_monitor.get_last_health_check()

    # --- IBackupProvider Implementation ---
    async def create_backup(self, backup_path: str, metadata: Optional[dict[str, Any]] = None) -> bool:
        return await self._active_provider.create_backup(backup_path, metadata)

    async def restore_backup(self, backup_path: str, clear_existing: bool = False) -> bool:
        return await self._active_provider.restore_backup(backup_path, clear_existing)

    async def list_backups(self, backup_dir: str) -> dict[str, dict[str, Any]]:
        return await self._active_provider.list_backups(backup_dir)

    async def validate_backup(self, backup_path: str) -> dict[str, Any]:
        return await self._active_provider.validate_backup(backup_path)
