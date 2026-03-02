from __future__ import annotations

"""
Graph Failover Provider.

Orchestrates failover and synchronization between a primary and secondary 
Graph Storage Provider.
"""

import asyncio
import logging
from datetime import datetime, UTC
from typing import Any, Dict, List, Optional, Union, Tuple

from ..interfaces.base_graph_provider import IGraphProvider
from ..interfaces.sync import ISyncProvider, SyncResult, SyncDirection, SyncConflictResolution
from ..interfaces.health import IHealthCheck, HealthMonitor
from ..interfaces.storage import StorageError

logger = logging.getLogger(__name__)


class GraphFailoverProvider(IGraphProvider, ISyncProvider, IHealthCheck):
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
        raise NotImplementedError("Sync not supported.")

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
        return True

    def get_health_status(self) -> dict[str, Any]:
        return self._health_monitor.get_health_status()

    def get_last_health_check(self) -> datetime:
        return self._health_monitor.get_last_health_check()
