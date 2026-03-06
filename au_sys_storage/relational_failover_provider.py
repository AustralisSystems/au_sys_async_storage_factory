from __future__ import annotations

"""
Relational Failover Provider.

Orchestrates failover and synchronization between a primary and secondary 
Relational Storage Provider to ensure zero data loss and high availability.
"""

import asyncio
import logging
from datetime import UTC, datetime
from typing import Any, Optional

from .interfaces.base_relational_provider import IRelationalProvider
from .interfaces.health import HealthMonitor
from .interfaces.storage import IStorageProvider, StorageError
from .interfaces.sync import ISyncProvider, SyncConflictResolution, SyncDirection, SyncResult

logger = logging.getLogger("storage.failover")


class RelationalFailoverProvider(IRelationalProvider, ISyncProvider):
    """
    Orchestrator for failover and data synchronization between two relational providers.
    """

    def __init__(
        self,
        primary: IRelationalProvider,
        secondary: IRelationalProvider,
        failover_threshold: int = 3,
        failback_delay: int = 60,
    ):
        self.primary = primary
        self.secondary = secondary
        self.failover_threshold = failover_threshold
        self.failback_delay = failback_delay

        self._active_provider: IRelationalProvider = primary
        self._is_failing_over = False
        self._primary_failures = 0
        self._last_primary_recovery: Optional[datetime] = None
        self._health_monitor = HealthMonitor()

        self._pending_sync = False
        self._last_sync_timestamp: Optional[datetime] = None

    async def initialize(self) -> None:
        """Initialize both providers."""
        await asyncio.gather(self.primary.initialize(), self.secondary.initialize())
        self._health_monitor.update_health(True)

    async def _handle_primary_failure(self, error: Exception) -> None:
        """Handle a failure in the primary provider."""
        self._primary_failures += 1
        logger.warning(f"Primary provider failure ({self._primary_failures}/{self.failover_threshold}): {error}")

        if self._primary_failures >= self.failover_threshold and not self._is_failing_over:
            logger.error("Primary threshold reached. Failing over to secondary.")
            self._active_provider = self.secondary
            self._is_failing_over = True
            self._pending_sync = True

    async def _check_primary_recovery(self) -> None:
        """Check if primary has recovered and handle failback."""
        if not self._is_failing_over:
            return

        try:
            if await self.primary.exists_async("__health_probe__"):
                self._primary_failures = 0
                if self._last_primary_recovery is None:
                    self._last_primary_recovery = datetime.now(UTC)
                    logger.info("Primary provider seems recovered. Monitoring for failback.")

                elapsed = (datetime.now(UTC) - self._last_primary_recovery).total_seconds()
                if elapsed >= self.failback_delay:
                    logger.info("Failback delay reached. Performing sync and failback.")
                    await self._perform_failback_sync()
                    self._active_provider = self.primary
                    self._is_failing_over = False
                    self._last_primary_recovery = None
            else:
                self._last_primary_recovery = None
        except Exception:
            self._last_primary_recovery = None

    async def _perform_failback_sync(self) -> None:
        """Synchronize data from secondary back to primary before failing back."""
        logger.info("Synchronizing data from secondary to primary...")
        if isinstance(self.secondary, ISyncProvider):
            await self.secondary.sync_to(self.primary, SyncDirection.TO_TARGET)
        else:
            logger.warning("Secondary provider does not implement ISyncProvider. Manual sync skipped.")

    # --- IRelationalProvider Implementation ---

    async def execute_raw_sql(self, sql: str, params: Optional[dict[str, Any]] = None) -> Any:
        try:
            return await self._active_provider.execute_raw_sql(sql, params)
        except Exception as e:
            if self._active_provider == self.primary:
                await self._handle_primary_failure(e)
                return await self.secondary.execute_raw_sql(sql, params)
            raise StorageError(f"Secondary provider also failed: {e}")

    # --- IStorageProvider Implementation (KV) ---

    async def get_async(self, key: str) -> Optional[Any]:
        await self._check_primary_recovery()
        try:
            return await self._active_provider.get_async(key)
        except Exception as e:
            if self._active_provider == self.primary:
                await self._handle_primary_failure(e)
                return await self.secondary.get_async(key)
            raise StorageError(f"Get failed on both providers: {e}")

    async def set_async(self, key: str, value: Any) -> bool:
        await self._check_primary_recovery()
        try:
            success = await self._active_provider.set_async(key, value)
            if not success and self._active_provider == self.primary:
                await self._handle_primary_failure(Exception("Primary set returned False"))
                return await self.secondary.set_async(key, value)
            return success
        except Exception as e:
            if self._active_provider == self.primary:
                await self._handle_primary_failure(e)
                return await self.secondary.set_async(key, value)
            raise StorageError(f"Set failed on both providers: {e}")

    async def set_with_ttl_async(self, key: str, value: Any, ttl: int) -> bool:
        await self._check_primary_recovery()
        try:
            return await self._active_provider.set_with_ttl_async(key, value, ttl)
        except Exception as e:
            if self._active_provider == self.primary:
                await self._handle_primary_failure(e)
                return await self.secondary.set_with_ttl_async(key, value, ttl)
            raise StorageError(f"SetWithTTL failed on both providers: {e}")

    async def delete_async(self, key: str) -> bool:
        await self._check_primary_recovery()
        try:
            return await self._active_provider.delete_async(key)
        except Exception as e:
            if self._active_provider == self.primary:
                await self._handle_primary_failure(e)
                return await self.secondary.delete_async(key)
            raise StorageError(f"Delete failed on both providers: {e}")

    async def exists_async(self, key: str) -> bool:
        await self._check_primary_recovery()
        try:
            return await self._active_provider.exists_async(key)
        except Exception as e:
            if self._active_provider == self.primary:
                await self._handle_primary_failure(e)
                return await self.secondary.exists_async(key)
            raise StorageError(f"Exists failed on both providers: {e}")

    async def list_keys_async(self, pattern: Optional[str] = None) -> list[str]:
        await self._check_primary_recovery()
        try:
            return await self._active_provider.list_keys_async(pattern)
        except Exception as e:
            if self._active_provider == self.primary:
                await self._handle_primary_failure(e)
                return await self.secondary.list_keys_async(pattern)
            raise StorageError(f"ListKeys failed on both providers: {e}")

    async def find_async(self, query: dict[str, Any]) -> list[Any]:
        await self._check_primary_recovery()
        try:
            return await self._active_provider.find_async(query)
        except Exception as e:
            if self._active_provider == self.primary:
                await self._handle_primary_failure(e)
                return await self.secondary.find_async(query)
            raise StorageError(f"Find failed on both providers: {e}")

    async def clear_async(self) -> int:
        await self._check_primary_recovery()
        try:
            return await self._active_provider.clear_async()
        except Exception as e:
            if self._active_provider == self.primary:
                await self._handle_primary_failure(e)
                return await self.secondary.clear_async()
            raise StorageError(f"Clear failed on both providers: {e}")

    # --- ISyncProvider Implementation ---

    async def sync_to(
        self,
        target_provider: IStorageProvider,
        direction: SyncDirection = SyncDirection.TO_TARGET,
        conflict_resolution: SyncConflictResolution = SyncConflictResolution.NEWEST_WINS,
        dry_run: bool = False,
    ) -> SyncResult:
        if target_provider == self.primary:
            if isinstance(self.secondary, ISyncProvider):
                return await self.secondary.sync_to(self.primary, direction, conflict_resolution, dry_run)

        if isinstance(self._active_provider, ISyncProvider):
            return await self._active_provider.sync_to(target_provider, direction, conflict_resolution, dry_run)

        result = SyncResult()
        result.success = False
        result.errors.append("Underlying provider does not support sync.")
        return result

    def get_sync_metadata(self) -> dict[str, Any]:
        return {
            "is_failing_over": self._is_failing_over,
            "pending_sync": self._pending_sync,
            "active_provider": "primary" if self._active_provider == self.primary else "secondary",
        }

    async def prepare_for_sync(self) -> bool:
        return True

    async def cleanup_after_sync(self, sync_result: SyncResult) -> None:
        self._pending_sync = False
        self._last_sync_timestamp = datetime.now(UTC)

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
            logger.warning("Primary deep health check failed: %s", exc)

        s_health = False
        try:
            s_health = await self.secondary.perform_deep_health_check()
        except Exception as exc:
            logger.warning("Secondary deep health check failed: %s", exc)

        healthy = p_health or s_health
        self._health_monitor.update_health(healthy)
        return healthy

    def get_health_status(self) -> dict[str, Any]:
        status = self._health_monitor.get_health_status()
        status.update(
            {
                "active_provider": "primary" if self._active_provider == self.primary else "secondary",
                "is_failing_over": self._is_failing_over,
                "primary_failures": self._primary_failures,
            }
        )
        return status

    def get_last_health_check(self) -> datetime:
        return self._health_monitor.get_last_health_check()

    def supports_ttl(self) -> bool:
        return self._active_provider.supports_ttl()

    # --- IBackupProvider Implementation ---

    async def create_backup(self, backup_path: str, metadata: Optional[dict[str, Any]] = None) -> bool:
        return await self._active_provider.create_backup(backup_path, metadata)

    async def restore_backup(self, backup_path: str, clear_existing: bool = False) -> bool:
        return await self._active_provider.restore_backup(backup_path, clear_existing)

    async def list_backups(self, backup_dir: str) -> dict[str, dict[str, Any]]:
        return await self._active_provider.list_backups(backup_dir)

    async def validate_backup(self, backup_path: str) -> dict[str, Any]:
        return await self._active_provider.validate_backup(backup_path)
