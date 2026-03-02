from __future__ import annotations

"""
Document Failover Provider.

Orchestrates failover and synchronization between a primary and secondary 
Document Storage Provider to ensure zero data loss and high availability.
"""

import asyncio
import logging
from datetime import UTC, datetime
from typing import Any, Optional, TypeVar

from .interfaces.backup import IBackupProvider
from .interfaces.base_document_provider import IDocumentProvider
from .interfaces.health import HealthMonitor, IHealthCheck
from .interfaces.storage import StorageError
from .interfaces.sync import ISyncProvider, SyncConflictResolution, SyncDirection, SyncResult

logger = logging.getLogger(__name__)

T = TypeVar("T")


class DocumentFailoverProvider(IDocumentProvider, ISyncProvider, IHealthCheck, IBackupProvider):
    """
    Orchestrator for failover and data synchronization between two document providers.

    Attributes:
        primary: The primary storage provider (e.g., Remote MongoDB)
        secondary: The secondary/failover storage provider (e.g., Local Beanie/SQLite)
        failover_threshold: Number of failures before switching to secondary
        failback_delay: Time in seconds to wait before attempting failback after recovery
    """

    def __init__(
        self,
        primary: IDocumentProvider,
        secondary: IDocumentProvider,
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

        # Sync state
        self._pending_sync = False
        self._last_sync_timestamp: Optional[datetime] = None

    async def initialize(self, document_models: Optional[list[type[Any]]] = None) -> None:
        """Initialize both providers."""
        await asyncio.gather(self.primary.initialize(document_models), self.secondary.initialize(document_models))
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
            # Simple existence check to see if primary is alive
            if await self.primary.exists_async("__health_check__"):
                self._primary_failures = 0
                if self._last_primary_recovery is None:
                    self._last_primary_recovery = datetime.now(UTC)
                    logger.info("Primary provider seems recovered. Monitoring for failback.")

                # Check if delay has passed
                if self._last_primary_recovery:
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
            logger.warning("Secondary provider does not implement ISyncProvider. Manual sync required.")

    # --- IDocumentProvider Implementation ---

    async def insert_one(self, document: Any) -> Any:
        await self._check_primary_recovery()
        try:
            return await self._active_provider.insert_one(document)
        except Exception as e:
            if self._active_provider == self.primary:
                await self._handle_primary_failure(e)
                return await self.secondary.insert_one(document)
            raise StorageError(f"InsertOne failed on both providers: {e}")

    async def insert_many(self, documents: list[Any]) -> list[Any]:
        await self._check_primary_recovery()
        try:
            return await self._active_provider.insert_many(documents)
        except Exception as e:
            if self._active_provider == self.primary:
                await self._handle_primary_failure(e)
                return await self.secondary.insert_many(documents)
            raise StorageError(f"InsertMany failed on both providers: {e}")

    async def find_one(self, model_class: type[T], query: dict[str, Any]) -> Optional[T]:
        await self._check_primary_recovery()
        try:
            return await self._active_provider.find_one(model_class, query)
        except Exception as e:
            if self._active_provider == self.primary:
                await self._handle_primary_failure(e)
                return await self.secondary.find_one(model_class, query)
            raise StorageError(f"FindOne failed on both providers: {e}")

    async def find_many(
        self,
        model_class: type[T],
        query: dict[str, Any],
        limit: int = 0,
        skip: int = 0,
        sort: Optional[Any] = None,
    ) -> list[T]:
        await self._check_primary_recovery()
        try:
            return await self._active_provider.find_many(model_class, query, limit, skip, sort)
        except Exception as e:
            if self._active_provider == self.primary:
                await self._handle_primary_failure(e)
                return await self.secondary.find_many(model_class, query, limit, skip, sort)
            raise StorageError(f"FindMany failed on both providers: {e}")

    async def delete_one(self, document: Any) -> bool:
        await self._check_primary_recovery()
        try:
            return await self._active_provider.delete_one(document)
        except Exception as e:
            if self._active_provider == self.primary:
                await self._handle_primary_failure(e)
                return await self.secondary.delete_one(document)
            raise StorageError(f"DeleteOne failed on both providers: {e}")

    async def delete_many(self, model_class: type[Any], query: dict[str, Any]) -> int:
        await self._check_primary_recovery()
        try:
            return await self._active_provider.delete_many(model_class, query)
        except Exception as e:
            if self._active_provider == self.primary:
                await self._handle_primary_failure(e)
                return await self.secondary.delete_many(model_class, query)
            raise StorageError(f"DeleteMany failed on both providers: {e}")

    async def update_one(self, document: Any, update_query: dict[str, Any]) -> Any:
        await self._check_primary_recovery()
        try:
            return await self._active_provider.update_one(document, update_query)
        except Exception as e:
            if self._active_provider == self.primary:
                await self._handle_primary_failure(e)
                return await self.secondary.update_one(document, update_query)
            raise StorageError(f"UpdateOne failed on both providers: {e}")

    # --- IStorageProvider (KV) Implementation (Async) ---

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
            return await self._active_provider.set_async(key, value)
        except Exception as e:
            if self._active_provider == self.primary:
                await self._handle_primary_failure(e)
                return await self.secondary.set_async(key, value)
            raise StorageError(f"Set failed on both providers: {e}")

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

    # --- IStorageProvider (KV) Implementation (Sync - Not Supported) ---

    def get(self, key: str) -> Optional[Any]:
        raise NotImplementedError("Use get_async")

    def set(self, key: str, value: Any) -> bool:
        raise NotImplementedError("Use set_async")

    def delete(self, key: str) -> bool:
        raise NotImplementedError("Use delete_async")

    def exists(self, key: str) -> bool:
        raise NotImplementedError("Use exists_async")

    def list_keys(self, pattern: Optional[str] = None) -> list[str]:
        raise NotImplementedError("Use list_keys_async")

    def find(self, query: dict[str, Any]) -> list[Any]:
        raise NotImplementedError("Use find_async")

    def clear(self) -> int:
        raise NotImplementedError("Use clear_async")

    def supports_ttl(self) -> bool:
        return self._active_provider.supports_ttl()

    def set_with_ttl(self, key: str, value: Any, ttl: int) -> bool:
        raise NotImplementedError("Use set_with_ttl_async")

    # --- ISyncProvider Implementation ---

    async def sync_to(
        self,
        target_provider: Any,
        direction: SyncDirection = SyncDirection.TO_TARGET,
        conflict_resolution: SyncConflictResolution = SyncConflictResolution.NEWEST_WINS,
        dry_run: bool = False,
    ) -> SyncResult:
        if target_provider == self.primary and isinstance(self.secondary, ISyncProvider):
            return await self.secondary.sync_to(self.primary, direction, conflict_resolution, dry_run)

        if isinstance(self._active_provider, ISyncProvider):
            return await self._active_provider.sync_to(target_provider, direction, conflict_resolution, dry_run)

        raise NotImplementedError("Underlying provider does not support sync.")

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

    # --- IBackupProvider Implementation ---

    async def create_backup(self, backup_path: str, metadata: Optional[dict[str, Any]] = None) -> bool:
        """Delegate backup creation to the primary provider if it supports backups."""
        if isinstance(self.primary, IBackupProvider):
            return await self.primary.create_backup(backup_path, metadata)
        logger.warning("Primary provider does not implement IBackupProvider; backup skipped.")
        return False

    async def restore_backup(self, backup_path: str, clear_existing: bool = False) -> bool:
        """Delegate backup restoration to the primary provider if it supports backups."""
        if isinstance(self.primary, IBackupProvider):
            return await self.primary.restore_backup(backup_path, clear_existing)
        logger.warning("Primary provider does not implement IBackupProvider; restore skipped.")
        return False

    async def list_backups(self, backup_dir: str) -> dict[str, dict[str, Any]]:
        """Delegate backup listing to the primary provider if it supports backups."""
        if isinstance(self.primary, IBackupProvider):
            return await self.primary.list_backups(backup_dir)
        logger.warning("Primary provider does not implement IBackupProvider; listing skipped.")
        return {}

    async def validate_backup(self, backup_path: str) -> dict[str, Any]:
        """Delegate backup validation to the primary provider if it supports backups."""
        if isinstance(self.primary, IBackupProvider):
            return await self.primary.validate_backup(backup_path)
        logger.warning("Primary provider does not implement IBackupProvider; validation skipped.")
        return {"valid": False, "error": "Primary provider does not support backups"}
