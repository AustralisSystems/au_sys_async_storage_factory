from __future__ import annotations

"""
Blob Failover Provider.

Orchestrates failover and synchronization between a primary and secondary 
Blob Storage Provider to ensure data availability and tier-syncing.
"""

import asyncio
import logging
from datetime import datetime, UTC
from typing import Any, AsyncIterator, Optional, Union

from ..interfaces.base_blob_provider import BaseBlobProvider
from ..interfaces.sync import ISyncProvider, SyncResult, SyncDirection, SyncConflictResolution
from ..interfaces.health import IHealthCheck, HealthMonitor

logger = logging.getLogger("storage.blob.failover")


class BlobFailoverProvider(BaseBlobProvider, ISyncProvider, IHealthCheck):
    """
    Orchestrator for failover and data synchronization between two blob providers.
    """

    def __init__(
        self,
        primary: BaseBlobProvider,
        secondary: BaseBlobProvider,
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

    async def _handle_primary_failure(self, error: Exception) -> None:
        """Handle a failure in the primary provider."""
        self._primary_failures += 1
        logger.warning(f"Primary blob provider failure ({self._primary_failures}/{self.failover_threshold}): {error}")

        if self._primary_failures >= self.failover_threshold and not self._is_failing_over:
            logger.error("Primary blob threshold reached. Failing over to secondary.")
            self._active_provider = self.secondary
            self._is_failing_over = True
            self._pending_sync = True

    async def _check_primary_recovery(self) -> None:
        """Check if primary has recovered and handle failback."""
        if not self._is_failing_over:
            return

        try:
            # Simple check for recovery
            if await self.primary.exists("__health_check__"):
                self._primary_failures = 0
                if self._last_primary_recovery is None:
                    self._last_primary_recovery = datetime.now(UTC)
                    logger.info("Primary blob provider seems recovered. Monitoring for failback.")

                # Check if delay has passed
                elapsed = (datetime.now(UTC) - self._last_primary_recovery).total_seconds()
                if elapsed >= self.failback_delay:
                    logger.info("Failback delay reached for blob. Performing sync and failback.")
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
        logger.info("Synchronizing blobs from secondary to primary...")
        if isinstance(self.secondary, ISyncProvider):
            await self.secondary.sync_to(self.primary, SyncDirection.TO_TARGET)
        else:
            logger.warning("Secondary blob provider does not implement ISyncProvider. Manual sync required.")

    # --- BaseBlobProvider Implementation ---

    async def upload(
        self,
        key: str,
        data: Union[bytes, str],
        content_type: Optional[str] = None,
        encryption_context: Optional[dict[str, str]] = None,
    ) -> bool:
        await self._check_primary_recovery()
        try:
            success = await self._active_provider.upload(key, data, content_type, encryption_context)
            if not success and self._active_provider == self.primary:
                await self._handle_primary_failure(Exception("Primary upload returned False"))
                return await self.secondary.upload(key, data, content_type, encryption_context)
            return success
        except Exception as e:
            if self._active_provider == self.primary:
                await self._handle_primary_failure(e)
                return await self.secondary.upload(key, data, content_type, encryption_context)
            raise e

    async def download(self, key: str, decryption_context: Optional[dict[str, str]] = None) -> Optional[bytes]:
        await self._check_primary_recovery()
        try:
            return await self._active_provider.download(key, decryption_context)
        except Exception as e:
            if self._active_provider == self.primary:
                await self._handle_primary_failure(e)
                return await self.secondary.download(key, decryption_context)
            raise e

    async def delete(self, key: str) -> bool:
        await self._check_primary_recovery()
        try:
            success = await self._active_provider.delete(key)
            if not success and self._active_provider == self.primary:
                await self._handle_primary_failure(Exception("Primary delete returned False"))
                return await self.secondary.delete(key)
            return success
        except Exception as e:
            if self._active_provider == self.primary:
                await self._handle_primary_failure(e)
                return await self.secondary.delete(key)
            raise e

    async def exists(self, key: str) -> bool:
        await self._check_primary_recovery()
        try:
            return await self._active_provider.exists(key)
        except Exception as e:
            if self._active_provider == self.primary:
                await self._handle_primary_failure(e)
                return await self.secondary.exists(key)
            raise e

    async def get_signed_url(self, key: str, expires_in: int = 3600) -> str:
        await self._check_primary_recovery()
        try:
            return await self._active_provider.get_signed_url(key, expires_in)
        except Exception as e:
            if self._active_provider == self.primary:
                await self._handle_primary_failure(e)
                return await self.secondary.get_signed_url(key, expires_in)
            raise e

    def list_blobs(self, prefix: Optional[str] = None, limit: Optional[int] = None) -> AsyncIterator[dict[str, Any]]:
        return self._active_provider.list_blobs(prefix, limit)

    def validate_compliance(self) -> Any:
        return self._active_provider.validate_compliance()

    # --- ISyncProvider Implementation ---

    async def sync_to(
        self,
        target_provider: Any,
        direction: SyncDirection = SyncDirection.TO_TARGET,
        conflict_resolution: SyncConflictResolution = SyncConflictResolution.NEWEST_WINS,
        dry_run: bool = False,
    ) -> SyncResult:
        if target_provider == self.primary:
            if isinstance(self.secondary, ISyncProvider):
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
            p_health = await self.primary.exists("__health_check__")
        except Exception as exc:
            logger.warning("Primary blob health check failed: %s", exc)

        s_health = False
        try:
            s_health = await self.secondary.exists("__health_check__")
        except Exception as exc:
            logger.warning("Secondary blob health check failed: %s", exc)

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
        return await self._active_provider.create_backup(backup_path, metadata)

    async def restore_backup(self, backup_path: str, clear_existing: bool = False) -> bool:
        return await self._active_provider.restore_backup(backup_path, clear_existing)

    async def validate_backup(self, backup_path: str) -> dict[str, Any]:
        return await self._active_provider.validate_backup(backup_path)

    async def list_backups(self, backup_dir: str) -> dict[str, dict[str, Any]]:
        return await self._active_provider.list_backups(backup_dir)
