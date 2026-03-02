from __future__ import annotations

"""
Universal Blob Index & Cache Middleware.

Enforces Dual-Tier Indexing (Redis -> SQLite) and handles metadata mapping 
across multiple storage providers.
"""

import asyncio
import logging
import json
from datetime import datetime, UTC
from typing import Any, AsyncIterator, Optional, Union

from ..interfaces.base_blob_provider import BaseBlobProvider
from ..interfaces.sync import ISyncProvider
from ..interfaces.health import IHealthCheck, HealthMonitor

logger = logging.getLogger(__name__)


class BlobIndexMiddleware(BaseBlobProvider, IHealthCheck):
    """
    Universal Middleware for Blob Storage.

    This middleware sits between the service and the actual providers.
    It maintains a local metadata index in Redis (or SQLite fallback)
    to track blob locations and properties across the storage factory.
    """

    def __init__(
        self,
        primary_provider: BaseBlobProvider,
        index_backend: Any,  # Should implement a common metadata interface
        cache_provider: Optional[BaseBlobProvider] = None,
    ):
        self.primary = primary_provider
        self.index = index_backend
        self.cache = cache_provider
        self._health_monitor = HealthMonitor()

    # --- BaseBlobProvider Implementation ---

    async def upload(
        self,
        key: str,
        data: Union[bytes, str],
        content_type: Optional[str] = None,
        encryption_context: Optional[dict[str, str]] = None,
    ) -> bool:
        """
        Upload to primary and update index.
        """
        success = await self.primary.upload(key, data, content_type, encryption_context)
        if success:
            # Update Index
            metadata = {
                "key": key,
                "size": len(data) if isinstance(data, bytes) else len(data.encode()),
                "content_type": content_type,
                "provider": "primary",
                "uploaded_at": datetime.now(UTC).isoformat(),
            }
            try:
                await self.index.set_metadata(key, metadata)
            except Exception as e:
                logger.warning(f"Failed to update blob index for {key}: {e}")

            # Update cache if available
            if self.cache:
                await self.cache.upload(key, data, content_type, encryption_context)

        return success

    async def download(self, key: str, decryption_context: Optional[dict[str, str]] = None) -> Optional[bytes]:
        """
        Try downloading from cache first, then primary.
        """
        # 1. Try Cache
        if self.cache:
            cached_data = await self.cache.download(key, decryption_context)
            if cached_data:
                logger.debug(f"Cache hit for blob: {key}")
                return cached_data

        # 2. Try Primary
        data = await self.primary.download(key, decryption_context)

        # 3. Populate Cache
        if data and self.cache:
            await self.cache.upload(key, data)

        return data

    async def delete(self, key: str) -> bool:
        """
        Delete from all tiers and index.
        """
        results = await asyncio.gather(
            self.primary.delete(key),
            self.cache.delete(key) if self.cache else asyncio.sleep(0, result=True),
            self.index.delete_metadata(key),
            return_exceptions=True,
        )
        return any(isinstance(r, bool) and r for r in results[:1])

    async def exists(self, key: str) -> bool:
        """
        Check index first, then primary.
        """
        # Check Index
        metadata = await self.index.get_metadata(key)
        if metadata:
            return True

        # Fallback to primary
        return await self.primary.exists(key)

    async def get_signed_url(self, key: str, expires_in: int = 3600) -> str:
        return await self.primary.get_signed_url(key, expires_in)

    async def list_blobs(
        self, prefix: Optional[str] = None, limit: Optional[int] = None
    ) -> AsyncIterator[dict[str, Any]]:
        # Prefer listing from index for speed
        try:
            async for blob in self.index.list_metadata(prefix, limit):
                yield blob
        except Exception:
            # Fallback to primary provider listing
            async for blob in self.primary.list_blobs(prefix, limit):
                yield blob

    def validate_compliance(self) -> Any:
        return self.primary.validate_compliance()

    # --- IHealthCheck Implementation ---

    def is_healthy(self) -> bool:
        return self._health_monitor.is_healthy()

    async def perform_deep_health_check(self) -> bool:
        # Check primary and index
        p_health = await self.primary.exists("__health_check__")
        i_health = await self.index.is_healthy()
        healthy = p_health and i_health
        self._health_monitor.update_health(healthy)
        return healthy

    def get_health_status(self) -> dict[str, Any]:
        return self._health_monitor.get_health_status()

    def get_last_health_check(self) -> datetime:
        return self._health_monitor.get_last_health_check()

    # --- IBackupProvider Implementation (delegates to primary) ---

    async def create_backup(self, backup_path: str, metadata: Optional[dict[str, Any]] = None) -> bool:
        """Delegate backup creation to the primary provider."""
        return await self.primary.create_backup(backup_path, metadata)

    async def restore_backup(self, backup_path: str, clear_existing: bool = False) -> bool:
        """Delegate backup restoration to the primary provider."""
        return await self.primary.restore_backup(backup_path, clear_existing)

    async def validate_backup(self, backup_path: str) -> dict[str, Any]:
        """Delegate backup validation to the primary provider."""
        return await self.primary.validate_backup(backup_path)

    async def list_backups(self, backup_dir: str) -> dict[str, dict[str, Any]]:
        """Delegate backup listing to the primary provider."""
        return await self.primary.list_backups(backup_dir)
