from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Optional, Union, AsyncIterator, Dict, List

from .storage import IStorageProvider
from .backup import IBackupProvider


class BaseBlobProvider(IStorageProvider, IBackupProvider, ABC):
    """
    Abstract Base Class for all blob storage providers.
    Enforces async contract, backup capabilities, and compliance checks.
    """

    @abstractmethod
    async def upload(
        self,
        key: str,
        data: Union[bytes, str],
        content_type: Optional[str] = None,
        encryption_context: Optional[dict[str, str]] = None,
    ) -> bool:
        """
        Asynchronously upload data to storage.
        """

    @abstractmethod
    async def download(self, key: str, decryption_context: Optional[dict[str, str]] = None) -> Optional[bytes]:
        """
        Asynchronously download data from storage.
        """

    @abstractmethod
    async def delete(self, key: str) -> bool:
        """
        Asynchronously delete object.
        """

    @abstractmethod
    async def get_signed_url(self, key: str, expires_in: int = 3600) -> str:
        """
        Generate a pre-signed URL for direct access (if supported).
        """

    @abstractmethod
    async def exists(self, key: str) -> bool:
        """
        Check if object exists.
        """

    @abstractmethod
    def list_blobs(self, prefix: Optional[str] = None, limit: Optional[int] = None) -> AsyncIterator[dict[str, Any]]:
        """
        List blobs with optional prefix and limit.
        """

    @abstractmethod
    async def create_backup(self, backup_path: str, metadata: Optional[dict[str, Any]] = None) -> bool:
        """Create a backup of the blob data."""

    @abstractmethod
    async def restore_backup(self, backup_path: str, clear_existing: bool = False) -> bool:
        """Restore blob data from a backup."""

    @abstractmethod
    async def validate_backup(self, backup_path: str) -> dict[str, Any]:
        """Validate a blob backup."""

    @abstractmethod
    async def list_backups(self, backup_dir: str) -> dict[str, dict[str, Any]]:
        """List available blob backups."""

    # --- IStorageProvider Implementation (KV-bridge) ---

    async def get_async(self, key: str) -> Optional[Any]:
        return await self.download(key)

    async def set_async(self, key: str, value: Any) -> bool:
        if isinstance(value, (bytes, str)):
            return await self.upload(key, value)
        import json

        return await self.upload(key, json.dumps(value), content_type="application/json")

    async def delete_async(self, key: str) -> bool:
        return await self.delete(key)

    async def exists_async(self, key: str) -> bool:
        return await self.exists(key)

    async def list_keys_async(self, pattern: Optional[str] = None) -> list[str]:
        keys = []
        async for blob in self.list_blobs():
            key = blob["name"]
            if pattern:
                import re

                if re.search(pattern, key, re.IGNORECASE):
                    keys.append(key)
            else:
                keys.append(key)
        return keys

    async def find_async(self, query: dict[str, Any]) -> list[Any]:
        # Blob storage find is very limited without an index
        return []

    async def clear_async(self) -> int:
        count = 0
        async for blob in self.list_blobs():
            if await self.delete(blob["name"]):
                count += 1
        return count
