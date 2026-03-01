"""
Blob Storage Service Implementation.
Refactored to use Universal Storage Factory and Async Providers.
"""

from typing import Optional, Union

from src.shared.observability.logger_factory import get_component_logger
from src.shared.services.storage.blob_factory import BlobStorageFactory
from src.shared.services.storage.providers.base_blob_provider import BaseBlobProvider
from src.shared.services.storage.providers.local_blob import LocalBlobProvider

# Alias for backward compatibility during refactoring
LocalStorageProvider = LocalBlobProvider

logger = get_component_logger("storage", "blob_service")


class BlobStorageService:
    """
    Unified service for blob storage operations.
    Delegates to the configured provider via BlobStorageFactory.
    """

    def __init__(self, provider_type: Optional[str] = None):
        self.provider: BaseBlobProvider = BlobStorageFactory.get_provider(provider_type)

    async def upload(
        self,
        key: str,
        data: Union[bytes, str],
        content_type: Optional[str] = None,
        encryption_context: Optional[dict[str, str]] = None,
    ) -> bool:
        """
        Upload data to the configured blob storage provider.
        """
        return await self.provider.upload(key, data, content_type, encryption_context)

    async def download(self, key: str, decryption_context: Optional[dict[str, str]] = None) -> Optional[bytes]:
        """
        Download data from the configured blob storage provider.
        """
        return await self.provider.download(key, decryption_context)

    async def delete(self, key: str) -> bool:
        """
        Delete data from storage.
        """
        return await self.provider.delete(key)

    async def exists(self, key: str) -> bool:
        """
        Check if key exists.
        """
        return await self.provider.exists(key)

    async def get_signed_url(self, key: str, expires_in: int = 3600) -> str:
        """
        Get a signed URL for direct access.
        """
        return await self.provider.get_signed_url(key, expires_in)

    def validate_compliance(self):
        """
        Validate compliance of the underlying provider.
        """
        return self.provider.validate_compliance()


_service = None


def get_blob_storage_service() -> BlobStorageService:
    """Singleton accessor for BlobStorageService."""
    global _service
    if _service is None:
        _service = BlobStorageService()
    return _service
