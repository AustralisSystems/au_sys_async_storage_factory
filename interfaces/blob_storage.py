"""
Blob Storage Interface.

Defines operations for object/blob storage (S3, Azure Blob, Local Files).
"""

from abc import ABC, abstractmethod
from typing import Optional


class IBlobStorageProvider(ABC):
    """Interface for blob storage providers."""

    @abstractmethod
    async def upload(self, key: str, data: bytes, content_type: Optional[str] = None) -> bool:
        """Upload data to storage."""

    @abstractmethod
    async def download(self, key: str) -> Optional[bytes]:
        """Download data from storage."""

    @abstractmethod
    async def delete(self, key: str) -> bool:
        """Delete data from storage."""

    @abstractmethod
    async def exists(self, key: str) -> bool:
        """Check if key exists in storage."""

    @abstractmethod
    async def get_signed_url(self, key: str, expires_in: int = 3600) -> str:
        """Generate a signed URL for temporary access."""
