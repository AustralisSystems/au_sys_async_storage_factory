from abc import ABC, abstractmethod
from typing import Any, Optional, Union, AsyncIterator



class BaseBlobProvider(ABC):
    """
    Abstract Base Class for all blob storage providers.
    Enforces async contract and compliance checks.
    """

    @abstractmethod
    async def upload(
        self,
        key: str,
        data: Union[bytes, str],  # Allow bytes or string (auto-encode)
        content_type: Optional[str] = None,
        encryption_context: Optional[dict[str, str]] = None,
    ) -> bool:
        """
        Asynchronously upload data to storage.
        Must enforce encryption compliance.
        """
        ...

    @abstractmethod
    async def download(self, key: str, decryption_context: Optional[dict[str, str]] = None) -> Optional[bytes]:
        """
        Asynchronously download data from storage.
        """
        ...

    @abstractmethod
    async def delete(self, key: str) -> bool:
        """
        Asynchronously delete object.
        """
        ...

    @abstractmethod
    async def get_signed_url(self, key: str, expires_in: int = 3600) -> str:
        """
        Generate a pre-signed URL for direct access (if supported).
        """
        ...

    @abstractmethod
    async def exists(self, key: str) -> bool:
        """
        Check if object exists.
        """
        ...

    @abstractmethod
    def list_blobs(self, prefix: Optional[str] = None, limit: Optional[int] = None) -> Any:  # Returns async iterator
        """
        List blobs with optional prefix and limit.
        """
        ...

    @abstractmethod
    def list_blobs(
        self, prefix: Optional[str] = None, limit: Optional[int] = None
    ) -> AsyncIterator[Any]:  # Returns async iterator or list
        """
        List blobs with optional prefix and limit.
        """
        ...
