from __future__ import annotations
"""
Core Storage Interface - Single Responsibility Principle.

Defines the essential CRUD operations that any storage provider must implement.
This interface is focused solely on data persistence operations.

Scaffolded from rest-api-orchestrator `src/services/storage/interfaces/storage.py` (commit a5f0cc2a4f33f312e3d340c06f9aba4688ae9f01).
"""


from abc import ABC, abstractmethod
from typing import Any, Optional

from src.shared.observability.logger_factory import create_debug_logger

logger = create_debug_logger(__name__)


class IStorageProvider(ABC):
    """
    Core storage interface for basic CRUD operations.

    This interface follows the Single Responsibility Principle by focusing
    only on data persistence. Other concerns like health checking, backup,
    and collection management are handled by separate interfaces.
    """

    @abstractmethod
    def get(self, key: str) -> Optional[Any]:
        """
        Retrieve a value by key.

        Args:
            key: The unique identifier for the data

        Returns:
            The stored value or None if not found
        """

    @abstractmethod
    def set(self, key: str, value: Any) -> bool:
        """
        Store a value with the given key.

        Args:
            key: The unique identifier for the data
            value: The data to store

        Returns:
            True if successful, False otherwise

        Raises:
            StorageError: If the operation fails
        """

    @abstractmethod
    def delete(self, key: str) -> bool:
        """
        Delete a value by key.

        Args:
            key: The unique identifier for the data

        Returns:
            True if the item was deleted, False if it didn't exist
        """

    @abstractmethod
    def exists(self, key: str) -> bool:
        """
        Check if a key exists.

        Args:
            key: The unique identifier to check

        Returns:
            True if the key exists, False otherwise
        """

    @abstractmethod
    def list_keys(self, pattern: Optional[str] = None) -> list[str]:
        """
        List all keys, optionally filtered by pattern.

        Args:
            pattern: Optional pattern to filter keys (implementation-specific)

        Returns:
            List of keys matching the pattern
        """

    @abstractmethod
    def find(self, query: dict[str, Any]) -> list[Any]:
        """
        Find values matching a simple equality query.

        Args:
            query: Dictionary of key-value pairs to match against stored data

        Returns:
            List of matching values
        """

    @abstractmethod
    def clear(self) -> int:
        """
        Clear all data from storage.

        Returns:
            Number of items that were deleted
        """

    # --- Asynchronous Interface ---

    async def get_async(self, key: str) -> Optional[Any]:
        """Retrieve a value by key (async)."""
        import asyncio

        return await asyncio.to_thread(self.get, key)

    async def set_async(self, key: str, value: Any) -> bool:
        """Store a value with the given key (async)."""
        import asyncio

        return await asyncio.to_thread(self.set, key, value)

    async def delete_async(self, key: str) -> bool:
        """Delete a value by key (async)."""
        import asyncio

        return await asyncio.to_thread(self.delete, key)

    async def exists_async(self, key: str) -> bool:
        """Check if a key exists (async)."""
        import asyncio

        return await asyncio.to_thread(self.exists, key)

    async def list_keys_async(self, pattern: Optional[str] = None) -> list[str]:
        """List all keys (async)."""
        import asyncio

        return await asyncio.to_thread(self.list_keys, pattern)

    async def clear_async(self) -> int:
        """Clear all data (async)."""
        import asyncio

        return await asyncio.to_thread(self.clear)

    def supports_ttl(self) -> bool:
        """
        Check if this adapter supports TTL (Time To Live).

        Returns:
            True if adapter supports TTL, False otherwise.
            Default implementation returns False.
            Override in subclasses that support TTL.
        """
        return False

    def set_with_ttl(self, key: str, value: Any, ttl: int) -> bool:
        """
        Store a value with TTL (Time To Live) in seconds.

        Args:
            key: The unique identifier for the data
            value: The data to store
            ttl: Time to live in seconds

        Returns:
            True if successful, False otherwise

        Raises:
            TTLNotSupported: If the adapter does not support TTL operations
            TTLNotImplemented: If the adapter declares TTL support but has no implementation
            StorageError: For other storage-related failures

        Note:
            Clients should check supports_ttl() before calling this method when possible.
        """
        if not self.supports_ttl():
            raise TTLNotSupported(
                f"{self.__class__.__name__} does not support TTL operations. "
                "Check supports_ttl() before calling set_with_ttl()."
            )

        # Adapter declares TTL support but hasn't implemented it correctly.
        raise TTLNotImplemented(
            f"{self.__class__.__name__} declares TTL support but does not implement set_with_ttl()."
        )


class StorageError(Exception):
    """Base exception for storage operations."""


class ConnectionError(StorageError):
    """Raised when storage connection fails."""


class AuthenticationError(StorageError):
    """Raised when storage authentication fails."""


class NotFoundError(StorageError):
    """Raised when requested data is not found."""


class TTLNotSupported(StorageError):
    """Raised when TTL operations are not supported by the adapter."""


class TTLNotImplemented(StorageError):
    """Raised when an adapter declares TTL support but does not implement set_with_ttl()."""


class OperationNotSupported(StorageError):
    """Raised when an operation is not supported by the storage adapter."""
