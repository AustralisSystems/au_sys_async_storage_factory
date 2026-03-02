from __future__ import annotations

"""
Core Storage Interface - Single Responsibility Principle (Async First).

Defines the essential CRUD operations that any storage provider must implement.
This interface is focused solely on data persistence operations.
"""

from abc import ABC, abstractmethod
from typing import Any, Optional
import logging

logger = logging.getLogger("storage.interfaces")


class IStorageProvider(ABC):
    """
    Core storage interface for basic CRUD operations (Async First).

    This interface follows the Single Responsibility Principle by focusing
    only on data persistence.
    """

    @abstractmethod
    async def get_async(self, key: str) -> Optional[Any]:
        """
        Retrieve a value by key.
        """

    @abstractmethod
    async def set_async(self, key: str, value: Any) -> bool:
        """
        Store a value with the given key.
        """

    @abstractmethod
    async def delete_async(self, key: str) -> bool:
        """
        Delete a value by key.
        """

    @abstractmethod
    async def exists_async(self, key: str) -> bool:
        """
        Check if a key exists.
        """

    @abstractmethod
    async def list_keys_async(self, pattern: Optional[str] = None) -> list[str]:
        """
        List all keys, optionally filtered by pattern.
        """

    @abstractmethod
    async def find_async(self, query: dict[str, Any]) -> list[Any]:
        """
        Find values matching a simple equality query.
        """

    @abstractmethod
    async def clear_async(self) -> int:
        """
        Clear all data from storage.
        """

    def supports_ttl(self) -> bool:
        """Check if this adapter supports TTL (Time To Live)."""
        return False

    async def set_with_ttl_async(self, key: str, value: Any, ttl: int) -> bool:
        """
        Store a value with TTL (Time To Live) in seconds.
        """
        if not self.supports_ttl():
            raise TTLNotSupported(f"{self.__class__.__name__} does not support TTL.")
        raise NotImplementedError("set_with_ttl_async not implemented.")


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
    """Raised when an adapter declares TTL support but does not implement it."""


class OperationNotSupported(StorageError):
    """Raised when an operation is not supported by the storage adapter."""
