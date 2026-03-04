from __future__ import annotations

"""
Base Vector Provider Interface.

Defines the contract for Vector storage providers (e.g., Qdrant, Redis Vector).
Extends IStorageProvider to include vector-specific operations.
"""

from abc import ABC, abstractmethod
from typing import Any, Optional

from .backup import IBackupProvider
from .health import IHealthCheck
from .storage import IStorageProvider


class IVectorProvider(IStorageProvider, IHealthCheck, IBackupProvider, ABC):
    """
    Interface for Vector storage providers.
    """

    @abstractmethod
    async def initialize(self) -> None:
        """Initialize connection and verify backend availability."""

    @abstractmethod
    async def create_index(self, name: str, dimension: int, distance_metric: str = "cosine", **kwargs: Any) -> bool:
        """Create or initialize a vector index/collection."""

    @abstractmethod
    async def upsert(
        self,
        index_name: str,
        vectors: list[list[float]],
        metadata: list[dict[str, Any]],
        ids: Optional[list[str]] = None,
    ) -> bool:
        """Insert or update vectors with associated metadata."""

    @abstractmethod
    async def search(
        self, index_name: str, query_vector: list[float], limit: int = 10, filters: Optional[dict[str, Any]] = None
    ) -> list[dict[str, Any]]:
        """Search for top-K similar vectors."""

    @abstractmethod
    async def delete(self, index_name: str, ids: list[str]) -> bool:
        """Delete vectors by ID."""

    @abstractmethod
    async def get_index_stats(self, index_name: str) -> dict[str, Any]:
        """Get statistics about the index."""

    @abstractmethod
    async def drop_index(self, index_name: str) -> bool:
        """Permanently delete an index."""
