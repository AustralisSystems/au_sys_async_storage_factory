from __future__ import annotations

"""
Base Vector Provider Interface.

Defines the contract for Vector storage providers (e.g., Qdrant, Redis Vector).
Extends IStorageProvider to include vector-specific operations.
"""

from abc import ABC, abstractmethod
from typing import Any, Optional, Dict, List

from .storage import IStorageProvider


class IVectorProvider(IStorageProvider, ABC):
    """
    Interface for Vector storage providers.
    """

    @abstractmethod
    async def initialize(self) -> None:
        """Initialize connection and collections."""

    @abstractmethod
    async def create_collection(self, name: str, dimension: int, **kwargs: Any) -> bool:
        """Create vector collection/index."""

    @abstractmethod
    async def upsert_vectors(self, collection: str, vectors: List[Dict[str, Any]]) -> bool:
        """Upsert vectors with metadata."""

    @abstractmethod
    async def search(self, collection: str, query_vector: List[float], limit: int = 10) -> List[Dict[str, Any]]:
        """Search for similar vectors."""

    @abstractmethod
    async def delete_vectors(self, collection: str, ids: List[str]) -> bool:
        """Delete vectors by ID."""
