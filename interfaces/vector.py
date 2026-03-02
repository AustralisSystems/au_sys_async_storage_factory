from abc import ABC, abstractmethod
from typing import Any


class IVectorProvider(ABC):
    """Unified interface for vector storage providers."""

    @abstractmethod
    async def create_collection(self, name: str, dimension: int, **kwargs: Any) -> None:
        """Create vector collection/index."""

    @abstractmethod
    async def upsert_vectors(self, collection: str, vectors: list[dict[str, Any]]) -> None:
        """Upsert vectors with metadata."""

    @abstractmethod
    async def search(self, collection: str, query_vector: list[float], limit: int = 10) -> list[dict[str, Any]]:
        """Search for similar vectors."""

    @abstractmethod
    async def delete(self, collection: str, ids: list[str]) -> None:
        """Delete vectors by ID."""
