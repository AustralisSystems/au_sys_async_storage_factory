from abc import ABC, abstractmethod
from typing import Any


class IGraphProvider(ABC):
    """Unified interface for graph database providers."""

    @abstractmethod
    async def create_node(self, labels: list[str], properties: dict[str, Any]) -> str:
        """Create node and return ID."""

    @abstractmethod
    async def create_relationship(self, from_id: str, to_id: str, rel_type: str, properties: dict[str, Any]) -> str:
        """Create relationship between nodes."""

    @abstractmethod
    async def query(self, cypher: str, params: dict[str, Any] = None) -> list[dict[str, Any]]:
        """Execute Cypher query."""

    @abstractmethod
    async def delete_node(self, node_id: str) -> None:
        """Delete node by ID."""
