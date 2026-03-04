from __future__ import annotations

"""
Base Graph Provider Interface.

Defines the contract for Graph storage providers (e.g., Neo4j, FalkorDB).
Extends IStorageProvider to include graph-specific operations.
"""

from abc import ABC, abstractmethod
from typing import Any, Optional

from .backup import IBackupProvider
from .health import IHealthCheck
from .storage import IStorageProvider


class IGraphProvider(IStorageProvider, IHealthCheck, IBackupProvider, ABC):
    """
    Interface for Graph storage providers.
    """

    @abstractmethod
    async def initialize(self) -> None:
        """Initialize connection and schema."""

    @abstractmethod
    async def create_node(self, labels: list[str], properties: dict[str, Any]) -> str:
        """Create node and return ID."""

    @abstractmethod
    async def create_relationship(self, from_id: str, to_id: str, rel_type: str, properties: dict[str, Any]) -> str:
        """Create relationship between nodes."""

    @abstractmethod
    async def execute_query(self, cypher: str, params: Optional[dict[str, Any]] = None) -> list[dict[str, Any]]:
        """Execute Cypher query."""

    @abstractmethod
    async def delete_node(self, node_id: str) -> bool:
        """Delete node by ID."""
