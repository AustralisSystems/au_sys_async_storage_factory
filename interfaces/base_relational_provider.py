from __future__ import annotations

"""
Base Relational Provider Interface.

Defines the contract for Relational/SQL storage providers.
This interface abstracts SQL operations but does not replace the underlying ORM.
"""

from abc import ABC, abstractmethod
from typing import Any, Optional

from .backup import IBackupProvider
from .health import IHealthCheck
from .storage import IStorageProvider


class IRelationalProvider(IStorageProvider, IHealthCheck, IBackupProvider, ABC):
    """
    Interface for Relational/SQL storage providers.
    Extends IStorageProvider, IHealthCheck, and IBackupProvider.
    """

    @abstractmethod
    async def initialize(self) -> None:
        """
        Initialize the provider, establishing connections and ensuring schema existence.
        """

    @abstractmethod
    async def execute_raw_sql(self, sql: str, params: Optional[dict[str, Any]] = None) -> Any:
        """
        Execute a raw SQL query asynchronously.
        """
