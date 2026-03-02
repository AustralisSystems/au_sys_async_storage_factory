from __future__ import annotations

"""
Base Document Provider Interface.

Defines the contract for Document/NoSQL storage providers.
This interface abstracts operations but does not replace the underlying ODM (e.g., Beanie).
"""

from abc import ABC, abstractmethod
from typing import Any, Optional, Dict, List, Type, TypeVar

from .storage import IStorageProvider

T = TypeVar("T")


class IDocumentProvider(IStorageProvider, ABC):
    """
    Interface for Document/NoSQL storage providers.
    Extends IStorageProvider to include document-specific operations.
    """

    @abstractmethod
    async def initialize(self, document_models: Optional[List[Type[Any]]] = None) -> None:
        """
        Initialize the provider, establishing connections and ensuring schema existence.

        Args:
            document_models: Optional list of document classes to manage.
        """

    @abstractmethod
    async def insert_one(self, document: Any) -> Any:
        """
        Persist a single document.
        """

    @abstractmethod
    async def insert_many(self, documents: List[Any]) -> List[Any]:
        """
        Persist multiple documents.
        """

    @abstractmethod
    async def find_one(self, model_class: Type[T], query: Dict[str, Any]) -> Optional[T]:
        """
        Locate a single document matching the query.
        """

    @abstractmethod
    async def find_many(
        self,
        model_class: Type[T],
        query: Dict[str, Any],
        limit: int = 0,
        skip: int = 0,
        sort: Optional[Any] = None,
    ) -> List[T]:
        """
        Locate multiple documents with support for pagination and sorting.
        """

    @abstractmethod
    async def delete_one(self, document: Any) -> bool:
        """
        Delete a single document.
        """

    @abstractmethod
    async def delete_many(self, model_class: Type[Any], query: Dict[str, Any]) -> int:
        """
        Delete all documents matching the query.
        """

    @abstractmethod
    async def update_one(self, document: Any, update_query: Dict[str, Any]) -> Any:
        """
        Update a single document.
        """
