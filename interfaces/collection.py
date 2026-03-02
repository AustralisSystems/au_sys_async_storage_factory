"""
Collection Interface - Single Responsibility Principle.

Defines bulk operations and collection-specific functionality.
Separated from basic CRUD for optional implementation by providers that support it.

Scaffolded from rest-api-orchestrator `src/services/storage/interfaces/collection.py` (commit a5f0cc2a4f33f312e3d340c06f9aba4688ae9f01).
"""

from abc import ABC, abstractmethod
from typing import Any, Optional, Union, cast

from .storage import IStorageProvider


class ICollectionProvider(ABC):
    """
    Interface for bulk operations and collection management.

    This interface handles bulk data operations and collection-specific features.
    Not all storage providers need to implement this - it's for advanced use cases.
    """

    @abstractmethod
    def store_objects(
        self,
        objects: Union[list[dict[str, Any]], dict[str, Any]],
        collection_name: Optional[str] = None,
    ) -> bool:
        """
        Store multiple objects efficiently.

        Args:
            objects: List of objects or dict containing categorized objects
            collection_name: Optional collection/namespace for the objects

        Returns:
            True if all objects were stored successfully
        """

    @abstractmethod
    def get_objects(
        self,
        collection_name: Optional[str] = None,
        filter_criteria: Optional[dict[str, Any]] = None,
    ) -> list[dict[str, Any]]:
        """
        Retrieve multiple objects efficiently.

        Args:
            collection_name: Optional collection/namespace to search in
            filter_criteria: Optional criteria to filter objects

        Returns:
            List of matching objects
        """

    @abstractmethod
    def search_objects(
        self, query: str, collection_name: Optional[str] = None, limit: int = 100
    ) -> list[dict[str, Any]]:
        """
        Search objects by text query.

        Args:
            query: Text query to search for
            collection_name: Optional collection to search in
            limit: Maximum number of results

        Returns:
            List of matching objects
        """

    @abstractmethod
    def count_objects(
        self,
        collection_name: Optional[str] = None,
        filter_criteria: Optional[dict[str, Any]] = None,
    ) -> int:
        """
        Count objects matching criteria.

        Args:
            collection_name: Optional collection to count in
            filter_criteria: Optional criteria to filter objects

        Returns:
            Number of matching objects
        """

    @abstractmethod
    def list_collections(self) -> list[str]:
        """
        List all available collections/namespaces.

        Returns:
            List of collection names
        """

    @abstractmethod
    def delete_collection(self, collection_name: str) -> bool:
        """
        Delete an entire collection.

        Args:
            collection_name: Name of collection to delete

        Returns:
            True if collection was deleted
        """


class CollectionManager:
    """
    Manager for collection operations.

    This provides common collection management functionality
    that can be composed with storage providers.
    """

    def __init__(self, storage_provider: IStorageProvider) -> None:
        """Initialize with a storage provider."""
        self.storage_provider = storage_provider

    def batch_store(
        self,
        objects: list[dict[str, Any]],
        batch_size: int = 100,
        collection_name: Optional[str] = None,
    ) -> dict[str, Any]:
        """
        Store objects in batches for better performance.

        Args:
            objects: List of objects to store
            batch_size: Number of objects per batch
            collection_name: Optional collection name

        Returns:
            Dictionary with batch results
        """
        if not hasattr(self.storage_provider, "store_objects"):
            # Fallback to individual storage
            return self._store_individually(objects, collection_name)

        total_batches = (len(objects) + batch_size - 1) // batch_size
        successful_batches = 0
        failed_objects = []

        for i in range(0, len(objects), batch_size):
            batch = objects[i : i + batch_size]
            try:
                if cast(Any, self.storage_provider).store_objects(batch, collection_name):
                    successful_batches += 1
                else:
                    failed_objects.extend(batch)
            except Exception:
                failed_objects.extend(batch)

        return {
            "total_objects": len(objects),
            "total_batches": total_batches,
            "successful_batches": successful_batches,
            "failed_objects": len(failed_objects),
            "success_rate": (successful_batches / total_batches if total_batches > 0 else 0),
        }

    def _store_individually(
        self, objects: list[dict[str, Any]], collection_name: Optional[str] = None
    ) -> dict[str, Any]:
        """Fallback to store objects individually."""
        successful = 0
        failed = 0

        for obj in objects:
            try:
                key = obj.get("id", f"object_{successful + failed}")
                cast(Any, self.storage_provider).set(key, obj)
                successful += 1
            except Exception:
                failed += 1

        return {
            "total_objects": len(objects),
            "successful": successful,
            "failed": failed,
            "success_rate": successful / len(objects) if objects else 0,
        }

    def get_collection_stats(self, collection_name: str) -> dict[str, Any]:
        """
        Get statistics for a collection.

        Args:
            collection_name: Name of the collection

        Returns:
            Dictionary with collection statistics
        """
        if hasattr(self.storage_provider, "count_objects"):
            count = cast(Any, self.storage_provider).count_objects(collection_name)
        else:
            # Fallback counting
            keys = cast(Any, self.storage_provider).list_keys()
            count = len([k for k in keys if k.startswith(f"{collection_name}_")])

        return {
            "collection_name": collection_name,
            "object_count": count,
            "exists": count > 0,
        }
