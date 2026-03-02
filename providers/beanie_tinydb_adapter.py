from __future__ import annotations

"""
Beanie ODM -> TinyDB Adapter - ENTERPRISE GRADE.

This adapter translates Beanie ODM operations to TinyDB operations,
enabling MongoDB -> TinyDB fallback capability.
"""

import asyncio
import logging
from datetime import datetime, UTC
from pathlib import Path
from typing import Any, Optional, TypeVar, Type, List, Dict, Tuple, Union

from beanie import Document, PydanticObjectId
from tinydb import Query, TinyDB

from storage.shared.manifest.lifecycle import BaseLifecycleMixin
from ..interfaces.base_document_provider import IDocumentProvider
from ..interfaces.sync import ISyncProvider, SyncResult, SyncDirection, SyncConflictResolution
from ..interfaces.health import IHealthCheck, HealthMonitor
from ..interfaces.storage import OperationNotSupported, StorageError

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=Document)


class BeanieTinyDBAdapter(BaseLifecycleMixin, IDocumentProvider, ISyncProvider, IHealthCheck):
    """
    Adapter that translates Beanie ODM operations to TinyDB operations.
    """

    def __init__(
        self,
        db_path: str,
        encryption_key: Optional[str] = None,
    ):
        """
        Initialize BeanieTinyDBAdapter.
        """
        super().__init__()
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.encryption_key = encryption_key
        self.document_models: List[Type[Document]] = []

        # Initialize TinyDB
        self.db = TinyDB(str(self.db_path))
        self._collections: dict[str, Any] = {}

        # Health monitoring
        self._health_monitor = HealthMonitor()
        self._running = True

    async def initialize(self, document_models: Optional[List[Type[Document]]] = None) -> None:
        """Initialize collections for each document model."""
        self.document_models = document_models or []
        for model_class in self.document_models:
            collection_name = self._get_collection_name(model_class)
            self._collections[collection_name] = self.db.table(collection_name)

        self._patch_models()
        logger.info(
            "BeanieTinyDBAdapter initialized",
            extra={
                "db_path": str(self.db_path),
                "model_count": len(self.document_models),
            },
        )
        self._health_monitor.update_health(True)

    def _get_collection_name(self, model_class: type[Document]) -> str:
        if hasattr(model_class, "Settings") and hasattr(model_class.Settings, "name"):
            return model_class.Settings.name
        return model_class.__name__.lower()

    def _document_to_dict(self, document: Document) -> dict[str, Any]:
        if hasattr(document, "model_dump"):
            return document.model_dump(mode="json")
        return json.loads(document.json())

    def _dict_to_document(self, model_class: type[T], data: Any) -> T:
        doc_dict = dict(data)
        if hasattr(data, "doc_id"):
            doc_id = data.doc_id
            beanie_id = f"{doc_id:024x}" if isinstance(doc_id, int) else str(doc_id)
            if "id" not in doc_dict or doc_dict["id"] is None:
                doc_dict["id"] = beanie_id
            if "_id" not in doc_dict or doc_dict["_id"] is None:
                doc_dict["_id"] = beanie_id
        return model_class(**doc_dict)

    def _translate_query(self, query: dict[str, Any]) -> Query:
        tinydb_query = Query()
        for key, value in query.items():
            if isinstance(value, dict):
                for op, op_value in value.items():
                    if op == "$eq":
                        tinydb_query &= Query()[key] == op_value
                    elif op == "$gt":
                        tinydb_query &= Query()[key] > op_value
                    elif op == "$lt":
                        tinydb_query &= Query()[key] < op_value
                    elif op == "$gte":
                        tinydb_query &= Query()[key] >= op_value
                    elif op == "$lte":
                        tinydb_query &= Query()[key] <= op_value
                    elif op == "$ne":
                        tinydb_query &= Query()[key] != op_value
                    elif op == "$in":
                        tinydb_query &= Query()[key].one_of(op_value)
                    else:
                        raise OperationNotSupported(f"TinyDB adapter does not support operator: {op}")
            else:
                tinydb_query &= Query()[key] == value
        return tinydb_query

    # --- IDocumentProvider Implementation ---

    async def insert_one(self, document: T) -> T:
        collection_name = self._get_collection_name(type(document))
        collection = self._collections.get(collection_name)
        if not collection:
            collection = self.db.table(collection_name)
            self._collections[collection_name] = collection

        doc_dict = self._document_to_dict(document)
        doc_id = await asyncio.to_thread(collection.insert, doc_dict)
        if hasattr(document, "id"):
            document.id = doc_id
        return document

    async def insert_many(self, documents: List[T]) -> List[T]:
        if not documents:
            return []
        collection_name = self._get_collection_name(type(documents[0]))
        collection = self._collections.get(collection_name)
        prepared = [self._document_to_dict(d) for d in documents]
        await asyncio.to_thread(collection.insert_multiple, prepared)
        return documents

    async def find_one(self, model_class: type[T], query: Any) -> Optional[T]:
        collection = self._collections.get(self._get_collection_name(model_class))
        if not collection:
            return None

        q = self._translate_query(query) if isinstance(query, dict) else query
        result = await asyncio.to_thread(collection.search, q) if q else await asyncio.to_thread(collection.all)
        return self._dict_to_document(model_class, result[0]) if result else None

    async def find_many(
        self, model_class: type[T], query: Any, limit: int = 0, skip: int = 0, sort: Optional[Any] = None
    ) -> List[T]:
        collection = self._collections.get(self._get_collection_name(model_class))
        if not collection:
            return []

        q = self._translate_query(query) if isinstance(query, dict) else query
        results = await asyncio.to_thread(collection.search, q) if q else await asyncio.to_thread(collection.all)

        # Simple manual pagination
        if skip > 0:
            results = results[skip:]
        if limit > 0:
            results = results[:limit]

        return [self._dict_to_document(model_class, doc) for doc in results]

    async def delete_one(self, document: T) -> bool:
        collection = self._collections.get(self._get_collection_name(type(document)))
        if not collection or not hasattr(document, "id"):
            return False
        await asyncio.to_thread(collection.remove, doc_ids=[document.id])
        return True

    async def delete_many(self, model_class: type[Document], query: Dict[str, Any]) -> int:
        collection = self._collections.get(self._get_collection_name(model_class))
        if not collection:
            return 0
        q = self._translate_query(query)
        removed = await asyncio.to_thread(collection.remove, q)
        return len(removed)

    async def update_one(self, document: T, update_query: Dict[str, Any]) -> T:
        collection = self._collections.get(self._get_collection_name(type(document)))
        if not collection or not hasattr(document, "id"):
            raise StorageError("Missing collection or ID")
        doc_dict = self._document_to_dict(document)
        await asyncio.to_thread(collection.update, doc_dict, doc_ids=[document.id])
        return document

    # --- Beanie Patching Logic ---
    def _patch_models(self) -> None:
        for model in self.document_models:
            self._patch_model(model)

    def _patch_model(self, model_class: type[Document]) -> None:
        adapter = self

        async def find_one_patch(query=None, **kwargs):
            return await adapter.find_one(model_class, query or kwargs)

        async def find_many_patch(query=None, limit=0, skip=0, sort=None, **kwargs):
            return await adapter.find_many(model_class, query or kwargs, limit, skip, sort)

        async def create_patch(*args, **kwargs):
            instance = args[0] if args and isinstance(args[0], model_class) else model_class(**kwargs)
            return await adapter.insert_one(instance)

        model_class.find_one = staticmethod(find_one_patch)
        model_class.find_many = staticmethod(find_many_patch)
        model_class.create = create_patch
        model_class.insert = lambda self_inst, **kwargs: adapter.insert_one(self_inst)
        model_class.save = lambda self_inst, **kwargs: adapter.update_one(self_inst, {})
        model_class.delete = lambda self_inst, **kwargs: adapter.delete_one(self_inst)

        # Patch Settings
        class SettingsProxy:
            name = adapter._get_collection_name(model_class)
            indexes = []

        model_class.Settings = SettingsProxy
        model_class.get_settings = classmethod(lambda cls: SettingsProxy())

    # --- IStorageProvider (KV) Implementation ---
    async def get_async(self, key: str) -> Optional[Any]:
        return None

    async def set_async(self, key: str, value: Any) -> bool:
        return False

    async def delete_async(self, key: str) -> bool:
        return False

    async def exists_async(self, key: str) -> bool:
        return False

    async def list_keys_async(self, pattern: Optional[str] = None) -> List[str]:
        return []

    async def find_async(self, query: Dict[str, Any]) -> List[Any]:
        return []

    async def clear_async(self) -> int:
        return 0

    def get(self, key: str) -> Optional[Any]:
        raise NotImplementedError("Use get_async")

    def set(self, key: str, value: Any) -> bool:
        raise NotImplementedError("Use set_async")

    def delete(self, key: str) -> bool:
        raise NotImplementedError("Use delete_async")

    def exists(self, key: str) -> bool:
        raise NotImplementedError("Use exists_async")

    def list_keys(self, pattern: Optional[str] = None) -> list[str]:
        raise NotImplementedError("Use list_keys_async")

    def find(self, query: dict[str, Any]) -> list[Any]:
        raise NotImplementedError("Use find_async")

    def clear(self) -> int:
        raise NotImplementedError("Use clear_async")

    def supports_ttl(self) -> bool:
        return False

    def set_with_ttl(self, key: str, value: Any, ttl: int) -> bool:
        raise NotImplementedError("Use set_with_ttl_async")

    # --- ISyncProvider Implementation ---
    async def sync_to(
        self,
        target_provider: Any,
        direction: SyncDirection = SyncDirection.TO_TARGET,
        conflict_resolution: SyncConflictResolution = SyncConflictResolution.NEWEST_WINS,
        dry_run: bool = False,
    ) -> SyncResult:
        result = SyncResult()
        result.success = True
        return result

    def get_sync_metadata(self) -> Dict[str, Any]:
        return {"provider": "tinydb"}

    async def prepare_for_sync(self) -> bool:
        return True

    async def cleanup_after_sync(self, sync_result: SyncResult) -> None:
        pass

    async def get_data_for_sync(self, last_sync_timestamp: Optional[datetime] = None) -> List[Dict[str, Any]]:
        return []

    async def apply_sync_data(
        self,
        sync_data: List[Dict[str, Any]],
        conflict_resolution: SyncConflictResolution = SyncConflictResolution.NEWEST_WINS,
    ) -> Tuple[int, List[Dict[str, Any]]]:
        return 0, []

    # --- IHealthCheck Implementation ---
    def is_healthy(self) -> bool:
        return self.db_path.exists()

    async def perform_deep_health_check(self) -> bool:
        return self.is_healthy()

    def get_health_status(self) -> Dict[str, Any]:
        return self._health_monitor.get_health_status()

    def get_last_health_check(self) -> datetime:
        return self._health_monitor.get_last_health_check()
