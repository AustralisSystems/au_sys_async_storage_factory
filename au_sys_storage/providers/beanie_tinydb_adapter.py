from __future__ import annotations

"""
Beanie ODM -> TinyDB Adapter - ENTERPRISE GRADE.

This adapter translates Beanie ODM operations to TinyDB operations,
enabling MongoDB -> TinyDB fallback capability.
"""

import asyncio
import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Optional, TypeVar, cast

from beanie import Document, PydanticObjectId
from tinydb import Query, TinyDB

from au_sys_storage.shared.manifest.lifecycle import BaseLifecycleMixin

from ..interfaces.base_document_provider import IDocumentProvider
from ..interfaces.health import HealthMonitor, IHealthCheck
from ..interfaces.storage import OperationNotSupported, StorageError
from ..interfaces.sync import ISyncProvider, SyncConflictResolution, SyncDirection, SyncResult

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
        self.document_models: list[type[Document]] = []

        # Initialize TinyDB
        self.db = TinyDB(str(self.db_path))
        self._collections: dict[str, Any] = {}

        # Health monitoring
        self._health_monitor = HealthMonitor()
        self._running = True

    async def initialize(self, document_models: Optional[list[type[Document]]] = None) -> None:
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
            return str(model_class.Settings.name)
        return str(model_class.__name__.lower())

    def _document_to_dict(self, document: Document) -> dict[str, Any]:
        if hasattr(document, "model_dump"):
            return cast(dict[str, Any], document.model_dump(mode="json"))
        return cast(dict[str, Any], json.loads(document.json()))

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

    def _translate_query(self, query: dict[str, Any]) -> Optional[Query]:
        if not query:
            return None

        tinydb_query: Optional[Query] = None

        def add_cond(new_cond: Any) -> None:
            nonlocal tinydb_query
            if tinydb_query is None:
                tinydb_query = new_cond
            else:
                tinydb_query &= new_cond

        for key, value in query.items():
            if isinstance(value, dict):
                for op, op_value in value.items():
                    if op == "$eq":
                        add_cond(Query()[key] == op_value)
                    elif op == "$gt":
                        add_cond(Query()[key] > op_value)
                    elif op == "$lt":
                        add_cond(Query()[key] < op_value)
                    elif op == "$gte":
                        add_cond(Query()[key] >= op_value)
                    elif op == "$lte":
                        add_cond(Query()[key] <= op_value)
                    elif op == "$ne":
                        add_cond(Query()[key] != op_value)
                    elif op == "$in":
                        add_cond(Query()[key].one_of(op_value))
                    else:
                        raise OperationNotSupported(f"TinyDB adapter does not support operator: {op}")
            else:
                add_cond(Query()[key] == value)
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

    async def insert_many(self, documents: list[T]) -> list[T]:
        if not documents:
            return []
        collection_name = self._get_collection_name(type(documents[0]))
        collection = self._collections.get(collection_name)
        if not collection:
            collection = self.db.table(collection_name)
            self._collections[collection_name] = collection

        prepared = [self._document_to_dict(d) for d in documents]
        await asyncio.to_thread(collection.insert_multiple, prepared)
        return documents

    async def find_one(self, model_class: type[T], query: dict[str, Any]) -> Optional[T]:  # type: ignore[override]
        collection = self._collections.get(self._get_collection_name(model_class))
        if not collection:
            return None

        q = self._translate_query(query) if isinstance(query, dict) else query
        result = await asyncio.to_thread(collection.search, q) if q else await asyncio.to_thread(collection.all)
        return self._dict_to_document(model_class, result[0]) if result else None

    async def find_many(  # type: ignore[override]
        self, model_class: type[T], query: dict[str, Any], limit: int = 0, skip: int = 0, sort: Optional[Any] = None
    ) -> list[T]:
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

    async def delete_many(self, model_class: type[Document], query: dict[str, Any]) -> int:
        collection = self._collections.get(self._get_collection_name(model_class))
        if not collection:
            return 0
        q = self._translate_query(query)
        removed = await asyncio.to_thread(collection.remove, q)
        return len(removed)

    async def update_one(self, document: T, update_query: dict[str, Any]) -> T:
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

        async def find_one_patch(query: Optional[dict[str, Any]] = None, **kwargs: Any) -> Optional[Document]:
            return await adapter.find_one(model_class, query or kwargs)

        async def find_many_patch(
            query: Optional[dict[str, Any]] = None,
            limit: int = 0,
            skip: int = 0,
            sort: Optional[Any] = None,
            **kwargs: Any,
        ) -> list[Document]:
            return await adapter.find_many(model_class, query or kwargs, limit, skip, sort)

        async def create_patch(*args: Any, **kwargs: Any) -> Document:
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
            name: str = ""
            id_type: Any = PydanticObjectId
            indexes: list[Any] = []
            motor_collection: Any = None
            pymongo_collection: Any = None
            use_revision: bool = False
            use_cache: bool = False
            use_state_management: bool = False
            is_root: bool = True
            union_doc: Any = None
            bson_encoders: dict[Any, Any] = {}
            validation_schema: Any = None
            projection: Any = None

            def __init__(self, m: type[Document]) -> None:
                self.name = adapter._get_collection_name(m)
                orig_settings = getattr(m, "Settings", None)
                self.indexes = getattr(orig_settings, "indexes", []) if orig_settings else []

            def get_collection(self) -> Any:
                return None

        proxy = SettingsProxy(model_class)
        model_class.get_settings = classmethod(lambda cls: proxy)
        model_class.Settings = proxy
        model_class._settings = proxy

        # Patch motor_collection getter
        model_class.get_motor_collection = classmethod(lambda cls: None)

    # --- IStorageProvider (KV) Implementation ---
    # BeanieTinyDBAdapter is a document-oriented provider; KV operations are not supported.
    # Async variants raise OperationNotSupported consistently with their sync counterparts.

    async def get_async(self, key: str) -> Optional[Any]:
        raise OperationNotSupported("KV operations are not supported by BeanieTinyDBAdapter; use document methods.")

    async def set_async(self, key: str, value: Any) -> bool:
        raise OperationNotSupported("KV operations are not supported by BeanieTinyDBAdapter; use document methods.")

    async def delete_async(self, key: str) -> bool:
        raise OperationNotSupported("KV operations are not supported by BeanieTinyDBAdapter; use document methods.")

    async def exists_async(self, key: str) -> bool:
        raise OperationNotSupported("KV operations are not supported by BeanieTinyDBAdapter; use document methods.")

    async def list_keys_async(self, pattern: Optional[str] = None) -> list[str]:
        raise OperationNotSupported("KV operations are not supported by BeanieTinyDBAdapter; use document methods.")

    async def find_async(self, query: dict[str, Any]) -> list[Any]:
        raise OperationNotSupported("KV operations are not supported by BeanieTinyDBAdapter; use document methods.")

    async def clear_async(self) -> int:
        raise OperationNotSupported("KV operations are not supported by BeanieTinyDBAdapter; use document methods.")

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
        result.sync_direction = direction
        start_time = datetime.now(UTC)
        try:
            if direction == SyncDirection.TO_TARGET:
                # Push all local documents to the target provider
                for model_class in self.document_models:
                    docs = await self.find_many(model_class, {})
                    for doc in docs:
                        if not dry_run:
                            await target_provider.insert_one(doc)
                        result.items_synced += 1

            elif direction == SyncDirection.FROM_TARGET:
                # Pull all documents from target and apply locally
                if isinstance(target_provider, ISyncProvider):
                    sync_data = await target_provider.get_data_for_sync()
                    if not dry_run:
                        applied, conflict_items = await self.apply_sync_data(sync_data, conflict_resolution)
                        result.items_synced = applied
                        result.conflicts_found = len(conflict_items)
                        result.conflicts_resolved = applied
                        result.details["unresolved_conflicts"] = conflict_items
                    else:
                        result.items_synced = len(sync_data)
                else:
                    raise StorageError("FROM_TARGET sync requires target to implement ISyncProvider.")

            elif direction == SyncDirection.BIDIRECTIONAL:
                # Push local docs to target, then pull target docs locally
                if not isinstance(target_provider, ISyncProvider):
                    raise StorageError("BIDIRECTIONAL sync requires target to implement ISyncProvider.")

                for model_class in self.document_models:
                    docs = await self.find_many(model_class, {})
                    for doc in docs:
                        if not dry_run:
                            await target_provider.insert_one(doc)
                        result.items_synced += 1

                sync_data = await target_provider.get_data_for_sync()
                if not dry_run:
                    applied, conflict_items = await self.apply_sync_data(sync_data, conflict_resolution)
                    result.items_synced += applied
                    result.conflicts_found = len(conflict_items)
                    result.conflicts_resolved = applied
                    result.details["unresolved_conflicts"] = conflict_items
                else:
                    result.items_synced += len(sync_data)

            result.success = True
        except Exception as e:
            result.errors.append(str(e))
            result.success = False
        result.sync_duration_seconds = (datetime.now(UTC) - start_time).total_seconds()
        return result

    def get_sync_metadata(self) -> dict[str, Any]:
        return {"provider": "tinydb", "supports_incremental": False, "supports_bidirectional": True}

    async def prepare_for_sync(self) -> bool:
        return True

    async def cleanup_after_sync(self, sync_result: SyncResult) -> None:
        pass

    async def get_data_for_sync(self, last_sync_timestamp: Optional[datetime] = None) -> list[dict[str, Any]]:
        """
        Return all documents (or those updated after last_sync_timestamp) serialised for sync.

        Each item includes a ``_model_class`` key so ``apply_sync_data`` can reconstruct
        the correct type on the receiving end.
        """
        sync_data: list[dict[str, Any]] = []
        for model_class in self.document_models:
            query: dict[str, Any] = {}
            if last_sync_timestamp:
                query = {"updated_at": {"$gt": last_sync_timestamp.isoformat()}}

            docs = await self.find_many(model_class, query)
            for doc in docs:
                data = self._document_to_dict(doc)
                data["_model_class"] = model_class.__name__
                sync_data.append(data)
        return sync_data

    async def apply_sync_data(
        self,
        sync_data: list[dict[str, Any]],
        conflict_resolution: SyncConflictResolution = SyncConflictResolution.NEWEST_WINS,
    ) -> tuple[int, list[dict[str, Any]]]:
        """
        Apply incoming sync data to local TinyDB storage.

        Conflict resolution strategy NEWEST_WINS: if the local document has an
        ``updated_at`` timestamp >= the incoming one, the incoming update is skipped.
        Any item that cannot be applied is recorded in the returned conflicts list.
        """
        count = 0
        conflicts: list[dict[str, Any]] = []

        for item in sync_data:
            item = dict(item)  # do not mutate the caller's data
            model_name = item.pop("_model_class", None)
            if not model_name:
                continue

            model_class = next((m for m in self.document_models if m.__name__ == model_name), None)
            if not model_class:
                continue

            try:
                doc_id = item.get("_id") or item.get("id")
                existing = await self.find_one(model_class, {"id": str(doc_id)}) if doc_id else None

                if existing:
                    if conflict_resolution == SyncConflictResolution.NEWEST_WINS:
                        existing_ts_raw = getattr(existing, "updated_at", None)
                        incoming_ts_raw = item.get("updated_at")
                        if existing_ts_raw is not None and incoming_ts_raw is not None:
                            if isinstance(incoming_ts_raw, str):
                                try:
                                    incoming_ts_raw = datetime.fromisoformat(incoming_ts_raw)
                                except ValueError:
                                    incoming_ts_raw = None
                            if incoming_ts_raw is not None:
                                existing_ts = (
                                    existing_ts_raw
                                    if isinstance(existing_ts_raw, datetime)
                                    else datetime.fromisoformat(str(existing_ts_raw))
                                )
                                if existing_ts >= incoming_ts_raw:
                                    # Local is newer or equal — skip this item
                                    continue
                    await self.update_one(existing, item)
                else:
                    new_doc = model_class(**item)
                    await self.insert_one(new_doc)
                count += 1
            except Exception as e:
                logger.error(f"TinyDB apply_sync_data failed for {model_name}: {e}")
                conflicts.append(item)

        return count, conflicts

    # --- IHealthCheck Implementation ---
    def is_healthy(self) -> bool:
        return self.db_path.exists()

    async def perform_deep_health_check(self) -> bool:
        return self.is_healthy()

    def get_health_status(self) -> dict[str, Any]:
        return self._health_monitor.get_health_status()

    def get_last_health_check(self) -> datetime:
        return self._health_monitor.get_last_health_check()

    # --- IBackupProvider Implementation ---

    async def create_backup(self, backup_path: str, metadata: Optional[dict[str, Any]] = None) -> bool:
        """
        Create a backup by copying the TinyDB JSON file.
        """
        import shutil

        try:
            # Ensure target directory exists
            Path(backup_path).parent.mkdir(parents=True, exist_ok=True)
            # Use asyncio.to_thread for blocking shutil call
            await asyncio.to_thread(shutil.copy2, str(self.db_path), backup_path)
            logger.info(f"Backup created at {backup_path}")
            return True
        except Exception as e:
            logger.error(f"Backup failed: {e}")
            return False

    async def restore_backup(self, backup_path: str, clear_existing: bool = False) -> bool:
        """
        Restore the database by copying from backup.
        """
        import shutil

        try:
            if not Path(backup_path).exists():
                logger.error(f"Backup file not found: {backup_path}")
                return False

            await asyncio.to_thread(shutil.copy2, backup_path, str(self.db_path))

            # Reload DB to reflect new file contents
            if self.db:
                self.db.close()
            self.db = TinyDB(str(self.db_path))

            logger.info(f"Database restored from {backup_path}")
            return True
        except Exception as e:
            logger.error(f"Restore failed: {e}")
            return False

    async def list_backups(self, backup_dir: str) -> dict[str, dict[str, Any]]:
        backups = {}
        dir_path = Path(backup_dir)
        if dir_path.exists():
            for f in dir_path.glob("*.json"):
                backups[f.name] = {
                    "size": f.stat().st_size,
                    "created": datetime.fromtimestamp(f.stat().st_ctime, UTC).isoformat(),
                }
        return backups

    async def validate_backup(self, backup_path: str) -> dict[str, Any]:
        path = Path(backup_path)
        if not path.exists():
            return {"valid": False, "error": "File not found"}

        try:
            # Try to parse the JSON to validate it
            with open(backup_path, encoding="utf-8") as f:
                json.load(f)
            return {"valid": True, "size": path.stat().st_size}
        except Exception as e:
            return {"valid": False, "error": str(e)}

    def close(self) -> None:
        """Close database connection."""
        if self.db:
            self.db.close()
        self._running = False
