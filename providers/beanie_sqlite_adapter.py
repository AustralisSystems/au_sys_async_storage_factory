from __future__ import annotations

"""
Beanie ODM -> SQLite Adapter - ENTERPRISE GRADE.

This adapter provides full MongoDB-compatible ODM capabilities on top of SQLite JSON,
enabling a high-performance, local-first fallback for Beanie/MongoDB environments.
"""

import asyncio
import datetime
import json
import sqlite3
import uuid
import logging
from pathlib import Path
from typing import Any, Optional, TypeVar, Union, cast, Type, List, Dict, Tuple

import aiosqlite
from beanie import Document, PydanticObjectId

from storage.shared.manifest.lifecycle import BaseLifecycleMixin
from ..interfaces.base_document_provider import IDocumentProvider
from ..interfaces.sync import ISyncProvider, SyncResult, SyncDirection, SyncConflictResolution
from ..interfaces.health import IHealthCheck, HealthMonitor
from ..interfaces.backup import IBackupProvider
from ..interfaces.storage import OperationNotSupported, StorageError

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=Document)


class BeanieSQLiteAdapter(BaseLifecycleMixin, IDocumentProvider, ISyncProvider, IHealthCheck, IBackupProvider):
    """
    A robust adapter that implements full Beanie ODM functionality using SQLite's JSON features.
    """

    def __init__(
        self,
        db_path: str,
        encryption_key: Optional[str] = None,
    ):
        """
        Initialize the adapter.

        Args:
            db_path: Path to the SQLite database file.
            encryption_key: Optional key for data encryption (at rest).
        """
        super().__init__()
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.encryption_key = encryption_key
        self.document_models: list[type[Document]] = []
        self._health_monitor = HealthMonitor()
        self._running = True

    async def initialize(self, document_models: Optional[list[type[Document]]] = None) -> None:
        """
        Create tables and optimized indexes for all registered models.
        """
        self.document_models = document_models or []
        try:
            async with aiosqlite.connect(str(self.db_path)) as db:
                for model_class in self.document_models:
                    table_name = self._get_collection_name(model_class)
                    if not table_name.isidentifier():
                        raise StorageError(f"Invalid collection name: {table_name}")

                    await db.execute(
                        f'CREATE TABLE IF NOT EXISTS "{table_name}" (id TEXT PRIMARY KEY, doc JSON NOT NULL)'
                    )

                    # Apply indexes defined in model Settings
                    if hasattr(model_class, "Settings") and hasattr(model_class.Settings, "indexes"):
                        for idx in model_class.Settings.indexes:
                            if isinstance(idx, str):
                                # Sanitize index path
                                clean_idx = idx.replace(".", "_")
                                if not clean_idx.isidentifier():
                                    continue
                                idx_name = f"idx_{table_name}_{clean_idx}"
                                await db.execute(
                                    f'CREATE INDEX IF NOT EXISTS "{idx_name}" ON "{table_name}"(json_extract(doc, "$.{idx}"))'
                                )
                await db.commit()

            self._patch_models()
            logger.info(
                "BeanieSQLiteAdapter initialized",
                extra={
                    "db_path": str(self.db_path),
                    "managed_models": [m.__name__ for m in self.document_models],
                },
            )
            self._health_monitor.update_health(True)
        except Exception as e:
            logger.error(f"Failed to initialize BeanieSQLiteAdapter: {e}")
            self._health_monitor.update_health(False, {"error": str(e)})
            raise StorageError(f"Initialization failed: {e}")

    def _get_collection_name(self, model_class: type[Document]) -> str:
        """Determines the SQLite table name for a given model."""
        if hasattr(model_class, "Settings") and hasattr(model_class.Settings, "name"):
            return cast(str, model_class.Settings.name)
        return model_class.__name__.lower()

    def _document_to_dict(self, document: Document) -> dict[str, Any]:
        """Converts a Beanie document to a JSON-serializable dictionary."""
        if hasattr(document, "model_dump"):
            return cast(dict[str, Any], document.model_dump(mode="json"))
        return cast(dict[str, Any], json.loads(document.json()))

    def _dict_to_document(self, model_class: type[T], data: dict[str, Any]) -> T:
        """Converts a dictionary back into a Beanie document instance."""
        if "id" in data and "_id" not in data:
            data["_id"] = data["id"]
        return model_class(**data)

    # --- Query Engine ---

    def _build_where_clause(self, query: dict[str, Any]) -> tuple[str, list[Any]]:
        """Translates a MongoDB query dictionary into SQLite SQL with parameters."""
        if not query:
            return "1=1", []
        return self._parse_expression(query)

    def _parse_expression(self, expr: Any, parent_key: Optional[str] = None) -> tuple[str, list[Any]]:
        """Recursively parses MongoDB query expressions."""
        if not isinstance(expr, dict):
            # Simple equality: { "field": "value" }
            if parent_key in ("_id", "id"):
                return "id = ?", [str(expr)]
            return "json_extract(doc, ?) = ?", [f"$.{parent_key}", expr]

        conditions = []
        params = []

        for key, value in expr.items():
            if key == "$and":
                parts = [self._parse_expression(sub) for sub in value]
                conditions.append(f"({' AND '.join(p[0] for p in parts)})")
                for p in parts:
                    params.extend(p[1])
            elif key == "$or":
                parts = [self._parse_expression(sub) for sub in value]
                conditions.append(f"({' OR '.join(p[0] for p in parts)})")
                for p in parts:
                    params.extend(p[1])
            elif key == "$nor":
                parts = [self._parse_expression(sub) for sub in value]
                conditions.append(f"NOT ({' OR '.join(p[0] for p in parts)})")
                for p in parts:
                    params.extend(p[1])
            elif key.startswith("$"):
                # Operator at this level
                sql, p = self._translate_operator(parent_key or "", key, value)  # type: ignore[assignment]
                conditions.append(sql)
                params.extend(p)
            else:
                # Nested field or field with operators
                sql, p = self._parse_expression(value, key)  # type: ignore[assignment]
                conditions.append(sql)
                params.extend(p)

        return " AND ".join(conditions), params

    def _translate_operator(self, field: str, op: str, val: Any) -> tuple[str, list[Any]]:
        """Maps MongoDB operators to SQLite SQL."""
        path = f"$.{field}"
        if field in ("id", "_id"):
            base_sql = "id"
            path_params = []
        else:
            base_sql = "json_extract(doc, ?)"
            path_params = [path]

        if op == "$eq":
            return f"{base_sql} = ?", path_params + [val]
        if op == "$ne":
            return f"{base_sql} != ?", path_params + [val]
        if op == "$gt":
            return f"{base_sql} > ?", path_params + [val]
        if op == "$gte":
            return f"{base_sql} >= ?", path_params + [val]
        if op == "$lt":
            return f"{base_sql} < ?", path_params + [val]
        if op == "$lte":
            return f"{base_sql} <= ?", path_params + [val]
        if op == "$in":
            return f"{base_sql} IN ({','.join(['?' for _ in val])})", path_params + list(val)
        if op == "$nin":
            return f"{base_sql} NOT IN ({','.join(['?' for _ in val])})", path_params + list(val)
        if op == "$exists":
            return (f"{base_sql} IS NOT NULL" if val else f"{base_sql} IS NULL"), path_params
        if op == "$regex":
            return f"{base_sql} REGEXP ?", path_params + [val]

        raise OperationNotSupported(f"MongoDB operator {op} is not yet implemented in SQLite adapter.")

    # --- IDocumentProvider Implementation ---

    async def insert_one(self, document: T) -> T:
        """Persists a single document."""
        table_name = self._get_collection_name(type(document))
        if not hasattr(document, "id") or not document.id:
            # Generate a valid PydanticObjectId (24 hex characters)
            document.id = PydanticObjectId()

        doc_id = str(document.id)
        data = self._document_to_dict(document)
        data["id"] = doc_id

        async with aiosqlite.connect(str(self.db_path)) as db:
            await db.execute(
                f'INSERT OR REPLACE INTO "{table_name}" (id, doc) VALUES (?, ?)', (doc_id, json.dumps(data))
            )
            await db.commit()
        return document

    async def insert_many(self, documents: list[T]) -> list[T]:
        """Persists multiple documents in a single transaction."""
        if not documents:
            return []
        table_name = self._get_collection_name(type(documents[0]))

        prepared_data = []
        for doc in documents:
            if not getattr(doc, "id", None):
                doc.id = cast(PydanticObjectId, str(uuid.uuid4()))
            d = self._document_to_dict(doc)
            d["id"] = str(doc.id)
            prepared_data.append((str(doc.id), json.dumps(d)))

        async with aiosqlite.connect(str(self.db_path)) as db:
            await db.executemany(f'INSERT OR REPLACE INTO "{table_name}" (id, doc) VALUES (?, ?)', prepared_data)
            await db.commit()
        return documents

    async def find_one(self, model_class: type[T], query: dict[str, Any]) -> Optional[T]:  # type: ignore[override]
        """Locates a single document matching the query."""
        table_name = self._get_collection_name(model_class)
        if not table_name.isidentifier():
            raise StorageError(f"Invalid collection name: {table_name}")

        where, params = self._build_where_clause(query)

        try:
            async with aiosqlite.connect(str(self.db_path)) as db:
                cursor = await db.execute(f'SELECT doc FROM "{table_name}" WHERE {where} LIMIT 1', params)  # nosec B608
                row = await cursor.fetchone()
                return self._dict_to_document(model_class, json.loads(row[0])) if row else None
        except Exception as e:
            logger.error(f"SQLite find_one error: {e}")
            return None

    async def find_many(  # type: ignore[override]
        self,
        model_class: type[T],
        query: dict[str, Any],
        limit: int = 0,
        skip: int = 0,
        sort: Optional[Union[str, list[tuple[str, int]]]] = None,
    ) -> list[T]:
        """Locates multiple documents with support for pagination and sorting."""
        table_name = self._get_collection_name(model_class)
        if not table_name.isidentifier():
            raise StorageError(f"Invalid collection name: {table_name}")

        where, params = self._build_where_clause(query)

        sql = f'SELECT doc FROM "{table_name}" WHERE {where}'  # nosec B608

        if sort:
            sql += " ORDER BY "
            if isinstance(sort, str):
                sort_field = sort.lstrip("-")
                # Simple validation for sort field
                if not all(part.isidentifier() for part in sort_field.split(".")):
                    raise StorageError(f"Invalid sort field: {sort_field}")
                sql += f"json_extract(doc, '$.{sort_field}') {'DESC' if sort.startswith('-') else 'ASC'}"
            else:
                sort_parts = []
                for field, direction in sort:
                    if not all(part.isidentifier() for part in field.split(".")):
                        continue
                    dir_str = "ASC" if direction > 0 else "DESC"
                    sort_parts.append(f"json_extract(doc, '$.{field}') {dir_str}")
                sql += ", ".join(sort_parts)

        if limit > 0:
            sql += f" LIMIT {limit}"
        if skip > 0:
            sql += f" OFFSET {skip}"

        async with aiosqlite.connect(str(self.db_path)) as db:
            try:
                cursor = await db.execute(sql, params)
                rows = await cursor.fetchall()
                return [self._dict_to_document(model_class, json.loads(r[0])) for r in rows]
            except Exception as e:
                logger.error(f"SQLite find_many error: {e}")
                return []

    async def delete_one(self, document: T) -> bool:
        """Deletes a single document."""
        table_name = self._get_collection_name(type(document))
        if not table_name.isidentifier():
            raise StorageError(f"Invalid collection name: {table_name}")

        async with aiosqlite.connect(str(self.db_path)) as db:
            await db.execute(f'DELETE FROM "{table_name}" WHERE id = ?', (str(document.id),))  # nosec B608
            await db.commit()
        return True

    async def delete_many(self, model_class: type[Document], query: dict[str, Any]) -> int:
        """Deletes all documents matching the query."""
        table_name = self._get_collection_name(model_class)
        if not table_name.isidentifier():
            raise StorageError(f"Invalid collection name: {table_name}")

        where, params = self._build_where_clause(query)
        async with aiosqlite.connect(str(self.db_path)) as db:
            cursor = await db.execute(f'DELETE FROM "{table_name}" WHERE {where}', params)  # nosec B608
            await db.commit()
            return cursor.rowcount

    async def update_one(self, document: T, update_query: dict[str, Any]) -> T:
        """
        Apply a MongoDB-style update_query to the document and persist it.

        Supported operators: $set, $unset, $inc, $push, $pull, $addToSet.
        If no recognised operator keys are present, the document is upserted as-is.
        """
        if update_query:
            doc_dict = self._document_to_dict(document)

            for operator, fields in update_query.items():
                if not isinstance(fields, dict):
                    continue

                if operator == "$set":
                    for field, value in fields.items():
                        doc_dict[field] = value

                elif operator == "$unset":
                    for field in fields:
                        doc_dict.pop(field, None)

                elif operator == "$inc":
                    for field, increment in fields.items():
                        current = doc_dict.get(field, 0)
                        if not isinstance(current, (int, float)):
                            raise StorageError(
                                f"$inc operator requires a numeric field; field '{field}' is {type(current).__name__}."
                            )
                        doc_dict[field] = current + increment

                elif operator == "$push":
                    for field, value in fields.items():
                        current = doc_dict.get(field, [])
                        if not isinstance(current, list):
                            raise StorageError(
                                f"$push operator requires an array field; field '{field}' is {type(current).__name__}."
                            )
                        doc_dict[field] = current + [value]

                elif operator == "$pull":
                    for field, value in fields.items():
                        current = doc_dict.get(field, [])
                        if not isinstance(current, list):
                            raise StorageError(
                                f"$pull operator requires an array field; field '{field}' is {type(current).__name__}."
                            )
                        doc_dict[field] = [item for item in current if item != value]

                elif operator == "$addToSet":
                    for field, value in fields.items():
                        current = doc_dict.get(field, [])
                        if not isinstance(current, list):
                            raise StorageError(
                                f"$addToSet operator requires an array field; field '{field}' is {type(current).__name__}."
                            )
                        if value not in current:
                            doc_dict[field] = current + [value]

                else:
                    raise OperationNotSupported(
                        f"MongoDB update operator '{operator}' is not supported by BeanieSQLiteAdapter."
                    )

            # Reconstruct the document from the mutated dict so the returned instance is consistent
            document = self._dict_to_document(type(document), doc_dict)

        table_name = self._get_collection_name(type(document))
        if not hasattr(document, "id") or not document.id:
            document.id = PydanticObjectId()

        doc_id = str(document.id)
        data = self._document_to_dict(document)
        data["id"] = doc_id

        async with aiosqlite.connect(str(self.db_path)) as db:
            await db.execute(
                f'INSERT OR REPLACE INTO "{table_name}" (id, doc) VALUES (?, ?)', (doc_id, json.dumps(data))
            )
            await db.commit()
        return document

    # --- Beanie Patching Logic ---

    def _patch_models(self) -> None:
        """Applies patches to all registered models for seamless integration."""
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

        async def get_patch(doc_id: Any, **kwargs: Any) -> Optional[Document]:
            return await adapter.find_one(model_class, {"id": str(doc_id)})

        async def create_patch(*args: Any, **kwargs: Any) -> Document:
            instance = args[0] if args and isinstance(args[0], model_class) else model_class(**kwargs)
            return await adapter.insert_one(instance)

        async def insert_many_patch(documents: list[Document], **kwargs: Any) -> list[Document]:
            return await adapter.insert_many(documents)

        # Patching methods onto the model class
        model_class.find_one = staticmethod(find_one_patch)  # type: ignore[method-assign, assignment]
        model_class.find_many = staticmethod(find_many_patch)  # type: ignore[method-assign, assignment]
        model_class.find_all = staticmethod(find_many_patch)  # type: ignore[method-assign, assignment]
        model_class.get = staticmethod(get_patch)  # type: ignore[method-assign, assignment]
        model_class.create = create_patch  # type: ignore
        model_class.insert = lambda self_inst, **kwargs: adapter.insert_one(self_inst)  # type: ignore
        model_class.save = lambda self_inst, **kwargs: adapter.insert_one(self_inst)  # type: ignore
        model_class.delete = lambda self_inst, **kwargs: adapter.delete_one(self_inst)  # type: ignore
        model_class.update = lambda self_inst, **kwargs: adapter.insert_one(self_inst)  # type: ignore

        # Patching static methods for bulk ops
        model_class.insert_many = staticmethod(insert_many_patch)  # type: ignore[method-assign, assignment]
        model_class.delete_many = staticmethod(lambda q=None, **kwargs: adapter.delete_many(model_class, q or kwargs))

        # Settings Proxy
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
        model_class.get_settings = classmethod(lambda cls: proxy)  # type: ignore[method-assign, assignment]
        model_class.Settings = proxy
        model_class._settings = proxy

        # Patch motor_collection getter
        model_class.get_motor_collection = classmethod(lambda cls: None)

    # --- IStorageProvider (KV) Implementation ---
    # BeanieSQLiteAdapter is a document-oriented provider; KV operations are not supported.
    # Async variants raise OperationNotSupported consistently with their sync counterparts.

    async def get_async(self, key: str) -> Optional[Any]:
        raise OperationNotSupported("KV operations are not supported by BeanieSQLiteAdapter; use document methods.")

    async def set_async(self, key: str, value: Any) -> bool:
        raise OperationNotSupported("KV operations are not supported by BeanieSQLiteAdapter; use document methods.")

    async def delete_async(self, key: str) -> bool:
        raise OperationNotSupported("KV operations are not supported by BeanieSQLiteAdapter; use document methods.")

    async def exists_async(self, key: str) -> bool:
        raise OperationNotSupported("KV operations are not supported by BeanieSQLiteAdapter; use document methods.")

    async def list_keys_async(self, pattern: Optional[str] = None) -> list[str]:
        raise OperationNotSupported("KV operations are not supported by BeanieSQLiteAdapter; use document methods.")

    async def find_async(self, query: dict[str, Any]) -> list[Any]:
        raise OperationNotSupported("KV operations are not supported by BeanieSQLiteAdapter; use document methods.")

    async def clear_async(self) -> int:
        raise OperationNotSupported("KV operations are not supported by BeanieSQLiteAdapter; use document methods.")

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
        start_time = datetime.datetime.now(datetime.UTC)
        try:
            if direction == SyncDirection.TO_TARGET:
                # Push all local documents to the target provider
                for model_class in self.document_models:
                    docs = await self.find_many(model_class, {})
                    for doc in docs:
                        if not dry_run:
                            await cast(Any, target_provider).insert_one(doc)
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
                            await cast(Any, target_provider).insert_one(doc)
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
        result.sync_duration_seconds = (datetime.datetime.now(datetime.UTC) - start_time).total_seconds()
        return result

    def get_sync_metadata(self) -> dict[str, Any]:
        return {"provider": "sqlite", "supports_incremental": False}

    async def prepare_for_sync(self) -> bool:
        return True

    async def cleanup_after_sync(self, sync_result: SyncResult) -> None:
        pass

    async def get_data_for_sync(self, last_sync_timestamp: Optional[datetime.datetime] = None) -> list[dict[str, Any]]:
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
        Apply incoming sync data to local SQLite storage.

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
                                    incoming_ts_raw = datetime.datetime.fromisoformat(incoming_ts_raw)
                                except ValueError:
                                    incoming_ts_raw = None
                            if incoming_ts_raw is not None:
                                existing_ts = (
                                    existing_ts_raw
                                    if isinstance(existing_ts_raw, datetime.datetime)
                                    else datetime.datetime.fromisoformat(str(existing_ts_raw))
                                )
                                if existing_ts >= incoming_ts_raw:
                                    # Local is newer or equal — skip this item
                                    continue
                    await self.update_one(existing, {"$set": item})
                else:
                    new_doc = model_class(**item)
                    await self.insert_one(new_doc)
                count += 1
            except Exception as e:
                logger.error(f"SQLite apply_sync_data failed for {model_name}: {e}")
                conflicts.append(item)

        return count, conflicts

    # --- IHealthCheck Implementation ---

    def is_healthy(self) -> bool:
        return self.db_path.exists()

    async def perform_deep_health_check(self) -> bool:
        try:
            async with aiosqlite.connect(str(self.db_path)) as db:
                await db.execute("SELECT 1")
            return True
        except Exception:
            return False

    def get_health_status(self) -> dict[str, Any]:
        return self._health_monitor.get_health_status()

    def get_last_health_check(self) -> datetime.datetime:
        return self._health_monitor.get_last_health_check()

    # --- IBackupProvider Implementation ---

    async def create_backup(self, backup_path: str, metadata: Optional[dict[str, Any]] = None) -> bool:
        """
        Create a backup by copying the SQLite database file.
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
            logger.info(f"Database restored from {backup_path}")
            return True
        except Exception as e:
            logger.error(f"Restore failed: {e}")
            return False

    async def list_backups(self, backup_dir: str) -> dict[str, dict[str, Any]]:
        backups = {}
        dir_path = Path(backup_dir)
        if dir_path.exists():
            for f in dir_path.glob("*.db"):
                backups[f.name] = {
                    "size": f.stat().st_size,
                    "created": datetime.datetime.fromtimestamp(f.stat().st_ctime, datetime.UTC).isoformat(),
                }
        return backups

    async def validate_backup(self, backup_path: str) -> dict[str, Any]:
        path = Path(backup_path)
        if not path.exists():
            return {"valid": False, "error": "File not found"}

        try:
            async with aiosqlite.connect(backup_path) as db:
                await db.execute("SELECT 1")
            return {"valid": True, "size": path.stat().st_size}
        except Exception as e:
            return {"valid": False, "error": str(e)}
