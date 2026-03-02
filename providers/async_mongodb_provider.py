from __future__ import annotations

"""
Async MongoDB Provider - ENTERPRISE GRADE.

This provider implements the IDocumentProvider interface using Motor and Beanie ODM,
providing a high-performance, async-native document storage solution.
"""

import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Optional, TypeVar, cast

from beanie import Document, init_beanie
from motor.motor_asyncio import AsyncIOMotorClient

from ..interfaces.backup import IBackupProvider
from ..interfaces.base_document_provider import IDocumentProvider
from ..interfaces.health import HealthMonitor, IHealthCheck
from ..interfaces.storage import StorageError
from ..interfaces.sync import ISyncProvider, SyncConflictResolution, SyncDirection, SyncResult

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=Document)


class AsyncMongoDBProvider(IDocumentProvider, ISyncProvider, IHealthCheck, IBackupProvider):
    """
    MongoDB storage provider using Motor and Beanie.
    """

    def __init__(
        self,
        connection_uri: str,
        database_name: str,
    ):
        self.connection_uri = connection_uri
        self.database_name = database_name
        self.client: Optional[AsyncIOMotorClient[Any]] = None
        self.db: Optional[Any] = None
        self.document_models: list[type[Document]] = []
        self._health_monitor = HealthMonitor()

    async def initialize(self, document_models: Optional[list[type[Document]]] = None) -> None:
        """
        Initialize the MongoDB connection and Beanie ODM.
        """
        try:
            self.client = AsyncIOMotorClient(self.connection_uri)
            self.db = self.client[self.database_name]
            self.document_models = document_models or []

            if self.document_models:
                await init_beanie(database=self.db, document_models=self.document_models)  # type: ignore[arg-type]

            logger.info(
                "AsyncMongoDBProvider initialized",
                extra={"database": self.database_name, "models": [m.__name__ for m in self.document_models]},
            )
            self._health_monitor.update_health(True)
        except Exception as e:
            logger.error(f"Failed to initialize AsyncMongoDBProvider: {e}")
            self._health_monitor.update_health(False, {"error": str(e)})
            raise StorageError(f"Initialization failed: {e}")

    # --- IDocumentProvider Implementation ---

    async def insert_one(self, document: T) -> T:
        try:
            return cast(T, await document.insert())
        except Exception as e:
            logger.error(f"MongoDB InsertOne failed: {e}")
            raise StorageError(f"Insert failed: {e}")

    async def insert_many(self, documents: list[T]) -> list[T]:
        try:
            if not documents:
                return []
            model_class = type(documents[0])
            await model_class.insert_many(documents)
            return documents
        except Exception as e:
            logger.error(f"MongoDB InsertMany failed: {e}")
            raise StorageError(f"InsertMany failed: {e}")

    async def find_one(self, model_class: type[T], query: dict[str, Any]) -> Optional[T]:  # type: ignore[override]
        try:
            return await model_class.find_one(query)
        except Exception as e:
            logger.error(f"MongoDB FindOne failed: {e}")
            raise StorageError(f"FindOne failed: {e}")

    async def find_many(  # type: ignore[override]
        self,
        model_class: type[T],
        query: dict[str, Any],
        limit: int = 0,
        skip: int = 0,
        sort: Optional[Any] = None,
    ) -> list[T]:
        try:
            q = model_class.find(query)
            if sort:
                q = q.sort(sort)
            if skip > 0:
                q = q.skip(skip)
            if limit > 0:
                q = q.limit(limit)
            return await q.to_list()
        except Exception as e:
            logger.error(f"MongoDB FindMany failed: {e}")
            raise StorageError(f"FindMany failed: {e}")

    async def delete_one(self, document: T) -> bool:
        try:
            await document.delete()
            return True
        except Exception as e:
            logger.error(f"MongoDB DeleteOne failed: {e}")
            raise StorageError(f"Delete failed: {e}")

    async def delete_many(self, model_class: type[T], query: dict[str, Any]) -> int:
        try:
            res = await model_class.find(query).delete()
            return res.deleted_count if res else 0
        except Exception as e:
            logger.error(f"MongoDB DeleteMany failed: {e}")
            raise StorageError(f"DeleteMany failed: {e}")

    async def update_one(self, document: T, update_query: dict[str, Any]) -> T:
        try:
            await document.update(update_query)
            return document
        except Exception as e:
            logger.error(f"MongoDB UpdateOne failed: {e}")
            raise StorageError(f"Update failed: {e}")

    # --- IStorageProvider (KV) Implementation ---

    async def get_async(self, key: str) -> Optional[Any]:
        """
        Retrieves a value from MongoDB using a simple KV abstraction.
        """
        try:
            if not self.db:
                return None
            collection = self.db["key_value_store"]
            doc = await collection.find_one({"_id": key})
            return doc.get("value") if doc else None
        except Exception as e:
            logger.error(f"MongoDB Get KV failed: {e}")
            return None

    async def set_async(self, key: str, value: Any) -> bool:
        """
        Sets a value in MongoDB using a simple KV abstraction.
        """
        try:
            if not self.db:
                return False
            collection = self.db["key_value_store"]
            await collection.replace_one({"_id": key}, {"value": value}, upsert=True)
            return True
        except Exception as e:
            logger.error(f"MongoDB Set KV failed: {e}")
            return False

    async def delete_async(self, key: str) -> bool:
        """
        Deletes a key from MongoDB.
        """
        try:
            if not self.db:
                return False
            collection = self.db["key_value_store"]
            result = await collection.delete_one({"_id": key})
            return (result.deleted_count > 0) if result else False
        except Exception as e:
            logger.error(f"MongoDB Delete KV failed: {e}")
            return False

    async def exists_async(self, key: str) -> bool:
        """
        Checks if a key exists in MongoDB.
        """
        try:
            if not self.db:
                return False
            collection = self.db["key_value_store"]
            count = await collection.count_documents({"_id": key}, limit=1)
            return bool(count > 0)
        except Exception as e:
            logger.error(f"MongoDB Exists KV failed: {e}")
            return False

    async def list_keys_async(self, pattern: Optional[str] = None) -> list[str]:
        """
        Lists keys matching the pattern.
        """
        try:
            if not self.db:
                return []
            collection = self.db["key_value_store"]
            query = {}
            if pattern:
                query = {"_id": {"$regex": pattern.replace("*", ".*")}}

            cursor = collection.find(query, {"_id": 1})
            return [doc["_id"] async for doc in cursor]
        except Exception as e:
            logger.error(f"MongoDB ListKeys failed: {e}")
            return []

    async def find_async(self, query: dict[str, Any]) -> list[Any]:
        """
        Finds values matching the query in the KV store.
        """
        try:
            if not self.db:
                return []
            collection = self.db["key_value_store"]
            cursor = collection.find(query)
            return [doc.get("value") async for doc in cursor]
        except Exception as e:
            logger.error(f"MongoDB Find KV failed: {e}")
            return []

    async def clear_async(self) -> int:
        """
        Clears the KV store.
        """
        try:
            if not self.db:
                return 0
            collection = self.db["key_value_store"]
            result = await collection.delete_many({})
            return result.deleted_count if result else 0
        except Exception as e:
            logger.error(f"MongoDB Clear KV failed: {e}")
            return 0

    # Required sync methods (fallbacks)
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
        return True

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

            result.success = True
        except Exception as e:
            result.errors.append(str(e))
            result.success = False

        result.sync_duration_seconds = (datetime.now(UTC) - start_time).total_seconds()
        return result

    def get_sync_metadata(self) -> dict[str, Any]:
        return {"provider": "mongodb", "supports_incremental": True, "supports_bidirectional": True}

    async def prepare_for_sync(self) -> bool:
        return True

    async def cleanup_after_sync(self, sync_result: SyncResult) -> None:
        pass

    async def get_data_for_sync(self, last_sync_timestamp: Optional[datetime] = None) -> list[dict[str, Any]]:
        sync_data = []
        for model_class in self.document_models:
            query = {}
            if last_sync_timestamp:
                # Assuming models have an 'updated_at' field
                query = {"updated_at": {"$gt": last_sync_timestamp}}

            docs = await self.find_many(model_class, query)
            for doc in docs:
                data = doc.model_dump(mode="json")
                data["_model_class"] = model_class.__name__
                sync_data.append(data)
        return sync_data

    async def apply_sync_data(
        self,
        sync_data: list[dict[str, Any]],
        conflict_resolution: SyncConflictResolution = SyncConflictResolution.NEWEST_WINS,
    ) -> tuple[int, list[dict[str, Any]]]:
        count = 0
        conflicts: list[dict[str, Any]] = []

        for item in sync_data:
            model_name = item.pop("_model_class", None)
            if not model_name:
                continue

            model_class = next((m for m in self.document_models if m.__name__ == model_name), None)
            if not model_class:
                continue

            try:
                doc_id = item.get("_id") or item.get("id")
                existing = await model_class.get(doc_id)

                if existing:
                    if conflict_resolution == SyncConflictResolution.NEWEST_WINS:
                        # Compare updated_at timestamps; skip if local version is newer or equal
                        existing_ts = getattr(existing, "updated_at", None)
                        incoming_ts = item.get("updated_at")
                        if existing_ts and incoming_ts:
                            if isinstance(incoming_ts, str):
                                try:
                                    incoming_ts = datetime.fromisoformat(incoming_ts)
                                except ValueError:
                                    incoming_ts = None
                            if incoming_ts and existing_ts >= incoming_ts:
                                continue
                    await existing.update({"$set": item})
                else:
                    new_doc = model_class(**item)
                    await new_doc.insert()
                count += 1
            except Exception as e:
                logger.error(f"Sync apply failed for {model_name}: {e}")
                conflicts.append(item)

        return count, conflicts

    # --- IHealthCheck Implementation ---

    def is_healthy(self) -> bool:
        return self._health_monitor.is_healthy()

    async def perform_deep_health_check(self) -> bool:
        try:
            if self.client:
                await self.client.admin.command("ping")
                return True
            return False
        except Exception:
            return False

    def get_health_status(self) -> dict[str, Any]:
        return self._health_monitor.get_health_status()

    def get_last_health_check(self) -> datetime:
        return self._health_monitor.get_last_health_check()

    # --- IBackupProvider Implementation ---

    async def create_backup(self, backup_path: str, metadata: Optional[dict[str, Any]] = None) -> bool:
        """
        Create a backup by exporting collections to JSON.
        """
        try:
            backup_dir = Path(backup_path)
            backup_dir.mkdir(parents=True, exist_ok=True)

            for model_class in self.document_models:
                docs = await self.find_many(model_class, {})
                data = [doc.model_dump(mode="json") for doc in docs]

                file_path = backup_dir / f"{model_class.__name__}.json"
                with open(file_path, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2)

            logger.info(f"MongoDB backup created at {backup_path}")
            return True
        except Exception as e:
            logger.error(f"MongoDB backup failed: {e}")
            return False

    async def restore_backup(self, backup_path: str, clear_existing: bool = False) -> bool:
        """
        Restore data from JSON backup files.
        """
        try:
            backup_dir = Path(backup_path)
            if not backup_dir.exists():
                return False

            for model_class in self.document_models:
                file_path = backup_dir / f"{model_class.__name__}.json"
                if file_path.exists():
                    if clear_existing:
                        await self.delete_many(model_class, {})

                    with open(file_path, encoding="utf-8") as f:
                        data = json.load(f)

                    docs = [model_class(**item) for item in data]
                    await self.insert_many(docs)

            logger.info(f"MongoDB restored from {backup_path}")
            return True
        except Exception as e:
            logger.error(f"MongoDB restore failed: {e}")
            return False

    async def list_backups(self, backup_dir: str) -> dict[str, dict[str, Any]]:
        backups = {}
        dir_path = Path(backup_dir)
        if dir_path.exists():
            for d in dir_path.iterdir():
                if d.is_dir():
                    backups[d.name] = {
                        "type": "mongodb_json",
                        "created": datetime.fromtimestamp(d.stat().st_ctime, UTC).isoformat(),
                    }
        return backups

    async def validate_backup(self, backup_path: str) -> dict[str, Any]:
        path = Path(backup_path)
        if not path.exists() or not path.is_dir():
            return {"valid": False, "error": "Backup directory not found"}

        return {"valid": True, "type": "mongodb_json"}
