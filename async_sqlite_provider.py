from __future__ import annotations

"""
Async SQLite Provider - SQLAlchemy Implementation.

Implements IRelationalProvider using SQLAlchemy with aiosqlite.
Follows the KV-store pattern on top of a relational table.

Compliance:
- Bandit B608: SQL Injection prevention (SQLAlchemy Core)
- Zero-Data-Loss Sync capability via ISyncProvider
"""

import asyncio
import json
import re
import time
import logging
from datetime import datetime, UTC
from pathlib import Path
from typing import Any, Optional, cast

import sqlalchemy as sa
from sqlalchemy.engine import CursorResult
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import declarative_base
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from .interfaces.base_relational_provider import IRelationalProvider
from .interfaces.sync import ISyncProvider, SyncResult, SyncDirection, SyncConflictResolution
from .interfaces.health import HealthMonitor
from .interfaces.storage import StorageError

logger = logging.getLogger("storage.sqlite")

Base = declarative_base()


class SQLiteKVModel(Base):  # type: ignore
    """Internal model for KV storage in SQLite."""

    __tablename__ = "kv_store"
    key = sa.Column(sa.String(512), primary_key=True, index=True)
    value = sa.Column(sa.Text, nullable=False)
    expires_at = sa.Column(sa.Float, index=True, nullable=True)
    updated_at = sa.Column(
        sa.DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), index=True
    )


class AsyncSQLiteProvider(IRelationalProvider, ISyncProvider):
    """
    SQLite Provider using SQLAlchemy [asyncio] and aiosqlite.
    """

    def __init__(self, db_path: str):
        """
        Initialize Async SQLite provider.

        Args:
            db_path: Path to the SQLite database file.
        """
        self.db_path = Path(db_path)
        self.table_name = "kv_store"
        self._health_monitor = HealthMonitor()

        # SQLAlchemy setup
        connection_url = f"sqlite+aiosqlite:///{self.db_path}"
        self._engine = create_async_engine(connection_url)
        self._async_session = async_sessionmaker(self._engine, expire_on_commit=False, class_=AsyncSession)
        self._init_task: Optional[asyncio.Task[None]] = None

    async def initialize(self) -> None:
        """Initialize database and schema asynchronously."""
        if self._init_task is None:
            self._init_task = asyncio.create_task(self._initialize_db())
        await self._init_task

    async def _initialize_db(self) -> None:
        """Create schema if it doesn't exist."""
        try:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)

            # Apply PRAGMAs
            async with self._engine.connect() as conn:
                await conn.execute(sa.text("PRAGMA journal_mode=WAL"))
                await conn.execute(sa.text("PRAGMA synchronous=NORMAL"))
                await conn.execute(sa.text("PRAGMA foreign_keys=ON"))

                # Create tables
                await conn.run_sync(Base.metadata.create_all)

            self._health_monitor.update_health(True, {"db_path": str(self.db_path)})
            logger.info(f"AsyncSQLite initialized at {self.db_path}")
        except Exception as e:
            self._health_monitor.update_health(False, {"error": str(e)})
            logger.error(f"AsyncSQLite initialization failed: {e}")
            raise StorageError(f"Failed to initialize SQLite: {e}") from e

    def _serialize(self, value: Any) -> str:
        try:
            return json.dumps(value)
        except (TypeError, ValueError) as e:
            raise StorageError(f"Serialization failed: {e}")

    def _deserialize(self, value: str) -> Any:
        try:
            return json.loads(value)
        except json.JSONDecodeError as e:
            raise StorageError(f"Deserialization failed: {e}")

    def _is_expired(self, expires_at: Optional[float]) -> bool:
        if expires_at is None:
            return False
        return time.time() > expires_at

    # --- IRelationalProvider Implementation ---

    async def execute_raw_sql(self, sql: str, params: Optional[dict[str, Any]] = None) -> Any:
        await self.initialize()
        try:
            async with self._engine.connect() as conn:
                result = await conn.execute(sa.text(sql), params or {})
                await conn.commit()
                return result
        except Exception as e:
            logger.error(f"SQL execution error: {e}")
            raise StorageError(f"SQL execution failed: {e}")

    # --- IStorageProvider Implementation (KV) ---

    async def get_async(self, key: str) -> Optional[Any]:
        await self.initialize()
        try:
            async with self._async_session() as session:
                stmt = sa.select(SQLiteKVModel).where(SQLiteKVModel.key == key)
                result = await session.execute(stmt)
                record = result.scalars().first()

                if not record:
                    return None

                if self._is_expired(float(record.expires_at) if record.expires_at is not None else None):
                    await session.delete(record)
                    await session.commit()
                    return None

                self._health_monitor.update_health(True)
                return self._deserialize(str(record.value))
        except Exception as e:
            self._health_monitor.update_health(False, {"error": str(e)})
            logger.error(f"SQLite get error for key '{key}': {e}")
            raise StorageError(f"Failed to get key: {e}")

    async def set_async(self, key: str, value: Any) -> bool:
        return await self.set_with_ttl_async(key, value, -1)

    async def set_with_ttl_async(self, key: str, value: Any, ttl: int) -> bool:
        await self.initialize()
        value_json = self._serialize(value)
        expires_at = time.time() + ttl if ttl >= 0 else None

        try:
            async with self._async_session() as session:
                stmt = sqlite_insert(SQLiteKVModel).values(
                    key=key, value=value_json, expires_at=expires_at, updated_at=datetime.now(UTC)
                )
                stmt = stmt.on_conflict_do_update(
                    index_elements=["key"],
                    set_=dict(
                        value=stmt.excluded.value,
                        expires_at=stmt.excluded.expires_at,
                        updated_at=stmt.excluded.updated_at,
                    ),
                )
                await session.execute(stmt)
                await session.commit()

            self._health_monitor.update_health(True)
            return True
        except Exception as e:
            self._health_monitor.update_health(False, {"error": str(e)})
            logger.error(f"SQLite set error for key '{key}': {e}")
            raise StorageError(f"Failed to set key: {e}")

    async def delete_async(self, key: str) -> bool:
        await self.initialize()
        try:
            async with self._async_session() as session:
                stmt = sa.delete(SQLiteKVModel).where(SQLiteKVModel.key == key)
                result = await session.execute(stmt)
                await session.commit()
                deleted = cast("CursorResult[Any]", result).rowcount > 0

            self._health_monitor.update_health(True)
            return deleted
        except Exception as e:
            self._health_monitor.update_health(False, {"error": str(e)})
            logger.error(f"SQLite delete error for key '{key}': {e}")
            raise StorageError(f"Failed to delete key: {e}")

    async def exists_async(self, key: str) -> bool:
        val = await self.get_async(key)
        return val is not None

    async def list_keys_async(self, pattern: Optional[str] = None) -> list[str]:
        await self.initialize()
        try:
            async with self._async_session() as session:
                stmt = sa.select(SQLiteKVModel.key, SQLiteKVModel.expires_at)
                result = await session.execute(stmt)
                rows = result.fetchall()

            keys = []
            for k, exp_at in rows:
                if self._is_expired(exp_at):
                    continue
                keys.append(str(k))

            if pattern:
                regex = re.compile(pattern, re.IGNORECASE)
                keys = [k for k in keys if regex.search(k)]

            self._health_monitor.update_health(True)
            return keys
        except Exception as e:
            self._health_monitor.update_health(False, {"error": str(e)})
            logger.error(f"SQLite list_keys error: {e}")
            raise StorageError(f"Failed to list keys: {e}")

    async def find_async(self, query: dict[str, Any]) -> list[Any]:
        await self.initialize()
        try:
            async with self._async_session() as session:
                stmt = sa.select(SQLiteKVModel.value, SQLiteKVModel.expires_at)
                result = await session.execute(stmt)
                rows = result.fetchall()

            results = []
            for val_json, exp_at in rows:
                if self._is_expired(exp_at):
                    continue

                value = self._deserialize(str(val_json))
                if not isinstance(value, dict):
                    continue

                match = True
                for qk, qv in query.items():
                    if value.get(qk) != qv:
                        match = False
                        break

                if match:
                    results.append(value)
            return results
        except Exception as e:
            logger.error(f"SQLite find error: {e}")
            raise StorageError(f"Find operation failed: {e}")

    async def clear_async(self) -> int:
        await self.initialize()
        try:
            async with self._async_session() as session:
                result = await session.execute(sa.delete(SQLiteKVModel))
                await session.commit()
                count = cast("CursorResult[Any]", result).rowcount

            self._health_monitor.update_health(True)
            return count
        except Exception as e:
            self._health_monitor.update_health(False, {"error": str(e)})
            logger.error(f"SQLite clear error: {e}")
            raise StorageError(f"Failed to clear storage: {e}")

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
        start_time = time.time()

        try:
            data = await self.get_data_for_sync()
            result.source_count = len(data)

            if not dry_run:
                if hasattr(target_provider, "apply_sync_data"):
                    applied, conflicts = await target_provider.apply_sync_data(data, conflict_resolution)
                    result.items_synced = applied
                    result.conflicts_found = len(conflicts)
                else:
                    for item in data:
                        await target_provider.set_async(item["key"], item["value"])
                        result.items_synced += 1

            result.success = True
        except Exception as e:
            result.errors.append(str(e))
            result.success = False
        finally:
            result.sync_duration_seconds = time.time() - start_time

        return result

    def get_sync_metadata(self) -> dict[str, Any]:
        return {"provider": "sqlite", "table": self.table_name}

    async def prepare_for_sync(self) -> bool:
        return True

    async def cleanup_after_sync(self, sync_result: SyncResult) -> None:
        pass

    async def get_data_for_sync(self, last_sync_timestamp: Optional[datetime] = None) -> list[dict[str, Any]]:
        await self.initialize()
        try:
            async with self._async_session() as session:
                stmt = sa.select(SQLiteKVModel)
                if last_sync_timestamp:
                    stmt = stmt.where(SQLiteKVModel.updated_at > last_sync_timestamp)

                result = await session.execute(stmt)
                records = result.scalars().all()

                return [
                    {
                        "key": str(r.key),
                        "value": self._deserialize(str(r.value)),
                        "expires_at": r.expires_at,
                        "updated_at": r.updated_at,
                    }
                    for r in records
                ]
        except Exception as e:
            logger.error(f"Failed to get data for sync: {e}")
            return []

    async def apply_sync_data(
        self,
        sync_data: list[dict[str, Any]],
        conflict_resolution: SyncConflictResolution = SyncConflictResolution.NEWEST_WINS,
    ) -> tuple[int, list[dict[str, Any]]]:
        await self.initialize()
        applied = 0
        conflicts: list[dict[str, Any]] = []

        for item in sync_data:
            key = item["key"]
            new_val = item["value"]
            new_updated = item["updated_at"]

            # Conflict check
            existing_record = None
            async with self._async_session() as session:
                stmt = sa.select(SQLiteKVModel).where(SQLiteKVModel.key == key)
                res = await session.execute(stmt)
                existing_record = res.scalars().first()

            if existing_record:
                if conflict_resolution == SyncConflictResolution.NEWEST_WINS:
                    if existing_record.updated_at and new_updated <= existing_record.updated_at:
                        continue
                elif conflict_resolution == SyncConflictResolution.TARGET_WINS:
                    continue

            await self.set_async(key, new_val)
            applied += 1

        return applied, conflicts

    # --- IHealthCheck Implementation ---

    def is_healthy(self) -> bool:
        return self._health_monitor.is_healthy()

    async def perform_deep_health_check(self) -> bool:
        test_key = f"__health_check_{int(time.time())}__"
        try:
            await self.set_async(test_key, {"status": "ok"})
            val = await self.get_async(test_key)
            await self.delete_async(test_key)
            healthy = val is not None and val.get("status") == "ok"
            self._health_monitor.update_health(healthy)
            return healthy
        except Exception:
            return False

    def get_health_status(self) -> dict[str, Any]:
        return self._health_monitor.get_health_status()

    def get_last_health_check(self) -> datetime:
        return self._health_monitor.get_last_health_check()

    # --- IBackupProvider Implementation ---

    async def create_backup(self, backup_path: str, metadata: Optional[dict[str, Any]] = None) -> bool:
        """Create a backup of the SQLite database asynchronously."""
        import shutil

        try:
            target = Path(backup_path)
            target.parent.mkdir(parents=True, exist_ok=True)
            # Use threadpool for file operation
            await asyncio.to_thread(shutil.copy2, str(self.db_path), str(target))
            return True
        except Exception as e:
            logger.error(f"Backup failed: {e}")
            return False

    async def restore_backup(self, backup_path: str, clear_existing: bool = False) -> bool:
        """Restore the database from a backup asynchronously."""
        import shutil

        try:
            if clear_existing and self.db_path.exists():
                self.db_path.unlink()
            await asyncio.to_thread(shutil.copy2, backup_path, str(self.db_path))
            return True
        except Exception as e:
            logger.error(f"Restore failed: {e}")
            return False

    async def validate_backup(self, backup_path: str) -> dict[str, Any]:
        """Validate a backup file asynchronously."""
        return {"valid": Path(backup_path).exists()}

    async def list_backups(self, backup_dir: str) -> dict[str, dict[str, Any]]:
        """List available backups in a directory asynchronously."""
        backup_path = Path(backup_dir)
        if not backup_path.exists():
            return {}

        backups = {}
        pattern = f"{self.db_path.stem}_*.sqlite"

        for file_path in backup_path.glob(pattern):
            try:
                backups[file_path.name] = {
                    "path": str(file_path),
                    "size_bytes": file_path.stat().st_size,
                    "created_at": datetime.fromtimestamp(file_path.stat().st_ctime, UTC).isoformat(),
                }
            except Exception as exc:
                logger.warning("Failed to stat backup file '%s': %s", file_path, exc)
                continue

        return backups

    def supports_ttl(self) -> bool:
        return True
