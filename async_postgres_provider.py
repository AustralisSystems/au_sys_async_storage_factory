from __future__ import annotations

"""
Async Postgres Provider - SQLAlchemy Implementation.

Implements IRelationalProvider using SQLAlchemy with asyncpg.
Follows the KV-store pattern on top of a relational table.

Compliance:
- Zero-Data-Loss Sync capability via ISyncProvider
- Idempotent initialization
"""

import asyncio
import json
import time
import logging
from datetime import datetime, UTC
from typing import Any, Optional, Tuple, List, Dict

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import declarative_base
from sqlalchemy.dialects.postgresql import insert as pg_insert

from .interfaces.base_relational_provider import IRelationalProvider
from .interfaces.sync import SyncResult, SyncDirection, SyncConflictResolution
from .interfaces.health import HealthMonitor
from .interfaces.storage import StorageError

logger = logging.getLogger("storage.postgres")

Base = declarative_base()


class PostgresKVModel(Base):  # type: ignore
    """Internal model for KV storage in Postgres."""

    __tablename__ = "kv_store"
    key = sa.Column(sa.String(512), primary_key=True, index=True)
    value = sa.Column(sa.Text, nullable=False)
    expires_at = sa.Column(sa.Float, index=True, nullable=True)
    updated_at = sa.Column(
        sa.DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), index=True
    )


class AsyncPostgresProvider(IRelationalProvider):
    """
    Postgres Provider using SQLAlchemy [asyncio] and asyncpg.
    """

    def __init__(self, connection_url: str, **engine_kwargs: Any):
        """
        Initialize Async Postgres provider.

        Args:
            connection_url: Async SQLAlchemy connection string (e.g., postgresql+asyncpg://...)
            engine_kwargs: Additional kwargs to pass to create_async_engine
        """
        self.connection_url = connection_url
        self.table_name = "kv_store"
        self._health_monitor = HealthMonitor()

        # SQLAlchemy setup
        self._engine = create_async_engine(self.connection_url, **engine_kwargs)
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
            async with self._engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)

            self._health_monitor.update_health(True, {"url": self._mask_url(self.connection_url)})
            logger.info("AsyncPostgres initialized.")
        except Exception as e:
            self._health_monitor.update_health(False, {"error": str(e)})
            logger.error(f"AsyncPostgres initialization failed: {e}")
            raise StorageError(f"Failed to initialize Postgres: {e}") from e

    def _mask_url(self, url: str) -> str:
        """Mask credentials from connection URL for logging."""
        try:
            from urllib.parse import urlparse

            parsed = urlparse(url)
            if parsed.password:
                return url.replace(f":{parsed.password}@", ":****@")
            return url
        except Exception:
            return "masked_url"

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
                stmt = sa.select(PostgresKVModel).where(PostgresKVModel.key == key)
                result = await session.execute(stmt)
                record = result.scalars().first()

                if not record:
                    return None

                if self._is_expired(record.expires_at):
                    await session.delete(record)
                    await session.commit()
                    return None

                self._health_monitor.update_health(True)
                return self._deserialize(record.value)
        except Exception as e:
            self._health_monitor.update_health(False, {"error": str(e)})
            logger.error(f"Postgres get error for key '{key}': {e}")
            raise StorageError(f"Failed to get key: {e}")

    async def set_async(self, key: str, value: Any) -> bool:
        return await self.set_with_ttl_async(key, value, -1)

    async def set_with_ttl_async(self, key: str, value: Any, ttl: int) -> bool:
        await self.initialize()
        value_json = self._serialize(value)
        expires_at = time.time() + ttl if (ttl >= 0) else None

        try:
            async with self._async_session() as session:
                stmt = pg_insert(PostgresKVModel).values(
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
            logger.error(f"Postgres set error for key '{key}': {e}")
            raise StorageError(f"Failed to set key: {e}")

    async def delete_async(self, key: str) -> bool:
        await self.initialize()
        try:
            async with self._async_session() as session:
                stmt = sa.delete(PostgresKVModel).where(PostgresKVModel.key == key)
                result = await session.execute(stmt)
                await session.commit()
                deleted = result.rowcount > 0

            self._health_monitor.update_health(True)
            return deleted
        except Exception as e:
            self._health_monitor.update_health(False, {"error": str(e)})
            logger.error(f"Postgres delete error for key '{key}': {e}")
            raise StorageError(f"Failed to delete key: {e}")

    async def exists_async(self, key: str) -> bool:
        val = await self.get_async(key)
        return val is not None

    async def list_keys_async(self, pattern: Optional[str] = None) -> list[str]:
        await self.initialize()
        try:
            async with self._async_session() as session:
                stmt = sa.select(PostgresKVModel.key, PostgresKVModel.expires_at)
                result = await session.execute(stmt)
                rows = result.fetchall()

            keys = []
            for k, expires_at in rows:
                if self._is_expired(expires_at):
                    continue
                keys.append(k)

            if pattern:
                import re

                regex = re.compile(pattern, re.IGNORECASE)
                keys = [k for k in keys if regex.search(k)]

            self._health_monitor.update_health(True)
            return keys
        except Exception as e:
            self._health_monitor.update_health(False, {"error": str(e)})
            logger.error(f"Postgres list_keys error: {e}")
            raise StorageError(f"Failed to list keys: {e}")

    async def find_async(self, query: dict[str, Any]) -> list[Any]:
        await self.initialize()
        try:
            async with self._async_session() as session:
                stmt = sa.select(PostgresKVModel.value, PostgresKVModel.expires_at)
                result = await session.execute(stmt)
                rows = result.fetchall()

            results = []
            for value_json, expires_at in rows:
                if self._is_expired(expires_at):
                    continue

                value = self._deserialize(value_json)
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
            logger.error(f"Postgres find error: {e}")
            raise StorageError(f"Find operation failed: {e}")

    async def clear_async(self) -> int:
        await self.initialize()
        try:
            async with self._async_session() as session:
                result = await session.execute(sa.delete(PostgresKVModel))
                await session.commit()
                count = result.rowcount

            self._health_monitor.update_health(True)
            return count
        except Exception as e:
            self._health_monitor.update_health(False, {"error": str(e)})
            logger.error(f"Postgres clear error: {e}")
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
        return {"provider": "postgres", "table": self.table_name}

    async def prepare_for_sync(self) -> bool:
        return True

    async def cleanup_after_sync(self, sync_result: SyncResult) -> None:
        pass

    async def get_data_for_sync(self, last_sync_timestamp: Optional[datetime] = None) -> list[dict[str, Any]]:
        await self.initialize()
        try:
            async with self._async_session() as session:
                stmt = sa.select(PostgresKVModel)
                if last_sync_timestamp:
                    stmt = stmt.where(PostgresKVModel.updated_at > last_sync_timestamp)

                result = await session.execute(stmt)
                records = result.scalars().all()

                return [
                    {
                        "key": r.key,
                        "value": self._deserialize(r.value),
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
    ) -> Tuple[int, List[Dict[str, Any]]]:
        await self.initialize()
        applied = 0
        conflicts: list[dict[str, Any]] = []

        for item in sync_data:
            key = item["key"]
            new_val = item["value"]
            new_updated = item["updated_at"]

            existing_record = None
            async with self._async_session() as session:
                stmt = sa.select(PostgresKVModel).where(PostgresKVModel.key == key)
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
        """Sovereign backup implementation (JSON)."""
        await self.initialize()
        try:
            data = await self.get_data_for_sync()
            backup_payload = {
                "metadata": {
                    "provider": "postgres",
                    "timestamp": datetime.now(UTC).isoformat(),
                    "custom": metadata or {},
                },
                "data": data,
            }
            import aiofiles
            from pathlib import Path

            target = Path(backup_path)
            target.parent.mkdir(parents=True, exist_ok=True)
            async with aiofiles.open(target, mode="w", encoding="utf-8") as f:
                await f.write(json.dumps(backup_payload, indent=2, default=str))
            return True
        except Exception as e:
            logger.error(f"Postgres backup failed: {e}")
            return False

    async def restore_backup(self, backup_path: str, clear_existing: bool = False) -> bool:
        await self.initialize()
        try:
            import aiofiles

            async with aiofiles.open(backup_path, mode="r", encoding="utf-8") as f:
                content = await f.read()
                backup_data = json.loads(content)

            if clear_existing:
                await self.clear_async()

            applied, conflicts = await self.apply_sync_data(
                backup_data.get("data", []), SyncConflictResolution.SOURCE_WINS
            )
            return True
        except Exception as e:
            logger.error(f"Postgres restore failed: {e}")
            return False

    async def validate_backup(self, backup_path: str) -> dict[str, Any]:
        try:
            from pathlib import Path

            if not Path(backup_path).exists():
                return {"valid": False, "error": "File not found"}
            return {"valid": True}
        except Exception as e:
            return {"valid": False, "error": str(e)}

    async def list_backups(self, backup_dir: str) -> dict[str, dict[str, Any]]:
        return {}

    def supports_ttl(self) -> bool:
        return True
