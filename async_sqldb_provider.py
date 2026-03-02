import asyncio
import json
import time
from datetime import datetime
from typing import Any, Optional, Union

from sqlalchemy import Column, Float, String, Text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import declarative_base

from storage.interfaces.backup import IBackupProvider
from storage.interfaces.health import HealthMonitor, IHealthCheck
from storage.interfaces.storage import IStorageProvider, StorageError

def get_logger():
    from storage.shared.observability.logger_factory import create_debug_logger
    return create_debug_logger(__name__)

class _LazyLoggerProxy:
    def __getattr__(self, item: str):
        return getattr(get_logger(), item)

logger: _LazyLoggerProxy = _LazyLoggerProxy()

Base = declarative_base()

class KVStoreModel(Base):
    __tablename__ = "kv_store"
    key = Column(String(512), primary_key=True, index=True)
    value = Column(Text, nullable=False)
    expires_at = Column(Float, index=True, nullable=True)


class AsyncSQLDBProvider(IHealthCheck, IBackupProvider, IStorageProvider):
    """
    Async SQL DB Provider implementing the KV-store pattern using SQLAlchemy [asyncio].
    Compatible with Any asyncio-supported DB (PostgreSQL via asyncpg, MySQL via aiomysql, etc)
    """

    def __init__(self, connection_url: str, **engine_kwargs: Any):
        """
        Initialize Async SQL DB provider.
        Complies with Zero-Hardcode mandate: connection_url must be injected via config/env.

        Args:
            connection_url: Async SQLAlchemy connection string (e.g., postgresql+asyncpg://...)
            engine_kwargs: Additional kwargs to pass to create_async_engine
        """
        self.connection_url = connection_url
        self._health_monitor = HealthMonitor()
        self._engine = create_async_engine(self.connection_url, **engine_kwargs)
        self._async_session = async_sessionmaker(self._engine, expire_on_commit=False, class_=AsyncSession)
        self._init_task = None

    async def initialize(self) -> None:
        """Initialize tables asynchronously."""
        if self._init_task is None:
            self._init_task = asyncio.create_task(self._initialize_db())
        await self._init_task

    async def _initialize_db(self) -> None:
        """Create schema on DB."""
        try:
            async with self._engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            self._health_monitor.update_health(True, {"url": self._mask_url(self.connection_url)})
            logger.info("AsyncSQLDB initialized schemas.")
        except Exception as e:
            self._health_monitor.update_health(False, {"error": str(e)})
            logger.error(f"AsyncSQLDB initialization failed: {e}")
            raise StorageError(f"Failed to initialize AsyncSQLDB: {e}") from e

    def _mask_url(self, url: str) -> str:
        """Mask credentials from connection URL for logging/health reporting."""
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

    # ------------------------------------------------------------------ #
    # Sync Interface (Fails or simulates via asyncio.run)                #
    # ------------------------------------------------------------------ #
    def _run_sync(self, coro):
        try:
            loop = asyncio.get_running_loop()
            raise NotImplementedError("Use async methods for AsyncSQLDBProvider inside an event loop.")
        except RuntimeError:
            return asyncio.run(coro)

    def get(self, key: str) -> Optional[Any]: return self._run_sync(self.get_async(key))
    def set(self, key: str, value: Any) -> bool: return self._run_sync(self.set_async(key, value))
    def delete(self, key: str) -> bool: return self._run_sync(self.delete_async(key))
    def exists(self, key: str) -> bool: return self._run_sync(self.exists_async(key))
    def list_keys(self, pattern: Optional[str] = None) -> list[str]: return self._run_sync(self.list_keys_async(pattern))
    def find(self, query: dict[str, Any]) -> list[Any]: return self._run_sync(self.find_async(query))
    def clear(self) -> int: return self._run_sync(self.clear_async())
    def set_with_ttl(self, key: str, value: Any, ttl: Optional[int]) -> bool: return self._run_sync(self.set_with_ttl_async(key, value, ttl))

    # ------------------------------------------------------------------ #
    # Native Async Interface Implementation                              #
    # ------------------------------------------------------------------ #

    async def get_async(self, key: str) -> Optional[Any]:
        from sqlalchemy.future import select
        await self.initialize()

        try:
            async with self._async_session() as session:
                result = await session.execute(select(KVStoreModel).where(KVStoreModel.key == key))
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
            logger.error(f"AsyncSQLDB get error for key '{key}': {e}")
            raise StorageError(f"Failed to get key '{key}': {e}")

    async def set_with_ttl_async(self, key: str, value: Any, ttl: Optional[int]) -> bool:
        await self.initialize()
        value_json = self._serialize(value)
        expires_at = time.time() + ttl if ttl is not None else None

        try:
            async with self._async_session() as session:
                from sqlalchemy.dialects.postgresql import insert as pg_insert
                from sqlalchemy.dialects.sqlite import insert as sqlite_insert
                from sqlalchemy import select

                dialect_name = self._engine.dialect.name

                if dialect_name == "postgresql":
                    stmt = pg_insert(KVStoreModel).values(key=key, value=value_json, expires_at=expires_at)
                    stmt = stmt.on_conflict_do_update(
                        index_elements=['key'],
                        set_=dict(value=stmt.excluded.value, expires_at=stmt.excluded.expires_at)
                    )
                    await session.execute(stmt)
                elif dialect_name == "sqlite":
                    stmt = sqlite_insert(KVStoreModel).values(key=key, value=value_json, expires_at=expires_at)
                    stmt = stmt.on_conflict_do_update(
                        index_elements=['key'],
                        set_=dict(value=stmt.excluded.value, expires_at=stmt.excluded.expires_at)
                    )
                    await session.execute(stmt)
                else:
                    # Fallback pattern for MySQL/others if ON CONFLICT not supported natively via sqlalchemy dialects
                    result = await session.execute(select(KVStoreModel).where(KVStoreModel.key == key))
                    record = result.scalars().first()
                    if record:
                        record.value = value_json
                        record.expires_at = expires_at
                    else:
                        record = KVStoreModel(key=key, value=value_json, expires_at=expires_at)
                        session.add(record)

                await session.commit()
            self._health_monitor.update_health(True)
            return True
        except Exception as e:
            self._health_monitor.update_health(False, {"error": str(e)})
            logger.error(f"AsyncSQLDB set error for key '{key}': {e}")
            raise StorageError(f"Failed to set key '{key}': {e}")

    async def set_async(self, key: str, value: Any) -> bool:
        return await self.set_with_ttl_async(key, value, None)

    async def delete_async(self, key: str) -> bool:
        from sqlalchemy import delete
        await self.initialize()
        try:
            async with self._async_session() as session:
                stmt = delete(KVStoreModel).where(KVStoreModel.key == key)
                result = await session.execute(stmt)
                await session.commit()
                deleted = result.rowcount > 0
            self._health_monitor.update_health(True)
            return deleted
        except Exception as e:
            self._health_monitor.update_health(False, {"error": str(e)})
            logger.error(f"AsyncSQLDB delete error for key '{key}': {e}")
            raise StorageError(f"Failed to delete key '{key}': {e}")

    async def exists_async(self, key: str) -> bool:
        val = await self.get_async(key)
        return val is not None

    async def list_keys_async(self, pattern: Optional[str] = None) -> list[str]:
        from sqlalchemy.future import select
        await self.initialize()

        try:
            async with self._async_session() as session:
                result = await session.execute(select(KVStoreModel.key, KVStoreModel.expires_at))
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
            logger.error(f"AsyncSQLDB list_keys error: {e}")
            raise StorageError(f"Failed to list keys: {e}")

    async def find_async(self, query: dict[str, Any]) -> list[Any]:
        from sqlalchemy.future import select
        await self.initialize()

        try:
            async with self._async_session() as session:
                result = await session.execute(select(KVStoreModel.value, KVStoreModel.expires_at))
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
            logger.error(f"AsyncSQLDB find error: {e}")
            raise StorageError(f"Find operation failed: {e}")

    async def clear_async(self) -> int:
        from sqlalchemy import delete
        await self.initialize()
        try:
            async with self._async_session() as session:
                result = await session.execute(delete(KVStoreModel))
                await session.commit()
                count = result.rowcount
            self._health_monitor.update_health(True)
            return count
        except Exception as e:
            self._health_monitor.update_health(False, {"error": str(e)})
            logger.error(f"AsyncSQLDB clear error: {e}")
            raise StorageError(f"Failed to clear storage: {e}")

    def supports_ttl(self) -> bool:
        return True

    # ------------------------------------------------------------------ #
    # IHealthCheck Implementation                                        #
    # ------------------------------------------------------------------ #

    def is_healthy(self) -> bool:
        return self._health_monitor.is_healthy()

    def perform_deep_health_check(self) -> bool:
        test_key = f"__health_check_{int(time.time())}__"
        try:
            self.set(test_key, {"status": "ok"})
            val = self.get(test_key)
            if not val or val.get("status") != "ok":
                return False
            self.delete(test_key)
            return True
        except Exception:
            return False

    def get_health_status(self) -> dict[str, Any]:
        status = self._health_monitor.get_health_status()
        status.update({"adapter": "async_sqldb", "provider": "sqlalchemy"})
        return status

    def get_last_health_check(self) -> datetime:
        return self._health_monitor.get_last_health_check()

    # ------------------------------------------------------------------ #
    # IBackupProvider Implementation                                     #
    # ------------------------------------------------------------------ #

    def create_backup(self, backup_dir: Optional[str] = None) -> Union[str, bool]:
        logger.warning("Backup function is not natively supported for generic AsyncSQLDBProvider. Use DB-native tools.")
        return False

    def restore_backup(self, backup_path: str) -> bool:
        return False

    def validate_backup(self, backup_path: str) -> dict[str, Any]:
        return {"valid": False, "error": "Not natively implemented"}

    def list_backups(self, backup_dir: Optional[str] = None) -> list[dict[str, Any]]:
        return []

    def get_latest_backup(self, backup_dir: Optional[str] = None) -> Optional[dict[str, Any]]:
        return None
