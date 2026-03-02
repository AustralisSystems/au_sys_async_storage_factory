import asyncio
import json
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Optional, Union

import aiosqlite

from storage.interfaces.backup import IBackupProvider
from storage.interfaces.health import HealthMonitor, IHealthCheck
from storage.interfaces.storage import IStorageProvider, StorageError


def get_logger():
    from src.shared.observability.logger_factory import create_debug_logger

    return create_debug_logger(__name__)


class _LazyLoggerProxy:
    def __getattr__(self, item: str):
        return getattr(get_logger(), item)


logger: _LazyLoggerProxy = _LazyLoggerProxy()


class AsyncSQLiteProvider(IHealthCheck, IBackupProvider, IStorageProvider):
    """
    Async SQLite Provider implementing the KV-store pattern using aiosqlite.

    Schema:
        CREATE TABLE IF NOT EXISTS kv_store (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,      -- JSON-serialized content
            expires_at REAL           -- Unix timestamp (optional TTL)
        )
    """

    def __init__(self, db_path: str):
        """
        Initialize Async SQLite provider.

        Args:
            db_path: Path to the SQLite file
        """
        self.db_path = Path(db_path)
        self.table_name = "kv_store"
        self._health_monitor = HealthMonitor()
        self._init_task = None

    async def initialize(self) -> None:
        """Initialize SQLite database and schema asynchronously."""
        if self._init_task is None:
            self._init_task = asyncio.create_task(self._initialize_db())
        await self._init_task

    async def _get_connection(self) -> aiosqlite.Connection:
        """Returns a connected aiosqlite connection with pragmas set."""
        try:
            conn = await aiosqlite.connect(str(self.db_path), timeout=30.0)
            await conn.execute("PRAGMA journal_mode=WAL")
            await conn.execute("PRAGMA synchronous=NORMAL")
            await conn.execute("PRAGMA busy_timeout=30000")
            await conn.execute("PRAGMA foreign_keys=ON")
            return conn
        except Exception as e:
            logger.error(f"AsyncSQLite connection error: {e}")
            raise StorageError(f"Connection failed: {e}")

    async def _initialize_db(self) -> None:
        """Initialize SQLite database and schema."""
        try:
            self.db_path = self.db_path.resolve()
            parent_dir = self.db_path.parent
            parent_dir.mkdir(parents=True, exist_ok=True)

            query = f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                expires_at REAL
            );
            CREATE INDEX IF NOT EXISTS idx_{self.table_name}_expires ON {self.table_name}(expires_at);
            """

            conn = await self._get_connection()
            try:
                await conn.executescript(query)
                await conn.commit()
            finally:
                await conn.close()

            self._health_monitor.update_health(True, {"file": str(self.db_path)})
            logger.info(f"AsyncSQLite initialized: {self.db_path}")

        except Exception as e:
            self._health_monitor.update_health(False, {"error": str(e)})
            logger.error(f"AsyncSQLite initialization failed: {e}")
            raise StorageError(f"Failed to initialize AsyncSQLite: {e}") from e

    def _serialize(self, value: Any) -> str:
        """Serialize value to JSON."""
        try:
            return json.dumps(value)
        except (TypeError, ValueError) as e:
            raise StorageError(f"Serialization failed: {e}")

    def _deserialize(self, value: str) -> Any:
        """Deserialize value from JSON."""
        try:
            return json.loads(value)
        except json.JSONDecodeError as e:
            raise StorageError(f"Deserialization failed: {e}")

    def _is_expired(self, expires_at: Optional[float]) -> bool:
        """Check if timestamp is expired."""
        if expires_at is None:
            return False
        return time.time() > expires_at

    # ------------------------------------------------------------------ #
    # Sync Interface (Fails or runs synchronously via event loop if needed) #
    # ------------------------------------------------------------------ #

    def _run_sync(self, coro):
        try:
            loop = asyncio.get_running_loop()
            # If we're already in an event loop, we can't just run_until_complete natively.
            # But the interface requires synchronous `get`, `set` etc.
            # Best is to raise NotImplementedError or simulate.
            raise NotImplementedError("Use async methods for AsyncSQLiteProvider when inside an event loop.")
        except RuntimeError:
            return asyncio.run(coro)

    def get(self, key: str) -> Optional[Any]:
        return self._run_sync(self.get_async(key))

    def set(self, key: str, value: Any) -> bool:
        return self._run_sync(self.set_async(key, value))

    def delete(self, key: str) -> bool:
        return self._run_sync(self.delete_async(key))

    def exists(self, key: str) -> bool:
        return self._run_sync(self.exists_async(key))

    def list_keys(self, pattern: Optional[str] = None) -> list[str]:
        return self._run_sync(self.list_keys_async(pattern))

    def find(self, query: dict[str, Any]) -> list[Any]:
        return self._run_sync(self.find_async(query))

    def clear(self) -> int:
        return self._run_sync(self.clear_async())

    def set_with_ttl(self, key: str, value: Any, ttl: Optional[int]) -> bool:
        return self._run_sync(self.set_with_ttl_async(key, value, ttl))

    # ------------------------------------------------------------------ #
    # Native Async Interface Implementation                              #
    # ------------------------------------------------------------------ #

    async def get_async(self, key: str) -> Optional[Any]:
        """Retrieve a value by key (async)."""
        await self.initialize()
        query = f"SELECT value, expires_at FROM {self.table_name} WHERE key = ?"

        try:
            conn = await self._get_connection()
            try:
                cursor = await conn.execute(query, (key,))
                row = await cursor.fetchone()

                if not row:
                    return None

                value_json, expires_at = row

                if self._is_expired(expires_at):
                    await self.delete_async(key)
                    return None

                self._health_monitor.update_health(True)
                return self._deserialize(value_json)
            finally:
                await conn.close()

        except Exception as e:
            self._health_monitor.update_health(False, {"error": str(e)})
            logger.error(f"AsyncSQLite get error for key '{key}': {e}")
            raise StorageError(f"Failed to get key '{key}': {e}")

    async def set_async(self, key: str, value: Any) -> bool:
        """Store a value with the given key (async)."""
        return await self.set_with_ttl_async(key, value, None)

    async def set_with_ttl_async(self, key: str, value: Any, ttl: Optional[int]) -> bool:
        """Store a value with optional TTL (async)."""
        await self.initialize()
        value_json = self._serialize(value)
        expires_at = time.time() + ttl if ttl is not None else None

        query = f"""
        INSERT INTO {self.table_name} (key, value, expires_at)
        VALUES (?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET
            value = excluded.value,
            expires_at = excluded.expires_at;
        """

        try:
            conn = await self._get_connection()
            try:
                await conn.execute(query, (key, value_json, expires_at))
                await conn.commit()
            finally:
                await conn.close()

            self._health_monitor.update_health(True)
            return True
        except Exception as e:
            self._health_monitor.update_health(False, {"error": str(e)})
            logger.error(f"AsyncSQLite set error for key '{key}': {e}")
            raise StorageError(f"Failed to set key '{key}': {e}")

    async def delete_async(self, key: str) -> bool:
        """Delete a value by key (async)."""
        await self.initialize()
        query = f"DELETE FROM {self.table_name} WHERE key = ?"

        try:
            conn = await self._get_connection()
            try:
                cursor = await conn.execute(query, (key,))
                await conn.commit()
                deleted = cursor.rowcount > 0
            finally:
                await conn.close()

            self._health_monitor.update_health(True)
            return deleted
        except Exception as e:
            self._health_monitor.update_health(False, {"error": str(e)})
            logger.error(f"AsyncSQLite delete error for key '{key}': {e}")
            raise StorageError(f"Failed to delete key '{key}': {e}")

    async def exists_async(self, key: str) -> bool:
        """Check if a key exists (async)."""
        val = await self.get_async(key)
        return val is not None

    async def list_keys_async(self, pattern: Optional[str] = None) -> list[str]:
        """List all keys, optionally filtered by pattern (async)."""
        await self.initialize()
        query = f"SELECT key, expires_at FROM {self.table_name}"

        try:
            conn = await self._get_connection()
            try:
                cursor = await conn.execute(query)
                rows = await cursor.fetchall()
            finally:
                await conn.close()

            keys = []
            for key, expires_at in rows:
                if self._is_expired(expires_at):
                    continue
                keys.append(key)

            if pattern:
                regex = re.compile(pattern, re.IGNORECASE)
                keys = [k for k in keys if regex.search(k)]

            self._health_monitor.update_health(True)
            return keys

        except Exception as e:
            self._health_monitor.update_health(False, {"error": str(e)})
            logger.error(f"AsyncSQLite list_keys error: {e}")
            raise StorageError(f"Failed to list keys: {e}")

    async def find_async(self, query: dict[str, Any]) -> list[Any]:
        """Find values matching a simple equality query (async)."""
        await self.initialize()
        try:
            sql = f"SELECT value, expires_at FROM {self.table_name}"
            conn = await self._get_connection()
            try:
                cursor = await conn.execute(sql)
                rows = await cursor.fetchall()
            finally:
                await conn.close()

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
            logger.error(f"AsyncSQLite find error: {e}")
            raise StorageError(f"Find operation failed: {e}")

    async def clear_async(self) -> int:
        """Clear all data from storage (async)."""
        await self.initialize()
        query = f"DELETE FROM {self.table_name}"

        try:
            conn = await self._get_connection()
            try:
                cursor = await conn.execute(query)
                await conn.commit()
                count = cursor.rowcount
            finally:
                await conn.close()

            self._health_monitor.update_health(True)
            return count
        except Exception as e:
            self._health_monitor.update_health(False, {"error": str(e)})
            logger.error(f"AsyncSQLite clear error: {e}")
            raise StorageError(f"Failed to clear storage: {e}")

    def supports_ttl(self) -> bool:
        """Check if this adapter supports TTL."""
        return True

    # ------------------------------------------------------------------ #
    # IHealthCheck Implementation                                        #
    # ------------------------------------------------------------------ #

    def is_healthy(self) -> bool:
        """Check if adapter is healthy."""
        try:
            return self.db_path.exists()
        except Exception:
            return False

    def perform_deep_health_check(self) -> bool:
        """Perform a deep health check (write/read/delete cycle) via sync wrapper."""
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
        """Get detailed health information."""
        status = self._health_monitor.get_health_status()
        status.update({"adapter": "async_sqlite_kv", "db_path": str(self.db_path), "db_exists": self.db_path.exists()})
        return status

    def get_last_health_check(self) -> datetime:
        return self._health_monitor.get_last_health_check()

    # ------------------------------------------------------------------ #
    # IBackupProvider Implementation                                     #
    # ------------------------------------------------------------------ #

    def create_backup(self, backup_dir: Optional[str] = None) -> Union[str, bool]:
        if not backup_dir:
            backup_dir = str(self.db_path.parent / "backups")

        backup_path = Path(backup_dir)
        backup_path.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        target = backup_path / f"{self.db_path.stem}_backup_{timestamp}.sqlite"

        import shutil
        try:
            shutil.copy2(str(self.db_path), str(target))
            return str(target)
        except Exception as e:
            logger.error(f"Backup failed: {e}")
            return False

    def restore_backup(self, backup_path: str) -> bool:
        try:
            import shutil
            shutil.copy2(backup_path, str(self.db_path))
            return True
        except Exception:
            return False

    def validate_backup(self, backup_path: str) -> dict[str, Any]:
        result = {"valid": False, "path": backup_path, "error": None, "metadata": {}}

        path = Path(backup_path)
        if not path.exists():
            result["error"] = "File not found"
            return result

        try:
            import sqlite3
            with sqlite3.connect(f"file:{path}?mode=ro", uri=True) as conn:
                cursor = conn.execute("PRAGMA integrity_check;")
                check_result = cursor.fetchone()
                if check_result and check_result[0] == "ok":
                    result["valid"] = True
                    try:
                        cursor = conn.execute(f"SELECT count(*) FROM {self.table_name}")
                        count = cursor.fetchone()[0]
                        result["metadata"]["key_count"] = count
                    except sqlite3.OperationalError:
                        result["metadata"]["key_count"] = 0
                else:
                    result["error"] = f"Integrity check failed: {check_result}"

        except Exception as e:
            result["error"] = str(e)

        return result

    def list_backups(self, backup_dir: Optional[str] = None) -> list[dict[str, Any]]:
        if not backup_dir:
            backup_dir = str(self.db_path.parent / "backups")

        backup_path = Path(backup_dir)
        if not backup_path.exists():
            return []

        backups = []
        pattern = f"{self.db_path.stem}_backup_*.sqlite"

        for file_path in backup_path.glob(pattern):
            try:
                parts = file_path.stem.split("_backup_")
                if len(parts) < 2:
                    continue

                timestamp_str = parts[-1]
                timestamp = datetime.strptime(timestamp_str, "%Y%m%d_%H%M%S")

                backups.append(
                    {
                        "path": str(file_path),
                        "filename": file_path.name,
                        "timestamp": timestamp.isoformat(),
                        "timestamp_obj": timestamp,
                        "size_bytes": file_path.stat().st_size,
                    }
                )
            except (ValueError, IndexError):
                continue

        backups.sort(key=lambda x: x["timestamp_obj"], reverse=True)

        for b in backups:
            del b["timestamp_obj"]

        return backups

    def get_latest_backup(self, backup_dir: Optional[str] = None) -> Optional[dict[str, Any]]:
        backups = self.list_backups(backup_dir)
        return backups[0] if backups else None
