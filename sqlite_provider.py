"""
SQLite Storage Provider - Clean implementation following SOLID principles.

This provider implements the core storage interfaces for SQLite (KV mode).
It focuses solely on SQLite operations without mixing concerns.

Compliance:
- V18 SQLite Persistence Standard
- Dual-Mode Storage Pattern (SQL + KV)
"""

import json
import re
import sqlite3
import time
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Optional, Union

from src.shared.services.storage.interfaces.backup import IBackupProvider
from src.shared.services.storage.interfaces.health import HealthMonitor, IHealthCheck
from src.shared.services.storage.interfaces.storage import IStorageProvider, StorageError


def get_logger():
    from src.shared.observability.logger_factory import create_debug_logger

    return create_debug_logger(__name__)


class _LazyLoggerProxy:
    def __getattr__(self, item: str):
        return getattr(get_logger(), item)


logger: _LazyLoggerProxy = _LazyLoggerProxy()


class SQLiteProvider(IHealthCheck, IBackupProvider, IStorageProvider):
    """
    SQLite Provider implementing the KV-store pattern.

    Schema:
        CREATE TABLE IF NOT EXISTS kv_store (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,      -- JSON-serialized content
            expires_at REAL           -- Unix timestamp (optional TTL)
        )
    """

    def __init__(self, db_path: str):
        """
        Initialize SQLite provider.

        Args:
            db_path: Path to the SQLite file
        """
        self.db_path = Path(db_path)
        self.table_name = "kv_store"

        # Health monitoring
        self._health_monitor = HealthMonitor()

        # Initialize database
        logger.debug(f"Initializing SQLite provider at {self.db_path}")
        self._initialize_db()

    # ------------------------------------------------------------------ #
    # Internal helpers                                                   #
    # ------------------------------------------------------------------ #

    @contextmanager
    def _get_connection(self):
        """Yields a SQLite connection context."""
        conn = None
        try:
            conn = sqlite3.connect(str(self.db_path), timeout=30.0, detect_types=sqlite3.PARSE_DECLTYPES)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA busy_timeout=30000")
            conn.execute("PRAGMA foreign_keys=ON")
            yield conn
        except sqlite3.Error as e:
            logger.error(f"SQLite connection error: {e}")
            raise StorageError(f"Connection failed: {e}")
        finally:
            if conn:
                conn.close()

    def _initialize_db(self) -> None:
        """Initialize SQLite database and schema."""
        try:
            self.db_path = self.db_path.resolve()

            # Ensure directory exists
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

            with self._get_connection() as conn:
                conn.executescript(query)
                conn.commit()

            self._health_monitor.update_health(True, {"file": str(self.db_path)})
            logger.info(f"SQLite initialized: {self.db_path}")

        except Exception as e:
            self._health_monitor.update_health(False, {"error": str(e)})
            logger.error(f"SQLite initialization failed: {e}")
            raise StorageError(f"Failed to initialize SQLite: {e}") from e

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
            # Handle corruption gracefully (log and return None logic handled in caller if needed)
            raise StorageError(f"Deserialization failed: {e}")

    def _is_expired(self, expires_at: Optional[float]) -> bool:
        """Check if timestamp is expired."""
        if expires_at is None:
            return False
        return time.time() > expires_at

    # ------------------------------------------------------------------ #
    # IStorageProvider Implementation                                    #
    # ------------------------------------------------------------------ #

    def get(self, key: str) -> Optional[Any]:
        """Retrieve a value by key."""
        query = f"SELECT value, expires_at FROM {self.table_name} WHERE key = ?"

        try:
            with self._get_connection() as conn:
                cursor = conn.execute(query, (key,))
                row = cursor.fetchone()

                if not row:
                    return None

                value_json, expires_at = row

                # Check expiry
                if self._is_expired(expires_at):
                    # Lazy delete
                    self.delete(key)
                    return None

                self._health_monitor.update_health(True)
                return self._deserialize(value_json)

        except Exception as e:
            self._health_monitor.update_health(False, {"error": str(e)})
            logger.error(f"SQLite get error for key '{key}': {e}")
            raise StorageError(f"Failed to get key '{key}': {e}")

    def set(self, key: str, value: Any) -> bool:
        """Store a value with the given key."""
        return self.set_with_ttl(key, value, None)

    def set_with_ttl(self, key: str, value: Any, ttl: Optional[int]) -> bool:
        """Store a value with optional TTL."""
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
            with self._get_connection() as conn:
                conn.execute(query, (key, value_json, expires_at))
                conn.commit()

            self._health_monitor.update_health(True)
            return True
        except Exception as e:
            self._health_monitor.update_health(False, {"error": str(e)})
            logger.error(f"SQLite set error for key '{key}': {e}")
            raise StorageError(f"Failed to set key '{key}': {e}")

    def delete(self, key: str) -> bool:
        """Delete a value by key."""
        query = f"DELETE FROM {self.table_name} WHERE key = ?"

        try:
            with self._get_connection() as conn:
                cursor = conn.execute(query, (key,))
                conn.commit()
                deleted = cursor.rowcount > 0

            self._health_monitor.update_health(True)
            return deleted
        except Exception as e:
            self._health_monitor.update_health(False, {"error": str(e)})
            logger.error(f"SQLite delete error for key '{key}': {e}")
            raise StorageError(f"Failed to delete key '{key}': {e}")

    def exists(self, key: str) -> bool:
        """Check if a key exists."""
        # Use get to handle expiry logic automatically
        val = self.get(key)
        return val is not None

    def list_keys(self, pattern: Optional[str] = None) -> list[str]:
        """List all keys, optionally filtered by pattern."""
        query = f"SELECT key, expires_at FROM {self.table_name}"
        # SQLite GLOB or LIKE could be used, but regex filtering in python is safer for generic patterns

        try:
            with self._get_connection() as conn:
                cursor = conn.execute(query)
                rows = cursor.fetchall()

            keys = []
            time.time()

            for key, expires_at in rows:
                if self._is_expired(expires_at):
                    continue
                keys.append(key)

            # Apply pattern filter
            if pattern:
                regex = re.compile(pattern, re.IGNORECASE)
                keys = [k for k in keys if regex.search(k)]

            self._health_monitor.update_health(True)
            return keys

        except Exception as e:
            self._health_monitor.update_health(False, {"error": str(e)})
            logger.error(f"SQLite list_keys error: {e}")
            raise StorageError(f"Failed to list keys: {e}")

    def find(self, query: dict[str, Any]) -> list[Any]:
        """
        Find values matching a simple equality query.
        For KV-store pattern, we fetch all non-expired items and filter in Python.
        """
        try:
            sql = f"SELECT value, expires_at FROM {self.table_name}"
            with self._get_connection() as conn:
                cursor = conn.execute(sql)
                rows = cursor.fetchall()

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
            logger.error(f"SQLite find error: {e}")
            raise StorageError(f"Find operation failed: {e}")

    def clear(self) -> int:
        """Clear all data from storage."""
        query = f"DELETE FROM {self.table_name}"

        try:
            with self._get_connection() as conn:
                cursor = conn.execute(query)
                conn.commit()
                count = cursor.rowcount

            self._health_monitor.update_health(True)
            return count
        except Exception as e:
            self._health_monitor.update_health(False, {"error": str(e)})
            logger.error(f"SQLite clear error: {e}")
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
        """Perform a deep health check (write/read/delete cycle)."""
        test_key = f"__health_check_{int(time.time())}__"
        try:
            # Write
            self.set(test_key, {"status": "ok"})
            # Read
            val = self.get(test_key)
            if not val or val.get("status") != "ok":
                return False
            # Delete
            self.delete(test_key)
            return True
        except Exception:
            return False

    def get_health_status(self) -> dict[str, Any]:
        """Get detailed health information."""
        status = self._health_monitor.get_health_status()
        status.update({"adapter": "sqlite_kv", "db_path": str(self.db_path), "db_exists": self.db_path.exists()})
        return status

    def get_last_health_check(self) -> datetime:
        return self._health_monitor.get_last_health_check()

    # ------------------------------------------------------------------ #
    # IBackupProvider Implementation
    # ------------------------------------------------------------------ #

    def create_backup(self, backup_dir: Optional[str] = None) -> Union[str, bool]:
        # Implement backup logic using sqlite3 backup API or file copy
        # For now, simplistic file copy
        if not backup_dir:
            backup_dir = str(self.db_path.parent / "backups")

        backup_path = Path(backup_dir)
        backup_path.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        target = backup_path / f"{self.db_path.stem}_backup_{timestamp}.sqlite"

        try:
            with self._get_connection() as bck, sqlite3.connect(str(target)) as dst:
                bck.backup(dst)
            return str(target)
        except Exception as e:
            logger.error(f"Backup failed: {e}")
            return False

    def restore_backup(self, backup_path: str) -> bool:
        # Implement restore
        try:
            import shutil

            shutil.copy2(backup_path, str(self.db_path))
            return True
        except Exception:
            return False

    def validate_backup(self, backup_path: str) -> dict[str, Any]:
        """
        Validate a backup file.

        Args:
            backup_path: Path to the backup file

        Returns:
            Dictionary with validation results and metadata
        """
        result = {"valid": False, "path": backup_path, "error": None, "metadata": {}}

        path = Path(backup_path)
        if not path.exists():
            result["error"] = "File not found"
            return result

        try:
            # Check if it's a valid SQLite database
            with sqlite3.connect(f"file:{path}?mode=ro", uri=True) as conn:
                cursor = conn.execute("PRAGMA integrity_check;")
                check_result = cursor.fetchone()
                if check_result and check_result[0] == "ok":
                    result["valid"] = True
                    # Get metadata if possible (e.g. count of keys)
                    try:
                        cursor = conn.execute(f"SELECT count(*) FROM {self.table_name}")
                        count = cursor.fetchone()[0]
                        result["metadata"]["key_count"] = count
                    except sqlite3.OperationalError:
                        # Table might not exist or be different
                        result["metadata"]["key_count"] = 0
                else:
                    result["error"] = f"Integrity check failed: {check_result}"

        except sqlite3.Error as e:
            result["error"] = str(e)

        return result

    def list_backups(self, backup_dir: Optional[str] = None) -> list[dict[str, Any]]:
        """List available backups sorted by date (newest first)."""
        if not backup_dir:
            backup_dir = str(self.db_path.parent / "backups")

        backup_path = Path(backup_dir)
        if not backup_path.exists():
            return []

        backups = []
        # Pattern matches: stems_backup_YYYYMMDD_HHMMSS.sqlite
        pattern = f"{self.db_path.stem}_backup_*.sqlite"

        for file_path in backup_path.glob(pattern):
            try:
                # Expected format: name_backup_20240101_120000.sqlite
                # Split by '_backup_' taking the last part
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
                        "timestamp_obj": timestamp,  # Helper for sorting
                        "size_bytes": file_path.stat().st_size,
                    }
                )
            except (ValueError, IndexError):
                continue

        # Sort by timestamp descending
        backups.sort(key=lambda x: x["timestamp_obj"], reverse=True)

        # Remove helper object before returning
        for b in backups:
            del b["timestamp_obj"]

        return backups

    def get_latest_backup(self, backup_dir: Optional[str] = None) -> Optional[dict[str, Any]]:
        """Get the most recent backup metadata."""
        backups = self.list_backups(backup_dir)
        return backups[0] if backups else None
