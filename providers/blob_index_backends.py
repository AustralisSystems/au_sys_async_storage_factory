from __future__ import annotations

"""
Blob Index Backends.

Implementations for Redis and SQLite metadata indexing.
Supports Dual-Tier Indexing and Cache Offline Persistency.
"""

import asyncio
import logging
import json
import sqlite3
from abc import ABC, abstractmethod
from datetime import datetime, UTC
from typing import Any, AsyncIterator, Optional

from ..interfaces.storage import IStorageProvider, StorageError
from ..interfaces.health import IHealthCheck, HealthMonitor

logger = logging.getLogger(__name__)


class IBlobIndex(ABC):
    """Interface for blob metadata indexing backends."""

    @abstractmethod
    async def set_metadata(self, key: str, metadata: dict[str, Any]) -> bool:
        """Store metadata for a blob."""
        ...

    @abstractmethod
    async def get_metadata(self, key: str) -> Optional[dict[str, Any]]:
        """Retrieve metadata for a blob."""
        ...

    @abstractmethod
    async def delete_metadata(self, key: str) -> bool:
        """Delete metadata for a blob."""
        ...

    @abstractmethod
    def list_metadata(self, prefix: Optional[str] = None, limit: Optional[int] = None) -> AsyncIterator[dict[str, Any]]:
        """List metadata for blobs."""
        ...

    @abstractmethod
    async def is_healthy(self) -> bool:
        """Check health of the backend."""
        ...


class RedisBlobIndex(IBlobIndex):
    """Redis-based implementation of the blob index."""

    def __init__(self, redis_client: Any, prefix: str = "blob_meta:"):
        self.client = redis_client
        self.prefix = prefix

    def _full_key(self, key: str) -> str:
        return f"{self.prefix}{key}"

    async def set_metadata(self, key: str, metadata: dict[str, Any]) -> bool:
        try:
            # Use RedisJSON if available, otherwise hash
            await self.client.set(self._full_key(key), json.dumps(metadata))
            return True
        except Exception as e:
            logger.error(f"Redis index set failed: {e}")
            return False

    async def get_metadata(self, key: str) -> Optional[dict[str, Any]]:
        try:
            data = await self.client.get(self._full_key(key))
            return json.loads(data) if data else None
        except Exception:
            return None

    async def delete_metadata(self, key: str) -> bool:
        try:
            result: int = await self.client.delete(self._full_key(key))
            return result > 0
        except Exception:
            return False

    async def list_metadata(
        self, prefix: Optional[str] = None, limit: Optional[int] = None
    ) -> AsyncIterator[dict[str, Any]]:
        search_pattern = f"{self.prefix}{prefix or ''}*"
        count = 0
        try:
            async for k in self.client.scan_iter(search_pattern):
                data = await self.client.get(k)
                if data:
                    yield json.loads(data)
                    count += 1
                    if limit and count >= limit:
                        return
        except Exception as e:
            logger.error(f"Redis index list failed: {e}")

    async def is_healthy(self) -> bool:
        try:
            await self.client.ping()
            return True
        except Exception:
            return False


class SQLiteBlobIndex(IBlobIndex):
    """SQLite-based implementation of the blob index (Local Fallback)."""

    def __init__(self, db_path: str = "data/storage/blob_index.sqlite"):
        self.db_path = db_path
        self._init_db()

    def _init_db(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS blob_metadata (
                    key TEXT PRIMARY KEY,
                    metadata TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
            """)
            conn.commit()

    async def set_metadata(self, key: str, metadata: dict[str, Any]) -> bool:
        loop = asyncio.get_running_loop()

        def _set() -> bool:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    "INSERT OR REPLACE INTO blob_metadata (key, metadata, updated_at) VALUES (?, ?, ?)",
                    (key, json.dumps(metadata), datetime.now(UTC).isoformat()),
                )
                conn.commit()
            return True

        return await loop.run_in_executor(None, _set)

    async def get_metadata(self, key: str) -> Optional[dict[str, Any]]:
        loop = asyncio.get_running_loop()

        def _get() -> Optional[dict[str, Any]]:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("SELECT metadata FROM blob_metadata WHERE key = ?", (key,))
                row = cursor.fetchone()
                return json.loads(row[0]) if row else None

        return await loop.run_in_executor(None, _get)

    async def delete_metadata(self, key: str) -> bool:
        loop = asyncio.get_running_loop()

        def _delete() -> bool:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("DELETE FROM blob_metadata WHERE key = ?", (key,))
                conn.commit()
                return cursor.rowcount > 0

        return await loop.run_in_executor(None, _delete)

    async def list_metadata(
        self, prefix: Optional[str] = None, limit: Optional[int] = None
    ) -> AsyncIterator[dict[str, Any]]:
        # This implementation is sync-wrapped in async generator
        def _list() -> list[dict[str, Any]]:
            with sqlite3.connect(self.db_path) as conn:
                query = "SELECT metadata FROM blob_metadata"
                params: list[Any] = []
                if prefix:
                    query += " WHERE key LIKE ?"
                    params.append(f"{prefix}%")
                if limit:
                    query += " LIMIT ?"
                    params.append(limit)

                cursor = conn.execute(query, params)
                return [json.loads(row[0]) for row in cursor.fetchall()]

        loop = asyncio.get_running_loop()
        items = await loop.run_in_executor(None, _list)
        for item in items:
            yield item

    async def is_healthy(self) -> bool:
        try:
            import os

            return os.path.exists(self.db_path)
        except Exception:
            return False


class CompositeBlobIndex(IBlobIndex):
    """Dual-Tier Composite Index with automatic fallback."""

    def __init__(self, primary: IBlobIndex, fallback: IBlobIndex):
        self.primary = primary
        self.fallback = fallback

    async def _get_active(self) -> IBlobIndex:
        if await self.primary.is_healthy():
            return self.primary
        return self.fallback

    async def set_metadata(self, key: str, metadata: dict[str, Any]) -> bool:
        # Write to both if possible for consistency (Cache Offline Persistency mandate)
        success_p = await self.primary.set_metadata(key, metadata)
        success_f = await self.fallback.set_metadata(key, metadata)
        return success_p or success_f

    async def get_metadata(self, key: str) -> Optional[dict[str, Any]]:
        active = await self._get_active()
        return await active.get_metadata(key)

    async def delete_metadata(self, key: str) -> bool:
        await self.primary.delete_metadata(key)
        await self.fallback.delete_metadata(key)
        return True

    async def list_metadata(
        self, prefix: Optional[str] = None, limit: Optional[int] = None
    ) -> AsyncIterator[dict[str, Any]]:
        active = await self._get_active()
        async for item in active.list_metadata(prefix, limit):
            yield item

    async def is_healthy(self) -> bool:
        return await self.primary.is_healthy() or await self.fallback.is_healthy()
