"""
Data Manager Module.

Provides the DataManager class for unified filesystem-level data lifecycle
operations: read, write, delete, archive, and path resolution.

All paths are resolved relative to the configured data_dir from settings.
All I/O operations are async (aiofiles) to support non-blocking execution.
"""

from __future__ import annotations

import asyncio
import csv
import hashlib
import io
import json
import os
import shutil
import tarfile
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, AsyncIterator, Optional, TypeVar, Union, cast

import aiofiles  # type: ignore[import-untyped]
import aiofiles.os  # type: ignore[import-untyped]
from pydantic import BaseModel

try:
    from redis.asyncio import Redis

    _HAS_REDIS = True
except ImportError:
    # Use Any for type hinting fallback to avoid LSP errors
    # and provide a dummy Redis class if needed.
    from typing import Generic, TypeVar

    TR = TypeVar("TR")

    class Redis(Generic[TR]):  # type: ignore[no-redef]
        async def get(self, *args: Any, **kwargs: Any) -> Any: ...
        async def setex(self, *args: Any, **kwargs: Any) -> Any: ...
        async def delete(self, *args: Any, **kwargs: Any) -> Any: ...
        async def keys(self, *args: Any, **kwargs: Any) -> Any: ...
        async def publish(self, *args: Any, **kwargs: Any) -> Any: ...
        async def sadd(self, *args: Any, **kwargs: Any) -> Any: ...
        async def srem(self, *args: Any, **kwargs: Any) -> Any: ...
        async def smembers(self, *args: Any, **kwargs: Any) -> Any: ...

    _HAS_REDIS = False

from au_sys_storage.redis_client import StorageRedisFactory
from au_sys_storage.interfaces.backup import IBackupProvider
from au_sys_storage.shared.observability.logger_factory import get_component_logger
from au_sys_storage.shared.services.config.settings import get_settings

logger = get_component_logger("storage", "data_manager")
T = TypeVar("T", bound=BaseModel)


class DataManager(IBackupProvider):
    """
    Manager for filesystem-level data lifecycle operations.

    Provides unified access patterns for reading, writing, deleting, archiving,
    and resolving paths within the configured data directory.

    All I/O operations are async and non-blocking via aiofiles.
    The base_data_dir is created on initialisation if it does not exist.
    """

    def __init__(self, base_data_dir: Optional[str] = None) -> None:
        """
        Initialise DataManager.

        Args:
            base_data_dir: Optional override for the base data directory.
                If not provided, uses data_dir from settings.
        """
        if base_data_dir is None:
            base_data_dir = get_settings().data_dir

        self.base_data_dir = Path(base_data_dir).resolve()
        self.base_data_dir.mkdir(parents=True, exist_ok=True)

        # Initialize Redis for caching (Action 9)
        self.redis: Optional[Redis[Any]] = None  # type: ignore[type-arg]
        if _HAS_REDIS:
            try:
                settings = get_settings()
                self.redis = StorageRedisFactory.get_client(host=settings.redis_host, port=settings.redis_port)
                logger.debug("Redis client resolved for DataManager")

                # Check if we should warm the index on startup (Action 10)
                # Note: We check a potentially new setting here.
                if getattr(settings, "data_index_warm_on_start", False):
                    asyncio.create_task(self.warm_index())
            except Exception as exc:
                logger.warning(
                    "Failed to resolve Redis client for DataManager",
                    extra={"error": str(exc)},
                )

        logger.debug(
            "DataManager initialised",
            extra={"base_data_dir": str(self.base_data_dir)},
        )

    # ------------------------------------------------------------------
    # Path resolution
    # ------------------------------------------------------------------

    def get_path(self, relative_path: str) -> Path:
        """
        Resolve a relative path against the base data directory.

        Args:
            relative_path: Path relative to base_data_dir.

        Returns:
            Absolute Path within base_data_dir.

        Raises:
            ValueError: If the resolved path escapes base_data_dir (path traversal).
        """
        resolved = (self.base_data_dir / relative_path).resolve()
        try:
            resolved.relative_to(self.base_data_dir)
        except ValueError as exc:
            raise ValueError(f"Path traversal detected: '{relative_path}' resolves outside base_data_dir") from exc
        return resolved

    # ------------------------------------------------------------------
    # Existence check
    # ------------------------------------------------------------------

    async def exists(self, relative_path: str) -> bool:
        """
        Check whether a file or directory exists within base_data_dir.

        Args:
            relative_path: Path relative to base_data_dir.

        Returns:
            True if the path exists, False otherwise.
        """
        target = self.get_path(relative_path)
        return cast(bool, await aiofiles.os.path.exists(target))

    # ------------------------------------------------------------------
    # Read operations
    # ------------------------------------------------------------------

    async def read_file(self, relative_path: str, encoding: str = "utf-8") -> str:
        """
        Read text content from a file.

        Args:
            relative_path: Path relative to base_data_dir.
            encoding: Text encoding (default: utf-8).

        Returns:
            File contents as a string.

        Raises:
            FileNotFoundError: If the file does not exist.
        """
        target = self.get_path(relative_path)

        # Check Redis cache (Action 9)
        if self.redis:
            try:
                stat = target.stat()
                cache_key = f"data:{relative_path}:{stat.st_mtime_ns}"
                cached = await self.redis.get(cache_key)
                if cached is not None:
                    logger.debug("Cache hit", extra={"path": relative_path})
                    return cached if isinstance(cached, str) else cached.decode(encoding)
            except Exception as exc:
                logger.debug("Cache read failed", extra={"error": str(exc)})

        logger.debug("Reading file", extra={"path": str(target)})
        async with aiofiles.open(target, encoding=encoding) as fh:
            content: str = cast(str, await fh.read())

        # Update cache (Action 9)
        if self.redis:
            try:
                stat = target.stat()
                cache_key = f"data:{relative_path}:{stat.st_mtime_ns}"
                await self.redis.setex(cache_key, 3600, content)
            except Exception as exc:
                logger.debug("Cache write failed", extra={"error": str(exc)})

        return content

    async def read_bytes(self, relative_path: str) -> bytes:
        """
        Read raw bytes from a file.

        Args:
            relative_path: Path relative to base_data_dir.

        Returns:
            File contents as bytes.

        Raises:
            FileNotFoundError: If the file does not exist.
        """
        target = self.get_path(relative_path)

        # Check Redis cache (Action 9)
        if self.redis:
            try:
                stat = target.stat()
                cache_key = f"data:{relative_path}:{stat.st_mtime_ns}:raw"
                cached = await self.redis.get(cache_key)
                if cached is not None:
                    logger.debug("Cache hit (bytes)", extra={"path": relative_path})
                    return cached if isinstance(cached, bytes) else cached.encode("utf-8")
            except Exception as exc:
                logger.debug("Cache read failed", extra={"error": str(exc)})

        logger.debug("Reading bytes", extra={"path": str(target)})
        async with aiofiles.open(target, mode="rb") as fh:
            content: bytes = cast(bytes, await fh.read())

        # Update cache (Action 9)
        if self.redis:
            try:
                stat = target.stat()
                cache_key = f"data:{relative_path}:{stat.st_mtime_ns}:raw"
                await self.redis.setex(cache_key, 3600, content)
            except Exception as exc:
                logger.debug("Cache write failed", extra={"error": str(exc)})

        return content

    async def read_json(self, relative_path: str, encoding: str = "utf-8") -> Any:
        """
        Read and parse a JSON file.

        Args:
            relative_path: Path relative to base_data_dir.
            encoding: Text encoding (default: utf-8).

        Returns:
            Parsed JSON value (dict, list, str, int, float, bool, or None).

        Raises:
            FileNotFoundError: If the file does not exist.
            json.JSONDecodeError: If the file is not valid JSON.
        """
        content = await self.read_file(relative_path, encoding=encoding)
        return json.loads(content)

    # ------------------------------------------------------------------
    # Write operations
    # ------------------------------------------------------------------

    async def write_file(
        self,
        relative_path: str,
        content: Union[str, bytes],
        encoding: str = "utf-8",
        create_parents: bool = True,
    ) -> Path:
        """
        Write text or bytes to a file, creating parent directories if needed.

        Args:
            relative_path: Path relative to base_data_dir.
            content: Text string or raw bytes to write.
            encoding: Text encoding used when content is a string (default: utf-8).
            create_parents: If True, create missing parent directories (default: True).

        Returns:
            Absolute Path of the written file.
        """
        target = self.get_path(relative_path)
        if create_parents:
            target.parent.mkdir(parents=True, exist_ok=True)

        logger.debug("Writing file", extra={"path": str(target)})
        if isinstance(content, bytes):
            async with aiofiles.open(target, mode="wb") as fh:
                await fh.write(content)
        else:
            async with aiofiles.open(target, mode="w", encoding=encoding) as fh:
                await fh.write(content)

        # Invalidate cache (Action 9)
        if self.redis:
            try:
                # We use keys matching data:path:* to invalidate all versions
                # Note: This is an expensive operation if Redis has millions of keys.
                # A better way would be using a dedicated set for each path's versions.
                # For now, we follow the pattern strategy.
                pattern = f"data:{relative_path}:*"
                keys = await self.redis.keys(pattern)
                if keys:
                    await self.redis.delete(*keys)

                # Update key index (Action 10)
                await self.redis.sadd("data:index:files", relative_path)  # type: ignore[misc]

                # Publish mutation event (Action 11)
                event_payload = {
                    "event": "write",
                    "path": relative_path,
                    "timestamp": datetime.now(tz=UTC).isoformat(),
                }
                await self.redis.publish("data.events", json.dumps(event_payload))
            except Exception as exc:
                logger.debug("Cache update or event publication failed", extra={"error": str(exc)})

        return target

    async def write_json(
        self,
        relative_path: str,
        data: Any,
        encoding: str = "utf-8",
        indent: int = 2,
        create_parents: bool = True,
    ) -> Path:
        """
        Serialise data as JSON and write to a file.

        Args:
            relative_path: Path relative to base_data_dir.
            data: JSON-serialisable value.
            encoding: Text encoding (default: utf-8).
            indent: JSON indentation level (default: 2).
            create_parents: If True, create missing parent directories (default: True).

        Returns:
            Absolute Path of the written file.
        """
        serialised = json.dumps(data, indent=indent, ensure_ascii=False)
        return await self.write_file(
            relative_path,
            serialised,
            encoding=encoding,
            create_parents=create_parents,
        )

    # ------------------------------------------------------------------
    # Delete operation
    # ------------------------------------------------------------------

    async def delete_file(self, relative_path: str) -> None:
        """
        Delete a file from the data directory.

        Args:
            relative_path: Path relative to base_data_dir.

        Raises:
            FileNotFoundError: If the file does not exist.
            IsADirectoryError: If the path is a directory (use shutil for directories).
        """
        target = self.get_path(relative_path)
        logger.debug("Deleting file", extra={"path": str(target)})
        await aiofiles.os.remove(target)

        # Invalidate cache (Action 9)
        if self.redis:
            try:
                pattern = f"data:{relative_path}:*"
                keys = await self.redis.keys(pattern)
                if keys:
                    await self.redis.delete(*keys)

                # Update key index (Action 10)
                await self.redis.srem("data:index:files", relative_path)  # type: ignore[misc]

                # Publish mutation event (Action 11)
                event_payload = {
                    "event": "delete",
                    "path": relative_path,
                    "timestamp": datetime.now(tz=UTC).isoformat(),
                }
                await self.redis.publish("data.events", json.dumps(event_payload))
            except Exception as exc:
                logger.debug("Cache update or event publication failed", extra={"error": str(exc)})

    # ------------------------------------------------------------------
    # Archive operation
    # ------------------------------------------------------------------

    async def archive(
        self,
        relative_path: str,
        archive_subdir: str = "archive",
    ) -> Path:
        """
        Move a file to a timestamped archive location within base_data_dir.

        The archive filename is prefixed with an ISO-8601 UTC timestamp to
        preserve history and prevent name collisions.

        Args:
            relative_path: Path of the file to archive, relative to base_data_dir.
            archive_subdir: Subdirectory within base_data_dir used for archives
                (default: "archive").

        Returns:
            Absolute Path of the archived file.

        Raises:
            FileNotFoundError: If the source file does not exist.
        """
        source = self.get_path(relative_path)
        if not source.exists():
            raise FileNotFoundError(f"Cannot archive non-existent file: {source}")

        timestamp = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")
        archive_filename = f"{timestamp}_{source.name}"
        archive_dir = (self.base_data_dir / archive_subdir).resolve()
        archive_dir.mkdir(parents=True, exist_ok=True)
        destination = archive_dir / archive_filename

        logger.debug(
            "Archiving file",
            extra={"source": str(source), "destination": str(destination)},
        )
        await aiofiles.os.rename(source, destination)

        # Invalidate cache (Action 9)
        if self.redis:
            try:
                pattern = f"data:{relative_path}:*"
                keys = await self.redis.keys(pattern)
                if keys:
                    await self.redis.delete(*keys)

                # Update key index (Action 10)
                await self.redis.srem("data:index:files", relative_path)  # type: ignore[misc]

                # Publish mutation event (Action 11)
                event_payload = {
                    "event": "archive",
                    "path": relative_path,
                    "timestamp": datetime.now(tz=UTC).isoformat(),
                }
                await self.redis.publish("data.events", json.dumps(event_payload))
            except Exception as exc:
                logger.debug("Cache update or event publication failed", extra={"error": str(exc)})

        return destination

    # ------------------------------------------------------------------
    # Directory listing
    # ------------------------------------------------------------------

    async def list_files(self, pattern: str = "*") -> list[Path]:
        """
        List files in the base data directory matching a glob pattern.

        Args:
            pattern: Glob pattern relative to base_data_dir (default: "*").
                     Use "**/*" for recursive listing.

        Returns:
            Sorted list of matching absolute Paths (files only, not directories).
        """
        # Attempt to use Redis index (Action 10)
        if self.redis:
            try:
                # SMEMBERS returns all files in the index
                all_files = await self.redis.smembers("data:index:files")  # type: ignore[misc]
                if all_files:
                    # Filter matching files using pure path matching (simulating glob)
                    import fnmatch

                    matched = [self.base_data_dir / f for f in all_files if fnmatch.fnmatch(f, pattern)]
                    # Verify they still exist on disk (optional but safer)
                    results = sorted(p for p in matched if p.is_file())
                    logger.debug(
                        "Listed files (via Redis index)",
                        extra={"pattern": pattern, "count": len(results)},
                    )
                    return results
            except Exception as exc:
                logger.debug("Redis index lookup failed, falling back to disk", extra={"error": str(exc)})

        results = sorted(p for p in self.base_data_dir.glob(pattern) if p.is_file())
        logger.debug(
            "Listed files",
            extra={"pattern": pattern, "count": len(results)},
        )
        return results

    async def warm_index(self) -> int:
        """
        Scan base_data_dir and populate the Redis file index.

        Returns:
            Number of files indexed.
        """
        if not self.redis:
            return 0

        files = [str(p.relative_to(self.base_data_dir)) for p in self.base_data_dir.glob("**/*") if p.is_file()]

        if files:
            try:
                # Clear existing index and rebuild
                await self.redis.delete("data:index:files")
                await self.redis.sadd("data:index:files", *files)  # type: ignore[misc]
                logger.info("Redis file index warmed", extra={"count": len(files)})
                return len(files)
            except Exception as exc:
                logger.error("Failed to warm Redis index", extra={"error": str(exc)})

        return 0

    # ------------------------------------------------------------------
    # Health check
    # ------------------------------------------------------------------

    async def get_health_status(self) -> dict[str, Any]:
        """
        Return the health status of the DataManager.

        Returns:
            Dict containing:
                - healthy (bool): True if base_data_dir exists and is writable.
                - base_data_dir (str): Absolute path of the data directory.
                - is_writable (bool): Whether the directory is writable.
        """
        try:
            dir_exists = self.base_data_dir.exists()
            is_writable = os.access(self.base_data_dir, os.W_OK) if dir_exists else False
            return {
                "healthy": dir_exists and is_writable,
                "base_data_dir": str(self.base_data_dir),
                "is_writable": is_writable,
            }
        except Exception as exc:
            logger.error("Data manager health check failed", extra={"error": str(exc)})
            return {"healthy": False, "error": str(exc)}

    # ------------------------------------------------------------------
    # Streaming I/O
    # ------------------------------------------------------------------

    async def stream_read(
        self,
        relative_path: str,
        chunk_size: int = 1024 * 1024,
    ) -> AsyncIterator[bytes]:
        """
        Stream read a file in chunks.

        Args:
            relative_path: Path relative to base_data_dir.
            chunk_size: Size of each chunk in bytes (default: 1 MB).

        Yields:
            File chunks as bytes.

        Raises:
            FileNotFoundError: If the file does not exist.
        """
        target = self.get_path(relative_path)
        logger.debug("Streaming file", extra={"path": str(target), "chunk_size": chunk_size})
        async with aiofiles.open(target, mode="rb") as fh:
            while True:
                chunk = await fh.read(chunk_size)
                if not chunk:
                    break
                yield chunk

    async def stream_write(
        self,
        relative_path: str,
        source: AsyncIterator[bytes],
        create_parents: bool = True,
    ) -> Path:
        """
        Stream write bytes from an iterator to a file.

        Args:
            relative_path: Path relative to base_data_dir.
            source: Async iterator yielding bytes.
            create_parents: If True, create missing parent directories (default: True).

        Returns:
            Absolute Path of the written file.
        """
        target = self.get_path(relative_path)
        if create_parents:
            target.parent.mkdir(parents=True, exist_ok=True)

        logger.debug("Streaming write to file", extra={"path": str(target)})
        async with aiofiles.open(target, mode="wb") as fh:
            async for chunk in source:
                await fh.write(chunk)

        return target

    # ------------------------------------------------------------------
    # Data Integrity and Checksums
    # ------------------------------------------------------------------

    async def checksum(
        self,
        relative_path: str,
        algorithm: str = "sha256",
        chunk_size: int = 1024 * 1024,
    ) -> str:
        """
        Compute the checksum of a file.

        Args:
            relative_path: Path relative to base_data_dir.
            algorithm: Hash algorithm to use (default: sha256).
            chunk_size: Size of each chunk in bytes (default: 1 MB).

        Returns:
            Hex digest string.

        Raises:
            FileNotFoundError: If the file does not exist.
        """
        hasher = hashlib.new(algorithm)
        async for chunk in self.stream_read(relative_path, chunk_size=chunk_size):
            hasher.update(chunk)
        return hasher.hexdigest()

    async def verify_checksum(
        self,
        relative_path: str,
        expected: str,
        algorithm: str = "sha256",
    ) -> bool:
        """
        Verify that a file's checksum matches the expected value.

        Args:
            relative_path: Path relative to base_data_dir.
            expected: Expected hex digest string.
            algorithm: Hash algorithm to use (default: sha256).

        Returns:
            True if the checksum matches, False otherwise.
        """
        actual = await self.checksum(relative_path, algorithm=algorithm)
        if actual == expected:
            return True

        logger.warning(
            "Checksum verification failed",
            extra={
                "path": relative_path,
                "algorithm": algorithm,
                "expected": expected,
                "actual": actual,
            },
        )
        return False

    # ------------------------------------------------------------------
    # Atomic Config Manifest Lifecycle
    # ------------------------------------------------------------------

    async def read_config(
        self,
        relative_path: str,
        schema: type[T],
    ) -> T:
        """
        Read and validate a JSON configuration file against a Pydantic schema.

        Args:
            relative_path: Path relative to base_data_dir.
            schema: Pydantic BaseModel subclass for validation.

        Returns:
            Validated Pydantic model instance.

        Raises:
            ValueError: If validation fails.
            FileNotFoundError: If the file does not exist.
        """
        data = await self.read_json(relative_path)
        try:
            return schema.model_validate(data)
        except Exception as exc:
            raise ValueError(f"Configuration validation failed for '{relative_path}': {exc}") from exc

    async def write_config(
        self,
        relative_path: str,
        config: BaseModel,
        archive_existing: bool = True,
    ) -> Path:
        """
        Atomically write a Pydantic model as JSON to a file.

        Uses a .tmp sibling file and os.replace for atomicity.
        Optionally archives the existing file before replacement.

        Args:
            relative_path: Path relative to base_data_dir.
            config: Pydantic model instance.
            archive_existing: If True, archive the existing file if it exists.

        Returns:
            Absolute Path of the written file.
        """
        target = self.get_path(relative_path)
        tmp_path = target.with_suffix(f"{target.suffix}.tmp")

        try:
            # Serialise and write to temporary file
            data = config.model_dump()
            serialised = json.dumps(data, indent=2, ensure_ascii=False)
            async with aiofiles.open(tmp_path, mode="w", encoding="utf-8") as fh:
                await fh.write(serialised)

            # Archive existing if requested
            if archive_existing and await self.exists(relative_path):
                await self.archive(relative_path)

            # Atomic replace
            tmp_path.replace(target)
            return target
        except Exception as exc:
            # Clean up temporary file on failure
            if tmp_path.exists():
                os.remove(tmp_path)
            raise exc

    # ------------------------------------------------------------------
    # Staging and Temp Workspace
    # ------------------------------------------------------------------

    async def create_staging_file(
        self,
        suffix: Optional[str] = None,
        prefix: Optional[str] = None,
    ) -> Path:
        """
        Create a temporary file in the staging directory.

        Args:
            suffix: Optional filename suffix.
            prefix: Optional filename prefix.

        Returns:
            Absolute Path of the created temporary file.
        """
        staging_dir = (self.base_data_dir / "staging").resolve()
        staging_dir.mkdir(parents=True, exist_ok=True)

        _, path_str = tempfile.mkstemp(suffix=suffix, prefix=prefix, dir=str(staging_dir))
        return Path(path_str).resolve()

    async def cleanup_staging(self, older_than_seconds: int = 3600) -> dict[str, Any]:
        """
        Clean up files in the staging directory older than a certain duration.

        Args:
            older_than_seconds: File age threshold in seconds (default: 1 hour).

        Returns:
            Dict containing:
                - removed_count (int): Number of files removed.
                - reclaimed_bytes (int): Total bytes reclaimed.
        """
        staging_dir = (self.base_data_dir / "staging").resolve()
        if not staging_dir.exists():
            return {"removed_count": 0, "reclaimed_bytes": 0}

        now = time.time()
        removed_count = 0
        reclaimed_bytes = 0

        for path in staging_dir.glob("*"):
            if not path.is_file():
                continue

            try:
                mtime = path.stat().st_mtime
                if now - mtime > older_than_seconds:
                    size = path.stat().st_size
                    path.unlink()
                    removed_count += 1
                    reclaimed_bytes += size
            except Exception as exc:
                logger.error(
                    "Failed to delete staging file",
                    extra={"path": str(path), "error": str(exc)},
                )

        logger.info(
            "Staging cleanup complete",
            extra={"removed_count": removed_count, "reclaimed_bytes": reclaimed_bytes},
        )
        return {"removed_count": removed_count, "reclaimed_bytes": reclaimed_bytes}

    # ------------------------------------------------------------------
    # Directory Lifecycle
    # ------------------------------------------------------------------

    async def make_dir(self, relative_path: str) -> Path:
        """
        Create a directory and any missing parent directories.

        Args:
            relative_path: Path relative to base_data_dir.

        Returns:
            Absolute Path of the created directory.
        """
        target = self.get_path(relative_path)
        target.mkdir(parents=True, exist_ok=True)
        return target

    async def remove_dir(
        self,
        relative_path: str,
        recursive: bool = False,
    ) -> bool:
        """
        Remove a directory.

        Args:
            relative_path: Path relative to base_data_dir.
            recursive: If True, remove the directory and its contents
                using shutil.rmtree (default: False).

        Returns:
            True if the removal was successful, False otherwise.
        """
        target = self.get_path(relative_path)
        if not target.exists() or not target.is_dir():
            return False

        try:
            if recursive:
                shutil.rmtree(target)
            else:
                target.rmdir()
            logger.info(
                "Removed directory",
                extra={"path": relative_path, "recursive": recursive},
            )
            return True
        except Exception:
            return False

    # ------------------------------------------------------------------
    # Transformation Helpers
    # ------------------------------------------------------------------

    async def read_csv(
        self,
        relative_path: str,
        has_header: bool = True,
        encoding: str = "utf-8",
    ) -> Union[list[dict[str, Any]], list[list[str]]]:
        """
        Read and parse a CSV file.

        Args:
            relative_path: Path relative to base_data_dir.
            has_header: If True, parse as DictReader and return list of dicts.
                If False, parse as list of lists.
            encoding: Text encoding (default: utf-8).

        Returns:
            List of dicts if has_header=True, otherwise list of lists.
        """
        content = await self.read_file(relative_path, encoding=encoding)
        fh = io.StringIO(content)
        if has_header:
            dict_reader = csv.DictReader(fh)
            return [dict(row) for row in dict_reader]

        list_reader = csv.reader(fh)
        return [row for row in list_reader]

    async def write_csv(
        self,
        relative_path: str,
        rows: Union[list[dict[str, Any]], list[list[Any]]],
        fieldnames: Optional[list[str]] = None,
        encoding: str = "utf-8",
        create_parents: bool = True,
    ) -> Path:
        """
        Serialise rows to CSV and write to a file.

        Args:
            relative_path: Path relative to base_data_dir.
            rows: List of dicts or list of lists to write.
            fieldnames: List of column names (required if rows are dicts).
            encoding: Text encoding (default: utf-8).
            create_parents: If True, create missing parent directories (default: True).

        Returns:
            Absolute Path of the written file.
        """
        fh = io.StringIO()
        if fieldnames:
            dict_writer = csv.DictWriter(fh, fieldnames=fieldnames)
            dict_writer.writeheader()
            # If fieldnames is provided, we expect rows to be list[dict[str, Any]]
            dict_writer.writerows(cast(list[dict[str, Any]], rows))
        else:
            list_writer = csv.writer(fh)
            # If no fieldnames, we expect rows to be list[list[Any]]
            list_writer.writerows(cast(list[list[Any]], rows))

        return await self.write_file(
            relative_path,
            fh.getvalue(),
            encoding=encoding,
            create_parents=create_parents,
        )

    async def read_ndjson(
        self,
        relative_path: str,
        encoding: str = "utf-8",
    ) -> list[dict[str, Any]]:
        """
        Read and parse a Newline Delimited JSON (NDJSON) file.

        Args:
            relative_path: Path relative to base_data_dir.
            encoding: Text encoding (default: utf-8).

        Returns:
            List of parsed JSON objects.
        """
        content = await self.read_file(relative_path, encoding=encoding)
        lines = content.strip().split("\n")
        return [json.loads(line) for line in lines if line.strip()]

    async def write_ndjson(
        self,
        relative_path: str,
        records: list[dict[str, Any]],
        encoding: str = "utf-8",
        create_parents: bool = True,
    ) -> Path:
        """
        Serialise objects as NDJSON and write to a file.

        Args:
            relative_path: Path relative to base_data_dir.
            records: List of JSON-serialisable objects.
            encoding: Text encoding (default: utf-8).
            create_parents: If True, create missing parent directories (default: True).

        Returns:
            Absolute Path of the written file.
        """
        serialised = "\n".join(json.dumps(r, ensure_ascii=False) for r in records)
        return await self.write_file(
            relative_path,
            serialised + "\n",
            encoding=encoding,
            create_parents=create_parents,
        )

    # ------------------------------------------------------------------
    # Backup Interface Implementation
    # ------------------------------------------------------------------

    async def create_backup(
        self,
        backup_path: str,
        metadata: Optional[dict[str, Any]] = None,
    ) -> bool:
        """
        Create a backup of the data directory asynchronously.

        Args:
            backup_path: Absolute or relative path (to base_data_dir)
                for the backup tarball.
            metadata: Optional metadata to include (currently not stored).

        Returns:
            True if the backup was successful, False otherwise.
        """
        try:
            # Resolve backup path. If it starts with "/" or "C:", treat as absolute,
            # otherwise resolve relative to base_data_dir.
            if backup_path.startswith("/") or ":" in backup_path:
                target_path = Path(backup_path).resolve()
            else:
                target_path = self.get_path(backup_path)

            target_path.parent.mkdir(parents=True, exist_ok=True)

            logger.info(
                "Starting backup",
                extra={"source": str(self.base_data_dir), "destination": str(target_path)},
            )

            # Use aiofiles.os.wrap for blocking tarfile operations if needed,
            # but tarfile is standard lib. We run in a thread to keep it async-friendly.
            def _create_tarball() -> None:
                with tarfile.open(target_path, "w:gz") as tar:
                    tar.add(self.base_data_dir, arcname=".")

            await asyncio.to_thread(_create_tarball)

            size = target_path.stat().st_size
            logger.info(
                "Backup complete",
                extra={
                    "destination": str(target_path),
                    "size_bytes": size,
                    "metadata": metadata,
                },
            )
            return True
        except Exception as exc:
            logger.error("Backup failed", extra={"error": str(exc)})
            return False

    async def restore_backup(
        self,
        backup_path: str,
        clear_existing: bool = False,
    ) -> bool:
        """
        Restore data from a backup tarball.

        Args:
            backup_path: Absolute or relative path (to base_data_dir)
                of the backup tarball.
            clear_existing: If True, clear base_data_dir before extracting.

        Returns:
            True if the restore was successful, False otherwise.
        """
        try:
            if backup_path.startswith("/") or ":" in backup_path:
                source_path = Path(backup_path).resolve()
            else:
                source_path = self.get_path(backup_path)

            if not source_path.exists():
                logger.error("Restore failed: backup file not found", extra={"path": str(source_path)})
                return False

            logger.info("Starting restore", extra={"source": str(source_path)})

            if clear_existing:
                logger.info("Clearing existing data before restore", extra={"dir": str(self.base_data_dir)})
                for item in self.base_data_dir.glob("*"):
                    if item.is_file():
                        item.unlink()
                    elif item.is_dir():
                        shutil.rmtree(item)

            def _extract_tarball() -> None:
                with tarfile.open(source_path, "r:gz") as tar:
                    tar.extractall(self.base_data_dir, filter="data")  # nosec S202

            await asyncio.to_thread(_extract_tarball)

            logger.info("Restore complete", extra={"source": str(source_path)})
            return True
        except Exception as exc:
            logger.error("Restore failed", extra={"error": str(exc)})
            return False

    async def list_backups(self, backup_dir: str) -> dict[str, dict[str, Any]]:
        """
        List available backups in a directory.

        Args:
            backup_dir: Path relative to base_data_dir.

        Returns:
            Dict mapping filename to metadata (size, mtime).
        """
        target_dir = self.get_path(backup_dir)
        if not target_dir.exists() or not target_dir.is_dir():
            return {}

        backups = {}
        for path in target_dir.glob("*.tar.gz"):
            if path.is_file():
                stat = path.stat()
                backups[path.name] = {
                    "size": stat.st_size,
                    "modified": datetime.fromtimestamp(stat.st_mtime, tz=UTC).isoformat(),
                }
        return backups

    async def validate_backup(self, backup_path: str) -> dict[str, Any]:
        """
        Validate a backup file.

        Args:
            backup_path: Path relative to base_data_dir.

        Returns:
            Dict containing validation results (valid, error).
        """
        try:
            source_path = self.get_path(backup_path)
            if not source_path.exists():
                return {"valid": False, "error": "File not found"}

            def _check_tarball() -> list[str]:
                with tarfile.open(source_path, "r:gz") as tar:
                    return tar.getnames()

            names = await asyncio.to_thread(_check_tarball)
            return {"valid": True, "file_count": len(names)}
        except Exception as exc:
            return {"valid": False, "error": str(exc)}
