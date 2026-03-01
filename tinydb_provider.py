"""
TinyDB Storage Provider - Clean implementation following SOLID principles.

This provider implements the core storage interfaces for TinyDB.
It focuses solely on TinyDB operations without mixing concerns.

Scaffolded from rest-api-orchestrator `src/services/storage/providers/tinydb.py` (commit a5f0cc2a4f33f312e3d340c06f9aba4688ae9f01).
"""

import asyncio
import json
import os
import time
from contextlib import asynccontextmanager, contextmanager
from datetime import datetime, UTC
from pathlib import Path
from typing import Any, Optional

from tinydb import Query, TinyDB  # type: ignore[attr-defined]
from tinydb.table import Document

from src.shared.services.storage.interfaces.backup import IBackupProvider
from src.shared.services.storage.interfaces.health import HealthMonitor, IHealthCheck
from src.shared.services.storage.interfaces.storage import IStorageProvider, StorageError
from src.shared.services.storage.interfaces.sync import (
    ISyncProvider,
    SyncConflictResolution,
    SyncDirection,
    SyncResult,
)
from src.shared.services.storage.tinydb_doc_ids import deterministic_doc_id


def get_logger():
    from src.shared.observability.logger_factory import create_debug_logger

    return create_debug_logger(__name__)


class _LazyLoggerProxy:
    def __getattr__(self, item: str):
        return getattr(get_logger(), item)


logger: _LazyLoggerProxy = _LazyLoggerProxy()


class TinyDBProvider(IHealthCheck, IBackupProvider, ISyncProvider):
    """
    This provider focuses on TinyDB operations.
    It does not implement ICollectionProvider since TinyDB is document-based.

    Note: IStorageProvider is inherited via ISyncProvider, so it's not listed
    directly to avoid MRO (Method Resolution Order) conflicts.
    """

    def __init__(self, db_path: str):
        """
        Initialize TinyDB provider.

        Args:
            db_path: Path to the TinyDB file
        """
        self.db_path = Path(db_path)

        # Health monitoring
        self._health_monitor = HealthMonitor()

        # Encryption (Legacy/Compatibility)
        # These are now handled by the EncryptedProvider decorator in factory.py
        self.cipher = None
        self.encryption_key = None

        # Initialize database
        self.db: Optional[TinyDB] = None
        logger.debug(f"Initializing TinyDB provider at {self.db_path}")
        self._initialize_db()

    # ------------------------------------------------------------------ #
    # Internal helpers                                                   #
    # ------------------------------------------------------------------ #

    @contextmanager
    def _write_lock(self, timeout: float = 5.0):
        """
        Context manager for acquiring a write lock on the TinyDB file (sync).
        Prevents concurrent writes from multiple processes/threads.

        Args:
            timeout: Maximum time to wait for lock acquisition (seconds)

        Yields:
            True if lock acquired, False otherwise
        """
        lock_file_path = self.db_path.with_suffix(".write.lock")
        lock_acquired = False
        lock_fd = None

        try:
            # Try to acquire lock file (non-blocking with retries)
            max_lock_attempts = int(timeout * 2)  # Try every 500ms
            lock_attempt = 0

            while lock_attempt < max_lock_attempts:
                try:
                    # Try to create lock file exclusively (fails if exists)
                    lock_fd = os.open(str(lock_file_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                    # Write process ID to lock file for debugging
                    os.write(lock_fd, str(os.getpid()).encode())
                    os.fsync(lock_fd)  # Ensure written to disk
                    lock_acquired = True
                    logger.debug(f"Acquired write lock for {self.db_path} (PID: {os.getpid()})")
                    break
                except FileExistsError:
                    # Another process is writing - wait and retry
                    lock_attempt += 1
                    if lock_attempt < max_lock_attempts:
                        time.sleep(0.5)  # Wait 500ms before retry
                    else:
                        logger.warning(
                            f"Could not acquire write lock for {self.db_path} after {timeout}s. "
                            f"Another process may be writing. Operation may fail."
                        )
                        yield False
                        return
                except Exception as lock_error:
                    logger.warning(f"Failed to create write lock file: {lock_error}")
                    # Continue without lock (risky but better than failing completely)
                    yield False
                    return

            if lock_acquired:
                yield True
            else:
                yield False

        finally:
            # Always release lock
            if lock_fd is not None:
                try:
                    os.close(lock_fd)
                except OSError:
                    # Ignore errors when closing file descriptor
                    logger.debug("OSError while closing lock file descriptor (likely already closed)")

            if lock_acquired and lock_file_path.exists():
                try:
                    lock_file_path.unlink()
                    logger.debug(f"Released write lock for {self.db_path}")
                except Exception as unlock_error:
                    logger.warning(f"Failed to remove write lock file: {unlock_error}")

    @asynccontextmanager
    async def _async_write_lock(self, timeout: float = 5.0):
        """
        Asynchronous context manager for acquiring a write lock.
        Uses asyncio.sleep to avoid blocking the event loop while waiting.
        """
        lock_file_path = self.db_path.with_suffix(".write.lock")
        lock_acquired = False
        lock_fd = None

        try:
            max_lock_attempts = int(timeout * 2)
            lock_attempt = 0

            while lock_attempt < max_lock_attempts:
                try:
                    # Still use os.open for atomic cross-process locking
                    # We wrap it in a thread to be safe, though O_EXCL is fast
                    lock_fd = os.open(str(lock_file_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                    os.write(lock_fd, str(os.getpid()).encode())
                    os.fsync(lock_fd)
                    lock_acquired = True
                    break
                except FileExistsError:
                    lock_attempt += 1
                    if lock_attempt < max_lock_attempts:
                        await asyncio.sleep(0.5)  # Non-blocking wait
                    else:
                        logger.warning(
                            f"Could not acquire async write lock for {self.db_path} after {timeout}s. "
                            f"Another process may be writing. Operation may fail."
                        )
                        yield False
                        return
                except OSError as e:
                    logger.warning(f"Failed to acquire advisory lock (non-critical): {e}")
                    yield False
                    return
                except Exception as e:
                    logger.debug(f"Unexpected error during lock acquisition: {e}")
                    yield False
                    return

            yield lock_acquired

        finally:
            if lock_fd is not None:
                try:
                    os.close(lock_fd)
                except OSError as close_err:
                    logger.debug(f"Failed to close lock fd: {close_err}")
            if lock_acquired and lock_file_path.exists():
                try:
                    lock_file_path.unlink()
                except Exception as unlink_err:
                    logger.debug(f"Failed to remove lock file: {unlink_err}")

    def _is_data_encrypted(self) -> tuple[bool, bool]:
        """
        Check if the actual data in the file is encrypted.

        Final implementation: encryption is handled by the EncryptedProvider decorator
        at the factory level. This provider focuses on raw TinyDB operations.
        """
        return False, True

    def _recover_from_corruption(self) -> bool:
        """
        Attempt to recover from JSON corruption by creating a new database.
        Uses temporary path to avoid TinyDB reading corrupted file.
        Uses process-level lock file to prevent concurrent recovery attempts.

        NOTE: When encryption is enabled, JSONDecodeError may occur if:
        - The encryption key is wrong (decryption produces invalid data)
        - Encrypted values contain control characters that break JSON parsing
        - The file structure itself is corrupted
        - Multiple processes writing concurrently (TinyDB is not process-safe)

        In all cases, we recover by creating a new empty database.

        Returns:
            True if recovery succeeded, False otherwise
        """
        # Track recovery attempts to prevent infinite loops (per-instance)
        if not hasattr(self, "_recovery_in_progress"):
            self._recovery_in_progress = False

        if self._recovery_in_progress:
            # Already recovering in this instance - return False to prevent infinite loop
            logger.warning(f"Recovery already in progress for {self.db_path}, skipping to prevent infinite loop")
            return False

        # Process-level lock file to prevent concurrent recovery across multiple processes
        lock_file_path = self.db_path.with_suffix(".recovery.lock")
        lock_acquired = False

        # Try to acquire lock file (non-blocking)
        max_lock_attempts = 10
        lock_attempt = 0
        while lock_attempt < max_lock_attempts:
            try:
                # Try to create lock file exclusively (fails if exists)
                # Note: os is imported at module level, no local shadowing
                lock_fd = os.open(str(lock_file_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.close(lock_fd)
                lock_acquired = True
                break
            except FileExistsError:
                # Another process is recovering - wait and retry
                lock_attempt += 1
                if lock_attempt < max_lock_attempts:
                    time.sleep(0.5)  # Wait 500ms before retry
                else:
                    logger.warning(
                        f"Could not acquire recovery lock for {self.db_path} after {max_lock_attempts} attempts. "
                        f"Another process may be recovering. Skipping recovery to prevent conflicts."
                    )
                    return False
            except Exception as lock_error:
                logger.warning(f"Failed to create recovery lock file: {lock_error}")
                break  # Continue without lock (risky but better than failing completely)

        self._recovery_in_progress = True

        try:
            # Check if data is corrupted
            data_is_encrypted, can_read_encrypted = self._is_data_encrypted()
            encryption_mismatch = False  # Handled by decorator
            wrong_key = False  # Handled by decorator

            # Close current DB
            if self.db:
                try:
                    self.db.close()
                except Exception as e:
                    logger.debug(f"Error closing database during recovery attempt: {e}")
                self.db = None

            # Use temporary path to create new database (avoids reading corrupted/encrypted file)
            import os
            import shutil

            temp_path = self.db_path.with_suffix(".tmp.json")

            # Remove temp file if it exists
            if temp_path.exists():
                temp_path.unlink()

            # Create new empty database at temporary path (TinyDB never reads corrupted file)
            temp_db = TinyDB(str(temp_path))
            temp_db.close()

            # Now backup/delete corrupted file
            if self.db_path.exists():
                backup_path = self.db_path.with_suffix(
                    f".corrupted.{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}.json"
                )
                encryption_note = ""
                if encryption_mismatch:
                    encryption_note = " (DATA IS ENCRYPTED but no encryption key provided - cannot read encrypted data)"
                elif wrong_key:
                    encryption_note = " (DATA IS ENCRYPTED but encryption key may be wrong - cannot decrypt)"
                elif data_is_encrypted:
                    encryption_note = " (DATA IS ENCRYPTED)"

                try:
                    shutil.move(str(self.db_path), str(backup_path))
                    if encryption_mismatch:
                        logger.error(
                            f"Backed up encrypted file to: {backup_path}{encryption_note}. "
                            f"Set ENCRYPTION_KEY environment variable to read encrypted data."
                        )
                    elif wrong_key:
                        logger.error(
                            f"Backed up encrypted file to: {backup_path}{encryption_note}. "
                            f"Verify ENCRYPTION_KEY environment variable matches the encryption key used to encrypt this data."
                        )
                    else:
                        logger.warning(
                            f"Backed up corrupted/unreadable file to: {backup_path}{encryption_note}. "
                            f"This may indicate wrong encryption key or corrupted encrypted data."
                        )
                except Exception as move_error:
                    logger.warning(f"Failed to move corrupted file: {move_error}, trying direct delete")
                    try:
                        os.chmod(str(self.db_path), 0o600)
                        os.remove(str(self.db_path))
                        logger.info(f"Deleted corrupted file: {self.db_path}")
                    except Exception as delete_error:
                        logger.warning(f"Failed to delete corrupted file: {delete_error}, will overwrite")

            # Move temp file to final location
            if temp_path.exists():
                shutil.move(str(temp_path), str(self.db_path))
                logger.info(f"Moved new database from temp to final location: {self.db_path}")

            # Open the clean database
            self.db = TinyDB(str(self.db_path))
            self._health_monitor.update_health(True, {"file": str(self.db_path), "recovered": True})

            logger.info(f"Successfully recovered from JSON corruption: {self.db_path}")

            # Reset recovery flag
            self._recovery_in_progress = False

            # Release lock file
            if lock_acquired and lock_file_path.exists():
                try:
                    lock_file_path.unlink()
                except Exception as unlock_error:
                    logger.warning(f"Failed to remove recovery lock file: {unlock_error}")

            return True

        except Exception as recovery_error:
            # Reset recovery flag even on failure to prevent permanent lock
            self._recovery_in_progress = False

            # Release lock file even on failure
            if lock_acquired and lock_file_path.exists():
                try:
                    lock_file_path.unlink()
                except Exception as unlock_error:
                    logger.warning(f"Failed to remove recovery lock file after error: {unlock_error}")

            logger.error(f"Recovery failed for {self.db_path}: {recovery_error}")
            return False

    @staticmethod
    def _doc_id_for_key(key: str) -> int:
        """Derive a deterministic doc_id for a given key (stable across processes)."""
        return deterministic_doc_id(key)

    def _require_db(self) -> TinyDB:
        if self.db is None:
            raise StorageError(f"Database not initialized: {self.db_path}")
        return self.db

    def _ensure_document_for_key(self, key: str) -> tuple[Optional[dict[str, Any]], int]:
        """Return (document, doc_id) ensuring deterministic doc_id usage."""

        db = self._require_db()
        doc_id = self._doc_id_for_key(key)
        try:
            record = db.get(doc_id=doc_id)  # type: ignore[attr-defined]
        except (json.JSONDecodeError, ValueError) as json_error:
            # JSON corruption detected - re-raise to be handled by calling method
            raise json_error
        if record and record.get("key") == key:
            return dict(record), doc_id

        # Legacy records may exist with auto-increment doc_ids – fall back to key search
        query = Query()  # type: ignore[misc]
        legacy = db.get(query.key == key)  # type: ignore[attr-defined]
        if isinstance(legacy, Document):
            legacy_dict: dict[str, Any] = dict(legacy)
            legacy_doc_id = legacy.doc_id
        elif legacy is not None:
            legacy_dict = dict(legacy)
            legacy_doc_id = None
        else:
            return None, doc_id

        # Migrate legacy document to deterministic doc_id if necessary
        if legacy_doc_id and legacy_doc_id != doc_id:
            db.remove(doc_ids=[legacy_doc_id])  # type: ignore[attr-defined]
            db.insert(Document(legacy_dict, doc_id=doc_id))  # type: ignore[attr-defined]
        return legacy_dict, doc_id

    def _write_document(self, doc_id: int, key: str, value: Any, created_at: Optional[str] = None) -> None:
        db = self._require_db()
        timestamp = datetime.now(UTC).isoformat()
        document = {
            "key": key,
            "value": value,
            "created_at": created_at or timestamp,
            "updated_at": timestamp,
        }
        if db.contains(doc_id=doc_id):  # type: ignore[attr-defined]
            db.update(document, doc_ids=[doc_id])  # type: ignore[attr-defined]
        else:
            db.insert(Document(document, doc_id=doc_id))  # type: ignore[attr-defined]

    def _initialize_db(self) -> None:
        """Initialize TinyDB database."""
        try:
            # Resolve path to absolute to ensure consistent path handling
            self.db_path = self.db_path.resolve()

            # Ensure directory exists with explicit error handling
            parent_dir = self.db_path.parent
            try:
                parent_dir.mkdir(parents=True, exist_ok=True)
                # Verify directory was created and is accessible
                if not parent_dir.exists():
                    raise OSError(f"Failed to create directory: {parent_dir}")
                if not parent_dir.is_dir():
                    raise OSError(f"Path exists but is not a directory: {parent_dir}")
            except (OSError, PermissionError) as dir_error:
                self._health_monitor.update_health(False, {"error": f"Directory creation failed: {dir_error}"})
                logger.error(f"Failed to create TinyDB directory {parent_dir}: {dir_error}")
                raise StorageError(f"Failed to create TinyDB directory {parent_dir}: {dir_error}") from dir_error

            # Check if file exists
            # CRITICAL: Do NOT read/validate the file here - it causes race conditions during concurrent initialization.
            # TinyDB itself will handle file reading and corruption detection gracefully.
            # If the file is corrupted, TinyDB will raise an exception which we catch and handle below.
            if self.db_path.exists():
                file_size = self.db_path.stat().st_size
                if file_size == 0:
                    logger.debug(f"TinyDB file is empty, will be initialized as new database: {self.db_path}")
                else:
                    logger.debug(
                        f"TinyDB file exists at {self.db_path} ({file_size} bytes). Letting TinyDB handle file reading."
                    )

            # Ensure parent directory still exists before TinyDB initialization
            if not parent_dir.exists():
                parent_dir.mkdir(parents=True, exist_ok=True)

            # Initialize TinyDB - let it handle file reading atomically
            # If file is corrupted, TinyDB will raise an exception which we catch and handle
            try:
                self.db = TinyDB(str(self.db_path))
            except (json.JSONDecodeError, ValueError, UnicodeDecodeError) as init_error:
                # TinyDB failed to initialize - file is corrupted
                logger.warning(
                    f"TinyDB initialization failed for {self.db_path}: {init_error}. "
                    "File is corrupted. Attempting to backup and recreate."
                )
                # Force backup and delete corrupted file
                if self.db_path.exists():
                    backup_path = self.db_path.with_suffix(
                        f".corrupted.{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}.json"
                    )
                    try:
                        import shutil

                        shutil.move(str(self.db_path), str(backup_path))
                        logger.info(f"Corrupted file backed up to: {backup_path}")
                    except Exception as backup_error:
                        logger.warning(f"Failed to backup corrupted file: {backup_error}")
                        try:
                            self.db_path.unlink()
                        except Exception as delete_error:
                            logger.error(f"Failed to delete corrupted file: {delete_error}")
                            raise StorageError(
                                f"Failed to initialize TinyDB and cannot remove corrupted file: {init_error}"
                            ) from init_error
                # Retry initialization with new file
                try:
                    self.db = TinyDB(str(self.db_path))
                    logger.info(f"Successfully created new TinyDB file after corruption recovery: {self.db_path}")
                except Exception as retry_error:
                    logger.error(f"Failed to initialize TinyDB even after removing corrupted file: {retry_error}")
                    raise StorageError(
                        f"Failed to initialize TinyDB after corruption recovery: {retry_error}"
                    ) from retry_error

            self._health_monitor.update_health(True, {"file": str(self.db_path)})
            logger.info(f"TinyDB initialized: {self.db_path}")

        except Exception as e:
            # Non-fatal - log and continue; we don't want validation to prevent boot
            logger.debug(f"TinyDB initialization cleanup error: {e}")

        except StorageError:
            # Re-raise StorageError as-is (already wrapped)
            raise
        except Exception as e:
            self._health_monitor.update_health(False, {"error": str(e)})
            logger.error(f"TinyDB initialization failed: {e}")
            raise StorageError(f"Failed to initialize TinyDB: {e}") from e

    def _coerce_value_types(self, value: Any) -> Any:
        # Compatibility method, now just returns value
        return value

    def close(self) -> None:
        """Close TinyDB connection."""
        if self.db:
            self.db.close()
            self.db = None
            logger.info("TinyDB connection closed")

    # IStorageProvider implementation

    def get(self, key: str) -> Optional[Any]:
        """Retrieve a value by key."""
        try:
            record, _ = self._ensure_document_for_key(key)

            if record:
                self._health_monitor.update_health(True)
                return record.get("value")

            return None

        except (json.JSONDecodeError, ValueError) as json_error:
            # JSON error detected - may be corruption or encrypted data issue
            logger.error(f"TinyDB JSON corruption detected in {self.db_path} during get('{key}'): {json_error}")

            if self._recover_from_corruption():
                # Recovery succeeded - return None (key not found after recovery)
                logger.warning(f"Recovered from JSON error, returning None for key '{key}' (data was lost)")
                return None
            # Recovery failed - return None instead of raising to prevent infinite loops
            logger.warning(f"Recovery failed, returning None for key '{key}' to prevent infinite loop")
            return None
        except Exception as e:
            self._health_monitor.update_health(False, {"error": str(e)})
            logger.error(f"TinyDB get error for key '{key}' in {self.db_path}: {e}")
            raise StorageError(f"Failed to get key '{key}' from {self.db_path}: {e}")

    async def get_async(self, key: str) -> Optional[Any]:
        """Retrieve a value by key (async)."""
        return await asyncio.to_thread(self.get, key)

    def set(self, key: str, value: Any) -> bool:
        """Store a value with the given key (sync)."""
        with self._write_lock() as lock_acquired:
            return self._set_internal(key, value, lock_acquired)

    async def set_async(self, key: str, value: Any) -> bool:
        """Store a value with the given key (async)."""
        async with self._async_write_lock() as lock_acquired:
            return await asyncio.to_thread(self._set_internal, key, value, lock_acquired)

    def _set_internal(self, key: str, value: Any, lock_acquired: bool) -> bool:
        """Internal set implementation without independent locking."""
        try:
            if not lock_acquired:
                logger.warning(f"Operation set('{key}') proceeding without confirmed lock")

            existing, doc_id = self._ensure_document_for_key(key)
            created_at = existing.get("created_at") if isinstance(existing, dict) else None
            self._write_document(doc_id, key, value, created_at=created_at)

            self._health_monitor.update_health(True)
            return True

        except (json.JSONDecodeError, ValueError) as json_error:
            logger.error(f"TinyDB JSON corruption detected in {self.db_path} during set('{key}'): {json_error}")
            if self._recover_from_corruption():
                # Retry once after recovery
                try:
                    existing, doc_id = self._ensure_document_for_key(key)
                    created_at = existing.get("created_at") if isinstance(existing, dict) else None
                    self._write_document(doc_id, key, value, created_at=created_at)
                    return True
                except Exception:
                    return False
            return False
        except Exception as e:
            self._health_monitor.update_health(False, {"error": str(e)})
            raise StorageError(f"Failed to set key '{key}': {e}")

    def supports_ttl(self) -> bool:
        """
        Check if this adapter supports TTL (Time To Live).

        Returns:
            False - TinyDB does not natively support TTL
        """
        return False

    def delete(self, key: str) -> bool:
        """Delete a value by key (sync)."""
        with self._write_lock() as lock_acquired:
            return self._delete_internal(key, lock_acquired)

    async def delete_async(self, key: str) -> bool:
        """Delete a value by key (async)."""
        async with self._async_write_lock() as lock_acquired:
            return await asyncio.to_thread(self._delete_internal, key, lock_acquired)

    def _delete_internal(self, key: str, lock_acquired: bool) -> bool:
        """Internal delete implementation without independent locking."""
        try:
            if not lock_acquired:
                logger.warning(f"Operation delete('{key}') proceeding without confirmed lock")

            db = self._require_db()
            _, doc_id = self._ensure_document_for_key(key)
            removed = db.remove(doc_ids=[doc_id])

            self._health_monitor.update_health(True)
            return len(removed) > 0

        except (json.JSONDecodeError, ValueError) as json_error:
            logger.error(f"TinyDB JSON corruption detected in {self.db_path} during delete('{key}'): {json_error}")
            if self._recover_from_corruption():
                return False
            return False
        except Exception as e:
            self._health_monitor.update_health(False, {"error": str(e)})
            raise StorageError(f"Failed to delete key '{key}': {e}")

    def exists(self, key: str) -> bool:
        """Check if a key exists."""
        try:
            db = self._require_db()
            record, doc_id = self._ensure_document_for_key(key)
            if record:
                self._health_monitor.update_health(True)
                return True

            if db.contains(doc_id=doc_id):  # type: ignore[attr-defined]
                self._health_monitor.update_health(True)
                return True

            self._health_monitor.update_health(True)
            return False

        except (json.JSONDecodeError, ValueError) as json_error:
            # JSON corruption detected - try to recover ONCE
            logger.error(f"TinyDB JSON corruption detected in {self.db_path} during exists('{key}'): {json_error}")
            if self._recover_from_corruption():
                # Recovery succeeded - return False (key doesn't exist after recovery)
                logger.warning(f"Recovered from corruption, returning False for exists('{key}') (data was lost)")
                return False
            # Recovery failed - return False instead of raising to prevent infinite loops
            logger.warning(f"Recovery failed, returning False for exists('{key}') to prevent infinite loop")
            return False

        except Exception as e:
            self._health_monitor.update_health(False, {"error": str(e)})
            logger.error(f"TinyDB exists error for key '{key}' in {self.db_path}: {e}")
            raise StorageError(f"Failed to check key '{key}' in {self.db_path}: {e}")

    async def exists_async(self, key: str) -> bool:
        """Check if a key exists (async)."""
        return await asyncio.to_thread(self.exists, key)

    def list_keys(self, pattern: Optional[str] = None) -> list[str]:
        """List all keys, optionally filtered by pattern."""
        # Track recovery attempts to prevent infinite loops
        if not hasattr(self, "_recovery_in_progress"):
            self._recovery_in_progress = False

        if self._recovery_in_progress:
            # Already recovering - return empty to prevent infinite loop
            logger.warning(
                f"Recovery already in progress for {self.db_path}, returning empty list to prevent infinite loop"
            )
            return []

        try:
            db = self._require_db()
            all_docs = db.all()
            keys = [doc["key"] for doc in all_docs if "key" in doc]

            if pattern:
                import re

                regex = re.compile(pattern, re.IGNORECASE)
                keys = [key for key in keys if regex.search(key)]

            self._health_monitor.update_health(True)
            return keys

        except (json.JSONDecodeError, ValueError) as json_error:
            # JSON corruption detected - try to recover ONCE
            if self._recovery_in_progress:
                # Already tried recovery - return empty to prevent infinite loop
                logger.error(
                    f"Recovery already attempted for {self.db_path}, returning empty list to prevent infinite loop"
                )
                return []

            self._recovery_in_progress = True
            self._health_monitor.update_health(False, {"error": str(json_error)})
            logger.error(
                f"TinyDB JSON corruption detected in {self.db_path}: {json_error}. "
                "Attempting ONE-TIME recovery using temporary path."
            )

            try:
                # Close current DB
                if self.db:
                    try:
                        self.db.close()
                    except Exception as e:
                        logger.debug(f"Error closing database during one-time recovery attempt: {e}")
                    self.db = None

                # Use temporary path to create new database (avoids reading corrupted file)
                import os
                import shutil

                temp_path = self.db_path.with_suffix(".tmp.json")

                # Remove temp file if it exists
                if temp_path.exists():
                    temp_path.unlink()

                # Create new empty database at temporary path (TinyDB never reads corrupted file)
                temp_db = TinyDB(str(temp_path))
                temp_db.close()

                # Now backup/delete corrupted file
                if self.db_path.exists():
                    backup_path = self.db_path.with_suffix(
                        f".corrupted.{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}.json"
                    )
                    try:
                        shutil.move(str(self.db_path), str(backup_path))
                        logger.info(f"Backed up corrupted file to: {backup_path}")
                    except Exception as move_error:
                        logger.warning(f"Failed to move corrupted file: {move_error}, trying direct delete")
                        try:
                            os.chmod(str(self.db_path), 0o600)
                            os.remove(str(self.db_path))
                            logger.info(f"Deleted corrupted file: {self.db_path}")
                        except Exception as delete_error:
                            logger.warning(f"Failed to delete corrupted file: {delete_error}, will overwrite")

                # Move temp file to final location
                if temp_path.exists():
                    shutil.move(str(temp_path), str(self.db_path))
                    logger.info(f"Moved new database from temp to final location: {self.db_path}")

                # Open the clean database
                self.db = TinyDB(str(self.db_path))
                self._health_monitor.update_health(True, {"file": str(self.db_path), "recovered": True})
                logger.info("Successfully recovered from JSON corruption - returning empty list (data was lost)")

                # Reset recovery flag
                self._recovery_in_progress = False
                return []  # Return empty list since corrupted data was lost

            except Exception as recovery_error:
                # Reset recovery flag even on failure to prevent permanent lock
                self._recovery_in_progress = False
                logger.error(f"Recovery failed for {self.db_path}: {recovery_error}")
                # Return empty list instead of raising to prevent infinite loops
                logger.warning("Returning empty list due to recovery failure to prevent infinite loop")
                return []
        except Exception as e:
            self._health_monitor.update_health(False, {"error": str(e)})
            logger.error(f"TinyDB list_keys error in {self.db_path}: {e}")
            raise StorageError(f"Failed to list keys from {self.db_path}: {e}")

    def find(self, query: dict[str, Any]) -> list[Any]:
        """Find items matching equality query."""
        try:
            db = self._require_db()
            q = Query()

            # Build TinyDB query
            tinydb_query = None
            for key, value in query.items():
                # We search inside the 'value' dict which is how TinyDBProvider stores data
                # Schema: {'key': '...', 'value': {...}, ...}
                condition = q.value[key] == value
                if tinydb_query is None:
                    tinydb_query = condition
                else:
                    tinydb_query &= condition

            if tinydb_query is None:
                return [doc["value"] for doc in db.all() if "value" in doc]

            results = db.search(tinydb_query)
            return [doc["value"] for doc in results if "value" in doc]
        except Exception as e:
            logger.error(f"TinyDB find error: {e}")
            raise StorageError(f"Find operation failed: {e}")

    def clear(self) -> int:
        """Clear all data from storage (sync)."""
        with self._write_lock() as lock_acquired:
            return self._clear_internal(lock_acquired)

    async def clear_async(self) -> int:
        """Clear all data from storage (async)."""
        async with self._async_write_lock() as lock_acquired:
            return await asyncio.to_thread(self._clear_internal, lock_acquired)

    def _clear_internal(self, lock_acquired: bool) -> int:
        """Internal clear implementation without independent locking."""
        try:
            if not lock_acquired:
                logger.warning("Operation clear() proceeding without confirmed lock")

            db = self._require_db()
            count = len(db)
            db.truncate()

            self._health_monitor.update_health(True)
            return count

        except Exception as e:
            self._health_monitor.update_health(False, {"error": str(e)})
            raise StorageError(f"Failed to clear storage: {e}")

    # IHealthCheck implementation

    def is_healthy(self) -> bool:
        """Check if TinyDB is healthy (basic check)."""
        try:
            if not self.db:
                return False
            # Read-only check
            len(self.db)
            self._health_monitor.update_health(True)
            return True
        except Exception:
            self._health_monitor.update_health(False)
            return False

    def perform_deep_health_check(self) -> bool:
        """Perform a deep health check (write/read/delete cycle)."""
        test_key = f"__health_check_{int(time.time())}__"
        test_value = {"status": "ok", "timestamp": time.time()}

        try:
            # 1. Write
            self.set(test_key, test_value)

            # 2. Read
            read_back = self.get(test_key)
            if read_back != test_value:
                logger.error(f"TinyDB deep health check failed: read mismatch for {test_key}")
                self._health_monitor.update_health(False, {"error": "read_mismatch"})
                return False

            # 3. Delete
            self.delete(test_key)

            self._health_monitor.update_health(True)
            return True

        except Exception as e:
            logger.error(f"TinyDB deep health check failed: {e}")
            self._health_monitor.update_health(False, {"error": str(e)})
            return False

    async def perform_deep_health_check_async(self) -> bool:
        """Asynchronous deep health check."""
        test_key = f"__health_check_async_{int(time.time())}__"
        test_value = {"status": "ok", "timestamp": time.time()}

        try:
            # 1. Write
            await self.set_async(test_key, test_value)

            # 2. Read
            read_back = await self.get_async(test_key)
            if read_back != test_value:
                self._health_monitor.update_health(False, {"error": "async_read_mismatch"})
                return False

            # 3. Delete
            await self.delete_async(test_key)

            self._health_monitor.update_health(True)
            return True

        except Exception as e:
            logger.error(f"TinyDB deep async health check failed: {e}")
            self._health_monitor.update_health(False, {"error": str(e)})
            return False

    def get_health_status(self) -> dict[str, Any]:
        """Get detailed health information."""
        status = self._health_monitor.get_health_status()

        # Add TinyDB-specific health info
        try:
            db = self.db
            if db and self.db_path.exists():
                file_stats = self.db_path.stat()
                status["details"].update(
                    {
                        "file_path": str(self.db_path),
                        "file_size_bytes": file_stats.st_size,
                        "file_modified": datetime.fromtimestamp(file_stats.st_mtime).isoformat(),
                        "document_count": len(db),
                        "encryption_enabled": False,  # Handled by decorator
                    }
                )
        except Exception as e:
            # Health check failure is non-critical - log and continue
            logger.debug(f"Health check error (non-critical): {e}")

        return status

    def get_last_health_check(self) -> datetime:
        """Get timestamp of last health check."""
        return self._health_monitor.get_last_health_check()

    # IBackupProvider implementation

    def create_backup(self, backup_path: str, metadata: Optional[dict[str, Any]] = None) -> bool:
        """Create a backup of TinyDB data."""
        try:
            if not self.db:
                raise StorageError("Database not initialized")

            # Get all data (decrypted if needed)
            all_data = []
            for doc in self.db.all():
                if "key" in doc and "value" in doc:
                    decrypted_doc = {
                        "key": doc["key"],
                        "value": doc["value"],
                        "created_at": doc.get("created_at"),
                        "updated_at": doc.get("updated_at"),
                    }
                    all_data.append(decrypted_doc)

            # Create backup data
            backup_data = {
                "metadata": {
                    "created_at": datetime.now(UTC).isoformat(),
                    "source_file": str(self.db_path),
                    "count": len(all_data),
                    "encryption_enabled": False,  # Handled by decorator
                    **(metadata or {}),
                },
                "data": all_data,
            }

            # Write backup file
            backup_file = Path(backup_path)
            backup_file.parent.mkdir(parents=True, exist_ok=True)

            with open(backup_file, "w") as f:
                json.dump(backup_data, f, indent=2, default=str)

            logger.info(f"TinyDB backup created: {backup_path} ({len(all_data)} documents)")
            return True

        except Exception as e:
            logger.error(f"TinyDB backup failed: {e}")
            return False

    def restore_backup(self, backup_path: str, clear_existing: bool = False) -> bool:
        """Restore data from backup."""
        try:
            if not self.db:
                raise StorageError("Database not initialized")

            with open(backup_path) as f:
                backup_data = json.load(f)

            if clear_existing:
                self.db.truncate()

            # Restore data
            documents = backup_data.get("data", [])
            for doc in documents:
                if "key" in doc and "value" in doc:
                    self.set(doc["key"], doc["value"])

            logger.info(f"TinyDB restore completed: {len(documents)} documents")
            return True

        except Exception as e:
            logger.error(f"TinyDB restore failed: {e}")
            return False

    def list_backups(self, backup_dir: str) -> dict[str, dict[str, Any]]:
        """List available backups."""
        try:
            backup_path = Path(backup_dir)
            backups = {}

            if backup_path.exists():
                for backup_file in backup_path.glob("*.json"):
                    try:
                        with open(backup_file) as f:
                            backup_data = json.load(f)

                        backups[backup_file.name] = backup_data.get("metadata", {})
                    except Exception:
                        continue

            return backups

        except Exception as e:
            logger.error(f"Failed to list backups: {e}")
            return {}

    def validate_backup(self, backup_path: str) -> dict[str, Any]:
        """Validate a backup file."""
        try:
            backup_file = Path(backup_path)

            if not backup_file.exists():
                return {"valid": False, "error": "File does not exist"}

            with open(backup_file) as f:
                backup_data = json.load(f)

            # Validate structure
            if "metadata" not in backup_data or "data" not in backup_data:
                return {"valid": False, "error": "Invalid backup structure"}

            metadata = backup_data["metadata"]
            data_count = len(backup_data["data"])
            expected_count = metadata.get("count", 0)

            return {
                "valid": True,
                "metadata": metadata,
                "actual_count": data_count,
                "expected_count": expected_count,
                "count_match": data_count == expected_count,
            }

        except Exception as e:
            return {"valid": False, "error": str(e)}

    # ISyncProvider implementation

    def sync_to(
        self,
        target_provider: "IStorageProvider",
        direction: SyncDirection = SyncDirection.TO_TARGET,
        conflict_resolution: SyncConflictResolution = SyncConflictResolution.NEWEST_WINS,
        dry_run: bool = False,
    ) -> SyncResult:
        """Synchronize data with another storage provider."""
        result = SyncResult()
        result.sync_direction = direction
        start_time = datetime.now(UTC)

        try:
            # Get data for sync
            sync_data = self.get_data_for_sync()
            result.source_count = len(sync_data)

            if direction == SyncDirection.TO_TARGET:
                # Sync from this provider to target
                if not dry_run and hasattr(target_provider, "apply_sync_data"):
                    items_applied, conflicts = target_provider.apply_sync_data(sync_data, conflict_resolution)
                    result.items_synced = items_applied
                    result.conflicts_found = len(conflicts)
                elif not dry_run:
                    # Fallback for providers without apply_sync_data method
                    for item in sync_data:
                        try:
                            target_provider.set(item["key"], item["value"])
                            result.items_synced += 1
                        except Exception as e:
                            result.errors.append(f"Failed to sync item {item['key']}: {str(e)}")

            elif direction == SyncDirection.FROM_TARGET:
                # Sync from target to this provider
                if hasattr(target_provider, "get_data_for_sync"):
                    target_data = target_provider.get_data_for_sync()
                    if not dry_run:
                        items_applied, conflicts = self.apply_sync_data(target_data, conflict_resolution)
                        result.items_synced = items_applied
                        result.conflicts_found = len(conflicts)

            result.success = True

        except Exception as e:
            result.success = False
            result.errors.append(f"Sync failed: {str(e)}")
            logger.error(f"TinyDB sync error: {e}")

        finally:
            end_time = datetime.now(UTC)
            result.sync_duration_seconds = (end_time - start_time).total_seconds()
            result.timestamp = end_time

        return result

    def get_sync_metadata(self) -> dict[str, Any]:
        """Get metadata about this provider's sync capabilities and state."""
        try:
            return {
                "provider_type": "tinydb",
                "supports_bidirectional": True,
                "supports_incremental": True,
                "file_path": str(self.db_path),
                "encryption_enabled": False,  # Handled by decorator
                "last_modified": (
                    datetime.fromtimestamp(self.db_path.stat().st_mtime).isoformat() if self.db_path.exists() else None
                ),
                "item_count": len(self.db) if self.db else 0,
                "healthy": self.is_healthy(),
            }
        except Exception as e:
            logger.error(f"Failed to get sync metadata: {e}")
            return {"provider_type": "tinydb", "error": str(e)}

    def prepare_for_sync(self) -> bool:
        """Prepare the storage provider for synchronization."""
        try:
            if not self.db:
                self._initialize_db()

            # Ensure database is healthy
            if not self.is_healthy():
                return False

            # For TinyDB, no special preparation needed
            return True

        except Exception as e:
            logger.error(f"TinyDB sync preparation failed: {e}")
            return False

    def cleanup_after_sync(self, sync_result: SyncResult) -> None:
        """Cleanup after synchronization operation."""
        try:
            # For TinyDB, no special cleanup needed
            # Log the sync result
            if sync_result.success:
                logger.info(f"TinyDB sync completed: {sync_result.items_synced} items synced")
            else:
                logger.error(f"TinyDB sync failed: {sync_result.errors}")

        except Exception as e:
            logger.error(f"TinyDB sync cleanup error: {e}")

    def get_data_for_sync(self, last_sync_timestamp: Optional[datetime] = None) -> list[dict[str, Any]]:
        """Get all data that needs to be synchronized."""
        try:
            if not self.db:
                return []

            sync_data = []

            for doc in self.db.all():
                if "key" in doc and "value" in doc:
                    # Prepare sync item with metadata
                    sync_item = {
                        "key": doc["key"],
                        "value": doc["value"],  # Sync raw data; decorator handles encryption
                        "created_at": doc.get("created_at"),
                        "updated_at": doc.get("updated_at"),
                        "provider_type": "tinydb",
                    }

                    # Filter by timestamp if provided
                    if last_sync_timestamp and "updated_at" in doc:
                        try:
                            updated_time = datetime.fromisoformat(doc["updated_at"].replace("Z", "+00:00"))
                            if updated_time <= last_sync_timestamp:
                                continue
                        except (ValueError, AttributeError) as e:
                            # Timestamp parsing failure - include item anyway (non-critical)
                            logger.debug(f"Timestamp parsing failed for sync item (including anyway): {e}")

                    sync_data.append(sync_item)

            logger.debug(f"TinyDB prepared {len(sync_data)} items for sync")
            return sync_data

        except Exception as e:
            logger.error(f"Failed to get TinyDB data for sync: {e}")
            return []

    def apply_sync_data(
        self,
        sync_data: list[dict[str, Any]],
        conflict_resolution: SyncConflictResolution = SyncConflictResolution.NEWEST_WINS,
    ) -> tuple[int, list[dict[str, Any]]]:
        """Apply synchronized data to this storage provider."""
        items_applied = 0
        conflicts_found = []

        try:
            for item in sync_data:
                if "key" not in item or "value" not in item:
                    continue

                key = item["key"]
                value = item["value"]

                # Check for conflicts
                if self.exists(key):
                    conflict_item = {
                        "key": key,
                        "source_value": value,
                        "target_value": self.get(key),
                        "source_updated": item.get("updated_at"),
                        "resolution": conflict_resolution.value,
                    }

                    # Resolve conflict based on strategy
                    should_update = False

                    if conflict_resolution == SyncConflictResolution.SOURCE_WINS:
                        should_update = True
                    elif conflict_resolution == SyncConflictResolution.TARGET_WINS:
                        should_update = False
                    elif conflict_resolution == SyncConflictResolution.NEWEST_WINS:
                        # Compare timestamps if available
                        source_time = item.get("updated_at")
                        if source_time:
                            try:
                                # Get current item timestamp
                                current_doc = None
                                if self.db:
                                    query = Query()
                                    current_doc = self.db.search(query.key == key)

                                if current_doc:
                                    target_time = current_doc[0].get("updated_at")
                                    if target_time and source_time:
                                        source_dt = datetime.fromisoformat(source_time.replace("Z", "+00:00"))
                                        target_dt = datetime.fromisoformat(target_time.replace("Z", "+00:00"))
                                        should_update = source_dt > target_dt
                                    else:
                                        should_update = True  # Update if no timestamp comparison possible
                                else:
                                    should_update = True
                            except (ValueError, AttributeError):
                                should_update = True  # Update if timestamp parsing fails

                    conflicts_found.append(conflict_item)

                    if not should_update:
                        continue

                # Apply the update
                self.set(key, value)
                items_applied += 1

        except Exception as e:
            logger.error(f"Failed to apply sync data to TinyDB: {e}")

        logger.info(f"TinyDB sync applied: {items_applied} items, {len(conflicts_found)} conflicts")
        return items_applied, conflicts_found
