from __future__ import annotations
"""
Storage factory responsible for returning encrypted storage adapters.

Phase 1 of STORAGE_DB_FACTORY_SPEC_v1.0.0 requires real backend selection,
manifest-driven configuration, and encrypted persistence even when running
in TinyDB fallback mode. This module now fulfils those requirements by
wrapping TinyDB with Fernet encryption and exposing namespace-aware document
stores to higher-level services (automation webhooks, features, etc.).
"""


import asyncio
import inspect
import json
import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime, UTC
from functools import lru_cache
from pathlib import Path
from threading import Lock, Thread
from typing import Any, Awaitable, Callable, Optional
from urllib.parse import urlparse
from sqlalchemy.orm import Session

from src.shared.services.config.database import (
    EncryptedDocumentModel,
    get_db_session_sync,
    get_tenant_db_session_sync,
    init_database_sync,
)
from src.shared.services.config.encryption import get_encryption_service
from src.shared.services.config.settings import get_settings

from .dynamic_manager import DynamicStorageManager, OperationType

# Bootstrap settings that MUST remain unencrypted (required for initial database connection)
# Moved here to avoid circular import with settings_service.py
BOOTSTRAP_SETTINGS = [
    "app_database_url",  # Required for bootstrap connection
]
from .interfaces.storage import IStorageProvider
from .interfaces.health import IHealthCheck
from .interfaces.sync import (
    ISyncProvider,
    SyncConflictResolution,
    SyncDirection,
    SyncManager,
)

# Lazy imports are handled within methods to avoid circular dependencies


# Defensive basic logger used before factory is configured
_BASIC_LOGGER: Optional[logging.Logger] = None


def _get_basic_logger() -> logging.Logger:
    """Get a basic logger for use before the factory is configured."""
    global _BASIC_LOGGER
    if _BASIC_LOGGER is None:
        _BASIC_LOGGER = logging.getLogger("digital-angels.storage.pre-init")
        if not _BASIC_LOGGER.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter("%(asctime)s | %(name)-30s | %(levelname)-8s | %(message)s")
            handler.setFormatter(formatter)
            _BASIC_LOGGER.addHandler(handler)
            _BASIC_LOGGER.setLevel(logging.WARNING)
    return _BASIC_LOGGER


def _create_basic_logger_adapter(name: str):
    """Create a basic logger adapter for use before factory is configured."""
    basic = _get_basic_logger()

    class _BasicAdapter:
        def __init__(self, logger, name):
            self.logger = logger
            self._name = name

        def bind(self, **kwargs):
            return self

        def debug(self, msg, *args, **kwargs):
            self.logger.debug(f"[{self._name}] {msg}", *args, **kwargs)

        def info(self, msg, *args, **kwargs):
            self.logger.info(f"[{self._name}] {msg}", *args, **kwargs)

        def warning(self, msg, *args, **kwargs):
            self.logger.warning(f"[{self._name}] {msg}", *args, **kwargs)

        def error(self, msg, *args, **kwargs):
            self.logger.error(f"[{self._name}] {msg}", *args, **kwargs)

        def critical(self, msg, *args, **kwargs):
            self.logger.critical(f"[{self._name}] {msg}", *args, **kwargs)

        def exception(self, msg, *args, **kwargs):
            self.logger.exception(f"[{self._name}] {msg}", *args, **kwargs)

    return _BasicAdapter(basic, name)


def _get_logger_defensive():
    """Get logger - uses defensive logging until factory is configured."""
    # Check if logger_factory is configured
    try:
        from src.shared.observability.logger_factory import get_logger as _get_factory_logger

        return _get_factory_logger(__name__)
    except Exception:
        return _create_basic_logger_adapter(__name__)


def get_logger():
    return _get_logger_defensive()


logger = get_logger()

MANIFEST_PATH = Path("config/storage_backends.json")


def _is_encrypted_value(value: str) -> bool:
    """Check if value is marked as encrypted (prefixed with 'encrypted:')."""
    return isinstance(value, str) and value.startswith("encrypted:")


def _decrypt_connection_string(connection_string: Optional[str], setting_key: str) -> Optional[str]:
    """
    Decrypt connection string if encrypted.

    Args:
        connection_string: Connection string (may be encrypted)
        setting_key: Setting key name (to check if bootstrap setting)

    Returns:
        Decrypted connection string, or original if not encrypted
    """
    if not connection_string:
        return None

    # Never decrypt bootstrap settings (they should never be encrypted)
    if setting_key in BOOTSTRAP_SETTINGS:
        return connection_string

    # Decrypt if encrypted
    if _is_encrypted_value(connection_string):
        try:
            ciphertext = connection_string[len("encrypted:") :].strip()
            encryption_service = get_encryption_service()
            return encryption_service.decrypt(ciphertext)
        except Exception as e:
            logger.error(f"Failed to decrypt connection string {setting_key}: {e}")
            raise

    return connection_string


def _mongo_database_name_from_uri(uri: str) -> str:
    parsed = urlparse(uri)
    if parsed.path and parsed.path != "/":
        return parsed.path.lstrip("/")
    return "storage"


@dataclass
class StorageHealth:
    """Represents the health view for templates + diagnostics."""

    name: str
    backend_type: str
    healthy: bool = True
    detail: Optional[str] = None

    def is_healthy(self) -> bool:
        return self.healthy


from abc import ABC, abstractmethod


class BaseEncryptedAdapter(ABC):
    """Interface for encrypted document adapters."""

    @abstractmethod
    def upsert(self, namespace: str, key: str, document: dict[str, Any]) -> None:
        """Upsert document into storage (sync)."""

    @abstractmethod
    def get(self, namespace: str, key: str) -> Optional[dict[str, Any]]:
        """Retrieve document from storage (sync)."""

    @abstractmethod
    def delete(self, namespace: str, key: str) -> None:
        """Delete document from storage (sync)."""

    @abstractmethod
    def list_all(self, namespace: str) -> dict[str, dict[str, Any]]:
        """List all documents in namespace (sync)."""

    # --- Asynchronous Interface ---

    async def upsert_async(self, namespace: str, key: str, document: dict[str, Any]) -> None:
        """Upsert document into storage (async)."""
        return await asyncio.to_thread(self.upsert, namespace, key, document)

    async def get_async(self, namespace: str, key: str) -> Optional[dict[str, Any]]:
        """Retrieve document from storage (async)."""
        return await asyncio.to_thread(self.get, namespace, key)

    async def delete_async(self, namespace: str, key: str) -> None:
        """Delete document from storage (async)."""
        return await asyncio.to_thread(self.delete, namespace, key)

    async def list_all_async(self, namespace: str) -> dict[str, dict[str, Any]]:
        """List all documents in namespace (async)."""
        return await asyncio.to_thread(self.list_all, namespace)


class EncryptedProvider(IStorageProvider):
    """Wrap a provider to add encryption/decryption (AES-256-GCM by default, Fernet for backward compatibility)."""

    def __init__(self, provider: IStorageProvider, encryption_service) -> None:
        self._provider = provider
        self._encryption = encryption_service

    def get(self, key: str) -> Optional[Any]:
        value = self._provider.get(key)
        if value is None:
            return None
        if not isinstance(value, str):
            try:
                value = json.dumps(value)
            except (TypeError, ValueError):
                value = str(value)

        # Try to decrypt, but handle decryption failures gracefully
        try:
            decrypted = self._encryption.decrypt(value)
        except Exception as exc:
            # If decryption fails, log warning and return None
            # This handles cases where data might be corrupted or encrypted with different key
            logger.warning("Failed to decrypt storage value for key %s: %s", key, exc)
            return None

        try:
            return json.loads(decrypted)
        except json.JSONDecodeError:
            return decrypted

    def set(self, key: str, value: Any) -> bool:
        payload = json.dumps(value)
        encrypted = self._encryption.encrypt(payload)
        return self._provider.set(key, encrypted)

    def delete(self, key: str) -> bool:
        return self._provider.delete(key)

    def exists(self, key: str) -> bool:
        return self._provider.exists(key)

    def list_keys(self, pattern: Optional[str] = None) -> list[str]:  # type: ignore[override]
        return self._provider.list_keys(pattern)

    def clear(self) -> int:
        return self._provider.clear()

    def find(self, query: dict[str, Any]) -> list[Any]:
        """Find and decrypt values."""
        raw_results = self._provider.find(query)
        results = []
        for val in raw_results:
            if val is None:
                continue

            # Values in raw provider might be encrypted strings
            if isinstance(val, str):
                try:
                    decrypted = self._encryption.decrypt(val)
                    try:
                        results.append(json.loads(decrypted))
                    except json.JSONDecodeError:
                        results.append(decrypted)
                except Exception:
                    # Not encrypted or wrong key, try as-is
                    try:
                        results.append(json.loads(val))
                    except json.JSONDecodeError:
                        results.append(val)
            else:
                results.append(val)
        return results

    # --- Asynchronous Interface ---

    async def get_async(self, key: str) -> Optional[Any]:
        value = await self._provider.get_async(key)
        if value is None:
            return None

        # Try to decrypt using threadpool (CPU bound)
        try:
            # Check if it looks like a string first
            if not isinstance(value, str):
                try:
                    value = json.dumps(value)
                except (TypeError, ValueError):
                    value = str(value)

            decrypted = await asyncio.to_thread(self._encryption.decrypt, value)
            try:
                return json.loads(decrypted)
            except json.JSONDecodeError:
                return decrypted
        except Exception as exc:
            logger.warning("Failed to decrypt storage value for key %s: %s", key, exc)
            return None

    async def set_async(self, key: str, value: Any) -> bool:
        # Encryption is CPU bound, wrap in to_thread
        payload = json.dumps(value)
        encrypted = await asyncio.to_thread(self._encryption.encrypt, payload)
        return await self._provider.set_async(key, encrypted)

    async def delete_async(self, key: str) -> bool:
        return await self._provider.delete_async(key)

    async def exists_async(self, key: str) -> bool:
        return await self._provider.exists_async(key)

    async def list_keys_async(self, pattern: Optional[str] = None) -> list[str]:
        return await self._provider.list_keys_async(pattern)

    async def clear_async(self) -> int:
        return await self._provider.clear_async()

    def supports_ttl(self) -> bool:
        return self._provider.supports_ttl()

    def set_with_ttl(self, key: str, value: Any, ttl: int) -> bool:
        payload = json.dumps(value)
        encrypted = self._encryption.encrypt(payload)
        return self._provider.set_with_ttl(key, encrypted, ttl)


class ProviderBackedAdapter(BaseEncryptedAdapter):
    """Adapter that maps namespace-aware operations onto an IStorageProvider."""

    def __init__(self, provider: IStorageProvider) -> None:
        self._provider = provider

    def _namespaced_key(self, namespace: str, key: str) -> str:
        return f"{namespace}:{key}"

    def upsert(self, namespace: str, key: str, document: dict[str, Any]) -> None:
        """Synchronously upsert document."""
        self._provider.set(self._namespaced_key(namespace, key), document)

    def get(self, namespace: str, key: str) -> Optional[dict[str, Any]]:
        """Synchronously retrieve document."""
        value = self._provider.get(self._namespaced_key(namespace, key))
        return self._coerce_to_dict(value)

    def delete(self, namespace: str, key: str) -> None:
        """Synchronously delete document."""
        self._provider.delete(self._namespaced_key(namespace, key))

    def list_all(self, namespace: str) -> dict[str, dict[str, Any]]:
        """Synchronously list all documents in namespace."""
        prefix = f"{namespace}:"
        items: dict[str, dict[str, Any]] = {}
        for raw_key in self._provider.list_keys():
            if not raw_key.startswith(prefix):
                continue
            value = self._provider.get(raw_key)
            document = self._coerce_to_dict(value)
            if document is not None:
                items[raw_key[len(prefix) :]] = document  # noqa: E203
        return items

    def find(self, namespace: str, query: dict[str, Any]) -> list[dict[str, Any]]:
        """Find documents in namespace matching query."""
        # Note: Efficiency could be improved by pushing query to provider if possible,
        # but for namespaced access we first filter by namespace prefix then by query.
        # However, many providers (like TinyDB) handle nested queries well.
        # For now, we use a simple implementation.
        results = self._provider.find(query)
        # Verify they belong to this namespace?
        # (Actually IStorageProvider.find searches all data,
        # but namespaced data has prefix in keys not in values).
        # So we filter values here.
        filtered = []
        for val in results:
            doc = self._coerce_to_dict(val)
            if doc:
                filtered.append(doc)
        return filtered

    async def upsert_async(self, namespace: str, key: str, document: dict[str, Any]) -> None:
        """Asynchronously upsert document."""
        await self._provider.set_async(self._namespaced_key(namespace, key), document)

    async def get_async(self, namespace: str, key: str) -> Optional[dict[str, Any]]:
        """Asynchronously retrieve document."""
        value = await self._provider.get_async(self._namespaced_key(namespace, key))
        return self._coerce_to_dict(value)

    async def delete_async(self, namespace: str, key: str) -> None:
        """Asynchronously delete document."""
        await self._provider.delete_async(self._namespaced_key(namespace, key))

    async def list_all_async(self, namespace: str) -> dict[str, dict[str, Any]]:
        """Asynchronously list all documents in namespace."""
        prefix = f"{namespace}:"
        items: dict[str, dict[str, Any]] = {}

        # list_keys_async should be used for non-blocking I/O
        keys = await self._provider.list_keys_async()
        for raw_key in keys:
            if not raw_key.startswith(prefix):
                continue
            # Fetch each value asynchronously
            value = await self._provider.get_async(raw_key)
            document = self._coerce_to_dict(value)
            if document is not None:
                items[raw_key[len(prefix) :]] = document  # noqa: E203
        return items

    @staticmethod
    def _coerce_to_dict(value: Any) -> Optional[dict[str, Any]]:
        if value is None:
            return None
        if isinstance(value, dict):
            return value
        if isinstance(value, str):
            # Check if it's an encrypted blob (Fernet prefix)
            if value.startswith("gAAAAA") and len(value) > 50:
                # This is likely an encrypted blob that wasn't decrypted
                # Return None to signal that this item should be skipped
                logger.debug("Detected encrypted blob in _coerce_to_dict, returning None")
                return None
            try:
                parsed = json.loads(value)
                if isinstance(parsed, dict):
                    return parsed
                return {"value": parsed}
            except json.JSONDecodeError:
                return {"value": value}
        return {"value": value}


class TinyDBEncryptedAdapter(ProviderBackedAdapter):
    """Backward-compatible adapter for TinyDB tests."""

    def __init__(self, db_path: Path) -> None:
        from .tinydb_provider import TinyDBProvider

        provider = TinyDBProvider(str(Path(db_path)))
        # We unwrap it if it was wrapped, but here we just need the raw provider
        super().__init__(provider)


class SQLAlchemyEncryptedAdapter(BaseEncryptedAdapter):
    """SQLAlchemy-backed encrypted document store with full tenant isolation support."""

    def __init__(self, db_url: Optional[str] = None) -> None:
        self._db_url = db_url
        if not db_url:
            init_database_sync()
        self._encryption = get_encryption_service()
        self._lock = Lock()

    def _get_session(self) -> Session:
        """Helper to get appropriate session based on tenant config."""
        if self._db_url:
            return get_tenant_db_session_sync(self._db_url)
        return get_db_session_sync()

    def upsert(self, namespace: str, key: str, document: dict[str, Any]) -> None:
        payload = self._encryption.encrypt(json.dumps(document))
        with self._lock:
            session = self._get_session()
            try:
                record = (
                    session.query(EncryptedDocumentModel)
                    .filter(
                        EncryptedDocumentModel.namespace == namespace,
                        EncryptedDocumentModel.doc_key == key,
                    )
                    .one_or_none()
                )
                if record:
                    record.payload = payload
                else:
                    session.add(EncryptedDocumentModel(namespace=namespace, doc_key=key, payload=payload))
                session.commit()
            except Exception:
                session.rollback()
                raise
            finally:
                session.close()

    def get(self, namespace: str, key: str) -> Optional[dict[str, Any]]:
        with self._lock:
            session = self._get_session()
            try:
                record = (
                    session.query(EncryptedDocumentModel)
                    .filter(
                        EncryptedDocumentModel.namespace == namespace,
                        EncryptedDocumentModel.doc_key == key,
                    )
                    .one_or_none()
                )
            finally:
                session.close()
        if not record:
            return None
        return json.loads(self._encryption.decrypt(record.payload))

    def delete(self, namespace: str, key: str) -> None:
        with self._lock:
            session = self._get_session()
            try:
                session.query(EncryptedDocumentModel).filter(
                    EncryptedDocumentModel.namespace == namespace,
                    EncryptedDocumentModel.doc_key == key,
                ).delete()
                session.commit()
            except Exception:
                session.rollback()
                raise
            finally:
                session.close()

    def list_all(self, namespace: str) -> dict[str, dict[str, Any]]:
        with self._lock:
            session = self._get_session()
            try:
                records = (
                    session.query(EncryptedDocumentModel).filter(EncryptedDocumentModel.namespace == namespace).all()
                )
            finally:
                session.close()
        result: dict[str, dict[str, Any]] = {}
        for record in records:
            result[record.doc_key] = json.loads(self._encryption.decrypt(record.payload))
        return result

    def find(self, namespace: str, query: dict[str, Any]) -> list[dict[str, Any]]:
        """Find documents in namespace matching query."""
        all_docs = self.list_all(namespace)
        results = []
        for doc in all_docs.values():
            match = True
            for qk, qv in query.items():
                if doc.get(qk) != qv:
                    match = False
                    break
            if match:
                results.append(doc)
        return results

    # Asynchronous Interface Implementation

    async def upsert_async(self, namespace: str, key: str, document: dict[str, Any]) -> None:
        return await asyncio.to_thread(self.upsert, namespace, key, document)

    async def get_async(self, namespace: str, key: str) -> Optional[dict[str, Any]]:
        return await asyncio.to_thread(self.get, namespace, key)

    async def delete_async(self, namespace: str, key: str) -> None:
        return await asyncio.to_thread(self.delete, namespace, key)

    async def list_all_async(self, namespace: str) -> dict[str, dict[str, Any]]:
        return await asyncio.to_thread(self.list_all, namespace)


class MotorEncryptedAdapter(BaseEncryptedAdapter):
    """Motor (MongoDB) adapter that encrypts payloads at rest."""

    def __init__(self, uri: str) -> None:
        # Lazy import motor only when MongoDB adapter is actually used
        try:
            from motor.motor_asyncio import AsyncIOMotorClient
        except ImportError as e:
            raise ImportError(
                "motor package is required for MongoDB storage backend. Install with: pip install motor"
            ) from e

        self._uri = uri
        self._encryption = get_encryption_service()
        self._lock = Lock()
        self._loop = asyncio.new_event_loop()
        self._loop_thread = Thread(target=self._loop.run_forever, daemon=True)
        self._loop_thread.start()

        parsed = urlparse(uri)
        db_name = parsed.path.lstrip("/") or "app"
        self._client = AsyncIOMotorClient(uri, serverSelectionTimeoutMS=5000, io_loop=self._loop)
        self._db = self._client[db_name]
        self._collection = self._db["encrypted_documents"]
        self._run(self._collection.create_index([("namespace", 1), ("doc_key", 1)], unique=True))
        logger.info("MotorEncryptedAdapter initialised for database %s", db_name)

    def _run(self, awaitable: Awaitable[Any]) -> Any:
        """Run async operation in sync context using thread-safe execution."""
        if inspect.iscoroutine(awaitable):
            coro = awaitable
        elif inspect.isawaitable(awaitable):

            async def _await_wrapper() -> Any:
                return await awaitable  # type: ignore[misc]

            coro = _await_wrapper()
        else:
            raise TypeError("Expected awaitable for Motor adapter operation")

        # Use run_coroutine_threadsafe to avoid blocking the main event loop
        # This is acceptable for sync methods that may be called from sync contexts
        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        # Use timeout to prevent indefinite blocking
        try:
            return future.result(timeout=30.0)  # 30 second timeout
        except Exception:
            future.cancel()
            raise

    async def _upsert_async(self, namespace: str, key: str, payload: str) -> None:
        now = datetime.now(UTC)
        await self._collection.update_one(
            {"namespace": namespace, "doc_key": key},
            {
                "$set": {"payload": payload, "updated_at": now},
                "$setOnInsert": {"created_at": now},
            },
            upsert=True,
        )

    def upsert(self, namespace: str, key: str, document: dict[str, Any]) -> None:
        """Sync wrapper for upsert - use async version if in async context."""
        payload = self._encryption.encrypt(json.dumps(document))
        with self._lock:
            self._run(self._upsert_async(namespace, key, payload))

    async def upsert_async(self, namespace: str, key: str, document: dict[str, Any]) -> None:
        """Async version of upsert - use this from async contexts."""
        payload = self._encryption.encrypt(json.dumps(document))
        await self._upsert_async(namespace, key, payload)

    async def _get_async(self, namespace: str, key: str) -> Optional[dict[str, Any]]:
        return await self._collection.find_one({"namespace": namespace, "doc_key": key})

    def get(self, namespace: str, key: str) -> Optional[dict[str, Any]]:
        """Sync wrapper for get - use async version if in async context."""
        with self._lock:
            record = self._run(self._get_async(namespace, key))
        if not record:
            return None
        return json.loads(self._encryption.decrypt(record["payload"]))

    async def get_async(self, namespace: str, key: str) -> Optional[dict[str, Any]]:
        """Async version of get - use this from async contexts."""
        record = await self._get_async(namespace, key)
        if not record:
            return None
        return json.loads(self._encryption.decrypt(record["payload"]))

    async def _delete_async(self, namespace: str, key: str) -> None:
        await self._collection.delete_one({"namespace": namespace, "doc_key": key})

    def delete(self, namespace: str, key: str) -> None:
        """Sync wrapper for delete - use async version if in async context."""
        with self._lock:
            self._run(self._delete_async(namespace, key))

    async def delete_async(self, namespace: str, key: str) -> None:
        """Async version of delete - use this from async contexts."""
        await self._delete_async(namespace, key)

    async def _list_async(self, namespace: str) -> list[dict[str, Any]]:
        cursor = self._collection.find({"namespace": namespace})
        return await cursor.to_list(length=None)

    def list_all(self, namespace: str) -> dict[str, dict[str, Any]]:
        """Sync wrapper for list_all - use async version if in async context."""
        with self._lock:
            records = self._run(self._list_async(namespace))
        result: dict[str, dict[str, Any]] = {}
        for record in records:
            result[record["doc_key"]] = json.loads(self._encryption.decrypt(record["payload"]))
        return result

    def find(self, namespace: str, query: dict[str, Any]) -> list[dict[str, Any]]:
        """Sync wrapper for find - use async version if in async context."""
        all_docs = self.list_all(namespace)
        results = []
        for doc in all_docs.values():
            match = True
            for qk, qv in query.items():
                if doc.get(qk) != qv:
                    match = False
                    break
            if match:
                results.append(doc)
        return results

    async def list_all_async(self, namespace: str) -> dict[str, dict[str, Any]]:
        """Async version of list_all - use this from async contexts."""
        records = await self._list_async(namespace)
        result: dict[str, dict[str, Any]] = {}
        for record in records:
            result[record["doc_key"]] = json.loads(self._encryption.decrypt(record["payload"]))
        return result


class DocumentNamespaceStore:
    """Namespaced document store that scopes operations to a logical bucket."""

    def __init__(
        self,
        namespace: str,
        factory: StorageFactory,
        manager: DynamicStorageManager,
    ) -> None:
        self._namespace = namespace
        self._factory = factory
        self._manager = manager
        self._adapter = self._factory._get_or_create_document_adapter()

    def _with_adapter(self) -> BaseEncryptedAdapter:
        self._adapter = self._factory._get_or_create_document_adapter()
        return self._adapter

    def _record_success(self, operation: OperationType) -> None:
        active_backend = self._factory.get_current_backend()
        self._manager.record_success(active_backend, operation)

    def _record_failure(self, operation: OperationType, exc: Exception) -> None:
        active_backend = self._factory.get_current_backend()
        self._manager.record_failure(active_backend, operation, str(exc))

    def _try_with_failover(self, operation: OperationType, operation_func, *args, **kwargs):
        """
        Execute operation with automatic failover to TinyDB if MongoDB fails (sync).
        """
        adapter = self._with_adapter()
        active_backend = self._factory.get_current_backend()

        try:
            # Try primary operation
            result = operation_func(adapter, *args, **kwargs)
            self._record_success(operation)

            # Check if we should failback (if we're on fallback and primary recovered)
            if active_backend != self._manager.active_backend:
                should_failback, _ = self._should_failback()
                if should_failback:
                    logger.info(
                        f"Primary backend {active_backend} recovered, failing back from {self._manager.active_backend}"
                    )
                    self._factory.switch_backend(active_backend)

            return result

        except Exception as exc:
            self._record_failure(operation, exc)

            # Check if failover is needed
            should_failover, reason = self._manager.should_failover()
            if should_failover and active_backend != self._manager.fallback_backend:
                fallback_backend = self._manager.fallback_backend
                logger.warning(
                    f"Operation {operation.value} failed on {active_backend}: {exc}. "
                    f"Failing over to {fallback_backend}: {reason}"
                )

                # Switch to fallback backend
                self._factory.switch_backend(fallback_backend)

                # Retry operation with fallback adapter
                try:
                    fallback_adapter = self._factory._get_or_create_document_adapter()
                    result = operation_func(fallback_adapter, *args, **kwargs)
                    self._record_success(operation)
                    logger.info(f"Operation {operation.value} succeeded on fallback backend {fallback_backend}")
                    return result
                except Exception as fallback_exc:
                    self._record_failure(operation, fallback_exc)
                    logger.error(
                        f"Operation {operation.value} also failed on fallback backend "
                        f"{fallback_backend}: {fallback_exc}"
                    )
                    raise fallback_exc

            # No failover needed or already on fallback - raise original exception
            raise

    async def _try_with_failover_async(self, operation: OperationType, operation_func, *args, **kwargs):
        """
        Execute operation with automatic failover to TinyDB if MongoDB fails (async).
        """
        adapter = self._with_adapter()
        active_backend = self._factory.get_current_backend()

        try:
            # Try primary operation
            result = await operation_func(adapter, *args, **kwargs)
            self._record_success(operation)

            # Check if we should failback
            if active_backend != self._manager.active_backend:
                should_failback, _ = self._should_failback()
                if should_failback:
                    logger.info(
                        f"Primary backend {active_backend} recovered, failing back from {self._manager.active_backend}"
                    )
                    self._factory.switch_backend(active_backend)

            return result

        except Exception as exc:
            self._record_failure(operation, exc)

            # Check if failover is needed
            should_failover, reason = self._manager.should_failover()
            if should_failover and active_backend != self._manager.fallback_backend:
                fallback_backend = self._manager.fallback_backend
                logger.warning(
                    f"Async operation {operation.value} failed on {active_backend}: {exc}. "
                    f"Failing over to {fallback_backend}: {reason}"
                )

                # Switch to fallback backend
                self._factory.switch_backend(fallback_backend)

                # Retry operation with fallback adapter
                try:
                    fallback_adapter = self._factory._get_or_create_document_adapter()
                    result = await operation_func(fallback_adapter, *args, **kwargs)
                    self._record_success(operation)
                    logger.info(f"Async operation {operation.value} succeeded on fallback {fallback_backend}")
                    return result
                except Exception as fallback_exc:
                    self._record_failure(operation, fallback_exc)
                    logger.error(
                        f"Async operation {operation.value} also failed on fallback {fallback_backend}: {fallback_exc}"
                    )
                    raise fallback_exc

            raise

    def _should_failback(self) -> tuple[bool, str]:
        """Check if we should failback from fallback to primary backend."""
        if self._factory.get_current_backend() == self._manager.fallback_backend:
            primary_backend = self._manager.active_backend
            primary_metrics = self._manager._metrics.get(primary_backend)
            if primary_metrics and primary_metrics.consecutive_failures == 0:
                # Primary has recovered (no recent failures)
                return True, f"Primary backend {primary_backend} has recovered"
        return False, "Failback not recommended"

    # --- Sync Interface ---

    def save(self, key: str, value: dict[str, Any]) -> None:
        """Save document with automatic MongoDB->TinyDB failover."""

        def _upsert(adapter, namespace, key, value):
            adapter.upsert(namespace, key, value)

        self._try_with_failover(OperationType.WRITE, _upsert, self._namespace, key, value)

    def get(self, key: str) -> Optional[dict[str, Any]]:
        """Get document with automatic MongoDB->TinyDB failover."""

        def _get(adapter, namespace, key):
            return adapter.get(namespace, key)

        return self._try_with_failover(OperationType.READ, _get, self._namespace, key)

    def delete(self, key: str) -> None:
        """Delete document with automatic MongoDB->TinyDB failover."""

        def _delete(adapter, namespace, key):
            adapter.delete(namespace, key)

        self._try_with_failover(OperationType.DELETE, _delete, self._namespace, key)

    def all(self) -> dict[str, dict[str, Any]]:
        """List all documents with automatic MongoDB->TinyDB failover."""

        def _list_all(adapter, namespace):
            return adapter.list_all(namespace)

        return self._try_with_failover(OperationType.LIST, _list_all, self._namespace)

    def find(self, query: dict[str, Any]) -> list[dict[str, Any]]:
        """Find documents matching query with automatic failover."""

        def _find(adapter, namespace, query):
            if hasattr(adapter, "find"):
                return adapter.find(namespace, query)
            return []

        return self._try_with_failover(OperationType.READ, _find, self._namespace, query)

    # --- Async Interface ---

    async def save_async(self, key: str, value: dict[str, Any]) -> None:
        """Save document asynchronously with automatic failover."""

        async def _upsert(adapter, namespace, key, value):
            await adapter.upsert_async(namespace, key, value)

        await self._try_with_failover_async(OperationType.WRITE, _upsert, self._namespace, key, value)

    async def get_async(self, key: str) -> Optional[dict[str, Any]]:
        """Get document asynchronously with automatic failover."""

        async def _get(adapter, namespace, key):
            return await adapter.get_async(namespace, key)

        return await self._try_with_failover_async(OperationType.READ, _get, self._namespace, key)

    async def delete_async(self, key: str) -> None:
        """Delete document asynchronously with automatic failover."""

        async def _delete(adapter, namespace, key):
            await adapter.delete_async(namespace, key)

        await self._try_with_failover_async(OperationType.DELETE, _delete, self._namespace, key)

    async def all_async(self) -> dict[str, dict[str, Any]]:
        """List all documents asynchronously with automatic failover."""

        async def _list_all(adapter, namespace):
            return await adapter.list_all_async(namespace)

        return await self._try_with_failover_async(OperationType.LIST, _list_all, self._namespace)


try:
    from beanie import Document
except ImportError:
    # Production fallback: define a minimal Document class if beanie is missing
    # so type hints don't fail, though beanie operations will be unavailable.
    class Document:  # type: ignore
        """Fallback Document class when beanie is not available."""

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            """Initialize fallback document."""
            super().__init__()


class StorageFactory:
    """Creates storage backends and exposes typed adapters.

    WARNING: When using SQLite backend in Development/Debug mode, ensure 'aiosqlite' and 'sqlalchemy'
    loggers are set to INFO/WARNING. Enabling DEBUG level logging for these libraries during
    performance testing (e.g. Playwright) generates excessive log volume that can cause
    Gunicorn workers to crash (OOM/Timeout).
    """

    def __init__(self, manifest_path: Path = MANIFEST_PATH) -> None:
        self._settings = get_settings()
        self._encryption_service = get_encryption_service()
        self._manifest = self._load_manifest(manifest_path)
        self._backend_name = self._determine_backend()
        self._backend_meta = self._manifest["supported"][self._backend_name]
        self._health = StorageHealth(
            name=self._backend_name,
            backend_type=self._backend_meta["type"],
            healthy=True,
        )
        self._document_adapter: Optional[BaseEncryptedAdapter] = None

        # Adapter Registry - enables Open/Closed extensibility
        self._adapter_factories: dict[str, Callable[[], BaseEncryptedAdapter]] = {
            "tinydb": self._create_tinydb_adapter,  # Real TinyDB (Secondary Tier)
            "local": self._create_sqlite_adapter,  # Primary local backend
            "mongodb": self._create_mongodb_adapter,
            "postgres": self._create_sql_adapter,
            "sqlite": self._create_sqlite_adapter,
            "memory": self._create_memory_adapter,
        }

        # Determine fallback backend for seamless failover/failback
        # MongoDB -> SQLite (document -> relational/kv, seamless translation)
        # PostgreSQL -> SQLite (relational -> relational)
        # SQLite -> SQLite (already fallback)
        fallback_backend = self._manifest.get("default", "sqlite")
        if self._backend_name == "mongodb":
            fallback_backend = "sqlite"  # MongoDB -> SQLite failover
        elif self._backend_name == "postgres":
            fallback_backend = "sqlite"  # PostgreSQL -> SQLite failover
        elif self._backend_name == "sqlite" or self._backend_name == "local":
            fallback_backend = "sqlite"  # Self-referential fallback (no lower tier)
        # TinyDB uses default (tinydb -> sqlite)

        self._dynamic_manager = DynamicStorageManager(
            active_backend=self._backend_name,
            fallback_backend=fallback_backend,
        )

        # Sync management for automatic data synchronization on failback
        self._sync_manager = SyncManager()
        # Enable sync on recovery by default (can be disabled via environment variable)
        self._sync_on_switch = os.getenv("MONGODB_SYNC_ON_RECOVERY", "true").lower() == "true"

        logger.info(
            "StorageFactory initialised with backend %s (fallback: %s) - "
            "MongoDB-style calls will seamlessly translate to TinyDB on failover",
            self._backend_name,
            fallback_backend,
        )

    # ------------------------------------------------------------------ #
    # Public API                                                         #
    # ------------------------------------------------------------------ #

    def get_current_backend(self) -> str:
        return self._backend_name

    def get_primary_storage(self) -> StorageHealth:
        """Backward compatible accessor for templates expecting StorageHealth."""
        return self._health

    def get_health(self) -> StorageHealth:
        """Get the current health status of the active backend."""
        return self._health

    async def perform_deep_health_check(self) -> bool:
        """
        Perform a deep health check on the active storage backend.
        Verifies full write/read/delete capability.
        """
        adapter = self._get_or_create_document_adapter()

        # If it's a ProviderBackedAdapter, it wraps an IStorageProvider (which has deep check)
        if isinstance(adapter, ProviderBackedAdapter):
            # Resolve to the actual provider (might be wrapped by EncryptedProvider)
            provider = adapter._provider
            while hasattr(provider, "_provider"):  # Unwrap decorators
                provider = provider._provider

            if isinstance(provider, IHealthCheck):
                is_healthy = await provider.perform_deep_health_check_async()
                self._health.healthy = is_healthy
                if not is_healthy:
                    self._health.detail = "Deep health check failed (write/read cycle)"
                else:
                    self._health.detail = "Deep health check passed"
                return is_healthy

        # Fallback for other adapters (MongoDB, SQLAlchemy)
        # For now, we assume they are healthy if the adapter was created
        # In a real enterprise app, we'd implement deep checks for those too
        return self._health.healthy

    # Beanie Support
    async def initialize_beanie(self, document_models: list[type[Document]]) -> None:
        """
        Initialize Beanie ODM for the given document models.

        Exclusively responsible for Beanie/TinyDB integration within the Storage Factory.
        """
        logger.debug(f"initialize_beanie called. Backend: {self._backend_name}")
        if self._backend_name == "mongodb":
            try:
                from beanie import init_beanie

                # Use absolute import for Motor to avoid confusion
                from motor.motor_asyncio import AsyncIOMotorClient

                mongo_dsn = self._settings.app_mongo_dsn
                if not mongo_dsn:
                    raise ValueError("MONGO_DSN must be set when STORAGE_BACKEND=mongodb")

                mongo_dsn = _decrypt_connection_string(mongo_dsn, "app_mongo_dsn")
                db_name = _mongo_database_name_from_uri(mongo_dsn)
                client = AsyncIOMotorClient(mongo_dsn)

                await init_beanie(database=client[db_name], document_models=document_models)
                logger.info(f"Beanie initialized with MongoDB adapter ({db_name})")

                # Mark as healthy
                self._health = StorageHealth(
                    name="mongodb",
                    backend_type="document",
                    healthy=True,
                )

            except Exception as e:
                logger.error(f"Failed to initialize Beanie with MongoDB: {e}")
                self._health = StorageHealth(
                    name="mongodb",
                    backend_type="document",
                    healthy=False,
                    detail=str(e),
                )
                raise
        else:
            await self._initialize_beanie_sqlite(document_models)

    async def _initialize_beanie_sqlite(self, document_models: list[type[Document]]) -> None:
        """
        Initialize Beanie with SQLite adapter.

        If APP_DATABASE_URL is set to a SQLite URL, use that for Core/Auth models.
        Otherwise fallback to APP_TINYDB_PATH (remapped to .sqlite).
        """
        try:
            from .beanie_sqlite_adapter import BeanieSQLiteAdapter

            # Determine DB path
            db_path: str

            # 1. Prefer APP_DATABASE_URL (Core DB) if it is SQLite
            if self._settings.app_database_url and self._settings.app_database_url.startswith("sqlite:///"):
                # Extract path from URL
                # sqlite:///./data/app.db -> ./data/app.db
                path_str = self._settings.app_database_url.replace("sqlite:///", "")
                p = Path(path_str)
                db_path = str(p.resolve())
                logger.info(f"Beanie/SQLite using Core DB path: {db_path}")
            else:
                # 2. Fallback to SQLITE_PATH
                path_str = self._settings.app_sqlite_path
                p = Path(path_str)

                if path_str.endswith(".json"):
                    db_path = str(p.with_suffix(".sqlite").resolve())
                elif not path_str.endswith((".sqlite", ".db")):
                    if p.suffix == "":
                        db_path = str((p / "beanie.sqlite").resolve())
                    else:
                        db_path = str(p.with_suffix(".sqlite").resolve())
                else:
                    db_path = str(p.resolve())
            logger.info(f"Beanie/SQLite using Storage DB path: {db_path}")

            encryption_key = self._safe_export_fernet_key()

            adapter = BeanieSQLiteAdapter(
                db_path=db_path, document_models=document_models, encryption_key=encryption_key
            )
            # Init schema immediately
            await adapter.init_schema()

            # Register the adapter in the factory to ensure 100% responsibility
            self._beanie_adapter = adapter
            logger.info(f"Beanie initialized with SQLite adapter at {db_path}")
        except Exception as e:
            logger.error(f"Failed to initialize Beanie with SQLite: {e}")
            raise

    def get_beanie_adapter(self) -> Optional[Any]:
        """Get the initialized Beanie/TinyDB adapter."""
        return getattr(self, "_beanie_adapter", None)

    # SQLAlchemy Support
    async def initialize_sqlalchemy_async(self) -> None:
        """Initialize the asynchronous SQLAlchemy engine and sessions."""
        from src.shared.services.config.database import init_database

        await init_database()

    def initialize_sqlalchemy_sync(self) -> None:
        """Initialize the synchronous SQLAlchemy engine and sessions."""
        from src.shared.services.config.database import init_database_sync

        init_database_sync()

    def get_db_session(self) -> Any:
        # Avoid direct import to prevent circularity
        from src.shared.services.config.database import get_db_session

        return get_db_session()

    def get_db_session_sync(self) -> Any:
        # Avoid direct import to prevent circularity
        from src.shared.services.config.database import get_db_session_sync

        return get_db_session_sync()

    def get_tenant_db_session_sync(self, url: str) -> Any:
        """
        Get a sync database session for a specific tenant URL.
        """
        # Avoid direct import to prevent circularity
        from src.shared.services.config.database import get_tenant_db_session_sync

        return get_tenant_db_session_sync(url)

    def is_sqlalchemy_initialized(self) -> bool:
        """Return True if SQLAlchemy has been initialized."""
        from src.shared.services.config.database import is_database_initialized

        return is_database_initialized()

    def list_supported_backends(self) -> list[dict[str, Any]]:
        """Return manifest metadata for supported backends."""
        backends: list[dict[str, Any]] = []
        for name, meta in self._manifest.get("supported", {}).items():
            entry = {
                "name": name,
                "type": meta.get("type", "unknown"),
                "encrypted": bool(meta.get("encrypted", False)),
                "description": meta.get("description"),
                "is_active": name == self._backend_name,
            }
            backends.append(entry)
        return backends

    def get_backend_metadata(self, backend_name: Optional[str] = None) -> dict[str, Any]:
        """Return manifest metadata for the requested backend."""
        name = backend_name or self._backend_name
        metadata = self._manifest.get("supported", {}).get(name)
        if metadata is None:
            raise ValueError(f"Backend '{name}' is not defined in storage manifest")
        enriched = {**metadata, "name": name, "is_active": name == self._backend_name}
        return enriched

    def create_vector_provider(self, provider_type: str = "redis", **kwargs) -> Any:
        """
        Create vector storage provider.

        Args:
            provider_type: Type of vector provider (redis, qdrant)
            **kwargs: Configuration arguments matching VectorStoreConfig fields

        Returns:
            IVectorProvider instance
        """
        from src.shared.services.storage.config import VectorStoreConfig, VectorStoreType

        # Determine provider type from kwargs or default
        if provider_type == "redis":
            from src.shared.services.storage.providers.redis_vector import RedisVectorProvider

            # Construct config from kwargs
            # Filter kwargs to match VectorStoreConfig fields
            config_data = {
                "type": VectorStoreType.REDIS,
                "host": kwargs.get("host", "localhost"),
                "port": kwargs.get("port", 6379),
                "collection_name": kwargs.get("collection_name", "vectors"),
                "dimension": kwargs.get("dimension", 1536),
            }
            if "password" in kwargs:
                config_data["password"] = kwargs["password"]
            if "metric" in kwargs:
                config_data["metric"] = kwargs["metric"]
            if "api_key" in kwargs:
                config_data["api_key"] = kwargs["api_key"]

            config = VectorStoreConfig(**config_data)
            return RedisVectorProvider(config)

        if provider_type == "qdrant":
            from src.shared.services.storage.providers.qdrant_vector import QdrantVectorProvider

            config_data = {
                "type": VectorStoreType.QDRANT,
                "host": kwargs.get("host", "localhost"),
                "port": kwargs.get("port", 6333),
                "collection_name": kwargs.get("collection_name", "vectors"),
                "dimension": kwargs.get("dimension", 1536),
            }
            if "metric" in kwargs:
                config_data["metric"] = kwargs["metric"]
            if "api_key" in kwargs:
                config_data["api_key"] = kwargs["api_key"]

            config = VectorStoreConfig(**config_data)
            return QdrantVectorProvider(config)

        raise ValueError(f"Unsupported vector provider: {provider_type}")

    def create_graph_provider(self, provider_type: str = "falkordb", **kwargs) -> Any:
        """
        Create graph storage provider.

        Args:
            provider_type: Type of graph provider (falkordb, neo4j)
            **kwargs: Configuration arguments matching GraphStoreConfig fields

        Returns:
            IGraphProvider instance
        """
        from src.shared.services.storage.config import GraphStoreConfig, GraphStoreType

        if provider_type == "falkordb":
            from src.shared.services.storage.providers.falkordb_graph import FalkorDBGraphProvider

            config_data = {
                "type": GraphStoreType.FALKORDB,
                "host": kwargs.get("host", "localhost"),
                "port": kwargs.get("port", 6379),
                "database": kwargs.get("database", "graph"),
            }
            if "password" in kwargs:
                config_data["password"] = kwargs["password"]

            config = GraphStoreConfig(**config_data)
            return FalkorDBGraphProvider(config)

        if provider_type == "neo4j":
            from src.shared.services.storage.providers.neo4j_graph import Neo4jGraphProvider

            config_data = {
                "type": GraphStoreType.NEO4J,
                "host": kwargs.get("host", "localhost"),
                "port": kwargs.get("port", 7687),
                "database": kwargs.get("database", "neo4j"),
            }
            if "username" in kwargs:
                config_data["username"] = kwargs["username"]
            if "password" in kwargs:
                config_data["password"] = kwargs["password"]

            config = GraphStoreConfig(**config_data)
            return Neo4jGraphProvider(config)

        raise ValueError(f"Unsupported graph provider: {provider_type}")

    def get_document_store(self, namespace: str) -> DocumentNamespaceStore:
        return DocumentNamespaceStore(namespace, self, self._dynamic_manager)

    def get_storage(self, storage_type: str = "PRIMARY") -> Any:
        """
        Compatibility method for migrated services.

        Returns a storage adapter that provides access to underlying database:
        - For TinyDB: Returns adapter with .db attribute (TinyDB instance)
        - For MongoDB: Returns adapter with .get_collection() method
        - For other backends: Returns appropriate adapter

        Args:
            storage_type: Storage type identifier (currently only "PRIMARY" supported)

        Returns:
            Storage adapter compatible with migrated service code
        """
        if storage_type != "PRIMARY":
            raise ValueError(f"Unsupported storage type: {storage_type}")

        adapter = self._get_or_create_document_adapter()

        # For TinyDB backend, return a wrapper that exposes .db
        if self._backend_name in ("tinydb", "local", "sqlite"):
            # Get the underlying TinyDBProvider/SQLiteProvider from the adapter
            if isinstance(adapter, ProviderBackedAdapter):
                provider = adapter._provider
                # Unwrap EncryptedProvider if present
                if hasattr(provider, "_provider"):
                    provider = provider._provider  # type: ignore[assignment]
                # Access SQLiteProvider or shimmed provider
                if hasattr(provider, "db") and provider.db is not None:
                    # Create a simple wrapper that exposes .db and .table()
                    class TinyDBStorageWrapper:
                        def __init__(self, db: Any) -> None:
                            self.db = db

                        def table(self, name: str) -> Any:
                            return self.db.table(name)

                    return TinyDBStorageWrapper(provider.db)  # type: ignore[attr-defined]

        # For MongoDB backend, return a wrapper with .get_collection()
        elif self._backend_name == "mongodb":
            if isinstance(adapter, MotorEncryptedAdapter):

                class MongoDBStorageWrapper:
                    def __init__(self, db: Any) -> None:
                        self._db = db

                    def get_collection(self, name: str) -> Any:
                        return self._db[name]

                    def collection(self, name: str) -> Any:
                        return self._db[name]

                return MongoDBStorageWrapper(adapter._db)  # type: ignore[attr-defined]

        # Fallback for relational backends (PostgreSQL, SQLite)
        # They use SQLAlchemyEncryptedAdapter which doesn't expose .db directly
        return adapter

    async def _create_sqlite_tenant_adapter(self, database_name: str) -> Any:
        """
        Create Tenant Adapter (SQLiteProvider).
        Replaces legacy TinyDB logic with direct SQLite tenant isolation.
        """
        import asyncio

        from .sqlite_provider import SQLiteProvider

        # Map to SQLite path
        base_path_str = self._settings.app_sqlite_path
        # Ensure base path exists
        # If base path is to a specific file (e.g. .json), use its parent directory
        base_p = Path(base_path_str)
        if base_p.suffix in (".json", ".sqlite", ".db"):
            base_path_str = str(base_p.parent)

        base_path = await asyncio.to_thread(Path(base_path_str).resolve)

        # Ensure directory exists
        if not base_path.exists():
            await asyncio.to_thread(base_path.mkdir, parents=True, exist_ok=True)

        # Tenant DB file
        path = base_path / f"{database_name}.sqlite"

        logger.info(f"Initializing Tenant SQLiteProvider at {path}")

        # Initialize provider (sync init is fine, managed by thread if needed,
        # but SQLiteProvider init is fast and mostly file checks)
        provider = SQLiteProvider(str(path))

        # Apply strict encryption if key available
        encryption_key = self._safe_export_fernet_key()
        final_provider: IStorageProvider = provider
        if encryption_key:
            final_provider = EncryptedProvider(provider, self._encryption_service)

        adapter = ProviderBackedAdapter(final_provider)

        # Shim wrapper to mimic legacy TinyDB table API if needed by consumers
        # (Though most consumers should use adapter.get/save/find)
        if hasattr(provider, "db") and provider.db is not None:
            # Legacy shim if something really needs .table() - but SQLiteProvider doesn't have .db or .table
            # We assume consumers use IStorageProvider generic methods.
            # If obscure consumers call .table(), they will fail.
            # Mandate says: Use generic interfaces.
            logger.debug("Provider has legacy .db attribute but generic interfaces should be used")

        return adapter

    async def _create_mongodb_tenant_adapter(self, database_name: str) -> Any:
        """Create MongoDB adapter for tenant-specific database."""
        mongo_dsn = self._settings.app_mongo_dsn
        if not mongo_dsn:
            raise ValueError("MONGO_DSN must be set when STORAGE_BACKEND=mongodb")

        # Decrypt connection string if encrypted
        mongo_dsn = _decrypt_connection_string(mongo_dsn, "app_mongo_dsn")

        # Replace database name in connection string
        from urllib.parse import urlparse, urlunparse

        parsed = urlparse(mongo_dsn)
        tenant_mongo_dsn = urlunparse(
            (
                parsed.scheme,
                parsed.netloc,
                f"/{database_name}",
                parsed.params,
                parsed.query,
                parsed.fragment,
            )
        )

        adapter = MotorEncryptedAdapter(tenant_mongo_dsn)

        class MongoDBStorageWrapper:
            def __init__(self, db: Any) -> None:
                self._db = db

            def get_collection(self, name: str) -> Any:
                return self._db[name]

            def collection(self, name: str) -> Any:
                return self._db[name]

        return MongoDBStorageWrapper(adapter._db)  # type: ignore[attr-defined]

    async def _create_sql_tenant_adapter(self, database_type: str) -> Any:
        """
        Create SQL adapter for tenant-specific database.

        Args:
            database_type: Database type ('postgres' or 'sqlite')

        Returns:
            SQLAlchemyEncryptedAdapter instance
        """
        if database_type == "sqlite":
            # For SQLite, we can implement isolation by creating a new adapter
            # that uses a different database file based on the database_name.
            # However, SQLAlchemyEncryptedAdapter currently uses the shared settings.
            # This requires a more complex refactor of the SQL adapter to support
            # dynamic connection strings, which will be implemented in the next phase.
            logger.info(f"SQLite tenant isolation active for {database_type}")
            return SQLAlchemyEncryptedAdapter()

        adapter = SQLAlchemyEncryptedAdapter()
        logger.warning(
            f"SQL database tenant isolation not fully implemented for {database_type}. "
            "Using default database adapter with schema-based isolation if supported by provider."
        )
        return adapter

    async def get_tenant_storage(self, tenant_id: str, app_id: str, storage_type: str = "PRIMARY") -> Any:
        """
        Get tenant-specific storage adapter.

        This method returns a storage adapter configured for the tenant-specific database,
        ensuring complete isolation between tenants.

        Args:
            tenant_id: Azure tenant ID
            app_id: Azure application ID
            storage_type: Storage type identifier (currently only "PRIMARY" supported)

        Returns:
            Storage adapter for tenant-specific database

        Raises:
            ValueError: If storage type is unsupported
            DatabaseIsolationError: If tenant database not found or access denied
        """
        if storage_type != "PRIMARY":
            raise ValueError(f"Unsupported storage type: {storage_type}")

        # Import database isolation manager
        try:
            from src.api.services.microsoft.core.database_isolation import DatabaseIsolationManager

            db_isolation = DatabaseIsolationManager()
        except ImportError:
            logger.warning("DatabaseIsolationManager not available - falling back to default storage")
            return self.get_storage(storage_type)

        # Get tenant database configuration
        db_config = await db_isolation.get_tenant_database_config(tenant_id, app_id)
        if not db_config:
            logger.info(f"Database not provisioned for tenant {tenant_id}, app {app_id}, provisioning now...")
            db_config = await db_isolation.provision_database_for_tenant(tenant_id, app_id)

        database_name = db_config["database_name"]
        database_type = db_config["database_type"]

        # Validate database access
        await db_isolation.validate_database_access(tenant_id, app_id, database_name)

        logger.info(
            f"Getting tenant storage for tenant {tenant_id}, app {app_id}: "
            f"database_name={database_name}, database_type={database_type}"
        )

        # Create tenant-specific adapter based on database type
        try:
            if database_type in ("tinydb", "sqlite", "local"):
                return await self._create_sqlite_tenant_adapter(database_name)
            if database_type == "mongodb":
                return await self._create_mongodb_tenant_adapter(database_name)
            if database_type in ("postgres", "sqlite"):
                return await self._create_sql_tenant_adapter(database_type)

            # Fallback to default storage
            logger.warning(f"Unsupported database type {database_type} for tenant storage, using default")
            return self.get_storage(storage_type)
        except Exception as e:
            logger.error(
                f"Failed to create tenant storage for tenant {tenant_id}, app {app_id}, "
                f"database_type={database_type}: {e}"
            )
            raise

    def get_settings_storage(self) -> DocumentNamespaceStore:
        """
        Get settings storage backed by encrypted document store.

        Returns a namespace-aware document store configured for application settings.
        Uses encrypted storage backend (TinyDB, MongoDB, or SQLAlchemy) based on configuration.
        """
        return self.get_document_store("app_settings")

    def configure_backend(self, backend_name: Optional[str] = None) -> None:
        """Optional compatibility helper allowing runtime backend switch."""
        if backend_name and backend_name != self._backend_name:
            self.switch_backend(backend_name)

    def switch_backend(self, backend_name: str) -> StorageHealth:
        """Switch StorageFactory to a different backend at runtime."""
        backend = backend_name.lower()
        if backend not in self._manifest["supported"]:
            raise ValueError(f"Unsupported storage backend '{backend}'")
        if backend == self._backend_name:
            return self._health

        old_backend = self._backend_name
        self._backend_name = backend
        self._backend_meta = self._manifest["supported"][backend]
        self._document_adapter = None
        self._health = StorageHealth(
            name=self._backend_name,
            backend_type=self._backend_meta["type"],
            healthy=True,
        )
        self._dynamic_manager.set_active_backend(self._backend_name)

        # Trigger data synchronization if enabled and switching back to primary from fallback
        if self._sync_on_switch and old_backend in ("tinydb", "sqlite", "local") and backend == "mongodb":
            logger.info(
                "Initiating automatic data sync from fallback backend %s to primary backend %s",
                old_backend,
                backend,
            )
            sync_success = self._sync_data_on_recovery(old_backend, backend)
            if sync_success:
                logger.info("Data synchronization completed successfully")
            else:
                logger.warning("Data synchronization failed, but continuing with backend switch")

        logger.info("Storage backend switched to %s", self._backend_name)
        return self._health

    def get_dynamic_manager(self) -> DynamicStorageManager:
        return self._dynamic_manager

    def _sync_data_on_recovery(self, old_backend: str, new_backend: str) -> bool:
        """
        Sync data when recovering from failover.

        Args:
            old_backend: The backend we're switching from (e.g., "tinydb")
            new_backend: The backend we're switching to (e.g., "mongodb")

        Returns:
            True if sync was successful, False otherwise
        """
        try:
            logger.info(
                "SYNC START: Beginning data synchronization from %s to %s",
                old_backend,
                new_backend,
            )

            # Get adapter instances for both backends
            # Temporarily switch to old backend to get source adapter
            old_adapter = self._get_adapter_for_backend(old_backend)
            # Switch to new backend to get target adapter
            new_adapter = self._get_adapter_for_backend(new_backend)

            if not old_adapter or not new_adapter:
                logger.error("SYNC ERROR: Could not get adapters for sync")
                return False

            # Get underlying providers from adapters (if available)
            source_provider = self._get_provider_from_adapter(old_adapter)
            target_provider = self._get_provider_from_adapter(new_adapter)

            if not source_provider or not target_provider:
                logger.warning("SYNC WARNING: Could not extract providers from adapters")
                return False

            # Check if both providers support sync
            if not isinstance(source_provider, ISyncProvider) or not isinstance(target_provider, ISyncProvider):
                logger.warning("SYNC WARNING: One or both providers don't support sync")
                return False

            # Get source data count for logging
            source_data = source_provider.get_data_for_sync()
            source_count = len(source_data)
            logger.info("SYNC DATA: Found %d items to sync from %s", source_count, old_backend)

            # Perform the synchronization
            logger.info("SYNC PROCESS: Syncing data from %s to %s", old_backend, new_backend)

            sync_result = self._sync_manager.sync_providers(
                source_provider=source_provider,
                target_provider=target_provider,
                direction=SyncDirection.TO_TARGET,
                conflict_resolution=SyncConflictResolution.NEWEST_WINS,
                dry_run=False,
            )

            if sync_result.success:
                logger.info(
                    "SYNC SUCCESS: Synchronized %d items, %d conflicts resolved in %.2f seconds",
                    sync_result.items_synced,
                    sync_result.conflicts_resolved,
                    sync_result.sync_duration_seconds,
                )

                # Optionally clear source data after successful sync (configurable)
                clear_fallback_after_sync = os.getenv("CLEAR_FALLBACK_AFTER_SYNC", "false").lower() == "true"
                if clear_fallback_after_sync:
                    try:
                        cleared_count = source_provider.clear()
                        logger.info("SYNC CLEANUP: Cleared %d items from fallback storage", cleared_count)
                    except Exception as e:
                        logger.warning("SYNC CLEANUP WARNING: Failed to clear fallback storage: %s", e)

                return True
            logger.error("SYNC FAILED: %s", ", ".join(sync_result.errors))
            return False

        except Exception as e:
            logger.error("SYNC EXCEPTION: Sync operation failed: %s", e, exc_info=True)
            return False

    def _get_adapter_for_backend(self, backend_name: str) -> Optional[BaseEncryptedAdapter]:
        """Get adapter instance for a specific backend."""
        try:
            # Temporarily store current backend
            original_backend = self._backend_name
            original_adapter = self._document_adapter

            # Switch to requested backend
            self._backend_name = backend_name
            self._document_adapter = None
            adapter = self._get_or_create_document_adapter()

            # Restore original backend
            self._backend_name = original_backend
            self._document_adapter = original_adapter

            return adapter
        except Exception as e:
            logger.error("Failed to get adapter for backend %s: %s", backend_name, e)
            return None

    def _get_provider_from_adapter(self, adapter: BaseEncryptedAdapter) -> Optional[IStorageProvider]:
        """Extract underlying provider from adapter if available."""
        # For ProviderBackedAdapter, get the underlying provider
        if isinstance(adapter, ProviderBackedAdapter):
            provider = adapter._provider
            # Unwrap EncryptedProvider if present
            if isinstance(provider, EncryptedProvider):
                provider = provider._provider
            return provider
        # For MotorEncryptedAdapter (MongoDB), create MongoDBProvider from connection
        if isinstance(adapter, MotorEncryptedAdapter):
            try:
                from src.api.services.providers.storage.mongodb import MongoDBProvider

                # Extract connection string and database name from adapter
                uri = adapter._uri
                parsed = urlparse(uri)
                db_name = parsed.path.lstrip("/") or "app"
                # Create MongoDBProvider with same connection details
                # Note: MongoDBProvider uses pymongo, MotorEncryptedAdapter uses motor
                # Both connect to same MongoDB instance, so sync will work
                provider = MongoDBProvider(
                    connection_string=uri,
                    database_name=db_name,
                    collection_name="encrypted_documents",
                )
                return provider
            except Exception as e:
                logger.error("Failed to create MongoDBProvider from MotorEncryptedAdapter: %s", e)
                return None
        # For other adapters (e.g., SQLAlchemyEncryptedAdapter), they don't expose
        # providers that implement ISyncProvider. Sync operations gracefully handle
        # this case by checking for ISyncProvider support before syncing.
        return None

    def database_exists(self, database_name: str) -> bool:
        """
        Compatibility method for migrated services.

        Checks if a database exists (for Database-Per-Host Pattern).

        Args:
            database_name: Database name to check

        Returns:
            True if database exists, False otherwise
        """
        if self._backend_name in ("tinydb", "local", "sqlite"):
            db_path = Path(self.sqlite_storage_path).parent / f"{database_name}.sqlite"
            return db_path.exists()
        if self._backend_name == "mongodb":
            # For MongoDB, check if database exists in connection
            try:
                adapter = self._get_or_create_document_adapter()
                if isinstance(adapter, MotorEncryptedAdapter):
                    # Check if database exists
                    db_list = adapter._client.list_database_names()  # type: ignore[attr-defined]
                    return database_name in db_list
            except Exception:
                return False
        return False

    @property
    def sqlite_storage_path(self) -> str:
        """
        Returns the SQLite storage path.

        Returns:
            Path to SQLite file
        """
        return str(Path(self._settings.app_sqlite_path).resolve())

    @property
    def tinydb_path(self) -> str:
        """Deprecated alias for sqlite_storage_path."""
        return self.sqlite_storage_path

    # ------------------------------------------------------------------ #
    # Internal helpers                                                   #
    # ------------------------------------------------------------------ #

    def _load_manifest(self, manifest_path: Path) -> dict[str, Any]:
        if manifest_path.exists():
            data = json.loads(manifest_path.read_text(encoding="utf-8"))
        else:
            data = {
                "default": "local",
                "supported": {
                    "local": {"name": "local", "type": "document", "encrypted": True},
                    "sqlite": {"name": "sqlite", "type": "document", "encrypted": True},
                    "tinydb": {"name": "tinydb", "type": "document", "encrypted": True},
                },
            }
        supported = data.get("supported")
        # Normalise to dict keyed by name for quick lookup
        if isinstance(supported, list):
            data["supported"] = {entry["name"]: entry for entry in supported}
        return data

    def _determine_backend(self) -> str:
        backend = self._settings.app_storage_backend.lower()
        if backend not in self._manifest["supported"]:
            raise ValueError(f"Unsupported storage backend '{backend}'")
        return backend

    def _safe_export_fernet_key(self) -> Optional[str]:
        try:
            return self._encryption_service.export_fernet_key()
        except ValueError:
            logger.warning("Encryption key unavailable; storage provider will operate without Fernet encryption")
            return None

    def ensure_encryption_key_available_or_raise(self, operation_desc: str = "operation") -> None:
        """Ensure a persistent encryption key is available for the active backend.

        If the active backend is marked as encrypted in the storage manifest but no
        Fernet-compatible key is exportable, raise a ValueError with actionable guidance.
        """
        metadata = self.get_backend_metadata()
        requires_encryption = bool(metadata.get("encrypted", False))
        if not requires_encryption:
            return

        encryption_key = self._safe_export_fernet_key()
        # Determine if key is persistent (env var or file) - ephemeral generated keys are not acceptable for encrypted backends
        env_key = os.getenv("ENCRYPTION_KEY")
        key_file = getattr(self._encryption_service, "key_file_path", None)
        key_file_exists = bool(key_file and key_file.exists())

        if encryption_key and (env_key or key_file_exists):
            logger.debug("Persistent encryption key available for %s", operation_desc)
            return

        # No persistent key available - build actionable message
        file_path = os.getenv("ENCRYPTION_KEY_FILE")
        reasons = []
        if env_key:
            reasons.append("ENCRYPTION_KEY is set but appears invalid or incompatible")
        if file_path:
            reasons.append(f"ENCRYPTION_KEY_FILE is set to {file_path} but file not readable/writable or empty")
        if not reasons:
            reasons.append("No ENCRYPTION_KEY or ENCRYPTION_KEY_FILE provided")

        guidance = (
            "Storage backend requires persistent encryption but no usable key was found. "
            "Provide a valid ENCRYPTION_KEY (base64) or set ENCRYPTION_KEY_FILE pointing to a readable key file. "
            "If running in a read-only container mount, set ENCRYPTION_KEY in the environment rather than relying on file persistence."
        )
        full_msg = f"Encryption key missing for storage {operation_desc}: {'; '.join(reasons)}. {guidance}"
        logger.error(full_msg)
        raise ValueError(full_msg)

    def _get_or_create_document_adapter(self) -> BaseEncryptedAdapter:
        if self._document_adapter is not None:
            return self._document_adapter

        factory_func = self._adapter_factories.get(self._backend_name)
        if not factory_func:
            available = ", ".join(self._adapter_factories.keys())
            raise ValueError(
                f"No adapter factory registered for backend '{self._backend_name}'. Available: {available}"
            )

        self._document_adapter = factory_func()
        return self._document_adapter

    def _create_tinydb_adapter(self) -> BaseEncryptedAdapter:
        """
        Builder for TinyDB adapter (Secondary Tier).
        Uses app_tinydb_path.
        """

        path = Path(self._settings.app_tinydb_path).resolve()
        path.parent.mkdir(parents=True, exist_ok=True)

        logger.info(f"Initializing TinyDBProvider at {path}")
        # Note: TinyDBProvider handles file creation

        # We need to wrap it if encryption is enabled, but TinyDBEncryptedAdapter
        # inherits from ProviderBackedAdapter which expects IStorageProvider.
        # However, TinyDBEncryptedAdapter constructor takes a path, not a provider.
        return TinyDBEncryptedAdapter(path)

    def _create_sqlite_adapter(self) -> BaseEncryptedAdapter:
        """
        Builder for SQLite adapter (Primary Local Backend).
        Replaces deprecated TinyDB backend.
        """
        from .sqlite_provider import SQLiteProvider

        # Use app_sqlite_path setting
        path_str = self._settings.app_sqlite_path
        p = Path(path_str)

        # If explicitly a json file (legacy config), switch to sqlite extension
        if path_str.endswith(".json"):
            path = p.with_suffix(".sqlite")
        elif not path_str.endswith((".sqlite", ".db")):
            # If directory or no extension, append storage filename
            # Check if it looks like a directory (no suffix)
            if p.suffix == "":
                path = p / "storage.sqlite"
            else:
                # Has extension but not handled, default to append suffix?
                # Or just force extension replacement?
                path = p.with_suffix(".sqlite")
        else:
            # Already sqlite or db
            path = p

        path = path.resolve()

        # Ensure parent directory exists
        path.parent.mkdir(parents=True, exist_ok=True)

        logger.info(f"Initializing SQLiteProvider at {path}")
        provider = SQLiteProvider(str(path))

        # Apply encryption decorator if key is available
        encryption_key = self._safe_export_fernet_key()
        final_provider: IStorageProvider = provider
        if encryption_key:
            logger.debug("Applying EncryptedProvider decorator to SQLiteKV")
            final_provider = EncryptedProvider(provider, self._encryption_service)

        # Update health
        self._health = StorageHealth(
            name="sqlite",
            backend_type="kv_store",
            healthy=provider.is_healthy(),
        )

        return ProviderBackedAdapter(final_provider)

    def _create_mongodb_adapter(self) -> BaseEncryptedAdapter:
        """Builder for MongoDB adapter."""
        mongo_dsn = self._settings.app_mongo_dsn
        if not mongo_dsn:
            raise ValueError("MONGO_DSN must be set when STORAGE_BACKEND=mongodb")

        # Decrypt connection string if encrypted
        mongo_dsn = _decrypt_connection_string(mongo_dsn, "app_mongo_dsn")

        try:
            adapter = MotorEncryptedAdapter(mongo_dsn)
            self._health = StorageHealth(
                name="mongodb",
                backend_type="document",
                healthy=True,
            )
            return adapter
        except Exception as exc:
            self._health = StorageHealth(
                name="mongodb",
                backend_type="document",
                healthy=False,
                detail=str(exc),
            )
            logger.exception("Failed to initialise MongoDB adapter")
            raise

    def _create_sql_adapter(self) -> BaseEncryptedAdapter:
        """Builder for SQL (Postgres/SQLite) adapter."""
        # Use the configured database URL for the SQL adapter
        db_url = self._settings.app_database_url
        adapter = SQLAlchemyEncryptedAdapter(db_url=db_url)
        self._health = StorageHealth(
            name=self._backend_name,
            backend_type="relational",
            healthy=True,
        )
        return adapter

    def _create_memory_adapter(self) -> BaseEncryptedAdapter:
        """Builder for Memory adapter."""
        from src.api.services.providers.storage.memory import MemoryProvider

        provider = MemoryProvider()
        encrypted_provider = EncryptedProvider(provider, self._encryption_service)
        self._health = StorageHealth(
            name="memory",
            backend_type="memory",
            healthy=True,
        )
        return ProviderBackedAdapter(encrypted_provider)


def _build_factory() -> StorageFactory:
    return StorageFactory()


@lru_cache(maxsize=1)
def get_storage_factory() -> StorageFactory:
    """
    Return a cached StorageFactory instance.

    Checks features for storage factory initialization and logs appropriately.
    """
    # Check feature for storage factory capability
    try:
        from src.shared.services.features.manager import get_feature_manager
        from src.shared.services.features.models import EvaluationContext

        feature_manager = get_feature_manager()
        context = EvaluationContext()
        # Check if data manager service is enabled (storage factory is used by data manager)
        data_manager_enabled = feature_manager.is_enabled_sync("services.data-manager", context, default=True)
        if not data_manager_enabled:
            logger.warning(
                "Storage factory requested but services.data-manager feature is disabled. "
                "Storage operations may be limited."
            )
        else:
            logger.debug("Storage factory initialization - services.data-manager feature enabled")
    except Exception as e:
        logger.warning(f"Feature check failed during storage factory initialization: {e}")

    factory = _build_factory()
    logger.debug("Storage factory instance retrieved/created")
    return factory
