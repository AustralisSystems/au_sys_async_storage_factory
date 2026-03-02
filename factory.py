"""
Core Asynchronous Storage Factory
Provides native AsyncSQLiteKV, AsyncSQLDB, and AsyncPostgres providers.
Enforces the Zero-Hardcode Mandate.
"""

import logging
import os
from typing import Optional, Dict, Any
from datetime import datetime, UTC

from storage.interfaces.storage import IStorageProvider, StorageError
from storage.async_sqlite_provider import AsyncSQLiteProvider
from storage.async_sqldb_provider import AsyncSQLDBProvider
from storage.async_postgres_provider import AsyncPostgresProvider
from storage.admin import StorageAdminPortal

logger = logging.getLogger(__name__)


class AsyncStorageFactory:
    """
    Factory for instantiating core asynchronous storage providers.
    Enforces the Zero-Hardcode Mandate.
    """

    def __init__(self):
        self._provider_type = os.getenv("ASYNC_STORAGE_PROVIDER", "sqlite_kv").lower()
        self._providers: Dict[str, IStorageProvider] = {}
        self._admin_portal: Optional[StorageAdminPortal] = None

    async def get_provider(self, namespace: str = "default") -> IStorageProvider:
        """Retrieves or creates a provider for the given namespace."""
        if namespace not in self._providers:
            self._providers[namespace] = await self._create_provider(namespace)
        return self._providers[namespace]

    async def _create_provider(self, namespace: str) -> IStorageProvider:
        if self._provider_type == "sqlite_kv":
            return await self._create_sqlite_kv_provider(namespace)
        elif self._provider_type == "sql_db":
            return await self._create_sql_db_provider()
        elif self._provider_type == "postgres":
            return await self._create_postgres_provider()
        else:
            raise ValueError(f"Unknown ASYNC_STORAGE_PROVIDER: {self._provider_type}")

    async def _create_sqlite_kv_provider(self, namespace: str) -> IStorageProvider:
        base_path = os.getenv("ASYNC_SQLITE_KV_PATH", "./data/async_storage")
        os.makedirs(base_path, exist_ok=True)
        db_path = os.path.join(base_path, f"{namespace}.sqlite")
        provider = AsyncSQLiteProvider(db_path=db_path)
        await provider.initialize()
        return provider

    async def _create_postgres_provider(self) -> IStorageProvider:
        url = os.getenv("ASYNC_POSTGRES_URL")
        if not url:
            raise ValueError("ASYNC_POSTGRES_URL environment variable required.")
        provider = AsyncPostgresProvider(connection_url=url)
        await provider.initialize()
        return provider

    async def _create_sql_db_provider(self) -> IStorageProvider:
        connection_url = os.getenv("ASYNC_SQLDB_URL")
        if not connection_url:
            raise ValueError("Environment variable ASYNC_SQLDB_URL is required.")
        provider = AsyncSQLDBProvider(connection_url=connection_url)
        await provider.initialize()
        return provider

    def get_admin_portal(self) -> StorageAdminPortal:
        """Returns the Storage Admin Portal for lifecycle and health management."""
        if self._admin_portal is None:
            self._admin_portal = StorageAdminPortal(self._providers)
        return self._admin_portal

    async def create_snapshot(self, namespace: str = "default") -> str:
        """Creates a point-in-time snapshot/backup of the provider data."""
        provider = await self.get_provider(namespace)
        if hasattr(provider, "create_backup"):
            backup_dir = os.getenv("STORAGE_BACKUP_DIR", "./data/backups")
            os.makedirs(backup_dir, exist_ok=True)
            timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
            path = os.path.join(backup_dir, f"{namespace}_{timestamp}.bak")
            success = await provider.create_backup(path)
            if success:
                return path
        raise StorageError(f"Backup not supported for provider in namespace {namespace}")

    def get_db_session(self):
        """
        Bridge to the underlying transactional database session.
        """
        from storage.shared.services.config.database import get_db_session

        return get_db_session()

    def get_db_session_sync(self):
        """
        Bridge to the underlying transactional database session (sync).
        """
        from storage.shared.services.config.database import get_db_session_sync

        return get_db_session_sync()

    def get_factory_health(self) -> dict[str, Any]:
        """
        Returns health metadata for the factory itself.
        """
        return {
            "factory_initialized": True,
            "provider_type_configured": self._provider_type,
            "providers_active": list(self._providers.keys()),
        }


# Singleton instance
_factory_instance = AsyncStorageFactory()


def get_storage_factory() -> AsyncStorageFactory:
    """
    Returns the singleton AsyncStorageFactory instance.
    """
    return _factory_instance
