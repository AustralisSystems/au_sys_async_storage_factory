"""
Core Asynchronous Storage Factory
Provides native AsyncSQLiteKV and AsyncSQLDB providers.
Enforces the Zero-Hardcode Mandate.
"""
import logging
import os
from typing import Optional

from storage.interfaces.storage import IStorageProvider
from storage.async_sqlite_provider import AsyncSQLiteProvider
from storage.async_sqldb_provider import AsyncSQLDBProvider

logger = logging.getLogger(__name__)


class AsyncStorageFactory:
    """
    Factory for instantiating the core asynchronous storage providers
    (SQLite KV and SQL DB) according to the Zero-Hardcode Mandate.
    """

    def __init__(self):
        # Determine the default provider from environment
        self._provider_type = os.getenv("ASYNC_STORAGE_PROVIDER", "sqlite_kv").lower()
        self._provider: Optional[IStorageProvider] = None

    async def get_provider(self, namespace: str = "default") -> IStorageProvider:
        """
        Retrieves the configured asynchronous storage provider.
        Instantiates it on the first call.

        Args:
            namespace: The isolation namespace/database name (chiefly used by sqlite_kv).

        Returns:
            An instance implementing IStorageProvider natively asynchronously.
        """
        if self._provider is None:
            self._provider = await self._create_provider(namespace)
        return self._provider

    async def _create_provider(self, namespace: str) -> IStorageProvider:
        if self._provider_type == "sqlite_kv":
            return await self._create_sqlite_kv_provider(namespace)
        elif self._provider_type == "sql_db":
            return await self._create_sql_db_provider()
        else:
            raise ValueError(f"Unknown ASYNC_STORAGE_PROVIDER: {self._provider_type}")

    async def _create_sqlite_kv_provider(self, namespace: str) -> IStorageProvider:
        # Zero hardcode mandate: connection path via env var
        base_path = os.getenv("ASYNC_SQLITE_KV_PATH", "./data/async_storage")
        os.makedirs(base_path, exist_ok=True)
        db_path = os.path.join(base_path, f"{namespace}.sqlite")

        logger.info(f"Initializing AsyncSQLiteProvider at {db_path}")
        provider = AsyncSQLiteProvider(db_path=db_path, identifier=namespace)
        return provider

    async def _create_sql_db_provider(self) -> IStorageProvider:
        # Zero hardcode mandate: connection url via env var
        # Must be an async dialect e.g. postgresql+asyncpg:// or sqlite+aiosqlite:///
        connection_url = os.getenv("ASYNC_SQLDB_URL")
        if not connection_url:
            raise ValueError(
                "Environment variable ASYNC_SQLDB_URL is required for sql_db provider. "
                "Ensure it uses an async dialect, e.g., sqlite+aiosqlite:/// or postgresql+asyncpg://"
            )

        logger.info("Initializing AsyncSQLDBProvider")
        provider = AsyncSQLDBProvider(connection_url=connection_url)
        # Initialize the database layout asynchronously
        await provider.initialize()
        return provider

    def get_factory_health(self) -> dict[str, bool]:
        """
        Returns rudimentary health metadata for the factory itself.
        """
        return {
            "factory_initialized": True,
            "provider_type_configured": self._provider_type,
            "provider_active": self._provider is not None,
        }


# Singleton instance
_factory_instance = AsyncStorageFactory()


def get_storage_factory() -> AsyncStorageFactory:
    """
    Returns the singleton AsyncStorageFactory instance.
    """
    return _factory_instance
