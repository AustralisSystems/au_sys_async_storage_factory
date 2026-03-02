from __future__ import annotations

"""
Core Asynchronous Storage Factory.

Dynamically instantiates storage providers based on manifest-driven 
configuration. Enforces lazy-loading, Zero-Hardcode Mandate, and 
cross-category failover/tiering.
"""

import logging
import json
import asyncio
from pathlib import Path
from typing import Optional, Any, cast
from datetime import datetime, UTC

from .config import get_storage_settings
from .models import StorageBackendConfig, StorageManifest, StorageBackendType
from .interfaces.storage import IStorageProvider, StorageError
from .interfaces.base_document_provider import IDocumentProvider
from .interfaces.base_vector_provider import IVectorProvider
from .interfaces.base_graph_provider import IGraphProvider
from .interfaces.base_blob_provider import BaseBlobProvider

logger = logging.getLogger(__name__)


class AsyncStorageFactory:
    """
    Universal Factory for the Storage Factory sub-module.
    Handles lifecycle, dynamic loading, and multi-tier orchestration.
    """

    def __init__(self, manifest_path: Optional[Path] = None):
        self._settings = get_storage_settings()
        self._manifest_path = manifest_path or Path(self._settings.storage_manifest_path)
        self._manifest: StorageManifest = self._load_manifest()

        # Instance caches
        self._instances: dict[str, Any] = {}
        self._lock = asyncio.Lock()

        logger.info(f"AsyncStorageFactory initialized with manifest: {self._manifest_path}")

    def _load_manifest(self) -> StorageManifest:
        """Loads and validates the storage manifest."""
        if not self._manifest_path.exists():
            logger.warning(f"Manifest not found at {self._manifest_path}, using environment defaults.")
            return self._build_manifest_from_env()

        try:
            with open(self._manifest_path) as f:
                data = json.load(f)
                return StorageManifest(**data)
        except Exception as e:
            logger.error(f"Failed to parse storage manifest: {e}")
            return self._build_manifest_from_env()

    def _build_manifest_from_env(self) -> StorageManifest:
        """Fallback: builds a manifest object from environment variables."""
        backends = {}

        # SQLite Default
        backends["sqlite"] = StorageBackendConfig(
            name="sqlite",
            type=StorageBackendType.RELATIONAL,
            provider="sqlite",
            connection_url=self._settings.async_sqlite_path,
        )

        # MongoDB Default
        backends["mongodb"] = StorageBackendConfig(
            name="mongodb",
            type=StorageBackendType.DOCUMENT,
            provider="mongodb",
            connection_url=self._settings.mongodb_uri,
            database=self._settings.mongodb_db,
            failover_enabled=True,
            secondary_backend="sqlite",
        )

        # Local Blob Default
        backends["local"] = StorageBackendConfig(
            name="local",
            type=StorageBackendType.BLOB,
            provider="local",
            options={"base_dir": self._settings.local_blob_path},
        )

        # Redis Vector Default
        backends["redis"] = StorageBackendConfig(
            name="redis",
            type=StorageBackendType.VECTOR,
            provider="redis",
            host=self._settings.redis_host,
            port=self._settings.redis_port,
        )

        # FalkorDB Graph Default
        backends["falkordb"] = StorageBackendConfig(
            name="falkordb",
            type=StorageBackendType.GRAPH,
            provider="falkordb",
            host=self._settings.falkordb_host,
            port=self._settings.falkordb_port,
        )

        return StorageManifest(backends=backends)

    # --- Provider Accessors ---

    async def get_relational(self, name: Optional[str] = None) -> IStorageProvider:
        name = name or self._manifest.default_relational
        return cast(IStorageProvider, await self._get_or_create_instance(name))

    async def get_document(self, name: Optional[str] = None) -> IDocumentProvider:
        name = name or self._manifest.default_document
        return cast(IDocumentProvider, await self._get_or_create_instance(name))

    async def get_blob(self, name: Optional[str] = None) -> BaseBlobProvider:
        name = name or self._manifest.default_blob
        return cast(BaseBlobProvider, await self._get_or_create_instance(name))

    async def get_vector(self, name: Optional[str] = None) -> IVectorProvider:
        name = name or self._manifest.default_vector
        return cast(IVectorProvider, await self._get_or_create_instance(name))

    async def get_graph(self, name: Optional[str] = None) -> IGraphProvider:
        name = name or self._manifest.default_graph
        return cast(IGraphProvider, await self._get_or_create_instance(name))

    async def get_transient(self, name: str = "memory") -> IStorageProvider:
        return cast(IStorageProvider, await self._get_or_create_instance(name))

    # --- Internal Orchestration ---

    async def _get_or_create_instance(self, name: str) -> Any:
        async with self._lock:
            if name in self._instances:
                return self._instances[name]

            config = self._manifest.backends.get(name)
            if not config:
                if name == "memory":
                    config = StorageBackendConfig(name="memory", type=StorageBackendType.TRANSIENT, provider="memory")
                else:
                    raise StorageError(f"Backend '{name}' not defined in manifest.")

            instance = await self._instantiate_provider(config)

            # Wrap in failover if enabled
            if config.failover_enabled and config.secondary_backend:
                secondary = await self._get_or_create_instance_internal(config.secondary_backend)
                instance = await self._wrap_failover(instance, secondary, config.type)

            self._instances[name] = instance
            return instance

    async def _get_or_create_instance_internal(self, name: str) -> Any:
        """Internal version without lock for recursion."""
        if name in self._instances:
            return self._instances[name]

        config = self._manifest.backends.get(name)
        if not config:
            raise StorageError(f"Backend '{name}' not defined in manifest.")

        instance = await self._instantiate_provider(config)
        self._instances[name] = instance
        return instance

    async def _instantiate_provider(self, config: StorageBackendConfig) -> Any:
        """Dynamic/Lazy loading of provider classes."""
        provider = config.provider.lower()

        if provider == "sqlite":
            from .async_sqlite_provider import AsyncSQLiteProvider

            conn_url = str(config.connection_url or self._settings.async_sqlite_path)
            # Strip the driver prefix to get the filesystem path
            # Handles: sqlite+aiosqlite:///absolute  sqlite+aiosqlite://./relative
            db_path = conn_url.split("///", 1)[-1] if "///" in conn_url else conn_url
            rel_instance = AsyncSQLiteProvider(db_path=db_path)
            await rel_instance.initialize()
            return rel_instance

        if provider == "mongodb":
            from .providers.async_mongodb_provider import AsyncMongoDBProvider

            uri = config.connection_url or self._settings.mongodb_uri
            db_name = config.database or self._settings.mongodb_db
            doc_instance = AsyncMongoDBProvider(connection_uri=uri, database_name=db_name)
            return doc_instance

        if provider == "postgres":
            from .async_postgres_provider import AsyncPostgresProvider

            url = config.connection_url or str(self._settings.async_postgres_url)
            pg_instance = AsyncPostgresProvider(connection_url=url)
            await pg_instance.initialize()
            return pg_instance

        if provider == "local":
            from .providers.local_blob import LocalBlobProvider

            base_dir = str(config.options.get("base_dir", self._settings.local_blob_path))
            return LocalBlobProvider(base_dir=base_dir)

        if provider == "s3":
            from .providers.aws_s3 import S3BlobProvider

            bucket = str(config.options.get("bucket", self._settings.s3_bucket))
            region = str(config.options.get("region", self._settings.aws_region))
            return S3BlobProvider(bucket_name=bucket, region_name=region)

        if provider == "azure":
            from .providers.azure_blob import AzureBlobProvider

            connection_string = str(
                config.options.get("connection_string", self._settings.azure_connection_string or "")
            )
            container_name = str(config.options.get("container_name", self._settings.azure_container_name))
            return AzureBlobProvider(connection_string=connection_string, container_name=container_name)

        if provider == "gcp":
            from .providers.gcp_storage import GCPBlobProvider

            bucket_name = str(config.options.get("bucket_name", self._settings.gcp_bucket_name))
            project_id = config.options.get("project_id", self._settings.gcp_project_id)
            credentials_json = config.options.get("credentials_json", self._settings.gcp_credentials_json)
            return GCPBlobProvider(
                bucket_name=bucket_name,
                project_id=project_id,
                credentials_json=credentials_json,
            )

        if provider == "redis":
            from .providers.redis_vector import RedisVectorProvider

            host = str(config.host or self._settings.redis_host)
            port = int(config.port or self._settings.redis_port)
            instance = RedisVectorProvider(host=host, port=port)
            await instance.initialize()
            return instance

        if provider == "qdrant":
            from .providers.qdrant_vector import QdrantVectorProvider

            host = str(config.host or self._settings.qdrant_host)
            qdrant_instance = QdrantVectorProvider(host=host)
            await qdrant_instance.initialize()
            return qdrant_instance

        if provider == "falkordb":
            from .providers.falkordb_graph import FalkorDBGraphProvider

            host = str(config.host or self._settings.falkordb_host)
            port = int(config.port or self._settings.falkordb_port)
            falkordb_instance = FalkorDBGraphProvider(host=host, port=port)
            await falkordb_instance.initialize()
            return falkordb_instance

        if provider == "neo4j":
            from .providers.neo4j_graph import Neo4jGraphProvider

            uri = str(config.connection_url or self._settings.neo4j_uri)
            neo4j_instance = Neo4jGraphProvider(uri=uri)
            await neo4j_instance.initialize()
            return neo4j_instance

        if provider == "memory":
            from .async_memory_provider import AsyncMemoryProvider

            return AsyncMemoryProvider()

        raise StorageError(f"Unsupported provider: {provider}")

    async def _wrap_failover(self, primary: Any, secondary: Any, b_type: StorageBackendType) -> Any:
        """Wraps providers in their respective failover orchestrators."""
        if b_type == StorageBackendType.RELATIONAL:
            from .relational_failover_provider import RelationalFailoverProvider

            return RelationalFailoverProvider(primary=primary, secondary=secondary)

        if b_type == StorageBackendType.DOCUMENT:
            from .document_failover_provider import DocumentFailoverProvider

            return DocumentFailoverProvider(primary=primary, secondary=secondary)

        if b_type == StorageBackendType.BLOB:
            from .providers.blob_failover_provider import BlobFailoverProvider

            return BlobFailoverProvider(primary=primary, secondary=secondary)

        if b_type == StorageBackendType.VECTOR:
            from .providers.vector_failover_provider import VectorFailoverProvider

            return VectorFailoverProvider(primary=primary, secondary=secondary)

        if b_type == StorageBackendType.GRAPH:
            from .providers.graph_failover_provider import GraphFailoverProvider

            return GraphFailoverProvider(primary=primary, secondary=secondary)

        return primary

    def get_factory_health(self) -> dict[str, Any]:
        return {
            "status": "active",
            "manifest_path": str(self._manifest_path),
            "active_instances": list(self._instances.keys()),
            "timestamp": datetime.now(UTC).isoformat(),
        }


# Singleton accessor
_factory_instance: Optional[AsyncStorageFactory] = None


def get_storage_factory() -> AsyncStorageFactory:
    global _factory_instance
    if _factory_instance is None:
        _factory_instance = AsyncStorageFactory()
    return _factory_instance
