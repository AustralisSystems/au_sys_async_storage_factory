
from src.shared.observability.logger_factory import get_logger
import json
from typing import Optional, Any, Union
from pydantic import SecretStr

from src.shared.services.storage.factory import get_storage_factory
from src.shared.security.encryption import get_encryption_helper
from src.api.services.storage.schemas.blob_connection_schema import (
    BlobConnectionCreate,
    BlobConnectionUpdate,
    BlobProviderType,
    S3ConnectionConfig,
    AzureConnectionConfig,
    GCPConnectionConfig,
    HttpConnectionConfig,
    LocalConnectionConfig,
    StorageValidationResult,
    HealthCheckResult
)

logger = get_logger(__name__)


class ConnectionRegistryService:
    """
    Service to manage blob storage provider connections.
    Handles encryption of sensitive fields at rest using DocumentNamespaceStore.
    """

    def __init__(self):
        # Use a namespaced document store for blob connections
        self.store = get_storage_factory().get_document_store("blob_registry")
        self.encryption = get_encryption_helper()

    def _get_secret_value(self, val: Union[str, SecretStr, None]) -> Optional[str]:
        """Helper to get string value from SecretStr or str."""
        if val is None:
            return None
        if isinstance(val, SecretStr):
            return val.get_secret_value()
        return str(val)

    def _encrypt_config(self, config_dict: dict[str, Any]) -> dict[str, Any]:
        """Encrypt sensitive fields in the config dictionary."""
        encrypted = config_dict.copy()

        # S3
        if "access_key" in encrypted and encrypted["access_key"]:
            val = self._get_secret_value(encrypted["access_key"])
            if val:
                encrypted["access_key"] = self.encryption.encrypt(val)
        if "secret_key" in encrypted and encrypted["secret_key"]:
            val = self._get_secret_value(encrypted["secret_key"])
            if val:
                encrypted["secret_key"] = self.encryption.encrypt(val)

        # Azure
        if "connection_string" in encrypted and encrypted["connection_string"]:
            val = self._get_secret_value(encrypted["connection_string"])
            if val:
                encrypted["connection_string"] = self.encryption.encrypt(val)
        if "account_key" in encrypted and encrypted["account_key"]:
            val = self._get_secret_value(encrypted["account_key"])
            if val:
                encrypted["account_key"] = self.encryption.encrypt(val)

        # GCP
        if "credentials_json" in encrypted and encrypted["credentials_json"]:
            val = self._get_secret_value(encrypted["credentials_json"])
            if val:
                encrypted["credentials_json"] = self.encryption.encrypt(val)

        # HTTP
        if "encryption_key" in encrypted and encrypted["encryption_key"]:
            val = self._get_secret_value(encrypted["encryption_key"])
            if val:
                encrypted["encryption_key"] = self.encryption.encrypt(val)
        if "auth_header" in encrypted and encrypted["auth_header"]:
            files_dict = encrypted["auth_header"]
            for k, v in files_dict.items():
                val = self._get_secret_value(v)
                if val:
                    files_dict[k] = self.encryption.encrypt(val)
            encrypted["auth_header"] = files_dict

        return encrypted

    def _decrypt_config(self, config_dict: dict[str, Any]) -> dict[str, Any]:
        """Decrypt sensitive fields in the config dictionary."""
        decrypted = config_dict.copy()

        try:
            # S3
            if "access_key" in decrypted and decrypted["access_key"]:
                decrypted["access_key"] = self.encryption.decrypt(decrypted["access_key"])
            if "secret_key" in decrypted and decrypted["secret_key"]:
                decrypted["secret_key"] = self.encryption.decrypt(decrypted["secret_key"])

            # Azure
            if "connection_string" in decrypted and decrypted["connection_string"]:
                decrypted["connection_string"] = self.encryption.decrypt(decrypted["connection_string"])
            if "account_key" in decrypted and decrypted["account_key"]:
                decrypted["account_key"] = self.encryption.decrypt(decrypted["account_key"])

            # GCP
            if "credentials_json" in decrypted and decrypted["credentials_json"]:
                decrypted["credentials_json"] = self.encryption.decrypt(decrypted["credentials_json"])

            # HTTP
            if "encryption_key" in decrypted and decrypted["encryption_key"]:
                decrypted["encryption_key"] = self.encryption.decrypt(decrypted["encryption_key"])
            if "auth_header" in decrypted and decrypted["auth_header"]:
                files_dict = decrypted["auth_header"]
                for k, v in files_dict.items():
                    files_dict[k] = self.encryption.decrypt(v)
                decrypted["auth_header"] = files_dict

        except Exception as e:
            logger.error(f"Failed to decrypt connection config: {e}")

        return decrypted

    async def register_connection(self, config: BlobConnectionCreate) -> BlobConnectionCreate:
        """Register a new connection configuration."""
        # Convert SecretStr to string for storage
        # Convert SecretStr to string for storage but keep original values first
        # Use mode='python' (default) to get SecretStr objects, allowing _encrypt_config to extract values
        config_dict = config.model_dump()

        # Encrypt sensitive fields
        encrypted_config = self._encrypt_config(config_dict)

        # Persist using DocumentNamespaceStore
        await self.store.save_async(config.connection_id, encrypted_config)

        # Invalidate cache in factory
        from src.shared.services.storage.blob_factory import BlobStorageFactory
        BlobStorageFactory.invalidate_connection_provider(config.connection_id)

        logger.info(f"Registered new blob connection: {config.connection_id} ({config.name})")
        return config

    async def get_connection(self, connection_id: str) -> Optional[BlobConnectionCreate]:
        """Retrieve and decrypt a connection configuration."""
        data = await self.store.get_async(connection_id)

        if not data:
            return None

        if isinstance(data, str):
            try:
                data = json.loads(data)
            except json.JSONDecodeError:
                logger.error(f"Failed to decode connection config for {connection_id}")
                return None

        # Decrypt
        decrypted_data = self._decrypt_config(data)

        return self._create_model_from_dict(decrypted_data)

    async def list_connections(self, user_id: Optional[str] = None, active_only: bool = False) -> list[BlobConnectionCreate]:
        """List all connections, optionally filtering by user or status."""
        # Get all documents from the store
        all_docs = await self.store.all_async()
        connections = []

        for conn_id, data in all_docs.items():
            # Process each document
            if isinstance(data, str):
                try:
                    data = json.loads(data)
                except json.JSONDecodeError:
                    logger.warning(f"Skipping invalid JSON for connection {conn_id}")
                    continue

            decrypted_data = self._decrypt_config(data)

            try:
                conn = self._create_model_from_dict(decrypted_data)

                if conn:
                    if user_id and conn.owner_user_id != user_id:
                        continue
                    if active_only and not conn.is_active:
                        continue
                    connections.append(conn)
            except Exception as e:
                logger.warning(f"Skipping invalid connection {conn_id}: {e}")
                continue

        return connections

    def _create_model_from_dict(self, data: dict[str, Any]) -> Optional[BlobConnectionCreate]:
        """Helper to create appropriate Pydantic model from dict."""
        provider_type = data.get("provider_type")

        if provider_type == BlobProviderType.AWS_S3:
            return S3ConnectionConfig(**data)
        if provider_type == BlobProviderType.AZURE_BLOB:
            return AzureConnectionConfig(**data)
        if provider_type == BlobProviderType.GCP_STORAGE:
            return GCPConnectionConfig(**data)
        if provider_type == BlobProviderType.HTTP_GENERIC:
            return HttpConnectionConfig(**data)
        if provider_type == BlobProviderType.LOCAL:
            return LocalConnectionConfig(**data)
        logger.error(f"Unknown provider type: {provider_type}")
        return None

    async def update_connection(self, connection_id: str, updates: BlobConnectionUpdate) -> Optional[BlobConnectionCreate]:
        """Update an existing connection."""
        existing = await self.get_connection(connection_id)
        if not existing:
            return None

        # Ensure connection_id matches
        updates.connection_id = connection_id

        return await self.register_connection(updates)

    async def delete_connection(self, connection_id: str) -> None:
        """Delete a connection."""
        await self.store.delete_async(connection_id)

        # Invalidate cache in factory
        from src.shared.services.storage.blob_factory import BlobStorageFactory
        BlobStorageFactory.invalidate_connection_provider(connection_id)

    async def update_health_status(self, connection_id: str, status: HealthCheckResult):
        """Update health status metadata."""
        conn = await self.get_connection(connection_id)
        if conn:
            conn.last_health_check = status
            await self.register_connection(conn)

    async def update_validation_status(self, connection_id: str, result: StorageValidationResult):
        """Update validation status metadata."""
        conn = await self.get_connection(connection_id)
        if conn:
            conn.last_validation = result
            await self.register_connection(conn)

    async def set_default_connection(self, connection_id: str) -> Optional[BlobConnectionCreate]:
        """
        Set a specific connection as the default.
        Unsets is_default for all other connections.
        """
        target_conn = await self.get_connection(connection_id)
        if not target_conn:
            return None

        # Get all connections
        all_conns = await self.list_connections()

        for conn in all_conns:
            if conn.connection_id == connection_id:
                if not conn.is_default:
                    conn.is_default = True
                    await self.register_connection(conn)
            else:
                if conn.is_default:
                    conn.is_default = False
                    await self.register_connection(conn)

        return await self.get_connection(connection_id)


_registry_service = None


def get_connection_registry() -> ConnectionRegistryService:
    global _registry_service
    if _registry_service is None:
        _registry_service = ConnectionRegistryService()
    return _registry_service
