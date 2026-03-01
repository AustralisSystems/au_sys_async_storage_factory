from typing import Optional, Any

from src.shared.services.storage.providers.base_blob_provider import BaseBlobProvider
from src.shared.services.storage.providers.local_blob import LocalBlobProvider
from src.shared.services.storage.providers.aws_s3 import S3BlobProvider
from src.shared.observability.logger_factory import get_component_logger
from src.shared.services.config.settings import get_settings
from src.api.services.storage.schemas.blob_connection_schema import (
    BlobConnectionCreate,
    BlobProviderType,
    S3ConnectionConfig,
    AzureConnectionConfig,
    GCPConnectionConfig,
    HttpConnectionConfig,
    LocalConnectionConfig,
)

logger = get_component_logger("storage", "factory")


class BlobStorageFactory:
    """
    Factory for creating blob storage providers.
    Supports lazy loading and configuration injection.
    """

    _providers: dict[str, BaseBlobProvider] = {}
    _connection_providers: dict[str, BaseBlobProvider] = {}

    @classmethod
    async def get_provider_from_connection(cls, connection_id: str) -> Optional[BaseBlobProvider]:
        """
        Get or create a provider instance from a specific connection ID.
        Uses caching to avoid recreating providers.
        """
        if connection_id in cls._connection_providers:
            return cls._connection_providers[connection_id]

        # Lazy import to avoid circular dependency
        from src.shared.services.storage.connection_registry import get_connection_registry

        registry = get_connection_registry()

        config = await registry.get_connection(connection_id)
        if not config:
            return None

        provider = cls.create_provider_from_config(config)
        cls._connection_providers[connection_id] = provider
        return provider

    @classmethod
    def invalidate_connection_provider(cls, connection_id: str):
        """Invalidate cached provider for a connection."""
        if connection_id in cls._connection_providers:
            del cls._connection_providers[connection_id]

    @classmethod
    def get_provider(cls, provider_type: Optional[str] = None) -> BaseBlobProvider:
        """
        Get or create a storage provider instance.
        If provider_type is None, use the default from settings.
        """
        settings = get_settings()

        # Determine provider type
        if not provider_type:
            provider_type = getattr(settings, "STORAGE_PROVIDER", "local").lower()

        # Return cached instance if available
        if provider_type in cls._providers:
            return cls._providers[provider_type]

        # Create new instance
        provider = cls._create_provider(provider_type, settings)
        cls._providers[provider_type] = provider

        # Compliance Check on instantiation
        validation = provider.validate_compliance()
        if not validation.compliant:
            logger.warning(
                f"Storage Provider {provider_type} is NOT compliant with security standards.",
                extra={"issues": validation.issues},
            )
        else:
            logger.info(
                f"Storage Provider {provider_type} created and validated.",
                extra={"standards": validation.standards_met},
            )

        return provider

    @classmethod
    def create_provider_from_config(cls, config: BlobConnectionCreate) -> BaseBlobProvider:
        """
        Create a new provider instance from a connection configuration object.
        Does NOT cache the provider.
        """
        provider = None

        if config.provider_type == BlobProviderType.AWS_S3:
            s3_config: S3ConnectionConfig = config
            provider = S3BlobProvider(
                bucket_name=s3_config.bucket_name,
                region_name=s3_config.region_name,
                access_key=s3_config.access_key.get_secret_value() if s3_config.access_key else None,
                secret_key=s3_config.secret_key.get_secret_value() if s3_config.secret_key else None,
                kms_key_id=s3_config.kms_key_id,
                endpoint_url=s3_config.endpoint_url,
            )

        elif config.provider_type == BlobProviderType.AZURE_BLOB:
            azure_config: AzureConnectionConfig = config
            # Lazy import
            from src.shared.services.storage.providers.azure_blob import AzureBlobProvider

            provider = AzureBlobProvider(
                connection_string=azure_config.connection_string.get_secret_value()
                if azure_config.connection_string
                else None,
                container_name=azure_config.container_name,
                account_key=azure_config.account_key.get_secret_value() if azure_config.account_key else None,
                account_name=azure_config.account_name,
            )

        elif config.provider_type == BlobProviderType.GCP_STORAGE:
            gcp_config: GCPConnectionConfig = config
            # Lazy import
            from src.shared.services.storage.providers.gcp_storage import GCPBlobProvider

            provider = GCPBlobProvider(
                bucket_name=gcp_config.bucket_name,
                project_id=gcp_config.project_id,
                credentials_json=gcp_config.credentials_json.get_secret_value()
                if gcp_config.credentials_json
                else None,
            )

        elif config.provider_type == BlobProviderType.HTTP_GENERIC:
            http_config: HttpConnectionConfig = config
            # Lazy import
            from src.shared.services.storage.providers.http_generic import HttpBlobProvider

            auth_header_dict = None
            if http_config.auth_header:
                auth_header_dict = {k: v.get_secret_value() for k, v in http_config.auth_header.items()}

            provider = HttpBlobProvider(
                base_url=http_config.base_url,
                auth_header=auth_header_dict,
                encryption_key=http_config.encryption_key.get_secret_value() if http_config.encryption_key else None,
            )

        elif config.provider_type == BlobProviderType.LOCAL:
            local_config: LocalConnectionConfig = config
            provider = LocalBlobProvider(base_dir=local_config.base_dir)

        else:
            logger.error(f"Unknown provider type: {config.provider_type}, falling back to local defaults")
            return LocalBlobProvider()

        # Validate compliance for this specific connection
        validation = provider.validate_compliance()
        if not validation.compliant:
            logger.warning(
                f"Storage Connection {config.name} ({config.provider_type}) is NOT compliant.",
                extra={"issues": validation.issues},
            )

        return provider

    @classmethod
    def _create_provider(cls, provider_type: str, settings: Any) -> BaseBlobProvider:
        if provider_type == "s3":
            return S3BlobProvider(
                bucket_name=getattr(settings, "STORAGE_BUCKET", ""),
                region_name=getattr(settings, "STORAGE_REGION", "us-east-1"),
                access_key=getattr(settings, "STORAGE_ACCESS_KEY", None),
                secret_key=getattr(settings, "STORAGE_SECRET_KEY", None),
                kms_key_id=getattr(settings, "STORAGE_KMS_KEY_ID", None),
                endpoint_url=getattr(settings, "STORAGE_ENDPOINT_URL", None),
            )
        if provider_type == "azure":
            # Lazy import to avoid hard dependency on azure if unconfigured
            from src.shared.services.storage.providers.azure_blob import AzureBlobProvider

            return AzureBlobProvider(
                connection_string=getattr(settings, "STORAGE_CONNECTION_STRING", ""),
                container_name=getattr(settings, "STORAGE_CONTAINER", "blobs"),
                account_key=getattr(settings, "STORAGE_ACCOUNT_KEY", None),
            )
        if provider_type == "gcp":
            # Lazy import
            from src.shared.services.storage.providers.gcp_storage import GCPBlobProvider

            return GCPBlobProvider(
                bucket_name=getattr(settings, "STORAGE_BUCKET", ""),
                project_id=getattr(settings, "STORAGE_PROJECT_ID", None),
                credentials_json=getattr(settings, "STORAGE_CREDENTIALS_JSON", None),
            )
        if provider_type == "local":
            return LocalBlobProvider(base_dir=getattr(settings, "STORAGE_LOCAL_DIR", "data/blobs"))
        logger.error(f"Unknown storage provider type: {provider_type}, falling back to local")
        return LocalBlobProvider()

    @classmethod
    def clear_providers(cls):
        """Clear cached providers (useful for testing)."""
        cls._providers.clear()
        cls._connection_providers.clear()
