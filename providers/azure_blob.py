from typing import Optional, Union
from datetime import datetime, timedelta, timezone

from src.shared.observability.logger_factory import get_component_logger
from src.shared.services.storage.providers.base_blob_provider import BaseBlobProvider
from src.shared.services.storage.security.compliance import (
    StorageCompliance,
    ValidationResult,
    EncryptionStandard,
    TransportSecurity,
)

try:
    from azure.storage.blob.aio import BlobServiceClient
    from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError

    AZURE_AVAILABLE = True
except ImportError:
    AZURE_AVAILABLE = False
    BlobServiceClient = None

logger = get_component_logger("storage", "azure_blob")


class AzureBlobProvider(BaseBlobProvider):
    """
    Azure Blob Storage Provider.
    Uses azure-storage-blob aio client.
    """

    def __init__(self, connection_string: str, container_name: str, account_key: Optional[str] = None):
        if not AZURE_AVAILABLE:
            raise ImportError("azure-storage-blob is required for Azure providers.")

        self.connection_string = connection_string
        self.container_name = container_name
        self.service_client = BlobServiceClient.from_connection_string(connection_string)
        self.container_client = self.service_client.get_container_client(container_name)

        # Store account key for SAS generation
        self.account_key = account_key
        if not self.account_key:
            # Attempt to parse account key from connection string
            try:
                # Connection string format: DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=...
                parts = dict(item.split("=", 1) for item in connection_string.split(";") if "=" in item)
                self.account_key = parts.get("AccountKey")
            except Exception as e:
                # If parsing fails, we proceed without key (SAS generation may fail later)
                logger.debug(f"Failed to parse account key from connection string: {e}")

    async def _ensure_container(self):
        try:
            await self.container_client.create_container()
        except ResourceExistsError:
            # Container already exists, which is expected
            logger.debug(f"Container {self.container_name} already exists")

    async def upload(
        self,
        key: str,
        data: Union[bytes, str],
        content_type: Optional[str] = None,
        encryption_context: Optional[dict[str, str]] = None,
    ) -> bool:
        await self._ensure_container()

        if isinstance(data, str):
            data = data.encode("utf-8")

        blob_client = self.container_client.get_blob_client(key)

        try:
            content_settings = None
            if content_type:
                from azure.storage.blob import ContentSettings

                content_settings = ContentSettings(content_type=content_type)

            await blob_client.upload_blob(data, overwrite=True, content_settings=content_settings)
            return True
        except Exception as e:
            logger.error(f"Azure Upload Failed: {e}", extra={"key": key})
            return False

    async def download(self, key: str, decryption_context: Optional[dict[str, str]] = None) -> Optional[bytes]:
        blob_client = self.container_client.get_blob_client(key)
        try:
            download_stream = await blob_client.download_blob()
            return await download_stream.readall()
        except ResourceNotFoundError:
            return None
        except Exception as e:
            logger.error(f"Azure Download Failed: {e}")
            return None

    async def delete(self, key: str) -> bool:
        blob_client = self.container_client.get_blob_client(key)
        try:
            await blob_client.delete_blob()
            return True
        except ResourceNotFoundError:
            return False
        except Exception as e:
            logger.error(f"Azure Delete Failed: {e}")
            return False

    async def exists(self, key: str) -> bool:
        blob_client = self.container_client.get_blob_client(key)
        try:
            return await blob_client.exists()
        except Exception:
            return False

    async def get_signed_url(self, key: str, expires_in: int = 3600) -> str:
        from azure.storage.blob import generate_blob_sas, BlobSasPermissions

        try:
            blob_client = self.container_client.get_blob_client(key)

            if not self.account_key:
                # Try to fallback to credential object if available, though less reliable for string key
                if hasattr(self.service_client, "credential") and hasattr(
                    self.service_client.credential, "account_key"
                ):
                    self.account_key = self.service_client.credential.account_key
                else:
                    logger.warning("Azure SAS Generation: No account key available.")
                    return ""

            sas_token = generate_blob_sas(
                account_name=self.service_client.account_name,
                container_name=self.container_name,
                blob_name=key,
                account_key=self.account_key,
                permission=BlobSasPermissions(read=True),
                expiry=datetime.now(timezone.utc) + timedelta(seconds=expires_in),
            )
            return f"{blob_client.url}?{sas_token}"
        except Exception as e:
            logger.error(f"Azure SAS Generation Failed: {e}")
            return ""

    async def list_blobs(self, prefix: Optional[str] = None, limit: Optional[int] = None):
        """Async iterator for Azure blobs."""
        count = 0
        try:
            async for blob in self.container_client.list_blobs(name_starts_with=prefix):
                yield {
                    "name": blob.name,
                    "size": blob.size,
                    "last_modified": blob.last_modified,
                    "content_type": blob.content_settings.content_type if blob.content_settings else None,
                    "provider_type": "azure",
                }
                count += 1
                if limit and count >= limit:
                    break
        except Exception as e:
            logger.error(f"Azure List Blobs Failed: {e}")

    def validate_compliance(self) -> ValidationResult:
        # Azure defaults to TLS 1.2 and AES-256 (Service Managed)
        return StorageCompliance.validate_nist_800_175b(
            encryption=EncryptionStandard.AES_256_GCM,
            transport=TransportSecurity.TLS_1_2,
            key_rotation_enabled=True,  # Azure managed
        )
