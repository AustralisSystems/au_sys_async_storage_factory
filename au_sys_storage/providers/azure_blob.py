import json
import sys
import zipfile
from datetime import UTC, datetime, timedelta
from typing import Any, AsyncIterator, Optional, Union

from au_sys_storage.interfaces.base_blob_provider import BaseBlobProvider
from au_sys_storage.security.compliance import (
    EncryptionStandard,
    StorageCompliance,
    TransportSecurity,
    ValidationResult,
)
from au_sys_storage.shared.observability.logger_factory import get_component_logger

# Force UTF-8 stdout encoding for Python CLIs
if getattr(sys.stdout, "encoding", None) != "utf-8" and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

try:
    from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
    from azure.storage.blob.aio import BlobServiceClient

    AZURE_AVAILABLE = True
except ImportError:
    AZURE_AVAILABLE = False
    # Use generic types for mypy compliance when package is missing
    BlobServiceClient = Any  # type: ignore
    ResourceExistsError = Exception  # type: ignore
    ResourceNotFoundError = Exception  # type: ignore

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
        self.account_key = account_key

        if not self.account_key:
            try:
                parts = dict(item.split("=", 1) for item in connection_string.split(";") if "=" in item)
                self.account_key = parts.get("AccountKey")
            except Exception as e:
                logger.debug(f"Failed to parse account key from connection string: {e}")

    async def _ensure_container(self) -> None:
        try:
            await self.container_client.create_container()
        except ResourceExistsError:
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
            data: bytes = await download_stream.readall()
            return data
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
        from azure.storage.blob import BlobSasPermissions, generate_blob_sas

        try:
            blob_client = self.container_client.get_blob_client(key)
            if not self.account_key:
                if hasattr(self.service_client, "credential") and hasattr(
                    self.service_client.credential, "account_key"
                ):
                    self.account_key = self.service_client.credential.account_key
                else:
                    logger.warning("Azure SAS Generation: No account key available.")
                    return ""

            sas_token = generate_blob_sas(
                account_name=str(self.service_client.account_name),
                container_name=self.container_name,
                blob_name=key,
                account_key=self.account_key,
                permission=BlobSasPermissions(read=True),
                expiry=datetime.now(UTC) + timedelta(seconds=expires_in),
            )
            return f"{blob_client.url}?{sas_token}"
        except Exception as e:
            logger.error(f"Azure SAS Generation Failed: {e}")
            return ""

    async def list_blobs(
        self, prefix: Optional[str] = None, limit: Optional[int] = None
    ) -> AsyncIterator[dict[str, Any]]:
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

    # --- IBackupProvider Implementation ---

    async def create_backup(self, backup_path: str, metadata: Optional[dict[str, Any]] = None) -> bool:
        """Creates a Sovereign local ZIP backup of the Azure container."""
        from pathlib import Path

        try:
            target = Path(backup_path)
            target.parent.mkdir(parents=True, exist_ok=True)

            with zipfile.ZipFile(target, "w", zipfile.ZIP_DEFLATED) as zf:
                async for blob in self.list_blobs():
                    key = blob["name"]
                    data = await self.download(key)
                    if data:
                        zf.writestr(key, data)

                if metadata:
                    zf.writestr("backup_metadata.json", json.dumps(metadata, indent=2))

            logger.info(f"Azure Backup created: {backup_path}")
            return True
        except Exception as e:
            logger.error(f"Azure Backup Failed: {e}")
            return False

    async def restore_backup(self, backup_path: str, clear_existing: bool = False) -> bool:
        """Restores Azure container from a sovereign ZIP backup."""
        try:
            await self._ensure_container()
            if clear_existing:
                async for blob in self.list_blobs():
                    await self.delete(blob["name"])

            with zipfile.ZipFile(backup_path, "r") as zf:
                for name in zf.namelist():
                    if name == "backup_metadata.json":
                        continue
                    data = zf.read(name)
                    await self.upload(name, data)

            logger.info(f"Azure Restore completed from {backup_path}")
            return True
        except Exception as e:
            logger.error(f"Azure Restore Failed: {e}")
            return False

    async def validate_backup(self, backup_path: str) -> dict[str, Any]:
        """Validates the integrity of an Azure ZIP backup."""
        try:
            with zipfile.ZipFile(backup_path, "r") as zf:
                bad_file = zf.testzip()
                if bad_file:
                    return {"valid": False, "error": f"Bad CRC for file {bad_file}"}
                return {"valid": True, "file_count": len(zf.namelist())}
        except Exception as e:
            return {"valid": False, "error": str(e)}

    async def list_backups(self, backup_dir: str) -> dict[str, dict[str, Any]]:
        """Lists available Azure backups locally."""
        from pathlib import Path

        backups = {}
        path = Path(backup_dir)
        if not path.exists():
            return {}
        for item in path.glob("azure_backup_*.zip"):
            stat = item.stat()
            backups[item.name] = {
                "path": str(item),
                "size_bytes": stat.st_size,
                "created_at": datetime.fromtimestamp(stat.st_mtime, UTC).isoformat(),
            }
        return backups

    def validate_compliance(self) -> ValidationResult:
        return StorageCompliance.validate_nist_800_175b(
            encryption=EncryptionStandard.AES_256_GCM,
            transport=TransportSecurity.TLS_1_2,
            key_rotation_enabled=True,
        )
