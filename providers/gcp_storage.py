import asyncio
import functools
from typing import Optional, Union
from datetime import timedelta

from src.shared.observability.logger_factory import get_component_logger
from src.shared.services.storage.providers.base_blob_provider import BaseBlobProvider
from src.shared.services.storage.security.compliance import (
    StorageCompliance,
    ValidationResult,
    EncryptionStandard,
    TransportSecurity,
)

try:
    from google.cloud import storage
    from google.oauth2 import service_account

    GCP_AVAILABLE = True
except ImportError:
    GCP_AVAILABLE = False

logger = get_component_logger("storage", "gcp_blob")


class GCPBlobProvider(BaseBlobProvider):
    """
    Google Cloud Storage Provider.
    Wraps synchronous google-cloud-storage with run_in_executor.
    """

    def __init__(self, bucket_name: str, project_id: Optional[str] = None, credentials_json: Optional[str] = None):
        if not GCP_AVAILABLE:
            raise ImportError("google-cloud-storage is required for GCP providers.")

        self.bucket_name = bucket_name

        if credentials_json:
            creds = service_account.Credentials.from_service_account_file(credentials_json)
            self.client = storage.Client(project=project_id, credentials=creds)
        else:
            # Fallback to default env vars
            self.client = storage.Client(project=project_id)

        self.bucket = self.client.bucket(bucket_name)

    async def _run_in_executor(self, func, *args, **kwargs):
        loop = asyncio.get_running_loop()
        partial_func = functools.partial(func, *args, **kwargs)
        return await loop.run_in_executor(None, partial_func)

    async def upload(
        self,
        key: str,
        data: Union[bytes, str],
        content_type: Optional[str] = None,
        encryption_context: Optional[dict[str, str]] = None,
    ) -> bool:
        if isinstance(data, str):
            data = data.encode("utf-8")

        try:
            blob = self.bucket.blob(key)
            if encryption_context:
                # GCP Customer-Supplied Encryption Keys (CSEK) or CMEK logic would go here
                # For basic implementation, we rely on Google-managed encryption (AES-256)
                logger.warning("Encryption context not yet supported for GCP, using bucket default encryption.")

            await self._run_in_executor(blob.upload_from_string, data, content_type=content_type)
            return True
        except Exception as e:
            logger.error(f"GCP Upload Failed: {e}", extra={"key": key})
            return False

    async def download(self, key: str, decryption_context: Optional[dict[str, str]] = None) -> Optional[bytes]:
        try:
            blob = self.bucket.blob(key)
            return await self._run_in_executor(blob.download_as_bytes)
        except Exception as e:
            # Check for 404 explicitly if possible, but generic catch for now
            logger.error(f"GCP Download Failed: {e}")
            return None

    async def delete(self, key: str) -> bool:
        try:
            blob = self.bucket.blob(key)
            await self._run_in_executor(blob.delete)
            return True
        except Exception as e:
            logger.error(f"GCP Delete Failed: {e}")
            return False

    async def exists(self, key: str) -> bool:
        try:
            blob = self.bucket.blob(key)
            return await self._run_in_executor(blob.exists)
        except Exception:
            return False

    async def get_signed_url(self, key: str, expires_in: int = 3600) -> str:
        try:
            blob = self.bucket.blob(key)
            # generate_signed_url is purely local crypto if credentials allow
            return await self._run_in_executor(
                functools.partial(
                    blob.generate_signed_url, version="v4", expiration=timedelta(seconds=expires_in), method="GET"
                )
            )
        except Exception as e:
            logger.error(f"GCP Signed URL Failed: {e}")
            return ""

    async def list_blobs(self, prefix: Optional[str] = None, limit: Optional[int] = None):
        """
        List GCP blobs.
        Note: iterating the full list might be slow if large bucket.
        """
        loop = asyncio.get_running_loop()

        def _list():
            # This is blocking
            # bucket.list_blobs returns an iterator
            blobs_iter = self.bucket.list_blobs(prefix=prefix, max_results=limit)
            # Fetch all (up to limit) into memory to yield safely from async gen
            return [
                {
                    "name": b.name,
                    "size": b.size,
                    "last_modified": b.updated,
                    "content_type": b.content_type,
                    "provider_type": "gcp",
                }
                for b in blobs_iter
            ]

        try:
            blobs = await loop.run_in_executor(None, _list)
            for b in blobs:
                yield b
        except Exception as e:
            logger.error(f"GCP List Failed: {e}")

    def validate_compliance(self) -> ValidationResult:
        # GCP uses AES-256 by default and TLS 1.2+
        return StorageCompliance.validate_nist_800_175b(
            encryption=EncryptionStandard.AES_256_GCM,
            transport=TransportSecurity.TLS_1_2,
            key_rotation_enabled=True,  # Google managed
        )
