import os
import httpx
from typing import Optional, Union
import base64

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from src.shared.observability.logger_factory import get_component_logger
from src.shared.services.storage.providers.base_blob_provider import BaseBlobProvider
from src.shared.services.storage.security.compliance import (
    StorageCompliance,
    ValidationResult,
    EncryptionStandard,
    TransportSecurity,
)

logger = get_component_logger("storage", "http_generic")


class HttpBlobProvider(BaseBlobProvider):
    """
    Generic HTTP Blob Provider.
    Supports client-side AES-256-GCM encryption.
    """

    def __init__(
        self,
        base_url: str,
        auth_header: Optional[dict[str, str]] = None,
        encryption_key: Optional[str] = None,  # Base64 encoded 32-byte key
    ):
        self.base_url = base_url.rstrip("/")
        self.auth_header = auth_header or {}

        self.aesgcm = None
        if encryption_key:
            try:
                key_bytes = base64.b64decode(encryption_key)
                if len(key_bytes) != 32:
                    raise ValueError("Encryption key must be 32 bytes (256 bits)")
                self.aesgcm = AESGCM(key_bytes)
            except Exception as e:
                logger.error(f"Invalid encryption key: {e}")
                raise

        self.client = httpx.AsyncClient(timeout=30.0)

    async def _encrypt(self, data: bytes) -> bytes:
        if not self.aesgcm:
            return data
        nonce = os.urandom(12)
        ciphertext = self.aesgcm.encrypt(nonce, data, None)
        return nonce + ciphertext

    async def _decrypt(self, data: bytes) -> bytes:
        if not self.aesgcm:
            return data
        if len(data) < 12:
            raise ValueError("Data too short for decryption")
        nonce = data[:12]
        ciphertext = data[12:]
        return self.aesgcm.decrypt(nonce, ciphertext, None)

    async def upload(
        self,
        key: str,
        data: Union[bytes, str],
        content_type: Optional[str] = None,
        encryption_context: Optional[dict[str, str]] = None,
    ) -> bool:
        url = f"{self.base_url}/{key}"

        if isinstance(data, str):
            data = data.encode("utf-8")

        try:
            encrypted_data = await self._encrypt(data)

            headers = self.auth_header.copy()
            if content_type:
                headers["Content-Type"] = content_type

            response = await self.client.put(url, content=encrypted_data, headers=headers)
            response.raise_for_status()
            return True
        except Exception as e:
            logger.error(f"HTTP Upload Failed: {e}", extra={"url": url})
            return False

    async def download(self, key: str, decryption_context: Optional[dict[str, str]] = None) -> Optional[bytes]:
        url = f"{self.base_url}/{key}"
        try:
            response = await self.client.get(url, headers=self.auth_header)
            if response.status_code == 404:
                return None
            response.raise_for_status()

            return await self._decrypt(response.content)
        except Exception as e:
            logger.error(f"HTTP Download Failed: {e}", extra={"url": url})
            return None

    async def delete(self, key: str) -> bool:
        url = f"{self.base_url}/{key}"
        try:
            response = await self.client.delete(url, headers=self.auth_header)
            if response.status_code == 404:
                return False
            response.raise_for_status()
            return True
        except Exception as e:
            logger.error(f"HTTP Delete Failed: {e}")
            return False

    async def exists(self, key: str) -> bool:
        url = f"{self.base_url}/{key}"
        try:
            response = await self.client.head(url, headers=self.auth_header)
            return response.status_code == 200
        except Exception:
            return False

    async def get_signed_url(self, key: str, expires_in: int = 3600) -> str:
        # Generic HTTP usually doesn't have a standardized signed URL mechanism
        # unless it's S3-compatible, but that would use S3Provider.
        # We return the direct URL if authorized, or public.
        return f"{self.base_url}/{key}"

    async def list_blobs(self, prefix: Optional[str] = None, limit: Optional[int] = None):
        """
        Generic HTTP listing not standard.
        Performs connectivity check on base_url.
        """
        try:
            # Check connectivity
            headers = self.auth_header.copy() if self.auth_header else {}
            # Many servers block HEAD, so verify generic availability
            response = await self.client.head(self.base_url, headers=headers)

            if response.status_code >= 400:
                # Fallback to GET if HEAD fails or is not allowed
                response = await self.client.get(self.base_url, headers=headers)

            # If still error, raise it
            response.raise_for_status()
        except Exception as e:
            logger.warning(f"HTTP List/Connectivity Check Failed: {e}")
            raise e

        # Yield nothing as listing is undefined
        return
        yield

    def validate_compliance(self) -> ValidationResult:
        transport = TransportSecurity.TLS_1_2 if self.base_url.startswith("https") else TransportSecurity.PLAIN
        encryption = EncryptionStandard.AES_256_GCM if self.aesgcm else EncryptionStandard.NONE

        return StorageCompliance.validate_nist_800_175b(
            encryption=encryption,
            transport=transport,
            key_rotation_enabled=False,  # Client side key management usually implies manual rotation
        )
