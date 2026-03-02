import os
import sys
import httpx
from typing import Optional, Union, AsyncIterator, Any
import base64

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from storage.shared.observability.logger_factory import get_component_logger
from storage.interfaces.base_blob_provider import BaseBlobProvider
from storage.security.compliance import (
    StorageCompliance,
    ValidationResult,
    EncryptionStandard,
    TransportSecurity,
)

# Force UTF-8 stdout encoding for Python CLIs
if sys.stdout.encoding != "utf-8" and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

logger = get_component_logger("storage", "http_generic")


class HttpBlobProvider(BaseBlobProvider):
    """
    Generic HTTP Blob Provider.
    Supports client-side AES-256-GCM encryption for standard web endpoints.
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
        """
        Generic HTTP usually doesn't have a standardized signed URL mechanism.
        Returns direct URL.
        """
        return f"{self.base_url}/{key}"

    async def list_blobs(
        self, prefix: Optional[str] = None, limit: Optional[int] = None
    ) -> AsyncIterator[dict[str, Any]]:
        """
        Attempts to list blobs via HTTP GET on the base URL.

        If the endpoint returns a JSON array, each element is yielded as blob metadata.
        Generic HTTP endpoints do not have a standardized listing protocol; if the
        response is not a parseable JSON array, a warning is logged and no items are
        yielded. Callers requiring listing MUST use a provider with native listing
        support (S3, Azure, GCP, or the BlobIndexMiddleware).
        """
        try:
            headers = self.auth_header.copy() if self.auth_header else {}
            list_url = f"{self.base_url}/" if prefix is None else f"{self.base_url}/{prefix}"
            response = await self.client.get(list_url, headers=headers)
            response.raise_for_status()

            content_type = response.headers.get("content-type", "")
            if "application/json" in content_type:
                payload = response.json()
                if isinstance(payload, list):
                    for count, item in enumerate(payload, start=1):
                        if isinstance(item, dict):
                            yield item
                        else:
                            yield {"name": str(item)}
                        if limit and count >= limit:
                            return
                    return

            # Response was not a JSON array — listing is not supported by this endpoint.
            logger.warning(
                "HTTP list_blobs: endpoint did not return a JSON array; "
                "listing is not supported for this generic HTTP provider. "
                "Use BlobIndexMiddleware or a native cloud provider for listing."
            )
        except Exception as e:
            logger.warning(f"HTTP list_blobs failed: {e}")

    def validate_compliance(self) -> ValidationResult:
        transport = TransportSecurity.TLS_1_2 if self.base_url.startswith("https") else TransportSecurity.PLAIN
        encryption = EncryptionStandard.AES_256_GCM if self.aesgcm else EncryptionStandard.NONE

        return StorageCompliance.validate_nist_800_175b(
            encryption=encryption,
            transport=transport,
            key_rotation_enabled=False,
        )
