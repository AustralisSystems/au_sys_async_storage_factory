import asyncio
from typing import Optional, Union
from pathlib import Path

from src.shared.observability.logger_factory import get_component_logger
from src.shared.services.storage.providers.base_blob_provider import BaseBlobProvider
from src.shared.services.storage.security.compliance import ValidationResult

logger = get_component_logger("storage", "local_blob")


class LocalBlobProvider(BaseBlobProvider):
    """
    Local Filesystem Blob Provider.
    Uses asyncio.to_thread / run_in_executor for all blocking I/O.
    """

    def __init__(self, base_dir: str = "data/blobs"):
        self.base_dir = Path(base_dir)
        # Ensure base directory exists (blocking is okay in __init__)
        self.base_dir.mkdir(parents=True, exist_ok=True)

    async def _write_file(self, path: Path, data: Union[bytes, str], mode: str, encoding: Optional[str]):
        with open(path, mode, encoding=encoding) as f:
            f.write(data)

    async def upload(
        self,
        key: str,
        data: Union[bytes, str],
        content_type: Optional[str] = None,
        encryption_context: Optional[dict[str, str]] = None,
    ) -> bool:
        loop = asyncio.get_running_loop()
        path = self.base_dir / key

        # Ensure parent directory exists
        await loop.run_in_executor(None, lambda: path.parent.mkdir(parents=True, exist_ok=True))

        try:
            if isinstance(data, str):
                mode = "w"
                encoding = "utf-8"
            else:
                mode = "wb"
                encoding = None

            # Use executor for file write
            await loop.run_in_executor(None, lambda: open(path, mode, encoding=encoding).write(data))
            return True
        except Exception as e:
            logger.error(f"Local Upload Failed: {e}", extra={"key": key})
            return False

    async def download(self, key: str, decryption_context: Optional[dict[str, str]] = None) -> Optional[bytes]:
        loop = asyncio.get_running_loop()
        path = self.base_dir / key

        path_exists = await loop.run_in_executor(None, path.exists)
        if not path_exists:
            return None

        try:
            # Use executor for file read
            return await loop.run_in_executor(None, lambda: open(path, "rb").read())
        except Exception as e:
            logger.error(f"Local Download Failed: {e}")
            return None

    async def delete(self, key: str) -> bool:
        loop = asyncio.get_running_loop()
        path = self.base_dir / key

        path_exists = await loop.run_in_executor(None, path.exists)
        if not path_exists:
            return False

        try:
            await loop.run_in_executor(None, path.unlink)
            return True
        except Exception as e:
            logger.error(f"Local Delete Failed: {e}")
            return False

    async def exists(self, key: str) -> bool:
        loop = asyncio.get_running_loop()
        path = self.base_dir / key
        return await loop.run_in_executor(None, path.exists)

    async def get_signed_url(self, key: str, expires_in: int = 3600) -> str:
        """
        For local storage, generate a proxied API URL.
        """
        return f"/api/v1/storage/blobs/local/{key}?expires={expires_in}"

    async def list_blobs(self, prefix: Optional[str] = None, limit: Optional[int] = None):
        """
        List local files.
        """
        loop = asyncio.get_running_loop()

        def _list():
            blobs = []
            count = 0
            # Simple recursive walk or glob
            # Using rglob for recursive
            from datetime import datetime

            for path in self.base_dir.rglob("*"):
                # Filter by prefix if needed (simple implementation)
                key = str(path.relative_to(self.base_dir)).replace("\\", "/")
                if prefix and not key.startswith(prefix):
                    continue

                if path.is_file():
                    stat = path.stat()
                    blobs.append(
                        {
                            "name": key,
                            "size": stat.st_size,
                            "last_modified": datetime.fromtimestamp(stat.st_mtime),
                            "content_type": None,
                            "provider_type": "local",
                        }
                    )
                    count += 1
                    if limit and count >= limit:
                        break
            return blobs

        blobs = await loop.run_in_executor(None, _list)
        for blob in blobs:
            yield blob

    def validate_compliance(self) -> ValidationResult:
        """
        Local storage is dev-only.
        """
        return ValidationResult(
            compliant=False,
            standards_met=["DevOnly"],
            issues=["Local filesystem is not for production", "No native encryption at rest"],
            remediation_steps=["Use S3/Azure/GCP for production"],
        )
