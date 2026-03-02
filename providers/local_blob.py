import asyncio
import sys
from typing import Optional, Union, AsyncIterator, Any
from pathlib import Path
from datetime import datetime

from storage.shared.observability.logger_factory import get_component_logger
from storage.interfaces.base_blob_provider import BaseBlobProvider
from storage.security.compliance import ValidationResult

# Force UTF-8 stdout encoding for Python CLIs
if sys.stdout.encoding != "utf-8" and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

logger = get_component_logger("storage", "local_blob")


class LocalBlobProvider(BaseBlobProvider):
    """
    Local Filesystem Blob Provider for Agentic Code Engine.
    Uses asyncio.to_thread / run_in_executor for blocking I/O operations.
    """

    def __init__(self, base_dir: str = "data/blobs"):
        # Resolve path relative to current project root if needed
        self.base_dir = Path(base_dir).resolve()
        # Ensure base directory exists
        self.base_dir.mkdir(parents=True, exist_ok=True)

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

                def _write_str():
                    with open(path, "w", encoding="utf-8") as f:
                        f.write(data)

                await loop.run_in_executor(None, _write_str)
            else:

                def _write_bytes():
                    with open(path, "wb") as f:
                        f.write(data)

                await loop.run_in_executor(None, _write_bytes)
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

            def _read_bytes():
                with open(path, "rb") as f:
                    return f.read()

            return await loop.run_in_executor(None, _read_bytes)
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
        Local storage pre-signed URLs are simulated as local API routes.
        """
        return f"/api/v1/storage/blobs/local/{key}?expires={expires_in}"

    async def list_blobs(
        self, prefix: Optional[str] = None, limit: Optional[int] = None
    ) -> AsyncIterator[dict[str, Any]]:
        """
        List local files asynchronously.
        """
        loop = asyncio.get_running_loop()

        def _list():
            blobs = []
            count = 0
            for path in self.base_dir.rglob("*"):
                if not path.is_file():
                    continue

                key = str(path.relative_to(self.base_dir)).replace("\\", "/")
                if prefix and not key.startswith(prefix):
                    continue

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
        Local storage is intended for development and transient caching only.
        """
        return ValidationResult(
            compliant=False,
            standards_met=["NIST.DevOnly"],
            issues=["Local filesystem lacks native NIST-compliant encryption at rest"],
            remediation_steps=["Transition to S3 or Azure Blob for production workloads"],
        )
