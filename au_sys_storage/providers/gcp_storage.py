import asyncio
import functools
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
    from google.cloud import storage  # type: ignore[attr-defined]
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
            from google.oauth2 import service_account

            creds = service_account.Credentials.from_service_account_file(credentials_json)  # type: ignore[no-untyped-call]
            self.client = storage.Client(project=project_id, credentials=creds)
        else:
            self.client = storage.Client(project=project_id)

        self.bucket = self.client.bucket(bucket_name)

    async def _run_in_executor(self, func: Any, *args: Any, **kwargs: Any) -> Any:
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
                logger.warning("Encryption context not yet supported for GCP, using bucket default encryption.")

            await self._run_in_executor(blob.upload_from_string, data, content_type=content_type)
            return True
        except Exception as e:
            logger.error(f"GCP Upload Failed: {e}", extra={"key": key})
            return False

    async def download(self, key: str, decryption_context: Optional[dict[str, str]] = None) -> Optional[bytes]:
        try:
            blob = self.bucket.blob(key)
            data: bytes = await self._run_in_executor(blob.download_as_bytes)
            return data
        except Exception as e:
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
            exists: bool = await self._run_in_executor(blob.exists)
            return exists
        except Exception:
            return False

    async def get_signed_url(self, key: str, expires_in: int = 3600) -> str:
        try:
            blob = self.bucket.blob(key)
            url: str = await self._run_in_executor(
                functools.partial(
                    blob.generate_signed_url, version="v4", expiration=timedelta(seconds=expires_in), method="GET"
                )
            )
            return url
        except Exception as e:
            logger.error(f"GCP Signed URL Failed: {e}")
            return ""

    async def list_blobs(
        self, prefix: Optional[str] = None, limit: Optional[int] = None
    ) -> AsyncIterator[dict[str, Any]]:
        def _list() -> list[dict[str, Any]]:
            blobs_iter = self.bucket.list_blobs(prefix=prefix, max_results=limit)
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
            blobs = await self._run_in_executor(_list)
            for b in blobs:
                yield b
        except Exception as e:
            logger.error(f"GCP List Failed: {e}")

    # --- IBackupProvider Implementation ---

    async def create_backup(self, backup_path: str, metadata: Optional[dict[str, Any]] = None) -> bool:
        """Creates a 'Sovereign' local ZIP backup of the GCP bucket."""
        from pathlib import Path

        try:
            target = Path(backup_path)
            target.parent.mkdir(parents=True, exist_ok=True)

            with zipfile.ZipFile(target, "w", zipfile.ZIP_DEFLATED) as zf:
                async for blob_meta in self.list_blobs():
                    key = blob_meta["name"]
                    data = await self.download(key)
                    if data:
                        zf.writestr(key, data)

                if metadata:
                    zf.writestr("backup_metadata.json", json.dumps(metadata, indent=2))

            logger.info(f"GCP Backup created: {backup_path}")
            return True
        except Exception as e:
            logger.error(f"GCP Backup Failed: {e}")
            return False

    async def restore_backup(self, backup_path: str, clear_existing: bool = False) -> bool:
        """Restores GCP bucket from a sovereign backup archive."""
        try:
            if clear_existing:
                async for blob_meta in self.list_blobs():
                    await self.delete(blob_meta["name"])

            with zipfile.ZipFile(backup_path, "r") as zf:
                for name in zf.namelist():
                    if name == "backup_metadata.json":
                        continue
                    data = zf.read(name)
                    await self.upload(name, data)

            logger.info(f"GCP Restore completed from {backup_path}")
            return True
        except Exception as e:
            logger.error(f"GCP Restore Failed: {e}")
            return False

    async def validate_backup(self, backup_path: str) -> dict[str, Any]:
        """Validates a GCP ZIP backup archive."""
        try:
            with zipfile.ZipFile(backup_path, "r") as zf:
                bad_file = zf.testzip()
                if bad_file:
                    return {"valid": False, "error": f"Bad CRC for file {bad_file}"}
                return {"valid": True, "file_count": len(zf.namelist())}
        except Exception as e:
            return {"valid": False, "error": str(e)}

    async def list_backups(self, backup_dir: str) -> dict[str, dict[str, Any]]:
        """Lists available GCP backups locally."""
        from pathlib import Path

        backups = {}
        path = Path(backup_dir)
        if not path.exists():
            return {}
        for item in path.glob("gcp_backup_*.zip"):
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
