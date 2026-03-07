import asyncio
import json
import shutil
import sys
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, AsyncIterator, Optional, Union

from au_sys_storage.interfaces.base_blob_provider import BaseBlobProvider
from au_sys_storage.security.compliance import ValidationResult
from au_sys_storage.shared.observability.logger_factory import get_component_logger

# Force UTF-8 stdout encoding for Python CLIs
if getattr(sys.stdout, "encoding", None) != "utf-8" and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

logger = get_component_logger("storage", "local_blob")


class LocalBlobProvider(BaseBlobProvider):
    """
    Local Filesystem Blob Provider for Agentic Code Engine.
    Uses asyncio.to_thread for non-blocking I/O operations.
    """

    def __init__(self, base_dir: str = "data/blobs"):
        self.base_dir = Path(base_dir).resolve()
        self.base_dir.mkdir(parents=True, exist_ok=True)

    async def upload(
        self,
        key: str,
        data: Union[bytes, str],
        content_type: Optional[str] = None,
        encryption_context: Optional[dict[str, str]] = None,
    ) -> bool:
        path = self.base_dir / key

        def _write() -> bool:
            try:
                path.parent.mkdir(parents=True, exist_ok=True)
                if isinstance(data, str):
                    path.write_text(data, encoding="utf-8")
                else:
                    path.write_bytes(data)
                return True
            except Exception as e:
                logger.error(f"Local Upload Failed: {e}", extra={"key": key})
                return False

        return await asyncio.to_thread(_write)

    async def download(self, key: str, decryption_context: Optional[dict[str, str]] = None) -> Optional[bytes]:
        path = self.base_dir / key

        def _read() -> Optional[bytes]:
            try:
                if not path.exists():
                    return None
                return path.read_bytes()
            except Exception as e:
                logger.error(f"Local Download Failed: {e}", extra={"key": key})
                return None

        return await asyncio.to_thread(_read)

    async def delete(self, key: str) -> bool:
        path = self.base_dir / key

        def _delete() -> bool:
            try:
                if not path.exists():
                    return False
                path.unlink()
                return True
            except Exception as e:
                logger.error(f"Local Delete Failed: {e}", extra={"key": key})
                return False

        return await asyncio.to_thread(_delete)

    async def exists(self, key: str) -> bool:
        path = self.base_dir / key
        return await asyncio.to_thread(path.exists)

    async def get_signed_url(self, key: str, expires_in: int = 3600) -> str:
        return f"/api/v1/storage/blobs/local/{key}?expires={expires_in}"

    async def list_blobs(
        self, prefix: Optional[str] = None, limit: Optional[int] = None
    ) -> AsyncIterator[dict[str, Any]]:
        def _list() -> list[dict[str, Any]]:
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
                        "last_modified": datetime.fromtimestamp(stat.st_mtime, UTC),
                        "content_type": None,
                        "provider_type": "local",
                    }
                )
                count += 1
                if limit and count >= limit:
                    break
            return blobs

        blobs_list = await asyncio.to_thread(_list)
        for blob in blobs_list:
            yield blob

    # --- IBackupProvider Implementation ---

    async def create_backup(self, backup_path: str, metadata: Optional[dict[str, Any]] = None) -> bool:
        def _archive() -> bool:
            try:
                target = Path(backup_path)
                target.parent.mkdir(parents=True, exist_ok=True)
                with zipfile.ZipFile(target, "w", zipfile.ZIP_DEFLATED) as zf:
                    for path in self.base_dir.rglob("*"):
                        if path.is_file():
                            arcname = str(path.relative_to(self.base_dir))
                            zf.write(path, arcname)
                    if metadata:
                        zf.writestr("backup_metadata.json", json.dumps(metadata, indent=2))
                return True
            except Exception as e:
                logger.error(f"Local Blob Backup Failed: {e}")
                return False

        return await asyncio.to_thread(_archive)

    async def restore_backup(self, backup_path: str, clear_existing: bool = False) -> bool:
        def _extract() -> bool:
            try:
                if clear_existing:
                    for item in self.base_dir.iterdir():
                        if item.is_dir():
                            shutil.rmtree(item)
                        else:
                            item.unlink()

                with zipfile.ZipFile(backup_path, "r") as zf:
                    for member in zf.infolist():
                        if member.filename == "backup_metadata.json":
                            continue
                        target_path = (self.base_dir / member.filename).resolve()
                        if not str(target_path).startswith(str(self.base_dir)):
                            continue
                        zf.extract(member, str(self.base_dir))
                return True
            except Exception as e:
                logger.error(f"Local Blob Restore Failed: {e}")
                return False

        return await asyncio.to_thread(_extract)

    async def validate_backup(self, backup_path: str) -> dict[str, Any]:
        def _validate() -> dict[str, Any]:
            try:
                with zipfile.ZipFile(backup_path, "r") as zf:
                    bad_file = zf.testzip()
                    if bad_file:
                        return {"valid": False, "error": f"Bad CRC for file {bad_file}"}
                    return {"valid": True, "file_count": len(zf.namelist())}
            except Exception as e:
                return {"valid": False, "error": str(e)}

        return await asyncio.to_thread(_validate)

    async def list_backups(self, backup_dir: str) -> dict[str, dict[str, Any]]:
        def _list() -> dict[str, dict[str, Any]]:
            backups = {}
            path = Path(backup_dir)
            if not path.exists():
                return {}
            for item in path.glob("*.zip"):
                stat = item.stat()
                backups[item.name] = {
                    "path": str(item),
                    "size_bytes": stat.st_size,
                    "created_at": datetime.fromtimestamp(stat.st_ctime, UTC).isoformat(),
                }
            return backups

        return await asyncio.to_thread(_list)

    def validate_compliance(self) -> ValidationResult:
        return ValidationResult(
            compliant=False,
            standards_met=["NIST.DevOnly"],
            issues=["Local filesystem lacks native NIST-compliant encryption at rest"],
            remediation_steps=["Transition to S3 or Azure Blob for production workloads"],
        )
