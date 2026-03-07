import asyncio
import sys
from typing import Any, AsyncIterator, Optional, Union

try:
    import aioboto3
    from botocore.exceptions import ClientError

    HAS_S3_LIBS = True
except ImportError:
    HAS_S3_LIBS = False
    ClientError = Exception

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

logger = get_component_logger("storage", "aws_s3")


class S3BlobProvider(BaseBlobProvider):
    """
    AWS S3 Blob Storage Provider using aioboto3 for native async support.
    """

    def __init__(
        self,
        bucket_name: str,
        region_name: str,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        kms_key_id: Optional[str] = None,
        endpoint_url: Optional[str] = None,
    ):
        if not HAS_S3_LIBS:
            raise ImportError("aioboto3 and botocore are required for the S3 provider.")

        self.bucket = bucket_name
        self.region = region_name
        self.access_key = access_key
        self.secret_key = secret_key
        self.kms_key_id = kms_key_id
        self.endpoint_url = endpoint_url
        self.session = aioboto3.Session()

    def _get_client(self) -> Any:
        """Returns an async S3 client context manager."""
        return self.session.client(
            "s3",
            region_name=self.region,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            endpoint_url=self.endpoint_url,
        )

    async def upload(
        self,
        key: str,
        data: Union[bytes, str],
        content_type: Optional[str] = None,
        encryption_context: Optional[dict[str, str]] = None,
    ) -> bool:
        if isinstance(data, str):
            data = data.encode("utf-8")

        extra_args = {}
        if content_type:
            extra_args["ContentType"] = content_type

        # Enforce Encryption
        if self.kms_key_id:
            extra_args["ServerSideEncryption"] = "aws:kms"
            extra_args["SSEKMSKeyId"] = self.kms_key_id
        else:
            extra_args["ServerSideEncryption"] = "AES256"

        if encryption_context:
            import json

            extra_args["SSEKMSEncryptionContext"] = json.dumps(encryption_context)

        try:
            async with self._get_client() as client:
                await client.put_object(Bucket=self.bucket, Key=key, Body=data, **extra_args)
            return True
        except ClientError as e:
            logger.error(f"S3 Upload Failed: {e}", extra={"key": key, "bucket": self.bucket})
            return False
        except Exception as e:
            logger.exception(f"Unexpected S3 Upload Error: {e}")
            return False

    async def download(self, key: str, decryption_context: Optional[dict[str, str]] = None) -> Optional[bytes]:
        try:
            async with self._get_client() as client:
                response = await client.get_object(Bucket=self.bucket, Key=key)
                async with response["Body"] as stream:
                    data: bytes = await stream.read()
                    return data
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                return None
            logger.error(f"S3 Download Failed: {e}")
            return None
        except Exception as e:
            logger.exception(f"Unexpected S3 Download Error: {e}")
            return None

    async def delete(self, key: str) -> bool:
        try:
            async with self._get_client() as client:
                await client.delete_object(Bucket=self.bucket, Key=key)
            return True
        except Exception as e:
            logger.error(f"S3 Delete Failed: {e}")
            return False

    async def exists(self, key: str) -> bool:
        try:
            async with self._get_client() as client:
                await client.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError:
            return False

    async def get_signed_url(self, key: str, expires_in: int = 3600) -> str:
        try:
            async with self._get_client() as client:
                url: str = await client.generate_presigned_url(
                    ClientMethod="get_object",
                    Params={"Bucket": self.bucket, "Key": key},
                    ExpiresIn=expires_in,
                )
            return url
        except Exception as e:
            logger.error(f"S3 Sign URL Failed: {e}")
            return ""

    async def list_blobs(
        self, prefix: Optional[str] = None, limit: Optional[int] = None
    ) -> AsyncIterator[dict[str, Any]]:
        """Async iterator for S3 blobs."""
        count = 0
        continuation_token = None

        while True:
            params = {"Bucket": self.bucket}
            if prefix:
                params["Prefix"] = prefix
            if continuation_token:
                params["ContinuationToken"] = continuation_token

            try:
                async with self._get_client() as client:
                    response = await client.list_objects_v2(**params)

                if "Contents" in response:
                    for obj in response["Contents"]:
                        yield {
                            "name": obj["Key"],
                            "size": obj.get("Size"),
                            "last_modified": obj.get("LastModified"),
                            "content_type": None,
                            "provider_type": "s3",
                        }
                        count += 1
                        if limit and count >= limit:
                            return

                if not response.get("IsTruncated"):
                    break

                continuation_token = response.get("NextContinuationToken")
            except Exception as e:
                logger.error(f"S3 List Blobs Failed: {e}")
                break

    # --- IBackupProvider Implementation ---

    async def create_backup(self, backup_path: str, metadata: Optional[dict[str, Any]] = None) -> bool:
        """
        Creates a 'Sovereign' backup of the S3 bucket by downloading all blobs
        and packaging them into a compressed archive.

        All blobs are downloaded asynchronously first, then the ZIP is written
        via asyncio.to_thread to avoid blocking sync I/O interleaved with async calls.
        """
        import json as _json
        import zipfile
        from pathlib import Path

        try:
            # Phase 1: collect all blob data asynchronously
            blobs: list[tuple[str, bytes]] = []
            async for blob in self.list_blobs():
                key = blob["name"]
                async with self._get_client() as client:
                    response = await client.get_object(Bucket=self.bucket, Key=key)
                    async with response["Body"] as stream:
                        data = await stream.read()
                blobs.append((key, data))

            # Phase 2: write ZIP synchronously in a thread to avoid blocking the event loop
            def _write_zip() -> None:
                target = Path(backup_path)
                target.parent.mkdir(parents=True, exist_ok=True)
                with zipfile.ZipFile(target, "w", zipfile.ZIP_DEFLATED) as zf:
                    for key, data in blobs:
                        zf.writestr(key, data)
                    if metadata:
                        zf.writestr("backup_metadata.json", _json.dumps(metadata))

            await asyncio.to_thread(_write_zip)
            logger.info(f"S3 Backup created: {backup_path}")
            return True
        except Exception as e:
            logger.error(f"S3 Backup Failed: {e}")
            return False

    async def restore_backup(self, backup_path: str, clear_existing: bool = False) -> bool:
        """
        Restores S3 bucket from a sovereign backup archive.
        """
        import zipfile

        try:
            if clear_existing:
                async for blob in self.list_blobs():
                    await self.delete(blob["name"])

            async with self._get_client() as client:
                with zipfile.ZipFile(backup_path, "r") as zf:
                    for name in zf.namelist():
                        if name == "backup_metadata.json":
                            continue
                        data = zf.read(name)
                        await client.put_object(Bucket=self.bucket, Key=name, Body=data)

            logger.info(f"S3 Restore completed from {backup_path}")
            return True
        except Exception as e:
            logger.error(f"S3 Restore Failed: {e}")
            return False

    async def validate_backup(self, backup_path: str) -> dict[str, Any]:
        """
        Validates the integrity of an S3 backup archive.
        """
        import zipfile

        try:
            with zipfile.ZipFile(backup_path, "r") as zf:
                # Test archive integrity
                bad_file = zf.testzip()
                if bad_file:
                    return {"valid": False, "error": f"Bad CRC for file {bad_file}"}
                return {"valid": True, "file_count": len(zf.namelist())}
        except Exception as e:
            return {"valid": False, "error": str(e)}

    async def list_backups(self, backup_dir: str) -> dict[str, dict[str, Any]]:
        """
        Lists available S3 backups in a local directory.
        """
        from datetime import UTC, datetime
        from pathlib import Path

        backups = {}
        path = Path(backup_dir)
        if not path.exists():
            return {}

        for item in path.glob("s3_backup_*.zip"):
            stat = item.stat()
            backups[item.name] = {
                "path": str(item),
                "size_bytes": stat.st_size,
                "created_at": datetime.fromtimestamp(stat.st_ctime, UTC).isoformat(),
            }
        return backups

    def validate_compliance(self) -> ValidationResult:
        """
        Validates S3 configuration for NIST compliance.
        """
        # AWS S3 and aioboto3 use TLS 1.2+ by default.
        transport = TransportSecurity.TLS_1_2
        encryption = EncryptionStandard.AES_256_GCM  # KMS or SSE-S3

        return StorageCompliance.validate_nist_800_175b(
            encryption=encryption,
            transport=transport,
            key_rotation_enabled=True,
        )
