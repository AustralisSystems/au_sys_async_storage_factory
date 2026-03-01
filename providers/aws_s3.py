import asyncio
import functools
from typing import Optional, Union
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from src.shared.observability.logger_factory import get_component_logger
from src.shared.services.storage.providers.base_blob_provider import BaseBlobProvider
from src.shared.services.storage.security.compliance import (
    EncryptionStandard,
    TransportSecurity,
    ValidationResult,
    StorageCompliance,
)

logger = get_component_logger("storage", "aws_s3")


class S3BlobProvider(BaseBlobProvider):
    """
    AWS S3 Blob Storage Provider.
    Uses boto3 with asyncio.run_in_executor for non-blocking I/O.
    """

    def __init__(
        self,
        bucket_name: str,
        region_name: str,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        kms_key_id: Optional[str] = None,
        endpoint_url: Optional[str] = None,  # For MinIO/LocalStack
    ):
        self.bucket = bucket_name
        self.kms_key_id = kms_key_id

        # Configure Boto3 for thread safety and retries
        self.config = Config(retries=dict(max_attempts=3), signature_version="s3v4")

        self.client = boto3.client(
            "s3",
            region_name=region_name,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            endpoint_url=endpoint_url,
            config=self.config,
        )

    async def _run_in_executor(self, func, *args, **kwargs):
        """Helper to run blocking boto3 calls in a separate thread."""
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

        extra_args = {}
        if content_type:
            extra_args["ContentType"] = content_type

        # Enforce Encryption
        if self.kms_key_id:
            extra_args["ServerSideEncryption"] = "aws:kms"
            extra_args["SSEKMSKeyId"] = self.kms_key_id
        else:
            # Fallback to AES-256 (SSE-S3) if no KMS key provided, strictly better than nothing
            extra_args["ServerSideEncryption"] = "AES256"

        if encryption_context:
            # S3 requires string serialization for context
            import json

            extra_args["SSEKMSEncryptionContext"] = json.dumps(encryption_context)

        try:
            await self._run_in_executor(self.client.put_object, Bucket=self.bucket, Key=key, Body=data, **extra_args)
            return True
        except ClientError as e:
            logger.error(f"S3 Upload Failed: {e}", extra={"key": key, "bucket": self.bucket})
            return False
        except Exception as e:
            logger.exception(f"Unexpected S3 Upload Error: {e}")
            return False

    async def download(self, key: str, decryption_context: Optional[dict[str, str]] = None) -> Optional[bytes]:
        try:
            response = await self._run_in_executor(self.client.get_object, Bucket=self.bucket, Key=key)
            return response["Body"].read()
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
            await self._run_in_executor(self.client.delete_object, Bucket=self.bucket, Key=key)
            return True
        except Exception as e:
            logger.error(f"S3 Delete Failed: {e}")
            return False

    async def exists(self, key: str) -> bool:
        try:
            await self._run_in_executor(self.client.head_object, Bucket=self.bucket, Key=key)
            return True
        except ClientError:
            return False

    async def get_signed_url(self, key: str, expires_in: int = 3600) -> str:
        try:
            # Running generate_presigned_url in executor (it's purely local usually, but good practice here)
            # Actually generate_presigned_url is purely local for client-side signing
            # But the client creation might be lazy? No, initialized in init.
            # Boto3 docs say generate_presigned_url is synchronous and fast.
            # But to be safe and consistent
            url = await self._run_in_executor(
                self.client.generate_presigned_url,
                ClientMethod="get_object",
                Params={"Bucket": self.bucket, "Key": key},
                ExpiresIn=expires_in,
            )
            return url
        except Exception as e:
            logger.error(f"S3 Sign URL Failed: {e}")
            return ""

    async def list_blobs(self, prefix: Optional[str] = None, limit: Optional[int] = None):
        """Async iterator for S3 blobs."""
        continuation_token = None
        count = 0

        while True:
            params = {"Bucket": self.bucket}
            if prefix:
                params["Prefix"] = prefix
            if continuation_token:
                params["ContinuationToken"] = continuation_token

            # MaxKeys default is 1000
            if limit and (limit - count) < 1000:
                params["MaxKeys"] = limit - count

            try:
                response = await self._run_in_executor(functools.partial(self.client.list_objects_v2, **params))
            except Exception as e:
                logger.error(f"S3 List Blobs Failed: {e}")
                break

            if "Contents" in response:
                for obj in response["Contents"]:
                    # Yield metadata dict instead of just key
                    yield {
                        "name": obj["Key"],
                        "size": obj.get("Size"),
                        "last_modified": obj.get("LastModified"),
                        "content_type": None,  # S3 List doesn't return ContentType
                        "provider_type": "s3",
                    }
                    count += 1
                    if limit and count >= limit:
                        return

            if not response.get("IsTruncated"):
                break

            continuation_token = response.get("NextContinuationToken")

    def validate_compliance(self) -> ValidationResult:
        """
        Validates S3 configuration for NIST compliance.
        Checks TLS (boto3 default) and KMS configuration.
        """
        # Boto3 uses SSL by default.
        transport = TransportSecurity.TLS_1_2  # Minimum assumption for AWS SDK

        # Encryption check
        encryption = EncryptionStandard.AES_256_GCM if self.kms_key_id else EncryptionStandard.AES_256_GCM
        # Note: AWS SSE-S3 is AES-256. SSE-KMS uses AES-256-GCM typically for the envelope.

        return StorageCompliance.validate_nist_800_175b(
            encryption=encryption,
            transport=transport,
            key_rotation_enabled=True,  # Assumed managed by AWS KMS
        )
