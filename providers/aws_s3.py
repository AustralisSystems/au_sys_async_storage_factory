import sys
from typing import Optional, Union, AsyncIterator, Any
import aioboto3
from botocore.exceptions import ClientError

from storage.shared.observability.logger_factory import get_component_logger
from storage.interfaces.base_blob_provider import BaseBlobProvider
from storage.security.compliance import (
    EncryptionStandard,
    TransportSecurity,
    ValidationResult,
    StorageCompliance,
)

# Force UTF-8 stdout encoding for Python CLIs
if sys.stdout.encoding != "utf-8" and hasattr(sys.stdout, "reconfigure"):
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
