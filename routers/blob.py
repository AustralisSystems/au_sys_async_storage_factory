"""
FastAPI router for Blob Storage operations.
"""

import json
from typing import Any, Optional, Union

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import Response as FastAPIResponse

from storage.interfaces.base_blob_provider import BaseBlobProvider
from storage.routers._deps import get_blob_provider

router = APIRouter()


@router.get("/blob/list", response_model=list[dict[str, Any]])
async def list_blobs(
    prefix: Optional[str] = Query(None, description="Filter blobs by prefix"),
    limit: Optional[int] = Query(None, description="Limit the number of results"),
    provider: BaseBlobProvider = Depends(get_blob_provider),
) -> list[dict[str, Any]]:
    """List blobs with optional prefix and limit."""
    blobs = []
    async for blob in provider.list_blobs(prefix=prefix, limit=limit):
        blobs.append(blob)
    return blobs


@router.get("/blob/{key:path}")
async def download_blob(
    key: str,
    encryption_context_json: Optional[str] = Query(None, alias="encryption_context"),
    provider: BaseBlobProvider = Depends(get_blob_provider),
) -> FastAPIResponse:
    """Download a blob by key."""
    decryption_context = None
    if encryption_context_json:
        try:
            decryption_context = json.loads(encryption_context_json)
        except json.JSONDecodeError:
            raise HTTPException(status_code=400, detail="Invalid encryption_context JSON")

    data = await provider.download(key, decryption_context=decryption_context)
    if data is None:
        raise HTTPException(status_code=404, detail=f"Blob '{key}' not found")

    # We return the raw bytes. The caller handles content type.
    return FastAPIResponse(content=data, media_type="application/octet-stream")


@router.post("/blob/{key:path}", status_code=status.HTTP_201_CREATED)
async def upload_blob(
    key: str,
    data: Union[str, bytes],
    content_type: Optional[str] = Query(None),
    encryption_context_json: Optional[str] = Query(None, alias="encryption_context"),
    provider: BaseBlobProvider = Depends(get_blob_provider),
) -> dict[str, Any]:
    """Upload a blob with the given key."""
    encryption_context = None
    if encryption_context_json:
        try:
            encryption_context = json.loads(encryption_context_json)
        except json.JSONDecodeError:
            raise HTTPException(status_code=400, detail="Invalid encryption_context JSON")

    success = await provider.upload(key, data, content_type=content_type, encryption_context=encryption_context)
    return {"success": success, "key": key}


@router.delete("/blob/{key:path}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_blob(
    key: str,
    provider: BaseBlobProvider = Depends(get_blob_provider),
) -> None:
    """Delete a blob by key."""
    success = await provider.delete(key)
    if not success:
        raise HTTPException(status_code=404, detail=f"Blob '{key}' not found or could not be deleted")


@router.get("/blob/{key:path}/exists")
async def blob_exists(
    key: str,
    provider: BaseBlobProvider = Depends(get_blob_provider),
) -> dict[str, Any]:
    """Check if a blob exists."""
    exists = await provider.exists(key)
    return {"key": key, "exists": exists}


@router.get("/blob/{key:path}/signed-url")
async def get_blob_signed_url(
    key: str,
    expires_in: int = Query(3600, description="Expiration time in seconds"),
    provider: BaseBlobProvider = Depends(get_blob_provider),
) -> dict[str, Any]:
    """Generate a pre-signed URL for direct access."""
    url = await provider.get_signed_url(key, expires_in=expires_in)
    return {"key": key, "signed_url": url}


@router.post("/blob/backup")
async def create_blob_backup(
    backup_path: str,
    metadata: Optional[dict[str, Any]] = None,
    provider: BaseBlobProvider = Depends(get_blob_provider),
) -> dict[str, Any]:
    """Create a backup of the blob storage."""
    success = await provider.create_backup(backup_path, metadata=metadata)
    return {"success": success, "backup_path": backup_path}


@router.post("/blob/restore")
async def restore_blob_backup(
    backup_path: str,
    clear_existing: bool = False,
    provider: BaseBlobProvider = Depends(get_blob_provider),
) -> dict[str, Any]:
    """Restore blob storage from a backup."""
    success = await provider.restore_backup(backup_path, clear_existing=clear_existing)
    return {"success": success, "backup_path": backup_path}


@router.get("/blob/backups")
async def list_blob_backups(
    backup_dir: str,
    provider: BaseBlobProvider = Depends(get_blob_provider),
) -> dict[str, dict[str, Any]]:
    """List available blob backups."""
    return await provider.list_backups(backup_dir)


@router.get("/blob/compliance")
async def validate_blob_compliance(
    provider: BaseBlobProvider = Depends(get_blob_provider),
) -> dict[str, Any]:
    """Validate compliance posture of the blob provider."""
    compliance = provider.validate_compliance()
    return {"compliance": compliance}
