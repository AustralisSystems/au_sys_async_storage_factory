"""
FastAPI router for Relational Storage CRUD operations.
"""

from typing import Any, Dict, List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query, status

from storage.interfaces.storage import IStorageProvider
from storage.routers._deps import get_relational_provider

router = APIRouter()


@router.get("/relational/keys", response_model=list[str])
async def list_relational_keys(
    pattern: Optional[str] = None,
    provider: IStorageProvider = Depends(get_relational_provider),
) -> list[str]:
    """List all keys in relational storage."""
    return await provider.list_keys_async(pattern=pattern)


@router.get("/relational/{key}")
async def get_relational_value(
    key: str,
    provider: IStorageProvider = Depends(get_relational_provider),
) -> Any:
    """Retrieve a value by key from relational storage."""
    value = await provider.get_async(key)
    if value is None:
        raise HTTPException(status_code=404, detail=f"Key '{key}' not found")
    return value


@router.post("/relational/{key}", status_code=status.HTTP_201_CREATED)
async def set_relational_value(
    key: str,
    value: Any,
    provider: IStorageProvider = Depends(get_relational_provider),
) -> dict[str, Any]:
    """Store a value with the given key in relational storage."""
    success = await provider.set_async(key, value)
    return {"success": success, "key": key}


@router.delete("/relational/{key}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_relational_value(
    key: str,
    provider: IStorageProvider = Depends(get_relational_provider),
) -> None:
    """Delete a value by key from relational storage."""
    success = await provider.delete_async(key)
    if not success:
        raise HTTPException(status_code=404, detail=f"Key '{key}' not found or could not be deleted")


@router.get("/relational/{key}/exists")
async def relational_key_exists(
    key: str,
    provider: IStorageProvider = Depends(get_relational_provider),
) -> dict[str, Any]:
    """Check if a key exists in relational storage."""
    exists: bool = bool(await provider.exists_async(key))
    return {"key": key, "exists": exists}


@router.post("/relational/find")
async def find_relational_values(
    query: dict[str, Any],
    provider: IStorageProvider = Depends(get_relational_provider),
) -> list[Any]:
    """Find values matching a simple equality query in relational storage."""
    return await provider.find_async(query)


@router.post("/relational/clear")
async def clear_relational_storage(
    provider: IStorageProvider = Depends(get_relational_provider),
) -> dict[str, Any]:
    """Clear all data from relational storage."""
    count = await provider.clear_async()
    return {"success": True, "cleared_count": count}
