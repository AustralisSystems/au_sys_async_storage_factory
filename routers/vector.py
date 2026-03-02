"""
FastAPI router for Vector Storage operations.
"""

from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status

from storage.interfaces.base_vector_provider import IVectorProvider
from storage.routers._deps import get_vector_provider

router = APIRouter()


@router.post("/vector/index/{name}", status_code=status.HTTP_201_CREATED)
async def create_vector_index(
    name: str,
    dimension: int,
    distance_metric: str = "cosine",
    options: Optional[dict[str, Any]] = None,
    provider: IVectorProvider = Depends(get_vector_provider),
) -> dict[str, Any]:
    """Create or initialize a vector index/collection."""
    kwargs = options or {}
    success = await provider.create_index(name, dimension, distance_metric=distance_metric, **kwargs)
    return {"success": success, "index_name": name}


@router.post("/vector/upsert/{index_name}")
async def upsert_vectors(
    index_name: str,
    vectors: list[list[float]],
    metadata: list[dict[str, Any]],
    ids: Optional[list[str]] = None,
    provider: IVectorProvider = Depends(get_vector_provider),
) -> dict[str, Any]:
    """Insert or update vectors with associated metadata."""
    if len(vectors) != len(metadata):
        raise HTTPException(status_code=400, detail="Vectors and metadata lists must have the same length")
    if ids and len(ids) != len(vectors):
        raise HTTPException(status_code=400, detail="IDs list must have the same length as vectors")

    success = await provider.upsert(index_name, vectors, metadata, ids=ids)
    return {"success": success, "count": len(vectors)}


@router.post("/vector/search/{index_name}")
async def search_vectors(
    index_name: str,
    query_vector: list[float],
    limit: int = Query(10, ge=1, le=100),
    filters: Optional[dict[str, Any]] = None,
    provider: IVectorProvider = Depends(get_vector_provider),
) -> list[dict[str, Any]]:
    """Search for top-K similar vectors."""
    return await provider.search(index_name, query_vector, limit=limit, filters=filters)


@router.delete("/vector/index/{index_name}/{ids}")
async def delete_vectors(
    index_name: str,
    ids: str,
    provider: IVectorProvider = Depends(get_vector_provider),
) -> dict[str, Any]:
    """Delete vectors by comma-separated IDs."""
    id_list = ids.split(",")
    success = await provider.delete(index_name, id_list)
    return {"success": success, "deleted_ids": id_list}


@router.get("/vector/index/{index_name}/stats")
async def get_index_stats(
    index_name: str,
    provider: IVectorProvider = Depends(get_vector_provider),
) -> dict[str, Any]:
    """Get statistics about the index."""
    return await provider.get_index_stats(index_name)


@router.delete("/vector/index/{index_name}")
async def drop_index(
    index_name: str,
    provider: IVectorProvider = Depends(get_vector_provider),
) -> dict[str, Any]:
    """Permanently delete an index."""
    success = await provider.drop_index(index_name)
    return {"success": success, "index_name": index_name}


@router.post("/vector/backup")
async def create_vector_backup(
    backup_path: str,
    metadata: Optional[dict[str, Any]] = None,
    provider: IVectorProvider = Depends(get_vector_provider),
) -> dict[str, Any]:
    """Create a backup of the vector storage."""
    success = await provider.create_backup(backup_path, metadata=metadata)
    return {"success": success, "backup_path": backup_path}


@router.post("/vector/restore")
async def restore_vector_backup(
    backup_path: str,
    clear_existing: bool = False,
    provider: IVectorProvider = Depends(get_vector_provider),
) -> dict[str, Any]:
    """Restore vector storage from a backup."""
    success = await provider.restore_backup(backup_path, clear_existing=clear_existing)
    return {"success": success, "backup_path": backup_path}
