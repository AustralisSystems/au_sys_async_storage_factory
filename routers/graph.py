"""
FastAPI router for Graph Storage operations.
"""

from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, status

from storage.interfaces.base_graph_provider import IGraphProvider
from storage.routers._deps import get_graph_provider

router = APIRouter()


@router.post("/graph/node", status_code=status.HTTP_201_CREATED)
async def create_graph_node(
    labels: list[str],
    properties: dict[str, Any],
    provider: IGraphProvider = Depends(get_graph_provider),
) -> dict[str, Any]:
    """Create a graph node and return its ID."""
    node_id = await provider.create_node(labels, properties)
    return {"success": True, "node_id": node_id}


@router.post("/graph/relationship", status_code=status.HTTP_201_CREATED)
async def create_graph_relationship(
    from_id: str,
    to_id: str,
    rel_type: str,
    properties: dict[str, Any] = {},
    provider: IGraphProvider = Depends(get_graph_provider),
) -> dict[str, Any]:
    """Create a relationship between nodes."""
    rel_id = await provider.create_relationship(from_id, to_id, rel_type, properties)
    return {"success": True, "relationship_id": rel_id}


@router.post("/graph/query")
async def execute_graph_query(
    cypher: str,
    params: Optional[dict[str, Any]] = None,
    provider: IGraphProvider = Depends(get_graph_provider),
) -> list[dict[str, Any]]:
    """Execute a Cypher query against the graph storage."""
    return await provider.execute_query(cypher, params=params)


@router.delete("/graph/node/{node_id}")
async def delete_graph_node(
    node_id: str,
    provider: IGraphProvider = Depends(get_graph_provider),
) -> dict[str, Any]:
    """Delete a graph node by ID."""
    success = await provider.delete_node(node_id)
    if not success:
        raise HTTPException(status_code=404, detail=f"Node '{node_id}' not found or could not be deleted")
    return {"success": True, "node_id": node_id}


@router.post("/graph/backup")
async def create_graph_backup(
    backup_path: str,
    metadata: Optional[dict[str, Any]] = None,
    provider: IGraphProvider = Depends(get_graph_provider),
) -> dict[str, Any]:
    """Create a backup of the graph storage."""
    success = await provider.create_backup(backup_path, metadata=metadata)
    return {"success": success, "backup_path": backup_path}


@router.post("/graph/restore")
async def restore_graph_backup(
    backup_path: str,
    clear_existing: bool = False,
    provider: IGraphProvider = Depends(get_graph_provider),
) -> dict[str, Any]:
    """Restore graph storage from a backup."""
    success = await provider.restore_backup(backup_path, clear_existing=clear_existing)
    return {"success": success, "backup_path": backup_path}
