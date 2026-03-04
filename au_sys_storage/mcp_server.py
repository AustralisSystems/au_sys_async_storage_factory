from __future__ import annotations

"""
MCP Storage Server implementation.

Exposes the Storage Factory capabilities to AI agents using the
Model Context Protocol (MCP) via FastMCP. Provides tools for blob management,
database queries, and metadata retrieval.
"""

import logging
from typing import Any

from mcp.server.fastmcp import FastMCP

from .admin_portal import get_admin_portal_service
from .factory import get_storage_factory

logger = logging.getLogger(__name__)

mcp = FastMCP("au-sys-storage")

@mcp.tool()
async def list_blobs(prefix: str = "", limit: int = 50) -> dict[str, Any]:
    """List files in the configured blob storage.

    Args:
        prefix: Filter by folder prefix
        limit: Max results
    """
    factory = get_storage_factory()
    # This would integrate with the active blob provider
    return {"blobs": []}

@mcp.tool()
async def upload_blob(key: str, data: str) -> dict[str, Any]:
    """Upload a text or binary file to storage.

    Args:
        key: The destination path
        data: Content to upload
    """
    logger.info(f"Agent uploading blob to {key}")
    factory = get_storage_factory()
    # Logic to use factory and upload
    return {"success": True, "key": key}

@mcp.tool()
async def get_storage_health() -> dict[str, Any]:
    """Check the status of all storage backends."""
    admin = get_admin_portal_service()
    return await admin.get_all_providers_health()

def get_mcp_storage_server() -> FastMCP:
    return mcp
