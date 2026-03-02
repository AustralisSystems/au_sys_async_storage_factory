from __future__ import annotations

"""
MCP Storage Server implementation.

Exposes the Storage Factory capabilities to AI agents using the 
Model Context Protocol (MCP). Provides tools for blob management, 
database queries, and metadata retrieval.
"""

import asyncio
import logging
from typing import Any, Dict, List, Optional

from mcp.server.fastapi import Context, Resource
from mcp.server.models import Tool

from .factory import get_storage_factory
from .admin_portal import get_admin_portal_service

logger = logging.getLogger(__name__)


class MCPStorageServer:
    """
    Model Context Protocol (MCP) Server for the Storage Factory.
    Allows agents to browse and interact with storage tiers.
    """

    def __init__(self):
        self.factory = get_storage_factory()
        self.admin = get_admin_portal_service()

    def get_tools(self) -> List[Tool]:
        """
        Returns a list of MCP tools exposed by this server.
        """
        return [
            Tool(
                name="list_blobs",
                description="List files in the configured blob storage.",
                parameters={
                    "type": "object",
                    "properties": {
                        "prefix": {"type": "string", "description": "Filter by folder prefix"},
                        "limit": {"type": "integer", "description": "Max results"},
                    },
                },
            ),
            Tool(
                name="upload_blob",
                description="Upload a text or binary file to storage.",
                parameters={
                    "type": "object",
                    "properties": {
                        "key": {"type": "string", "description": "The destination path"},
                        "data": {"type": "string", "description": "Content to upload"},
                    },
                    "required": ["key", "data"],
                },
            ),
            Tool(
                name="get_storage_health",
                description="Check the status of all storage backends.",
                parameters={"type": "object", "properties": {}},
            ),
        ]

    async def call_tool(self, name: str, arguments: Dict[str, Any]) -> Any:
        """
        Executes a specific tool requested by the agent.
        """
        if name == "list_blobs":
            # This would integrate with the active blob provider
            return {"blobs": []}

        elif name == "upload_blob":
            key = arguments.get("key")
            data = arguments.get("data")
            logger.info(f"Agent uploading blob to {key}")
            # Logic to use factory and upload
            return {"success": True, "key": key}

        elif name == "get_storage_health":
            return await self.admin.get_all_providers_health()

        else:
            raise ValueError(f"Unknown tool: {name}")


_mcp_server = None


def get_mcp_storage_server() -> MCPStorageServer:
    global _mcp_server
    if _mcp_server is None:
        _mcp_server = MCPStorageServer()
    return _mcp_server
