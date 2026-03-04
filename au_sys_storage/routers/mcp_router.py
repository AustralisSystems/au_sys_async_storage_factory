from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Request
from pydantic import BaseModel

from ..mcp_server import get_mcp_storage_server

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/mcp", tags=["mcp"])


class MCPJsonRPCRequest(BaseModel):
    jsonrpc: str = "2.0"
    id: int | str | None = None
    method: str
    params: dict[str, Any] | None = None


class MCPJsonRPCResponse(BaseModel):
    jsonrpc: str = "2.0"
    id: int | str | None = None
    result: dict[str, Any] | None = None
    error: dict[str, Any] | None = None


@router.get("/health")
async def mcp_health() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/tools")
async def list_tools() -> dict[str, Any]:
    """List all available MCP tools (non-JSON-RPC compatible endpoint)."""
    server = get_mcp_storage_server()
    tools = server.get_tools()
    return {
        "tools": [
            {
                "name": tool.name,
                "description": tool.description,
                "inputSchema": tool.inputSchema,
            }
            for tool in tools
        ]
    }


@router.post("/tools/call")
async def call_tool(request: MCPJsonRPCRequest) -> MCPJsonRPCResponse:
    """Call an MCP tool via JSON-RPC 2.0."""
    server = get_mcp_storage_server()

    if request.method != "tools/call":
        return MCPJsonRPCResponse(
            id=request.id,
            error={"code": -32601, "message": f"Method not found: {request.method}"},
        )

    if not request.params:
        return MCPJsonRPCResponse(
            id=request.id,
            error={"code": -32602, "message": "Missing params"},
        )

    tool_name = request.params.get("name")
    arguments = request.params.get("arguments", {})

    if not tool_name:
        return MCPJsonRPCResponse(
            id=request.id,
            error={"code": -32602, "message": "Missing tool name"},
        )

    try:
        result = await server.call_tool(tool_name, arguments)
        return MCPJsonRPCResponse(id=request.id, result=result)
    except Exception as e:
        logger.error(f"MCP tool call error: {e}")
        return MCPJsonRPCResponse(
            id=request.id,
            error={"code": -32000, "message": str(e)},
        )


@router.post("")
async def mcp_jsonrpc(request: MCPJsonRPCRequest) -> MCPJsonRPCResponse:
    """Generic JSON-RPC 2.0 endpoint for MCP protocol."""
    method = request.method

    if method == "tools/list":
        server = get_mcp_storage_server()
        tools = server.get_tools()
        return MCPJsonRPCResponse(
            id=request.id,
            result={
                "tools": [
                    {
                        "name": tool.name,
                        "description": tool.description,
                        "inputSchema": tool.inputSchema,
                    }
                    for tool in tools
                ]
            },
        )

    if method == "tools/call":
        return await call_tool(request)

    return MCPJsonRPCResponse(
        id=request.id,
        error={"code": -32601, "message": f"Method not found: {method}"},
    )
