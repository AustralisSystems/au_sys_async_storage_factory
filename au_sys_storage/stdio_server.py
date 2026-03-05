from .mcp_server import get_mcp_storage_server


def main_sync():
    """Entry point for the au-sys-storage-mcp CLI command."""
    # FastMCP's .run() handles the stdio event loop automatically.
    mcp = get_mcp_storage_server()
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main_sync()
