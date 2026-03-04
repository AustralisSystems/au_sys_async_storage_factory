import os
from typing import Any

from mcp.server.fastmcp import FastMCP
from motor.motor_asyncio import AsyncIOMotorClient

# Initialize FastMCP server
mcp = FastMCP("MongoDB Storage Connector")

# Configuration
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017/")
DATABASE_NAME = os.getenv("MONGODB_DATABASE", "ace_storage")

client: AsyncIOMotorClient[Any] = AsyncIOMotorClient(MONGODB_URI)
db = client[DATABASE_NAME]


@mcp.tool()
async def list_collections() -> list[str]:
    """List all collections in the configured database."""
    collections = await db.list_collection_names()
    return list(collections)


@mcp.tool()
async def query_documents(collection_name: str, query: dict[str, Any], limit: int = 10) -> list[dict[str, Any]]:
    """Query documents from a specific collection."""
    cursor = db[collection_name].find(query).limit(limit)
    docs = await cursor.to_list(length=limit)
    # Convert ObjectIds to strings for JSON serialization
    for doc in docs:
        if "_id" in doc:
            doc["_id"] = str(doc["_id"])
    return docs


@mcp.tool()
async def insert_document(collection_name: str, document: dict[str, Any]) -> dict[str, Any]:
    """Insert a document into a specific collection."""
    result = await db[collection_name].insert_one(document)
    return {"inserted_id": str(result.inserted_id)}


if __name__ == "__main__":
    mcp.run()
