import os
import asyncio
from mcp.server.fastmcp import FastMCP
from motor.motor_asyncio import AsyncIOMotorClient

# Initialize FastMCP server
mcp = FastMCP("MongoDB Storage Connector")

# Configuration
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017/")
DATABASE_NAME = os.getenv("MONGODB_DATABASE", "ace_storage")

client = AsyncIOMotorClient(MONGODB_URI)
db = client[DATABASE_NAME]


@mcp.tool()
async def list_collections():
    """List all collections in the configured database."""
    collections = await db.list_collection_names()
    return collections


@mcp.tool()
async def query_documents(collection_name: str, query: dict, limit: int = 10):
    """Query documents from a specific collection."""
    cursor = db[collection_name].find(query).limit(limit)
    docs = await cursor.to_list(length=limit)
    # Convert ObjectIds to strings for JSON serialization
    for doc in docs:
        if "_id" in doc:
            doc["_id"] = str(doc["_id"])
    return docs


@mcp.tool()
async def insert_document(collection_name: str, document: dict):
    """Insert a document into a specific collection."""
    result = await db[collection_name].insert_one(document)
    return {"inserted_id": str(result.inserted_id)}


if __name__ == "__main__":
    mcp.run()
