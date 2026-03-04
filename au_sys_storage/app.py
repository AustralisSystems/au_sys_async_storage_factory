from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from au_sys_storage.routers.storage_router import get_storage_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup logic
    yield
    # Shutdown logic


app = FastAPI(
    title="AU-SYS Storage Sovereign Capability",
    description="Unified Storage Module Sidecar for Database and Blob interactions.",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include the main storage router
app.include_router(get_storage_router())

# Include the MCP SSE endpoints
from au_sys_storage.mcp_server import mcp
app.mount("/mcp", mcp.sse_app)

@app.get("/healthz", tags=["health"])
async def health_check():
    """Simple health check endpoint."""
    return {"status": "healthy", "service": "au_sys_storage"}
