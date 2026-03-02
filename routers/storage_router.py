"""
Main entry point for storage routers.
"""

from fastapi import APIRouter

from storage.routers.blob import router as blob_router
from storage.routers.data_manager import router as data_manager_router
from storage.routers.document import router as document_router
from storage.routers.factory import router as factory_router
from storage.routers.graph import router as graph_router
from storage.routers.relational import router as relational_router
from storage.routers.vector import router as vector_router


def get_storage_router() -> APIRouter:
    """Factory function to get the storage router."""
    main_router = APIRouter(prefix="/storage", tags=["storage"])

    # Include factory-level endpoints directly on /storage
    main_router.include_router(factory_router)

    # Include DataManager endpoints under /storage/data
    main_router.include_router(data_manager_router, prefix="/data", tags=["data-manager"])

    # Include specific backend routers
    main_router.include_router(relational_router, tags=["relational"])
    main_router.include_router(document_router, tags=["document"])
    main_router.include_router(blob_router, tags=["blob"])
    main_router.include_router(vector_router, tags=["vector"])
    main_router.include_router(graph_router, tags=["graph"])

    return main_router
