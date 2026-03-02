"""
Shared dependencies for storage routers.
"""

from storage.admin_portal import AdminPortalService, get_admin_portal_service
from storage.factory import AsyncStorageFactory, get_storage_factory
from storage.interfaces.base_blob_provider import BaseBlobProvider
from storage.interfaces.base_document_provider import IDocumentProvider
from storage.interfaces.base_graph_provider import IGraphProvider
from storage.interfaces.base_vector_provider import IVectorProvider
from storage.interfaces.storage import IStorageProvider
from storage.shared.services.data import get_data_manager_async
from storage.shared.services.data.data_manager import DataManager


def get_factory() -> AsyncStorageFactory:
    """Get the AsyncStorageFactory singleton."""
    return get_storage_factory()


def get_admin_service() -> AdminPortalService:
    """Get the AdminPortalService singleton."""
    return get_admin_portal_service()


async def get_data_manager() -> DataManager:
    """Get the DataManager instance."""
    return await get_data_manager_async()


async def get_relational_provider() -> IStorageProvider:
    """Get the default relational storage provider."""
    factory = get_storage_factory()
    return await factory.get_relational()


async def get_document_provider() -> IDocumentProvider:
    """Get the default document storage provider."""
    factory = get_storage_factory()
    return await factory.get_document()


async def get_blob_provider() -> BaseBlobProvider:
    """Get the default blob storage provider."""
    factory = get_storage_factory()
    return await factory.get_blob()


async def get_vector_provider() -> IVectorProvider:
    """Get the default vector storage provider."""
    factory = get_storage_factory()
    return await factory.get_vector()


async def get_graph_provider() -> IGraphProvider:
    """Get the default graph storage provider."""
    factory = get_storage_factory()
    return await factory.get_graph()
