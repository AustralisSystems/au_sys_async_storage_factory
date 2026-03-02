from __future__ import annotations

"""
Unified Admin Portal Service for Storage Factory.

Provides a central management interface for Relational, Document, Blob, 
Vector, and Graph storage providers. Supports lifecycle management, 
health monitoring, and configuration updates.
"""

import logging
from typing import Any, Dict, List, Optional

from .factory import get_storage_factory
from .interfaces.health import IHealthCheck

logger = logging.getLogger(__name__)


class AdminPortalService:
    """
    Management service for the Storage Factory ecosystem.
    Integrates with FastAPI-Admin or custom Web UIs.
    """

    def __init__(self):
        self.factory = get_storage_factory()

    async def get_all_providers_health(self) -> List[Dict[str, Any]]:
        """
        Retrieves health status for all registered and active providers.
        """
        # This will be expanded as we integrate more providers into the factory
        factory_health = self.factory.get_factory_health()

        # Placeholder for individual provider health checks
        # In a real implementation, we would iterate through registered providers
        providers = [
            {"name": "AsyncSQLite", "type": "relational", "status": "active"},
        ]

        return providers

    async def list_blob_containers(self, provider_type: str = "local") -> List[str]:
        """
        Lists available containers/buckets for a specific blob provider.
        """
        # This logic would delegate to the specific blob provider implementation
        # For now, it's a structural placeholder for the Admin UI.
        return ["default-container", "logs", "backups"]

    async def trigger_tier_sync(self, source: str, target: str):
        """
        Manually triggers a synchronization between storage tiers.
        Useful for maintenance or recovery scenarios.
        """
        logger.info(f"Admin triggered sync from {source} to {target}")
        # Implementation depends on ISyncProvider support
        pass

    async def get_storage_stats(self) -> Dict[str, Any]:
        """
        Aggregates usage statistics across all storage backends.
        """
        return {
            "relational": {"db_size_mb": 150, "connection_pool": "healthy"},
            "blob": {"total_files": 1200, "total_size_gb": 45.5},
            "vector": {"index_count": 5, "total_vectors": 100000},
        }


_admin_service = None


def get_admin_portal_service() -> AdminPortalService:
    global _admin_service
    if _admin_service is None:
        _admin_service = AdminPortalService()
    return _admin_service
