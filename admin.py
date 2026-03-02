from typing import Any, Dict, List, Optional, Union
from datetime import datetime, UTC
import asyncio

from storage.interfaces.storage import IStorageProvider
from storage.interfaces.base_document_provider import IDocumentProvider
from storage.interfaces.base_relational_provider import IRelationalProvider
from storage.interfaces.base_vector_provider import IVectorProvider
from storage.interfaces.base_graph_provider import IGraphProvider
from storage.interfaces.base_blob_provider import BaseBlobProvider
from storage.interfaces.health import IHealthCheck
from storage.shared.observability.logger_factory import get_component_logger

logger = get_component_logger("storage", "admin")

try:
    from sqladmin import Admin, ModelView

    HAS_SQLADMIN = True
except ImportError:
    HAS_SQLADMIN = False


class StorageAdminPortal:
    """
    Programmatic Admin Portal for Storage Factory.
    Provides lifecycle management, health monitoring, and sync orchestration.
    """

    def __init__(self, providers: dict[str, IStorageProvider]):
        self.providers = providers
        self._admin_ui: Optional[Any] = None

    async def initialize_all(self) -> None:
        """Initialize all registered providers."""
        tasks = []
        for name, provider in self.providers.items():
            if hasattr(provider, "initialize"):
                logger.info(f"Initializing provider: {name}")
                # Use type check to satisfy LSP if possible, or just ignore
                tasks.append(provider.initialize())  # type: ignore
        if tasks:
            await asyncio.gather(*tasks)

    async def get_all_health(self) -> dict[str, Any]:
        """Check health across all configured providers."""
        health_results = {}
        for name, provider in self.providers.items():
            try:
                if isinstance(provider, IHealthCheck):
                    is_healthy = await provider.perform_deep_health_check()
                    status = provider.get_health_status()
                    health_results[name] = {
                        "healthy": is_healthy,
                        "details": status,
                        "timestamp": datetime.now(UTC).isoformat(),
                    }
                elif hasattr(provider, "is_healthy"):
                    health_results[name] = {"healthy": provider.is_healthy()}  # type: ignore
                else:
                    health_results[name] = {"status": "missing_health_check_interface"}
            except Exception as e:
                logger.error(f"Health check failed for {name}: {e}")
                health_results[name] = {"healthy": False, "error": str(e)}

        return health_results

    async def trigger_sync(self, source_name: str, target_name: str) -> dict[str, Any]:
        """Trigger a manual sync between providers if supported."""
        source = self.providers.get(source_name)
        target = self.providers.get(target_name)

        if not source or not target:
            return {"success": False, "error": f"Invalid provider names: {source_name} or {target_name}"}

        if hasattr(source, "sync_to"):
            try:
                result = await source.sync_to(target)
                return {"success": True, "result": result}
            except Exception as e:
                logger.exception(f"Sync failed from {source_name} to {target_name}")
                return {"success": False, "error": str(e)}

        return {"success": False, "error": f"Sync not supported by source provider {source_name}"}

    def mount_to_fastapi(self, app: Any, engine: Any) -> bool:
        """
        Mounts the SQLAdmin UI to a FastAPI application.
        Provides a visual dashboard for SQLAlchemy-based providers.
        """
        if not HAS_SQLADMIN:
            logger.warning("sqladmin not installed. Visual Admin Portal disabled.")
            return False

        try:
            self._admin_ui = Admin(app, engine)
            # Future: Auto-register ModelViews for discovered models
            logger.info("SQLAdmin portal mounted successfully.")
            return True
        except Exception as e:
            logger.error(f"Failed to mount SQLAdmin: {e}")
            return False

    def list_providers(self) -> list[str]:
        """List all active storage providers managed by the factory."""
        return list(self.providers.keys())

    async def create_all_backups(self, backup_dir: str) -> dict[str, bool]:
        """Trigger backups for all supporting providers."""
        results = {}
        for name, provider in self.providers.items():
            if hasattr(provider, "create_backup"):
                try:
                    res = await provider.create_backup(backup_dir)
                    results[name] = bool(res)
                except Exception as e:
                    logger.error(f"Backup failed for {name}: {e}")
                    results[name] = False
        return results
