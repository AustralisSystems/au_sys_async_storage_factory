from src.shared.observability.logger_factory import get_logger
import time

from src.shared.services.storage.blob_factory import BlobStorageFactory
from src.shared.services.storage.connection_registry import get_connection_registry
from src.api.services.storage.schemas.blob_connection_schema import (
    BlobConnectionCreate,
    HealthCheckResult,
    ConnectionHealthStatus,
    StorageValidationResult,
)

logger = get_logger(__name__)


class LifecycleManager:
    """
    Manages the lifecycle of blob storage connections.
    Handles testing, health checks, and compliance validation.
    """

    def __init__(self):
        self.registry = get_connection_registry()

    async def test_connection(self, config: BlobConnectionCreate) -> HealthCheckResult:
        """
        Test a connection configuration by attempting a simple operation.
        Does not persist the connection.
        """
        start_time = time.time()
        try:
            # Create provider instance
            provider = BlobStorageFactory.create_provider_from_config(config)

            # Test listing blobs with a limit of 1 to verify authentication and permissions
            # We iterate over the async generator to trigger the API call
            async for _ in provider.list_blobs(limit=1):
                # We only need to see if we can get the first item to verify connection
                break

            duration = time.time() - start_time

            return HealthCheckResult(
                status=ConnectionHealthStatus.HEALTHY,
                message="Connection verified successfully",
                details={"latency_ms": round(duration * 1000, 2), "provider": config.provider_type},
            )

        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"Connection test failed for {config.name}: {e}")
            return HealthCheckResult(
                status=ConnectionHealthStatus.UNHEALTHY,
                message=str(e),
                details={"latency_ms": round(duration * 1000, 2), "error_type": type(e).__name__},
            )

    async def validate_connection(self, config: BlobConnectionCreate) -> StorageValidationResult:
        """
        Validate compliance of a connection configuration.
        """
        # Create provider to use its internal validation logic
        provider = BlobStorageFactory.create_provider_from_config(config)
        compliance = provider.validate_compliance()

        return StorageValidationResult(
            compliant=compliance.compliant,
            standards_met=compliance.standards_met,
            issues=compliance.issues,
            remediation_steps=compliance.remediation_steps,
        )

    async def check_connection_health(self, connection_id: str) -> HealthCheckResult:
        """
        Perform health check on a registered connection and update its status.
        """
        config = await self.registry.get_connection(connection_id)
        if not config:
            return HealthCheckResult(
                status=ConnectionHealthStatus.UNKNOWN, message=f"Connection {connection_id} not found"
            )

        result = await self.test_connection(config)

        # Update registry with result
        await self.registry.update_health_status(connection_id, result)

        return result


_lifecycle_manager = None


def get_lifecycle_manager() -> LifecycleManager:
    global _lifecycle_manager
    if _lifecycle_manager is None:
        _lifecycle_manager = LifecycleManager()
    return _lifecycle_manager
