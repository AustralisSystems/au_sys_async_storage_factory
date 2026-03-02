from __future__ import annotations

"""
Qdrant Vector Provider - ENTERPRISE GRADE.

Implements IVectorProvider using qdrant-client for native async support.
"""

import json
import uuid
import logging
from datetime import datetime, UTC
from typing import Any, Optional, Dict, List, Tuple

try:
    from qdrant_client import AsyncQdrantClient
    from qdrant_client.models import (
        Distance,
        PointIdsList,
        PointStruct,
        VectorParams,
    )

    HAS_QDRANT = True
except ImportError:
    HAS_QDRANT = False

from ..interfaces.base_vector_provider import IVectorProvider
from ..interfaces.sync import ISyncProvider, SyncResult, SyncDirection, SyncConflictResolution
from ..interfaces.health import IHealthCheck, HealthMonitor
from ..interfaces.storage import StorageError

logger = logging.getLogger(__name__)


class QdrantVectorProvider(IVectorProvider, ISyncProvider, IHealthCheck):
    """
    Qdrant implementation of IVectorProvider using qdrant-client.
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 6333,
        api_key: Optional[str] = None,
        prefer_grpc: bool = False,
    ):
        self.host = host
        self.port = port
        self.api_key = api_key
        self.prefer_grpc = prefer_grpc
        self._url = f"http://{host}:{port}"
        self._client: Optional[AsyncQdrantClient] = None
        self._health_monitor = HealthMonitor()

    async def _get_client(self) -> AsyncQdrantClient:
        if not self._client:
            if not HAS_QDRANT:
                raise ImportError("qdrant-client is required for QdrantVectorProvider")
            self._client = AsyncQdrantClient(url=self._url, api_key=self.api_key, prefer_grpc=self.prefer_grpc)
        return self._client

    def _to_uuid(self, doc_id: str) -> str:
        try:
            return str(uuid.UUID(doc_id))
        except ValueError:
            return str(uuid.uuid5(uuid.NAMESPACE_DNS, doc_id))

    async def initialize(self) -> None:
        client = await self._get_client()
        try:
            # Simple health check call
            await client.get_collections()
            self._health_monitor.update_health(True)
            logger.info(f"QdrantVectorProvider initialized at {self._url}")
        except Exception as e:
            self._health_monitor.update_health(False, {"error": str(e)})
            raise StorageError(f"Failed to connect to Qdrant: {e}")

    async def create_index(self, name: str, dimension: int, distance_metric: str = "cosine", **kwargs: Any) -> bool:
        client = await self._get_client()
        metric_str = distance_metric.upper()

        distance = Distance.COSINE
        if metric_str == "EUCLIDEAN":
            distance = Distance.EUCLID
        elif metric_str == "DOT":
            distance = Distance.DOT

        params = VectorParams(size=dimension, distance=distance)
        try:
            if not await client.collection_exists(collection_name=name):
                await client.create_collection(collection_name=name, vectors_config=params)
            return True
        except Exception as e:
            logger.error(f"Failed to create Qdrant collection {name}: {e}")
            return False

    async def upsert(
        self,
        index_name: str,
        vectors: list[list[float]],
        metadata: list[dict[str, Any]],
        ids: Optional[list[str]] = None,
    ) -> bool:
        client = await self._get_client()
        points = []
        for i, vector in enumerate(vectors):
            doc_id = ids[i] if ids and i < len(ids) else str(uuid.uuid4())
            meta = metadata[i] if i < len(metadata) else {}
            payload = {
                "original_id": doc_id,
                "content": meta.get("content", ""),
                "metadata_json": json.dumps(meta),
            }
            points.append(PointStruct(id=self._to_uuid(doc_id), vector=vector, payload=payload))

        if not points:
            return True
        try:
            await client.upsert(collection_name=index_name, points=points)
            return True
        except Exception as e:
            logger.error(f"Qdrant upsert failed: {e}")
            return False

    async def search(
        self,
        index_name: str,
        query_vector: list[float],
        limit: int = 10,
        filters: Optional[dict[str, Any]] = None,
    ) -> list[dict[str, Any]]:
        client = await self._get_client()
        try:
            response = await client.query_points(
                collection_name=index_name, query=query_vector, limit=limit, with_payload=True
            )
            search_results = []
            for res in response.points:
                payload = res.payload or {}
                meta = json.loads(payload.get("metadata_json", "{}"))
                search_results.append(
                    {
                        "id": payload.get("original_id", str(res.id)),
                        "score": res.score,
                        "content": payload.get("content", ""),
                        "metadata": meta,
                    }
                )
            return search_results
        except Exception as e:
            logger.error(f"Qdrant search failed: {e}")
            return []

    async def delete(self, index_name: str, ids: list[str]) -> bool:
        client = await self._get_client()
        uuids = [self._to_uuid(did) for did in ids]
        try:
            await client.delete(  # nosec B608
                collection_name=index_name,
                points_selector=PointIdsList(points=uuids),  # type: ignore[arg-type]
            )
            return True
        except Exception as e:
            logger.error(f"Qdrant delete failed: {e}")
            return False

    async def get_index_stats(self, index_name: str) -> dict[str, Any]:
        client = await self._get_client()
        try:
            info = await client.get_collection(collection_name=index_name)
            return {
                "name": index_name,
                "vectors_count": info.indexed_vectors_count,
                "status": str(info.status),
            }
        except Exception as e:
            logger.error(f"Qdrant get_index_stats failed for {index_name}: {e}")
            return {"name": index_name, "error": str(e)}

    async def drop_index(self, index_name: str) -> bool:
        client = await self._get_client()
        try:
            await client.delete_collection(collection_name=index_name)
            return True
        except Exception as e:
            logger.error(f"Qdrant drop_index failed for {index_name}: {e}")
            return False

    # --- IStorageProvider Implementation ---
    async def get_async(self, key: str) -> Optional[Any]:
        return None

    async def set_async(self, key: str, value: Any) -> bool:
        return False

    async def delete_async(self, key: str) -> bool:
        return False

    async def exists_async(self, key: str) -> bool:
        return False

    async def list_keys_async(self, pattern: Optional[str] = None) -> list[str]:
        return []

    async def find_async(self, query: dict[str, Any]) -> list[Any]:
        return []

    async def clear_async(self) -> int:
        return 0

    def supports_ttl(self) -> bool:
        return False

    # --- IBackupProvider Implementation ---
    async def create_backup(self, backup_path: str, metadata: Optional[dict[str, Any]] = None) -> bool:
        logger.warning("QdrantVectorProvider.create_backup: Qdrant snapshots not yet implemented via this provider.")
        return False

    async def restore_backup(self, backup_path: str, clear_existing: bool = False) -> bool:
        logger.warning("QdrantVectorProvider.restore_backup: Qdrant restore not yet implemented via this provider.")
        return False

    async def list_backups(self, backup_dir: str) -> dict[str, dict[str, Any]]:
        return {}

    async def validate_backup(self, backup_path: str) -> dict[str, Any]:
        return {"valid": False, "error": "Backup validation not supported for Qdrant via this provider."}

    # --- ISyncProvider Implementation ---
    async def sync_to(
        self,
        target_provider: Any,
        direction: SyncDirection = SyncDirection.TO_TARGET,
        conflict_resolution: SyncConflictResolution = SyncConflictResolution.NEWEST_WINS,
        dry_run: bool = False,
    ) -> SyncResult:
        return SyncResult()

    def get_sync_metadata(self) -> dict[str, Any]:
        return {"provider": "qdrant"}

    async def prepare_for_sync(self) -> bool:
        return True

    async def cleanup_after_sync(self, sync_result: SyncResult) -> None:
        pass

    async def get_data_for_sync(self, last_sync_timestamp: Optional[datetime] = None) -> list[dict[str, Any]]:
        return []

    async def apply_sync_data(
        self,
        sync_data: list[dict[str, Any]],
        conflict_resolution: SyncConflictResolution = SyncConflictResolution.NEWEST_WINS,
    ) -> tuple[int, list[dict[str, Any]]]:
        return 0, []

    # --- IHealthCheck Implementation ---
    def is_healthy(self) -> bool:
        return self._health_monitor.is_healthy()

    async def perform_deep_health_check(self) -> bool:
        try:
            client = await self._get_client()
            await client.get_collections()
            return True
        except Exception:
            return False

    def get_health_status(self) -> dict[str, Any]:
        return self._health_monitor.get_health_status()

    def get_last_health_check(self) -> datetime:
        return self._health_monitor.get_last_health_check()
