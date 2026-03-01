from src.shared.observability.logger_factory import get_logger
import uuid
from typing import Any, Optional

try:
    from qdrant_client import AsyncQdrantClient
    from qdrant_client.models import (
        Distance,
        FieldCondition,
        Filter,
        MatchValue,
        PointIdsList,
        PointStruct,
        VectorParams,
    )
except ImportError:
    AsyncQdrantClient = None  # type: ignore
    PointStruct = None  # type: ignore
    VectorParams = None  # type: ignore
    Distance = None  # type: ignore
    Filter = None  # type: ignore
    FieldCondition = None  # type: ignore
    MatchValue = None  # type: ignore
    PointIdsList = None  # type: ignore

from src.shared.services.storage.config import VectorStoreConfig, VectorDistanceMetric
from src.shared.services.storage.interfaces.vector import IVectorProvider

logger = get_logger(__name__)


class QdrantVectorProvider(IVectorProvider):
    """
    Qdrant implementation of IVectorProvider using qdrant-client.
    """

    def __init__(self, config: VectorStoreConfig):
        if AsyncQdrantClient is None:
            raise ImportError("qdrant-client is required for QdrantVectorProvider")

        self.config = config
        self._url = f"http://{config.host}:{config.port}"
        # Handle API key if necessary
        self._api_key = config.api_key.get_secret_value() if config.api_key else None

        self._client: Optional[AsyncQdrantClient] = None

    async def _get_client(self) -> AsyncQdrantClient:
        """Lazy initialization of the Qdrant client."""
        if self._client:
            return self._client

        self._client = AsyncQdrantClient(url=self._url, api_key=self._api_key)
        return self._client

    def _map_metric(self, metric: VectorDistanceMetric) -> Any:
        # Convert config metric to Qdrant Distance enum
        if metric == VectorDistanceMetric.COSINE:
            return Distance.COSINE
        if metric == VectorDistanceMetric.L2:
            return Distance.EUCLID
        if metric == VectorDistanceMetric.IP:
            return Distance.DOT
        return Distance.COSINE

    def _to_uuid(self, doc_id: str) -> str:
        """Convert string ID to deterministic UUID."""
        try:
            return str(uuid.UUID(doc_id))
        except ValueError:
            return str(uuid.uuid5(uuid.NAMESPACE_DNS, doc_id))

    async def create_collection(self, name: str, dimension: int, **kwargs) -> None:
        """Create vector collection/index."""
        client = await self._get_client()

        metric = kwargs.get("metric", self.config.metric)
        if isinstance(metric, str):
            # Map string to Enum if needed, but _map_metric expects Enum
            try:
                metric = VectorDistanceMetric(metric)
            except ValueError:
                metric = VectorDistanceMetric.COSINE

        params = VectorParams(size=dimension, distance=self._map_metric(metric))

        try:
            exists = await client.collection_exists(collection_name=name)
            if not exists:
                await client.create_collection(collection_name=name, vectors_config=params)
        except Exception as e:
            logger.error(f"Failed to create Qdrant collection {name}: {e}")
            raise

    async def upsert_vectors(self, collection: str, vectors: list[dict[str, Any]]) -> None:
        """Upsert vectors with metadata."""
        client = await self._get_client()

        points = []
        for vec_data in vectors:
            doc_id = vec_data.get("id")
            if not doc_id:
                continue

            point_id = self._to_uuid(doc_id)
            vector = vec_data.get("vector")

            if not vector:
                # Require vector for indexing
                logger.warning(f"Document {doc_id} missing vector embedding. Skipping.")
                continue

            content = vec_data.get("content", "")
            metadata = vec_data.get("metadata", {})

            # Payload construction
            payload = {"original_id": doc_id, "content": content, **metadata}

            points.append(PointStruct(id=point_id, vector=vector, payload=payload))

        if points:
            # Check if collection exists, if not try to create (auto-recovery)
            if not await client.collection_exists(collection_name=collection):
                # We need dimension to create collection. Assume it matches first vector?
                dim = len(points[0].vector)
                await self.create_collection(collection, dim)

            await client.upsert(collection_name=collection, points=points)

    async def search(self, collection: str, query_vector: list[float], limit: int = 10) -> list[dict[str, Any]]:
        """Search for similar vectors."""
        client = await self._get_client()

        # Handle filter if any (kwargs? interface doesn't have filters in signature yet)
        # But for now, just basic search

        query_filter = None
        # if filters: ...

        try:
            results = await client.search(
                collection_name=collection,
                query_vector=query_vector,
                limit=limit,
                query_filter=query_filter,
                with_payload=True,
                with_vectors=False,
            )
        except Exception as e:
            # Handle missing collection gracefully?
            logger.warning(f"Search failed for collection {collection}: {e}")
            return []

        search_results = []
        for res in results:
            payload = res.payload or {}
            original_id = str(payload.get("original_id", str(res.id)))
            content = str(payload.get("content", ""))

            # Remove content/original_id from metadata to avoid duplication
            metadata = {k: v for k, v in payload.items() if k not in ["content", "original_id"]}

            search_results.append({"id": original_id, "score": res.score, "content": content, "metadata": metadata})

        return search_results

    async def delete(self, collection: str, ids: list[str]) -> None:
        """Delete vectors by ID."""
        client = await self._get_client()
        uuids = [self._to_uuid(did) for did in ids]

        if uuids:
            await client.delete(collection_name=collection, points_selector=PointIdsList(points=uuids))
