import json
from src.shared.observability.logger_factory import get_logger
from typing import Any, Optional

try:
    from redis.asyncio import Redis
    from redisvl.index import AsyncSearchIndex
    from redisvl.query import FilterQuery, VectorQuery
    from redisvl.query.filter import Tag
    from redisvl.schema import IndexSchema
except ImportError:
    AsyncSearchIndex = None  # type: ignore
    IndexSchema = None  # type: ignore
    VectorQuery = None  # type: ignore
    FilterQuery = None  # type: ignore
    Tag = None  # type: ignore
    Redis = None  # type: ignore

from src.shared.services.storage.config import VectorStoreConfig, VectorDistanceMetric
from src.shared.services.storage.interfaces.vector import IVectorProvider

logger = get_logger(__name__)


class RedisVectorProvider(IVectorProvider):
    """
    Redis implementation of IVectorProvider using redisvl.
    """

    def __init__(self, config: VectorStoreConfig):
        if AsyncSearchIndex is None:
            raise ImportError("redisvl and redis are required for RedisVectorProvider")

        self.config = config
        self._url = f"redis://{config.host}:{config.port}"
        if config.password:
            self._url = f"redis://:{config.password.get_secret_value()}@{config.host}:{config.port}"
        elif config.api_key:
            # Fallback if api_key is used as password
            self._url = f"redis://:{config.api_key.get_secret_value()}@{config.host}:{config.port}"

        self._indices: dict[str, AsyncSearchIndex] = {}

    async def _get_index(self, collection: str) -> Optional[AsyncSearchIndex]:
        return self._indices.get(collection)

    async def create_collection(self, name: str, dimension: int, **kwargs) -> None:
        """Create vector collection/index."""
        # Define schema with JSON metadata support
        metric = kwargs.get("metric", self.config.metric)
        if isinstance(metric, VectorDistanceMetric):
            metric = metric.value

        schema_dict = {
            "index": {
                "name": name,
                "prefix": name,
            },
            "fields": [
                {"name": "id", "type": "tag"},
                {"name": "content", "type": "text"},
                {"name": "metadata_json", "type": "text"},  # Serialize arbitrary metadata
                {
                    "name": "vector",
                    "type": "vector",
                    "attrs": {
                        "dims": dimension,
                        "distance_metric": metric,
                        "algorithm": "flat",  # Default to flat for exact search
                        "datatype": "float32",
                    },
                },
            ],
        }

        schema = IndexSchema.from_dict(schema_dict)
        index = AsyncSearchIndex(schema, redis_url=self._url)

        # Create index if it doesn't exist
        try:
            exists = await index.exists()
            if not exists:
                await index.create()
        except Exception as e:
            logger.error(f"Failed to create Redis index {name}: {e}")
            raise

        self._indices[name] = index

    async def upsert_vectors(self, collection: str, vectors: list[dict[str, Any]]) -> None:
        """Upsert vectors with metadata."""
        idx = await self._get_index(collection)
        if not idx:
            # Attempt to recover index if config matches
            if collection == self.config.collection_name:
                await self.create_collection(collection, self.config.dimension)
                idx = await self._get_index(collection)

            if not idx:
                raise ValueError(f"Collection {collection} not found. Call create_collection first.")

        records = []
        for vec_data in vectors:
            # Flatten metadata into the record, but also store full metadata as JSON
            # Expects keys: id, vector, content (optional), metadata (optional dict)
            doc_id = vec_data.get("id")
            vector = vec_data.get("vector")
            content = vec_data.get("content", "")
            metadata = vec_data.get("metadata", {})

            # Merge top level keys that are not reserved into metadata if provided flat
            reserved = {"id", "vector", "content", "metadata"}
            extra_meta = {k: v for k, v in vec_data.items() if k not in reserved}
            metadata.update(extra_meta)

            record: dict[str, Any] = {"id": doc_id, "content": content, "metadata_json": json.dumps(metadata)}
            record.update(metadata)  # Flatten for potential separate indexing

            if vector:
                record["vector"] = vector

            records.append(record)

        await idx.load(records)

    async def search(self, collection: str, query_vector: list[float], limit: int = 10) -> list[dict[str, Any]]:
        """Search for similar vectors."""
        idx = await self._get_index(collection)
        if not idx:
            # Attempt recovery
            if collection == self.config.collection_name:
                await self.create_collection(collection, self.config.dimension)
                idx = await self._get_index(collection)

            if not idx:
                raise ValueError(f"Collection {collection} not found.")

        # Build filter expression (not supported in generic interface yet, but prepared)
        filter_expression = None
        # if filters and Tag: ...

        query = VectorQuery(
            vector=query_vector,
            vector_field_name="vector",
            return_fields=["id", "content", "metadata_json", "vector_distance"],
            num_results=limit,
            filter_expression=filter_expression,
        )

        results = await idx.query(query)

        search_results = []
        for res in results:
            score = float(res.get("vector_distance", 0.0))

            # Reconstruct metadata
            metadata = {}
            if "metadata_json" in res:
                try:
                    metadata = json.loads(res["metadata_json"])
                except json.JSONDecodeError:
                    logger.warning(f"Failed to decode metadata_json for doc {res.get('id')}")

            search_results.append(
                {"id": res.get("id"), "score": score, "content": res.get("content", ""), "metadata": metadata}
            )

        return search_results

    async def delete(self, collection: str, ids: list[str]) -> None:
        """Delete vectors by ID."""
        idx = await self._get_index(collection)
        if not idx:
            return

        client = idx.client
        keys = [idx.key(doc_id) for doc_id in ids]
        if keys:
            await client.delete(*keys)
