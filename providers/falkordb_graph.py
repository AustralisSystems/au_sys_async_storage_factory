import asyncio
from src.shared.observability.logger_factory import get_logger
from typing import Any, Optional

try:
    from falkordb import FalkorDB
except ImportError:
    FalkorDB = None  # type: ignore

from src.shared.services.storage.config import GraphStoreConfig
from src.shared.services.storage.interfaces.graph import IGraphProvider

logger = get_logger(__name__)


class FalkorDBGraphProvider(IGraphProvider):
    """
    FalkorDB implementation of the IGraphProvider interface.
    """

    def __init__(self, config: GraphStoreConfig):
        if FalkorDB is None:
            raise ImportError("falkordb is required for FalkorDBGraphProvider")

        self.config = config
        self._client: Optional[FalkorDB] = None
        self._graph = None

    def _get_graph(self):
        """Lazy initialization of FalkorDB graph (sync)."""
        if self._graph:
            return self._graph

        # Connect
        password = self.config.password.get_secret_value() if self.config.password else None
        self._client = FalkorDB(host=self.config.host, port=self.config.port, password=password)

        self._graph = self._client.select_graph(self.config.database)
        return self._graph

    async def _run_query(self, query: str, params: Optional[dict[str, Any]] = None) -> Any:
        """Run Cypher query in a thread."""
        graph = self._get_graph()
        try:
            return await asyncio.to_thread(graph.query, query, params)
        except Exception as e:
            logger.error(f"FalkorDB query failed: {query} Error: {e}")
            raise

    async def create_node(self, labels: list[str], properties: dict[str, Any]) -> str:
        """Create node and return ID (from properties['id'] or internal ID)."""
        label_str = "".join([f":{label}" for label in labels])

        # Ensure ID is present if not provided?
        # Interface doesn't mandate 'id' in properties, but Knowledge Service relied on it.
        # We will return the property 'id' if exists, otherwise internal ID converted to string.

        query = f"CREATE (n{label_str} $props) RETURN n"
        result = await self._run_query(query, {"props": properties})

        if not result or not result.result_set or len(result.result_set) == 0:
            raise RuntimeError(f"Failed to create node with labels {labels}")

        raw_node = result.result_set[0][0]

        # Prefer user-provided ID
        if "id" in properties:
            return str(properties["id"])

        # Fallback to internal ID (available on raw_node.id)
        return str(raw_node.id)

    async def create_relationship(self, from_id: str, to_id: str, rel_type: str, properties: dict[str, Any]) -> str:
        """Create relationship between nodes."""
        query = f"MATCH (a), (b) WHERE (a.id = $start OR ID(a) = $start) AND (b.id = $end OR ID(b) = $end) CREATE (a)-[r:{rel_type} $props]->(b) RETURN r"

        # Attempt to handle both property-id and internal-id (integer) if start/end look like ints?
        # Safe bet: assume user uses property 'id'.
        # FalkorDBGraphAdapter implementation used: MATCH (a {id: $start}), (b {id: $end})
        # I should stick to that if 'id' is standard.
        # But 'from_id' might be internal ID if I returned it from create_node.
        # Let's assume property-based ID for consistency with Knowledge Service expectations.

        query = f"MATCH (a {{id: $start}}), (b {{id: $end}}) CREATE (a)-[r:{rel_type} $props]->(b) RETURN r"
        params = {"start": from_id, "end": to_id, "props": properties}

        result = await self._run_query(query, params)

        if not result or not result.result_set or len(result.result_set) == 0:
            # Fallback: maybe they are internal IDs?
            # It's risky to mix strategies. I'll stick to property ID which is safer for portability.
            raise ValueError(
                f"Could not create relationship. Nodes {from_id} or {to_id} might not exist (by property 'id')."
            )

        raw_rel = result.result_set[0][0]
        return str(raw_rel.properties.get("id", raw_rel.id))

    async def query(self, cypher: str, params: dict[str, Any] = None) -> list[dict[str, Any]]:
        """Execute Cypher query and return list of dicts."""
        result = await self._run_query(cypher, params)

        if not result or not hasattr(result, "header") or not hasattr(result, "result_set"):
            return []

        # Parse header
        try:
            header = []
            for h in result.header:
                name = h[1]
                if isinstance(name, bytes):
                    name = name.decode("utf-8")
                header.append(name)
        except (IndexError, TypeError):
            return []

        output = []
        for row in result.result_set:
            record = {}
            for i, val in enumerate(row):
                if i < len(header):
                    record[header[i]] = val
            output.append(record)

        return output

    async def delete_node(self, node_id: str) -> None:
        """Delete node by ID."""
        query = "MATCH (n {id: $id}) DETACH DELETE n"
        await self._run_query(query, {"id": node_id})
