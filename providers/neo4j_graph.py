from src.shared.observability.logger_factory import get_logger
from typing import Any, Optional

try:
    from neo4j import AsyncGraphDatabase
    from neo4j.exceptions import Neo4jError
except ImportError:
    AsyncGraphDatabase = None  # type: ignore
    Neo4jError = None  # type: ignore

from src.shared.services.storage.config import GraphStoreConfig
from src.shared.services.storage.interfaces.graph import IGraphProvider

logger = get_logger(__name__)


class Neo4jGraphProvider(IGraphProvider):
    """
    Neo4j implementation of the IGraphProvider interface.
    """

    def __init__(self, config: GraphStoreConfig):
        if AsyncGraphDatabase is None:
            raise ImportError("neo4j is required for Neo4jGraphProvider")

        self.config = config
        self._driver: Any = None
        self._uri = config.host
        # Sanitize URI
        if not (
            self._uri.startswith("bolt://") or self._uri.startswith("neo4j://") or self._uri.startswith("neo4j+s://")
        ):
            self._uri = f"neo4j://{config.host}:{config.port}"

    async def _get_driver(self) -> Any:
        """Lazy initialization of Neo4j driver."""
        if self._driver:
            return self._driver

        auth = None
        if self.config.username and self.config.password:
            auth = (self.config.username, self.config.password.get_secret_value())

        self._driver = AsyncGraphDatabase.driver(self._uri, auth=auth)
        # Verify connectivity
        await self._driver.verify_connectivity()

        return self._driver

    async def query(self, cypher: str, params: Optional[dict[str, Any]] = None) -> list[dict[str, Any]]:
        """Execute Cypher query."""
        driver = await self._get_driver()

        # Use execute_query for simple execution (auto-commit transaction)
        records, _, _ = await driver.execute_query(cypher, params, database_=self.config.database)

        # Convert records to list of dicts
        output = []
        for record in records:
            output.append(record.data())  # record.data() returns dict

        return output

    async def create_node(self, labels: list[str], properties: dict[str, Any]) -> str:
        """Create node and return ID."""
        label_str = "".join([f":{label}" for label in labels])

        # We return the 'id' property if set, otherwise we might need to handle internal ID.
        # But 'id' property is standard for portability.

        query_cypher = f"CREATE (n{label_str} $props) RETURN n"
        result = await self.query(query_cypher, {"props": properties})

        if not result:
            raise RuntimeError(f"Failed to create node with labels {labels}")

        raw_props = result[0]["n"]

        if "id" in properties:
            return str(properties["id"])

        # Fallback to 'id' if present in props (it should be if we passed it)
        return str(raw_props.get("id", ""))

    async def create_relationship(self, from_id: str, to_id: str, rel_type: str, properties: dict[str, Any]) -> str:
        """Create relationship between nodes."""
        props = properties or {}

        # Match nodes by 'id' property
        query_cypher = (
            f"MATCH (a {{id: $start_id}}), (b {{id: $end_id}}) CREATE (a)-[r:{rel_type} $props]->(b) RETURN r"
        )
        params = {"start_id": from_id, "end_id": to_id, "props": props}

        result = await self.query(query_cypher, params)

        if not result:
            raise ValueError(f"Could not create relationship. Nodes {from_id} or {to_id} might not exist.")

        raw_props = result[0]["r"]
        return str(raw_props.get("id", ""))

    async def delete_node(self, node_id: str) -> None:
        """Delete node by ID."""
        query_cypher = "MATCH (n {id: $id}) DETACH DELETE n"
        await self.query(query_cypher, {"id": node_id})
