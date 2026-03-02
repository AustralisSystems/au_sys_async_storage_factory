from __future__ import annotations

"""
Neo4j Graph Provider - ENTERPRISE GRADE.

Implements IGraphProvider using neo4j-python-driver for native async support.
"""

import logging
from datetime import datetime, UTC
from typing import Any, Optional, Dict, List, Tuple

try:
    from neo4j import AsyncGraphDatabase

    HAS_NEO4J = True
except ImportError:
    HAS_NEO4J = False

from ..interfaces.base_graph_provider import IGraphProvider
from ..interfaces.sync import ISyncProvider, SyncResult, SyncDirection, SyncConflictResolution
from ..interfaces.health import IHealthCheck, HealthMonitor
from ..interfaces.storage import StorageError

logger = logging.getLogger(__name__)


class Neo4jGraphProvider(IGraphProvider, ISyncProvider, IHealthCheck):
    """
    Neo4j implementation of the IGraphProvider interface.
    """

    def __init__(
        self,
        uri: str = "bolt://localhost:7687",
        username: Optional[str] = None,
        password: Optional[str] = None,
        database: str = "neo4j",
    ):
        self.uri = uri
        self.username = username
        self.password = password
        self.database = database
        self._driver: Any = None
        self._health_monitor = HealthMonitor()

    async def _get_driver(self) -> Any:
        if not self._driver:
            if not HAS_NEO4J:
                raise ImportError("neo4j is required for Neo4jGraphProvider")
            auth = (self.username, self.password) if self.username and self.password else None
            self._driver = AsyncGraphDatabase.driver(self.uri, auth=auth)
        return self._driver

    async def initialize(self) -> None:
        driver = await self._get_driver()
        try:
            await driver.verify_connectivity()
            self._health_monitor.update_health(True)
            logger.info(f"Neo4jGraphProvider initialized at {self.uri}")
        except Exception as e:
            self._health_monitor.update_health(False, {"error": str(e)})
            raise StorageError(f"Failed to connect to Neo4j: {e}")

    async def execute_query(
        self, query: str, params: Optional[dict[str, Any]] = None, database: Optional[str] = None
    ) -> list[dict[str, Any]]:
        driver = await self._get_driver()
        try:
            target_db = database or self.database
            records, _, _ = await driver.execute_query(query, params, database_=target_db)
            return [record.data() for record in records]
        except Exception as e:
            logger.error(f"Neo4j query execution failed: {e}")
            raise StorageError(f"Query failed: {e}")

    async def create_node(self, labels: list[str], properties: dict[str, Any]) -> str:
        label_str = "".join([f":{label}" for label in labels])
        cypher = f"CREATE (n{label_str} $props) RETURN n"
        result = await self.execute_query(cypher, {"props": properties})
        if not result:
            raise StorageError("Failed to create node.")
        return str(properties.get("id", ""))

    async def create_relationship(self, from_id: str, to_id: str, rel_type: str, properties: dict[str, Any]) -> str:
        cypher = f"MATCH (a {{id: $start_id}}), (b {{id: $end_id}}) CREATE (a)-[r:{rel_type} $props]->(b) RETURN r"
        params = {"start_id": from_id, "end_id": to_id, "props": properties or {}}
        result = await self.execute_query(cypher, params)
        if not result:
            raise StorageError("Failed to create relationship.")
        return str(result[0].get("id", ""))

    async def delete_node(self, node_id: str) -> bool:
        cypher = "MATCH (n {id: $id}) DETACH DELETE n"
        await self.execute_query(cypher, {"id": node_id})
        return True

    async def get_schema(self) -> dict[str, Any]:
        """Retrieve graph schema."""
        labels = await self.execute_query("CALL db.labels()")
        rel_types = await self.execute_query("CALL db.relationshipTypes()")
        return {
            "labels": [r["label"] for r in labels],
            "relationship_types": [r["relationshipType"] for r in rel_types],
        }

    async def clear_database(self) -> bool:
        """Wipe all nodes and relationships."""
        await self.execute_query("MATCH (n) DETACH DELETE n")
        return True

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
        return {"provider": "neo4j"}

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
            driver = await self._get_driver()
            await driver.verify_connectivity()
            return True
        except Exception:
            return False

    def get_health_status(self) -> dict[str, Any]:
        return self._health_monitor.get_health_status()

    def get_last_health_check(self) -> datetime:
        return self._health_monitor.get_last_health_check()
