from __future__ import annotations

"""
FalkorDB Graph Provider - ENTERPRISE GRADE.

Implements IGraphProvider using falkordb-python for graph storage on top of Redis.
"""

import asyncio
import logging
from datetime import datetime, UTC
from typing import Any, Optional, Dict, List, Tuple

try:
    from falkordb import FalkorDB

    HAS_FALKORDB = True
except ImportError:
    HAS_FALKORDB = False

from ..interfaces.base_graph_provider import IGraphProvider
from ..interfaces.sync import ISyncProvider, SyncResult, SyncDirection, SyncConflictResolution
from ..interfaces.health import IHealthCheck, HealthMonitor
from ..interfaces.storage import StorageError

logger = logging.getLogger(__name__)


class FalkorDBGraphProvider(IGraphProvider, ISyncProvider, IHealthCheck):
    """
    FalkorDB implementation of the IGraphProvider interface.
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        password: Optional[str] = None,
        database: str = "falkordb",
    ):
        self.host = host
        self.port = port
        self.password = password
        self.database = database
        self._client: Optional[FalkorDB] = None
        self._graph: Any = None
        self._health_monitor = HealthMonitor()

    def _get_graph(self) -> Any:
        if not self._graph:
            if not HAS_FALKORDB:
                raise ImportError("falkordb is required for FalkorDBGraphProvider")
            self._client = FalkorDB(host=self.host, port=self.port, password=self.password)
            self._graph = self._client.select_graph(self.database)
        return self._graph

    async def initialize(self) -> None:
        try:
            # Simple health check via ping
            from redis import Redis

            r = Redis(host=self.host, port=self.port, password=self.password)
            r.ping()
            r.close()
            self._health_monitor.update_health(True)
            logger.info(f"FalkorDBGraphProvider initialized at {self.host}:{self.port}")
        except Exception as e:
            self._health_monitor.update_health(False, {"error": str(e)})
            raise StorageError(f"Failed to connect to Redis/FalkorDB: {e}")

    async def execute_query(
        self, query: str, params: Optional[dict[str, Any]] = None, database: Optional[str] = None
    ) -> list[dict[str, Any]]:
        target_db = database or self.database
        if target_db != self.database:
            if not HAS_FALKORDB:
                raise ImportError("falkordb is required for FalkorDBGraphProvider")
            client = FalkorDB(host=self.host, port=self.port, password=self.password)
            graph = client.select_graph(target_db)
        else:
            graph = self._get_graph()

        try:
            result = await asyncio.to_thread(graph.query, query, params)
            if not result or not hasattr(result, "header") or not hasattr(result, "result_set"):
                return []

            header = [h[1].decode("utf-8") if isinstance(h[1], bytes) else h[1] for h in result.header]
            output = []
            for row in result.result_set:
                record = {header[i]: row[i] for i in range(min(len(header), len(row)))}
                output.append(record)
            return output
        except Exception as e:
            logger.error(f"FalkorDB query failed: {e}")
            raise StorageError(f"Query failed: {e}")

    async def create_node(self, labels: list[str], properties: dict[str, Any]) -> str:
        label_str = "".join([f":{label}" for label in labels])
        cypher = f"CREATE (n{label_str} $props) RETURN n"
        await self.execute_query(cypher, {"props": properties})
        return str(properties.get("id", ""))

    async def create_relationship(self, from_id: str, to_id: str, rel_type: str, properties: dict[str, Any]) -> str:
        cypher = f"MATCH (a {{id: $start}}), (b {{id: $end}}) CREATE (a)-[r:{rel_type} $props]->(b) RETURN r"
        params = {"start": from_id, "end": to_id, "props": properties or {}}
        result = await self.execute_query(cypher, params)
        if not result:
            raise StorageError("Failed to create relationship.")
        return str(result[0].get("id", ""))

    async def delete_node(self, node_id: str) -> bool:
        cypher = "MATCH (n {id: $id}) DETACH DELETE n"
        await self.execute_query(cypher, {"id": node_id})
        return True

    async def get_schema(self) -> dict[str, Any]:
        """Retrieve graph schema for FalkorDB."""
        labels = await self.execute_query("CALL db.labels()")
        rel_types = await self.execute_query("CALL db.relationshipTypes()")
        return {
            "labels": [r["label"] for r in labels] if labels else [],
            "relationship_types": [r["relationshipType"] for r in rel_types] if rel_types else [],
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
        return {"provider": "falkordb"}

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
            from redis import Redis

            r = Redis(host=self.host, port=self.port, password=self.password)
            r.ping()
            r.close()
            return True
        except Exception:
            return False

    def get_health_status(self) -> dict[str, Any]:
        return self._health_monitor.get_health_status()

    def get_last_health_check(self) -> datetime:
        return self._health_monitor.get_last_health_check()
