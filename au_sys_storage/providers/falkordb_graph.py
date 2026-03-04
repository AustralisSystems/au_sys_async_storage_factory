from __future__ import annotations

"""
FalkorDB Graph Provider - ENTERPRISE GRADE.

Implements IGraphProvider using falkordb-python for graph storage on top of Redis.
"""

import asyncio
import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Awaitable, Optional, cast

try:
    import aiofiles  # type: ignore[import-untyped]

    HAS_AIOFILES = True
except ImportError:
    HAS_AIOFILES = False

try:
    from falkordb import FalkorDB

    HAS_FALKORDB = True
except ImportError:
    HAS_FALKORDB = False

from ..interfaces.backup import IBackupProvider
from ..interfaces.base_graph_provider import IGraphProvider
from ..interfaces.health import HealthMonitor, IHealthCheck
from ..interfaces.storage import StorageError
from ..interfaces.sync import ISyncProvider, SyncConflictResolution, SyncDirection, SyncResult

logger = logging.getLogger(__name__)


class FalkorDBGraphProvider(IGraphProvider, ISyncProvider, IHealthCheck, IBackupProvider):
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
            from redis.asyncio import Redis as AsyncRedis

            r = AsyncRedis(host=self.host, port=self.port, password=self.password)
            await cast(Awaitable[bool], r.ping())
            await r.aclose()
            self._health_monitor.update_health(True)
            logger.info(f"FalkorDBGraphProvider initialized at {self.host}:{self.port}")
        except Exception as e:
            self._health_monitor.update_health(False, {"error": str(e)})
            raise StorageError(f"Failed to connect to Redis/FalkorDB: {e}")

    async def execute_query(
        self, cypher: str, params: Optional[dict[str, Any]] = None, database: Optional[str] = None
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
            result = await asyncio.to_thread(graph.query, cypher, params)
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
            from redis.asyncio import Redis as AsyncRedis

            r = AsyncRedis(host=self.host, port=self.port, password=self.password)
            await cast(Awaitable[bool], r.ping())
            await r.aclose()
            return True
        except Exception:
            return False

    def get_health_status(self) -> dict[str, Any]:
        return self._health_monitor.get_health_status()

    def get_last_health_check(self) -> datetime:
        return self._health_monitor.get_last_health_check()

    # --- IBackupProvider Implementation ---
    async def create_backup(self, backup_path: str, metadata: Optional[dict[str, Any]] = None) -> bool:
        """Sovereign JSON backup: export all nodes and relationships via Cypher."""
        if not HAS_AIOFILES:
            logger.error("aiofiles is required for FalkorDBGraphProvider.create_backup")
            return False
        try:
            nodes = await self.execute_query("MATCH (n) RETURN labels(n) AS labels, properties(n) AS props")
            relationships = await self.execute_query(
                "MATCH (a)-[r]->(b) RETURN "
                "properties(a) AS from_props, type(r) AS rel_type, "
                "properties(r) AS rel_props, properties(b) AS to_props"
            )
            backup_data = {
                "provider": "falkordb",
                "timestamp": datetime.now(UTC).isoformat(),
                "metadata": metadata or {},
                "nodes": nodes,
                "relationships": relationships,
            }
            target = Path(backup_path)
            target.parent.mkdir(parents=True, exist_ok=True)
            async with aiofiles.open(target, mode="w", encoding="utf-8") as f:
                await f.write(json.dumps(backup_data, indent=2, default=str))
            logger.info("FalkorDB backup created: %s", backup_path)
            return True
        except Exception as e:
            logger.error("FalkorDB backup failed: %s", e)
            return False

    async def restore_backup(self, backup_path: str, clear_existing: bool = False) -> bool:
        """Restore FalkorDB graph from sovereign JSON backup."""
        if not HAS_AIOFILES:
            logger.error("aiofiles is required for FalkorDBGraphProvider.restore_backup")
            return False
        try:
            if not Path(backup_path).exists():
                return False
            async with aiofiles.open(backup_path, encoding="utf-8") as f:
                content = await f.read()
            backup_data = json.loads(content)

            if clear_existing:
                await self.execute_query("MATCH (n) DETACH DELETE n")

            for node in backup_data.get("nodes", []):
                props = node.get("props", {})
                labels = node.get("labels", [])
                label_str = "".join(f":{lbl}" for lbl in labels)
                await self.execute_query(
                    f"MERGE (n{label_str} {{id: $id}}) SET n += $props",
                    {"id": props.get("id", ""), "props": props},
                )

            for rel in backup_data.get("relationships", []):
                from_id = rel.get("from_props", {}).get("id", "")
                to_id = rel.get("to_props", {}).get("id", "")
                rel_type = rel.get("rel_type", "RELATED")
                rel_props = rel.get("rel_props", {})
                await self.execute_query(
                    f"MATCH (a {{id: $from_id}}), (b {{id: $to_id}}) MERGE (a)-[r:{rel_type}]->(b) SET r += $props",
                    {"from_id": from_id, "to_id": to_id, "props": rel_props},
                )

            logger.info("FalkorDB restored from %s", backup_path)
            return True
        except Exception as e:
            logger.error("FalkorDB restore failed: %s", e)
            return False

    async def list_backups(self, backup_dir: str) -> dict[str, dict[str, Any]]:
        """List available FalkorDB backup files in a directory."""
        backups: dict[str, dict[str, Any]] = {}
        path = Path(backup_dir)
        if path.exists():
            for item in path.glob("*.json"):
                backups[item.name] = {
                    "size": item.stat().st_size,
                    "created": datetime.fromtimestamp(item.stat().st_ctime, UTC).isoformat(),
                }
        return backups

    async def validate_backup(self, backup_path: str) -> dict[str, Any]:
        """Validate a FalkorDB JSON backup file."""
        path = Path(backup_path)
        if not path.exists():
            return {"valid": False, "error": "File not found"}
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
            if data.get("provider") == "falkordb" and "nodes" in data and "relationships" in data:
                return {
                    "valid": True,
                    "metadata": data.get("metadata"),
                    "node_count": len(data.get("nodes", [])),
                    "relationship_count": len(data.get("relationships", [])),
                }
            return {"valid": False, "error": "Invalid backup format"}
        except Exception as e:
            return {"valid": False, "error": str(e)}
