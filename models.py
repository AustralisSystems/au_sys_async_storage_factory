from __future__ import annotations

"""
Storage Configuration Models.

Standardizes backend configuration using Pydantic, enforcing the 
Zero-Hardcode Mandate.
"""

from enum import Enum
from typing import Any, Optional
from pydantic import BaseModel, ConfigDict, Field, SecretStr


class StorageBackendType(str, Enum):
    RELATIONAL = "relational"
    DOCUMENT = "document"
    BLOB = "blob"
    VECTOR = "vector"
    GRAPH = "graph"
    TRANSIENT = "transient"


class StorageBackendConfig(BaseModel):
    """Universal configuration for any storage backend."""

    name: str
    type: StorageBackendType
    provider: str  # e.g., "sqlite", "mongodb", "s3"
    connection_url: Optional[str] = None
    host: Optional[str] = None
    port: Optional[int] = None
    username: Optional[str] = None
    password: Optional[SecretStr] = None
    database: Optional[str] = None
    options: dict[str, Any] = Field(default_factory=dict)

    # Failover / Tiering
    secondary_backend: Optional[str] = None
    failover_enabled: bool = False

    model_config = ConfigDict(arbitrary_types_allowed=True)


class StorageManifest(BaseModel):
    """Root configuration manifest for the Storage Factory."""

    default_relational: str = "sqlite"
    default_document: str = "mongodb"
    default_blob: str = "local"
    default_vector: str = "redis"
    default_graph: str = "falkordb"

    backends: dict[str, StorageBackendConfig] = Field(default_factory=dict)
