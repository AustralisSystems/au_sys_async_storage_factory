"""
Storage configuration utilities.

Central credential management functions for storage providers.
"""

import os
from enum import Enum
from functools import lru_cache
from typing import Optional

from pydantic import BaseModel, Field, SecretStr

from src.shared.services.config.settings import get_settings


class VectorStoreType(str, Enum):
    """Supported vector store backends."""

    REDIS = "redis"
    QDRANT = "qdrant"


class VectorDistanceMetric(str, Enum):
    """Distance metrics for vector search."""

    COSINE = "cosine"
    L2 = "l2"
    IP = "ip"


class VectorStoreConfig(BaseModel):
    """Configuration for vector storage."""

    type: VectorStoreType = Field(default=VectorStoreType.REDIS)
    host: str = Field(default="localhost")
    port: int = Field(default=6379)
    password: Optional[SecretStr] = None
    collection_name: str = Field(default="vectors")
    dimension: int = Field(default=1536)  # Default for OpenAI embeddings
    metric: VectorDistanceMetric = Field(default=VectorDistanceMetric.COSINE)
    api_key: Optional[SecretStr] = None


class GraphStoreType(str, Enum):
    """Supported graph store backends."""

    FALKORDB = "falkordb"
    NEO4J = "neo4j"


class GraphStoreConfig(BaseModel):
    """Configuration for graph storage."""

    type: GraphStoreType = Field(default=GraphStoreType.FALKORDB)
    host: str = Field(default="localhost")
    port: int = Field(default=6379)
    password: Optional[SecretStr] = None
    username: Optional[str] = None
    database: str = Field(default="graph")
    api_key: Optional[SecretStr] = None


@lru_cache
def get_admin_credentials() -> tuple[str, str, str]:
    """
    Get admin credentials from central configuration.

    SINGLE SOURCE OF TRUTH for admin credentials across the entire application.
    All components MUST use this function instead of directly calling os.getenv().

    Returns:
        Tuple of (username, password, email)

    Raises:
        ValueError: If credentials cannot be determined
    """
    try:
        # Priority 1: Try to get from validated Settings (preferred)
        settings = get_settings()
        username = settings.admin_username or os.getenv("ADMIN_USERNAME", "admin")
        password = settings.admin_password or os.getenv("ADMIN_PASSWORD", "admin123!")
        # If ADMIN_EMAIL is not provided, default sensibly:
        # - If ADMIN_USERNAME already looks like an email, treat it as the email.
        # - Otherwise, fall back to a deterministic local domain.
        email = os.getenv("ADMIN_EMAIL")
        if not email:
            email = username if "@" in username else f"{username}@application.local"

        return (username, password, email)
    except Exception:
        # Priority 2: Environment variables (for initialization before Settings loads)
        username = os.getenv("ADMIN_USERNAME", os.getenv("ADMIN_USERNAME", "admin"))
        password = os.getenv("ADMIN_PASSWORD", os.getenv("ADMIN_PASSWORD", "admin123!"))
        email = os.getenv("ADMIN_EMAIL")
        if not email:
            email = username if "@" in username else f"{username}@application.local"

        return (username, password, email)


def get_credential_with_priority(env_var_name: str, required_in_production: bool = True) -> str | None:
    """
    Get credential from environment variable with priority handling.

    Args:
        env_var_name: Name of the environment variable
        required_in_production: Whether this credential is required in production

    Returns:
        Credential value or None if not found
    """
    value = os.getenv(env_var_name)
    if not value and required_in_production:
        settings = get_settings()
        if settings.app_mode == "production":
            raise ValueError(f"Required credential {env_var_name} not set in production mode")
    return value
