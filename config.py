from __future__ import annotations

"""
Storage Configuration Manager.

Centralizes all storage-related settings using Pydantic Settings.
Enforces the Zero-Hardcode Mandate by resolving all values from 
environment variables with appropriate defaults.
"""

import os
from typing import Optional, Dict, Any
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, SecretStr, PostgresDsn, RedisDsn, HttpUrl


class StorageSettings(BaseSettings):
    """
    Settings for the Storage Factory sub-module.
    Strictly adheres to Zero-Hardcode Mandate.
    """

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # General
    app_mode: str = Field(default="development")
    storage_manifest_path: str = Field(default="storage/configs/storage_backends.json")

    # Relational
    async_sqlite_path: str = Field(default="sqlite+aiosqlite:///data/storage/default.sqlite")
    async_postgres_url: Optional[PostgresDsn] = None
    async_sqldb_url: Optional[str] = None

    # Document
    mongodb_uri: str = Field(default="mongodb://localhost:27017")
    mongodb_db: str = Field(default="ace_storage")

    # Blob
    local_blob_path: str = Field(default="./data/blobs")
    s3_bucket: str = Field(default="ace-blobs")
    aws_region: str = Field(default="us-east-1")

    # Vector
    redis_host: str = Field(default="localhost")
    redis_port: int = Field(default=6379)
    qdrant_host: str = Field(default="localhost")

    # Graph
    falkordb_host: str = Field(default="localhost")
    falkordb_port: int = Field(default=6379)
    neo4j_uri: str = Field(default="bolt://localhost:7687")

    # Admin
    admin_username: str = Field(default="admin")
    admin_password: SecretStr = Field(default="admin123!")
    storage_backup_dir: str = Field(default="./data/backups")


_settings: Optional[StorageSettings] = None


def get_storage_settings() -> StorageSettings:
    """Singleton accessor for storage settings."""
    global _settings
    if _settings is None:
        _settings = StorageSettings()
    return _settings
