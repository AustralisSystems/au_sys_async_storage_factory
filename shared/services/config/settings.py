from __future__ import annotations

import ast
import json
import os
from functools import lru_cache
from typing import Any, Callable, TypeVar, Union

from dotenv import load_dotenv
from pydantic import Field, validator
from pydantic_settings import BaseSettings
from storage.shared.observability.logger_factory import get_logger

logger = get_logger(__name__)

# Attempt to load local environment file if present.
# Note: In production containers, these variables should be injected by the orchestrator.
load_dotenv(".env")
load_dotenv(".env.standalone")

T = TypeVar("T")


class Settings(BaseSettings):
    """
    Centralized settings for the Digital Angels application.
    Supports hierarchical loading (Env Vars > .env file > Defaults).
    """

    # --- Core Application ---
    app_name: str = Field("Digital Angels API", env="APP_NAME")
    app_description: str = Field("Digital Angels Core API - AI Automation Platform", env="APP_DESCRIPTION")
    app_version: str = Field("2.0.0", env="APP_VERSION")
    debug: bool = Field(False, env="DEBUG")
    environment: str = Field("production", env="ENVIRONMENT")
    host: str = Field("0.0.0.0", env="HOST")  # nosec B104 — runtime-configurable via HOST env var
    port: int = Field(8867, env="PORT")
    log_level: str = Field("INFO", env="LOG_LEVEL")

    # --- Security & Auth ---
    secret_key: str = Field("fallback_secret_key_change_in_production", env="SECRET_KEY")
    jwt_secret_key: str = Field("fallback_jwt_secret_key_change_in_production", env="JWT_SECRET_KEY")
    jwt_algorithm: str = Field("HS256", env="JWT_ALGORITHM")
    access_token_expire_minutes: int = Field(1440, env="ACCESS_TOKEN_EXPIRE_MINUTES")

    # --- Storage & Database ---
    app_storage_backend: str = Field("local", env="APP_STORAGE_BACKEND")
    app_sqlite_path: str = Field("./data/storage.sqlite", env="APP_SQLITE_PATH")
    tinydb_path: str = Field("./data/app-storage.json", env="TINYDB_PATH")
    redis_failover_url: str = Field("redis://localhost:6380/0", env="REDIS_FAILOVER_URL")
    celery_broker_url: str = Field("redis://localhost:6379/1", env="CELERY_BROKER_URL")

    data_dir: str = Field("./data", env="DATA_DIR")
    # For distributed setup we expect external connection strings
    database_url: str = Field("sqlite+aiosqlite:///./data/app.db", env="DATABASE_URL")
    mongo_url: str = Field("mongodb://localhost:27017", env="MONGO_URL")
    mongo_db_name: str = Field("digital_angels", env="MONGO_DB_NAME")

    # --- Redis / Messaging ---
    redis_url: str = Field("redis://localhost:6380/0", env="REDIS_URL")

    # --- Web UI ---
    web_static_url_prefix: str = Field("/static", env="WEB_STATIC_URL_PREFIX")
    web_base_url: str = Field("http://localhost:8867", env="WEB_BASE_URL")
    web_theme: str = Field("dark", env="WEB_THEME")

    # --- Feature Flags ---
    apex_enabled: bool = Field(True, env="APEX_ENABLED")
    knowledge_graph_enabled: bool = Field(True, env="KNOWLEDGE_GRAPH_ENABLED")
    standalone_mode: bool = Field(False, env="STANDALONE_MODE")
    app_apex_config_path: str = Field("./config/policies/apex", env="APP_APEX_CONFIG_PATH")

    # --- Settings Service Specific ---
    settings_cache_ttl: int = Field(300, env="SETTINGS_CACHE_TTL")
    settings_auto_reload: bool = Field(True, env="SETTINGS_AUTO_RELOAD")

    class Config:
        case_sensitive = False
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"  # Ignore extra environment variables to prevent validation errors

    @validator("debug", "apex_enabled", "knowledge_graph_enabled", "standalone_mode", "settings_auto_reload", pre=True)
    @classmethod
    def parse_bools(cls, v: Any) -> bool:
        if isinstance(v, bool):
            return v
        if isinstance(v, str):
            return v.lower() in ("true", "1", "yes", "on")
        return bool(v)

    def is_production(self) -> bool:
        return self.environment.lower() in ["production", "prod", "standalone"]


@lru_cache
def get_settings() -> Settings:
    """Get the cached application settings."""
    return Settings()
