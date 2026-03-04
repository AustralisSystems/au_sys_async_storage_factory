from __future__ import annotations

from functools import lru_cache
from typing import Any, TypeVar

from dotenv import load_dotenv
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from au_sys_storage.shared.observability.logger_factory import get_component_logger as get_logger

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

    model_config = SettingsConfigDict(
        case_sensitive=False,
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",  # Ignore extra environment variables to prevent validation errors
    )

    # --- Core Application ---
    app_name: str = Field(default="Digital Angels API", validation_alias="APP_NAME")
    app_description: str = Field(
        default="Digital Angels Core API - AI Automation Platform", validation_alias="APP_DESCRIPTION"
    )
    app_version: str = Field(default="2.0.0", validation_alias="APP_VERSION")
    debug: bool = Field(default=False, validation_alias="DEBUG")
    environment: str = Field(default="production", validation_alias="ENVIRONMENT")
    host: str = Field(default="0.0.0.0", validation_alias="HOST")  # nosec B104 — runtime-configurable via HOST env var
    port: int = Field(default=8867, validation_alias="PORT")
    log_level: str = Field(default="INFO", validation_alias="LOG_LEVEL")

    # --- Security & Auth ---
    secret_key: str = Field(default="fallback_secret_key_change_in_production", validation_alias="SECRET_KEY")
    jwt_secret_key: str = Field(
        default="fallback_jwt_secret_key_change_in_production", validation_alias="JWT_SECRET_KEY"
    )
    jwt_algorithm: str = Field(default="HS256", validation_alias="JWT_ALGORITHM")
    access_token_expire_minutes: int = Field(default=1440, validation_alias="ACCESS_TOKEN_EXPIRE_MINUTES")

    # --- Storage & Database ---
    app_storage_backend: str = Field(default="local", validation_alias="APP_STORAGE_BACKEND")
    app_sqlite_path: str = Field(default="./data/storage.sqlite", validation_alias="APP_SQLITE_PATH")
    tinydb_path: str = Field(default="./data/app-storage.json", validation_alias="TINYDB_PATH")
    redis_failover_url: str = Field(default="redis://localhost:6380/0", validation_alias="REDIS_FAILOVER_URL")
    celery_broker_url: str = Field(default="redis://localhost:6379/1", validation_alias="CELERY_BROKER_URL")

    data_dir: str = Field(default="./data", validation_alias="DATA_DIR")
    # For distributed setup we expect external connection strings
    database_url: str = Field(default="sqlite+aiosqlite:///./data/app.db", validation_alias="DATABASE_URL")
    mongo_url: str = Field(default="mongodb://localhost:27017", validation_alias="MONGO_URL")
    mongo_db_name: str = Field(default="digital_angels", validation_alias="MONGO_DB_NAME")

    # --- Redis / Messaging ---
    redis_url: str = Field(default="redis://localhost:6380/0", validation_alias="REDIS_URL")

    # --- Web UI ---
    web_static_url_prefix: str = Field(default="/static", validation_alias="WEB_STATIC_URL_PREFIX")
    web_base_url: str = Field(default="http://localhost:8867", validation_alias="WEB_BASE_URL")
    web_theme: str = Field(default="dark", validation_alias="WEB_THEME")

    # --- Feature Flags ---
    apex_enabled: bool = Field(default=True, validation_alias="APEX_ENABLED")
    knowledge_graph_enabled: bool = Field(default=True, validation_alias="KNOWLEDGE_GRAPH_ENABLED")
    standalone_mode: bool = Field(default=False, validation_alias="STANDALONE_MODE")
    app_apex_config_path: str = Field(default="./config/policies/apex", validation_alias="APP_APEX_CONFIG_PATH")

    # --- Settings Service Specific ---
    settings_cache_ttl: int = Field(default=300, validation_alias="SETTINGS_CACHE_TTL")
    settings_auto_reload: bool = Field(default=True, validation_alias="SETTINGS_AUTO_RELOAD")

    @field_validator(
        "debug", "apex_enabled", "knowledge_graph_enabled", "standalone_mode", "settings_auto_reload", mode="before"
    )
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
