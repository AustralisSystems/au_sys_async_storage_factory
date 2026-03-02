"""
Digital Angels - Database Configuration.

Provides database session management and base models for encrypted settings storage.

Compliance:
- Enterprise Canonical Execution Protocol v1.0.0
- ORM Database Provider Best Practices 2025

Author: Digital Angels Team
Version: 1.0.0
"""

from typing import Any, Optional
from uuid import uuid4

from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
    create_engine,
    event,
)
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import Session, declarative_base, sessionmaker
from sqlalchemy.sql import func

from storage.shared.services.config.settings import get_settings


def get_logger():
    from storage.shared.observability.logger_factory import create_debug_logger

    return create_debug_logger(__name__)


class _LazyLoggerProxy:
    """Simple lazy proxy to obtain an actual logger only when first used.

    This avoids circular imports during module import time while still allowing
    module code to call `logger.info(...)` at runtime. We keep the API minimal
    so we can support typical logger methods accessed throughout this module.
    """

    def __getattr__(self, item: str):
        return getattr(get_logger(), item)


logger: _LazyLoggerProxy = _LazyLoggerProxy()

# Import transaction log models to register them with Base.metadata
try:
    logger.debug("Transaction log models imported successfully")
except ImportError as import_err:
    # Models may not be available during initial import
    logger.debug(f"Transaction log models not available during import: {import_err}")

# Import onboarding ORM models to register them with Base.metadata
try:
    logger.debug("Onboarding models imported successfully")
except ImportError as import_err:
    # Models may not be available during initial import
    logger.debug(f"Onboarding models not available during import: {import_err}")

# Base class for all models
# SQLAlchemy declarative_base() returns a dynamic metaclass that mypy cannot properly type.
# This is a known limitation of SQLAlchemy typing. All Base subclasses require type: ignore[valid-type, misc].
Base = declarative_base()


class ProviderAPIKeyModel(Base):  # type: ignore[valid-type, misc]
    """ORM model for encrypted provider API keys."""

    __tablename__ = "provider_api_keys"

    id = Column(Integer, primary_key=True, autoincrement=True)
    provider_name = Column(
        String(100), nullable=False, index=True
    )  # Provider name (e.g., 'openai', 'anthropic', 'openrouter')
    key_name = Column(
        String(100), nullable=True, index=True
    )  # Optional key identifier (e.g., 'primary', 'backup', 'gpt-4-key')
    encrypted_key = Column(Text, nullable=False)  # Encrypted API key
    key_metadata = Column(JSON, nullable=True)  # Additional metadata (model, base_url, assigned_models, etc.)
    assigned_models = Column(JSON, nullable=True)  # List of model names this key is assigned to (null = all models)
    assigned_providers = Column(JSON, nullable=True)  # List of provider aliases this key can be used for
    priority = Column(Integer, default=100, nullable=False)  # Priority for key selection (lower = higher priority)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    enabled = Column(Boolean, default=True, nullable=False)

    # Composite unique constraint: provider_name + key_name must be unique
    __table_args__ = ({"sqlite_autoincrement": True},)

    def __repr__(self) -> str:
        return f"<ProviderAPIKey(provider={self.provider_name}, key_name={self.key_name}, enabled={self.enabled})>"


class FeatureSettingsModel(Base):  # type: ignore[valid-type, misc]
    """ORM model for feature settings."""

    __tablename__ = "feature_settings"

    id = Column(Integer, primary_key=True, autoincrement=True)
    feature_name = Column(String(100), unique=True, nullable=False, index=True)
    enabled = Column(Boolean, default=False, nullable=False)
    config = Column(JSON, nullable=True)  # Additional flag configuration
    rollout_percentage = Column(Integer, default=0, nullable=False)
    enabled_for_users = Column(JSON, nullable=True)  # List of user IDs
    enabled_for_groups = Column(JSON, nullable=True)  # List of group IDs
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    def __repr__(self) -> str:
        return f"<FeatureSettings(flag={self.feature_name}, enabled={self.enabled})>"


class DefaultToolConfigModel(Base):  # type: ignore[valid-type, misc]
    """ORM model for default tool configuration."""

    __tablename__ = "default_tool_config"

    id = Column(Integer, primary_key=True, autoincrement=True)
    enabled_tools = Column(JSON, nullable=True)  # List of enabled tool names (empty = all enabled)
    disabled_tools = Column(JSON, nullable=True)  # List of disabled tool names
    enabled_mcp_tools = Column(JSON, nullable=True)  # List of enabled MCP tool IDs
    disabled_mcp_tools = Column(JSON, nullable=True)  # List of disabled MCP tool IDs
    enabled_categories = Column(JSON, nullable=True)  # List of enabled tool categories
    disabled_categories = Column(JSON, nullable=True)  # List of disabled tool categories
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    def __repr__(self) -> str:
        return f"<DefaultToolConfig(id={self.id})>"


class SessionToolConfigModel(Base):  # type: ignore[valid-type, misc]
    """ORM model for session-specific tool configuration."""

    __tablename__ = "session_tool_config"

    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(String(255), unique=True, nullable=False, index=True)
    enabled_tools = Column(JSON, nullable=True)  # List of enabled tool names (None = use defaults)
    disabled_tools = Column(JSON, nullable=True)  # List of disabled tool names
    enabled_mcp_tools = Column(JSON, nullable=True)  # List of enabled MCP tool IDs
    disabled_mcp_tools = Column(JSON, nullable=True)  # List of disabled MCP tool IDs
    enabled_categories = Column(JSON, nullable=True)  # List of enabled tool categories
    disabled_categories = Column(JSON, nullable=True)  # List of disabled tool categories
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    def __repr__(self) -> str:
        return f"<SessionToolConfig(session={self.session_id})>"


class DefaultLLMSettingsModel(Base):  # type: ignore[valid-type, misc]
    """ORM model for default LLM settings."""

    __tablename__ = "default_llm_settings"

    id = Column(Integer, primary_key=True, autoincrement=True)
    temperature = Column(Float, nullable=True)  # None = use system default
    max_tokens = Column(Integer, nullable=True)  # None = use system default
    min_tokens = Column(Integer, nullable=True)  # None = no minimum
    top_p = Column(Float, nullable=True)  # None = use system default
    top_k = Column(Integer, nullable=True)  # None = use system default (if supported)
    frequency_penalty = Column(Float, nullable=True)  # None = use system default
    presence_penalty = Column(Float, nullable=True)  # None = use system default
    stop_sequences = Column(JSON, nullable=True)  # List of stop sequences
    response_format = Column(String(50), nullable=True)  # e.g., "json_object"
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    def __repr__(self) -> str:
        return f"<DefaultLLMSettings(id={self.id})>"


class SessionLLMSettingsModel(Base):  # type: ignore[valid-type, misc]
    """ORM model for session-specific LLM settings."""

    __tablename__ = "session_llm_settings"

    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(String(255), unique=True, nullable=False, index=True)
    temperature = Column(Float, nullable=True)  # None = use defaults
    max_tokens = Column(Integer, nullable=True)  # None = use defaults
    min_tokens = Column(Integer, nullable=True)  # None = use defaults
    top_p = Column(Float, nullable=True)  # None = use defaults
    top_k = Column(Integer, nullable=True)  # None = use defaults
    frequency_penalty = Column(Float, nullable=True)  # None = use defaults
    presence_penalty = Column(Float, nullable=True)  # None = use defaults
    stop_sequences = Column(JSON, nullable=True)  # None = use defaults
    response_format = Column(String(50), nullable=True)  # None = use defaults
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    def __repr__(self) -> str:
        return f"<SessionLLMSettings(session={self.session_id})>"


class EncryptedDocumentModel(Base):  # type: ignore[valid-type, misc]
    """Generic encrypted document store for StorageFactory."""

    __tablename__ = "encrypted_documents"

    id = Column(Integer, primary_key=True, autoincrement=True)
    namespace = Column(String(255), nullable=False, index=True)
    doc_key = Column(String(255), nullable=False)
    payload = Column(Text, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        UniqueConstraint("namespace", "doc_key", name="uq_encrypted_documents_namespace_key"),
        {"sqlite_autoincrement": True},
    )

    def __repr__(self) -> str:
        return f"<EncryptedDocument(namespace={self.namespace}, key={self.doc_key})>"


class NotificationSettingsModel(Base):  # type: ignore[valid-type, misc]
    """ORM model for notification settings (email/Slack configuration)."""

    __tablename__ = "notification_settings"

    id = Column(Integer, primary_key=True, autoincrement=True)
    email_enabled = Column(Boolean, default=False, nullable=False)
    email_provider = Column(String(50), default="smtp", nullable=False)  # smtp, aws_ses, microsoft_graph
    email_from = Column(String(255), nullable=True)
    smtp_host = Column(String(255), nullable=True)
    smtp_port = Column(Integer, default=587, nullable=True)
    smtp_username = Column(String(255), nullable=True)
    smtp_password = Column(Text, nullable=True)  # Encrypted
    smtp_use_tls = Column(Boolean, default=True, nullable=False)
    slack_enabled = Column(Boolean, default=False, nullable=False)
    slack_webhook_url = Column(Text, nullable=True)  # Encrypted
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    def __repr__(self) -> str:
        return f"<NotificationSettings(id={self.id}, email_enabled={self.email_enabled}, slack_enabled={self.slack_enabled})>"


class AzureRuntimeConfigModel(Base):  # type: ignore[valid-type, misc]
    """ORM model for Azure + M365 runtime configuration entities."""

    __tablename__ = "azure_runtime_configs"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid4()))
    config_type = Column(String(150), nullable=False, index=True)
    name = Column(String(150), nullable=False)
    version = Column(Integer, nullable=False, default=1)
    payload_encrypted = Column(Text, nullable=False)
    encryption_key_fingerprint = Column(String(64), nullable=True)
    context = Column(JSON, nullable=True)  # Non-sensitive metadata (environment label, tenant hints, etc.)
    tags = Column(JSON, nullable=True)  # Optional categorisation tags
    created_by = Column(String(150), nullable=True)
    updated_by = Column(String(150), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        UniqueConstraint("config_type", "name", name="uq_azure_runtime_configs_type_name"),
        {"sqlite_autoincrement": False},
    )

    def __repr__(self) -> str:
        return f"<AzureRuntimeConfig(type={self.config_type}, name={self.name}, version={self.version})>"


class AzureConfigAuditLogModel(Base):  # type: ignore[valid-type, misc]
    """Audit log for Azure runtime configuration mutations."""

    __tablename__ = "azure_config_audit_logs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    config_id = Column(
        String(36), ForeignKey("azure_runtime_configs.id", ondelete="CASCADE"), nullable=False, index=True
    )
    action = Column(String(50), nullable=False)  # e.g., create, update, delete, rotate, test
    actor = Column(String(150), nullable=True)
    request_id = Column(String(100), nullable=True)
    ip_address = Column(String(64), nullable=True)
    diff = Column(JSON, nullable=True)  # Stores before/after payload deltas
    audit_metadata = Column(JSON, nullable=True)  # Additional structured metadata (e.g., feature, UI route)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    def __repr__(self) -> str:
        return f"<AzureConfigAuditLog(config_id={self.config_id}, action={self.action})>"


class DefaultLLMProviderConfigModel(Base):  # type: ignore[valid-type, misc]
    """ORM model for default LLM provider/model configuration."""

    __tablename__ = "default_llm_provider_config"

    id = Column(Integer, primary_key=True, autoincrement=True)
    primary_provider = Column(String(100), nullable=True)  # None = use system default
    primary_model = Column(String(100), nullable=True)  # None = use system default
    fallback_providers = Column(JSON, nullable=True)  # List of provider names in order
    fallback_models = Column(JSON, nullable=True)  # Dict mapping provider -> model
    provider_preferences = Column(JSON, nullable=True)  # Dict with provider preferences/config
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    def __repr__(self) -> str:
        return f"<DefaultLLMProviderConfig(id={self.id})>"


class SessionLLMProviderConfigModel(Base):  # type: ignore[valid-type, misc]
    """ORM model for session-specific LLM provider/model configuration."""

    __tablename__ = "session_llm_provider_config"

    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(String(255), unique=True, nullable=False, index=True)
    primary_provider = Column(String(100), nullable=True)  # None = use defaults
    primary_model = Column(String(100), nullable=True)  # None = use defaults
    fallback_providers = Column(JSON, nullable=True)  # List of provider names in order (None = use defaults)
    fallback_models = Column(JSON, nullable=True)  # Dict mapping provider -> model (None = use defaults)
    provider_preferences = Column(JSON, nullable=True)  # Dict with provider preferences/config (None = use defaults)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    def __repr__(self) -> str:
        return f"<SessionLLMProviderConfig(session={self.session_id})>"


class ProviderModelPreferencesModel(Base):  # type: ignore[valid-type, misc]
    """ORM model for provider model preferences (default and preferred models per provider)."""

    __tablename__ = "provider_model_preferences"

    id = Column(Integer, primary_key=True, autoincrement=True)
    provider_name = Column(
        String(100), nullable=False, unique=True, index=True
    )  # Provider name (e.g., 'openai', 'anthropic', 'openrouter')
    default_model = Column(String(200), nullable=True)  # Default model ID for this provider
    preferred_models = Column(JSON, nullable=True)  # Ordered list of preferred model IDs (first = most preferred)
    available_models = Column(JSON, nullable=True)  # Cached list of available models from provider
    available_models_updated_at = Column(
        DateTime(timezone=True), nullable=True
    )  # When available_models was last fetched
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    def __repr__(self) -> str:
        return f"<ProviderModelPreferences(provider={self.provider_name}, default={self.default_model})>"


class APPSettingsModel(Base):  # type: ignore[valid-type, misc]
    """ORM model for storing all APP settings from .env/APPSettings class."""

    __tablename__ = "app_settings"

    id = Column(Integer, primary_key=True, autoincrement=True)
    setting_key = Column(String(255), unique=True, nullable=False, index=True)  # Setting name (e.g., 'app_redis_url')
    setting_value = Column(Text, nullable=True)  # Setting value (JSON-encoded for complex types)
    value_type = Column(String(50), nullable=False)  # Type: 'str', 'int', 'bool', 'float', 'json', 'list', 'dict'
    description = Column(Text, nullable=True)  # Description from Field
    source = Column(String(50), nullable=False, default="env")  # Source: 'env', 'default', 'database'
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    def __repr__(self) -> str:
        return f"<APPSettings(key={self.setting_key}, type={self.value_type}, source={self.source})>"


class GlobalLLMPreferencesModel(Base):  # type: ignore[valid-type, misc]
    """ORM model for global LLM model preferences with priority rankings (primary, backup1, backup2, etc.)."""

    __tablename__ = "global_llm_preferences"

    id = Column(Integer, primary_key=True, autoincrement=True)
    primary_model = Column(String(200), nullable=True)  # Primary LLM model ID
    backup1_model = Column(String(200), nullable=True)  # Backup 1 LLM model ID
    backup2_model = Column(String(200), nullable=True)  # Backup 2 LLM model ID
    backup3_model = Column(String(200), nullable=True)  # Backup 3 LLM model ID
    backup4_model = Column(String(200), nullable=True)  # Backup 4 LLM model ID
    backup5_model = Column(String(200), nullable=True)  # Backup 5 LLM model ID
    additional_backups = Column(
        JSON, nullable=True
    )  # Additional backup models as ordered list (backup6, backup7, etc.)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    def __repr__(self) -> str:
        return f"<GlobalLLMPreferences(primary={self.primary_model})>"


class GlobalProviderPreferencesModel(Base):  # type: ignore[valid-type, misc]
    """ORM model for global provider preferences with priority rankings (primary, backup1, backup2, etc.)."""

    __tablename__ = "global_provider_preferences"

    id = Column(Integer, primary_key=True, autoincrement=True)
    primary_provider = Column(String(100), nullable=True)  # Primary provider name
    backup1_provider = Column(String(100), nullable=True)  # Backup 1 provider name
    backup2_provider = Column(String(100), nullable=True)  # Backup 2 provider name
    backup3_provider = Column(String(100), nullable=True)  # Backup 3 provider name
    backup4_provider = Column(String(100), nullable=True)  # Backup 4 provider name
    backup5_provider = Column(String(100), nullable=True)  # Backup 5 provider name
    additional_backups = Column(
        JSON, nullable=True
    )  # Additional backup providers as ordered list (backup6, backup7, etc.)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    def __repr__(self) -> str:
        return f"<GlobalProviderPreferences(primary={self.primary_provider})>"


# Database engine and session management
_async_engine: Optional[Any] = None
_async_session_factory: Optional[async_sessionmaker[AsyncSession]] = None
_sync_engine: Optional[Any] = None
_sync_session_factory: Optional[sessionmaker[Session]] = None


def get_database_url() -> Optional[str]:
    """
    Get database URL from settings.

    This is a bootstrap setting and MUST remain unencrypted.
    Use getattr to avoid raising AttributeError if `app_database_url` is not present.
    Algorithm:
    1. Check `settings.app_database_url` (Legacy/Primary)
    2. Check `settings.database_url` (Validation/Fallback)
    3. Return None if neither set (prevents initialization)
    """
    settings = get_settings()

    # 1. Try Legacy/Primary field
    url = getattr(settings, "app_database_url", None)
    if url:
        return url

    # 2. Try Standard field (often set by validators)
    url = getattr(settings, "database_url", None)
    if url:
        return url

    return None


def ensure_async_driver(url: str) -> str:
    """Ensure database URL uses async driver."""
    if url.startswith("postgresql://"):
        return url.replace("postgresql://", "postgresql+asyncpg://")
    if url.startswith("mysql://"):
        return url.replace("mysql://", "mysql+aiomysql://")
    if url.startswith("sqlite:///"):
        return url.replace("sqlite:///", "sqlite+aiosqlite:///")
    if url.startswith("sqlite://"):
        return url.replace("sqlite://", "sqlite+aiosqlite://")
    return url


def ensure_sync_driver(url: str) -> str:
    """Ensure database URL uses sync driver."""
    # Remove async drivers
    url = url.replace("postgresql+asyncpg://", "postgresql://")
    url = url.replace("mysql+aiomysql://", "mysql://")
    url = url.replace("sqlite+aiosqlite:///", "sqlite:///")
    url = url.replace("sqlite+aiosqlite://", "sqlite://")
    return url


async def init_database() -> None:
    """Initialize database connection and create tables."""
    global _async_engine, _async_session_factory

    db_url = get_database_url()
    if not db_url:
        logger.warning("No database URL configured - encrypted settings storage disabled")
        return

    try:
        async_url = ensure_async_driver(db_url)
        # SQLite doesn't support connection pooling arguments like pool_size
        if async_url.startswith("sqlite"):
            _async_engine = create_async_engine(
                async_url,
                echo=False,
                future=True,
                pool_pre_ping=True,
            )
        else:
            _async_engine = create_async_engine(
                async_url,
                echo=False,
                future=True,
                pool_pre_ping=True,
                pool_size=20,
                max_overflow=10,
                pool_timeout=30,
                pool_recycle=3600,
            )

        # Enable WAL mode for SQLite to prevent locking
        if "sqlite" in async_url:

            @event.listens_for(_async_engine.sync_engine, "connect")
            def set_sqlite_pragma(dbapi_connection, connection_record):
                cursor = dbapi_connection.cursor()
                cursor.execute("PRAGMA journal_mode=WAL")
                cursor.execute("PRAGMA synchronous=NORMAL")
                cursor.execute("PRAGMA busy_timeout=30000")
                cursor.close()

        _async_session_factory = async_sessionmaker(
            _async_engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )

        # Create tables (handle race condition with multiple workers)
        try:
            async with _async_engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
        except Exception as table_error:
            # Handle race condition: if table already exists (from another worker), that's OK
            error_str = str(table_error).lower()
            if "already exists" in error_str or ("table" in error_str and "exists" in error_str):
                logger.debug(f"Tables already exist (likely from another worker): {table_error}")
            else:
                # Re-raise if it's a different error
                raise

        logger.info("Database initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        raise


def init_database_sync() -> None:
    """Initialize database connection synchronously (for migrations)."""
    global _sync_engine, _sync_session_factory

    db_url = get_database_url()
    if not db_url:
        logger.warning("No database URL configured - encrypted settings storage disabled")
        return

    try:
        sync_url = ensure_sync_driver(db_url)
        _sync_engine = create_engine(sync_url, echo=False, future=True)

        # Enable WAL mode for SQLite
        if "sqlite" in sync_url:

            @event.listens_for(_sync_engine, "connect")
            def set_sqlite_pragma_sync(dbapi_connection, connection_record):
                cursor = dbapi_connection.cursor()
                cursor.execute("PRAGMA journal_mode=WAL")
                cursor.execute("PRAGMA synchronous=NORMAL")
                cursor.execute("PRAGMA busy_timeout=30000")
                cursor.close()

        _sync_session_factory = sessionmaker(bind=_sync_engine, expire_on_commit=False)

        # Create tables (handle race condition with multiple workers)
        try:
            Base.metadata.create_all(_sync_engine)
        except Exception as table_error:
            # Handle race condition: if table already exists (from another worker), that's OK
            error_str = str(table_error).lower()
            if "already exists" in error_str or ("table" in error_str and "exists" in error_str):
                logger.debug(f"Tables already exist (likely from another worker): {table_error}")
            else:
                # Re-raise if it's a different error
                raise

        logger.info("Database initialized successfully (sync)")
    except Exception as e:
        logger.error(f"Failed to initialize database (sync): {e}")
        raise


def is_database_initialized() -> bool:
    """Return True if the async database session factory has been initialized."""
    return _async_session_factory is not None


def get_db_session() -> AsyncSession:
    """
    Get async database session context manager.

    Returns:
        Async database session context manager

    Raises:
        ValueError: If database not initialized
    """
    if not _async_session_factory:
        raise ValueError("Database not initialized. Call init_database() first.")
    session = _async_session_factory()
    return session


def get_db_session_sync() -> Session:
    """
    Get sync database session.

    Returns:
        Sync database session

    Raises:
        ValueError: If database not initialized
    """
    if not _sync_session_factory:
        raise ValueError("Database not initialized. Call init_database_sync() first.")
    return _sync_session_factory()


# Tenant isolation support
_tenant_sync_factories: dict[str, sessionmaker[Session]] = {}


def get_tenant_db_session_sync(url: str) -> Session:
    """
    Get a sync database session for a specific tenant URL.
    Implements connection pooling and factory caching per URL.
    """
    global _tenant_sync_factories

    sync_url = ensure_sync_driver(url)

    if sync_url not in _tenant_sync_factories:
        try:
            engine = create_engine(
                sync_url,
                echo=False,
                future=True,
                pool_pre_ping=True,
                pool_recycle=3600,
            )
            # Ensure tables exist for this tenant
            Base.metadata.create_all(engine)
            _tenant_sync_factories[sync_url] = sessionmaker(bind=engine, expire_on_commit=False)
            logger.info(f"Initialized tenant database session factory for: {sync_url}")
        except Exception as e:
            logger.error(f"Failed to initialize tenant database for {sync_url}: {e}")
            raise

    return _tenant_sync_factories[sync_url]()


async def dispose_database() -> None:
    """Dispose database engines to clean up connection pools."""
    global _async_engine, _sync_engine
    
    if _async_engine:
        await _async_engine.dispose()
        _async_engine = None
        
    if _sync_engine:
        _sync_engine.dispose()
        _sync_engine = None
