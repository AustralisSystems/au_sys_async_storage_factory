# Storage Factory - Document Database Access Patterns

**Version**: 1.0.0
**Last Updated**: 2025-12-15
**Status**: Production Ready

---

## Overview

The Storage Factory (`src/services/storage/factory.py`) provides a unified, encrypted document storage interface for APP. It abstracts database access patterns, enabling seamless failover between MongoDB and TinyDB, and supports both document stores (MongoDB/TinyDB) and relational stores (SQLite/PostgreSQL) through a single API.

**Key Features:**
- ✅ **Unified API**: Same code works with MongoDB, TinyDB, SQLite, and PostgreSQL
- ✅ **Automatic Failover**: MongoDB → TinyDB failover on failures
- ✅ **Automatic Failback**: Returns to primary backend when recovered
- ✅ **Encryption**: All data encrypted at rest (AES-256-GCM or Fernet)
- ✅ **Namespace Isolation**: Logical separation of data by namespace
- ✅ **Deployment Mode Support**: Standalone, distributed, and full stack modes

---

## Architecture

### Storage Backends

| Backend | Type | Use Case | Failover To |
|---------|------|----------|--------------|
| **TinyDB** | Document (NoSQL) | Standalone mode, local development, failover | TinyDB (self) |
| **MongoDB** | Document (NoSQL) | Distributed mode, production | TinyDB |
| **SQLite** | Relational (ORM) | Local DB, PostgreSQL failover/cache | TinyDB |
| **PostgreSQL** | Relational (ORM) | External DB, production | SQLite |

### Data Access Patterns

**Document Stores (PRIMARY for most APP data):**
- **TinyDB**: Default for standalone mode - Most APP REST API data, config, storage, settings
- **MongoDB**: Default for distributed mode - Most APP REST API data, config, storage, settings

**Relational Stores (ONLY for SQL-required data):**
- **SQLite**: Local DB requirements, PostgreSQL failover/cache
- **PostgreSQL**: External DB requirements, SQLite as cache/failover

---

## Usage

### Basic Operations

```python
from src.services.storage.factory import get_storage_factory

# Get storage factory instance
factory = get_storage_factory()

# Get a namespaced document store
store = factory.get_document_store("m365.exchange_policies")

# Save a document (MongoDB-style call, works with TinyDB automatically)
store.save("policy_123", {
    "name": "Exchange Policy",
    "enabled": True,
    "rules": ["rule1", "rule2"]
})

# Get a document
policy = store.get("policy_123")
print(policy["name"])  # "Exchange Policy"

# List all documents in namespace
all_policies = store.all()
for key, value in all_policies.items():
    print(f"{key}: {value['name']}")

# Delete a document
store.delete("policy_123")
```

### Automatic Failover

The Storage Factory automatically handles failover when the primary backend fails:

```python
# Configured for MongoDB, but MongoDB is unavailable
store = factory.get_document_store("m365.config")

try:
    # This will try MongoDB first
    store.save("config", {"setting": "value"})
except Exception:
    # After 5 consecutive failures, automatically fails over to TinyDB
    # Operation retries automatically on TinyDB
    # No code changes needed!
    ...
```

**Failover Behavior:**
- **Failure Threshold**: 5 consecutive failures (configurable)
- **Automatic Retry**: Operations automatically retry on fallback backend
- **Transparent**: No code changes required - same API works for both backends
- **Failback**: Automatically returns to primary when it recovers

### Deployment Modes

#### Standalone Mode (1 container = 1 user)

```python
# Uses TinyDB by default
# Configuration: STORAGE_BACKEND=tinydb
factory = get_storage_factory()
store = factory.get_document_store("user_data")
store.save("user_123", {"name": "John Doe"})
```

#### Distributed Mode (Multi-user with dual auth)

```python
# Uses MongoDB by default, fails over to TinyDB
# Configuration:AGE_BACKEND=mongodb
factory = get_storage_factory()
store = factory.get_document_store("shared_config")
store.save("global_setting", {"value": "shared"})
```

#### Full Stack Mode

```python
# Uses MongoDB with automatic TinyDB failover
# Configuration:AGE_BACKEND=mongodb
# Fallback: TinyDB (automatic)
factory = get_storage_factory()
store = factory.get_document_store("production_data")
store.save("key", {"data": "value"})
```

---

## Configuration

### Environment Variables

```bash
# Storage Backend Selection
STORAGE_BACKEND=tinydb        # Options: tinydb, mongodb, sqlite, postgres

# TinyDB Configuration (standalone mode)
TINYDB_PATH=data/app-storage.json

# MongoDB Configuration (distributed mode)
MONGO_DSN=mongodb://user:pass@host:27017/db?authSource=admin

# PostgreSQL Configuration (relational data)
POSTGRES_DSN=postgresql+asyncpg://user:pass@host:5432/db

# Database URL (for SQLite/PostgreSQL ORM)
DATABASE_URL=sqlite:///./data/app.db  # SQLite
#BASE_URL=postgresql+asyncpg://user:pass@host:5432/db  # PostgreSQL

# Encryption
ENCRYPTION_KEY_FILE=config/secrets/app-fernet.key
ENCRYPTION_LEVEL=nist_highest  # Options: fernet, nist_highest
```

### Storage Backends Manifest

The factory reads `config/storage_backends.json` to determine available backends:

```json
{
  "default": "tinydb",
  "supported": [
    {
      "name": "tinydb",
      "type": "document",
      "encrypted": true,
      "description": "Encrypted TinyDB NoSQL document store - PRIMARY for most APP REST API data, config, storage, settings (standalone mode)"
    },
    {
      "name": "mongodb",
      "type": "document",
      "encrypted": true,
      "description": "Motor-powered MongoDB adapter - PRIMARY for most APP REST API data, config, storage, settings (distributed mode)"
    },
    {
      "name": "sqlite",
      "type": "relational",
      "encrypted": true,
      "description": "SQLAlchemy ORM with SQLite - ONLY for relational data requiring SQL features (local DB, PostgreSQL failover/cache)"
    },
    {
      "name": "postgres",
      "type": "relational",
      "encrypted": true,
      "description": "SQLAlchemy ORM with PostgreSQL - ONLY for relational data requiring SQL features (external DB, SQLite as cache/failover)"
    }
  ]
}
```

---

## MongoDB → TinyDB Translation

### Seamless Translation

The Storage Factory ensures MongoDB-style document DB calls seamlessly translate to TinyDB:

**MongoDB Operations → TinyDB Equivalent:**
- `upsert(namespace, key, document)` → `TinyDBProvider.set(namespace:key, document)`
- `get(namespace, key)` → `TinyDBProvider.get(namespace:key)`
- `delete(namespace, key)` → `TinyDBProvider.delete(namespace:key)`
- `list_all(namespace)` → `TinyDBProvider.list_keys()` filtered by namespace prefix

**No Code Changes Required:**
- Write code expecting MongoDB semantics
- Works automatically with TinyDB in standalone mode
- Fails over to TinyDB when MongoDB unavailable
- Same API, same behavior, transparent translation

### Example: M365 Access Configuration

```python
# This code works with both MongoDB and TinyDB
from src.services.storage.factory import get_storage_factory

factory = get_storage_factory()

# MongoDB-style operations, automatically translated to TinyDB when needed
exchange_policies = factory.get_document_store("m365.exchange_policies")
conditional_access = factory.get_document_store("m365.conditional_access_policies")
resource_access = factory.get_document_store("m365.resource_access")

# Save policies (works with MongoDB or TinyDB)
exchange_policies.save("policy_1", {"name": "Policy 1", "enabled": True})
conditional_access.save("ca_1", {"name": "CA Policy", "rules": []})
resource_access.save("resource_1", {"name": "Resource", "permissions": []})

# Get policies (works with MongoDB or TinyDB)
policy = exchange_policies.get("policy_1")
all_policies = exchange_policies.all()
```

---

## Requirements for APP Services

### MANDATORY: Use Storage Factory Only

**✅ CORRECT:**
```python
from src.services.storage.factory import get_storage_factory

factory = get_storage_factory()
store = factory.get_document_store("namespace")
store.save("key", {"data": "value"})
```

**❌ FORBIDDEN:**
```python
# Direct database access - FORBIDDEN
from tinydb import TinyDB
db = TinyDB("data/db.json")  # ❌ DO NOT DO THIS

# Direct MongoDB access - FORBIDDEN
from motor.motor_asyncio import AsyncIOMotorClient
client = AsyncIOMotorClient("mongodb://...")  # ❌ DO NOT DO THIS

# Direct SQLAlchemy access - FORBIDDEN (unless for relational data requiring SQL)
from sqlalchemy import create_engine
engine = create_engine("sqlite:///db.db")  # ❌ DO NOT DO THIS
```

### MANDATORY: Use ORM for Relational Data

**For relational data requiring SQL features:**
- ✅ Use SQLAlchemy ORM via `SQLAlchemyEncryptedAdapter`
- ✅ Use `DATABASE_URL` for SQLite/PostgreSQL connection
- ✅ Use ORM models defined in `src/services/config/database.py`

**For document data (config, settings, REST API data):**
- ✅ Use Storage Factory with MongoDB/TinyDB
- ✅ Use `get_document_store()` for namespaced document access
- ✅ Use `save()`, `get()`, `delete()`, `all()` methods

### MANDATORY: Namespace Isolation

Always use namespaced document stores:

```python
# ✅ CORRECT: Use namespaces
store = factory.get_document_store("m365.exchange_policies")
store.save("key", data)

# ❌ INCORRECT: Direct access without namespace
# This breaks isolation and causes conflicts
```

**Common Namespaces:**
- `m365.*` - M365 service data
- `azure.*` - Azure service data
- `app_settings` - Application settings
- `features` - Feature flag configuration
- `user_data.*` - User-specific data

---

## Failover & Failback Behavior

### Automatic Failover

When the primary backend fails:

1. **Failure Detection**: Operations fail on primary backend
2. **Failure Tracking**: `DynamicStorageManager` tracks consecutive failures
3. **Threshold Check**: After 5 consecutive failures, failover is triggered
4. **Backend Switch**: Storage Factory switches to fallback backend
5. **Operation Retry**: Failed operation automatically retries on fallback backend
6. **Logging**: Failover events logged with warnings

### Automatic Failback

When the primary backend recovers:

1. **Recovery Detection**: Successful operations on fallback backend
2. **Health Check**: Primary backend health metrics checked
3. **Failback Trigger**: When primary has 0 consecutive failures
4. **Backend Switch**: Storage Factory switches back to primary backend
5. **Logging**: Failback events logged with info messages

### Manual Backend Switching

```python
# Switch backend manually (e.g., for testing)
factory = get_storage_factory()
factory.switch_backend("tinydb")  # Switch to TinyDB
factory.switch_backend("mongodb")  # Switch to MongoDB
```

---

## Health Monitoring

### Check Backend Health

```python
factory = get_storage_factory()

# Get current backend
backend = factory.get_current_backend()  # "tinydb", "mongodb", etc.

# Get health status
health = factory.get_health()
print(f"Backend: {health.name}")
print(f"Type: {health.backend_type}")
print(f"Healthy: {health.healthy}")

# Get dynamic manager status (failover info)
manager = factory.get_dynamic_manager()
status = manager.get_status()
print(f"Active: {status['active_backend']}")
print(f"Fallback: {status['fallback_backend']}")
print(f"Should Failover: {status['should_failover']}")
print(f"Reason: {status['reason']}")
```

---

## Best Practices

### 1. Always Use Storage Factory

**✅ DO:**
```python
from src.services.storage.factory import get_storage_factory
store = factory.get_document_store("namespace")
```

**❌ DON'T:**
```python
# Direct database access
from tinydb import TinyDB
db = TinyDB("data/db.json")
```

### 2. Use Namespaces for Isolation

**✅ DO:**
```python
store = factory.get_document_store("m365.exchange_policies")
```

**❌ DON'T:**
```python
# Global namespace causes conflicts
store = factory.get_document_store("")
```

### 3. Handle Failures Gracefully

**✅ DO:**
```python
try:
    store.save("key", data)
except Exception as e:
    logger.error(f"Storage operation failed: {e}")
    # Failover happens automatically, operation retries
```

### 4. Use Appropriate Backend for Data Type

**✅ DO:**
- Document data (config, settings) → MongoDB/TinyDB
- Relational data (SQL queries) → SQLite/PostgreSQL

**❌ DON'T:**
- Use relational stores for simple key-value data
- Use document stores for complex SQL queries

### 5. Configure Fallback Backends

**✅ DO:**
- MongoDB → TinyDB (document → document)
- PostgreSQL → SQLite (relational → relational)

**❌ DON'T:**
- MongoDB → PostgreSQL (different types)
- TinyDB → MongoDB (TinyDB is already fallback)

---

## Troubleshooting

### "Backend not found" Error

**Problem**: Backend not configured in manifest

**Solution**: Check `config/storage_backends.json` includes the backend

### "Connection failed" Error

**Problem**: Primary backend unavailable

**Solution**: Failover happens automatically after 5 failures. Check backend connectivity.

### "Failover not working" Error

**Problem**: Failover threshold not reached

**Solution**: Check `DynamicStorageManager` failure threshold (default: 5)

### "Data not persisting" Error

**Problem**: Encryption key not configured

**Solution**: Set `ENCRYPTION_KEY_FILE` or `ENCRYPTION_KEY`

---

## Migration Guide

### From Direct TinyDB Access

**Before:**
```python
from tinydb import TinyDB
db = TinyDB("data/db.json")
db.insert({"key": "value"})
```

**After:**
```python
from src.services.storage.factory import get_storage_factory
factory = get_storage_factory()
store = factory.get_document_store("namespace")
store.save("key", {"key": "value"})
```

### From Direct MongoDB Access

**Before:**
```python
from motor.motor_asyncio import AsyncIOMotorClient
client = AsyncIOMotorClient("mongodb://...")
db = client["database"]
collection = db["collection"]
await collection.insert_one({"key": "value"})
```

**After:**
```python
from src.services.storage.factory import get_storage_factory
factory = get_storage_factory()
store = factory.get_document_store("namespace")
store.save("key", {"key": "value"})  # Works with MongoDB or TinyDB
```

---

## API Reference

### StorageFactory

```python
class StorageFactory:
    def get_current_backend(self) -> str
    def get_health(self) -> StorageHealth
    def get_document_store(self, namespace: str) -> DocumentNamespaceStore
    def switch_backend(self, backend_name: str) -> StorageHealth
    def get_dynamic_manager(self) -> DynamicStorageManager
```

### DocumentNamespaceStore

```python
class DocumentNamespaceStore:
    def save(self, key: str, value: Dict[str, Any]) -> None
    def get(self, key: str) -> Optional[Dict[str, Any]]
    def delete(self, key: str) -> None
    def all(self) -> Dict[str, Dict[str, Any]]
```

---

## Related Documentation

- **Main README**: `README.md` - Storage and database access patterns overview
- **Storage Interfaces**: `src/services/storage/interfaces/` - Storage provider interfaces
- **Database Configuration**: `src/services/config/database.py` - ORM models and configuration
- **Settings**: `src/services/config/settings.py` - Storage backend configuration

---

## Version History

- **v1.0.0** (2025-12-15): Initial production release with MongoDB→TinyDB failover, automatic failback, deployment mode support

<!-- AUTO-GENERATED: START - DO NOT EDIT MANUALLY -->


**Auto-generated metadata (idempotent)**

Generated at: 2026-02-18T04:45:03Z

Key files summary:

- `README.md` — **Version**: 1.0.0 **Last Updated**: 2025-12-15 **Status**: Production Ready
- `__init__.py` — Storage service package scaffolding.
- `beanie_sqlite_adapter.py` — Beanie ODM -> SQLite Adapter - ENTERPRISE GRADE.
- `beanie_tinydb_adapter.py` — Beanie ODM -> TinyDB Adapter - CRITICAL AND MANDATORY.
- `blob_factory.py` — from typing import Optional, Any
- `blob_storage.py` — Blob Storage Service Implementation.
- `config.py` — Storage configuration utilities.
- `connection_registry.py` — import json
- `dynamic_manager.py` — Dynamic storage manager.
- `factory.py` — Storage factory responsible for returning encrypted storage adapters.
- `lifecycle_manager.py` — import logging
- `sqlite_provider.py` — SQLite Storage Provider - Clean implementation following SOLID principles.
- `tinydb_doc_ids.py` — Utility helpers for deterministic TinyDB document identifiers.
- `tinydb_provider.py` — TinyDB Storage Provider - Clean implementation following SOLID principles.


<!-- AUTO-GENERATED: END -->
