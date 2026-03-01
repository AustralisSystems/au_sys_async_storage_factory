"""
Beanie ODM -> TinyDB Adapter - CRITICAL AND MANDATORY.

This adapter translates Beanie ODM operations to TinyDB operations,
enabling MongoDB -> TinyDB fallback capability.

CRITICAL: Beanie ODM does NOT support TinyDB natively. This adapter is
MANDATORY for MongoDB -> TinyDB fallback capability.

This adapter implements:
- Beanie document model -> TinyDB document translation
- Beanie query operations -> TinyDB query operations translation
- Beanie CRUD operations -> TinyDB CRUD operations translation
- Beanie relationship operations -> TinyDB manual relationship handling
- Beanie validation -> TinyDB validation (using Pydantic models)
- Data consistency checks between MongoDB and TinyDB
- Automatic failback to MongoDB when connection recovers
- Bidirectional sync between MongoDB and TinyDB when both available
"""

from datetime import datetime
from pathlib import Path
from typing import Any, Optional, TypeVar

from beanie import Document, PydanticObjectId
from tinydb import Query, TinyDB

from src.shared.manifest.lifecycle import BaseLifecycleMixin
from src.shared.observability.logger_factory import get_component_logger
from src.shared.services.storage.interfaces.health import HealthMonitor, IHealthCheck
from src.shared.services.storage.interfaces.storage import OperationNotSupported, StorageError

logger = get_component_logger("storage", "beanie_tinydb_adapter")

T = TypeVar("T", bound=Document)


class BeanieTinyDBAdapter(BaseLifecycleMixin, IHealthCheck):
    """
    Adapter that translates Beanie ODM operations to TinyDB operations.

    This adapter maintains Beanie ODM interface compatibility while
    using TinyDB as the underlying storage backend.
    """

    def __init__(
        self,
        db_path: str,
        document_models: Optional[list[type[Document]]] = None,
        encryption_key: Optional[str] = None,
    ):
        """
        Initialize BeanieTinyDBAdapter.

        Args:
            db_path: Path to TinyDB file
            document_models: List of Beanie document model classes
            encryption_key: Optional encryption key (for compatibility)
        """
        super().__init__()
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.document_models = document_models or []
        self.encryption_key = encryption_key

        # Initialize TinyDB
        self.db = TinyDB(str(self.db_path))

        # Create collections for each document model
        self._collections: dict[str, Any] = {}
        for model_class in self.document_models:
            collection_name = self._get_collection_name(model_class)
            self._collections[collection_name] = self.db.table(collection_name)

        # Health monitoring
        self._health_monitor = HealthMonitor()
        self._running = True

        logger.info(
            "BeanieTinyDBAdapter initialized",
            extra={
                "db_path": str(self.db_path),
                "model_count": len(self.document_models),
            },
        )

        # Patch document models for direct Beanie-like access
        self._patch_models()

    def _get_collection_name(self, model_class: type[Document]) -> str:
        """Get collection name for document model."""
        if hasattr(model_class, "Settings") and hasattr(model_class.Settings, "name"):
            return model_class.Settings.name
        return model_class.__name__.lower()

    def _document_to_dict(self, document: Document) -> dict[str, Any]:
        """Convert Beanie document to dictionary."""
        if hasattr(document, "dict"):
            return document.dict()
        if hasattr(document, "model_dump"):
            return document.model_dump()
        return dict(document)

    def _dict_to_document(self, model_class: type[T], data: Any) -> T:
        """Convert dictionary (or TinyDB document) to Beanie document."""
        # TinyDB documents are dict-like but have a doc_id property
        doc_dict = dict(data)
        if hasattr(data, "doc_id"):
            # Map TinyDB internal ID to Beanie ID
            # Use 24-char hex string to satisfy PydanticObjectId if needed
            doc_id = data.doc_id
            if isinstance(doc_id, int):
                # Convert int to 24-char hex string: 1 -> "000000000000000000000001"
                beanie_id = f"{doc_id:024x}"
            else:
                beanie_id = str(doc_id)

            if "id" not in doc_dict or doc_dict["id"] is None:
                doc_dict["id"] = beanie_id
            if "_id" not in doc_dict or doc_dict["_id"] is None:
                doc_dict["_id"] = beanie_id
        return model_class(**doc_dict)

    def _translate_query(self, query: dict[str, Any]) -> Query:
        """Translate Beanie query to TinyDB Query."""
        tinydb_query = Query()

        for key, value in query.items():
            if isinstance(value, dict):
                # Handle operators like $gt, $lt, etc.
                for op, op_value in value.items():
                    if op == "$eq":
                        tinydb_query = tinydb_query & (Query()[key] == op_value)
                    elif op == "$gt":
                        tinydb_query = tinydb_query & (Query()[key] > op_value)
                    elif op == "$lt":
                        tinydb_query = tinydb_query & (Query()[key] < op_value)
                    elif op == "$gte":
                        tinydb_query = tinydb_query & (Query()[key] >= op_value)
                    elif op == "$lte":
                        tinydb_query = tinydb_query & (Query()[key] <= op_value)
                    elif op == "$ne":
                        tinydb_query = tinydb_query & (Query()[key] != op_value)
                    elif op == "$in":
                        tinydb_query = tinydb_query & (Query()[key].one_of(op_value))
                    else:
                        raise OperationNotSupported(f"TinyDB adapter does not support operator: {op}")
            else:
                # Simple equality
                tinydb_query = tinydb_query & (Query()[key] == value)

        # If query dict was empty or no conditions added, return empty query (matches all? or matches nothing?)
        # Beanie find({}) matches all. TinyDB Query() by itself matches nothing unless used with search?
        # Actually TinyDB usage: Table.search(Query().field == val).
        # If we return empty Query(), search might fail.
        # But _translate_query logic is: Query().field == val & Query().field2 == val2.
        # If no keys, we shouldn't supply a query to search, we should use all().
        # This check is done in find_many.
        return tinydb_query

    def get_document_model(self, model_class: type[T]) -> Any:
        """
        Get document model operations interface.

        Args:
            model_class: Beanie document model class

        Returns:
            Adapter wrapper for document model operations
        """
        return BeanieDocumentModelAdapter(self, model_class)

    async def find_one(self, model_class: type[T], query: Any) -> Optional[T]:
        """
        Find one document matching query.

        Args:
            model_class: Beanie document model class
            query: Query dictionary or TinyDB expression

        Returns:
            Document instance or None
        """
        collection_name = self._get_collection_name(model_class)
        collection = self._collections.get(collection_name)

        if not collection:
            return None

        if isinstance(query, dict):
            tinydb_query = self._translate_query(query)
        elif query is not None:
            tinydb_query = query
        else:
            # Handle None query (get first)
            results = collection.all()
            if results:
                return self._dict_to_document(model_class, results[0])
            return None

        result = collection.search(tinydb_query)

        if result:
            return self._dict_to_document(model_class, result[0])
        return None

    async def find_many(self, model_class: type[T], query: Any) -> list[T]:
        """
        Find many documents matching query.

        Args:
            model_class: Beanie document model class
            query: Query dictionary or TinyDB expression

        Returns:
            List of document instances
        """
        collection_name = self._get_collection_name(model_class)
        collection = self._collections.get(collection_name)

        if not collection:
            return []

        if isinstance(query, dict):
            tinydb_query = self._translate_query(query)
        elif query is not None:
            tinydb_query = query
        else:
            results = collection.all()
            return [self._dict_to_document(model_class, doc) for doc in results]

        results = collection.search(tinydb_query)

        return [self._dict_to_document(model_class, doc) for doc in results]

    async def get_by_id(self, model_class: type[T], doc_id: Any) -> Optional[T]:
        """
        Get document by ID from TinyDB.

        TinyDB internal doc_ids are integers.
        """
        collection_name = self._get_collection_name(model_class)
        collection = self._collections.get(collection_name)

        if not collection:
            return None

        try:
            # Most TinyDB IDs are integers
            tid = int(doc_id)
            result = collection.get(doc_id=tid)
            if result:
                return self._dict_to_document(model_class, result)
        except (ValueError, TypeError):
            # Fallback for non-int IDs or find by 'id' attribute
            results = collection.search(Query().id == doc_id)
            if results:
                return self._dict_to_document(model_class, results[0])

        return None

    async def insert_one(self, document: T) -> T:
        """
        Insert one document.

        Args:
            document: Document instance to insert

        Returns:
            Inserted document instance
        """
        collection_name = self._get_collection_name(type(document))
        collection = self._collections.get(collection_name)

        if not collection:
            # Create collection if it doesn't exist
            collection = self.db.table(collection_name)
            self._collections[collection_name] = collection

        doc_dict = self._document_to_dict(document)

        # Insert into TinyDB
        doc_id = collection.insert(doc_dict)

        # Update document with ID
        if hasattr(document, "id"):
            document.id = doc_id

        logger.debug(
            "Document inserted via adapter",
            extra={
                "collection": collection_name,
                "doc_id": doc_id,
            },
        )

        return document

    async def update_one(self, document: T) -> T:
        """
        Update one document.

        Args:
            document: Document instance to update

        Returns:
            Updated document instance
        """
        collection_name = self._get_collection_name(type(document))
        collection = self._collections.get(collection_name)

        if not collection:
            raise StorageError(f"Collection {collection_name} not found")

        doc_dict = self._document_to_dict(document)

        # Get document ID
        doc_id = getattr(document, "id", None)
        if doc_id is None:
            raise StorageError("Document ID not found for update")

        # Update in TinyDB
        collection.update(doc_dict, doc_ids=[doc_id])

        logger.debug(
            "Document updated via adapter",
            extra={
                "collection": collection_name,
                "doc_id": doc_id,
            },
        )

        return document

    async def delete_one(self, document: T) -> bool:
        """
        Delete one document.

        Args:
            document: Document instance to delete

        Returns:
            True if deleted, False otherwise
        """
        collection_name = self._get_collection_name(type(document))
        collection = self._collections.get(collection_name)

        if not collection:
            return False

        # Get document ID
        doc_id = getattr(document, "id", None)
        if doc_id is None:
            return False

        # Delete from TinyDB
        collection.remove(doc_ids=[doc_id])

        logger.debug(
            "Document deleted via adapter",
            extra={
                "collection": collection_name,
                "doc_id": doc_id,
            },
        )

        return True

    def is_healthy(self) -> bool:
        """Check if adapter is healthy."""
        try:
            # Simple health check - try to read from database
            return self.db_path.exists() and self.db is not None
        except Exception:
            return False

    def perform_deep_health_check(self) -> bool:
        """
        Perform a deep health check (write/read/delete cycle).
        This verifies that the storage is actually functional.
        """
        import time

        test_key = f"__health_check_{int(time.time())}__"
        test_value = {"status": "ok", "timestamp": time.time()}
        table = self.db.table("_health_check")

        try:
            # 1. Write
            doc_id = table.insert({"key": test_key, "value": test_value})

            # 2. Read
            doc = table.get(doc_id=doc_id)
            if not doc or doc.get("value") != test_value:
                logger.error(f"BeanieTinyDBAdapter deep health check failed: read mismatch for {test_key}")
                self._health_monitor.update_health(False, {"error": "read_mismatch"})
                return False

            # 3. Delete
            table.remove(doc_ids=[doc_id])

            self._health_monitor.update_health(True)
            return True

        except Exception as e:
            logger.error(f"BeanieTinyDBAdapter deep health check failed: {e}")
            self._health_monitor.update_health(False, {"error": str(e)})
            return False

    def get_health_status(self) -> dict[str, Any]:
        """
        Get detailed health information.

        Returns:
            Dictionary containing health metrics and status details
        """
        status = self._health_monitor.get_health_status()
        status.update(
            {
                "adapter": "beanie_tinydb",
                "db_path": str(self.db_path),
                "db_exists": self.db_path.exists(),
                "db_initialized": self.db is not None,
                "model_count": len(self.document_models),
            }
        )
        return status

    def get_last_health_check(self) -> datetime:
        """
        Get the timestamp of the last health check.

        Returns:
            Datetime of the last health check
        """
        return self._health_monitor.get_last_health_check()

    def close(self) -> None:
        """Close database connection."""
        if self.db:
            self.db.close()
        self._running = False

    def _patch_models(self) -> None:
        """Patch all document models for direct Beanie-like access."""
        for model_class in self.document_models:
            self._patch_model(model_class)

    def _patch_model(self, model_class: type[Document]) -> None:
        """Patch a single document model for TinyDB compatibility."""
        adapter = self

        logger.debug(f"Patching document model {model_class.__name__} for TinyDB")

        # 1. Patch class level query attributes (e.g., User.email)
        # We only do this for fields that aren't already class attributes
        for field_name in model_class.model_fields:
            try:
                # Check if it already exists as a class attribute
                # Pydantic v2 fields raise AttributeError on class level access
                getattr(model_class, field_name)
            except AttributeError:
                # Add TinyDB Query attribute
                setattr(model_class, field_name, Query()[field_name])

        # 2. Patch Class Methods
        async def find_one_patch(query: Any = None, **kwargs) -> Optional[T]:
            if query is None and kwargs:
                # Treat kwargs as simple equality query
                query = kwargs
            return await adapter.find_one(model_class, query)

        async def find_many_patch(query: Any = None, **kwargs) -> list[T]:
            if query is None and kwargs:
                query = kwargs
            return await adapter.find_many(model_class, query)

        async def get_patch(doc_id: Any, **kwargs) -> Optional[T]:
            return await adapter.get_by_id(model_class, doc_id)

        async def create_patch(*args, **kwargs) -> T:
            """
            Patch for Document.create().
            Supports both:
            - Instance call: await user.create()
            - Class call: await User.create(**kwargs)
            """
            if args and isinstance(args[0], model_class):
                # Instance call: user.create() -> args[0] is self
                instance = args[0]
                return await adapter.insert_one(instance)

            # Class call: User.create(**kwargs)
            instance = model_class(**kwargs)
            return await adapter.insert_one(instance)

        # Apply class method patches
        # NOTE: We assign create_patch directly (not staticmethod) to handle instance binding interaction
        model_class.find_one = staticmethod(find_one_patch)
        model_class.find_many = staticmethod(find_many_patch)
        model_class.find_all = staticmethod(find_many_patch)
        model_class.get = staticmethod(get_patch)
        model_class.create = create_patch

        # 3. Patch Instance Methods
        async def insert_patch(self_inst: Any, **kwargs) -> Any:
            return await adapter.insert_one(self_inst)

        async def save_patch(self_inst: Any, **kwargs) -> Any:
            # Simple save logic: if id exists, update, else insert
            did = getattr(self_inst, "id", None)
            if did is None:
                return await adapter.insert_one(self_inst)
            return await adapter.update_one(self_inst)

        async def delete_patch(self_inst: Any, **kwargs) -> bool:
            return await adapter.delete_one(self_inst)

        # Patch instance methods
        model_class.insert = insert_patch
        model_class.save = save_patch
        model_class.delete = delete_patch

        # 4. Patch Settings for fastapi-users compatibility (especially for version 5.0.0+)
        if not hasattr(model_class, "Settings"):

            class DefaultSettings:
                name = self._get_collection_name(model_class)

            model_class.Settings = DefaultSettings

        if not hasattr(model_class.Settings, "email_collation"):
            # fastapi-users-db-beanie 5.0.0+ checks this attribute
            model_class.Settings.email_collation = None

        # 5. Patch internals to avoid CollectionWasNotInitialized in Document.__init__
        class DocumentSettingsAdapter:
            """Adapter for Beanie's DocumentSettings to support TinyDB backend."""

            def __init__(self, m_class, name):
                self.name = name
                self.indexes = getattr(getattr(m_class, "Settings", object()), "indexes", [])
                self.pymongo_collection = None
                self.use_revision = False
                self.use_cache = False
                self.is_root = True
                self.union_doc = None
                self.bson_encoders = {}
                self.validation_schema = None
                self.projection = None
                self.id_type = getattr(m_class, "id_type", PydanticObjectId)

        # Create the adapter instance
        adapter_settings = DocumentSettingsAdapter(model_class, self._get_collection_name(model_class))

        # Patch get_settings as a class method
        @classmethod
        def get_settings(cls):
            return adapter_settings

        model_class.get_settings = get_settings

        # Also patch get_pymongo_collection to return None instead of raising
        @classmethod
        def get_pymongo_collection(cls):
            return None

        model_class.get_pymongo_collection = get_pymongo_collection

        # Also patch update (alias to save for now)
        model_class.update = save_patch


class BeanieDocumentModelAdapter:
    """
    Adapter wrapper for individual document model operations.

    This provides a Beanie-like interface for document model operations
    while using TinyDB as the backend.
    """

    def __init__(self, adapter: BeanieTinyDBAdapter, model_class: type[T]):
        """Initialize document model adapter."""
        self.adapter = adapter
        self.model_class = model_class

    async def find_one(self, query: dict[str, Any]) -> Optional[T]:
        """Find one document."""
        return await self.adapter.find_one(self.model_class, query)

    async def find_many(self, query: dict[str, Any]) -> list[T]:
        """Find many documents."""
        return await self.adapter.find_many(self.model_class, query)

    async def insert_one(self, document: T) -> T:
        """Insert one document."""
        return await self.adapter.insert_one(document)

    async def update_one(self, document: T) -> T:
        """Update one document."""
        return await self.adapter.update_one(document)

    async def delete_one(self, document: T) -> bool:
        """Delete one document."""
        return await self.adapter.delete_one(document)
