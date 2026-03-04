"""Storage service package scaffolding."""

from .factory import AsyncStorageFactory, get_storage_factory

__all__ = ["AsyncStorageFactory", "get_storage_factory"]
