from __future__ import annotations

"""
Backup Interface - Single Responsibility Principle (Async First).

Defines backup and restore capabilities for storage providers.
"""

from abc import ABC, abstractmethod
from typing import Any, Optional


class IBackupProvider(ABC):
    """
    Interface for backup and restore operations (Async First).
    """

    @abstractmethod
    async def create_backup(self, backup_path: str, metadata: Optional[dict[str, Any]] = None) -> bool:
        """
        Create a backup of the storage data asynchronously.
        """

    @abstractmethod
    async def restore_backup(self, backup_path: str, clear_existing: bool = False) -> bool:
        """
        Restore data from a backup asynchronously.
        """

    @abstractmethod
    async def list_backups(self, backup_dir: str) -> dict[str, dict[str, Any]]:
        """
        List available backups in a directory asynchronously.
        """

    @abstractmethod
    async def validate_backup(self, backup_path: str) -> dict[str, Any]:
        """
        Validate a backup file asynchronously.
        """
