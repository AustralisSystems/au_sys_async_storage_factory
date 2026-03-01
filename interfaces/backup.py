"""
Backup Interface - Single Responsibility Principle.

Defines backup and restore capabilities for storage providers.
Separated from core storage for optional implementation and better testing.

Scaffolded from rest-api-orchestrator `src/services/storage/interfaces/backup.py` (commit a5f0cc2a4f33f312e3d340c06f9aba4688ae9f01).
"""

from abc import ABC, abstractmethod
from typing import Any, Optional

from src.shared.observability.logger_factory import get_logger

logger = get_logger(__name__)


class IBackupProvider(ABC):
    """
    Interface for backup and restore operations.

    This interface handles only backup/restore concerns.
    Not all storage providers need to implement this.
    """

    @abstractmethod
    def create_backup(self, backup_path: str, metadata: Optional[dict[str, Any]] = None) -> bool:
        """
        Create a backup of the storage data.

        Args:
            backup_path: Path where backup should be stored
            metadata: Optional metadata to include with backup

        Returns:
            True if backup was created successfully
        """

    @abstractmethod
    def restore_backup(self, backup_path: str, clear_existing: bool = False) -> bool:
        """
        Restore data from a backup.

        Args:
            backup_path: Path to the backup file
            clear_existing: Whether to clear existing data before restore

        Returns:
            True if restore was successful
        """

    @abstractmethod
    def list_backups(self, backup_dir: str) -> dict[str, dict[str, Any]]:
        """
        List available backups in a directory.

        Args:
            backup_dir: Directory to search for backups

        Returns:
            Dictionary mapping backup names to their metadata
        """

    @abstractmethod
    def validate_backup(self, backup_path: str) -> dict[str, Any]:
        """
        Validate a backup file.

        Args:
            backup_path: Path to the backup file

        Returns:
            Dictionary with validation results and metadata
        """


class BackupManager:
    """
    Simple backup manager implementation.

    This can be composed with storage providers to add backup capabilities.
    """

    def __init__(self, storage_provider: IBackupProvider) -> None:
        """Initialize with a storage provider that supports backup operations."""
        self.storage_provider = storage_provider

    def create_incremental_backup(self, backup_path: str, last_backup_time: Optional[str] = None) -> bool:
        """
        Create an incremental backup.

        Args:
            backup_path: Path for the backup
            last_backup_time: Time of last backup for incremental logic

        Returns:
            True if successful
        """
        # Create incremental backup of data modified since last backup
        try:
            # Get data modified since last backup
            # This would need to be implemented by specific providers
            result = self.storage_provider.create_backup(backup_path)
            return bool(result)
        except Exception:
            return False

    def schedule_backup(self, schedule: str, backup_dir: str) -> bool:
        """
        Schedule regular backups.

        Args:
            schedule: Cron-like schedule string
            backup_dir: Directory to store backups

        Returns:
            True if scheduling was successful
        """
        try:
            from src.api.services.automation.tasks.services.scheduler import TaskScheduler
            from src.shared.services.config.settings import get_settings

            # Get task scheduler instance
            settings = get_settings()
            scheduler = TaskScheduler(settings)

            # Create backup job function
            async def backup_job():
                """Execute scheduled backup."""
                try:
                    # Generate backup filename with timestamp
                    import os
                    from datetime import datetime

                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    backup_path = os.path.join(backup_dir, f"backup_{timestamp}")

                    # Ensure backup directory exists
                    os.makedirs(backup_dir, exist_ok=True)

                    # Execute backup
                    success = await self.create_backup(backup_path)
                    if success:
                        logger.info(f"Scheduled backup completed successfully: {backup_path}")
                        # Cleanup old backups
                        await self.cleanup_old_backups_async(backup_dir)
                    else:
                        logger.error(f"Scheduled backup failed: {backup_path}")
                except Exception as e:
                    logger.error(f"Scheduled backup job failed: {e}")

            # Parse schedule and add job
            if " " in schedule:  # Cron format
                # Parse cron schedule (basic support for common formats)
                schedule_parts = schedule.split()
                if len(schedule_parts) >= 5:
                    minute, hour, day, month, day_of_week = schedule_parts[:5]
                    job_id = f"backup_{backup_dir.replace('/', '_').replace('\\', '_')}"

                    scheduler.add_cron_job(
                        func=backup_job,
                        minute=minute,
                        hour=hour,
                        day=day,
                        month=month,
                        day_of_week=day_of_week,
                        job_id=job_id,
                    )
                    logger.info(f"Scheduled backup job with cron schedule: {schedule}")
                    return True
            else:
                # Try to parse as interval (e.g., "daily", "weekly")
                intervals = {"hourly": 3600, "daily": 86400, "weekly": 604800, "monthly": 2592000}
                if schedule.lower() in intervals:
                    job_id = f"backup_{backup_dir.replace('/', '_').replace('\\', '_')}"
                    scheduler.add_interval_job(func=backup_job, seconds=intervals[schedule.lower()], job_id=job_id)
                    logger.info(f"Scheduled backup job with interval: {schedule}")
                    return True

            logger.error(f"Unsupported schedule format: {schedule}")
            return False

        except Exception as e:
            logger.error(f"Failed to schedule backup: {e}")
            return False

    def cleanup_old_backups(self, backup_dir: str, keep_count: int = 5) -> int:
        """
        Clean up old backups, keeping only the specified number.

        Args:
            backup_dir: Directory containing backups
            keep_count: Number of backups to keep

        Returns:
            Number of backups removed
        """
        try:
            from pathlib import Path

            backup_path = Path(backup_dir)
            if not backup_path.exists():
                logger.warning(f"Backup directory does not exist: {backup_dir}")
                return 0

            # Find all backup files (assuming they follow naming pattern backup_YYYYMMDD_HHMMSS)
            backup_files = []
            for item in backup_path.iterdir():
                if item.is_file() and item.name.startswith("backup_"):
                    try:
                        # Extract timestamp from filename
                        timestamp_str = item.name.replace("backup_", "")
                        # Parse timestamp (YYYYMMDD_HHMMSS)
                        if "_" in timestamp_str:
                            date_str, time_str = timestamp_str.split("_")
                            if len(date_str) == 8 and len(time_str) == 6:
                                backup_files.append((item, timestamp_str))
                    except ValueError:
                        continue

            if len(backup_files) <= keep_count:
                logger.debug(f"Only {len(backup_files)} backups found, keeping all (keep_count: {keep_count})")
                return 0

            # Sort by timestamp (newest first)
            backup_files.sort(key=lambda x: x[1], reverse=True)

            # Remove old backups
            removed_count = 0
            for backup_file, _ in backup_files[keep_count:]:
                try:
                    backup_file.unlink()
                    logger.info(f"Removed old backup: {backup_file}")
                    removed_count += 1
                except Exception as e:
                    logger.error(f"Failed to remove backup {backup_file}: {e}")

            logger.info(f"Backup cleanup completed: removed {removed_count} old backups")
            return removed_count

        except Exception as e:
            logger.error(f"Failed to cleanup old backups: {e}")
            return 0

    async def cleanup_old_backups_async(self, backup_dir: str, keep_count: int = 5) -> int:
        """
        Async version of cleanup_old_backups for use in scheduled jobs.

        Args:
            backup_dir: Directory containing backups
            keep_count: Number of backups to keep

        Returns:
            Number of backups removed
        """
        import asyncio

        return await asyncio.to_thread(self.cleanup_old_backups, backup_dir, keep_count)
