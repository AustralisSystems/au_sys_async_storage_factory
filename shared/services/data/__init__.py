from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from .data_manager import DataManager

_instance: Optional[DataManager] = None
_lock: asyncio.Lock = asyncio.Lock()


def get_data_manager() -> DataManager:
    """
    Return the singleton DataManager instance (synchronous fast-path).

    This function is safe to call from synchronous contexts after the
    singleton has been initialised.  For concurrent async startup scenarios
    where two coroutines may race on first creation, use
    ``await get_data_manager_async()`` instead.

    Returns:
        The singleton DataManager instance.
    """
    global _instance
    if _instance is None:
        from .data_manager import DataManager

        _instance = DataManager()
    return _instance


async def get_data_manager_async() -> DataManager:
    """
    Return the singleton DataManager instance (async, thread-safe).

    Uses an asyncio.Lock to guarantee that exactly one DataManager is created
    even when multiple coroutines call this function concurrently during
    application startup.

    Returns:
        The singleton DataManager instance.
    """
    global _instance
    if _instance is not None:
        return _instance

    async with _lock:
        # Double-checked locking: re-test after acquiring the lock.
        if _instance is None:
            from .data_manager import DataManager

            _instance = DataManager()

    return _instance


def reload_data_manager() -> DataManager:
    """
    Reset and reload the DataManager singleton (synchronous).

    Discards the current instance and creates a fresh one.  Intended for
    use in testing or when the data_dir configuration changes at runtime.

    Returns:
        The new singleton DataManager instance.
    """
    global _instance
    _instance = None
    return get_data_manager()
