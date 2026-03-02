"""
Lifecycle Mixins - Minimal implementation for storage sub-module.
"""


class BaseLifecycleMixin:
    """
    Base class for objects with a lifecycle.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._running = False

    async def start(self):
        self._running = True

    async def stop(self):
        self._running = False

    @property
    def is_running(self) -> bool:
        return self._running
