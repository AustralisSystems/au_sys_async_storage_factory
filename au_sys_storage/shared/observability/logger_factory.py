import logging
import sys
from typing import Optional


def get_component_logger(component: str, subcomponent: Optional[str] = None) -> logging.Logger:
    """
    Returns a logger for a specific component and subcomponent.
    Ensures UTF-8 configuration for consistency.
    """
    # Force UTF-8 stdout encoding for Python CLIs
    if getattr(sys.stdout, "encoding", None) != "utf-8" and hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    name = component
    if subcomponent:
        name = f"{component}.{subcomponent}"

    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.DEBUG)
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter("[%(levelname)s] %(name)s: %(message)s"))
        logger.addHandler(handler)

    return logger
