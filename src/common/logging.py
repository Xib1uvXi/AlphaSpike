"""Logging configuration module."""

import logging
import os
import sys

# Default log level from environment
DEFAULT_LOG_LEVEL = os.getenv("LOG_LEVEL", "WARNING")


def get_logger(name: str) -> logging.Logger:
    """
    Get a configured logger.

    Args:
        name: Logger name (typically __name__)

    Returns:
        Configured logger instance.
    """
    logger = logging.getLogger(name)

    # Only configure if no handlers exist (avoid duplicate handlers)
    if not logger.handlers:
        logger.setLevel(getattr(logging, DEFAULT_LOG_LEVEL.upper(), logging.WARNING))

        # Console handler with simple format
        handler = logging.StreamHandler(sys.stderr)
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger
