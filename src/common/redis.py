"""Unified Redis client management module."""

import os

import redis
from dotenv import load_dotenv

load_dotenv()

# Module-level connection pool for reuse across calls
_connection_pool: redis.ConnectionPool | None = None


def _get_connection_pool() -> redis.ConnectionPool:
    """
    Get or create a Redis connection pool.

    Returns:
        redis.ConnectionPool: Shared connection pool instance.
    """
    global _connection_pool  # pylint: disable=global-statement

    if _connection_pool is None:
        host = os.getenv("REDIS_HOST", "localhost")
        port = int(os.getenv("REDIS_PORT", "6379"))
        db = int(os.getenv("REDIS_DB", "0"))
        password = os.getenv("REDIS_PASSWORD")

        _connection_pool = redis.ConnectionPool(
            host=host,
            port=port,
            db=db,
            password=password,
            decode_responses=True,
        )

    return _connection_pool


def get_redis_client() -> redis.Redis | None:
    """
    Get Redis client with connection pooling.

    Uses a shared connection pool for better performance.
    Returns None if Redis is unavailable (consistent error handling).

    Environment variables:
        REDIS_HOST: Redis host (default: localhost)
        REDIS_PORT: Redis port (default: 6379)
        REDIS_DB: Redis database number (default: 0)
        REDIS_PASSWORD: Redis password (optional)

    Returns:
        redis.Redis or None if connection fails.
    """
    try:
        pool = _get_connection_pool()
        client = redis.Redis(connection_pool=pool)
        client.ping()
        return client
    except Exception:  # pylint: disable=broad-exception-caught
        return None
