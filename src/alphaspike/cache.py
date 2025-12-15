"""Feature result caching module."""

import json
import os

import redis
from dotenv import load_dotenv

load_dotenv()


def get_redis_client() -> redis.Redis | None:
    """
    Get Redis client, return None if unavailable.

    Environment variables:
        REDIS_HOST: Redis host (default: localhost)
        REDIS_PORT: Redis port (default: 6379)
        REDIS_DB: Redis database number (default: 0)
        REDIS_PASSWORD: Redis password (optional)

    Returns:
        redis.Redis or None if connection fails.
    """
    try:
        host = os.getenv("REDIS_HOST", "localhost")
        port = int(os.getenv("REDIS_PORT", "6379"))
        db = int(os.getenv("REDIS_DB", "0"))
        password = os.getenv("REDIS_PASSWORD")
        client = redis.Redis(host=host, port=port, db=db, password=password, decode_responses=True)
        client.ping()
        return client
    except Exception:
        return None


def _get_feature_cache_key(feature_name: str, date: str) -> str:
    """
    Generate cache key for feature results.

    Args:
        feature_name: Feature name (e.g., 'bbc', 'volume_upper_shadow')
        date: Date in YYYYMMDD format

    Returns:
        Cache key string.
    """
    return f"feature:{feature_name}:{date}"


def get_feature_cache(feature_name: str, date: str, client: redis.Redis) -> list[str] | None:
    """
    Get cached feature results.

    Args:
        feature_name: Feature name (e.g., 'bbc')
        date: Date in YYYYMMDD format
        client: Redis client instance

    Returns:
        List of ts_codes with signals, or None if not cached.
    """
    key = _get_feature_cache_key(feature_name, date)
    cached = client.get(key)
    if cached:
        return json.loads(cached)
    return None


def set_feature_cache(feature_name: str, date: str, ts_codes: list[str], client: redis.Redis) -> None:
    """
    Cache feature results (no TTL - permanent).

    Args:
        feature_name: Feature name (e.g., 'bbc')
        date: Date in YYYYMMDD format
        ts_codes: List of stock codes with signals
        client: Redis client instance
    """
    key = _get_feature_cache_key(feature_name, date)
    client.set(key, json.dumps(ts_codes))
