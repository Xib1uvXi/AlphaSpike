"""Feature result caching module."""

import json

import redis

from src.common.config import FEATURE_CACHE_TTL_SECONDS
from src.common.redis import get_redis_client

# Re-export for backward compatibility
__all__ = ["get_redis_client", "get_feature_cache", "set_feature_cache"]


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
    Cache feature results with TTL.

    Args:
        feature_name: Feature name (e.g., 'bbc')
        date: Date in YYYYMMDD format
        ts_codes: List of stock codes with signals
        client: Redis client instance
    """
    key = _get_feature_cache_key(feature_name, date)
    client.set(key, json.dumps(ts_codes), ex=FEATURE_CACHE_TTL_SECONDS)
