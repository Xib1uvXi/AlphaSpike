"""Feature result caching module with SQLite persistence."""

import json

import redis

from src.alphaspike.db import get_feature_result, init_feature_db, save_feature_result
from src.common.config import FEATURE_CACHE_TTL_SECONDS
from src.common.redis import get_redis_client

# Re-export for backward compatibility
__all__ = ["get_redis_client", "get_feature_cache", "set_feature_cache"]

# Initialize feature database on module load
init_feature_db()


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


def get_feature_cache(feature_name: str, date: str, client: redis.Redis | None) -> list[str] | None:
    """
    Get cached feature results with Redis -> SQLite fallback.

    Read strategy:
    1. Check Redis (hot cache)
    2. If Redis hit, ensure SQLite has the data (backfill if missing)
    3. If Redis miss, check SQLite (persistence layer)
    4. If found in SQLite, populate Redis cache
    5. If not found anywhere, return None

    Args:
        feature_name: Feature name (e.g., 'bbc')
        date: Date in YYYYMMDD format
        client: Redis client instance (can be None if Redis unavailable)

    Returns:
        List of ts_codes with signals, or None if not cached.
    """
    # Step 1: Try Redis first (hot cache)
    if client is not None:
        key = _get_feature_cache_key(feature_name, date)
        cached = client.get(key)
        if cached:
            result = json.loads(cached)
            # Step 2: Backfill SQLite if missing (migrate from Redis-only cache)
            if get_feature_result(feature_name, date) is None:
                save_feature_result(feature_name, date, result)
            return result

    # Step 3: Try SQLite (persistence layer)
    result = get_feature_result(feature_name, date)
    if result is not None:
        # Step 4: Populate Redis cache on SQLite hit
        if client is not None:
            key = _get_feature_cache_key(feature_name, date)
            client.set(key, json.dumps(result), ex=FEATURE_CACHE_TTL_SECONDS)
        return result

    # Step 5: Not found anywhere
    return None


def set_feature_cache(feature_name: str, date: str, ts_codes: list[str], client: redis.Redis | None) -> None:
    """
    Cache feature results to both Redis and SQLite (write-through).

    Write strategy:
    1. Always write to SQLite (persistence)
    2. Also write to Redis if available (hot cache)

    Args:
        feature_name: Feature name (e.g., 'bbc')
        date: Date in YYYYMMDD format
        ts_codes: List of stock codes with signals
        client: Redis client instance (can be None if Redis unavailable)
    """
    # Step 1: Always persist to SQLite
    save_feature_result(feature_name, date, ts_codes)

    # Step 2: Also cache to Redis if available
    if client is not None:
        key = _get_feature_cache_key(feature_name, date)
        client.set(key, json.dumps(ts_codes), ex=FEATURE_CACHE_TTL_SECONDS)
