"""Redis cache module for tracking daily sync status."""

from datetime import datetime

import redis

from src.common.redis import get_redis_client


def _get_today() -> str:
    """Get today's date in YYYYMMDD format."""
    return datetime.now().strftime("%Y%m%d")


def _get_cache_key(ts_code: str, date: str) -> str:
    """
    Generate cache key for a stock's daily sync status.

    Args:
        ts_code: Stock code (e.g., '000001.SZ')
        date: Date in YYYYMMDD format

    Returns:
        Cache key string.
    """
    return f"datahub:sync:{date}:{ts_code}"


def is_synced_today(ts_code: str, client: redis.Redis | None = None) -> bool:
    """
    Check if a stock has been synced today.

    Args:
        ts_code: Stock code (e.g., '000001.SZ')
        client: Optional Redis client. If None, creates a new one.

    Returns:
        True if synced today, False if not synced or Redis unavailable.
    """
    if client is None:
        client = get_redis_client()

    if client is None:
        return False

    key = _get_cache_key(ts_code, _get_today())
    return client.exists(key) > 0


def mark_synced(ts_code: str, client: redis.Redis | None = None) -> None:
    """
    Mark a stock as synced for today.

    The key will expire at midnight (end of today).
    Silently returns if Redis is unavailable.

    Args:
        ts_code: Stock code (e.g., '000001.SZ')
        client: Optional Redis client. If None, creates a new one.
    """
    if client is None:
        client = get_redis_client()

    if client is None:
        return

    key = _get_cache_key(ts_code, _get_today())
    # Set with expiration at end of day (calculate seconds until midnight)
    now = datetime.now()
    midnight = now.replace(hour=23, minute=59, second=59, microsecond=999999)
    seconds_until_midnight = int((midnight - now).total_seconds()) + 1

    client.setex(key, seconds_until_midnight, "1")


def clear_sync_cache(date: str | None = None, client: redis.Redis | None = None) -> int:
    """
    Clear sync cache for a specific date or today.

    Args:
        date: Date in YYYYMMDD format. If None, clears today's cache.
        client: Optional Redis client. If None, creates a new one.

    Returns:
        Number of keys deleted, or 0 if Redis unavailable.
    """
    if client is None:
        client = get_redis_client()

    if client is None:
        return 0

    if date is None:
        date = _get_today()

    pattern = f"datahub:sync:{date}:*"
    keys = list(client.scan_iter(match=pattern))

    if keys:
        return client.delete(*keys)
    return 0


def get_synced_count(date: str | None = None, client: redis.Redis | None = None) -> int:
    """
    Get count of stocks synced for a specific date.

    Args:
        date: Date in YYYYMMDD format. If None, counts today's synced stocks.
        client: Optional Redis client. If None, creates a new one.

    Returns:
        Number of stocks synced, or 0 if Redis unavailable.
    """
    if client is None:
        client = get_redis_client()

    if client is None:
        return 0

    if date is None:
        date = _get_today()

    pattern = f"datahub:sync:{date}:*"
    count = 0
    for _ in client.scan_iter(match=pattern):
        count += 1
    return count
