"""Redis cache module for tracking daily sync status."""

import os
from datetime import datetime

import redis
from dotenv import load_dotenv

load_dotenv()


def get_redis_client() -> redis.Redis:
    """
    Get Redis client from environment configuration.

    Environment variables:
        REDIS_HOST: Redis host (default: localhost)
        REDIS_PORT: Redis port (default: 6379)
        REDIS_DB: Redis database number (default: 0)
        REDIS_PASSWORD: Redis password (optional)

    Returns:
        redis.Redis: Redis client instance.
    """
    host = os.getenv("REDIS_HOST", "localhost")
    port = int(os.getenv("REDIS_PORT", "6379"))
    db = int(os.getenv("REDIS_DB", "0"))
    password = os.getenv("REDIS_PASSWORD")

    return redis.Redis(host=host, port=port, db=db, password=password, decode_responses=True)


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
        True if synced today, False otherwise.
    """
    if client is None:
        client = get_redis_client()

    key = _get_cache_key(ts_code, _get_today())
    return client.exists(key) > 0


def mark_synced(ts_code: str, client: redis.Redis | None = None) -> None:
    """
    Mark a stock as synced for today.

    The key will expire at midnight (end of today).

    Args:
        ts_code: Stock code (e.g., '000001.SZ')
        client: Optional Redis client. If None, creates a new one.
    """
    if client is None:
        client = get_redis_client()

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
        Number of keys deleted.
    """
    if client is None:
        client = get_redis_client()

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
        Number of stocks synced.
    """
    if client is None:
        client = get_redis_client()

    if date is None:
        date = _get_today()

    pattern = f"datahub:sync:{date}:*"
    count = 0
    for _ in client.scan_iter(match=pattern):
        count += 1
    return count


# Daily bar cache configuration
_DAILY_BAR_CACHE_TTL = 7200  # 2 hours in seconds


def _get_daily_bar_cache_key(ts_code: str, start_date: str | None, end_date: str | None) -> str:
    """
    Generate cache key for daily bar query.

    Args:
        ts_code: Stock code (e.g., '000001.SZ')
        start_date: Start date in YYYYMMDD format (or None)
        end_date: End date in YYYYMMDD format (or None)

    Returns:
        Cache key string.
    """
    today = _get_today()
    start = start_date or "none"
    end = end_date or "none"
    return f"datahub:daily_bar:{today}:{ts_code}:{start}:{end}"


def get_daily_bar_cache(
    ts_code: str,
    start_date: str | None,
    end_date: str | None,
    client: redis.Redis | None = None,
) -> str | None:
    """
    Get daily bar data from cache.

    Args:
        ts_code: Stock code (e.g., '000001.SZ')
        start_date: Start date in YYYYMMDD format (or None)
        end_date: End date in YYYYMMDD format (or None)
        client: Optional Redis client. If None, creates a new one.

    Returns:
        Cached JSON string, or None if not found.
    """
    if client is None:
        client = get_redis_client()

    key = _get_daily_bar_cache_key(ts_code, start_date, end_date)
    return client.get(key)


def set_daily_bar_cache(
    ts_code: str,
    start_date: str | None,
    end_date: str | None,
    data: str,
    client: redis.Redis | None = None,
) -> None:
    """
    Set daily bar data in cache with 2-hour TTL.

    Args:
        ts_code: Stock code (e.g., '000001.SZ')
        start_date: Start date in YYYYMMDD format (or None)
        end_date: End date in YYYYMMDD format (or None)
        data: JSON string of DataFrame to cache
        client: Optional Redis client. If None, creates a new one.
    """
    if client is None:
        client = get_redis_client()

    key = _get_daily_bar_cache_key(ts_code, start_date, end_date)
    client.setex(key, _DAILY_BAR_CACHE_TTL, data)
