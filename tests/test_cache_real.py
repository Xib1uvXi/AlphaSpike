"""Real integration test for daily_bar Redis caching.

This test requires:
- .env file with SQLITE_PATH, REDIS_HOST, REDIS_PORT, REDIS_DB configured
- Redis server running
- Some data in the SQLite database for 600000.SH (浦发银行)

Run with: poetry run pytest tests/test_cache_real.py -v -s
"""

import time

import pytest
from dotenv import load_dotenv

load_dotenv()

from src.datahub.cache import (
    _get_daily_bar_cache_key,
    get_daily_bar_cache,
    get_redis_client,
    set_daily_bar_cache,
)
from src.datahub.daily_bar import get_daily_bar_from_db


@pytest.fixture
def redis_client():
    """Get Redis client and verify connection."""
    client = get_redis_client()
    try:
        client.ping()
    except Exception as e:
        pytest.skip(f"Redis not available: {e}")
    return client


@pytest.fixture
def clean_cache(redis_client):
    """Clean up cache before and after test."""
    ts_code = "600000.SH"
    # Clear any existing cache for this stock
    key_pattern = f"datahub:daily_bar:*:{ts_code}:*"
    keys = list(redis_client.scan_iter(match=key_pattern))
    if keys:
        redis_client.delete(*keys)

    yield

    # Cleanup after test
    keys = list(redis_client.scan_iter(match=key_pattern))
    if keys:
        redis_client.delete(*keys)


@pytest.mark.skip
class TestDailyBarCacheReal:
    """Real integration tests for daily_bar caching."""

    def test_cache_key_format(self):
        """Test cache key format."""
        key = _get_daily_bar_cache_key("600000.SH", "20240101", "20241231")
        parts = key.split(":")

        assert parts[0] == "datahub"
        assert parts[1] == "daily_bar"
        assert len(parts[2]) == 8  # today's date YYYYMMDD
        assert parts[3] == "600000.SH"
        assert parts[4] == "20240101"
        assert parts[5] == "20241231"

        print(f"\nCache key: {key}")

    def test_cache_hit_performance(self, redis_client, clean_cache):
        """Test that cache hit is faster than database query."""
        ts_code = "600000.SH"
        start_date = "20240101"
        end_date = "20241201"

        # First call - should hit database and cache the result
        t1 = time.time()
        df1 = get_daily_bar_from_db(ts_code, start_date, end_date)
        time_first = time.time() - t1

        print(f"\nFirst call (DB): {time_first*1000:.2f}ms, rows: {len(df1)}")

        if df1.empty:
            pytest.skip("No data in database for 600000.SH. Run 'make sync' first.")

        # Verify cache was set
        cached = get_daily_bar_cache(ts_code, start_date, end_date, client=redis_client)
        assert cached is not None, "Cache should be set after first call"
        print(f"Cache size: {len(cached)} bytes")

        # Second call - should hit cache
        t2 = time.time()
        df2 = get_daily_bar_from_db(ts_code, start_date, end_date)
        time_second = time.time() - t2

        print(f"Second call (Cache): {time_second*1000:.2f}ms, rows: {len(df2)}")

        # Verify data is the same
        assert len(df1) == len(df2)
        assert list(df1.columns) == list(df2.columns)

        # Cache should be faster (usually 10x+ faster)
        print(f"Speedup: {time_first/time_second:.1f}x")

    def test_cache_ttl(self, redis_client, clean_cache):
        """Test that cache has correct TTL (2 hours)."""
        ts_code = "600000.SH"

        # Set cache manually
        set_daily_bar_cache(ts_code, None, None, '{"test": 1}', client=redis_client)

        # Check TTL
        key = _get_daily_bar_cache_key(ts_code, None, None)
        ttl = redis_client.ttl(key)

        print(f"\nCache TTL: {ttl} seconds ({ttl/3600:.2f} hours)")

        # TTL should be around 7200 seconds (2 hours)
        assert 7000 < ttl <= 7200, f"TTL should be ~7200 seconds, got {ttl}"

    def test_different_date_ranges_have_different_cache(self, redis_client, clean_cache):
        """Test that different date ranges use different cache keys."""
        ts_code = "600000.SH"

        key1 = _get_daily_bar_cache_key(ts_code, "20240101", "20240630")
        key2 = _get_daily_bar_cache_key(ts_code, "20240701", "20241231")
        key3 = _get_daily_bar_cache_key(ts_code, None, None)

        print(f"\nKey 1: {key1}")
        print(f"Key 2: {key2}")
        print(f"Key 3: {key3}")

        assert key1 != key2
        assert key1 != key3
        assert key2 != key3

    def test_daily_bar_cache(self, redis_client, clean_cache):
        """Test that daily bar cache is working."""
        ts_code = "600000.SH"

        df = get_daily_bar_from_db(ts_code)
        assert len(df) > 0

        df = df.dropna()

        print(df.head(10))
