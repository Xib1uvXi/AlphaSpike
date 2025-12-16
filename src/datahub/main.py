"""Main script to synchronize daily bar data for all stocks."""

import argparse
import sys
import time
import warnings

from src.datahub.cache import (
    get_redis_client,
    get_synced_count,
    is_synced_today,
    mark_synced,
)
from src.datahub.daily_bar import sync_daily_bar
from src.datahub.symbol import get_ts_codes

warnings.filterwarnings("ignore")


def format_duration(seconds: float) -> str:
    """Format duration in adaptive units (hours, minutes, seconds)."""
    if seconds < 60:
        return f"{seconds:.0f}s"
    if seconds < 3600:
        minutes = int(seconds // 60)
        secs = int(seconds % 60)
        return f"{minutes}m{secs}s"

    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    return f"{hours}h{minutes}m"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync daily bar data for all stocks.")
    parser.add_argument(
        "--end-date",
        dest="end_date",
        help="Optional end date in YYYYMMDD format. Defaults to latest trading day.",
    )
    return parser.parse_args()


def sync_all_daily_bars(
    end_date: str | None = None,
):  # pylint: disable=too-many-locals,too-many-statements,broad-exception-caught
    """Synchronize daily bar data for all stocks."""
    print("Loading all stock symbols...")
    ts_codes = get_ts_codes()
    total = len(ts_codes)
    print(f"Found {total} stocks to sync")
    if end_date:
        print(f"Using custom end date: {end_date}")

    # Check Redis connection and get already synced count
    try:
        redis_client = get_redis_client()
        redis_client.ping()
        redis_info = redis_client.connection_pool.connection_kwargs
        print(f"Redis connected: {redis_info.get('host', 'localhost')}:{redis_info.get('port', 6379)}")
        already_synced = get_synced_count(client=redis_client)
        print(f"Already synced today: {already_synced}")
    except Exception as e:  # pylint: disable=broad-exception-caught
        print(f"Redis connection failed: {e}")
        redis_client = None

    print()

    success_count = 0
    error_count = 0
    skip_count = 0
    cache_skip_count = 0
    total_records = 0

    start_time = time.time()

    for i, ts_code in enumerate(ts_codes, 1):
        # Check if already synced today (via Redis cache)
        if redis_client and is_synced_today(ts_code, client=redis_client):
            cache_skip_count += 1
            print(f"[{i}/{total}] {ts_code}: cached (synced today)")
            continue

        try:
            count = sync_daily_bar(ts_code, end_date=end_date)

            if count > 0:
                success_count += 1
                total_records += count
                status = f"+{count} records"
            else:
                skip_count += 1
                status = "up to date"

            # Mark as synced in Redis cache
            if redis_client:
                mark_synced(ts_code, client=redis_client)

            # Progress output
            elapsed = time.time() - start_time
            processed = i - cache_skip_count
            if processed > 0:
                avg_time = elapsed / processed
                remaining_to_process = total - i
                remaining = avg_time * remaining_to_process
                print(f"[{i}/{total}] {ts_code}: {status} (ETA: {format_duration(remaining)})")
            else:
                print(f"[{i}/{total}] {ts_code}: {status}")

        except Exception as e:  # pylint: disable=broad-exception-caught
            error_count += 1
            print(f"[{i}/{total}] {ts_code}: ERROR - {e}")

    # Summary
    elapsed = time.time() - start_time
    print("\n" + "=" * 50)
    print("Sync completed!")
    print(f"  Total stocks: {total}")
    print(f"  Updated: {success_count}")
    print(f"  Skipped (up to date): {skip_count}")
    print(f"  Skipped (cached): {cache_skip_count}")
    print(f"  Errors: {error_count}")
    print(f"  Total new records: {total_records}")
    print(f"  Time elapsed: {format_duration(elapsed)}")
    print("=" * 50)


if __name__ == "__main__":
    try:
        args = parse_args()
        sync_all_daily_bars(end_date=args.end_date)
    except KeyboardInterrupt:
        print("\n\nSync interrupted by user")
        sys.exit(1)
