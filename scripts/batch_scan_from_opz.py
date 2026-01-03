#!/usr/bin/env python3
"""Batch scan volume_upper_shadow for all dates that have volume_upper_shadow_opz results in Redis."""

import subprocess
import sys
from dotenv import load_dotenv

load_dotenv()

from src.common.redis import get_redis_client


def get_opz_dates() -> list[str]:
    """Get all dates that have volume_upper_shadow_opz results in Redis."""
    client = get_redis_client()
    if client is None:
        print("Error: Redis not available")
        sys.exit(1)

    # Find all keys matching feature:volume_upper_shadow_opz:*
    pattern = "feature:volume_upper_shadow_opz:*"
    keys = client.keys(pattern)

    # Extract dates from keys
    dates = []
    for key in keys:
        # Key format: feature:volume_upper_shadow_opz:YYYYMMDD
        key_str = key.decode() if isinstance(key, bytes) else key
        parts = key_str.split(":")
        if len(parts) == 3:
            dates.append(parts[2])

    return sorted(dates)


def main():
    dates = get_opz_dates()

    if not dates:
        print("No dates found for volume_upper_shadow_opz in Redis")
        return

    print(f"Found {len(dates)} dates for volume_upper_shadow_opz:")
    for date in dates:
        print(f"  {date}")

    print()
    print("Starting batch scan for volume_upper_shadow...")
    print("=" * 60)

    for i, date in enumerate(dates, 1):
        print(f"\n[{i}/{len(dates)}] Scanning volume_upper_shadow for {date}...")
        result = subprocess.run(
            [
                "make",
                "scan",
                f"END_DATE={date}",
                "FEATURE=volume_upper_shadow",
                "NO_CACHE=1",
            ],
            cwd="/Users/xib/workspaces/xib/AlphaSpike",
        )
        if result.returncode != 0:
            print(f"Warning: Scan failed for {date}")

    print()
    print("=" * 60)
    print("Batch scan completed!")


if __name__ == "__main__":
    main()
