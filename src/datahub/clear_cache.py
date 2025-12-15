"""CLI to clear Redis cache keys used by AlphaSpike."""

from __future__ import annotations

import argparse

from datahub.cache import get_redis_client


def clear_cache(prefix: str) -> int:
    """
    Delete Redis keys matching the given prefix.

    Args:
        prefix: Key prefix to match

    Returns:
        Number of keys deleted.
    """
    client = get_redis_client()
    pattern = f"{prefix}*"

    keys_to_delete: list[str] = []
    for key in client.scan_iter(match=pattern):
        keys_to_delete.append(key)

    if not keys_to_delete:
        print(f"No keys found matching '{pattern}'.")
        return 0

    deleted = client.delete(*keys_to_delete)
    print(f"Deleted {deleted} keys matching '{pattern}'.")
    return deleted


def main() -> None:
    parser = argparse.ArgumentParser(description="Clear AlphaSpike Redis cache keys.")
    parser.add_argument(
        "--datahub",
        action="store_true",
        help="Clear datahub cache (sync status, daily bar cache)",
    )
    parser.add_argument(
        "--feature",
        action="store_true",
        help="Clear feature scan results cache",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Clear all caches (datahub + feature)",
    )
    args = parser.parse_args()

    # Default to --all if no specific flag is provided
    if not args.datahub and not args.feature and not args.all:
        args.all = True

    total_deleted = 0

    if args.all or args.datahub:
        total_deleted += clear_cache(prefix="datahub:")

    if args.all or args.feature:
        total_deleted += clear_cache(prefix="feature:")

    if total_deleted == 0:
        print("No cache keys found.")
    else:
        print(f"Total: {total_deleted} keys deleted.")


if __name__ == "__main__":
    main()
