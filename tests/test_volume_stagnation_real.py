"""Real test for Volume Stagnation feature detection.

This test runs the Volume Stagnation feature on all symbols and prints those with signals.

Run with: poetry run pytest tests/test_volume_stagnation_real.py -v -s
"""

import time

import pytest
from dotenv import load_dotenv

load_dotenv()

from src.datahub.daily_bar import get_daily_bar_from_db
from src.datahub.symbol import get_ts_codes
from src.feature.volume_stagnation import volume_stagnation

# Test configuration
END_DATE = "20251101"  # Scan up to this date


@pytest.mark.skip
class TestVolumeStagnationReal:
    """Real tests for Volume Stagnation feature detection."""

    def test_scan_all_symbols(self):
        """Scan all symbols for Volume Stagnation signals."""
        ts_codes = get_ts_codes()
        total = len(ts_codes)

        print(f"\n{'='*60}")
        print(f"Scanning {total} symbols for Volume Stagnation signals...")
        print(f"End date: {END_DATE}")
        print(f"{'='*60}\n")

        signals_5days = []
        signals_10days = []
        skipped = 0
        errors = 0

        start_time = time.time()

        for i, ts_code in enumerate(ts_codes):
            try:
                df = get_daily_bar_from_db(ts_code, end_date=END_DATE)

                # Need at least 550 days for price quantile calculation
                if len(df) < 550:
                    skipped += 1
                    continue

                # Check for 5-day consecutive signal
                if volume_stagnation(df, min_consecutive_days=3):
                    signals_5days.append(ts_code)

                    # Also check if it meets 3-day criteria
                    if volume_stagnation(df, min_consecutive_days=5):
                        signals_10days.append(ts_code)
                        print(f"[{i+1}/{total}] {ts_code}: 5-day signal!")
                    else:
                        print(f"[{i+1}/{total}] {ts_code}: 3-day signal")

            except Exception as e:
                errors += 1
                if errors <= 5:
                    print(f"[{i+1}/{total}] {ts_code}: Error - {e}")

            # Progress every 500 symbols
            if (i + 1) % 500 == 0:
                elapsed = time.time() - start_time
                print(f"... Progress: {i+1}/{total} ({elapsed:.1f}s)")

        elapsed = time.time() - start_time

        print(f"\n{'='*60}")
        print(f"Scan completed in {elapsed:.1f}s")
        print(f"{'='*60}")
        print(f"Total symbols: {total}")
        print(f"Skipped (insufficient data <550 days): {skipped}")
        print(f"Errors: {errors}")
        print(f"5-day signals found: {len(signals_5days)}")
        print(f"10-day signals found: {len(signals_10days)}")

        if signals_10days:
            print(f"\nSymbols with 5-day Volume Stagnation signals:")
            for code in signals_10days:
                print(f"  - {code}")

        if signals_5days:
            print(f"\nSymbols with 3-day Volume Stagnation signals:")
            for code in signals_5days:
                print(f"  - {code}")

        # This test always passes - it's for scanning, not assertions
        assert True


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
