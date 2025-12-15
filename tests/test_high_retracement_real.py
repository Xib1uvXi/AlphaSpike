"""Real test for High Retracement (冲高回落) feature detection.

This test runs the High Retracement feature on all symbols and prints those with signals.

Run with: poetry run pytest tests/test_high_retracement_real.py -v -s
"""

import time

import pytest
from dotenv import load_dotenv

load_dotenv()

from src.datahub.daily_bar import get_daily_bar_from_db
from src.datahub.symbol import get_ts_codes
from src.feature.high_retracement import high_retracement

# Test configuration
END_DATE = "20251212"  # Scan up to this date


@pytest.mark.skip
class TestHighRetracementReal:
    """Real tests for High Retracement feature detection."""

    def test_scan_all_symbols(self):
        """Scan all symbols for High Retracement signals."""
        ts_codes = get_ts_codes()
        total = len(ts_codes)

        print(f"\n{'='*60}")
        print(f"Scanning {total} symbols for High Retracement signals...")
        print(f"End date: {END_DATE}")
        print(f"{'='*60}\n")

        signals_2days = []
        signals_3days = []
        signals_5days = []
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

                # Check for 2-day consecutive signal
                if high_retracement(df, min_consecutive_days=2):
                    signals_2days.append(ts_code)

                    # Also check if it meets 3-day criteria
                    if high_retracement(df, min_consecutive_days=3):
                        signals_3days.append(ts_code)

                        # Also check if it meets 5-day criteria
                        if high_retracement(df, min_consecutive_days=5):
                            signals_5days.append(ts_code)
                            print(f"[{i+1}/{total}] {ts_code}: 5-day signal!")
                        else:
                            print(f"[{i+1}/{total}] {ts_code}: 3-day signal")
                    else:
                        print(f"[{i+1}/{total}] {ts_code}: 2-day signal")

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
        print(f"2-day signals found: {len(signals_2days)}")
        print(f"3-day signals found: {len(signals_3days)}")
        print(f"5-day signals found: {len(signals_5days)}")

        if signals_5days:
            print(f"\nSymbols with 5-day High Retracement signals:")
            for code in signals_5days:
                print(f"  - {code}")

        if signals_3days:
            print(f"\nSymbols with 3-day High Retracement signals:")
            for code in signals_3days:
                print(f"  - {code}")

        if signals_2days:
            print(f"\nSymbols with 2-day High Retracement signals:")
            for code in signals_2days:
                print(f"  - {code}")

        # This test always passes - it's for scanning, not assertions
        assert True


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
