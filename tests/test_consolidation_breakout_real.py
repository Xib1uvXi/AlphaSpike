"""Real test for Consolidation Breakout (横盘突破) feature detection.

This test runs the Consolidation Breakout feature on all symbols and prints those with signals.

Run with: poetry run pytest tests/test_consolidation_breakout_real.py -v -s
"""

import time

import pytest
from dotenv import load_dotenv

load_dotenv()

from src.datahub.daily_bar import get_daily_bar_from_db
from src.datahub.symbol import get_ts_codes
from src.feature.consolidation_breakout import consolidation_breakout

# Test configuration
END_DATE = "20251212"  # Scan up to this date


@pytest.mark.skip
class TestConsolidationBreakoutReal:
    """Real tests for Consolidation Breakout feature detection."""

    def test_scan_all_symbols(self):
        """Scan all symbols for Consolidation Breakout signals."""
        ts_codes = get_ts_codes()
        total = len(ts_codes)

        print(f"\n{'='*60}")
        print(f"Scanning {total} symbols for Consolidation Breakout signals...")
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

                # Need at least 60 days for indicator calculation
                if len(df) < 60:
                    skipped += 1
                    continue

                # Check for 3-day consolidation signal (default)
                if consolidation_breakout(df, min_consolidation_days=3):
                    signals_5days.append(ts_code)

                    # Also check if it meets 5-day criteria (stricter)
                    if consolidation_breakout(df, min_consolidation_days=5):
                        signals_10days.append(ts_code)
                        print(f"[{i+1}/{total}] {ts_code}: 5-day consolidation breakout!")
                    else:
                        print(f"[{i+1}/{total}] {ts_code}: 3-day consolidation breakout")

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
        print(f"Skipped (insufficient data <60 days): {skipped}")
        print(f"Errors: {errors}")
        print(f"3-day consolidation breakout signals: {len(signals_5days)}")
        print(f"5-day consolidation breakout signals: {len(signals_10days)}")

        if signals_10days:
            print(f"\nSymbols with 5-day Consolidation Breakout signals:")
            for code in signals_10days:
                print(f"  - {code}")

        if signals_5days:
            print(f"\nSymbols with 3-day Consolidation Breakout signals:")
            for code in signals_5days:
                print(f"  - {code}")

        # This test always passes - it's for scanning, not assertions
        assert True


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
