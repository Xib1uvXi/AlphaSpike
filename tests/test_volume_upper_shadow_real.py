"""Real test for Volume Upper Shadow (放量上影线) feature detection.

This test runs the Volume Upper Shadow feature on all symbols and prints those with signals.

Run with: poetry run pytest tests/test_volume_upper_shadow_real.py -v -s
"""

import time

import pytest
from dotenv import load_dotenv

load_dotenv()

from src.datahub.daily_bar import get_daily_bar_from_db
from src.datahub.symbol import get_ts_codes
from src.feature.volume_upper_shadow import volume_upper_shadow

# Test configuration
END_DATE = "20251212"  # Scan up to this date


@pytest.mark.skip
class TestVolumeUpperShadowReal:
    """Real tests for Volume Upper Shadow feature detection."""

    def test_scan_all_symbols(self):
        """Scan all symbols for Volume Upper Shadow signals."""
        ts_codes = get_ts_codes()
        total = len(ts_codes)

        print(f"\n{'='*60}")
        print(f"Scanning {total} symbols for Volume Upper Shadow signals...")
        print(f"End date: {END_DATE}")
        print(f"{'='*60}\n")

        signals = []
        skipped = 0
        errors = 0

        start_time = time.time()

        for i, ts_code in enumerate(ts_codes):
            try:
                df = get_daily_bar_from_db(ts_code, end_date=END_DATE)

                # Need at least 220 days for price quantile calculation
                if len(df) < 220:
                    skipped += 1
                    continue

                if volume_upper_shadow(df):
                    signals.append(ts_code)
                    print(f"[{i+1}/{total}] {ts_code}: Volume Upper Shadow signal!")

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
        print(f"Skipped (insufficient data <220 days): {skipped}")
        print(f"Errors: {errors}")
        print(f"Volume Upper Shadow signals: {len(signals)}")

        if signals:
            print(f"\nSymbols with Volume Upper Shadow signals:")
            for code in signals:
                print(f"  - {code}")

        # This test always passes - it's for scanning, not assertions
        assert True


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
