"""Real test for BBC (Big Bearish Candle) feature detection.

This test runs the BBC feature on all symbols and prints those with signals.

Run with: poetry run pytest tests/test_bbc_real.py -v -s
"""

import time

import pytest
from dotenv import load_dotenv

load_dotenv()

from src.datahub.daily_bar import get_daily_bar_from_db
from src.datahub.symbol import get_ts_codes
from src.feature.bbc import bbc


@pytest.mark.skip
class TestBBCReal:
    """Real tests for BBC feature detection."""

    def test_scan_all_symbols_for_bbc(self):
        """Scan all symbols for BBC signals and print those with True."""
        ts_codes = get_ts_codes()
        total = len(ts_codes)

        print(f"\n{'='*60}")
        print(f"Scanning {total} symbols for BBC signals...")
        print(f"{'='*60}\n")

        bbc_signals = []
        skipped = 0
        errors = 0

        start_time = time.time()

        for i, ts_code in enumerate(ts_codes):
            try:
                df = get_daily_bar_from_db(ts_code)

                if len(df) < 1000:
                    skipped += 1
                    continue

                result = bbc(df)

                if result:
                    bbc_signals.append(ts_code)
                    print(f"[{i+1}/{total}] {ts_code}: BBC signal detected!")

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
        print(f"Skipped (insufficient data): {skipped}")
        print(f"Errors: {errors}")
        print(f"BBC signals found: {len(bbc_signals)}")

        if bbc_signals:
            print(f"\nSymbols with BBC signals:")
            for code in bbc_signals:
                print(f"  - {code}")

        # This test always passes - it's for scanning, not assertions
        assert True


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
