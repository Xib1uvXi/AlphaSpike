"""Real test for Bullish Cannon (多方炮) feature detection.

This test runs the Bullish Cannon feature on all symbols and prints those with signals.

Run with: poetry run pytest tests/test_bullish_cannon_real.py -v -s
"""

import time

import pytest
from dotenv import load_dotenv

load_dotenv()

from src.datahub.daily_bar import get_daily_bar_from_db
from src.datahub.symbol import get_ts_codes
from src.feature.bullish_cannon import bullish_cannon

# Test configuration
END_DATE = "20251212"  # Scan up to this date


@pytest.mark.skip
class TestBullishCannonKnownCases:
    """Test known cases for Bullish Cannon feature detection."""

    def test_300043_sz_20250609(self):
        """300043.SZ should have Bullish Cannon signal on 2025-06-09."""
        df = get_daily_bar_from_db("300043.SZ", end_date="20250609")
        assert len(df) >= 30, "Insufficient data for 300043.SZ"
        assert bullish_cannon(df) is True, "300043.SZ should have Bullish Cannon signal on 20250609"


@pytest.mark.skip
class TestBullishCannonReal:
    """Real tests for Bullish Cannon feature detection."""

    def test_scan_all_symbols(self):
        """Scan all symbols for Bullish Cannon signals."""
        ts_codes = get_ts_codes()
        total = len(ts_codes)

        print(f"\n{'='*60}")
        print(f"Scanning {total} symbols for Bullish Cannon signals...")
        print(f"End date: {END_DATE}")
        print(f"{'='*60}\n")

        signals = []
        skipped = 0
        errors = 0

        start_time = time.time()

        for i, ts_code in enumerate(ts_codes):
            try:
                df = get_daily_bar_from_db(ts_code, end_date=END_DATE)

                # Need at least 30 days for HHV calculation
                if len(df) < 30:
                    skipped += 1
                    continue

                if bullish_cannon(df):
                    signals.append(ts_code)
                    print(f"[{i+1}/{total}] {ts_code}: Bullish Cannon signal!")

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
        print(f"Skipped (insufficient data <30 days): {skipped}")
        print(f"Errors: {errors}")
        print(f"Bullish Cannon signals: {len(signals)}")

        if signals:
            print(f"\nSymbols with Bullish Cannon signals:")
            for code in signals:
                print(f"  - {code}")

        # This test always passes - it's for scanning, not assertions
        assert True


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
