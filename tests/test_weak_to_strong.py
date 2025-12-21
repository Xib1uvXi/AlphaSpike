"""Tests for weak_to_strong feature."""

import pandas as pd

from src.feature.weak_to_strong import _get_limit_up_threshold, weak_to_strong


class TestGetLimitUpThreshold:
    """Tests for _get_limit_up_threshold helper."""

    def test_main_board_sz(self):
        """00 prefix should return 9.5%."""
        assert _get_limit_up_threshold("000001.SZ") == 9.5

    def test_main_board_sh(self):
        """60 prefix should return 9.5%."""
        assert _get_limit_up_threshold("600001.SH") == 9.5

    def test_chinext(self):
        """30 prefix should return 19.2%."""
        assert _get_limit_up_threshold("300001.SZ") == 19.2


class TestWeakToStrong:
    """Tests for weak_to_strong feature."""

    def _create_df(self, ts_code: str, data: list[dict]) -> pd.DataFrame:
        """Helper to create test DataFrame."""
        df = pd.DataFrame(data)
        df["ts_code"] = ts_code
        return df

    def test_signal_detected_main_board(self):
        """Signal detected for main board stock (00 prefix)."""
        data = [
            {"open": 10.0, "high": 11.0, "low": 10.0, "close": 11.0, "pct_chg": 10.0},  # T-2: limit up
            {"open": 11.0, "high": 12.1, "low": 11.0, "close": 12.1, "pct_chg": 10.0},  # T-1: limit up
            {"open": 11.5, "high": 11.8, "low": 11.0, "close": 11.2, "pct_chg": -7.4},  # T: gap down, high < prev close
        ]
        df = self._create_df("000001.SZ", data)
        assert weak_to_strong(df) is True

    def test_signal_detected_chinext(self):
        """Signal detected for ChiNext stock (30 prefix)."""
        data = [
            {"open": 10.0, "high": 12.0, "low": 10.0, "close": 12.0, "pct_chg": 20.0},  # T-2: limit up (20%)
            {"open": 12.0, "high": 14.4, "low": 12.0, "close": 14.4, "pct_chg": 20.0},  # T-1: limit up (20%)
            {
                "open": 13.0,
                "high": 14.0,
                "low": 12.5,
                "close": 13.5,
                "pct_chg": -6.25,
            },  # T: gap down, high < prev close
        ]
        df = self._create_df("300001.SZ", data)
        assert weak_to_strong(df) is True

    def test_no_signal_t_minus_2_not_limit_up(self):
        """No signal when T-2 is not limit up."""
        data = [
            {"open": 10.0, "high": 10.5, "low": 10.0, "close": 10.5, "pct_chg": 5.0},  # T-2: only 5%
            {"open": 10.5, "high": 11.55, "low": 10.5, "close": 11.55, "pct_chg": 10.0},  # T-1: limit up
            {"open": 11.0, "high": 11.3, "low": 10.8, "close": 11.1, "pct_chg": -3.9},  # T: gap down
        ]
        df = self._create_df("000001.SZ", data)
        assert weak_to_strong(df) is False

    def test_no_signal_t_minus_1_not_limit_up(self):
        """No signal when T-1 is not limit up."""
        data = [
            {"open": 10.0, "high": 11.0, "low": 10.0, "close": 11.0, "pct_chg": 10.0},  # T-2: limit up
            {"open": 11.0, "high": 11.5, "low": 11.0, "close": 11.5, "pct_chg": 4.5},  # T-1: only 4.5%
            {"open": 11.0, "high": 11.3, "low": 10.8, "close": 11.1, "pct_chg": -3.5},  # T: gap down
        ]
        df = self._create_df("000001.SZ", data)
        assert weak_to_strong(df) is False

    def test_no_signal_gap_up(self):
        """No signal when T opens above previous close (gap up)."""
        data = [
            {"open": 10.0, "high": 11.0, "low": 10.0, "close": 11.0, "pct_chg": 10.0},  # T-2: limit up
            {"open": 11.0, "high": 12.1, "low": 11.0, "close": 12.1, "pct_chg": 10.0},  # T-1: limit up
            {"open": 12.5, "high": 12.6, "low": 11.5, "close": 11.8, "pct_chg": -2.5},  # T: gap UP
        ]
        df = self._create_df("000001.SZ", data)
        assert weak_to_strong(df) is False

    def test_no_signal_high_recovers(self):
        """No signal when T's high recovers previous close."""
        data = [
            {"open": 10.0, "high": 11.0, "low": 10.0, "close": 11.0, "pct_chg": 10.0},  # T-2: limit up
            {"open": 11.0, "high": 12.1, "low": 11.0, "close": 12.1, "pct_chg": 10.0},  # T-1: limit up
            {"open": 11.5, "high": 12.5, "low": 11.0, "close": 11.8, "pct_chg": -2.5},  # T: high > prev close
        ]
        df = self._create_df("000001.SZ", data)
        assert weak_to_strong(df) is False

    def test_no_signal_insufficient_data(self):
        """No signal when less than 3 days of data."""
        data = [
            {"open": 10.0, "high": 11.0, "low": 10.0, "close": 11.0, "pct_chg": 10.0},
            {"open": 11.0, "high": 12.1, "low": 11.0, "close": 12.1, "pct_chg": 10.0},
        ]
        df = self._create_df("000001.SZ", data)
        assert weak_to_strong(df) is False

    def test_no_signal_missing_ts_code(self):
        """No signal when ts_code column is missing."""
        data = [
            {"open": 10.0, "high": 11.0, "low": 10.0, "close": 11.0, "pct_chg": 10.0},
            {"open": 11.0, "high": 12.1, "low": 11.0, "close": 12.1, "pct_chg": 10.0},
            {"open": 11.5, "high": 11.8, "low": 11.0, "close": 11.2, "pct_chg": -7.4},
        ]
        df = pd.DataFrame(data)
        assert weak_to_strong(df) is False

    def test_chinext_threshold_boundary(self):
        """ChiNext stock at exactly 19.2% should not trigger (need > 19.2)."""
        data = [
            {"open": 10.0, "high": 11.92, "low": 10.0, "close": 11.92, "pct_chg": 19.2},  # T-2: exactly 19.2%
            {"open": 11.92, "high": 14.21, "low": 11.92, "close": 14.21, "pct_chg": 19.2},  # T-1: exactly 19.2%
            {"open": 13.0, "high": 14.0, "low": 12.5, "close": 13.5, "pct_chg": -5.0},  # T: gap down
        ]
        df = self._create_df("300001.SZ", data)
        assert weak_to_strong(df) is False
