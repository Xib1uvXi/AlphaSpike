"""Tests for the tushare module."""

import pytest

from src.datahub.tushare import get_daily_bar


class TestGetDailyBar:
    """Tests for get_daily_bar function."""

    @pytest.mark.skip(reason="Skip this test")
    def test_returns_dataframe(self):
        result = get_daily_bar("000001.SZ", "20240105", "20240119")

        assert not result.empty
        print(result)
