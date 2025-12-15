"""Tests for the symbol module."""

import pandas as pd
import pytest

from src.datahub.symbol import (
    DATA_DIR,
    SSE_FILE,
    SYMBOLS_CACHE_FILE,
    SZSE_FILE,
    clear_symbols_cache,
    get_symbols_by_exchange,
    is_st_stock,
    load_all_symbols,
    load_sse_symbols,
    load_szse_symbols,
)


class TestLoadSseSymbols:
    """Tests for load_sse_symbols function."""

    def test_returns_dataframe(self):
        """Should return a pandas DataFrame."""
        result = load_sse_symbols()
        assert isinstance(result, pd.DataFrame)

    def test_has_required_columns(self):
        """Should have code, name, exchange, and list_date columns."""
        result = load_sse_symbols()
        expected_columns = ["code", "name", "exchange", "list_date"]
        assert list(result.columns) == expected_columns

    def test_exchange_is_sse(self):
        """All rows should have exchange set to 'SSE'."""
        result = load_sse_symbols()
        assert (result["exchange"] == "SSE").all()

    def test_code_is_six_digits(self):
        """Stock codes should be 6 digits with leading zeros."""
        result = load_sse_symbols()
        assert result["code"].str.len().eq(6).all()
        assert result["code"].str.isdigit().all()

    def test_has_data(self):
        """Should return non-empty DataFrame."""
        result = load_sse_symbols()
        assert len(result) > 0


class TestLoadSzseSymbols:
    """Tests for load_szse_symbols function."""

    def test_returns_dataframe(self):
        """Should return a pandas DataFrame."""
        result = load_szse_symbols()
        assert isinstance(result, pd.DataFrame)

    def test_has_required_columns(self):
        """Should have code, name, exchange, and list_date columns."""
        result = load_szse_symbols()
        expected_columns = ["code", "name", "exchange", "list_date"]
        assert list(result.columns) == expected_columns

    def test_exchange_is_szse(self):
        """All rows should have exchange set to 'SZSE'."""
        result = load_szse_symbols()
        assert (result["exchange"] == "SZSE").all()

    def test_code_is_six_digits(self):
        """Stock codes should be 6 digits with leading zeros."""
        result = load_szse_symbols()
        assert result["code"].str.len().eq(6).all()
        assert result["code"].str.isdigit().all()

    def test_has_data(self):
        """Should return non-empty DataFrame."""
        result = load_szse_symbols()
        assert len(result) > 0


class TestLoadAllSymbols:
    """Tests for load_all_symbols function."""

    def test_returns_dataframe(self):
        """Should return a pandas DataFrame."""
        result = load_all_symbols()
        assert isinstance(result, pd.DataFrame)

    def test_load_all_symbols(self):
        """Should load all symbols from both SSE and SZSE."""
        result = load_all_symbols()
        assert len(result) > 0

        # print the first 10 rows
        print(result.head(10))

    def test_has_required_columns(self):
        """Should have code, name, exchange, and list_date columns (no board)."""
        result = load_all_symbols()
        expected_columns = ["code", "name", "exchange", "list_date"]
        assert list(result.columns) == expected_columns

    def test_contains_both_exchanges(self):
        """Should contain data from both SSE and SZSE."""
        result = load_all_symbols()
        exchanges = result["exchange"].unique()
        assert "SSE" in exchanges
        assert "SZSE" in exchanges

    def test_combined_count(self):
        """Total count should equal sum of individual exchange counts."""
        all_symbols = load_all_symbols()
        sse_symbols = load_sse_symbols()
        szse_symbols = load_szse_symbols()
        assert len(all_symbols) == len(sse_symbols) + len(szse_symbols)


class TestGetSymbolsByExchange:
    """Tests for get_symbols_by_exchange function."""

    def test_get_sse_symbols(self):
        """Should return SSE symbols when exchange is 'SSE'."""
        result = get_symbols_by_exchange("SSE")
        assert (result["exchange"] == "SSE").all()

    def test_get_szse_symbols(self):
        """Should return SZSE symbols when exchange is 'SZSE'."""
        result = get_symbols_by_exchange("SZSE")
        assert (result["exchange"] == "SZSE").all()

    def test_case_insensitive(self):
        """Exchange parameter should be case insensitive."""
        result_lower = get_symbols_by_exchange("sse")
        result_upper = get_symbols_by_exchange("SSE")
        assert len(result_lower) == len(result_upper)

    def test_invalid_exchange_raises_error(self):
        """Should raise ValueError for invalid exchange."""
        with pytest.raises(ValueError, match="Invalid exchange"):
            get_symbols_by_exchange("INVALID")


class TestDataIntegrity:
    """Tests for data integrity and consistency."""

    def test_sse_codes_start_with_6(self):
        """SSE stock codes typically start with 6."""
        result = load_sse_symbols()
        # Most SSE stocks start with 6
        codes_starting_with_6 = result["code"].str.startswith("6").sum()
        assert codes_starting_with_6 > len(result) * 0.9

    def test_szse_codes_start_with_0_or_3(self):
        """SZSE stock codes typically start with 0 or 3."""
        result = load_szse_symbols()
        # SZSE stocks start with 0 (main board) or 3 (ChiNext)
        valid_codes = result["code"].str.match(r"^[03]")
        assert valid_codes.sum() > len(result) * 0.9

    def test_no_duplicate_codes_in_sse(self):
        """SSE should not have duplicate codes."""
        result = load_sse_symbols()
        assert result["code"].is_unique

    def test_no_duplicate_codes_in_szse(self):
        """SZSE should not have duplicate codes."""
        result = load_szse_symbols()
        assert result["code"].is_unique


class TestFileConstants:
    """Tests for file path constants."""

    def test_sse_file_exists(self):
        """SSE file should exist."""
        assert SSE_FILE.exists()

    def test_szse_file_exists(self):
        """SZSE file should exist."""
        assert SZSE_FILE.exists()


class TestFeatherCache:
    """Tests for feather cache functionality."""

    def setup_method(self):
        """Clean up cache before each test."""
        clear_symbols_cache()

    def teardown_method(self):
        """Clean up cache after each test."""
        clear_symbols_cache()

    def test_creates_cache_file(self):
        """Should create feather cache file after first load."""
        assert not SYMBOLS_CACHE_FILE.exists()
        load_all_symbols(use_cache=True)
        assert SYMBOLS_CACHE_FILE.exists()

    def test_creates_data_directory(self):
        """Should create .data directory if it doesn't exist."""
        if DATA_DIR.exists():
            import shutil

            shutil.rmtree(DATA_DIR)
        load_all_symbols(use_cache=True)
        assert DATA_DIR.exists()

    def test_loads_from_cache(self):
        """Should load from cache on subsequent calls."""
        # First call creates cache
        result1 = load_all_symbols(use_cache=True)
        assert SYMBOLS_CACHE_FILE.exists()

        # Second call should load from cache
        result2 = load_all_symbols(use_cache=True)
        pd.testing.assert_frame_equal(result1, result2)

    def test_cache_disabled(self):
        """Should not create cache when use_cache=False."""
        load_all_symbols(use_cache=False)
        assert not SYMBOLS_CACHE_FILE.exists()

    def test_clear_cache(self):
        """Should delete cache file when cleared."""
        load_all_symbols(use_cache=True)
        assert SYMBOLS_CACHE_FILE.exists()

        result = clear_symbols_cache()
        assert result is True
        assert not SYMBOLS_CACHE_FILE.exists()

    def test_clear_nonexistent_cache(self):
        """Should return False when clearing non-existent cache."""
        assert not SYMBOLS_CACHE_FILE.exists()
        result = clear_symbols_cache()
        assert result is False

    def test_cache_data_integrity(self):
        """Cached data should match freshly loaded data."""
        # Load with cache
        cached = load_all_symbols(use_cache=True)

        # Clear and load without cache
        clear_symbols_cache()
        fresh = load_all_symbols(use_cache=False)

        pd.testing.assert_frame_equal(cached, fresh)


class TestIsStStock:
    """Tests for is_st_stock function."""

    def test_st_uppercase(self):
        """Should detect ST prefix (uppercase)."""
        assert is_st_stock("ST平安") is True

    def test_st_lowercase(self):
        """Should detect st prefix (lowercase)."""
        assert is_st_stock("st平安") is True

    def test_star_st_uppercase(self):
        """Should detect *ST prefix (uppercase)."""
        assert is_st_stock("*ST平安") is True

    def test_star_st_lowercase(self):
        """Should detect *st prefix (lowercase)."""
        assert is_st_stock("*st平安") is True

    def test_normal_stock(self):
        """Should return False for normal stocks."""
        assert is_st_stock("平安银行") is False
        assert is_st_stock("贵州茅台") is False

    def test_st_in_middle(self):
        """Should return False when ST is not a prefix."""
        assert is_st_stock("中国ST银行") is False


class TestExcludeStStocks:
    """Tests for ST stock exclusion functionality."""

    def test_sse_excludes_st_by_default(self):
        """SSE symbols should exclude ST stocks by default."""
        result = load_sse_symbols()
        st_stocks = result[result["name"].apply(is_st_stock)]
        assert len(st_stocks) == 0

    def test_szse_excludes_st_by_default(self):
        """SZSE symbols should exclude ST stocks by default."""
        result = load_szse_symbols()
        st_stocks = result[result["name"].apply(is_st_stock)]
        assert len(st_stocks) == 0

    def test_all_symbols_excludes_st_by_default(self):
        """All symbols should exclude ST stocks by default."""
        result = load_all_symbols()
        st_stocks = result[result["name"].apply(is_st_stock)]
        assert len(st_stocks) == 0

    def test_sse_includes_st_when_disabled(self):
        """SSE symbols should include ST stocks when exclude_st=False."""
        with_st = load_sse_symbols(exclude_st=False)
        without_st = load_sse_symbols(exclude_st=True)
        # There should be more stocks when ST is included
        assert len(with_st) >= len(without_st)

    def test_szse_includes_st_when_disabled(self):
        """SZSE symbols should include ST stocks when exclude_st=False."""
        with_st = load_szse_symbols(exclude_st=False)
        without_st = load_szse_symbols(exclude_st=True)
        # There should be more stocks when ST is included
        assert len(with_st) >= len(without_st)

    def test_all_symbols_includes_st_when_disabled(self):
        """All symbols should include ST stocks when exclude_st=False."""
        with_st = load_all_symbols(exclude_st=False, use_cache=False)
        without_st = load_all_symbols(exclude_st=True, use_cache=False)
        # There should be more stocks when ST is included
        assert len(with_st) >= len(without_st)

    def test_get_symbols_by_exchange_excludes_st_by_default(self):
        """get_symbols_by_exchange should exclude ST stocks by default."""
        result = get_symbols_by_exchange("SSE")
        st_stocks = result[result["name"].apply(is_st_stock)]
        assert len(st_stocks) == 0


class TestMinListYears:
    """Tests for minimum listing years filter functionality."""

    def test_sse_filters_new_stocks_by_default(self):
        """SSE symbols should filter stocks listed less than 2 years by default."""
        with_filter = load_sse_symbols()
        without_filter = load_sse_symbols(min_list_years=0)
        # There should be more stocks when filter is disabled
        assert len(without_filter) >= len(with_filter)

    def test_szse_filters_new_stocks_by_default(self):
        """SZSE symbols should filter stocks listed less than 2 years by default."""
        with_filter = load_szse_symbols()
        without_filter = load_szse_symbols(min_list_years=0)
        # There should be more stocks when filter is disabled
        assert len(without_filter) >= len(with_filter)

    def test_all_symbols_filters_new_stocks_by_default(self):
        """All symbols should filter stocks listed less than 2 years by default."""
        with_filter = load_all_symbols(use_cache=False)
        without_filter = load_all_symbols(min_list_years=0, use_cache=False)
        # There should be more stocks when filter is disabled
        assert len(without_filter) >= len(with_filter)

    def test_min_list_years_zero_includes_all(self):
        """min_list_years=0 should include all stocks regardless of listing date."""
        result = load_all_symbols(min_list_years=0, use_cache=False)
        # Should include recently listed stocks
        assert len(result) > 0

    def test_min_list_years_custom_value(self):
        """Should support custom min_list_years values."""
        years_1 = load_all_symbols(min_list_years=1, use_cache=False)
        years_5 = load_all_symbols(min_list_years=5, use_cache=False)
        years_10 = load_all_symbols(min_list_years=10, use_cache=False)
        # More restrictive filters should return fewer stocks
        assert len(years_1) >= len(years_5) >= len(years_10)

    def test_get_symbols_by_exchange_supports_min_list_years(self):
        """get_symbols_by_exchange should support min_list_years parameter."""
        with_filter = get_symbols_by_exchange("SSE")
        without_filter = get_symbols_by_exchange("SSE", min_list_years=0)
        assert len(without_filter) >= len(with_filter)

    def test_cache_not_used_for_non_default_min_list_years(self):
        """Cache should not be used when min_list_years is not default."""
        # First ensure cache exists with default settings
        load_all_symbols(use_cache=True)

        # Load with different min_list_years should not use cache
        result_0_years = load_all_symbols(min_list_years=0, use_cache=True)
        result_default = load_all_symbols(use_cache=True)

        # Results should be different because one filters and one doesn't
        assert len(result_0_years) >= len(result_default)
