"""Symbol module for loading and processing stock symbols from SSE and SZSE exchanges."""

from datetime import datetime
from pathlib import Path

import pandas as pd

# Default paths for exchange data files
_MODULE_DIR = Path(__file__).parent
_PROJECT_ROOT = _MODULE_DIR.parent.parent
SSE_FILE = _MODULE_DIR / "sse.xls"
SZSE_FILE = _MODULE_DIR / "szse.xlsx"

# Cache directory and file
DATA_DIR = _PROJECT_ROOT / ".data"
SYMBOLS_CACHE_FILE = DATA_DIR / "symbols.feather"
EXCHANGE_SUFFIX = {"SSE": ".SH", "SZSE": ".SZ"}

# Column names from Excel files
_SSE_CODE_COL = "A\u80a1\u4ee3\u7801"  # A股代码
_SSE_NAME_COL = "\u8bc1\u5238\u7b80\u79f0"  # 证券简称
_SSE_LIST_DATE_COL = "\u4e0a\u5e02\u65e5\u671f"  # 上市日期

_SZSE_CODE_COL = "A\u80a1\u4ee3\u7801"  # A股代码
_SZSE_NAME_COL = "A\u80a1\u7b80\u79f0"  # A股简称
_SZSE_LIST_DATE_COL = "A\u80a1\u4e0a\u5e02\u65e5\u671f"  # A股上市日期


def is_st_stock(name: str) -> bool:
    """
    Check if a stock is ST (Special Treatment) based on its name.

    ST stocks include: *ST, ST, *st, st (case-insensitive prefix match).

    Args:
        name: Stock name.

    Returns:
        True if the stock is ST, False otherwise.
    """
    name_lower = name.lower()
    return name_lower.startswith("*st") or name_lower.startswith("st")


def _filter_st_stocks(df: pd.DataFrame) -> pd.DataFrame:
    """Filter out ST stocks from a DataFrame."""
    return df[~df["name"].apply(is_st_stock)]


def _filter_by_list_years(df: pd.DataFrame, min_years: int) -> pd.DataFrame:
    """
    Filter out stocks that have been listed for less than min_years.

    Args:
        df: DataFrame with 'list_date' column in YYYYMMDD or YYYY-MM-DD format.
        min_years: Minimum number of years since listing.

    Returns:
        DataFrame with only stocks listed for at least min_years.
    """
    if min_years <= 0:
        return df

    today = datetime.now()

    def is_listed_long_enough(list_date: str) -> bool:
        # Normalize date format: '1991-04-03' or '19910403' -> datetime
        date_str = list_date.replace("-", "")
        try:
            listed = datetime.strptime(date_str, "%Y%m%d")
            years_listed = (today - listed).days / 365.25
            return years_listed >= min_years
        except ValueError:
            return False

    return df[df["list_date"].apply(is_listed_long_enough)]


def load_sse_symbols(
    file_path: Path | str | None = None,
    exclude_st: bool = True,
    min_list_years: int = 2,
) -> pd.DataFrame:
    """
    Load stock symbols from Shanghai Stock Exchange (SSE) Excel file.

    Args:
        file_path: Path to the SSE Excel file. Defaults to the bundled sse.xls.
        exclude_st: Whether to exclude ST stocks. Defaults to True.
        min_list_years: Minimum years since listing. Set to 0 to include all. Defaults to 2.

    Returns:
        DataFrame with columns: code, name, exchange, list_date
    """
    file_path = Path(file_path) if file_path else SSE_FILE
    df = pd.read_excel(file_path)

    # Extract and rename columns
    result = pd.DataFrame(
        {
            "code": df[_SSE_CODE_COL].astype(str).str.zfill(6),
            "name": df[_SSE_NAME_COL],
            "exchange": "SSE",
            "list_date": df[_SSE_LIST_DATE_COL].astype(str),
        }
    )

    if exclude_st:
        result = _filter_st_stocks(result)

    if min_list_years > 0:
        result = _filter_by_list_years(result, min_list_years)

    return result


def load_szse_symbols(
    file_path: Path | str | None = None,
    exclude_st: bool = True,
    min_list_years: int = 2,
) -> pd.DataFrame:
    """
    Load stock symbols from Shenzhen Stock Exchange (SZSE) Excel file.

    Args:
        file_path: Path to the SZSE Excel file. Defaults to the bundled szse.xlsx.
        exclude_st: Whether to exclude ST stocks. Defaults to True.
        min_list_years: Minimum years since listing. Set to 0 to include all. Defaults to 2.

    Returns:
        DataFrame with columns: code, name, exchange, list_date
    """
    file_path = Path(file_path) if file_path else SZSE_FILE
    df = pd.read_excel(file_path)

    # Extract and rename columns
    result = pd.DataFrame(
        {
            "code": df[_SZSE_CODE_COL].astype(str).str.zfill(6),
            "name": df[_SZSE_NAME_COL],
            "exchange": "SZSE",
            "list_date": df[_SZSE_LIST_DATE_COL].astype(str),
        }
    )

    if exclude_st:
        result = _filter_st_stocks(result)

    if min_list_years > 0:
        result = _filter_by_list_years(result, min_list_years)

    return result


def load_all_symbols(
    sse_file: Path | str | None = None,
    szse_file: Path | str | None = None,
    use_cache: bool = True,
    exclude_st: bool = True,
    min_list_years: int = 2,
) -> pd.DataFrame:
    """
    Load and combine stock symbols from both SSE and SZSE exchanges.

    Uses feather cache for faster subsequent loads. Cache is stored in .data/symbols.feather.
    Note: Cache is only used when using default parameters (exclude_st=True, min_list_years=2).

    Args:
        sse_file: Path to the SSE Excel file. Defaults to the bundled sse.xls.
        szse_file: Path to the SZSE Excel file. Defaults to the bundled szse.xlsx.
        use_cache: Whether to use feather cache. Defaults to True.
        exclude_st: Whether to exclude ST stocks. Defaults to True.
        min_list_years: Minimum years since listing. Set to 0 to include all. Defaults to 2.

    Returns:
        DataFrame with columns: code, name, exchange, list_date
    """
    # Cache is only valid for default filter settings
    use_default_filters = exclude_st and min_list_years == 2

    # Try to load from cache if enabled and using default files and filters
    if use_cache and sse_file is None and szse_file is None and use_default_filters and SYMBOLS_CACHE_FILE.exists():
        return pd.read_feather(SYMBOLS_CACHE_FILE)

    # Load from Excel files
    sse_df = load_sse_symbols(sse_file, exclude_st=exclude_st, min_list_years=min_list_years)
    szse_df = load_szse_symbols(szse_file, exclude_st=exclude_st, min_list_years=min_list_years)

    # Combine both exchanges
    combined = pd.concat([sse_df, szse_df], ignore_index=True)

    # Save to cache if using default files and default filters
    if use_cache and sse_file is None and szse_file is None and use_default_filters:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        combined.to_feather(SYMBOLS_CACHE_FILE)

    return combined


def to_ts_codes(df: pd.DataFrame) -> list[str]:
    """
    Convert a symbols DataFrame to tushare-style codes.

    Args:
        df: DataFrame with 'code' and 'exchange' columns.

    Returns:
        List of ts_codes (e.g., ['000001.SZ', '600000.SH']).
    """
    return [f"{row.code}{EXCHANGE_SUFFIX.get(row.exchange, '')}" for row in df.itertuples(index=False)]


def get_ts_codes(
    sse_file: Path | str | None = None,
    szse_file: Path | str | None = None,
    use_cache: bool = True,
    exclude_st: bool = True,
    min_list_years: int = 2,
) -> list[str]:
    """
    Load all symbols and return tushare-style codes.

    Args mirror load_all_symbols to allow reuse of filters.
    """
    symbols = load_all_symbols(
        sse_file=sse_file,
        szse_file=szse_file,
        use_cache=use_cache,
        exclude_st=exclude_st,
        min_list_years=min_list_years,
    )
    return to_ts_codes(symbols)


def clear_symbols_cache() -> bool:
    """
    Clear the symbols feather cache.

    Returns:
        True if cache was deleted, False if cache didn't exist.
    """
    if SYMBOLS_CACHE_FILE.exists():
        SYMBOLS_CACHE_FILE.unlink()
        return True
    return False


def get_symbols_by_exchange(
    exchange: str,
    sse_file: Path | str | None = None,
    szse_file: Path | str | None = None,
    exclude_st: bool = True,
    min_list_years: int = 2,
) -> pd.DataFrame:
    """
    Get stock symbols filtered by exchange.

    Args:
        exchange: Exchange code ('SSE' or 'SZSE').
        sse_file: Path to the SSE Excel file.
        szse_file: Path to the SZSE Excel file.
        exclude_st: Whether to exclude ST stocks. Defaults to True.
        min_list_years: Minimum years since listing. Set to 0 to include all. Defaults to 2.

    Returns:
        DataFrame with symbols from the specified exchange.

    Raises:
        ValueError: If exchange is not 'SSE' or 'SZSE'.
    """
    exchange = exchange.upper()
    if exchange not in ("SSE", "SZSE"):
        raise ValueError(f"Invalid exchange: {exchange}. Must be 'SSE' or 'SZSE'.")

    if exchange == "SSE":
        return load_sse_symbols(sse_file, exclude_st=exclude_st, min_list_years=min_list_years)
    return load_szse_symbols(szse_file, exclude_st=exclude_st, min_list_years=min_list_years)
