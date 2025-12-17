"""Centralized configuration module with environment variable support."""

import os
from dataclasses import dataclass

from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


def _get_env_float(key: str, default: float) -> float:
    """Get float from environment variable with default."""
    value = os.getenv(key)
    if value is not None:
        try:
            return float(value)
        except ValueError:
            pass
    return default


def _get_env_int(key: str, default: int) -> int:
    """Get int from environment variable with default."""
    value = os.getenv(key)
    if value is not None:
        try:
            return int(value)
        except ValueError:
            pass
    return default


# =============================================================================
# Tushare API Settings
# =============================================================================

# Rate limiting: 45 requests per minute = 1.33s between requests
# Use 1.4s interval for safety margin
TUSHARE_RATE_LIMIT_INTERVAL = _get_env_float("TUSHARE_RATE_LIMIT_INTERVAL", 1.4)


# =============================================================================
# Cache Settings
# =============================================================================

# Feature cache TTL in days (default: 14 days)
FEATURE_CACHE_TTL_DAYS = _get_env_int("FEATURE_CACHE_TTL_DAYS", 14)
FEATURE_CACHE_TTL_SECONDS = FEATURE_CACHE_TTL_DAYS * 24 * 60 * 60


# =============================================================================
# Scanner / Backtest Settings
# =============================================================================

# Default number of parallel workers
DEFAULT_MAX_WORKERS = _get_env_int("MAX_WORKERS", 6)

# Default holding period for backtesting (days)
DEFAULT_HOLDING_DAYS = _get_env_int("HOLDING_DAYS", 5)


# =============================================================================
# Four Edge Feature Thresholds
# =============================================================================


@dataclass(frozen=True)
class FourEdgeConfig:
    """Configuration for Four-Edge feature detection."""

    # Edge 1: ATR Volatility threshold
    atr_volatility_threshold: float = 0.025  # 2.5%

    # Edge 2 Type 1: Compression -> Expansion
    box_width_threshold: float = 0.18  # 18%
    ma20_slope_threshold: float = 0.008  # 0.8%
    close_to_ma_threshold: float = 0.03  # 3%

    # Edge 2 Type 2: Trend Pullback
    t2_pullback_range_low: float = 0.97  # Close/MA20 >= 97%
    t2_pullback_range_high: float = 1.03  # Close/MA20 <= 103%
    t2_support_ratio: float = 0.98  # LLV5 >= MA60 * 0.98

    # Edge 2 Type 3: Breakout -> Retest
    t3_breakout_vol_ratio: float = 1.5  # Vol / Vol_MA5 >= 1.5
    t3_retest_days_min: int = 3  # Minimum days since breakout
    t3_retest_days_max: int = 10  # Maximum days since breakout
    t3_retest_support_ratio: float = 0.99  # LLV3 >= breakout_level * 0.99

    # Edge 3: Entry signals
    ar_compress: float = 1.3  # AR threshold for COMPRESS
    ar_pullback: float = 1.2  # AR threshold for PULLBACK
    ar_retest: float = 1.3  # AR threshold for RETEST
    volup_threshold: float = 1.3  # theta for VolUp (mild surge)
    close_strong_ratio: float = 0.3  # Close >= High - 0.3 * Range
    bullish_body_ratio: float = 0.5  # RealBody / Range >= 0.5
    retest_support_ratio: float = 0.99  # LLV3 >= BreakoutLevel * 0.99

    # Edge 4: Overheated rejection filter
    consecutive_bullish_days: int = 4  # Consecutive bullish candle days
    cumulative_return_threshold: float = 15.0  # Sum of pct_chg >= 15%


# Default four-edge configuration
FOUR_EDGE_CONFIG = FourEdgeConfig()
