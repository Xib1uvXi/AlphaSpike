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
# Feature Thresholds
# =============================================================================


@dataclass(frozen=True)
class BullishCannonConfig:
    """Configuration for Bullish Cannon (多方炮) feature detection."""

    # First Cannon thresholds
    first_cannon_return: float = 0.07  # ret0 >= 7%
    first_cannon_vol_ratio: float = 1.8  # vol0 >= vol_ma5 * 1.8
    first_cannon_body_ratio: float = 0.40  # body0 / range0 >= 0.40
    first_cannon_upper_wick_ratio: float = 0.50  # upper_wick0 / range0 <= 0.50

    # Cannon Body thresholds
    body_vol_contraction: float = 0.8  # mean(vol1..k) <= vol0 * 0.8
    body_max_amplitude: float = 0.08  # max(amplitude) <= 8%

    # Second Cannon thresholds
    second_cannon_vol_ratio: float = 1.0  # vol >= mean(vol1..k) * 1.0
    second_cannon_upper_ratio: float = 0.25  # (high - close) / range <= 0.25


@dataclass(frozen=True)
class ConsolidationBreakoutConfig:
    """Configuration for Consolidation Breakout (横盘突破) feature detection."""

    # Consolidation thresholds
    atr_close_ratio: float = 1.5  # ATR_14 / Close < 1.5%
    adx_threshold: float = 22  # ADX_14 < 22
    bb_width_quantile: float = 0.30  # BB_Width < 30th percentile

    # MA20 flat thresholds
    ma20_stable_ratio: float = 0.003  # |MA20 - MA20[5]| / MA20 < 0.3%
    ma20_variance_ratio: float = 0.002  # STD(MA20, 10) / MA20 < 0.2%

    # Breakout thresholds
    breakout_vol_ratio: float = 1.5  # Volume > SMA(Volume, 20) * 1.5


@dataclass(frozen=True)
class VolumeUpperShadowConfig:
    """Configuration for Volume Upper Shadow (放量上影线) feature detection."""

    # Upper shadow threshold
    upper_shadow_ratio: float = 2.0  # Upper shadow > 2%

    # Volume surge range
    vol_surge_min: float = 1.2  # Volume >= prev_vol_ma10 * 1.2
    vol_surge_max: float = 2.0  # Volume <= prev_vol_ma10 * 2.0

    # Price position
    price_quantile_max: float = 0.45  # Price quantile < 45%

    # Limit-up and gain limits
    limit_up_threshold: float = 9.8  # A-share limit-up is ~10%
    cumulative_gain_max: float = 15.0  # Cumulative gain < 15%


@dataclass(frozen=True)
class VolumeStagnationConfig:
    """Configuration for Volume Stagnation (放量滞涨) feature detection."""

    # Volume surge threshold
    vol_surge_ratio: float = 1.5  # vol > vol_ma10 * 1.5

    # Price stagnation range
    price_change_min: float = -3.0  # pct_chg > -3%
    price_change_max: float = 3.0  # pct_chg < 3%

    # Price quantile range
    price_quantile_min: float = 0.05  # Price quantile >= 5%
    price_quantile_max: float = 0.45  # Price quantile <= 45%


# Default feature configurations
BULLISH_CANNON_CONFIG = BullishCannonConfig()
CONSOLIDATION_BREAKOUT_CONFIG = ConsolidationBreakoutConfig()
VOLUME_UPPER_SHADOW_CONFIG = VolumeUpperShadowConfig()
VOLUME_STAGNATION_CONFIG = VolumeStagnationConfig()


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
