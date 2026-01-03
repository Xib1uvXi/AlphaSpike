"""Feature value extraction module for volume_upper_shadow."""

import warnings
from dataclasses import dataclass

import pandas as pd
import talib

from src.common.config import VOLUME_UPPER_SHADOW_CONFIG
from src.common.returns import calculate_period_returns
from src.feature.utils import calculate_price_quantile, calculate_upper_shadow_ratio
from src.feature_engineering.db import FeatureData

warnings.filterwarnings("ignore")

_cfg = VOLUME_UPPER_SHADOW_CONFIG


@dataclass
class VolumeUpperShadowFeatures:
    """Extracted feature values for volume_upper_shadow signal."""

    ts_code: str
    signal_date: str
    upper_shadow: float
    vol_ratio: float
    price_quantile: float
    pct_chg: float
    body_ratio: float
    close_vs_ma20: float
    prev_vol_ratio: float
    gain_2d: float
    is_signal: bool  # Whether this meets all signal conditions


def extract_volume_upper_shadow_features(  # pylint: disable=too-many-locals
    df: pd.DataFrame,
    ts_code: str,
) -> VolumeUpperShadowFeatures | None:
    """
    Extract all feature values for volume_upper_shadow from the last candle.

    This extracts the actual feature values (not just True/False) so they can be
    stored and analyzed for feature engineering purposes.

    Args:
        df: DataFrame with daily bar data containing OHLCV columns
        ts_code: Stock code

    Returns:
        VolumeUpperShadowFeatures with all extracted values, or None if insufficient data.
    """
    df = df.dropna()

    # Need at least 200 days for price quantile calculation + buffer
    if len(df) < 220:
        return None

    tmp_df = df.copy()

    # Calculate indicators
    tmp_df["ma3"] = talib.SMA(tmp_df["close"], timeperiod=3)
    tmp_df["ma5"] = talib.SMA(tmp_df["close"], timeperiod=5)
    tmp_df["ma10"] = talib.SMA(tmp_df["close"], timeperiod=10)
    tmp_df["ma20"] = talib.SMA(tmp_df["close"], timeperiod=20)
    tmp_df["vol_ma10"] = talib.SMA(tmp_df["vol"], timeperiod=10)
    tmp_df["upper_shadow"] = calculate_upper_shadow_ratio(tmp_df)
    tmp_df["price_quantile"] = calculate_price_quantile(tmp_df["close"], window=200)

    # Get last row for feature extraction
    last = tmp_df.iloc[-1]
    prev = tmp_df.iloc[-2]
    signal_date = str(last["trade_date"])

    # Calculate vol_ratio
    prev_vol_ma10 = tmp_df["vol_ma10"].iloc[-2]
    vol_ratio = last["vol"] / prev_vol_ma10 if prev_vol_ma10 > 0 else 0

    # Calculate body_ratio
    body = abs(last["close"] - last["open"])
    high_low = last["high"] - last["low"]
    body_ratio = body / high_low if high_low > 0 else 0

    # Calculate close_vs_ma20 (percentage difference)
    close_vs_ma20 = ((last["close"] - last["ma20"]) / last["ma20"] * 100) if last["ma20"] > 0 else 0

    # Calculate prev_vol_ratio (previous day's volume vs its vol_ma10)
    prev_prev_vol_ma10 = tmp_df["vol_ma10"].iloc[-3] if len(tmp_df) > 2 else prev_vol_ma10
    prev_vol_ratio = prev["vol"] / prev_prev_vol_ma10 if prev_prev_vol_ma10 > 0 else 0

    # Calculate gain_2d (2-day cumulative gain)
    last_2_pct = tmp_df["pct_chg"].tail(2)
    gain_2d = ((1 + last_2_pct / 100).prod() - 1) * 100

    # Check if all signal conditions are met
    last_3_pct = tmp_df["pct_chg"].tail(3)
    cumulative_gain = ((1 + last_3_pct / 100).prod() - 1) * 100
    no_limit_up = (last_3_pct < _cfg.limit_up_threshold).all()

    cond1 = last["upper_shadow"] > _cfg.upper_shadow_ratio
    cond2 = _cfg.vol_surge_min <= vol_ratio <= _cfg.vol_surge_max
    cond3 = last["price_quantile"] < _cfg.price_quantile_max
    cond4 = last["close"] > last["ma5"]
    cond5 = last["close"] > last["ma10"]
    cond6 = last["ma3"] > last["ma5"]
    cond7 = no_limit_up and cumulative_gain < _cfg.cumulative_gain_max

    is_signal = all([cond1, cond2, cond3, cond4, cond5, cond6, cond7])

    return VolumeUpperShadowFeatures(
        ts_code=ts_code,
        signal_date=signal_date,
        upper_shadow=round(last["upper_shadow"], 4),
        vol_ratio=round(vol_ratio, 4),
        price_quantile=round(last["price_quantile"], 4),
        pct_chg=round(last["pct_chg"], 2),
        body_ratio=round(body_ratio, 4),
        close_vs_ma20=round(close_vs_ma20, 2),
        prev_vol_ratio=round(prev_vol_ratio, 4),
        gain_2d=round(gain_2d, 2),
        is_signal=is_signal,
    )


def create_feature_data_with_returns(
    features: VolumeUpperShadowFeatures,
    df: pd.DataFrame,
) -> FeatureData:
    """
    Create a FeatureData record with returns calculated.

    Args:
        features: Extracted feature values
        df: Full daily bar DataFrame for calculating returns

    Returns:
        FeatureData with all features and returns.
    """
    # Calculate returns using common utility
    returns_result = calculate_period_returns(df, features.signal_date, [1, 2, 3])

    return_1d = None
    return_2d = None
    return_3d = None

    if returns_result:
        returns = returns_result["returns"]
        return_1d = returns.get(1)
        return_2d = returns.get(2)
        return_3d = returns.get(3)

    return FeatureData(
        ts_code=features.ts_code,
        signal_date=features.signal_date,
        feature_name="volume_upper_shadow",
        upper_shadow=features.upper_shadow,
        vol_ratio=features.vol_ratio,
        price_quantile=features.price_quantile,
        pct_chg=features.pct_chg,
        body_ratio=features.body_ratio,
        close_vs_ma20=features.close_vs_ma20,
        prev_vol_ratio=features.prev_vol_ratio,
        gain_2d=features.gain_2d,
        return_1d=return_1d,
        return_2d=return_2d,
        return_3d=return_3d,
    )
