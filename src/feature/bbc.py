import warnings

import pandas as pd
import talib

warnings.filterwarnings("ignore")


def bbc(df: pd.DataFrame) -> bool:
    """
    Feature: BBC
    Description: big bearish candle
    """

    df = df.dropna()

    if len(df) < 4 * 250:
        return False

    tmp_df = df.copy()

    tmp_df["vol_quantile"] = tmp_df["vol"].rank(pct=True)
    tmp_df["vol_quantile_ma10"] = talib.MA(tmp_df["vol_quantile"], timeperiod=10)
    tmp_df["vol_quantile_ma3"] = talib.MA(tmp_df["vol_quantile"], timeperiod=3)

    # volume condition
    cond1 = (
        (tmp_df["vol_quantile_ma10"].shift() < 0.75)
        & (tmp_df["vol_quantile_ma3"].shift() < 0.75)
        & (tmp_df["vol_quantile"].shift() < 0.75)
    )

    # pre day is limit up
    cond2 = (tmp_df["pct_chg"].shift() > 9.5) & (tmp_df["close"].shift() == tmp_df["high"].shift())

    # gap up condition
    tmp_df["high_10"] = talib.MAX(tmp_df["high"], timeperiod=10)
    tmp_df["high_144"] = talib.MAX(tmp_df["high"], timeperiod=144)
    gap_up = (
        (tmp_df["open"] > tmp_df["pre_close"])
        & (tmp_df["high"] >= tmp_df["high_10"])
        & (tmp_df["high"] < tmp_df["high_144"])
    )
    cond3 = (tmp_df["close"] < tmp_df["open"] * 0.95) & (tmp_df["close"] < tmp_df["open"]) & gap_up

    tmp_df["ma_close_5"] = talib.MA(tmp_df["close"], timeperiod=5)
    tmp_df["ma_close_10"] = talib.MA(tmp_df["close"], timeperiod=10)
    cond4 = tmp_df["ma_close_5"].shift() > tmp_df["ma_close_10"].shift()

    tmp_df["signal"] = cond1 & cond2 & cond3 & cond4

    # check last 3 trading days for signal
    recent_signal = tmp_df["signal"].tail(3)
    return recent_signal.any()
