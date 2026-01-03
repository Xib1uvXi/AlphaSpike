"""Unified feature registry module.

This module provides a single source of truth for all feature definitions.
Adding a new feature only requires adding it here - scanner and backtest
will automatically pick it up.
"""

from dataclasses import dataclass
from typing import Callable

import pandas as pd

from src.feature.bbc import bbc
from src.feature.bullish_cannon import bullish_cannon
from src.feature.consolidation_breakout import consolidation_breakout
from src.feature.four_edge import four_edge
from src.feature.high_retracement import high_retracement
from src.feature.volume_stagnation import volume_stagnation
from src.feature.volume_upper_shadow import volume_upper_shadow
from src.feature.volume_upper_shadow_opz import volume_upper_shadow_opz
from src.feature.volume_upper_shadow_v2 import volume_upper_shadow_v2
from src.feature.weak_to_strong import weak_to_strong


@dataclass
class FeatureConfig:
    """Configuration for a feature.

    Attributes:
        name: Feature name (used as cache key and display)
        func: Feature detection function that takes DataFrame and returns bool
        min_days: Minimum trading days of data required for detection
    """

    name: str
    func: Callable[[pd.DataFrame], bool]
    min_days: int


# Central feature registry - add new features here
FEATURES: list[FeatureConfig] = [
    FeatureConfig("bbc", bbc, 1000),
    FeatureConfig("volume_upper_shadow", volume_upper_shadow, 220),
    FeatureConfig("volume_upper_shadow_opz", volume_upper_shadow_opz, 220),
    FeatureConfig("volume_upper_shadow_v2", volume_upper_shadow_v2, 220),
    FeatureConfig("volume_stagnation", volume_stagnation, 550),
    FeatureConfig("high_retracement", high_retracement, 1500),
    FeatureConfig("consolidation_breakout", consolidation_breakout, 60),
    FeatureConfig("bullish_cannon", bullish_cannon, 30),
    FeatureConfig("four_edge", four_edge, 130),
    FeatureConfig("weak_to_strong", weak_to_strong, 5),
]

# Derived mapping for worker processes (auto-generated from FEATURES)
FEATURE_FUNCS: dict[str, Callable[[pd.DataFrame], bool]] = {f.name: f.func for f in FEATURES}


def get_feature_by_name(name: str) -> FeatureConfig | None:
    """
    Get feature config by name.

    Args:
        name: Feature name

    Returns:
        FeatureConfig or None if not found.
    """
    for feature in FEATURES:
        if feature.name == name:
            return feature
    return None


def get_all_feature_names() -> list[str]:
    """
    Get list of all registered feature names.

    Returns:
        List of feature names.
    """
    return [f.name for f in FEATURES]
