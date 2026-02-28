"""Configuration for technical indicators calculation."""

from dataclasses import dataclass
from typing import Literal


@dataclass
class IndicatorConfig:
    """Configuration for technical indicator calculations."""

    # SMA (Simple Moving Average) periods
    sma_periods: list[int] = None

    # EMA (Exponential Moving Average) periods
    ema_periods: list[int] = None

    # RSI configuration
    rsi_period: int = 14

    # MACD configuration
    macd_fast: int = 12
    macd_slow: int = 26
    macd_signal: int = 9

    # Bollinger Bands configuration
    bb_period: int = 20
    bb_std_dev: float = 2.0
    bb_ma_type: Literal["sma", "ema"] = "sma"

    # KDJ configuration
    kdj_k_period: int = 9
    kdj_d_period: int = 3

    # ADX (Average Directional Index)
    adx_period: int = 14

    # ATR (Average True Range)
    atr_period: int = 14

    # Volume MA periods
    volume_ma_periods: list[int] = None

    # Minimum data points required for calculation
    min_data_points: int = 50

    def __post_init__(self):
        """Set default values for list fields."""
        if self.sma_periods is None:
            self.sma_periods = [5, 10, 20, 50, 200]
        if self.ema_periods is None:
            self.ema_periods = [20, 50, 200]
        if self.volume_ma_periods is None:
            self.volume_ma_periods = [20, 50]


# Default configuration
DEFAULT_CONFIG = IndicatorConfig()


def get_indicator_set(preset: str = "standard") -> IndicatorConfig:
    """Get indicator configuration based on preset."""
    if preset == "minimal":
        return IndicatorConfig(
            sma_periods=[20, 50],
            ema_periods=[20],
            volume_ma_periods=[20],
        )
    elif preset == "extended":
        return IndicatorConfig(
            sma_periods=[5, 10, 20, 50, 100, 200],
            ema_periods=[10, 20, 50, 100, 200],
            volume_ma_periods=[10, 20, 50],
        )
    else:  # standard
        return DEFAULT_CONFIG
