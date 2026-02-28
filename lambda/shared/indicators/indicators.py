"""Technical indicator calculations using NumPy and Pandas.

All indicators are implemented using pure NumPy/Pandas without ta-lib dependency.
"""

import numpy as np
import pandas as pd


class TechnicalIndicators:
    """Technical indicator calculator using NumPy and Pandas."""

    @staticmethod
    def sma(data: pd.Series, period: int) -> pd.Series:
        """Calculate Simple Moving Average (SMA)."""
        return data.rolling(window=period, min_periods=period).mean()

    @staticmethod
    def ema(data: pd.Series, period: int) -> pd.Series:
        """Calculate Exponential Moving Average (EMA)."""
        return data.ewm(span=period, adjust=False, min_periods=period).mean()

    @staticmethod
    def rsi(data: pd.Series, period: int = 14) -> pd.Series:
        """Calculate Relative Strength Index (RSI). Returns 0-100."""
        delta = data.diff()
        gain = delta.where(delta > 0, 0.0)
        loss = -delta.where(delta < 0, 0.0)

        avg_gain = gain.ewm(span=period, adjust=False, min_periods=period).mean()
        avg_loss = loss.ewm(span=period, adjust=False, min_periods=period).mean()

        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        return rsi

    @staticmethod
    def macd(
        data: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9
    ) -> dict[str, pd.Series]:
        """Calculate MACD. Returns dict with 'macd_line', 'signal_line', 'histogram'."""
        ema_fast = data.ewm(span=fast, adjust=False, min_periods=fast).mean()
        ema_slow = data.ewm(span=slow, adjust=False, min_periods=slow).mean()

        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=signal, adjust=False, min_periods=signal).mean()
        histogram = macd_line - signal_line

        return {"macd_line": macd_line, "signal_line": signal_line, "histogram": histogram}

    @staticmethod
    def bollinger_bands(
        data: pd.Series, period: int = 20, std_dev: float = 2.0, ma_type: str = "sma"
    ) -> dict[str, pd.Series]:
        """Calculate Bollinger Bands. Returns dict with 'upper', 'middle', 'lower'."""
        if ma_type == "ema":
            middle_band = data.ewm(span=period, adjust=False, min_periods=period).mean()
        else:
            middle_band = data.rolling(window=period, min_periods=period).mean()

        std = data.rolling(window=period, min_periods=period).std()
        upper_band = middle_band + (std * std_dev)
        lower_band = middle_band - (std * std_dev)

        return {"upper": upper_band, "middle": middle_band, "lower": lower_band}

    @staticmethod
    def adx(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
        """Calculate Average Directional Index (ADX)."""
        tr1 = high - low
        tr2 = abs(high - close.shift(1))
        tr3 = abs(low - close.shift(1))
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

        up_move = high - high.shift(1)
        down_move = low.shift(1) - low

        plus_dm = pd.Series(
            np.where((up_move > down_move) & (up_move > 0), up_move, 0), index=high.index
        )
        minus_dm = pd.Series(
            np.where((down_move > up_move) & (down_move > 0), down_move, 0), index=low.index
        )

        tr_smooth = tr.ewm(span=period, adjust=False, min_periods=period).mean()
        plus_di = 100 * (plus_dm.ewm(span=period, adjust=False, min_periods=period).mean() / tr_smooth)
        minus_di = 100 * (minus_dm.ewm(span=period, adjust=False, min_periods=period).mean() / tr_smooth)

        dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di)
        adx = dx.ewm(span=period, adjust=False, min_periods=period).mean()
        return adx

    @staticmethod
    def atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
        """Calculate Average True Range (ATR)."""
        tr1 = high - low
        tr2 = abs(high - close.shift(1))
        tr3 = abs(low - close.shift(1))

        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr = tr.ewm(span=period, adjust=False, min_periods=period).mean()
        return atr

    @staticmethod
    def kdj(
        high: pd.Series, low: pd.Series, close: pd.Series,
        k_period: int = 9, d_period: int = 3,
    ) -> dict[str, pd.Series]:
        """Calculate KDJ Indicator (Stochastic with J line).

        KDJ is primarily used in Asian markets. J = 3K - 2D.
        """
        lowest_low = low.rolling(window=k_period, min_periods=k_period).min()
        highest_high = high.rolling(window=k_period, min_periods=k_period).max()

        k = 100 * (close - lowest_low) / (highest_high - lowest_low)
        d = k.rolling(window=d_period, min_periods=d_period).mean()
        j = 3 * k - 2 * d

        return {"k": k, "d": d, "j": j}

    @staticmethod
    def obv(close: pd.Series, volume: pd.Series) -> pd.Series:
        """Calculate On-Balance Volume (OBV)."""
        direction = np.sign(close.diff())
        direction.iloc[0] = 0
        obv = (volume * direction).cumsum()
        return obv
