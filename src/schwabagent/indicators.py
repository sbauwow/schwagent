"""Technical indicator functions — pure numpy/pandas, no ta-lib dependency."""
from __future__ import annotations

import numpy as np
import pandas as pd


def sma(prices: pd.Series, period: int) -> float:
    """Simple moving average of the last *period* values."""
    if len(prices) < period:
        return float("nan")
    return float(prices.iloc[-period:].mean())


def ema(prices: pd.Series, period: int) -> float:
    """Exponential moving average (last value of the EMA series)."""
    if len(prices) < period:
        return float("nan")
    alpha = 2.0 / (period + 1)
    # Seed with SMA of first *period* values, then apply EMA
    vals = prices.values.astype(float)
    result = float(vals[:period].mean())
    for v in vals[period:]:
        result = alpha * v + (1 - alpha) * result
    return result


def ema_series(prices: pd.Series, period: int) -> pd.Series:
    """Return the full EMA series (same length as *prices*)."""
    return prices.ewm(span=period, adjust=False).mean()


def rsi(prices: pd.Series, period: int = 14) -> float:
    """Relative Strength Index (0–100).

    Uses Wilder's smoothing (RMA).
    """
    if len(prices) < period + 1:
        return float("nan")
    deltas = prices.diff().dropna()
    gains = deltas.clip(lower=0)
    losses = (-deltas).clip(lower=0)

    # Initial averages
    avg_gain = float(gains.iloc[:period].mean())
    avg_loss = float(losses.iloc[:period].mean())

    # Wilder smoothing over remaining bars
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + float(gains.iloc[i])) / period
        avg_loss = (avg_loss * (period - 1) + float(losses.iloc[i])) / period

    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def macd(
    prices: pd.Series,
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
) -> tuple[float, float, float]:
    """MACD indicator.

    Returns:
        (macd_line, signal_line, histogram) — all floats, last bar values.
    """
    if len(prices) < slow + signal:
        nan = float("nan")
        return nan, nan, nan

    fast_ema = ema_series(prices, fast)
    slow_ema = ema_series(prices, slow)
    macd_line = fast_ema - slow_ema
    signal_line = ema_series(macd_line, signal)
    histogram = macd_line - signal_line

    return float(macd_line.iloc[-1]), float(signal_line.iloc[-1]), float(histogram.iloc[-1])


def bollinger_bands(
    prices: pd.Series,
    period: int = 20,
    std: float = 2.0,
) -> tuple[float, float, float]:
    """Bollinger Bands.

    Returns:
        (upper, middle, lower) — all floats, last bar values.
    """
    if len(prices) < period:
        nan = float("nan")
        return nan, nan, nan
    window = prices.iloc[-period:]
    middle = float(window.mean())
    std_val = float(window.std(ddof=1))
    upper = middle + std * std_val
    lower = middle - std * std_val
    return upper, middle, lower


def adx(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    period: int = 14,
) -> float:
    """Average Directional Index (0–100).

    Higher values indicate stronger trend (regardless of direction).
    """
    min_len = period * 2 + 1
    if len(close) < min_len:
        return float("nan")

    # True Range
    prev_close = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    ], axis=1).max(axis=1)

    # Directional movement
    up_move = high.diff()
    down_move = (-low.diff())

    plus_dm = pd.Series(
        np.where((up_move > down_move) & (up_move > 0), up_move, 0.0),
        index=close.index,
    )
    minus_dm = pd.Series(
        np.where((down_move > up_move) & (down_move > 0), down_move, 0.0),
        index=close.index,
    )

    # Wilder smoothing
    def wilder_smooth(series: pd.Series, n: int) -> pd.Series:
        result = [float("nan")] * n
        initial = float(series.iloc[1:n + 1].sum())
        result.append(initial)
        for i in range(n + 1, len(series)):
            val = result[-1] - result[-1] / n + float(series.iloc[i])
            result.append(val)
        return pd.Series(result, index=series.index)

    smoothed_tr = wilder_smooth(tr, period)
    smoothed_plus = wilder_smooth(plus_dm, period)
    smoothed_minus = wilder_smooth(minus_dm, period)

    plus_di = 100.0 * smoothed_plus / smoothed_tr.replace(0, float("nan"))
    minus_di = 100.0 * smoothed_minus / smoothed_tr.replace(0, float("nan"))

    dx = 100.0 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, float("nan"))

    # ADX = Wilder smooth of DX
    adx_series = wilder_smooth(dx.fillna(0), period)
    val = float(adx_series.iloc[-1])
    return val if not np.isnan(val) else float("nan")


def zscore(prices: pd.Series, period: int = 20) -> float:
    """Z-score of the last price relative to a rolling window.

    Returns how many standard deviations the current price is from the
    rolling mean of the last *period* bars.
    """
    if len(prices) < period:
        return float("nan")
    window = prices.iloc[-period:]
    mean = float(window.mean())
    std = float(window.std(ddof=1))
    if std == 0:
        return 0.0
    return (float(prices.iloc[-1]) - mean) / std
