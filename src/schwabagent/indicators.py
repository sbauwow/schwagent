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
    Uses Wilder's smoothing (RMA) for TR, +DM, -DM, and DX.
    """
    min_len = period * 2 + 1
    if len(close) < min_len:
        return float("nan")

    h = high.values.astype(float)
    l = low.values.astype(float)
    c = close.values.astype(float)
    n = len(c)

    # True Range, +DM, -DM — skip index 0 (needs previous bar)
    tr = np.empty(n)
    plus_dm = np.empty(n)
    minus_dm = np.empty(n)
    tr[0] = plus_dm[0] = minus_dm[0] = 0.0

    for i in range(1, n):
        hl = h[i] - l[i]
        hc = abs(h[i] - c[i - 1])
        lc = abs(l[i] - c[i - 1])
        tr[i] = max(hl, hc, lc)

        up = h[i] - h[i - 1]
        down = l[i - 1] - l[i]
        plus_dm[i] = up if up > down and up > 0 else 0.0
        minus_dm[i] = down if down > up and down > 0 else 0.0

    # Wilder smoothing for TR, +DM, -DM:
    # Seed = SUM of first `period` values (indices 1..period).
    # Then: smoothed[i] = smoothed[i-1] - smoothed[i-1]/period + value[i]
    def wilder_sum(arr: np.ndarray, p: int) -> np.ndarray:
        """Wilder smooth seeded with sum (for TR, DM)."""
        out = np.full(n, np.nan)
        out[p] = np.sum(arr[1:p + 1])
        for i in range(p + 1, n):
            out[i] = out[i - 1] - out[i - 1] / p + arr[i]
        return out

    smoothed_tr = wilder_sum(tr, period)
    smoothed_plus = wilder_sum(plus_dm, period)
    smoothed_minus = wilder_sum(minus_dm, period)

    # +DI, -DI (0–100 range): DI = 100 * smoothed_DM / smoothed_TR
    with np.errstate(divide="ignore", invalid="ignore"):
        plus_di = np.where(smoothed_tr > 0, 100.0 * smoothed_plus / smoothed_tr, 0.0)
        minus_di = np.where(smoothed_tr > 0, 100.0 * smoothed_minus / smoothed_tr, 0.0)
        di_sum = plus_di + minus_di
        dx = np.where(di_sum > 0, 100.0 * np.abs(plus_di - minus_di) / di_sum, 0.0)

    # ADX = Wilder smooth of DX, seeded with MEAN of first `period` valid DX values.
    # DX is first valid at index `period` (where smoothed TR/DM become valid).
    first_valid_dx = period
    adx_seed_end = first_valid_dx + period
    adx_vals = np.full(n, np.nan)
    if adx_seed_end <= n:
        adx_vals[adx_seed_end - 1] = np.mean(dx[first_valid_dx:adx_seed_end])
        for i in range(adx_seed_end, n):
            adx_vals[i] = (adx_vals[i - 1] * (period - 1) + dx[i]) / period

    val = adx_vals[-1]
    return float(val) if not np.isnan(val) else float("nan")


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
