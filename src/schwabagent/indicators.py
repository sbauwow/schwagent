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


# ══════════════════════════════════════════════════════════════════════════════
# VOLATILITY
# ══════════════════════════════════════════════════════════════════════════════


def atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> float:
    """Average True Range — Wilder's volatility measure.

    Returns the last ATR value. Used for volatility-based stops (e.g., 2×ATR
    trailing stop) and position sizing (risk per share = ATR).
    """
    if len(close) < period + 1:
        return float("nan")
    prev_c = close.shift(1)
    tr = pd.concat([high - low, (high - prev_c).abs(), (low - prev_c).abs()], axis=1).max(axis=1)
    atr_s = tr.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    return float(atr_s.iloc[-1])


def atr_series(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
    """Full ATR series."""
    prev_c = close.shift(1)
    tr = pd.concat([high - low, (high - prev_c).abs(), (low - prev_c).abs()], axis=1).max(axis=1)
    return tr.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()


# ══════════════════════════════════════════════════════════════════════════════
# ADAPTIVE — Kaufman
# ══════════════════════════════════════════════════════════════════════════════


def kama(prices: pd.Series, er_period: int = 10, fast: int = 2, slow: int = 30) -> float:
    """Kaufman Adaptive Moving Average — adjusts speed based on market noise.

    In a trending market (high efficiency ratio), KAMA tracks price closely.
    In a choppy market (low ER), KAMA barely moves — avoiding whipsaws.

    Args:
        er_period: Efficiency Ratio lookback.
        fast: Fast EMA constant (2 = EMA(2), very responsive).
        slow: Slow EMA constant (30 = EMA(30), very smooth).
    """
    if len(prices) < er_period + 1:
        return float("nan")
    vals = prices.values.astype(float)
    fast_sc = 2.0 / (fast + 1)
    slow_sc = 2.0 / (slow + 1)

    # Seed KAMA with first value after warmup
    k = vals[er_period]
    for i in range(er_period + 1, len(vals)):
        direction = abs(vals[i] - vals[i - er_period])
        volatility = sum(abs(vals[j] - vals[j - 1]) for j in range(i - er_period + 1, i + 1))
        er = direction / volatility if volatility > 0 else 0.0
        sc = (er * (fast_sc - slow_sc) + slow_sc) ** 2
        k = k + sc * (vals[i] - k)
    return float(k)


def kama_series(prices: pd.Series, er_period: int = 10, fast: int = 2, slow: int = 30) -> pd.Series:
    """Full KAMA series."""
    vals = prices.values.astype(float)
    n = len(vals)
    out = np.full(n, np.nan)
    if n < er_period + 1:
        return pd.Series(out, index=prices.index)

    fast_sc = 2.0 / (fast + 1)
    slow_sc = 2.0 / (slow + 1)
    out[er_period] = vals[er_period]

    for i in range(er_period + 1, n):
        direction = abs(vals[i] - vals[i - er_period])
        volatility = sum(abs(vals[j] - vals[j - 1]) for j in range(i - er_period + 1, i + 1))
        er = direction / volatility if volatility > 0 else 0.0
        sc = (er * (fast_sc - slow_sc) + slow_sc) ** 2
        out[i] = out[i - 1] + sc * (vals[i] - out[i - 1])

    return pd.Series(out, index=prices.index)


def efficiency_ratio(prices: pd.Series, period: int = 10) -> float:
    """Kaufman Efficiency Ratio — signal-to-noise measure (0 to 1).

    ER = |net price change| / sum of |bar-to-bar changes|.
    1.0 = perfect trend (every bar moves in one direction).
    0.0 = pure noise (lots of movement, no net progress).

    Use to decide when to trade: ER > 0.6 = trending, ER < 0.3 = choppy.
    """
    if len(prices) < period + 1:
        return float("nan")
    vals = prices.values.astype(float)
    direction = abs(vals[-1] - vals[-period - 1])
    volatility = sum(abs(vals[-period + i] - vals[-period + i - 1]) for i in range(period))
    return float(direction / volatility) if volatility > 0 else 0.0


# ══════════════════════════════════════════════════════════════════════════════
# MULTI-TIMEFRAME MOMENTUM — Pring
# ══════════════════════════════════════════════════════════════════════════════


def roc(prices: pd.Series, period: int = 12) -> float:
    """Rate of Change — percentage change over N periods."""
    if len(prices) < period + 1:
        return float("nan")
    return float((prices.iloc[-1] / prices.iloc[-period - 1] - 1) * 100)


def kst(prices: pd.Series) -> tuple[float, float]:
    """Pring's Know Sure Thing — multi-timeframe smoothed ROC.

    Combines 4 ROC periods (10, 15, 20, 30) smoothed by SMAs (10, 10, 10, 15)
    with increasing weights (1, 2, 3, 4). Signal line = SMA(9) of KST.

    Returns (kst_value, signal_value).
    """
    if len(prices) < 45:
        nan = float("nan")
        return nan, nan

    roc1 = prices.pct_change(10) * 100
    roc2 = prices.pct_change(15) * 100
    roc3 = prices.pct_change(20) * 100
    roc4 = prices.pct_change(30) * 100

    sm1 = roc1.rolling(10).mean()
    sm2 = roc2.rolling(10).mean()
    sm3 = roc3.rolling(10).mean()
    sm4 = roc4.rolling(15).mean()

    kst_line = 1 * sm1 + 2 * sm2 + 3 * sm3 + 4 * sm4
    signal_line = kst_line.rolling(9).mean()

    return float(kst_line.iloc[-1]), float(signal_line.iloc[-1])


# ══════════════════════════════════════════════════════════════════════════════
# VOLUME ANALYSIS
# ══════════════════════════════════════════════════════════════════════════════


def obv(close: pd.Series, volume: pd.Series) -> float:
    """On Balance Volume — cumulative volume flow.

    Rising OBV confirms price uptrend. Divergence (price up, OBV down)
    warns of weak buying pressure.
    """
    if len(close) < 2:
        return float("nan")
    sign = np.sign(close.diff().fillna(0))
    return float((sign * volume).cumsum().iloc[-1])


def obv_series(close: pd.Series, volume: pd.Series) -> pd.Series:
    """Full OBV series."""
    sign = np.sign(close.diff().fillna(0))
    return (sign * volume).cumsum()


def cmf(high: pd.Series, low: pd.Series, close: pd.Series, volume: pd.Series,
        period: int = 20) -> float:
    """Chaikin Money Flow — institutional accumulation/distribution (-1 to +1).

    Positive = buying pressure (closes near highs on volume).
    Negative = selling pressure (closes near lows on volume).
    """
    if len(close) < period:
        return float("nan")
    hl_range = high - low
    clv = ((close - low) - (high - close)) / hl_range.replace(0, np.nan)
    mf_volume = clv * volume
    return float(mf_volume.iloc[-period:].sum() / volume.iloc[-period:].sum())


def mfi(high: pd.Series, low: pd.Series, close: pd.Series, volume: pd.Series,
        period: int = 14) -> float:
    """Money Flow Index — RSI weighted by volume (0-100).

    Like RSI but incorporates volume. Overbought > 80, oversold < 20.
    Better than RSI for spotting divergences with institutional activity.
    """
    if len(close) < period + 1:
        return float("nan")
    tp = (high + low + close) / 3
    mf = tp * volume
    tp_diff = tp.diff()

    pos_mf = pd.Series(np.where(tp_diff > 0, mf, 0), index=close.index)
    neg_mf = pd.Series(np.where(tp_diff < 0, mf, 0), index=close.index)

    pos_sum = float(pos_mf.iloc[-period:].sum())
    neg_sum = float(neg_mf.iloc[-period:].sum())

    if neg_sum == 0:
        return 100.0
    ratio = pos_sum / neg_sum
    return 100.0 - (100.0 / (1.0 + ratio))


def vwap(high: pd.Series, low: pd.Series, close: pd.Series, volume: pd.Series) -> float:
    """Volume Weighted Average Price — institutional fair value benchmark.

    Typically computed intraday from session open. This version uses the
    full series provided (caller should filter to the session).
    """
    if len(close) < 1:
        return float("nan")
    tp = (high + low + close) / 3
    cumvol = volume.cumsum()
    if float(cumvol.iloc[-1]) == 0:
        return float(close.iloc[-1])
    return float((tp * volume).cumsum().iloc[-1] / cumvol.iloc[-1])


# ══════════════════════════════════════════════════════════════════════════════
# DIVERGENCE DETECTION — Constance Brown
# ══════════════════════════════════════════════════════════════════════════════


def rsi_series(prices: pd.Series, period: int = 14) -> pd.Series:
    """Full RSI series using Wilder's smoothing."""
    delta = prices.diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    avg_gain = gain.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100.0 - (100.0 / (1.0 + rs))


def detect_divergence(
    price: pd.Series,
    oscillator: pd.Series,
    lookback: int = 20,
    min_swing: float = 0.5,
) -> str:
    """Detect bullish/bearish divergence between price and an oscillator.

    Constance Brown's approach: compare the last two swing lows/highs in
    price vs the corresponding oscillator values.

    Returns: "bullish", "bearish", "hidden_bullish", "hidden_bearish", or "none".

    - Bullish: price makes lower low, oscillator makes higher low (reversal up)
    - Bearish: price makes higher high, oscillator makes lower high (reversal down)
    - Hidden bullish: price higher low, oscillator lower low (trend continuation up)
    - Hidden bearish: price lower high, oscillator higher high (trend continuation down)
    """
    if len(price) < lookback or len(oscillator) < lookback:
        return "none"

    p = price.iloc[-lookback:].values.astype(float)
    o = oscillator.iloc[-lookback:].values.astype(float)

    # Find swing lows (local minima)
    lows = []
    for i in range(1, len(p) - 1):
        if p[i] < p[i - 1] and p[i] < p[i + 1]:
            lows.append(i)

    # Find swing highs (local maxima)
    highs = []
    for i in range(1, len(p) - 1):
        if p[i] > p[i - 1] and p[i] > p[i + 1]:
            highs.append(i)

    # Bullish divergence: last two swing lows
    if len(lows) >= 2:
        l1, l2 = lows[-2], lows[-1]
        if p[l2] < p[l1] - min_swing and o[l2] > o[l1]:
            return "bullish"
        if p[l2] > p[l1] + min_swing and o[l2] < o[l1]:
            return "hidden_bullish"

    # Bearish divergence: last two swing highs
    if len(highs) >= 2:
        h1, h2 = highs[-2], highs[-1]
        if p[h2] > p[h1] + min_swing and o[h2] < o[h1]:
            return "bearish"
        if p[h2] < p[h1] - min_swing and o[h2] > o[h1]:
            return "hidden_bearish"

    return "none"


# ══════════════════════════════════════════════════════════════════════════════
# CHANNELS
# ══════════════════════════════════════════════════════════════════════════════


def keltner_channels(
    high: pd.Series, low: pd.Series, close: pd.Series,
    ema_period: int = 20, atr_period: int = 14, multiplier: float = 2.0,
) -> tuple[float, float, float]:
    """Keltner Channels — ATR-based bands around an EMA.

    Better than Bollinger in trending markets (ATR doesn't spike as much
    as standard deviation during breakouts).

    Returns: (upper, middle, lower).
    """
    if len(close) < max(ema_period, atr_period + 1):
        nan = float("nan")
        return nan, nan, nan
    mid = float(close.ewm(span=ema_period, adjust=False).mean().iloc[-1])
    a = atr(high, low, close, atr_period)
    return mid + multiplier * a, mid, mid - multiplier * a


def donchian_channels(high: pd.Series, low: pd.Series, period: int = 20) -> tuple[float, float, float]:
    """Donchian Channels — highest high / lowest low over N periods.

    Breakout system: price above upper = bullish breakout.
    Turtle Traders used 20-day Donchian for entries, 10-day for exits.

    Returns: (upper, middle, lower).
    """
    if len(high) < period:
        nan = float("nan")
        return nan, nan, nan
    upper = float(high.iloc[-period:].max())
    lower = float(low.iloc[-period:].min())
    return upper, (upper + lower) / 2, lower


# ══════════════════════════════════════════════════════════════════════════════
# OSCILLATORS
# ══════════════════════════════════════════════════════════════════════════════


def stochastic(
    high: pd.Series, low: pd.Series, close: pd.Series,
    k_period: int = 14, d_period: int = 3,
) -> tuple[float, float]:
    """Stochastic Oscillator (%K, %D) — 0 to 100.

    Better than RSI in ranging markets. Overbought > 80, oversold < 20.
    %K = where price sits within the N-period range.
    %D = SMA of %K (signal line).

    Returns: (%K, %D).
    """
    if len(close) < k_period + d_period:
        nan = float("nan")
        return nan, nan
    lowest = low.rolling(k_period).min()
    highest = high.rolling(k_period).max()
    k = 100 * (close - lowest) / (highest - lowest).replace(0, np.nan)
    d = k.rolling(d_period).mean()
    return float(k.iloc[-1]), float(d.iloc[-1])


def williams_r(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> float:
    """Williams %R — momentum oscillator (-100 to 0).

    Overbought > -20, oversold < -80. Inverted stochastic.
    """
    if len(close) < period:
        return float("nan")
    highest = float(high.iloc[-period:].max())
    lowest = float(low.iloc[-period:].min())
    if highest == lowest:
        return -50.0
    return float((highest - close.iloc[-1]) / (highest - lowest) * -100)


def cci(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 20) -> float:
    """Commodity Channel Index — measures price deviation from the mean.

    CCI > +100 = overbought / strong uptrend.
    CCI < -100 = oversold / strong downtrend.
    """
    if len(close) < period:
        return float("nan")
    tp = (high + low + close) / 3
    tp_sma = tp.rolling(period).mean()
    tp_mad = tp.rolling(period).apply(lambda x: np.abs(x - x.mean()).mean(), raw=True)
    cci_val = (tp - tp_sma) / (0.015 * tp_mad)
    return float(cci_val.iloc[-1])


# ══════════════════════════════════════════════════════════════════════════════
# ICHIMOKU — Complete trading system
# ══════════════════════════════════════════════════════════════════════════════


def ichimoku(
    high: pd.Series, low: pd.Series, close: pd.Series,
    tenkan: int = 9, kijun: int = 26, senkou_b: int = 52,
) -> dict[str, float]:
    """Ichimoku Kinko Hyo — complete trend/momentum/support/resistance system.

    Returns dict with:
        tenkan_sen: Conversion line (fast signal)
        kijun_sen: Base line (medium signal)
        senkou_a: Leading Span A (cloud boundary, projected 26 bars ahead)
        senkou_b: Leading Span B (cloud boundary, projected 26 bars ahead)
        chikou_span: Lagging span (close shifted back 26 bars)
        cloud_top/bottom: Current cloud boundaries
        signal: "bullish", "bearish", or "neutral"
    """
    result: dict[str, float | str] = {}
    if len(close) < senkou_b + kijun:
        return {k: float("nan") for k in
                ["tenkan_sen", "kijun_sen", "senkou_a", "senkou_b",
                 "chikou_span", "cloud_top", "cloud_bottom", "signal"]}

    def midline(h: pd.Series, l: pd.Series, period: int) -> pd.Series:
        return (h.rolling(period).max() + l.rolling(period).min()) / 2

    tenkan_s = midline(high, low, tenkan)
    kijun_s = midline(high, low, kijun)
    span_a = ((tenkan_s + kijun_s) / 2).shift(kijun)
    span_b = midline(high, low, senkou_b).shift(kijun)
    chikou = close.shift(-kijun)

    t = float(tenkan_s.iloc[-1])
    k = float(kijun_s.iloc[-1])
    sa = float(span_a.iloc[-1]) if not pd.isna(span_a.iloc[-1]) else float("nan")
    sb = float(span_b.iloc[-1]) if not pd.isna(span_b.iloc[-1]) else float("nan")
    ch = float(chikou.iloc[-kijun - 1]) if len(chikou) > kijun else float("nan")
    price = float(close.iloc[-1])

    cloud_top = max(sa, sb) if not (np.isnan(sa) or np.isnan(sb)) else float("nan")
    cloud_bottom = min(sa, sb) if not (np.isnan(sa) or np.isnan(sb)) else float("nan")

    # Signal logic
    if not np.isnan(cloud_top):
        if price > cloud_top and t > k:
            sig = "bullish"
        elif price < cloud_bottom and t < k:
            sig = "bearish"
        else:
            sig = "neutral"
    else:
        sig = "neutral"

    return {
        "tenkan_sen": round(t, 4),
        "kijun_sen": round(k, 4),
        "senkou_a": round(sa, 4) if not np.isnan(sa) else sa,
        "senkou_b": round(sb, 4) if not np.isnan(sb) else sb,
        "chikou_span": round(ch, 4) if not np.isnan(ch) else ch,
        "cloud_top": round(cloud_top, 4) if not np.isnan(cloud_top) else cloud_top,
        "cloud_bottom": round(cloud_bottom, 4) if not np.isnan(cloud_bottom) else cloud_bottom,
        "signal": sig,
    }


# ══════════════════════════════════════════════════════════════════════════════
# TREND QUALITY
# ══════════════════════════════════════════════════════════════════════════════


def aroon(high: pd.Series, low: pd.Series, period: int = 25) -> tuple[float, float, float]:
    """Aroon indicator — trend age and direction.

    Aroon Up = how recently the highest high occurred (0-100).
    Aroon Down = how recently the lowest low occurred (0-100).
    Aroon Oscillator = Up - Down (-100 to +100).

    Returns: (aroon_up, aroon_down, oscillator).
    """
    if len(high) < period + 1:
        nan = float("nan")
        return nan, nan, nan
    h_window = high.iloc[-(period + 1):]
    l_window = low.iloc[-(period + 1):]
    days_since_high = period - h_window.values.argmax()
    days_since_low = period - l_window.values.argmin()
    up = (period - days_since_high) / period * 100
    down = (period - days_since_low) / period * 100
    return float(up), float(down), float(up - down)


# ══════════════════════════════════════════════════════════════════════════════
# TRAILING STOPS
# ══════════════════════════════════════════════════════════════════════════════


def chandelier_exit(
    high: pd.Series, low: pd.Series, close: pd.Series,
    atr_period: int = 22, multiplier: float = 3.0,
) -> tuple[float, float]:
    """Chandelier Exit — ATR-based trailing stop.

    Long stop = highest high - multiplier × ATR (trail from peak).
    Short stop = lowest low + multiplier × ATR.

    Returns: (long_stop, short_stop).
    """
    if len(close) < max(atr_period + 1, 22):
        nan = float("nan")
        return nan, nan
    a = atr(high, low, close, atr_period)
    highest = float(high.iloc[-22:].max())
    lowest = float(low.iloc[-22:].min())
    return highest - multiplier * a, lowest + multiplier * a


def parabolic_sar(high: pd.Series, low: pd.Series, af_start: float = 0.02,
                  af_step: float = 0.02, af_max: float = 0.20) -> float:
    """Parabolic SAR — trailing stop that accelerates with the trend.

    Returns the current SAR value. Price above SAR = long, below = short.
    """
    if len(high) < 3:
        return float("nan")
    h = high.values.astype(float)
    l = low.values.astype(float)
    n = len(h)

    # Initialize
    is_long = True
    sar = l[0]
    ep = h[0]  # extreme point
    af = af_start

    for i in range(1, n):
        prev_sar = sar
        sar = prev_sar + af * (ep - prev_sar)

        if is_long:
            sar = min(sar, l[i - 1])
            if i >= 2:
                sar = min(sar, l[i - 2])
            if l[i] < sar:
                is_long = False
                sar = ep
                ep = l[i]
                af = af_start
            else:
                if h[i] > ep:
                    ep = h[i]
                    af = min(af + af_step, af_max)
        else:
            sar = max(sar, h[i - 1])
            if i >= 2:
                sar = max(sar, h[i - 2])
            if h[i] > sar:
                is_long = True
                sar = ep
                ep = h[i]
                af = af_start
            else:
                if l[i] < ep:
                    ep = l[i]
                    af = min(af + af_step, af_max)

    return float(sar)


# ══════════════════════════════════════════════════════════════════════════════
# ELDER
# ══════════════════════════════════════════════════════════════════════════════


def elder_ray(close: pd.Series, high: pd.Series, low: pd.Series,
              period: int = 13) -> tuple[float, float]:
    """Elder Ray — Bull Power and Bear Power.

    Bull Power = High - EMA (buyer strength above equilibrium).
    Bear Power = Low - EMA (seller strength below equilibrium).

    Buy when: Bear Power < 0 but rising, and Bull Power > 0.
    Sell when: Bull Power > 0 but falling, and Bear Power < 0.

    Returns: (bull_power, bear_power).
    """
    if len(close) < period:
        nan = float("nan")
        return nan, nan
    ema_val = float(close.ewm(span=period, adjust=False).mean().iloc[-1])
    return float(high.iloc[-1]) - ema_val, float(low.iloc[-1]) - ema_val


# ══════════════════════════════════════════════════════════════════════════════
# STRUCTURE — Support / Resistance
# ══════════════════════════════════════════════════════════════════════════════


def pivot_points(high: float, low: float, close: float) -> dict[str, float]:
    """Classic pivot points from prior period's HLC.

    Returns: PP, R1, R2, R3, S1, S2, S3.
    Used by floor traders and institutional desks for intraday S/R levels.
    """
    pp = (high + low + close) / 3
    return {
        "pp": round(pp, 4),
        "r1": round(2 * pp - low, 4),
        "r2": round(pp + (high - low), 4),
        "r3": round(high + 2 * (pp - low), 4),
        "s1": round(2 * pp - high, 4),
        "s2": round(pp - (high - low), 4),
        "s3": round(low - 2 * (high - pp), 4),
    }


def fibonacci_levels(high: float, low: float) -> dict[str, float]:
    """Fibonacci retracement levels from a swing high to low.

    Returns: levels at 0%, 23.6%, 38.2%, 50%, 61.8%, 78.6%, 100%.
    """
    diff = high - low
    return {
        "0.0": round(high, 4),
        "0.236": round(high - 0.236 * diff, 4),
        "0.382": round(high - 0.382 * diff, 4),
        "0.500": round(high - 0.500 * diff, 4),
        "0.618": round(high - 0.618 * diff, 4),
        "0.786": round(high - 0.786 * diff, 4),
        "1.0": round(low, 4),
    }


# ══════════════════════════════════════════════════════════════════════════════
# ORIGINAL — kept at bottom for module compatibility
# ══════════════════════════════════════════════════════════════════════════════


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
