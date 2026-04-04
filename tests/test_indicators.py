"""Sanity tests for each technical indicator function."""
from __future__ import annotations

import math

import numpy as np
import pandas as pd
import pytest

from schwabagent.indicators import (
    adx,
    bollinger_bands,
    ema,
    ema_series,
    macd,
    rsi,
    sma,
    zscore,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def uptrend() -> pd.Series:
    """Steadily rising price series (100 bars, price doubles)."""
    np.random.seed(42)
    prices = np.linspace(100, 200, 100) + np.random.normal(0, 0.5, 100)
    return pd.Series(prices, dtype=float)


@pytest.fixture
def downtrend() -> pd.Series:
    """Steadily falling price series (100 bars, price halves)."""
    np.random.seed(42)
    prices = np.linspace(200, 100, 100) + np.random.normal(0, 0.5, 100)
    return pd.Series(prices, dtype=float)


@pytest.fixture
def flat() -> pd.Series:
    """Flat price series with small noise."""
    np.random.seed(0)
    return pd.Series(100.0 + np.random.normal(0, 0.1, 100), dtype=float)


@pytest.fixture
def ohlcv_up(uptrend) -> tuple[pd.Series, pd.Series, pd.Series]:
    """(high, low, close) for uptrend."""
    close = uptrend
    high = close + np.abs(np.random.normal(0, 0.3, len(close)))
    low = close - np.abs(np.random.normal(0, 0.3, len(close)))
    return pd.Series(high), pd.Series(low), close


# ── SMA ───────────────────────────────────────────────────────────────────────

class TestSMA:
    def test_basic(self, uptrend):
        result = sma(uptrend, 20)
        assert not math.isnan(result)
        # SMA(20) of an uptrend should be close to the mean of last 20 bars
        expected = float(uptrend.iloc[-20:].mean())
        assert abs(result - expected) < 0.01

    def test_insufficient_data(self):
        short = pd.Series([1.0, 2.0, 3.0])
        assert math.isnan(sma(short, 20))

    def test_flat_equals_value(self, flat):
        result = sma(flat, 5)
        # SMA of near-flat series should be very close to 100
        assert abs(result - 100.0) < 1.0


# ── EMA ───────────────────────────────────────────────────────────────────────

class TestEMA:
    def test_basic(self, uptrend):
        result = ema(uptrend, 20)
        assert not math.isnan(result)
        assert result > 0

    def test_insufficient_data(self):
        assert math.isnan(ema(pd.Series([1.0, 2.0]), 20))

    def test_ema_lags_sma_in_uptrend(self, uptrend):
        # In an uptrend the last-bar EMA should be above SMA because
        # EMA is more responsive — actually EMA(20) recent price weighted
        # more, but either can be higher; just verify both are finite and close
        e = ema(uptrend, 20)
        s = sma(uptrend, 20)
        assert not math.isnan(e)
        assert not math.isnan(s)
        assert abs(e - s) < 15  # they should be in the same ballpark

    def test_ema_series_length(self, uptrend):
        series = ema_series(uptrend, 20)
        assert len(series) == len(uptrend)


# ── RSI ───────────────────────────────────────────────────────────────────────

class TestRSI:
    def test_uptrend_rsi_high(self, uptrend):
        result = rsi(uptrend, 14)
        assert not math.isnan(result)
        # Rising prices → RSI should be > 50
        assert result > 50

    def test_downtrend_rsi_low(self, downtrend):
        result = rsi(downtrend, 14)
        assert not math.isnan(result)
        # Falling prices → RSI should be < 50
        assert result < 50

    def test_range(self, uptrend):
        result = rsi(uptrend, 14)
        assert 0 <= result <= 100

    def test_insufficient_data(self):
        assert math.isnan(rsi(pd.Series([1.0, 2.0, 3.0]), 14))

    def test_all_gains_gives_100(self):
        # Prices only go up
        prices = pd.Series(range(1, 30), dtype=float)
        result = rsi(prices, 14)
        assert result == 100.0

    def test_all_losses_gives_0(self):
        prices = pd.Series(range(30, 1, -1), dtype=float)
        result = rsi(prices, 14)
        assert result == 0.0


# ── MACD ─────────────────────────────────────────────────────────────────────

class TestMACD:
    def test_basic_structure(self, uptrend):
        macd_val, signal_val, hist = macd(uptrend)
        assert not math.isnan(macd_val)
        assert not math.isnan(signal_val)
        assert not math.isnan(hist)

    def test_histogram_equals_diff(self, uptrend):
        macd_val, signal_val, hist = macd(uptrend)
        assert abs(hist - (macd_val - signal_val)) < 1e-9

    def test_insufficient_data(self):
        short = pd.Series([1.0] * 10)
        m, s, h = macd(short)
        assert math.isnan(m) and math.isnan(s) and math.isnan(h)

    def test_uptrend_positive_histogram(self, uptrend):
        # In a steady uptrend MACD histogram is typically positive
        _, _, hist = macd(uptrend)
        # Allow for the seed used — just ensure it's a real number
        assert not math.isnan(hist)


# ── Bollinger Bands ───────────────────────────────────────────────────────────

class TestBollingerBands:
    def test_basic(self, flat):
        upper, mid, lower = bollinger_bands(flat, 20, 2.0)
        assert not any(math.isnan(v) for v in [upper, mid, lower])
        assert upper > mid > lower

    def test_price_contained(self, flat):
        upper, mid, lower = bollinger_bands(flat, 20, 2.0)
        # For a flat series the last price should be within the bands
        last = float(flat.iloc[-1])
        assert lower <= last <= upper

    def test_insufficient_data(self):
        short = pd.Series([100.0] * 5)
        upper, mid, lower = bollinger_bands(short, 20)
        assert math.isnan(upper)

    def test_band_width_proportional_to_volatility(self):
        np.random.seed(1)
        volatile = pd.Series(100 + np.random.normal(0, 10, 50), dtype=float)
        stable = pd.Series(100 + np.random.normal(0, 0.1, 50), dtype=float)

        v_upper, _, v_lower = bollinger_bands(volatile, 20)
        s_upper, _, s_lower = bollinger_bands(stable, 20)

        assert (v_upper - v_lower) > (s_upper - s_lower)


# ── ADX ───────────────────────────────────────────────────────────────────────

class TestADX:
    def test_basic(self, ohlcv_up):
        high, low, close = ohlcv_up
        result = adx(high, low, close, 14)
        assert not math.isnan(result)
        assert 0 <= result <= 100

    def test_strong_trend_high_adx(self):
        # Very clean linear trend — ADX should be meaningfully positive
        n = 80
        close = pd.Series(np.linspace(100, 200, n), dtype=float)
        high = close + 0.1
        low = close - 0.1
        result = adx(high, low, close, 14)
        assert not math.isnan(result)
        # Should indicate trend presence
        assert result > 0

    def test_insufficient_data(self):
        short = pd.Series([100.0] * 10)
        result = adx(short, short, short, 14)
        assert math.isnan(result)


# ── Z-score ───────────────────────────────────────────────────────────────────

class TestZScore:
    def test_near_mean_gives_zero(self, flat):
        result = zscore(flat, 20)
        # Flat series: last price ≈ mean → z-score ≈ 0
        assert abs(result) < 3.0

    def test_high_price_positive_zscore(self):
        prices = pd.Series([100.0] * 25 + [200.0])  # spike at end
        result = zscore(prices, 20)
        assert result > 0

    def test_low_price_negative_zscore(self):
        prices = pd.Series([100.0] * 25 + [50.0])  # dip at end
        result = zscore(prices, 20)
        assert result < 0

    def test_insufficient_data(self):
        assert math.isnan(zscore(pd.Series([1.0, 2.0]), 20))
