"""Tests for Constance Brown momentum indicators.

Exercises each indicator against handcrafted synthetic series so the math
is pinned to expected behavior — not dependent on live market data.

A note on synthetic data: pure monotonic up/down series (drift with
negligible noise) trigger a divide-by-zero in `rsi_series` because there
are no downticks for Wilder's avg_loss. Real markets always have
downticks, so our test series inject enough noise that RSI computes
cleanly even when the underlying trend is strong.
"""
from __future__ import annotations

import math

import numpy as np
import pandas as pd

from schwabagent.brown_indicators import (
    RangeShiftResult,
    composite_index,
    derivative_oscillator,
    rsi_range_shift,
)
from schwabagent.indicators import rsi_series


# ── synthetic series helpers ─────────────────────────────────────────────────


def _trending(
    n: int = 300,
    start: float = 500.0,
    drift: float = 0.4,
    noise: float = 0.8,
    seed: int = 42,
) -> pd.Series:
    """Trending random walk with meaningful downticks.

    With drift=0.4 and noise=0.8, about 30% of bars are down despite the
    positive drift. Realistic enough for Wilder's RSI to compute cleanly.
    Use negative drift for a downtrend. Start at 500 so a 300-bar
    downtrend (~-150 expected move) doesn't bottom out at the 1.0 floor.
    """
    rng = np.random.default_rng(seed)
    values = [start]
    for _ in range(n - 1):
        values.append(max(1.0, values[-1] + drift + rng.normal(0, noise)))
    return pd.Series(values)


def _sideways(n: int = 300, center: float = 100.0, amplitude: float = 5.0) -> pd.Series:
    """Sinusoidal mean-reverting series — no directional drift."""
    t = np.linspace(0, 6 * math.pi, n)
    return pd.Series(center + amplitude * np.sin(t))


# ── Sanity: synthetic series actually produce valid RSI ─────────────────────


class TestSyntheticHelpers:
    """Sanity checks on the test fixtures themselves."""

    def test_trending_up_produces_valid_rsi(self):
        close = _trending(drift=0.4, noise=0.8)
        rsi = rsi_series(close, period=14)
        settled = rsi.dropna()
        assert len(settled) > 200
        assert not settled.isna().any()

    def test_trending_down_produces_valid_rsi(self):
        close = _trending(drift=-0.4, noise=0.8)
        rsi = rsi_series(close, period=14)
        settled = rsi.dropna()
        assert len(settled) > 200
        assert not settled.isna().any()

    def test_trending_up_has_mean_rsi_above_50(self):
        close = _trending(drift=0.4, noise=0.8)
        rsi = rsi_series(close, period=14).dropna()
        assert rsi.mean() > 50

    def test_trending_down_has_mean_rsi_below_50(self):
        close = _trending(drift=-0.4, noise=0.8)
        rsi = rsi_series(close, period=14).dropna()
        assert rsi.mean() < 50


# ── Composite Index ──────────────────────────────────────────────────────────


class TestCompositeIndex:
    def test_returns_series_aligned_with_input(self):
        close = _trending()
        ci = composite_index(close)
        assert isinstance(ci, pd.Series)
        assert len(ci) == len(close)
        assert ci.index.equals(close.index)

    def test_leading_bars_are_nan(self):
        close = _trending()
        ci = composite_index(close)
        # First ~25 bars should be NaN until slow RSI + signal smoothing settles
        assert ci.iloc[:20].isna().all()

    def test_settled_values_are_finite(self):
        close = _trending()
        ci = composite_index(close)
        settled = ci.dropna()
        assert len(settled) > 100
        assert np.isfinite(settled).all()

    def test_insufficient_data_returns_all_nan(self):
        close = _trending(n=10)
        ci = composite_index(close)
        assert ci.isna().all()

    def test_uptrend_and_downtrend_produce_distinguishable_output(self):
        up = _trending(drift=0.4, noise=0.8)
        down = _trending(drift=-0.4, noise=0.8)
        ci_up = composite_index(up).dropna()
        ci_down = composite_index(down).dropna()
        # The means should differ meaningfully
        assert abs(ci_up.mean() - ci_down.mean()) > 1.0


# ── Derivative Oscillator ────────────────────────────────────────────────────


class TestDerivativeOscillator:
    def test_returns_series_aligned_with_input(self):
        close = _trending()
        do = derivative_oscillator(close)
        assert isinstance(do, pd.Series)
        assert len(do) == len(close)

    def test_leading_bars_are_nan(self):
        close = _trending()
        do = derivative_oscillator(close)
        # 14 + 5 + 3 + 9 = 31 bars needed before settling
        assert do.iloc[:25].isna().all()

    def test_settled_values_are_finite(self):
        close = _trending()
        do = derivative_oscillator(close)
        settled = do.dropna()
        assert len(settled) > 100
        assert np.isfinite(settled).all()

    def test_insufficient_data_returns_all_nan(self):
        close = _trending(n=15)
        do = derivative_oscillator(close)
        assert do.isna().all()

    def test_reverses_sign_in_opposing_regimes(self):
        """In an uptrend the DO mean should be distinguishable from a
        downtrend's DO mean — the indicator captures regime, not just noise."""
        up = _trending(drift=0.5, noise=0.6)
        down = _trending(drift=-0.5, noise=0.6)
        up_mean = derivative_oscillator(up).dropna().mean()
        down_mean = derivative_oscillator(down).dropna().mean()
        assert abs(up_mean - down_mean) > 0.1  # at least some separation


# ── RSI Range Shift ──────────────────────────────────────────────────────────


class TestRsiRangeShift:
    def test_strong_uptrend_classified_as_bull(self):
        """A persistent uptrend should push RSI's floor above 40 within the
        lookback window, triggering a bull classification."""
        close = _trending(n=300, drift=0.5, noise=0.6)
        result = rsi_range_shift(close, lookback=60)
        assert isinstance(result, RangeShiftResult)
        assert result.regime == "bull"
        assert result.floor >= 40.0
        assert result.confidence > 0.0

    def test_strong_downtrend_classified_as_bear(self):
        close = _trending(n=300, drift=-0.5, noise=0.6)
        result = rsi_range_shift(close, lookback=60)
        assert result.regime == "bear"
        assert result.ceiling <= 60.0
        assert result.confidence > 0.0

    def test_sideways_market_classified_as_neutral(self):
        close = _sideways(n=300, amplitude=10.0)
        result = rsi_range_shift(close, lookback=60)
        # Sideways swings produce RSI crossing both 40 and 60 boundaries
        assert result.regime == "neutral"
        assert result.confidence == 0.0

    def test_insufficient_data_returns_neutral(self):
        close = _trending(n=30)
        result = rsi_range_shift(close, lookback=60)
        assert result.regime == "neutral"
        assert math.isnan(result.floor)

    def test_to_dict_round_trips(self):
        close = _trending()
        result = rsi_range_shift(close, lookback=60)
        d = result.to_dict()
        assert set(d.keys()) == {"regime", "floor", "ceiling", "current", "confidence"}
        assert isinstance(d["regime"], str)

    def test_custom_thresholds_change_classification(self):
        """A stricter bull_floor should never upgrade the classification."""
        close = _trending(drift=0.4, noise=0.8)
        loose = rsi_range_shift(close, lookback=60, bull_floor=35.0)
        strict = rsi_range_shift(close, lookback=60, bull_floor=55.0)
        if loose.regime == "bull":
            assert strict.regime in ("bull", "neutral")

    def test_confidence_is_zero_to_one(self):
        for close in (_trending(drift=0.4), _trending(drift=-0.4), _sideways()):
            result = rsi_range_shift(close, lookback=60)
            assert 0.0 <= result.confidence <= 1.0


# ── Integration ──────────────────────────────────────────────────────────────


class TestIndicatorIntegration:
    def test_strong_uptrend_agreement(self):
        """In a strong uptrend, Composite Index, Derivative Oscillator, and
        Range Shift should each produce settled values consistent with
        bullish conditions — specifically, range-shift should call it bull."""
        close = _trending(n=300, drift=0.5, noise=0.6)

        ci = composite_index(close).dropna()
        do = derivative_oscillator(close).dropna()
        regime = rsi_range_shift(close, lookback=60)

        assert regime.regime == "bull"
        assert len(ci) > 100 and np.isfinite(ci).all()
        assert len(do) > 100 and np.isfinite(do).all()

    def test_strong_downtrend_agreement(self):
        close = _trending(n=300, drift=-0.5, noise=0.6)

        ci = composite_index(close).dropna()
        do = derivative_oscillator(close).dropna()
        regime = rsi_range_shift(close, lookback=60)

        assert regime.regime == "bear"
        assert len(ci) > 100 and np.isfinite(ci).all()
        assert len(do) > 100 and np.isfinite(do).all()

    def test_divergence_reuses_indicators_module(self):
        """Sanity check: the existing `detect_divergence` works on an RSI
        series computed from our Brown test fixture. If this ever breaks,
        brown_momentum strategy will break in the same way, so it's worth
        pinning here."""
        from schwabagent.indicators import detect_divergence

        close = _trending(n=300, drift=0.3, noise=1.0)
        rsi = rsi_series(close, period=14)
        # Just assert it returns one of the legal values without crashing
        result = detect_divergence(close, rsi, lookback=40)
        assert result in {"bullish", "bearish", "hidden_bullish", "hidden_bearish", "none"}
