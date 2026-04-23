"""Constance Brown-style momentum indicators.

Implements three pieces of Brown's methodology from *Technical Analysis
for the Trading Professional*:

1. `composite_index`       — her custom RSI-of-RSI-momentum leading indicator
2. `derivative_oscillator` — triple-smoothed RSI derivative for trend confirmation
3. `rsi_range_shift`       — bull/bear regime detection from RSI floor/ceiling

For divergence detection, use the pre-existing
`schwabagent.indicators.detect_divergence(price, rsi_series(price))` —
same four-way classification (classic + hidden bullish/bearish).

All functions are pure — no client, no config, no side effects. Feed them
a `pd.Series` of closing prices and they return numeric outputs or
structured dicts suitable for strategy consumption. Unit-testable
against synthetic price series.

These indicators are practitioner wisdom — they backtest well in some
regimes and poorly in others. Validate with the existing backtest suite
(Monte Carlo / bootstrap / walk-forward) before turning anything on live.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import numpy as np
import pandas as pd

from schwabagent.indicators import rsi_series


# ── Composite Index ──────────────────────────────────────────────────────────


def composite_index(
    close: pd.Series,
    fast: int = 3,
    slow: int = 14,
    signal: int = 9,
) -> pd.Series:
    """Constance Brown's Composite Index.

    Formula from *Technical Analysis for the Trading Professional* p. 119:

        CI = SMA( RSI(slow) - RSI(fast), signal ) + SMA( RSI(fast), 3 )

    Interpretation: the Composite Index leads RSI at turning points because
    it mixes a fast-vs-slow RSI momentum component with a smoothed fast-RSI
    signal line. Brown reports that *divergences on the Composite Index are
    often more reliable than divergences on price/RSI itself* — when price
    makes a higher high but the Composite Index makes a lower high, the
    move is losing momentum even if RSI hasn't rolled over yet.

    Args:
        close: Closing price series (any length ≥ slow + signal).
        fast:  Fast RSI period. Brown uses 3.
        slow:  Slow RSI period. Brown uses 14.
        signal: Smoothing window for the momentum component. Brown uses 9.

    Returns:
        pd.Series aligned with `close`. Leading bars are NaN until all
        inputs have enough history.
    """
    if len(close) < slow + signal + 3:
        return pd.Series(np.nan, index=close.index)

    rsi_fast = rsi_series(close, period=fast)
    rsi_slow = rsi_series(close, period=slow)

    momentum = rsi_slow - rsi_fast
    momentum_smoothed = momentum.rolling(window=signal, min_periods=signal).mean()
    fast_smoothed = rsi_fast.rolling(window=3, min_periods=3).mean()

    return momentum_smoothed + fast_smoothed


# ── Derivative Oscillator ────────────────────────────────────────────────────


def derivative_oscillator(
    close: pd.Series,
    rsi_period: int = 14,
    smooth_1: int = 5,
    smooth_2: int = 3,
    signal_period: int = 9,
) -> pd.Series:
    """Brown's Derivative Oscillator — a triple-smoothed RSI derivative.

    Formula:
        step1 = SMA( RSI(rsi_period),   smooth_1 )
        step2 = SMA( step1,             smooth_2 )
        signal = SMA( step2,            signal_period )
        DO = step2 - signal

    Interpretation: this is RSI processed through two layers of smoothing
    then differenced against its own signal line. The result is a very
    slow-moving oscillator that rarely false-signals — Brown uses it as a
    trend *confirmation* filter rather than an entry trigger. Positive
    values confirm an uptrend; negative values confirm a downtrend.

    Use it alongside faster signals (Composite Index, price/RSI divergence)
    to filter out entries that contradict the prevailing trend.
    """
    if len(close) < rsi_period + smooth_1 + smooth_2 + signal_period:
        return pd.Series(np.nan, index=close.index)

    rsi = rsi_series(close, period=rsi_period)
    step1 = rsi.rolling(window=smooth_1, min_periods=smooth_1).mean()
    step2 = step1.rolling(window=smooth_2, min_periods=smooth_2).mean()
    signal = step2.rolling(window=signal_period, min_periods=signal_period).mean()
    return step2 - signal


# ── RSI Range Shift ──────────────────────────────────────────────────────────


RegimeLabel = Literal["bull", "bear", "neutral"]


@dataclass
class RangeShiftResult:
    """Output of `rsi_range_shift`.

    Attributes:
        regime:     "bull" / "bear" / "neutral"
        floor:      lowest RSI value hit during the lookback window
        ceiling:    highest RSI value hit during the lookback window
        current:    most recent RSI value
        confidence: 0.0-1.0, measures how decisively floor/ceiling pin the regime
    """

    regime: RegimeLabel
    floor: float
    ceiling: float
    current: float
    confidence: float

    def to_dict(self) -> dict:
        return {
            "regime": self.regime,
            "floor": round(self.floor, 2),
            "ceiling": round(self.ceiling, 2),
            "current": round(self.current, 2),
            "confidence": round(self.confidence, 2),
        }


def rsi_range_shift(
    close: pd.Series,
    rsi_period: int = 14,
    lookback: int = 60,
    bull_floor: float = 40.0,
    bear_ceiling: float = 60.0,
) -> RangeShiftResult:
    """Detect Constance Brown's RSI range shift regime.

    Brown's core insight (*Technical Analysis for the Trading Professional*
    ch. 4): RSI oscillates in *different bands* depending on market regime.

        Bull regime:  RSI respects 40-80   (rarely drops below 40)
        Bear regime:  RSI respects 20-60   (rarely rises above 60)

    A persistent shift of the floor (from <30 to >40) or the ceiling (from
    >70 to <60) is a regime change *ahead of* price confirmation — by the
    time price confirms, the RSI range has usually already re-set.

    Classification:
        - floor   ≥ bull_floor   → bull
        - ceiling ≤ bear_ceiling → bear
        - otherwise              → neutral (regime ambiguous)

    Confidence measures how decisively the bands pin the regime:
        bull   = (floor - bull_floor) / (80 - bull_floor), clamped 0-1
        bear   = (bear_ceiling - ceiling) / (bear_ceiling - 20), clamped 0-1
        neutral = 0

    Args:
        close:        Closing price series, ≥ rsi_period + lookback bars.
        rsi_period:   Wilder RSI period. Default 14.
        lookback:     Bars to scan for the range floor/ceiling. Default 60.
        bull_floor:   Minimum floor for bull classification. Default 40.
        bear_ceiling: Maximum ceiling for bear classification. Default 60.

    Returns:
        RangeShiftResult dataclass.
    """
    if len(close) < rsi_period + lookback:
        return RangeShiftResult(
            regime="neutral", floor=float("nan"), ceiling=float("nan"),
            current=float("nan"), confidence=0.0,
        )

    rsi = rsi_series(close, period=rsi_period).dropna()
    if len(rsi) < lookback:
        return RangeShiftResult(
            regime="neutral", floor=float("nan"), ceiling=float("nan"),
            current=float("nan"), confidence=0.0,
        )

    window = rsi.iloc[-lookback:]
    floor = float(window.min())
    ceiling = float(window.max())
    current = float(rsi.iloc[-1])

    regime: RegimeLabel = "neutral"
    confidence = 0.0

    if floor >= bull_floor:
        regime = "bull"
        confidence = max(0.0, min(1.0, (floor - bull_floor) / max(1e-6, 80.0 - bull_floor)))
    elif ceiling <= bear_ceiling:
        regime = "bear"
        confidence = max(0.0, min(1.0, (bear_ceiling - ceiling) / max(1e-6, bear_ceiling - 20.0)))

    return RangeShiftResult(
        regime=regime,
        floor=floor,
        ceiling=ceiling,
        current=current,
        confidence=confidence,
    )


# ── Divergence Detection ─────────────────────────────────────────────────────
# See `schwabagent.indicators.detect_divergence` — already implemented, covers
# classic + hidden bullish/bearish in the Brown style. No need to reimplement.
