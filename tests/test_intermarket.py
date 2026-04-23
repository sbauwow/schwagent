"""Tests for intermarket regime detection model."""
from __future__ import annotations

import json
import math
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import numpy as np
import pytest

from schwabagent.config import Config
from schwabagent.intermarket import (
    DEFAULT_REFERENCE_SYMBOLS,
    Regime,
    RegimeModel,
    RegimeResult,
    RegimeSignal,
    regime_sizing_factor,
)


# ── Helpers ────────────────────────────────────────────────────────────────────

def _make_config(**overrides) -> Config:
    defaults = {
        "DRY_RUN": True,
        "STATE_DIR": "/tmp/schwab-test-intermarket",
        "REGIME_ENABLED": True,
        "REGIME_REFERENCE_SYMBOLS": "SPY,TLT,HYG,GLD,IWM,UUP,VIXY",
    }
    defaults.update(overrides)
    return Config(**defaults)


def _make_quotes(**prices) -> dict:
    """Create a quotes dict from symbol=price pairs."""
    return {sym: {"lastPrice": price} for sym, price in prices.items()}


def _make_series(values: list[float]) -> pd.Series:
    return pd.Series(values, dtype=float)


def _make_histories(**series_data) -> dict:
    """Create price_histories dict. Each value is a list of floats -> pd.DataFrame."""
    result = {}
    for sym, values in series_data.items():
        df = pd.DataFrame({"close": values})
        result[sym] = df
    return result


def _rising_series(start: float, n: int, step: float = 1.0) -> list[float]:
    """Create a steadily rising price series."""
    return [start + i * step for i in range(n)]


def _falling_series(start: float, n: int, step: float = 1.0) -> list[float]:
    """Create a steadily falling price series."""
    return [start - i * step for i in range(n)]


def _flat_series(value: float, n: int) -> list[float]:
    return [value] * n


# ── Regime Enum Tests ──────────────────────────────────────────────────────────

class TestRegimeEnum:
    def test_all_regimes_have_labels(self):
        for regime in Regime:
            assert isinstance(regime.label(), str)
            assert len(regime.label()) > 0

    def test_all_regimes_have_descriptions(self):
        for regime in Regime:
            assert isinstance(regime.description(), str)
            assert len(regime.description()) > 10

    def test_all_regimes_have_strategy_weights(self):
        for regime in Regime:
            weights = regime.strategy_weights()
            assert isinstance(weights, dict)
            assert "momentum" in weights
            assert "etf_rotation" in weights
            assert "mean_reversion" in weights
            assert "trend_following" in weights

    def test_bull_favors_momentum(self):
        weights = Regime.BULL.strategy_weights()
        assert weights["momentum"] > 1.0

    def test_risk_off_disables_momentum(self):
        weights = Regime.RISK_OFF.strategy_weights()
        assert weights["momentum"] == 0.0

    def test_bear_favors_trend_following(self):
        weights = Regime.BEAR.strategy_weights()
        assert weights["trend_following"] > 1.0

    def test_correction_favors_mean_reversion(self):
        weights = Regime.CORRECTION.strategy_weights()
        assert weights["mean_reversion"] > 1.0


# ── RegimeSignal Tests ─────────────────────────────────────────────────────────

class TestRegimeSignal:
    def test_positive_indicator(self):
        s = RegimeSignal(name="test", value=1.5, signal=1)
        assert s.indicator() == "+"

    def test_negative_indicator(self):
        s = RegimeSignal(name="test", value=-1.5, signal=-1)
        assert s.indicator() == "-"

    def test_neutral_indicator(self):
        s = RegimeSignal(name="test", value=0.0, signal=0)
        assert s.indicator() == "="


# ── Signal Computation Tests ───────────────────────────────────────────────────

class TestSignalComputation:
    def setup_method(self):
        self.model = RegimeModel(_make_config())

    def test_spy_trend_bullish(self):
        """SPY above SMA200 should give +1."""
        # 210 data points rising, current price above average
        prices = _rising_series(400, 210, 0.5)
        quotes = _make_quotes(SPY=505.0)  # well above sma200
        histories = _make_histories(SPY=prices)
        sig = self.model._compute_spy_trend(quotes, histories)
        assert sig.signal == 1
        assert sig.name == "SPY Trend"

    def test_spy_trend_bearish(self):
        """SPY below SMA200 should give -1."""
        prices = _rising_series(400, 210, 0.5)
        quotes = _make_quotes(SPY=380.0)  # below sma200
        histories = _make_histories(SPY=prices)
        sig = self.model._compute_spy_trend(quotes, histories)
        assert sig.signal == -1

    def test_spy_trend_no_data(self):
        """Missing data should give 0."""
        sig = self.model._compute_spy_trend({}, None)
        assert sig.signal == 0

    def test_spy_momentum_positive(self):
        """Positive 20-day ROC should give +1."""
        # Need 21+ data points; recent prices higher than 20 days ago
        prices = _rising_series(100, 30, 1.0)
        histories = _make_histories(SPY=prices)
        sig = self.model._compute_spy_momentum({}, histories)
        assert sig.signal == 1

    def test_spy_momentum_negative(self):
        """Negative 20-day ROC should give -1."""
        prices = _falling_series(130, 30, 1.0)
        histories = _make_histories(SPY=prices)
        sig = self.model._compute_spy_momentum({}, histories)
        assert sig.signal == -1

    def test_credit_stress_risk_on(self):
        """HYG outperforming TLT should give +1."""
        hyg = _rising_series(70, 35, 0.5)
        tlt = _flat_series(90, 35)
        histories = _make_histories(HYG=hyg, TLT=tlt)
        sig = self.model._compute_credit_stress({}, histories)
        assert sig.signal == 1

    def test_credit_stress_risk_off(self):
        """HYG underperforming TLT should give -1."""
        hyg = _falling_series(75, 35, 0.3)
        tlt = _rising_series(85, 35, 0.5)
        histories = _make_histories(HYG=hyg, TLT=tlt)
        sig = self.model._compute_credit_stress({}, histories)
        assert sig.signal == -1

    def test_safe_haven_demand(self):
        """GLD above SMA50 gives +1."""
        prices = _rising_series(170, 55, 0.5)
        quotes = _make_quotes(GLD=200.0)
        histories = _make_histories(GLD=prices)
        sig = self.model._compute_safe_haven(quotes, histories)
        assert sig.signal == 1

    def test_volatility_low(self):
        """VIX below 20 should give +1."""
        quotes = _make_quotes(VIX=15.0)
        sig = self.model._compute_volatility(quotes, None)
        assert sig.signal == 1

    def test_volatility_high(self):
        """VIX above 30 should give -1."""
        quotes = _make_quotes(VIX=35.0)
        sig = self.model._compute_volatility(quotes, None)
        assert sig.signal == -1

    def test_volatility_neutral(self):
        """VIX 20-30 should give 0."""
        quotes = _make_quotes(VIX=25.0)
        sig = self.model._compute_volatility(quotes, None)
        assert sig.signal == 0

    def test_volatility_vixy_fallback(self):
        """VIXY proxy should work when VIX unavailable."""
        quotes = _make_quotes(VIXY=18.0)
        sig = self.model._compute_volatility(quotes, None)
        assert sig.signal == 1


# ── Regime Classification Tests ────────────────────────────────────────────────

class TestRegimeClassification:
    def setup_method(self):
        self.model = RegimeModel(_make_config())

    def _make_signals(self, values: list[int]) -> list[RegimeSignal]:
        """Create 7 signals from a list of -1/0/+1 values."""
        names = [
            "SPY Trend", "SPY Momentum", "Credit Stress",
            "Safe Haven (GLD)", "Breadth (IWM/SPY)", "Dollar (UUP)",
            "Volatility (VIX)",
        ]
        return [
            RegimeSignal(name=names[i], value=float(v), signal=v, weight=1.0)
            for i, v in enumerate(values)
        ]

    def test_bull_regime(self):
        """Score >= 4 should be Bull."""
        signals = self._make_signals([1, 1, 1, 1, 1, -1, 1])  # score=5
        regime, conf = self.model._classify(signals)
        assert regime == Regime.BULL

    def test_recovery_regime(self):
        """Score 2-3 should be Recovery."""
        signals = self._make_signals([1, 1, 0, 0, 0, 0, 1])  # score=3
        regime, conf = self.model._classify(signals)
        assert regime == Regime.RECOVERY

    def test_correction_regime(self):
        """Score 0-1 should be Correction."""
        signals = self._make_signals([1, -1, 0, 0, 1, -1, 0])  # score=0
        regime, conf = self.model._classify(signals)
        assert regime == Regime.CORRECTION

    def test_bear_regime(self):
        """Score -1 to -2 should be Bear."""
        signals = self._make_signals([-1, -1, 0, 0, 0, 0, 0])  # score=-2
        regime, conf = self.model._classify(signals)
        assert regime == Regime.BEAR

    def test_risk_off_regime(self):
        """Score <= -3 should be RiskOff."""
        signals = self._make_signals([-1, -1, -1, -1, -1, 1, -1])  # score=-5
        regime, conf = self.model._classify(signals)
        assert regime == Regime.RISK_OFF

    def test_stagflation_special_case(self):
        """GLD +1, SPY -1, Dollar +1 triggers Stagflation."""
        # SPY trend=-1, SPY mom=-1, credit=0, GLD=+1, breadth=0, dollar=+1, vol=0
        signals = self._make_signals([-1, -1, 0, 1, 0, 1, 0])
        regime, conf = self.model._classify(signals)
        assert regime == Regime.STAGFLATION

    def test_confidence_high_for_decisive(self):
        """All signals agreeing should give high confidence."""
        signals = self._make_signals([1, 1, 1, 1, 1, 1, 1])  # score=7
        regime, conf = self.model._classify(signals)
        assert conf >= 0.9

    def test_confidence_low_for_mixed(self):
        """Mixed signals should give lower confidence."""
        signals = self._make_signals([1, -1, 1, -1, 0, 0, 0])  # score=0
        regime, conf = self.model._classify(signals)
        assert conf < 0.3


# ── Full Detection Tests ──────────────────────────────────────────────────────

class TestFullDetection:
    def setup_method(self):
        self.model = RegimeModel(_make_config())

    def test_detect_returns_result(self):
        """detect() should always return a RegimeResult."""
        result = self.model.detect({}, None)
        assert isinstance(result, RegimeResult)
        assert isinstance(result.regime, Regime)

    def test_detect_bull_scenario(self):
        """Fully bullish inputs should yield Bull regime."""
        # Build data: everything rising, low vol
        n = 210
        spy_prices = _rising_series(400, n, 0.5)
        gld_prices = _falling_series(200, max(n, 55), 0.2)  # gold falling (not safe haven)
        uup_prices = _falling_series(30, max(n, 55), 0.05)  # dollar weakening
        hyg_prices = _rising_series(70, max(n, 35), 0.3)
        tlt_prices = _flat_series(90, max(n, 35))
        iwm_prices = _rising_series(180, n, 0.8)  # IWM outperforming

        quotes = _make_quotes(
            SPY=spy_prices[-1] + 10,  # above sma200
            GLD=gld_prices[-1] - 10,  # below sma50
            UUP=uup_prices[-1] - 5,   # below sma50
            VIX=14.0,                   # low vol
        )
        histories = _make_histories(
            SPY=spy_prices,
            GLD=gld_prices[:55],
            UUP=uup_prices[:55],
            HYG=hyg_prices[:35],
            TLT=tlt_prices[:35],
            IWM=iwm_prices,
        )

        result = self.model.detect(quotes, histories)
        # Should be bullish-leaning (Bull or Recovery)
        assert result.regime in (Regime.BULL, Regime.RECOVERY)
        assert len(result.signals) == 8

    def test_detect_disabled(self):
        """When REGIME_ENABLED=False, should return neutral Recovery."""
        model = RegimeModel(_make_config(REGIME_ENABLED=False))
        result = model.detect({}, None)
        assert result.regime == Regime.RECOVERY
        assert result.confidence == 0.0
        assert result.signals == []

    def test_regime_change_detection(self):
        """Should detect regime change across calls."""
        # First detection with no data → Correction (all signals 0)
        r1 = self.model.detect({}, None)
        # r1 doesn't "change" because previous is None from fresh model
        # Now force a regime change:
        self.model._previous_regime = Regime.BULL
        r2 = self.model.detect({}, None)
        # With no data, all signals are 0, score=0 → Correction
        assert r2.regime == Regime.CORRECTION
        assert r2.previous_regime == Regime.BULL
        assert r2.changed is True


# ── Sizing Factor Tests ────────────────────────────────────────────────────────

class TestSizingFactor:
    def test_known_strategy(self):
        factor = regime_sizing_factor(Regime.BULL, "momentum")
        assert factor == 1.2

    def test_unknown_strategy_defaults_to_1(self):
        factor = regime_sizing_factor(Regime.BULL, "unknown_strat")
        assert factor == 1.0

    def test_risk_off_momentum_zero(self):
        factor = regime_sizing_factor(Regime.RISK_OFF, "momentum")
        assert factor == 0.0

    def test_bear_trend_following(self):
        factor = regime_sizing_factor(Regime.BEAR, "trend_following")
        assert factor == 1.3


# ── RegimeResult Tests ─────────────────────────────────────────────────────────

class TestRegimeResult:
    def test_to_dict(self):
        result = RegimeResult(
            regime=Regime.BULL,
            confidence=0.85,
            signals=[RegimeSignal("test", 1.0, 1, 1.0)],
            timestamp="2025-01-01T00:00:00",
            previous_regime=Regime.CORRECTION,
            composite_score=5,
        )
        d = result.to_dict()
        assert d["regime"] == "bull"
        assert d["regime_label"] == "Bull Market"
        assert d["confidence"] == 0.85
        assert d["changed"] is True
        assert d["previous_regime"] == "correction"
        assert len(d["signals"]) == 1

    def test_changed_false_same_regime(self):
        result = RegimeResult(
            regime=Regime.BULL,
            confidence=0.85,
            signals=[],
            timestamp="2025-01-01T00:00:00",
            previous_regime=Regime.BULL,
        )
        assert result.changed is False

    def test_changed_false_no_previous(self):
        result = RegimeResult(
            regime=Regime.BULL,
            confidence=0.85,
            signals=[],
            timestamp="2025-01-01T00:00:00",
            previous_regime=None,
        )
        assert result.changed is False


# ── Persistence Tests ──────────────────────────────────────────────────────────

class TestPersistence:
    def test_save_and_load(self, tmp_path):
        config = _make_config(STATE_DIR=str(tmp_path))
        model = RegimeModel(config)
        result = model.detect({}, None)

        # Now create a new model and check it loads the persisted regime
        model2 = RegimeModel(config)
        assert model2._previous_regime == result.regime

    def test_load_missing_file(self, tmp_path):
        config = _make_config(STATE_DIR=str(tmp_path / "nonexistent"))
        model = RegimeModel(config)
        assert model._previous_regime is None


# ── Display Test ───────────────────────────────────────────────────────────────

class TestDisplay:
    def test_display_regime_does_not_crash(self):
        """Ensure display_regime runs without error."""
        result = RegimeResult(
            regime=Regime.BULL,
            confidence=0.85,
            signals=[
                RegimeSignal("SPY Trend", 5.0, 1, 1.0),
                RegimeSignal("Volatility (VIX)", 15.0, 1, 1.0),
            ],
            timestamp="2025-01-01T00:00:00",
            previous_regime=Regime.CORRECTION,
            composite_score=5,
        )
        # Just ensure no exceptions
        RegimeModel.display_regime(result)
