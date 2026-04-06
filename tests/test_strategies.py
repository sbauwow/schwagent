"""Tests for all 7 trading strategies — classification logic and scan/execute flow.

Tests the core _classify() methods directly (pure logic, no API calls),
plus scan/execute with fully mocked dependencies.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from unittest.mock import MagicMock, patch, PropertyMock

import pandas as pd
import numpy as np
import pytest

from schwabagent.config import Config
from schwabagent.strategies.base import Signal, SIGNAL_SCORE


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_config(**overrides) -> Config:
    """Create a Config with safe defaults for testing."""
    defaults = {
        "DRY_RUN": True,
        "STATE_DIR": "/tmp/schwab-test-strategies",
    }
    defaults.update(overrides)
    return Config(**defaults)


def _mock_client():
    client = MagicMock()
    client.get_quotes.return_value = {}
    client.get_ohlcv.return_value = pd.DataFrame()
    return client


def _mock_risk():
    risk = MagicMock()
    risk.can_buy.return_value = (True, "")
    risk.is_killed.return_value = False
    return risk


def _mock_state():
    state = MagicMock()
    state.get_strategy_pnl.return_value = {}
    return state


@dataclass
class FakePosition:
    symbol: str
    quantity: float
    avg_price: float = 100.0
    market_value: float = 0.0


@dataclass
class FakeAccount:
    account_hash: str = "test_hash"
    cash_available: float = 100_000.0
    total_value: float = 200_000.0
    unsettled_cash: float = 0.0
    positions: list = None

    def __post_init__(self):
        if self.positions is None:
            self.positions = []


def _make_ohlcv(n: int = 100, base_price: float = 150.0, trend: float = 0.0) -> pd.DataFrame:
    """Generate a synthetic OHLCV DataFrame."""
    dates = pd.date_range("2024-01-01", periods=n, freq="B")
    close = base_price + trend * np.arange(n) + np.random.normal(0, 1, n).cumsum() * 0.5
    close = np.maximum(close, 1.0)  # no negatives
    return pd.DataFrame({
        "open": close * 0.999,
        "high": close * 1.005,
        "low": close * 0.995,
        "close": close,
        "volume": np.random.randint(1_000_000, 10_000_000, n),
    }, index=dates)


# ══════════════════════════════════════════════════════════════════════════════
# MOMENTUM STRATEGY
# ══════════════════════════════════════════════════════════════════════════════

class TestMomentumClassify:
    """Test MomentumStrategy._classify() directly — pure signal logic."""

    def _make_strategy(self):
        from schwabagent.strategies.momentum import MomentumStrategy
        return MomentumStrategy(
            client=_mock_client(), config=_make_config(),
            risk=_mock_risk(), state=_mock_state(),
        )

    def test_strong_buy(self):
        s = self._make_strategy()
        # price > SMA20 > SMA50 AND RSI in [40,70] AND MACD hist > 0
        assert s._classify(price=110, sma20=105, sma50=100, rsi_val=55, macd_hist=0.5) == Signal.STRONG_BUY

    def test_buy(self):
        s = self._make_strategy()
        # price > SMA50 AND RSI < 65 (but not matching STRONG_BUY)
        assert s._classify(price=105, sma20=110, sma50=100, rsi_val=50, macd_hist=-0.1) == Signal.BUY

    def test_sell(self):
        s = self._make_strategy()
        # price < SMA20 AND RSI > 60
        assert s._classify(price=95, sma20=100, sma50=90, rsi_val=65, macd_hist=0.1) == Signal.SELL

    def test_strong_sell(self):
        s = self._make_strategy()
        # price < SMA50 AND MACD hist < 0 AND RSI > 60
        assert s._classify(price=85, sma20=90, sma50=95, rsi_val=65, macd_hist=-0.5) == Signal.STRONG_SELL

    def test_hold(self):
        s = self._make_strategy()
        # price < SMA50 but RSI <= 60, and MACD > 0 → no sell/buy conditions
        assert s._classify(price=90, sma20=95, sma50=100, rsi_val=50, macd_hist=0.1) == Signal.HOLD

    def test_hold_when_rsi_too_high_for_buy(self):
        s = self._make_strategy()
        # price > SMA20 > SMA50, RSI=75 > 70 → not STRONG_BUY. RSI >= 65 → not BUY. Not SELL. → HOLD
        result = s._classify(price=102, sma20=100, sma50=98, rsi_val=75, macd_hist=0.1)
        assert result == Signal.HOLD

    def test_strong_buy_boundary_rsi_40(self):
        s = self._make_strategy()
        assert s._classify(price=110, sma20=105, sma50=100, rsi_val=40, macd_hist=0.1) == Signal.STRONG_BUY

    def test_strong_buy_boundary_rsi_70(self):
        s = self._make_strategy()
        assert s._classify(price=110, sma20=105, sma50=100, rsi_val=70, macd_hist=0.1) == Signal.STRONG_BUY


class TestMomentumScan:
    """Test MomentumStrategy.scan() with mocked data."""

    def test_scan_returns_list(self):
        from schwabagent.strategies.momentum import MomentumStrategy
        config = _make_config(MOMENTUM_SYMBOLS="AAPL")
        client = _mock_client()
        # Provide enough data for indicators
        df = _make_ohlcv(100, base_price=150, trend=0.2)
        client.get_ohlcv.return_value = df
        s = MomentumStrategy(client=client, config=config, risk=_mock_risk(), state=_mock_state())
        opps = s.scan()
        assert isinstance(opps, list)

    def test_scan_empty_when_no_data(self):
        from schwabagent.strategies.momentum import MomentumStrategy
        config = _make_config(MOMENTUM_SYMBOLS="AAPL")
        client = _mock_client()
        client.get_ohlcv.return_value = pd.DataFrame()
        s = MomentumStrategy(client=client, config=config, risk=_mock_risk(), state=_mock_state())
        opps = s.scan()
        assert opps == []

    def test_scan_opportunity_has_required_keys(self):
        from schwabagent.strategies.momentum import MomentumStrategy
        config = _make_config(MOMENTUM_SYMBOLS="AAPL")
        client = _mock_client()
        df = _make_ohlcv(100, base_price=150, trend=0.3)
        client.get_ohlcv.return_value = df
        s = MomentumStrategy(client=client, config=config, risk=_mock_risk(), state=_mock_state())
        opps = s.scan()
        if opps:
            opp = opps[0]
            for key in ("symbol", "signal", "score", "price", "strategy", "reason"):
                assert key in opp, f"Missing key: {key}"
            assert opp["strategy"] == "momentum"


# ══════════════════════════════════════════════════════════════════════════════
# MEAN REVERSION STRATEGY
# ══════════════════════════════════════════════════════════════════════════════

class TestMeanReversionClassify:
    def _make_strategy(self):
        from schwabagent.strategies.mean_reversion import MeanReversionStrategy
        return MeanReversionStrategy(
            client=_mock_client(), config=_make_config(),
            risk=_mock_risk(), state=_mock_state(),
        )

    def test_strong_buy(self):
        s = self._make_strategy()
        # price < lower AND RSI < 30
        assert s._classify(price=90, upper=110, lower=95, rsi_val=25) == Signal.STRONG_BUY

    def test_buy_below_lower_band(self):
        s = self._make_strategy()
        # price < lower but RSI >= 30
        assert s._classify(price=93, upper=110, lower=95, rsi_val=40) == Signal.BUY

    def test_buy_rsi_oversold(self):
        s = self._make_strategy()
        # RSI < 35 but price > lower
        assert s._classify(price=100, upper=110, lower=95, rsi_val=32) == Signal.BUY

    def test_strong_sell(self):
        s = self._make_strategy()
        # price > upper AND RSI > 70
        assert s._classify(price=115, upper=110, lower=95, rsi_val=75) == Signal.STRONG_SELL

    def test_sell_above_upper(self):
        s = self._make_strategy()
        # price > upper but RSI <= 70
        assert s._classify(price=112, upper=110, lower=95, rsi_val=60) == Signal.SELL

    def test_sell_rsi_overbought(self):
        s = self._make_strategy()
        # RSI > 65 but price < upper
        assert s._classify(price=108, upper=110, lower=95, rsi_val=67) == Signal.SELL

    def test_hold(self):
        s = self._make_strategy()
        # Everything in normal range
        assert s._classify(price=102, upper=110, lower=95, rsi_val=50) == Signal.HOLD

    def test_hold_neutral_rsi(self):
        s = self._make_strategy()
        # Price between bands, RSI between 35-65
        assert s._classify(price=100, upper=108, lower=92, rsi_val=50) == Signal.HOLD


class TestMeanReversionScan:
    def test_scan_empty_insufficient_data(self):
        from schwabagent.strategies.mean_reversion import MeanReversionStrategy
        config = _make_config(MEAN_REVERSION_SYMBOLS="AAPL")
        client = _mock_client()
        client.get_ohlcv.return_value = pd.DataFrame()
        s = MeanReversionStrategy(client=client, config=config, risk=_mock_risk(), state=_mock_state())
        assert s.scan() == []


# ══════════════════════════════════════════════════════════════════════════════
# TREND FOLLOWING STRATEGY
# ══════════════════════════════════════════════════════════════════════════════

class TestTrendFollowingClassify:
    def _make_strategy(self):
        from schwabagent.strategies.trend_following import TrendFollowingStrategy
        return TrendFollowingStrategy(
            client=_mock_client(), config=_make_config(),
            risk=_mock_risk(), state=_mock_state(),
        )

    def test_strong_buy(self):
        s = self._make_strategy()
        # EMA20 > EMA50 > EMA200 AND ADX > 35
        assert s._classify(ema20=110, ema50=105, ema200=100, adx_val=40) == Signal.STRONG_BUY

    def test_buy(self):
        s = self._make_strategy()
        # EMA20 > EMA50 AND ADX > 30 (but not full bull alignment or ADX < 35)
        assert s._classify(ema20=110, ema50=105, ema200=115, adx_val=32) == Signal.BUY

    def test_sell(self):
        s = self._make_strategy()
        # EMA20 < EMA50 (no full bear alignment)
        assert s._classify(ema20=100, ema50=105, ema200=95, adx_val=15) == Signal.SELL

    def test_strong_sell(self):
        s = self._make_strategy()
        # EMA20 < EMA50 < EMA200 AND ADX > 35
        assert s._classify(ema20=95, ema50=100, ema200=110, adx_val=40) == Signal.STRONG_SELL

    def test_hold_emas_equal(self):
        s = self._make_strategy()
        # EMA20 > EMA50 but ADX too low
        assert s._classify(ema20=101, ema50=100, ema200=float("nan"), adx_val=15) == Signal.HOLD

    def test_hold_no_ema200(self):
        s = self._make_strategy()
        # EMA20 > EMA50, no EMA200, ADX < 30
        result = s._classify(ema20=110, ema50=105, ema200=float("nan"), adx_val=20)
        assert result == Signal.HOLD

    def test_strong_buy_requires_ema200(self):
        s = self._make_strategy()
        # Even with perfect alignment but no EMA200 → only BUY (if ADX > 30)
        result = s._classify(ema20=110, ema50=105, ema200=float("nan"), adx_val=40)
        assert result == Signal.BUY  # STRONG_BUY requires has_200


class TestTrendFollowingScan:
    def test_scan_empty_insufficient_data(self):
        from schwabagent.strategies.trend_following import TrendFollowingStrategy
        config = _make_config(TREND_FOLLOWING_SYMBOLS="AAPL")
        client = _mock_client()
        client.get_ohlcv.return_value = pd.DataFrame()
        s = TrendFollowingStrategy(client=client, config=config, risk=_mock_risk(), state=_mock_state())
        assert s.scan() == []


# ══════════════════════════════════════════════════════════════════════════════
# COMPOSITE STRATEGY
# ══════════════════════════════════════════════════════════════════════════════

class TestCompositeScoreToSignal:
    """Test the module-level _score_to_signal helper."""

    def test_strong_buy(self):
        from schwabagent.strategies.composite import _score_to_signal
        assert _score_to_signal(1.5) == Signal.STRONG_BUY
        assert _score_to_signal(2.0) == Signal.STRONG_BUY

    def test_buy(self):
        from schwabagent.strategies.composite import _score_to_signal
        assert _score_to_signal(0.5) == Signal.BUY
        assert _score_to_signal(1.0) == Signal.BUY

    def test_hold(self):
        from schwabagent.strategies.composite import _score_to_signal
        assert _score_to_signal(0.0) == Signal.HOLD
        assert _score_to_signal(0.49) == Signal.HOLD
        assert _score_to_signal(-0.49) == Signal.HOLD

    def test_sell(self):
        from schwabagent.strategies.composite import _score_to_signal
        assert _score_to_signal(-0.5) == Signal.SELL  # score > -0.5 is HOLD, -0.5 is not > -0.5
        assert _score_to_signal(-0.51) == Signal.SELL
        assert _score_to_signal(-1.0) == Signal.SELL

    def test_strong_sell(self):
        from schwabagent.strategies.composite import _score_to_signal
        assert _score_to_signal(-1.5) == Signal.STRONG_SELL
        assert _score_to_signal(-2.0) == Signal.STRONG_SELL


class TestCompositeScan:
    def test_scan_returns_list(self):
        from schwabagent.strategies.composite import CompositeStrategy
        config = _make_config(
            MOMENTUM_SYMBOLS="AAPL",
            MEAN_REVERSION_SYMBOLS="AAPL",
            TREND_FOLLOWING_SYMBOLS="AAPL",
        )
        client = _mock_client()
        client.get_ohlcv.return_value = pd.DataFrame()
        s = CompositeStrategy(client=client, config=config, risk=_mock_risk(), state=_mock_state())
        opps = s.scan()
        assert isinstance(opps, list)

    def test_scan_opportunity_keys(self):
        from schwabagent.strategies.composite import CompositeStrategy
        config = _make_config(
            MOMENTUM_SYMBOLS="AAPL",
            MEAN_REVERSION_SYMBOLS="AAPL",
            TREND_FOLLOWING_SYMBOLS="AAPL",
        )
        client = _mock_client()
        df = _make_ohlcv(250, base_price=150, trend=0.3)
        client.get_ohlcv.return_value = df
        s = CompositeStrategy(client=client, config=config, risk=_mock_risk(), state=_mock_state())
        opps = s.scan()
        if opps:
            opp = opps[0]
            assert opp["strategy"] == "composite"
            assert "sub_signals" in opp
            assert "sub_scores" in opp


# ══════════════════════════════════════════════════════════════════════════════
# ETF ROTATION STRATEGY
# ══════════════════════════════════════════════════════════════════════════════

class TestETFRotation:
    def _make_strategy(self, **config_overrides):
        from schwabagent.strategies.etf_rotation import ETFRotationStrategy
        config = _make_config(
            ETF_UNIVERSE="SPY,QQQ,TLT",
            ETF_TOP_N=2,
            ETF_BEAR_FILTER=False,
            **config_overrides,
        )
        client = _mock_client()
        df = _make_ohlcv(300, base_price=400, trend=0.1)
        client.get_ohlcv.return_value = df
        return ETFRotationStrategy(
            client=client, config=config, risk=_mock_risk(), state=_mock_state(),
        )

    def test_scan_returns_list(self):
        s = self._make_strategy()
        opps = s.scan()
        assert isinstance(opps, list)

    def test_scan_empty_universe(self):
        from schwabagent.strategies.etf_rotation import ETFRotationStrategy
        config = _make_config(ETF_UNIVERSE="")
        client = _mock_client()
        s = ETFRotationStrategy(client=client, config=config, risk=_mock_risk(), state=_mock_state())
        assert s.scan() == []

    def test_scan_opportunity_has_required_keys(self):
        s = self._make_strategy()
        opps = s.scan()
        if opps:
            opp = opps[0]
            for key in ("symbol", "signal", "score", "strategy", "reason", "momentum_score", "rank"):
                assert key in opp, f"Missing key: {key}"
            assert opp["strategy"] == "etf_rotation"

    def test_name_property(self):
        s = self._make_strategy()
        assert s.name == "etf_rotation"

    def test_scores_table(self):
        s = self._make_strategy()
        s.scan()
        table = s.scores_table()
        assert isinstance(table, list)


# ══════════════════════════════════════════════════════════════════════════════
# ETF SCALP STRATEGY
# ══════════════════════════════════════════════════════════════════════════════

class TestETFScalp:
    def _make_strategy(self):
        from schwabagent.strategies.etf_scalp import ETFScalpStrategy
        config = _make_config(SCALP_UNIVERSE="SPY,QQQ")
        client = _mock_client()
        client.get_intraday_ohlcv.return_value = pd.DataFrame()
        return ETFScalpStrategy(
            client=client, config=config, risk=_mock_risk(), state=_mock_state(),
        )

    def test_name_property(self):
        s = self._make_strategy()
        assert s.name == "etf_scalp"

    def test_scan_returns_list(self):
        s = self._make_strategy()
        account = FakeAccount()
        s.set_account(account)
        opps = s.scan()
        assert isinstance(opps, list)

    def test_scalp_status_initial(self):
        s = self._make_strategy()
        status = s.scalp_status()
        assert status["open_positions"] == 0

    def test_set_account_initializes_tranches(self):
        s = self._make_strategy()
        account = FakeAccount(cash_available=100_000, unsettled_cash=0)
        s.set_account(account)
        assert len(s._tranches) == s.config.SCALP_TRANCHES

    def test_get_available_tranche(self):
        s = self._make_strategy()
        account = FakeAccount(cash_available=100_000, unsettled_cash=0)
        s.set_account(account)
        tranche = s._get_available_tranche()
        assert tranche is not None
        assert tranche.available is True


# ══════════════════════════════════════════════════════════════════════════════
# CONVICTION HOLD STRATEGY
# ══════════════════════════════════════════════════════════════════════════════

class TestConvictionHold:
    def _make_strategy(self):
        import tempfile
        from schwabagent.strategies.conviction_hold import ConvictionHoldStrategy
        tmpdir = tempfile.mkdtemp()
        config = _make_config(
            CONVICTION_SYMBOLS="RKLB",
            CONVICTION_HOLD_DAYS=30,
            STATE_DIR=tmpdir,
        )
        client = _mock_client()
        client.get_ohlcv.return_value = pd.DataFrame()
        return ConvictionHoldStrategy(
            client=client, config=config, risk=_mock_risk(), state=_mock_state(),
        )

    def test_name_property(self):
        s = self._make_strategy()
        assert s.name == "conviction_hold"

    def test_scan_empty_no_data(self):
        s = self._make_strategy()
        opps = s.scan()
        assert isinstance(opps, list)

    def test_scan_with_data(self):
        import tempfile
        from schwabagent.strategies.conviction_hold import ConvictionHoldStrategy
        tmpdir = tempfile.mkdtemp()
        config = _make_config(
            CONVICTION_SYMBOLS="RKLB",
            STATE_DIR=tmpdir,
        )
        client = _mock_client()
        df = _make_ohlcv(200, base_price=20, trend=0.05)
        client.get_ohlcv.return_value = df
        s = ConvictionHoldStrategy(client=client, config=config, risk=_mock_risk(), state=_mock_state())
        opps = s.scan()
        assert isinstance(opps, list)
        # If entry conditions met, check keys
        if opps:
            opp = opps[0]
            assert opp["strategy"] == "conviction_hold"
            assert "symbol" in opp
            assert "signal" in opp

    def test_conviction_status_empty(self):
        s = self._make_strategy()
        status = s.conviction_status()
        assert status == []


# ══════════════════════════════════════════════════════════════════════════════
# SIGNAL ENUM AND SCORING
# ══════════════════════════════════════════════════════════════════════════════

class TestSignalEnum:
    def test_signal_values(self):
        assert Signal.STRONG_BUY.value == "STRONG_BUY"
        assert Signal.BUY.value == "BUY"
        assert Signal.HOLD.value == "HOLD"
        assert Signal.SELL.value == "SELL"
        assert Signal.STRONG_SELL.value == "STRONG_SELL"

    def test_signal_scores(self):
        assert SIGNAL_SCORE[Signal.STRONG_BUY] == 2.0
        assert SIGNAL_SCORE[Signal.BUY] == 1.0
        assert SIGNAL_SCORE[Signal.HOLD] == 0.0
        assert SIGNAL_SCORE[Signal.SELL] == -1.0
        assert SIGNAL_SCORE[Signal.STRONG_SELL] == -2.0

    def test_all_signals_have_scores(self):
        for sig in Signal:
            assert sig in SIGNAL_SCORE


# ══════════════════════════════════════════════════════════════════════════════
# EXECUTION TESTS (scan → execute flow)
# ══════════════════════════════════════════════════════════════════════════════

class TestMomentumExecute:
    """Test that execute() handles buy/sell/no-account correctly."""

    def _make_strategy(self):
        from schwabagent.strategies.momentum import MomentumStrategy
        config = _make_config(MOMENTUM_SYMBOLS="AAPL", DRY_RUN=True)
        client = _mock_client()
        return MomentumStrategy(
            client=client, config=config, risk=_mock_risk(), state=_mock_state(),
        )

    def test_execute_returns_none_no_account(self):
        s = self._make_strategy()
        opp = {
            "symbol": "AAPL", "signal": Signal.BUY, "score": 1.0,
            "price": 150.0, "strategy": "momentum", "reason": "test",
        }
        result = s.execute(opp)
        assert result is None

    def test_execute_buy_dry_run(self):
        s = self._make_strategy()
        s.set_account(FakeAccount())
        opp = {
            "symbol": "AAPL", "signal": Signal.BUY, "score": 1.0,
            "price": 150.0, "strategy": "momentum", "reason": "test",
        }
        result = s.execute(opp)
        if result is not None:
            assert result["side"] == "BUY"
            assert result["dry_run"] is True

    def test_execute_sell_no_position(self):
        s = self._make_strategy()
        s.set_account(FakeAccount(positions=[]))
        opp = {
            "symbol": "AAPL", "signal": Signal.SELL, "score": -1.0,
            "price": 150.0, "strategy": "momentum", "reason": "test",
        }
        result = s.execute(opp)
        assert result is None  # Can't sell what we don't hold

    def test_execute_sell_with_position(self):
        s = self._make_strategy()
        s.set_account(FakeAccount(positions=[FakePosition("AAPL", 10, 140.0)]))
        opp = {
            "symbol": "AAPL", "signal": Signal.SELL, "score": -1.0,
            "price": 150.0, "strategy": "momentum", "reason": "test",
        }
        result = s.execute(opp)
        assert result is not None
        assert result["side"] == "SELL"
        assert result["symbol"] == "AAPL"

    def test_execute_hold_returns_none(self):
        s = self._make_strategy()
        s.set_account(FakeAccount())
        opp = {
            "symbol": "AAPL", "signal": Signal.HOLD, "score": 0.0,
            "price": 150.0, "strategy": "momentum", "reason": "test",
        }
        result = s.execute(opp)
        assert result is None

    def test_execute_below_min_score(self):
        s = self._make_strategy()
        s.set_account(FakeAccount())
        opp = {
            "symbol": "AAPL", "signal": Signal.BUY, "score": 0.1,
            "price": 150.0, "strategy": "momentum", "reason": "test",
        }
        result = s.execute(opp)
        assert result is None


class TestMeanReversionExecute:
    def test_execute_sell_with_position(self):
        from schwabagent.strategies.mean_reversion import MeanReversionStrategy
        config = _make_config(DRY_RUN=True)
        s = MeanReversionStrategy(
            client=_mock_client(), config=config,
            risk=_mock_risk(), state=_mock_state(),
        )
        s.set_account(FakeAccount(positions=[FakePosition("AAPL", 5, 100.0)]))
        opp = {
            "symbol": "AAPL", "signal": Signal.STRONG_SELL, "score": -2.0,
            "price": 120.0, "strategy": "mean_reversion", "reason": "test",
        }
        result = s.execute(opp)
        assert result is not None
        assert result["side"] == "SELL"


class TestTrendFollowingExecute:
    def test_execute_buy_dry_run(self):
        from schwabagent.strategies.trend_following import TrendFollowingStrategy
        config = _make_config(DRY_RUN=True)
        s = TrendFollowingStrategy(
            client=_mock_client(), config=config,
            risk=_mock_risk(), state=_mock_state(),
        )
        s.set_account(FakeAccount())
        opp = {
            "symbol": "AAPL", "signal": Signal.STRONG_BUY, "score": 2.0,
            "price": 150.0, "strategy": "trend_following", "reason": "test",
        }
        result = s.execute(opp)
        if result:
            assert result["side"] == "BUY"
            assert result["dry_run"] is True


# ══════════════════════════════════════════════════════════════════════════════
# CROSS-STRATEGY TESTS
# ══════════════════════════════════════════════════════════════════════════════

class TestAllStrategiesHaveName:
    """Verify all strategy classes set a unique .name attribute."""

    def test_unique_names(self):
        from schwabagent.strategies.momentum import MomentumStrategy
        from schwabagent.strategies.mean_reversion import MeanReversionStrategy
        from schwabagent.strategies.trend_following import TrendFollowingStrategy
        from schwabagent.strategies.composite import CompositeStrategy
        from schwabagent.strategies.etf_rotation import ETFRotationStrategy
        from schwabagent.strategies.etf_scalp import ETFScalpStrategy
        from schwabagent.strategies.conviction_hold import ConvictionHoldStrategy

        names = [
            MomentumStrategy.name,
            MeanReversionStrategy.name,
            TrendFollowingStrategy.name,
            CompositeStrategy.name,
            ETFRotationStrategy.name,
            ETFScalpStrategy.name,
            ConvictionHoldStrategy.name,
        ]
        assert len(names) == len(set(names)), f"Duplicate strategy names: {names}"
        expected = {"momentum", "mean_reversion", "trend_following", "composite",
                    "etf_rotation", "etf_scalp", "conviction_hold"}
        assert set(names) == expected
