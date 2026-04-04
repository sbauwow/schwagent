"""Tests for RiskManager — position limits, drawdown kill switch."""
from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from schwabagent.config import Config
from schwabagent.persistence import StateStore
from schwabagent.risk import RiskManager
from schwabagent.schwab_client import AccountSummary, Position


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def tmp_dir(tmp_path):
    return str(tmp_path)


@pytest.fixture
def config(tmp_dir) -> Config:
    c = Config(
        SCHWAB_API_KEY="test_key",
        SCHWAB_APP_SECRET="test_secret",
        STATE_DIR=tmp_dir,
        DRY_RUN=True,
        MAX_POSITION_PCT=0.10,
        MAX_POSITION_VALUE=5000.0,
        MAX_TOTAL_EXPOSURE=50000.0,
        MAX_DRAWDOWN_PCT=15.0,
        MIN_SIGNAL_SCORE=1.0,
        MIN_ORDER_VALUE=100.0,
        MAX_ORDER_VALUE=2000.0,
    )
    return c


@pytest.fixture
def state(tmp_dir) -> StateStore:
    return StateStore(tmp_dir)


@pytest.fixture
def risk(config, state) -> RiskManager:
    return RiskManager(config, state)


def _make_account(
    total_value: float = 100_000.0,
    cash: float = 50_000.0,
    positions: list[Position] | None = None,
) -> AccountSummary:
    return AccountSummary(
        account_hash="hash123",
        account_number="****1234",
        total_value=total_value,
        cash_available=cash,
        positions=positions or [],
    )


# ── Kill switch ───────────────────────────────────────────────────────────────

class TestKillSwitch:
    def test_initially_not_killed(self, risk):
        assert not risk.is_killed()

    def test_kill_sets_flag(self, risk):
        risk.kill("test reason")
        assert risk.is_killed()

    def test_unkill_clears_flag(self, risk):
        risk.kill("reason")
        risk.unkill()
        assert not risk.is_killed()

    def test_kill_persists_across_reload(self, config, state, tmp_dir):
        r1 = RiskManager(config, state)
        r1.kill("persistent kill")
        # New instance from same state dir
        r2 = RiskManager(config, StateStore(tmp_dir))
        assert r2.is_killed()

    def test_can_buy_blocked_when_killed(self, risk):
        risk.kill("test")
        account = _make_account()
        allowed, reason = risk.can_buy("AAPL", 10, 100.0, account)
        assert not allowed
        assert "kill" in reason.lower() or "Kill" in reason


# ── can_buy ───────────────────────────────────────────────────────────────────

class TestCanBuy:
    def test_basic_buy_allowed(self, risk):
        account = _make_account()
        allowed, reason = risk.can_buy("AAPL", 5, 100.0, account)
        assert allowed, reason

    def test_order_below_minimum(self, risk):
        account = _make_account()
        # 1 share @ $10 = $10 < MIN_ORDER_VALUE=$100
        allowed, reason = risk.can_buy("AAPL", 1, 10.0, account)
        assert not allowed
        assert "minimum" in reason.lower()

    def test_order_exceeds_max_order(self, risk):
        account = _make_account()
        # 100 shares @ $100 = $10_000 > MAX_ORDER_VALUE=$2000
        allowed, reason = risk.can_buy("AAPL", 100, 100.0, account)
        assert not allowed
        assert "max order" in reason.lower() or "exceeds" in reason.lower()

    def test_order_exceeds_max_position_value(self, risk):
        account = _make_account()
        # 50 shares @ $150 = $7500 > MAX_POSITION_VALUE=$5000
        allowed, reason = risk.can_buy("AAPL", 50, 150.0, account)
        assert not allowed

    def test_order_exceeds_position_pct(self, risk):
        account = _make_account(total_value=10_000.0, cash=10_000.0)
        # 15 shares @ $100 = $1500 = 15% > MAX_POSITION_PCT=10%
        allowed, reason = risk.can_buy("AAPL", 15, 100.0, account)
        assert not allowed
        assert "portfolio" in reason.lower() or "%" in reason

    def test_insufficient_cash(self, risk):
        account = _make_account(cash=50.0)
        # 10 shares @ $100 = $1000 but only $50 cash
        allowed, reason = risk.can_buy("AAPL", 10, 100.0, account)
        assert not allowed
        assert "cash" in reason.lower() or "Insufficient" in reason

    def test_total_exposure_cap(self, risk):
        # Build account with positions near the cap
        positions = [
            Position("SPY", 400, 48_000.0, 100.0, 0.0, 0.48),  # $48k of $50k cap used
        ]
        account = _make_account(
            total_value=200_000.0,
            cash=100_000.0,
            positions=positions,
        )
        # 15 shares @ $300 = $4500 → exposure would be $52500 > $50000
        allowed, reason = risk.can_buy("MSFT", 15, 300.0, account)
        assert not allowed
        assert "exposure" in reason.lower() or "cap" in reason.lower()


# ── Drawdown kill switch ──────────────────────────────────────────────────────

class TestDrawdown:
    def test_no_drawdown_initially(self, risk):
        ok, dd = risk.check_drawdown(100_000.0)
        assert ok
        assert dd == 0.0

    def test_update_peak(self, risk):
        risk.update_peak(100_000.0)
        risk.update_peak(120_000.0)
        assert risk._peak_value == 120_000.0

    def test_peak_never_decreases(self, risk):
        risk.update_peak(100_000.0)
        risk.update_peak(80_000.0)
        assert risk._peak_value == 100_000.0

    def test_within_drawdown_limit(self, risk):
        risk.update_peak(100_000.0)
        ok, dd = risk.check_drawdown(90_000.0)  # -10% drawdown, limit is -15%
        assert ok
        assert abs(dd - 10.0) < 0.1

    def test_exceeds_drawdown_triggers_kill(self, risk):
        risk.update_peak(100_000.0)
        ok, dd = risk.check_drawdown(80_000.0)  # -20% > limit of -15%
        assert not ok
        assert risk.is_killed()
        assert dd > 15.0

    def test_drawdown_message_in_kill_reason(self, risk):
        risk.update_peak(100_000.0)
        risk.check_drawdown(80_000.0)
        assert "drawdown" in risk._kill_reason.lower()


# ── record_trade ──────────────────────────────────────────────────────────────

class TestRecordTrade:
    def test_trade_appended_to_history(self, risk, state):
        risk.record_trade("AAPL", "BUY", 10, 150.0, strategy="momentum")
        history = state.get_trade_history()
        assert len(history) == 1
        t = history[0]
        assert t["symbol"] == "AAPL"
        assert t["side"] == "BUY"
        assert t["quantity"] == 10
        assert t["price"] == 150.0

    def test_trade_value_calculated(self, risk, state):
        risk.record_trade("MSFT", "SELL", 5, 300.0, strategy="composite")
        history = state.get_trade_history()
        assert history[0]["value"] == 1500.0

    def test_dry_run_flag_recorded(self, risk, state):
        risk.record_trade("TSLA", "BUY", 2, 200.0)
        history = state.get_trade_history()
        assert history[0]["dry_run"] is True
