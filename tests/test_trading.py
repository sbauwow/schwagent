"""Tests for trading_rules, persistence, rate_limiter, scheduler, and order_tracker."""
from __future__ import annotations

import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from schwabagent.config import Config
from schwabagent.persistence import StateStore
from schwabagent.trading_rules import TradingRules
from schwabagent.rate_limiter import RateLimiter
from schwabagent.scheduler import Scheduler, Job
from schwabagent.order_tracker import OrderTracker, PendingOrder


# ── Shared fixtures ──────────────────────────────────────────────────────────

def _make_config(tmp_dir: str, **overrides) -> Config:
    defaults = dict(
        SCHWAB_API_KEY="test",
        SCHWAB_APP_SECRET="test",
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
    defaults.update(overrides)
    return Config(**defaults)


@pytest.fixture
def tmp_dir(tmp_path):
    return str(tmp_path)


@pytest.fixture
def config(tmp_dir) -> Config:
    return _make_config(tmp_dir)


@pytest.fixture
def state(tmp_dir) -> StateStore:
    return StateStore(tmp_dir)


@pytest.fixture
def rules(config, state) -> TradingRules:
    return TradingRules(config, state)


@pytest.fixture
def tracker(config, state) -> OrderTracker:
    return OrderTracker(config, state)


@pytest.fixture
def scheduler(config) -> Scheduler:
    return Scheduler(config)


# ══════════════════════════════════════════════════════════════════════════════
# TradingRules tests
# ══════════════════════════════════════════════════════════════════════════════

class TestTradingRulesPDT:
    """Pattern Day Trader rule: 4+ day trades in 5 days on margin under $25k."""

    def test_pdt_not_triggered_below_limit(self, rules):
        """3/3 round trips used but no same-day opposite trade => allowed."""
        allowed, reason = rules.check_order(
            symbol="AAPL", side="BUY", quantity=10, price=150.0,
            account_value=20_000.0, account_type="MARGIN",
            round_trips=3,
        )
        # No trades in history, so _would_be_day_trade returns False
        assert allowed

    def test_pdt_blocks_at_limit_with_day_trade(self, rules, state):
        """At round_trips >= 3, a buy that would create a day trade is blocked."""
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        # Simulate a SELL of AAPL today (opposite side)
        state.append_trade({
            "symbol": "AAPL", "side": "SELL", "quantity": 10, "price": 150.0,
            "timestamp": f"{today}T10:00:00+00:00",
        })
        allowed, reason = rules.check_order(
            symbol="AAPL", side="BUY", quantity=10, price=150.0,
            account_value=20_000.0, account_type="MARGIN",
            round_trips=3,
        )
        assert not allowed
        assert "PDT" in reason

    def test_pdt_blocks_at_4_round_trips(self, rules, state):
        """round_trips=4 with same-day opposite trade => blocked."""
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        state.append_trade({
            "symbol": "TSLA", "side": "BUY", "quantity": 5, "price": 200.0,
            "timestamp": f"{today}T09:30:00+00:00",
        })
        allowed, reason = rules.check_order(
            symbol="TSLA", side="SELL", quantity=5, price=210.0,
            account_value=20_000.0, account_type="MARGIN",
            round_trips=4,
        )
        assert not allowed
        assert "PDT" in reason

    def test_pdt_not_applicable_above_25k(self, rules, state):
        """Margin account with $25k+ equity is exempt from PDT."""
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        state.append_trade({
            "symbol": "AAPL", "side": "SELL", "quantity": 10, "price": 150.0,
            "timestamp": f"{today}T10:00:00+00:00",
        })
        allowed, reason = rules.check_order(
            symbol="AAPL", side="BUY", quantity=10, price=150.0,
            account_value=30_000.0, account_type="MARGIN",
            round_trips=5,
        )
        assert allowed

    def test_pdt_allows_non_day_trade(self, rules, state):
        """At limit, but buying a symbol NOT sold today => allowed."""
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        state.append_trade({
            "symbol": "MSFT", "side": "SELL", "quantity": 5, "price": 300.0,
            "timestamp": f"{today}T10:00:00+00:00",
        })
        # Buying AAPL (no opposite SELL today for AAPL)
        allowed, reason = rules.check_order(
            symbol="AAPL", side="BUY", quantity=10, price=150.0,
            account_value=20_000.0, account_type="MARGIN",
            round_trips=3,
        )
        assert allowed


class TestTradingRulesCashAccount:
    """PDT does not apply to cash accounts."""

    def test_cash_account_exempt_from_pdt(self, rules, state):
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        state.append_trade({
            "symbol": "AAPL", "side": "SELL", "quantity": 10, "price": 150.0,
            "timestamp": f"{today}T10:00:00+00:00",
        })
        allowed, reason = rules.check_order(
            symbol="AAPL", side="BUY", quantity=10, price=150.0,
            account_value=5_000.0, account_type="CASH",
            round_trips=10,
        )
        assert allowed

    def test_cash_account_default(self, rules):
        """Default account_type is CASH, so PDT should not apply."""
        allowed, reason = rules.check_order(
            symbol="AAPL", side="BUY", quantity=10, price=150.0,
            account_value=5_000.0,
            round_trips=5,
        )
        assert allowed


class TestTradingRulesClosingOnly:
    """Closing-only restriction blocks new buys."""

    def test_closing_only_blocks_buy(self, rules):
        allowed, reason = rules.check_order(
            symbol="AAPL", side="BUY", quantity=10, price=150.0,
            account_value=50_000.0,
            is_closing_only=True,
        )
        assert not allowed
        assert "closing-only" in reason.lower()

    def test_closing_only_allows_sell(self, rules):
        allowed, reason = rules.check_order(
            symbol="AAPL", side="SELL", quantity=10, price=150.0,
            account_value=50_000.0,
            is_closing_only=True,
        )
        assert allowed

    def test_no_restriction_allows_buy(self, rules):
        allowed, reason = rules.check_order(
            symbol="AAPL", side="BUY", quantity=10, price=150.0,
            account_value=50_000.0,
            is_closing_only=False,
        )
        assert allowed


class TestTradingRulesWashSale:
    """Wash sale: sell at loss + rebuy within 30 days => warning (not block)."""

    def test_wash_sale_warning_logged(self, rules, state, caplog):
        """Buying a symbol sold at a loss within 30 days triggers a warning."""
        recent = (datetime.now(timezone.utc) - timedelta(days=10)).isoformat()
        state.append_trade({
            "symbol": "AAPL", "side": "SELL", "quantity": 10, "price": 140.0,
            "realized_pnl": -200.0,
            "timestamp": recent,
        })
        import logging
        with caplog.at_level(logging.WARNING, logger="schwabagent.trading_rules"):
            allowed, reason = rules.check_order(
                symbol="AAPL", side="BUY", quantity=10, price=150.0,
                account_value=50_000.0,
            )
        # Wash sale is a warning, not a block
        assert allowed
        assert "wash sale" in caplog.text.lower() or "Wash sale" in caplog.text

    def test_no_wash_sale_for_old_trade(self, rules, state, caplog):
        """Trade older than 30 days doesn't trigger wash sale."""
        old = (datetime.now(timezone.utc) - timedelta(days=45)).isoformat()
        state.append_trade({
            "symbol": "AAPL", "side": "SELL", "quantity": 10, "price": 140.0,
            "realized_pnl": -200.0,
            "timestamp": old,
        })
        import logging
        with caplog.at_level(logging.WARNING, logger="schwabagent.trading_rules"):
            allowed, reason = rules.check_order(
                symbol="AAPL", side="BUY", quantity=10, price=150.0,
                account_value=50_000.0,
            )
        assert allowed
        assert "wash sale" not in caplog.text.lower()

    def test_no_wash_sale_for_profit(self, rules, state, caplog):
        """Selling at a profit doesn't trigger wash sale."""
        recent = (datetime.now(timezone.utc) - timedelta(days=5)).isoformat()
        state.append_trade({
            "symbol": "AAPL", "side": "SELL", "quantity": 10, "price": 160.0,
            "realized_pnl": 100.0,
            "timestamp": recent,
        })
        import logging
        with caplog.at_level(logging.WARNING, logger="schwabagent.trading_rules"):
            allowed, reason = rules.check_order(
                symbol="AAPL", side="BUY", quantity=10, price=150.0,
                account_value=50_000.0,
            )
        assert allowed
        assert "wash sale" not in caplog.text.lower()

    def test_wash_sale_only_on_buy(self, rules, state, caplog):
        """Selling doesn't trigger wash sale check."""
        recent = (datetime.now(timezone.utc) - timedelta(days=5)).isoformat()
        state.append_trade({
            "symbol": "AAPL", "side": "SELL", "quantity": 10, "price": 140.0,
            "realized_pnl": -200.0,
            "timestamp": recent,
        })
        import logging
        with caplog.at_level(logging.WARNING, logger="schwabagent.trading_rules"):
            allowed, reason = rules.check_order(
                symbol="AAPL", side="SELL", quantity=10, price=135.0,
                account_value=50_000.0,
            )
        assert allowed
        assert "wash sale" not in caplog.text.lower()


class TestTradingRulesCheckOrder:
    """check_order() integrates all rules."""

    def test_clean_order_passes(self, rules):
        allowed, reason = rules.check_order(
            symbol="AAPL", side="BUY", quantity=10, price=150.0,
            account_value=50_000.0,
        )
        assert allowed
        assert reason == ""

    def test_closing_only_checked_first(self, rules, state):
        """Closing-only is checked before PDT."""
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        state.append_trade({
            "symbol": "AAPL", "side": "SELL", "quantity": 10, "price": 150.0,
            "timestamp": f"{today}T10:00:00+00:00",
        })
        allowed, reason = rules.check_order(
            symbol="AAPL", side="BUY", quantity=10, price=150.0,
            account_value=20_000.0, account_type="MARGIN",
            round_trips=5, is_closing_only=True,
        )
        assert not allowed
        assert "closing-only" in reason.lower()

    def test_sell_always_allowed_no_pdt_restriction(self, rules):
        """SELL with closing-only=False should pass regardless of PDT params."""
        allowed, reason = rules.check_order(
            symbol="AAPL", side="SELL", quantity=10, price=150.0,
            account_value=50_000.0,
        )
        assert allowed


class TestTradingRulesEventBlackout:
    """Earnings / ex-dividend blackout gate, fed from the scraper cache files."""

    @staticmethod
    def _write_earnings_cache(tmp_dir: str, symbol: str, days_out: int):
        path = Path(tmp_dir) / "earnings_calendar.json"
        target = (datetime.now(timezone.utc).date() + timedelta(days=days_out)).isoformat()
        path.write_text(json.dumps({
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "rows": [{
                "date": target, "session": "AMC", "company": "Test Co",
                "symbol": symbol, "confirmed": True, "reported": False,
                "actual_eps": None, "consensus_eps": 1.0,
                "year_ago_eps": 0.9, "yoy_rev_pct": 5.0,
            }],
        }))

    @staticmethod
    def _write_dividend_cache(tmp_dir: str, symbol: str, days_out: int):
        path = Path(tmp_dir) / "dividend_calendar.json"
        target = (datetime.now(timezone.utc).date() + timedelta(days=days_out)).isoformat()
        path.write_text(json.dumps({
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "rows": [{
                "ex_date": target, "symbol": symbol, "company": "Test Co",
                "amount": 0.5, "annual_dividend": 2.0,
                "payment_date": target, "record_date": target,
                "announce_date": target,
            }],
        }))

    def test_missing_cache_fails_open(self, rules):
        """No cache file → order allowed (fail-open)."""
        allowed, reason = rules.check_order(
            symbol="AAPL", side="BUY", quantity=10, price=150.0,
            account_value=50_000.0,
        )
        assert allowed
        assert reason == ""

    def test_earnings_within_window_warns_by_default(self, tmp_dir, state, caplog):
        """Default mode is 'warn' — order allowed but warning logged."""
        self._write_earnings_cache(tmp_dir, "AAPL", days_out=1)
        cfg = _make_config(tmp_dir)
        rules = TradingRules(cfg, state)
        import logging
        with caplog.at_level(logging.WARNING, logger="schwabagent.trading_rules"):
            allowed, reason = rules.check_order(
                symbol="AAPL", side="BUY", quantity=10, price=150.0,
                account_value=50_000.0,
            )
        assert allowed
        assert "earnings blackout" in caplog.text.lower()

    def test_earnings_block_mode_rejects(self, tmp_dir, state):
        self._write_earnings_cache(tmp_dir, "AAPL", days_out=1)
        cfg = _make_config(tmp_dir, EARNINGS_BLACKOUT_MODE="block")
        rules = TradingRules(cfg, state)
        allowed, reason = rules.check_order(
            symbol="AAPL", side="BUY", quantity=10, price=150.0,
            account_value=50_000.0,
        )
        assert not allowed
        assert "earnings blackout" in reason.lower()

    def test_earnings_outside_window_passes(self, tmp_dir, state):
        self._write_earnings_cache(tmp_dir, "AAPL", days_out=10)
        cfg = _make_config(tmp_dir, EARNINGS_BLACKOUT_MODE="block")
        rules = TradingRules(cfg, state)
        allowed, reason = rules.check_order(
            symbol="AAPL", side="BUY", quantity=10, price=150.0,
            account_value=50_000.0,
        )
        assert allowed

    def test_earnings_blackout_does_not_block_sells(self, tmp_dir, state):
        self._write_earnings_cache(tmp_dir, "AAPL", days_out=1)
        cfg = _make_config(tmp_dir, EARNINGS_BLACKOUT_MODE="block")
        rules = TradingRules(cfg, state)
        allowed, reason = rules.check_order(
            symbol="AAPL", side="SELL", quantity=10, price=150.0,
            account_value=50_000.0,
        )
        assert allowed

    def test_dividend_blackout_opt_in(self, tmp_dir, state):
        """Dividend gate is off by default, even with fresh cache."""
        self._write_dividend_cache(tmp_dir, "KO", days_out=0)
        cfg = _make_config(tmp_dir)  # DIVIDEND_BLACKOUT_ENABLED default False
        rules = TradingRules(cfg, state)
        allowed, reason = rules.check_order(
            symbol="KO", side="BUY", quantity=10, price=60.0,
            account_value=50_000.0,
        )
        assert allowed

    def test_dividend_blackout_blocks_when_enabled(self, tmp_dir, state):
        self._write_dividend_cache(tmp_dir, "KO", days_out=0)
        cfg = _make_config(
            tmp_dir,
            DIVIDEND_BLACKOUT_ENABLED=True,
            DIVIDEND_BLACKOUT_MODE="block",
        )
        rules = TradingRules(cfg, state)
        allowed, reason = rules.check_order(
            symbol="KO", side="BUY", quantity=10, price=60.0,
            account_value=50_000.0,
        )
        assert not allowed
        assert "ex-dividend" in reason.lower()

    def test_cache_refresh_on_mtime_change(self, tmp_dir, state):
        """Rewriting the cache picks up new rows without restarting the process."""
        self._write_earnings_cache(tmp_dir, "AAPL", days_out=10)
        cfg = _make_config(tmp_dir, EARNINGS_BLACKOUT_MODE="block")
        rules = TradingRules(cfg, state)
        # Warm the cache
        rules.check_order(
            symbol="AAPL", side="BUY", quantity=10, price=150.0,
            account_value=50_000.0,
        )
        # Bump mtime by rewriting with earnings now inside the window
        time.sleep(0.01)
        self._write_earnings_cache(tmp_dir, "AAPL", days_out=1)
        allowed, reason = rules.check_order(
            symbol="AAPL", side="BUY", quantity=10, price=150.0,
            account_value=50_000.0,
        )
        assert not allowed
        assert "earnings blackout" in reason.lower()


class TestTradingRulesStatus:
    """status() returns a summary dict."""

    def test_status_margin_under_25k(self, rules):
        s = rules.status(
            account_value=20_000.0, account_type="MARGIN",
            round_trips=2, is_day_trader=False, is_closing_only=False,
        )
        assert s["pdt_applies"] is True
        assert s["round_trips"] == 2
        assert s["round_trips_limit"] == 3
        assert s["round_trips_remaining"] == 1
        assert s["account_type"] == "MARGIN"
        assert s["is_closing_only"] is False

    def test_status_margin_above_25k(self, rules):
        s = rules.status(account_value=30_000.0, account_type="MARGIN")
        assert s["pdt_applies"] is False
        assert s["round_trips_limit"] is None
        assert s["round_trips_remaining"] is None

    def test_status_cash_account(self, rules):
        s = rules.status(account_value=5_000.0, account_type="CASH")
        assert s["pdt_applies"] is False

    def test_status_closing_only_flag(self, rules):
        s = rules.status(account_value=50_000.0, is_closing_only=True)
        assert s["is_closing_only"] is True

    def test_status_has_pdt_threshold(self, rules):
        s = rules.status(account_value=10_000.0, account_type="MARGIN")
        assert s["pdt_threshold"] == 25_000.0


# ══════════════════════════════════════════════════════════════════════════════
# StateStore (persistence) tests
# ══════════════════════════════════════════════════════════════════════════════

class TestStateStoreRiskState:
    """load_risk_state / save_risk_state roundtrip."""

    def test_empty_state_on_fresh_dir(self, state):
        assert state.load_risk_state() == {}

    def test_save_and_load_roundtrip(self, state):
        data = {"killed": True, "kill_reason": "test", "peak_value": 100_000.0}
        state.save_risk_state(data)
        loaded = state.load_risk_state()
        assert loaded == data

    def test_overwrite_state(self, state):
        state.save_risk_state({"a": 1})
        state.save_risk_state({"b": 2})
        loaded = state.load_risk_state()
        assert loaded == {"b": 2}
        assert "a" not in loaded


class TestStateStoreTradeHistory:
    """append_trade + get_trade_history."""

    def test_empty_history(self, state):
        assert state.get_trade_history() == []

    def test_append_and_retrieve(self, state):
        state.append_trade({"symbol": "AAPL", "side": "BUY", "quantity": 10, "price": 150.0})
        history = state.get_trade_history()
        assert len(history) == 1
        assert history[0]["symbol"] == "AAPL"
        assert "timestamp" in history[0]

    def test_multiple_trades(self, state):
        for i in range(5):
            state.append_trade({"symbol": f"SYM{i}", "side": "BUY", "quantity": 1, "price": float(i)})
        history = state.get_trade_history()
        assert len(history) == 5

    def test_limit_parameter(self, state):
        for i in range(10):
            state.append_trade({"symbol": f"SYM{i}", "side": "BUY", "quantity": 1, "price": float(i)})
        history = state.get_trade_history(limit=3)
        assert len(history) == 3
        # Should be the last 3
        assert history[0]["symbol"] == "SYM7"

    def test_auto_timestamp(self, state):
        state.append_trade({"symbol": "AAPL", "side": "BUY"})
        history = state.get_trade_history()
        ts = history[0]["timestamp"]
        assert ts  # non-empty
        # Should be valid ISO format
        datetime.fromisoformat(ts.replace("Z", "+00:00"))

    def test_preserves_existing_timestamp(self, state):
        custom_ts = "2025-01-15T10:30:00+00:00"
        state.append_trade({"symbol": "AAPL", "side": "BUY", "timestamp": custom_ts})
        history = state.get_trade_history()
        assert history[0]["timestamp"] == custom_ts


class TestStateStoreStrategyPnl:
    """update_strategy_pnl + get_strategy_pnl."""

    def test_empty_pnl(self, state):
        assert state.get_strategy_pnl() == {}

    def test_single_win(self, state):
        state.update_strategy_pnl("momentum", 50.0, win=True)
        pnl = state.get_strategy_pnl()
        assert "momentum" in pnl
        assert pnl["momentum"]["trades"] == 1
        assert pnl["momentum"]["wins"] == 1
        assert pnl["momentum"]["losses"] == 0
        assert pnl["momentum"]["realized_pnl"] == 50.0

    def test_single_loss(self, state):
        state.update_strategy_pnl("momentum", -30.0, win=False)
        pnl = state.get_strategy_pnl()
        assert pnl["momentum"]["losses"] == 1
        assert pnl["momentum"]["realized_pnl"] == -30.0

    def test_cumulative_pnl(self, state):
        state.update_strategy_pnl("momentum", 50.0, win=True)
        state.update_strategy_pnl("momentum", -20.0, win=False)
        state.update_strategy_pnl("momentum", 30.0, win=True)
        pnl = state.get_strategy_pnl()["momentum"]
        assert pnl["trades"] == 3
        assert pnl["wins"] == 2
        assert pnl["losses"] == 1
        assert pnl["realized_pnl"] == 60.0

    def test_multiple_strategies(self, state):
        state.update_strategy_pnl("momentum", 50.0, win=True)
        state.update_strategy_pnl("etf_rotation", -10.0, win=False)
        pnl = state.get_strategy_pnl()
        assert "momentum" in pnl
        assert "etf_rotation" in pnl


class TestStateStoreAudit:
    """audit() appends to JSONL."""

    def test_audit_appends(self, state):
        state.audit("order_placed", {"symbol": "AAPL", "side": "BUY"})
        state.audit("order_filled", {"symbol": "AAPL", "fill_price": 150.0})
        log = state.get_audit_log()
        assert len(log) == 2
        assert log[0]["action"] == "order_placed"
        assert log[1]["action"] == "order_filled"
        assert "ts" in log[0]

    def test_audit_empty_initially(self, state):
        assert state.get_audit_log() == []

    def test_audit_limit(self, state):
        for i in range(10):
            state.audit("test", {"i": i})
        log = state.get_audit_log(limit=3)
        assert len(log) == 3
        assert log[0]["i"] == 7


class TestStateStoreUseTmpDir:
    """Ensure StateStore uses tmp directory, not ~/.schwagent."""

    def test_uses_provided_dir(self, tmp_path):
        s = StateStore(str(tmp_path))
        assert s.state_dir == tmp_path

    def test_does_not_write_to_home(self, tmp_path):
        s = StateStore(str(tmp_path))
        s.save_risk_state({"test": True})
        s.append_trade({"symbol": "AAPL"})
        s.audit("test", {})
        # Verify files exist under tmp_path, not under default
        assert (tmp_path / "risk_state.json").exists()
        assert (tmp_path / "trade_history.jsonl").exists()
        assert (tmp_path / "audit.jsonl").exists()


# ══════════════════════════════════════════════════════════════════════════════
# RateLimiter tests
# ══════════════════════════════════════════════════════════════════════════════

class TestRateLimiterAcquire:
    """acquire() within limit succeeds."""

    def test_acquire_succeeds(self):
        rl = RateLimiter(max_calls=10, window=60.0)
        assert rl.acquire(block=False) is True

    def test_acquire_multiple(self):
        rl = RateLimiter(max_calls=5, window=60.0)
        for _ in range(5):
            assert rl.acquire(block=False) is True

    def test_acquire_at_limit_returns_false(self):
        rl = RateLimiter(max_calls=3, window=60.0)
        for _ in range(3):
            assert rl.acquire(block=False) is True
        # 4th should fail
        assert rl.acquire(block=False) is False

    def test_acquire_at_limit_with_block_false(self):
        rl = RateLimiter(max_calls=2, window=60.0)
        rl.acquire(block=False)
        rl.acquire(block=False)
        result = rl.acquire(block=False)
        assert result is False


class TestRateLimiterUsage:
    """current_usage / utilization / stats."""

    def test_current_usage_empty(self):
        rl = RateLimiter(max_calls=10, window=60.0)
        assert rl.current_usage == 0

    def test_current_usage_after_calls(self):
        rl = RateLimiter(max_calls=10, window=60.0)
        for _ in range(5):
            rl.acquire(block=False)
        assert rl.current_usage == 5

    def test_utilization_empty(self):
        rl = RateLimiter(max_calls=10, window=60.0)
        assert rl.utilization == 0.0

    def test_utilization_half(self):
        rl = RateLimiter(max_calls=10, window=60.0)
        for _ in range(5):
            rl.acquire(block=False)
        assert abs(rl.utilization - 0.5) < 0.01

    def test_stats_structure(self):
        rl = RateLimiter(max_calls=10, window=60.0)
        rl.acquire(block=False)
        s = rl.stats()
        assert s["current"] == 1
        assert s["max"] == 10
        assert s["window_seconds"] == 60.0
        assert s["utilization_pct"] == 10.0
        assert s["total_calls"] == 1
        assert s["total_throttled"] == 0

    def test_stats_throttled_count(self):
        rl = RateLimiter(max_calls=1, window=60.0)
        rl.acquire(block=False)
        rl.acquire(block=False)  # this should be throttled
        s = rl.stats()
        assert s["total_throttled"] == 1


class TestRateLimiterWindowExpiry:
    """Old calls drop off after the window."""

    def test_calls_expire(self):
        rl = RateLimiter(max_calls=2, window=0.2)  # 200ms window
        rl.acquire(block=False)
        rl.acquire(block=False)
        assert rl.acquire(block=False) is False  # at limit
        time.sleep(0.3)  # wait for window to expire
        assert rl.current_usage == 0
        assert rl.acquire(block=False) is True  # should work now

    def test_window_sliding(self):
        rl = RateLimiter(max_calls=3, window=0.3)
        rl.acquire(block=False)
        time.sleep(0.1)
        rl.acquire(block=False)
        time.sleep(0.1)
        rl.acquire(block=False)
        # First call should expire after 0.3s total
        time.sleep(0.15)
        assert rl.acquire(block=False) is True  # first call expired


# ══════════════════════════════════════════════════════════════════════════════
# Scheduler tests
# ══════════════════════════════════════════════════════════════════════════════

class TestSchedulerJobManagement:
    """add_job / remove_job / list_jobs."""

    def test_add_job(self, scheduler):
        cb = MagicMock()
        scheduler.add_job("test_job", "every 5m", cb)
        jobs = scheduler.list_jobs()
        assert len(jobs) == 1
        assert jobs[0]["name"] == "test_job"
        assert jobs[0]["schedule"] == "every 5m"
        assert jobs[0]["enabled"] is True

    def test_add_multiple_jobs(self, scheduler):
        scheduler.add_job("job1", "every 5m", MagicMock())
        scheduler.add_job("job2", "every 10m", MagicMock())
        jobs = scheduler.list_jobs()
        assert len(jobs) == 2

    def test_remove_job(self, scheduler):
        scheduler.add_job("to_remove", "every 5m", MagicMock())
        scheduler.remove_job("to_remove")
        assert len(scheduler.list_jobs()) == 0

    def test_remove_nonexistent_job(self, scheduler):
        # Should not raise
        scheduler.remove_job("nonexistent")

    def test_list_jobs_empty(self, scheduler):
        assert scheduler.list_jobs() == []

    def test_update_existing_job(self, scheduler):
        cb1 = MagicMock()
        cb2 = MagicMock()
        scheduler.add_job("job", "every 5m", cb1)
        scheduler.add_job("job", "every 10m", cb2)
        jobs = scheduler.list_jobs()
        assert len(jobs) == 1
        assert jobs[0]["schedule"] == "every 10m"


class TestSchedulerEnableDisable:
    """enable_job / disable_job."""

    def test_disable_job(self, scheduler):
        scheduler.add_job("job", "every 5m", MagicMock())
        scheduler.disable_job("job")
        jobs = scheduler.list_jobs()
        assert jobs[0]["enabled"] is False

    def test_enable_job(self, scheduler):
        scheduler.add_job("job", "every 5m", MagicMock(), enabled=False)
        scheduler.enable_job("job")
        jobs = scheduler.list_jobs()
        assert jobs[0]["enabled"] is True

    def test_disable_nonexistent(self, scheduler):
        # Should not raise
        scheduler.disable_job("nonexistent")

    def test_enable_nonexistent(self, scheduler):
        scheduler.enable_job("nonexistent")


class TestSchedulerNextRun:
    """Job next_run computation from schedule expressions."""

    def test_interval_minutes(self, scheduler):
        scheduler.add_job("job", "every 5m", MagicMock())
        jobs = scheduler.list_jobs()
        next_run = jobs[0]["next_run"]
        assert next_run  # non-empty
        dt = datetime.fromisoformat(next_run)
        now = datetime.now(timezone.utc)
        # Should be ~5 minutes from now
        delta = dt - now
        assert timedelta(minutes=4) < delta < timedelta(minutes=6)

    def test_interval_hours(self, scheduler):
        scheduler.add_job("job", "every 2h", MagicMock())
        jobs = scheduler.list_jobs()
        next_run = jobs[0]["next_run"]
        dt = datetime.fromisoformat(next_run)
        now = datetime.now(timezone.utc)
        delta = dt - now
        assert timedelta(hours=1, minutes=50) < delta < timedelta(hours=2, minutes=10)

    def test_interval_seconds(self, scheduler):
        scheduler.add_job("job", "every 30s", MagicMock())
        jobs = scheduler.list_jobs()
        next_run = jobs[0]["next_run"]
        dt = datetime.fromisoformat(next_run)
        now = datetime.now(timezone.utc)
        delta = dt - now
        assert timedelta(seconds=25) < delta < timedelta(seconds=35)

    def test_in_syntax(self, scheduler):
        scheduler.add_job("job", "in 10m", MagicMock())
        jobs = scheduler.list_jobs()
        next_run = jobs[0]["next_run"]
        dt = datetime.fromisoformat(next_run)
        now = datetime.now(timezone.utc)
        delta = dt - now
        assert timedelta(minutes=9) < delta < timedelta(minutes=11)


class TestSchedulerPersistence:
    """Jobs persist across scheduler instances."""

    def test_jobs_saved_to_disk(self, config):
        sched = Scheduler(config)
        sched.add_job("persist_test", "every 5m", MagicMock())
        path = Path(config.STATE_DIR).expanduser() / "cron.json"
        assert path.exists()
        data = json.loads(path.read_text())
        assert "persist_test" in data

    def test_jobs_restored_from_disk(self, config):
        sched1 = Scheduler(config)
        sched1.add_job("persist_test", "every 5m", MagicMock())

        # New scheduler instance reads from same state dir
        sched2 = Scheduler(config)
        jobs = sched2.list_jobs()
        assert len(jobs) == 1
        assert jobs[0]["name"] == "persist_test"
        # Callback won't be restored (not persisted), but metadata is


class TestSchedulerJobFields:
    """list_jobs returns proper field structure."""

    def test_job_dict_fields(self, scheduler):
        scheduler.add_job("job", "every 5m", MagicMock(), one_shot=True)
        job = scheduler.list_jobs()[0]
        assert "name" in job
        assert "schedule" in job
        assert "enabled" in job
        assert "last_run" in job
        assert "next_run" in job
        assert "run_count" in job
        assert "last_error" in job
        assert "one_shot" in job
        assert job["one_shot"] is True
        assert job["run_count"] == 0


# ══════════════════════════════════════════════════════════════════════════════
# OrderTracker tests
# ══════════════════════════════════════════════════════════════════════════════

class TestOrderTrackerTrack:
    """track() adds pending order."""

    def test_track_adds_order(self, tracker):
        tracker.track("ORD001", "AAPL", "BUY", 10, 150.0, "hash123", "momentum")
        assert tracker.pending_count == 1

    def test_track_multiple(self, tracker):
        tracker.track("ORD001", "AAPL", "BUY", 10, 150.0, "hash123")
        tracker.track("ORD002", "MSFT", "BUY", 5, 300.0, "hash123")
        assert tracker.pending_count == 2

    def test_track_status(self, tracker):
        tracker.track("ORD001", "AAPL", "BUY", 10, 150.0, "hash123", "momentum")
        status = tracker.status()
        assert status["pending"] == 1
        assert len(status["orders"]) == 1
        order = status["orders"][0]
        assert order["id"] == "ORD001"
        assert order["symbol"] == "AAPL"
        assert order["side"] == "BUY"
        assert order["expected"] == 150.0
        assert order["status"] == "PENDING"

    def test_track_normalizes_side(self, tracker):
        tracker.track("ORD001", "AAPL", "buy", 10, 150.0, "hash123")
        status = tracker.status()
        assert status["orders"][0]["side"] == "BUY"


class TestOrderTrackerCallbacks:
    """on_fill callback registration."""

    def test_register_callback(self, tracker):
        cb = MagicMock()
        tracker.on_fill(cb)
        assert cb in tracker._callbacks

    def test_multiple_callbacks(self, tracker):
        cb1 = MagicMock()
        cb2 = MagicMock()
        tracker.on_fill(cb1)
        tracker.on_fill(cb2)
        assert len(tracker._callbacks) == 2

    def test_callback_invoked_on_stream_fill(self, tracker):
        cb = MagicMock()
        tracker.on_fill(cb)
        tracker.track("ORD001", "AAPL", "BUY", 10, 150.0, "hash123")

        # Simulate a streaming fill event
        fill_event = MagicMock()
        fill_event.data = {"orderId": "ORD001", "ExecutionPrice": 150.5, "Quantity": 10}
        tracker.handle_stream_fill(fill_event)

        cb.assert_called_once()
        filled_order = cb.call_args[0][0]
        assert filled_order.status == "FILLED"
        assert filled_order.fill_price == 150.5


class TestOrderTrackerPersistence:
    """_save_pending / _load_pending roundtrip."""

    def test_save_and_load(self, config, state, tmp_dir):
        tracker1 = OrderTracker(config, state)
        tracker1.track("ORD001", "AAPL", "BUY", 10, 150.0, "hash123", "momentum")
        tracker1.track("ORD002", "MSFT", "SELL", 5, 300.0, "hash123", "etf_rotation")

        # Create new tracker from same state dir
        tracker2 = OrderTracker(config, state)
        assert tracker2.pending_count == 2
        status = tracker2.status()
        ids = {o["id"] for o in status["orders"]}
        assert "ORD001" in ids
        assert "ORD002" in ids

    def test_pending_file_exists(self, config, state, tmp_dir):
        tracker = OrderTracker(config, state)
        tracker.track("ORD001", "AAPL", "BUY", 10, 150.0, "hash123")
        path = Path(tmp_dir) / "pending_orders.json"
        assert path.exists()
        data = json.loads(path.read_text())
        assert "ORD001" in data

    def test_empty_on_fresh_start(self, config, state):
        tracker = OrderTracker(config, state)
        assert tracker.pending_count == 0

    def test_resolved_orders_removed_from_pending(self, config, state, tracker):
        tracker.track("ORD001", "AAPL", "BUY", 10, 150.0, "hash123")
        # Simulate fill via stream
        fill_event = MagicMock()
        fill_event.data = {"orderId": "ORD001", "ExecutionPrice": 150.0, "Quantity": 10}
        tracker.handle_stream_fill(fill_event)
        assert tracker.pending_count == 0

        # Verify persistence reflects removal
        tracker2 = OrderTracker(config, state)
        assert tracker2.pending_count == 0


# ── _compute_limit_price ─────────────────────────────────────────────────────


class _FakeQuote:
    """Minimal stand-in for schwab_client.Quote for unit tests."""

    def __init__(self, bid: float = 0.0, ask: float = 0.0, last: float = 0.0):
        self.bid = bid
        self.ask = ask
        self.last = last


class TestComputeLimitPrice:
    """Unit tests for the buffered-limit math — no Schwab client needed."""

    def test_buy_uses_ask_plus_buffer(self):
        from schwabagent.schwab_client import _compute_limit_price
        q = _FakeQuote(bid=99.98, ask=100.00, last=99.99)
        # 25 bps of $100 = $0.25, so limit = $100.25
        assert _compute_limit_price("BUY", q, 25.0) == 100.25

    def test_sell_uses_bid_minus_buffer(self):
        from schwabagent.schwab_client import _compute_limit_price
        q = _FakeQuote(bid=100.00, ask=100.02, last=100.01)
        # 25 bps of $100 = $0.25, so limit = $99.75
        assert _compute_limit_price("SELL", q, 25.0) == 99.75

    def test_zero_buffer_yields_exact_ask_or_bid(self):
        from schwabagent.schwab_client import _compute_limit_price
        q = _FakeQuote(bid=50.00, ask=50.10, last=50.05)
        assert _compute_limit_price("BUY", q, 0.0) == 50.10
        assert _compute_limit_price("SELL", q, 0.0) == 50.00

    def test_falls_back_to_last_when_ask_missing(self):
        from schwabagent.schwab_client import _compute_limit_price
        q = _FakeQuote(bid=0.0, ask=0.0, last=200.00)
        assert _compute_limit_price("BUY", q, 25.0) == 200.50
        assert _compute_limit_price("SELL", q, 25.0) == 199.50

    def test_returns_none_when_no_usable_price(self):
        from schwabagent.schwab_client import _compute_limit_price
        q = _FakeQuote(bid=0.0, ask=0.0, last=0.0)
        assert _compute_limit_price("BUY", q, 25.0) is None
        assert _compute_limit_price("SELL", q, 25.0) is None

    def test_rounds_to_penny(self):
        from schwabagent.schwab_client import _compute_limit_price
        # Ask 86.71 * (1 + 25/10000) = 86.92675 → 86.93
        q = _FakeQuote(bid=86.67, ask=86.71, last=86.69)
        result = _compute_limit_price("BUY", q, 25.0)
        assert result == 86.93

    def test_wider_buffer_is_more_aggressive(self):
        from schwabagent.schwab_client import _compute_limit_price
        q = _FakeQuote(bid=100.00, ask=100.00, last=100.00)
        tight = _compute_limit_price("BUY", q, 10.0)   # 10 bps
        wide = _compute_limit_price("BUY", q, 100.0)   # 100 bps
        assert wide > tight
        assert tight == 100.10
        assert wide == 101.00

    def test_unknown_side_returns_none(self):
        from schwabagent.schwab_client import _compute_limit_price
        q = _FakeQuote(bid=100, ask=100, last=100)
        assert _compute_limit_price("HOLD", q, 25.0) is None

    def test_case_insensitive_side(self):
        from schwabagent.schwab_client import _compute_limit_price
        q = _FakeQuote(bid=100.00, ask=100.10, last=100.05)
        assert _compute_limit_price("buy", q, 25.0) == _compute_limit_price("BUY", q, 25.0)
        assert _compute_limit_price("sell", q, 25.0) == _compute_limit_price("SELL", q, 25.0)

    def test_handles_none_attributes_gracefully(self):
        """Quote dataclass fields can be None if the provider returns nulls."""
        from schwabagent.schwab_client import _compute_limit_price
        q = _FakeQuote()
        q.bid = None
        q.ask = None
        q.last = 80.00
        # 80 * (1 + 50/10000) = 80.40 exactly, no rounding ambiguity
        assert _compute_limit_price("BUY", q, 50.0) == 80.40


class TestConfigOrderDefaults:
    """The new config fields must default correctly."""

    def test_default_order_type_is_limit(self):
        cfg = Config(SCHWAB_API_KEY="t", SCHWAB_APP_SECRET="t")
        assert cfg.ORDER_TYPE == "LIMIT"

    def test_default_buffer_is_25_bps(self):
        cfg = Config(SCHWAB_API_KEY="t", SCHWAB_APP_SECRET="t")
        assert cfg.LIMIT_PRICE_BUFFER_BPS == 25.0

    def test_default_duration_is_day(self):
        cfg = Config(SCHWAB_API_KEY="t", SCHWAB_APP_SECRET="t")
        assert cfg.ORDER_DURATION == "DAY"

    def test_default_session_is_normal(self):
        cfg = Config(SCHWAB_API_KEY="t", SCHWAB_APP_SECRET="t")
        assert cfg.ORDER_SESSION == "NORMAL"

    def test_env_override_order_type(self):
        cfg = Config(SCHWAB_API_KEY="t", SCHWAB_APP_SECRET="t", ORDER_TYPE="MARKET")
        assert cfg.ORDER_TYPE == "MARKET"

    def test_env_override_buffer(self):
        cfg = Config(SCHWAB_API_KEY="t", SCHWAB_APP_SECRET="t", LIMIT_PRICE_BUFFER_BPS=50.0)
        assert cfg.LIMIT_PRICE_BUFFER_BPS == 50.0

    def test_env_override_duration(self):
        cfg = Config(SCHWAB_API_KEY="t", SCHWAB_APP_SECRET="t", ORDER_DURATION="GOOD_TILL_CANCEL")
        assert cfg.ORDER_DURATION == "GOOD_TILL_CANCEL"

    def test_env_override_session(self):
        cfg = Config(SCHWAB_API_KEY="t", SCHWAB_APP_SECRET="t", ORDER_SESSION="SEAMLESS")
        assert cfg.ORDER_SESSION == "SEAMLESS"


class TestOrderBuilderDurationSession:
    """Verify schwab-py builder produces correct payloads for each combo.

    These are pure-stdlib checks — they exercise the builder directly
    instead of hitting Schwab, so they catch enum path changes in
    schwab-py without requiring credentials.
    """

    def test_day_normal_default(self):
        import schwab.orders.equities as eq
        order = eq.equity_buy_limit("TLT", 1, "85.00").build()
        assert order["duration"] == "DAY"
        assert order["session"] == "NORMAL"

    def test_gtc_seamless(self):
        import schwab.orders.equities as eq
        from schwab.orders.common import Duration, Session
        order = (
            eq.equity_buy_limit("TLT", 1, "85.00")
            .set_duration(Duration.GOOD_TILL_CANCEL)
            .set_session(Session.SEAMLESS)
            .build()
        )
        assert order["duration"] == "GOOD_TILL_CANCEL"
        assert order["session"] == "SEAMLESS"

    def test_market_with_gtc_seamless(self):
        import schwab.orders.equities as eq
        from schwab.orders.common import Duration, Session
        order = (
            eq.equity_buy_market("TLT", 1)
            .set_duration(Duration.GOOD_TILL_CANCEL)
            .set_session(Session.SEAMLESS)
            .build()
        )
        assert order["orderType"] == "MARKET"
        assert order["duration"] == "GOOD_TILL_CANCEL"
        assert order["session"] == "SEAMLESS"

    def test_enum_name_lookup_matches_string(self):
        """The wrapper looks up Duration[string] / Session[string] — prove
        that every name we accept at the config layer is a valid enum key."""
        from schwab.orders.common import Duration, Session
        for name in ("DAY", "GOOD_TILL_CANCEL"):
            assert Duration[name].name == name
        for name in ("NORMAL", "SEAMLESS"):
            assert Session[name].name == name
