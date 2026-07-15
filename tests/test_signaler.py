"""Tests for the signal-change notifier."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from schwabagent.config import Config
from schwabagent.persistence import StateStore
from schwabagent.signaler import SignalNotifier
from schwabagent.strategies.base import Signal


@pytest.fixture
def store(tmp_path):
    return StateStore(state_dir=str(tmp_path))


@pytest.fixture
def config(tmp_path):
    return Config(
        _env_file=None,
        STATE_DIR=str(tmp_path),
        SIGNALER_ENABLED=True,
        SIGNALER_STRATEGIES="trend_following",
        SIGNALER_MIN_ABS_SCORE=1.0,
        SIGNALER_COOLDOWN_MINUTES=240,
    )


@pytest.fixture
def sent():
    return []


@pytest.fixture
def notifier(config, store, sent):
    return SignalNotifier(config, store, send=sent.append)


def opp(symbol="AAPL", signal="BUY", score=1.0, price=100.0, reason="test"):
    return {"symbol": symbol, "signal": signal, "score": score,
            "price": price, "reason": reason}


class TestSignalNotifier:
    def test_first_hold_is_silent(self, notifier, sent):
        events = notifier.process("trend_following", [opp(signal="HOLD", score=0.0)])
        assert events == []
        assert sent == []

    def test_first_actionable_alerts(self, notifier, sent):
        events = notifier.process("trend_following", [opp(signal="BUY", score=1.0)])
        assert len(events) == 1
        assert events[0]["prev_signal"] == "HOLD"
        assert events[0]["signal"] == "BUY"
        assert len(sent) == 1
        assert "AAPL" in sent[0]

    def test_unchanged_signal_is_silent(self, notifier, sent):
        notifier.process("trend_following", [opp(signal="BUY")])
        events = notifier.process("trend_following", [opp(signal="BUY")])
        assert events == []
        assert len(sent) == 1

    def test_signal_enum_normalized(self, notifier, store):
        notifier.process("trend_following", [opp(signal=Signal.BUY)])
        state = store.get_signal_state()
        assert state["trend_following:AAPL"]["signal"] == "BUY"

    def test_reversal_alerts(self, notifier, store, sent):
        notifier.process("trend_following", [opp(signal="BUY", score=1.0)])
        # age the alert past the cooldown
        state = store.get_signal_state()
        old = (datetime.now(timezone.utc) - timedelta(hours=5)).isoformat()
        state["trend_following:AAPL"]["alerted_ts"] = old
        store.save_signal_state(state)

        events = notifier.process("trend_following", [opp(signal="SELL", score=-1.0)])
        assert len(events) == 1
        assert events[0]["prev_signal"] == "BUY"
        assert events[0]["signal"] == "SELL"

    def test_exit_to_hold_alerts(self, notifier, store):
        notifier.process("trend_following", [opp(signal="BUY", score=1.0)])
        state = store.get_signal_state()
        old = (datetime.now(timezone.utc) - timedelta(hours=5)).isoformat()
        state["trend_following:AAPL"]["alerted_ts"] = old
        store.save_signal_state(state)

        events = notifier.process("trend_following", [opp(signal="HOLD", score=0.0)])
        assert len(events) == 1
        assert events[0]["signal"] == "HOLD"

    def test_below_min_score_entry_is_silent(self, notifier, store, sent):
        events = notifier.process("trend_following", [opp(signal="BUY", score=0.4)])
        assert events == []
        assert sent == []
        # state must stay unseeded so a later real signal still alerts
        assert store.get_signal_state() == {}

    def test_cooldown_suppresses_flap(self, notifier, sent):
        notifier.process("trend_following", [opp(signal="BUY", score=1.0)])
        # flap to HOLD and back within the cooldown window — both silent
        assert notifier.process("trend_following", [opp(signal="HOLD", score=0.0)]) == []
        assert notifier.process("trend_following", [opp(signal="BUY", score=1.0)]) == []
        assert len(sent) == 1

    def test_unwatched_strategy_ignored(self, notifier, sent):
        events = notifier.process("momentum", [opp(signal="STRONG_BUY", score=2.0)])
        assert events == []
        assert sent == []

    def test_all_watches_every_strategy(self, config, store, sent):
        config.SIGNALER_STRATEGIES = "all"
        notifier = SignalNotifier(config, store, send=sent.append)
        for strat in ("momentum", "etf_rotation", "conviction_hold"):
            events = notifier.process(strat, [opp(signal="BUY", score=1.0)])
            assert len(events) == 1, strat
        # same symbol tracked independently per strategy
        assert len(store.get_signal_state()) == 3
        assert len(sent) == 3

    def test_disabled_is_silent(self, config, store, sent):
        config.SIGNALER_ENABLED = False
        notifier = SignalNotifier(config, store, send=sent.append)
        assert notifier.process("trend_following", [opp(signal="BUY")]) == []
        assert sent == []

    def test_events_persisted(self, notifier, store):
        notifier.process("trend_following", [opp(signal="BUY", score=1.0)])
        events = store.get_signal_events()
        assert len(events) == 1
        assert events[0]["symbol"] == "AAPL"
        assert events[0]["signal"] == "BUY"

    def test_multiple_symbols_one_scan(self, notifier, sent):
        events = notifier.process("trend_following", [
            opp(symbol="AAPL", signal="BUY", score=1.0),
            opp(symbol="MSFT", signal="HOLD", score=0.0),
            opp(symbol="NVDA", signal="STRONG_SELL", score=-2.0),
        ])
        assert {e["symbol"] for e in events} == {"AAPL", "NVDA"}
        assert len(sent) == 2

    def test_send_failure_does_not_block_other_alerts(self, config, store):
        calls = []

        def flaky(text):
            calls.append(text)
            raise RuntimeError("telegram down")

        notifier = SignalNotifier(config, store, send=flaky)
        events = notifier.process("trend_following", [
            opp(symbol="AAPL", signal="BUY", score=1.0),
            opp(symbol="NVDA", signal="SELL", score=-1.0),
        ])
        assert len(events) == 2
        assert len(calls) == 2
        # state still saved despite send failures
        assert len(store.get_signal_state()) == 2

    def test_format_event(self):
        text = SignalNotifier.format_event({
            "strategy": "trend_following",
            "symbol": "AAPL",
            "prev_signal": "HOLD",
            "signal": "BUY",
            "score": 1.0,
            "price": 234.5,
            "reason": "EMA20>EMA50 ADX=32",
            "ts": "2026-07-14T00:00:00+00:00",
        })
        assert "AAPL" in text
        assert "HOLD → BUY" in text
        assert "$234.50" in text
        assert "ADX=32" in text
