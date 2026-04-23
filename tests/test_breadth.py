"""Tests for the breadth module — pure signal classification + state update.

These tests don't touch the Schwab client, the strategy class, or the
filesystem. They exercise classify_tick_signal, update_daily_state,
is_within_cooldown, and the TickPair parser against hand-crafted
scenarios so the signal math is pinned to expected behavior.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from schwabagent.breadth import (
    TickDailyState,
    TickPair,
    TickSignalResult,
    classify_tick_signal,
    is_within_cooldown,
    update_daily_state,
)
from schwabagent.strategies.base import Signal


# ── TickPair parser ──────────────────────────────────────────────────────────


class TestTickPairParse:
    def test_parses_basic_spec(self):
        pair = TickPair.parse("SPY:$TICK")
        assert pair.underlying == "SPY"
        assert pair.tick_symbol == "$TICK"
        assert pair.trade_enabled is True

    def test_uppercases_underlying_but_preserves_tick_symbol_case(self):
        pair = TickPair.parse("spy:$TICK")
        assert pair.underlying == "SPY"
        assert pair.tick_symbol == "$TICK"

    def test_strips_whitespace(self):
        pair = TickPair.parse("  SPY  :  $TICK  ")
        assert pair.underlying == "SPY"
        assert pair.tick_symbol == "$TICK"

    def test_trade_enabled_flag(self):
        pair = TickPair.parse("QQQ:$TICKQ", trade_enabled=False)
        assert pair.trade_enabled is False

    @pytest.mark.parametrize("bad", ["", "SPY", ":TICK", "SPY:", "::", ":"])
    def test_rejects_bad_specs(self, bad):
        with pytest.raises(ValueError):
            TickPair.parse(bad)


# ── classify_tick_signal ─────────────────────────────────────────────────────


def _state(
    tick_high: float = 600.0,
    tick_low: float = -500.0,
    u_high: float = 500.0,
    u_low: float = 490.0,
) -> TickDailyState:
    """Build a pre-populated state for signal tests."""
    return TickDailyState(
        session_date="2026-04-13",
        tick_high=tick_high,
        tick_low=tick_low,
        underlying_high=u_high,
        underlying_low=u_low,
    )


class TestClassifyTickSignal:
    def test_uninitialized_state_returns_hold(self):
        state = TickDailyState()  # session_date="" → not initialized
        result = classify_tick_signal(
            prior_state=state,
            current_tick=1000.0,
            current_underlying=500.0,
            extreme_threshold=500.0,
            confirmation_bps=5.0,
        )
        assert result.kind == "no_extreme"
        assert result.signal == Signal.HOLD

    def test_no_new_extreme_returns_hold(self):
        state = _state(tick_high=800.0, tick_low=-700.0, u_high=500.0, u_low=490.0)
        result = classify_tick_signal(
            prior_state=state,
            current_tick=400.0,   # between prior high and low, not an extreme
            current_underlying=495.0,
            extreme_threshold=500.0,
            confirmation_bps=5.0,
        )
        assert result.kind == "no_extreme"
        assert result.signal == Signal.HOLD

    def test_new_tick_high_confirmed_by_underlying_holds(self):
        """Classic confirmation: TICK prints new THOD and underlying is at
        its own new daily high. No signal — trend is intact."""
        state = _state(tick_high=700.0, u_high=500.0)
        result = classify_tick_signal(
            prior_state=state,
            current_tick=900.0,
            current_underlying=501.0,   # above prior underlying_high
            extreme_threshold=500.0,
            confirmation_bps=5.0,
        )
        assert result.kind == "confirmed_extreme"
        assert result.signal == Signal.HOLD

    def test_new_tick_high_unconfirmed_triggers_sell(self):
        """Classic bearish non-confirmation: TICK prints new THOD but the
        underlying is well below its daily high. Signal SELL."""
        state = _state(tick_high=700.0, u_high=500.0)
        result = classify_tick_signal(
            prior_state=state,
            current_tick=1100.0,
            current_underlying=497.0,   # 60 bps below u_high = not confirming
            extreme_threshold=500.0,
            confirmation_bps=5.0,
        )
        assert result.kind == "bearish_nonconfirmation"
        assert result.signal == Signal.SELL
        assert result.score > 0
        assert result.new_tick_extreme == 1100.0
        assert result.prior_tick_extreme == 700.0
        assert result.underlying_gap_bps is not None
        assert result.underlying_gap_bps > 5.0

    def test_new_tick_low_confirmed_by_underlying_holds(self):
        state = _state(tick_low=-700.0, u_low=490.0)
        result = classify_tick_signal(
            prior_state=state,
            current_tick=-900.0,
            current_underlying=489.0,   # below prior underlying_low
            extreme_threshold=500.0,
            confirmation_bps=5.0,
        )
        assert result.kind == "confirmed_extreme"
        assert result.signal == Signal.HOLD

    def test_new_tick_low_unconfirmed_triggers_buy(self):
        """Classic bullish non-confirmation: TICK prints new TLOD but the
        underlying is holding well above its daily low. Signal BUY."""
        state = _state(tick_low=-700.0, u_low=490.0)
        result = classify_tick_signal(
            prior_state=state,
            current_tick=-1100.0,
            current_underlying=493.0,   # 60 bps above u_low = not confirming
            extreme_threshold=500.0,
            confirmation_bps=5.0,
        )
        assert result.kind == "bullish_nonconfirmation"
        assert result.signal == Signal.BUY
        assert result.score > 0

    def test_tick_extreme_below_threshold_is_ignored(self):
        """A new tick 'high' that's below the extreme threshold doesn't count."""
        state = _state(tick_high=100.0, u_high=500.0)
        result = classify_tick_signal(
            prior_state=state,
            current_tick=200.0,   # > prior but < 500 threshold
            current_underlying=495.0,
            extreme_threshold=500.0,
            confirmation_bps=5.0,
        )
        assert result.kind == "no_extreme"
        assert result.signal == Signal.HOLD

    def test_threshold_exactly_matches(self):
        """TICK exactly at threshold still counts as an extreme."""
        state = _state(tick_high=400.0, u_high=500.0)
        result = classify_tick_signal(
            prior_state=state,
            current_tick=500.0,
            current_underlying=495.0,
            extreme_threshold=500.0,
            confirmation_bps=5.0,
        )
        # This IS a new extreme, and underlying is 100 bps below confirmation → bearish
        assert result.kind == "bearish_nonconfirmation"
        assert result.signal == Signal.SELL

    def test_tighter_confirmation_window_flips_to_unconfirmed(self):
        """Same underlying gap, different confirmation windows change the verdict."""
        state = _state(tick_high=700.0, u_high=500.0)
        loose = classify_tick_signal(
            prior_state=state,
            current_tick=900.0,
            current_underlying=499.90,   # 2 bps below daily high
            extreme_threshold=500.0,
            confirmation_bps=5.0,         # 5 bps window → confirmed
        )
        strict = classify_tick_signal(
            prior_state=state,
            current_tick=900.0,
            current_underlying=499.90,
            extreme_threshold=500.0,
            confirmation_bps=1.0,         # 1 bps window → unconfirmed
        )
        assert loose.kind == "confirmed_extreme"
        assert strict.kind == "bearish_nonconfirmation"

    def test_score_scales_with_gap(self):
        """Larger underlying gap → higher divergence score."""
        state = _state(tick_high=700.0, u_high=500.0)
        narrow = classify_tick_signal(
            prior_state=state,
            current_tick=900.0,
            current_underlying=499.0,   # 20 bps below
            extreme_threshold=500.0,
            confirmation_bps=5.0,
        )
        wide = classify_tick_signal(
            prior_state=state,
            current_tick=900.0,
            current_underlying=495.0,   # 100 bps below
            extreme_threshold=500.0,
            confirmation_bps=5.0,
        )
        assert narrow.kind == "bearish_nonconfirmation"
        assert wide.kind == "bearish_nonconfirmation"
        assert wide.score > narrow.score


# ── update_daily_state ───────────────────────────────────────────────────────


class TestUpdateDailyState:
    def test_new_session_resets_state(self):
        old = _state()
        new = update_daily_state(old, "2026-04-14", current_tick=300.0, current_underlying=500.0)
        assert new.session_date == "2026-04-14"
        assert new.tick_high == 300.0
        assert new.tick_low == 300.0
        assert new.underlying_high == 500.0
        assert new.underlying_low == 500.0
        assert new.last_entry_ts is None

    def test_same_session_extends_running_extremes(self):
        old = _state(tick_high=600.0, tick_low=-500.0, u_high=500.0, u_low=490.0)
        new = update_daily_state(old, "2026-04-13", current_tick=800.0, current_underlying=502.0)
        assert new.tick_high == 800.0
        assert new.tick_low == -500.0    # still the prior low
        assert new.underlying_high == 502.0
        assert new.underlying_low == 490.0

    def test_same_session_no_new_extreme(self):
        old = _state(tick_high=600.0, tick_low=-500.0, u_high=500.0, u_low=490.0)
        new = update_daily_state(old, "2026-04-13", current_tick=400.0, current_underlying=495.0)
        # Nothing changes — current values are inside the running range
        assert new.tick_high == 600.0
        assert new.tick_low == -500.0
        assert new.underlying_high == 500.0
        assert new.underlying_low == 490.0

    def test_preserves_last_entry_ts_on_same_session(self):
        old = TickDailyState(
            session_date="2026-04-13",
            tick_high=600.0, tick_low=-500.0,
            underlying_high=500.0, underlying_low=490.0,
            last_entry_ts="2026-04-13T14:23:00-04:00",
        )
        new = update_daily_state(old, "2026-04-13", current_tick=700.0, current_underlying=501.0)
        assert new.last_entry_ts == "2026-04-13T14:23:00-04:00"

    def test_clears_last_entry_ts_on_new_session(self):
        old = TickDailyState(
            session_date="2026-04-13",
            tick_high=600.0, tick_low=-500.0,
            underlying_high=500.0, underlying_low=490.0,
            last_entry_ts="2026-04-13T14:23:00-04:00",
        )
        new = update_daily_state(old, "2026-04-14", current_tick=100.0, current_underlying=498.0)
        assert new.last_entry_ts is None


# ── is_within_cooldown ──────────────────────────────────────────────────────


class TestIsWithinCooldown:
    def test_no_prior_entry_never_in_cooldown(self):
        state = TickDailyState(session_date="2026-04-13")
        assert is_within_cooldown(state, datetime.now(timezone.utc), cooldown_minutes=10) is False

    def test_recent_entry_is_in_cooldown(self):
        now = datetime(2026, 4, 13, 14, 30, 0, tzinfo=timezone.utc)
        state = TickDailyState(
            session_date="2026-04-13",
            last_entry_ts=(now - timedelta(minutes=3)).isoformat(),
        )
        assert is_within_cooldown(state, now, cooldown_minutes=10) is True

    def test_old_entry_is_not_in_cooldown(self):
        now = datetime(2026, 4, 13, 14, 30, 0, tzinfo=timezone.utc)
        state = TickDailyState(
            session_date="2026-04-13",
            last_entry_ts=(now - timedelta(minutes=15)).isoformat(),
        )
        assert is_within_cooldown(state, now, cooldown_minutes=10) is False

    def test_boundary_exactly_at_cooldown(self):
        now = datetime(2026, 4, 13, 14, 30, 0, tzinfo=timezone.utc)
        state = TickDailyState(
            session_date="2026-04-13",
            last_entry_ts=(now - timedelta(minutes=10)).isoformat(),
        )
        # exactly 10 min elapsed → NOT within cooldown (strict <)
        assert is_within_cooldown(state, now, cooldown_minutes=10) is False

    def test_bad_timestamp_returns_false(self):
        now = datetime(2026, 4, 13, 14, 30, 0, tzinfo=timezone.utc)
        state = TickDailyState(
            session_date="2026-04-13",
            last_entry_ts="not-a-timestamp",
        )
        assert is_within_cooldown(state, now, cooldown_minutes=10) is False


# ── TickDailyState serialization ────────────────────────────────────────────


class TestTickDailyStateSerialization:
    def test_round_trip_preserves_values(self):
        original = TickDailyState(
            session_date="2026-04-13",
            tick_high=900.0, tick_low=-600.0,
            underlying_high=501.25, underlying_low=498.80,
            last_entry_ts="2026-04-13T14:23:00-04:00",
        )
        restored = TickDailyState.from_dict(original.to_dict())
        assert restored == original

    def test_from_dict_handles_none_values(self):
        d = {
            "session_date": "",
            "tick_high": None, "tick_low": None,
            "underlying_high": None, "underlying_low": None,
            "last_entry_ts": None,
        }
        state = TickDailyState.from_dict(d)
        assert not state.is_initialized()
        assert state.tick_high == float("-inf")
        assert state.tick_low == float("inf")


# ── Scenario: a full session flow ───────────────────────────────────────────


class TestFullSessionFlow:
    """Walk a fake session: state starts empty, TICK makes multiple extremes,
    signals should fire at the right moments."""

    def test_first_tick_initializes_then_later_divergence_fires(self):
        empty = TickDailyState()
        # Session opens, first reading
        state = update_daily_state(empty, "2026-04-13", current_tick=200.0, current_underlying=500.0)
        assert state.is_initialized()

        # TICK climbs to 800 with underlying also at new high — confirmed
        state = update_daily_state(state, "2026-04-13", current_tick=800.0, current_underlying=502.0)
        assert state.tick_high == 800.0
        assert state.underlying_high == 502.0

        # Later: TICK prints NEW HIGH at 1100 but underlying is only at 501.50
        # (50 bps below its daily high of 502.0)
        result = classify_tick_signal(
            prior_state=state,
            current_tick=1100.0,
            current_underlying=501.50,
            extreme_threshold=500.0,
            confirmation_bps=5.0,
        )
        assert result.kind == "bearish_nonconfirmation"
        assert result.signal == Signal.SELL
