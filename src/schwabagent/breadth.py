"""Intraday breadth-divergence primitives for $TICK-style strategies.

Pure functions and dataclasses — no Schwab client, no config, no side
effects. The strategy (strategies/tick_breadth.py) composes these with
live quote polling, state persistence, and order routing.

Core thesis: when $TICK (NYSE tick index) prints a new daily extreme
without confirmation from the underlying index, internal momentum is
diverging from price, which historically precedes a short-term reversal.

    new THOD + price NOT at new high   → bearish non-confirmation → SELL
    new TLOD + price NOT at new low    → bullish non-confirmation → BUY
    confirmed extreme (both new)       → trend intact, no entry
    no new extreme                     → no signal

The strategy also supports "observe" mode on a pair — signals are
computed and logged but no orders are placed. Useful for evaluating
new pairs before giving them real capital.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal

from schwabagent.strategies.base import Signal


TickSignalKind = Literal[
    "bearish_nonconfirmation",   # new TICK high, underlying doesn't confirm
    "bullish_nonconfirmation",   # new TICK low, underlying doesn't confirm
    "confirmed_extreme",          # both TICK and underlying make new extreme
    "no_extreme",                 # no new TICK extreme this tick
]


@dataclass
class TickPair:
    """A (underlying, tick_symbol) pairing the strategy tracks.

    Attributes:
        underlying:    Price symbol (e.g. "SPY")
        tick_symbol:   Schwab symbol for the matching tick index (e.g. "$TICK")
        trade_enabled: If False, signals are computed and logged but no orders
                       are placed. Used for evaluating new pairs before
                       committing real capital.
    """

    underlying: str
    tick_symbol: str
    trade_enabled: bool = True

    @classmethod
    def parse(cls, spec: str, trade_enabled: bool = True) -> "TickPair":
        """Parse a "UNDERLYING:TICK" spec. Raises ValueError on bad input."""
        parts = spec.strip().split(":", 1)
        if len(parts) != 2 or not parts[0] or not parts[1]:
            raise ValueError(f"Invalid tick pair spec: {spec!r} (expected 'UNDER:TICK')")
        return cls(underlying=parts[0].strip().upper(), tick_symbol=parts[1].strip(), trade_enabled=trade_enabled)


@dataclass
class TickDailyState:
    """Running intraday state for one pair, reset at each session open."""

    session_date: str = ""          # YYYY-MM-DD (US/Eastern)
    tick_high: float = float("-inf")
    tick_low: float = float("inf")
    underlying_high: float = float("-inf")
    underlying_low: float = float("inf")
    last_entry_ts: str | None = None   # ISO 8601 datetime of last order placed

    def is_initialized(self) -> bool:
        return self.session_date != ""

    def to_dict(self) -> dict:
        return {
            "session_date": self.session_date,
            "tick_high": self.tick_high if self.tick_high != float("-inf") else None,
            "tick_low": self.tick_low if self.tick_low != float("inf") else None,
            "underlying_high": self.underlying_high if self.underlying_high != float("-inf") else None,
            "underlying_low": self.underlying_low if self.underlying_low != float("inf") else None,
            "last_entry_ts": self.last_entry_ts,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "TickDailyState":
        def _num(v, default):
            return float(v) if v is not None else default
        return cls(
            session_date=str(d.get("session_date", "")),
            tick_high=_num(d.get("tick_high"), float("-inf")),
            tick_low=_num(d.get("tick_low"), float("inf")),
            underlying_high=_num(d.get("underlying_high"), float("-inf")),
            underlying_low=_num(d.get("underlying_low"), float("inf")),
            last_entry_ts=d.get("last_entry_ts"),
        )


@dataclass
class TickSignalResult:
    """Output of classify_tick_signal."""

    kind: TickSignalKind
    signal: Signal            # Signal.BUY / Signal.SELL / Signal.HOLD
    score: float              # magnitude of the divergence (0.0-1.0)
    reason: str               # human-readable explanation
    new_tick_extreme: float | None = None   # the new TICK extreme that triggered the signal
    prior_tick_extreme: float | None = None  # the prior extreme it crossed
    underlying_gap_bps: float | None = None  # how far underlying is from its extreme, in bps


def classify_tick_signal(
    prior_state: TickDailyState,
    current_tick: float,
    current_underlying: float,
    extreme_threshold: float,
    confirmation_bps: float,
) -> TickSignalResult:
    """Classify a new (tick, underlying) reading against the prior running state.

    Args:
        prior_state:        State captured BEFORE this reading was incorporated.
                            The function does not mutate it.
        current_tick:       Latest $TICK value.
        current_underlying: Latest price of the underlying (e.g. SPY).
        extreme_threshold:  Minimum |TICK| for an extreme to count. Values
                            near zero aren't meaningful breadth extremes.
                            Typical: 500.
        confirmation_bps:   How close (in basis points) to its own daily
                            high/low the underlying must be to "confirm"
                            a TICK extreme. Typical: 5.0 (= 0.05%).

    Returns:
        TickSignalResult. The signal is Signal.HOLD unless a non-confirmation
        divergence fires.
    """
    # Guard: if state hasn't seen a value yet, there's nothing to compare.
    if not prior_state.is_initialized():
        return TickSignalResult(kind="no_extreme", signal=Signal.HOLD, score=0.0,
                                reason="state not yet initialized")

    # Was a new TICK extreme actually printed?
    made_new_tick_high = (
        current_tick > prior_state.tick_high
        and current_tick >= extreme_threshold
    )
    made_new_tick_low = (
        current_tick < prior_state.tick_low
        and current_tick <= -extreme_threshold
    )

    if not made_new_tick_high and not made_new_tick_low:
        return TickSignalResult(kind="no_extreme", signal=Signal.HOLD, score=0.0,
                                reason=f"tick={current_tick:.0f} within day range")

    # Compute underlying confirmation. "Confirms" means within confirmation_bps
    # of the running daily extreme (above for highs, below for lows).
    bps_factor = confirmation_bps / 10000.0
    if made_new_tick_high:
        high_threshold = prior_state.underlying_high * (1 - bps_factor)
        underlying_confirms = current_underlying >= high_threshold
        gap_bps = (
            (prior_state.underlying_high - current_underlying) / prior_state.underlying_high * 10000
            if prior_state.underlying_high > 0
            else 0.0
        )
        if underlying_confirms:
            return TickSignalResult(
                kind="confirmed_extreme",
                signal=Signal.HOLD,
                score=0.0,
                reason=f"tick new THOD {current_tick:.0f} confirmed by underlying near daily high",
                new_tick_extreme=current_tick,
                prior_tick_extreme=prior_state.tick_high,
                underlying_gap_bps=round(gap_bps, 2),
            )
        # Bearish non-confirmation — signal SELL
        return TickSignalResult(
            kind="bearish_nonconfirmation",
            signal=Signal.SELL,
            score=_divergence_score(gap_bps, extreme_threshold, current_tick),
            reason=f"tick new THOD {current_tick:.0f} NOT confirmed (underlying {gap_bps:.1f} bps below daily high)",
            new_tick_extreme=current_tick,
            prior_tick_extreme=prior_state.tick_high,
            underlying_gap_bps=round(gap_bps, 2),
        )

    # made_new_tick_low branch
    low_threshold = prior_state.underlying_low * (1 + bps_factor)
    underlying_confirms = current_underlying <= low_threshold
    gap_bps = (
        (current_underlying - prior_state.underlying_low) / prior_state.underlying_low * 10000
        if prior_state.underlying_low > 0
        else 0.0
    )
    if underlying_confirms:
        return TickSignalResult(
            kind="confirmed_extreme",
            signal=Signal.HOLD,
            score=0.0,
            reason=f"tick new TLOD {current_tick:.0f} confirmed by underlying near daily low",
            new_tick_extreme=current_tick,
            prior_tick_extreme=prior_state.tick_low,
            underlying_gap_bps=round(gap_bps, 2),
        )
    return TickSignalResult(
        kind="bullish_nonconfirmation",
        signal=Signal.BUY,
        score=_divergence_score(gap_bps, extreme_threshold, current_tick),
        reason=f"tick new TLOD {current_tick:.0f} NOT confirmed (underlying {gap_bps:.1f} bps above daily low)",
        new_tick_extreme=current_tick,
        prior_tick_extreme=prior_state.tick_low,
        underlying_gap_bps=round(gap_bps, 2),
    )


def _divergence_score(gap_bps: float, extreme_threshold: float, current_tick: float) -> float:
    """Convert the observed gap into a 0-1 score.

    Larger underlying gap and more extreme TICK both push the score up.
    Score is used for ranking opportunities when multiple pairs fire in
    the same scan cycle; the absolute value isn't economically meaningful.
    """
    # Gap component: 0 bps = 0 score, 50+ bps = 1.0 score
    gap_component = min(1.0, abs(gap_bps) / 50.0)
    # TICK component: threshold TICK = 0 score, 2x threshold = 1.0
    tick_component = min(1.0, max(0.0, (abs(current_tick) - extreme_threshold) / extreme_threshold))
    return round(0.5 * gap_component + 0.5 * tick_component, 3)


def update_daily_state(
    state: TickDailyState,
    session_date: str,
    current_tick: float,
    current_underlying: float,
) -> TickDailyState:
    """Return a new TickDailyState with the current tick/underlying folded in.

    - If `session_date` differs from the state's date, the state is reset
      (new trading day).
    - Running high/low values are updated to include the new reading.
    - `last_entry_ts` is preserved.
    """
    if state.session_date != session_date:
        # New session — discard old running state
        return TickDailyState(
            session_date=session_date,
            tick_high=current_tick,
            tick_low=current_tick,
            underlying_high=current_underlying,
            underlying_low=current_underlying,
            last_entry_ts=None,
        )

    return TickDailyState(
        session_date=session_date,
        tick_high=max(state.tick_high, current_tick),
        tick_low=min(state.tick_low, current_tick),
        underlying_high=max(state.underlying_high, current_underlying),
        underlying_low=min(state.underlying_low, current_underlying),
        last_entry_ts=state.last_entry_ts,
    )


def is_within_cooldown(
    state: TickDailyState,
    now: datetime,
    cooldown_minutes: float,
) -> bool:
    """Return True if an entry should be skipped due to the cooldown gate."""
    if state.last_entry_ts is None:
        return False
    try:
        last = datetime.fromisoformat(state.last_entry_ts)
    except ValueError:
        return False
    elapsed_min = (now - last).total_seconds() / 60.0
    return elapsed_min < cooldown_minutes
