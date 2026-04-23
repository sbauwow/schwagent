"""TickBreadth — intraday $TICK-vs-price non-confirmation strategy.

Thesis: when $TICK prints a new daily extreme without the underlying
(SPY) confirming, internal breadth is diverging from price → short-term
reversal is likely. Details in src/schwabagent/breadth.py.

Execution model:
  - Long-only (accounts are cash-level)
  - Single underlying per pair (default: SPY)
  - One position at a time per underlying
  - LIMIT orders via the shared place_order config defaults
  - Exits: take-profit / stop-loss / session-close time stop

Pairs are configured via TICK_PAIRS (traded) and TICK_OBSERVE_PAIRS
(signal-only logging). Adding a new pair is a config change, not a
code change — the scan loop iterates over self.config.tick_pairs().

Intraday running state is persisted to
    ~/.schwagent/tick_breadth_state.json
and reset at the start of each US/Eastern trading day. If the state
file is missing or stale, the strategy bootstraps from Schwab's daily
$TICK OHLC bar which gives today's session high/low at worst a few
minutes late.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, time as dtime
from pathlib import Path
from zoneinfo import ZoneInfo

from schwabagent.breadth import (
    TickDailyState,
    TickPair,
    TickSignalResult,
    classify_tick_signal,
    is_within_cooldown,
    update_daily_state,
)
from schwabagent.config import Config
from schwabagent.persistence import StateStore
from schwabagent.risk import RiskManager
from schwabagent.schwab_client import AccountSummary, SchwabClient
from schwabagent.strategies.base import Signal, Strategy

logger = logging.getLogger(__name__)

ET = ZoneInfo("America/New_York")
RTH_OPEN = dtime(9, 30)


class TickBreadthStrategy(Strategy):
    """Intraday $TICK divergence strategy."""

    name = "tick_breadth"

    def __init__(
        self,
        client: SchwabClient,
        config: Config,
        risk: RiskManager,
        state: StateStore,
        account: AccountSummary | None = None,
    ):
        super().__init__(client, config, risk, state)
        self._account = account
        self._state_path = Path(config.STATE_DIR).expanduser() / "tick_breadth_state.json"
        self._pair_states: dict[str, TickDailyState] = {}
        self._load_state()

    def set_account(self, account: AccountSummary) -> None:
        self._account = account

    # ── pair configuration ───────────────────────────────────────────────────

    def _pairs(self) -> list[TickPair]:
        """Return the full list of pairs — traded first, then observe-only."""
        pairs: list[TickPair] = []
        for spec in self.config.tick_pairs_traded:
            try:
                pairs.append(TickPair.parse(spec, trade_enabled=True))
            except ValueError as e:
                logger.warning("[%s] %s", self.name, e)
        for spec in self.config.tick_pairs_observed:
            try:
                pairs.append(TickPair.parse(spec, trade_enabled=False))
            except ValueError as e:
                logger.warning("[%s] %s", self.name, e)
        return pairs

    # ── state persistence ───────────────────────────────────────────────────

    def _load_state(self) -> None:
        if not self._state_path.exists():
            return
        try:
            raw = json.loads(self._state_path.read_text())
            for underlying, d in (raw.get("pairs") or {}).items():
                self._pair_states[underlying] = TickDailyState.from_dict(d)
        except (OSError, ValueError) as e:
            logger.warning("[%s] could not load state: %s", self.name, e)

    def _save_state(self) -> None:
        try:
            self._state_path.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                "pairs": {u: s.to_dict() for u, s in self._pair_states.items()}
            }
            self._state_path.write_text(json.dumps(payload, indent=2))
        except OSError as e:
            logger.warning("[%s] could not save state: %s", self.name, e)

    def _get_pair_state(self, underlying: str) -> TickDailyState:
        return self._pair_states.get(underlying, TickDailyState())

    # ── session helpers ──────────────────────────────────────────────────────

    def _now_et(self) -> datetime:
        return datetime.now(ET)

    def _session_date(self, now: datetime) -> str:
        return now.date().isoformat()

    def _close_by_time(self) -> dtime:
        """Parse config.TICK_CLOSE_BY_ET (e.g. "15:45") into a dtime."""
        raw = str(self.config.TICK_CLOSE_BY_ET or "15:45")
        try:
            h, m = raw.split(":")
            return dtime(int(h), int(m))
        except (ValueError, AttributeError):
            return dtime(15, 45)

    def _is_rth_open(self, now: datetime) -> bool:
        """True between 09:30 and TICK_CLOSE_BY_ET on weekdays."""
        if now.weekday() >= 5:
            return False
        t = now.timetz().replace(tzinfo=None)
        return RTH_OPEN <= t <= self._close_by_time()

    def _is_past_close_by(self, now: datetime) -> bool:
        t = now.timetz().replace(tzinfo=None)
        return t >= self._close_by_time()

    # ── bootstrap from daily $TICK bar ───────────────────────────────────────

    def _bootstrap_from_daily(
        self,
        pair: TickPair,
        session_date: str,
    ) -> TickDailyState | None:
        """Seed running state from Schwab's most recent daily $TICK candle.

        When the loop starts mid-session or after a restart, we don't want
        our running high/low to begin at whatever the current tick value is
        (that would hide extremes that happened earlier in the day). The
        daily OHLC endpoint gives today's high/low even for partial sessions.

        Known limitation: Schwab's daily $TICK endpoint is intermittent —
        occasionally returns empty for valid symbols. On failure we return
        None and let the caller seed from the current tick/underlying
        reading. That means early-session extremes can be lost if the loop
        restarts after some intraday action.
        """
        try:
            df = self.client.get_ohlcv(pair.tick_symbol, days=3)
        except Exception as e:
            logger.info("[%s] bootstrap %s: OHLC fetch error (%s)", self.name, pair.tick_symbol, e)
            return None
        if df is None or df.empty:
            logger.info(
                "[%s] bootstrap %s: daily bar empty — will seed from current reading",
                self.name, pair.tick_symbol,
            )
            return None
        row = df.iloc[-1]

        try:
            u_df = self.client.get_ohlcv(pair.underlying, days=3)
            if u_df is None or u_df.empty:
                logger.info(
                    "[%s] bootstrap %s: underlying daily bar empty — seeding from current",
                    self.name, pair.underlying,
                )
                return None
            u_row = u_df.iloc[-1]
        except Exception as e:
            logger.info("[%s] bootstrap %s: OHLC fetch error (%s)", self.name, pair.underlying, e)
            return None

        state = TickDailyState(
            session_date=session_date,
            tick_high=float(row.get("high", float("-inf"))),
            tick_low=float(row.get("low", float("inf"))),
            underlying_high=float(u_row.get("high", float("-inf"))),
            underlying_low=float(u_row.get("low", float("inf"))),
        )
        logger.info(
            "[%s] bootstrap %s/%s from daily bar: tick[%.0f, %.0f] underlying[%.2f, %.2f]",
            self.name, pair.underlying, pair.tick_symbol,
            state.tick_low, state.tick_high, state.underlying_low, state.underlying_high,
        )
        return state

    # ── scan ─────────────────────────────────────────────────────────────────

    def scan(self) -> list[dict]:
        opportunities: list[dict] = []
        now = self._now_et()

        if not self._is_rth_open(now):
            logger.debug("[%s] outside RTH window — skipping", self.name)
            return opportunities

        pairs = self._pairs()
        if not pairs:
            return opportunities

        session_date = self._session_date(now)

        # Batch-fetch all quotes in one call
        all_symbols: list[str] = []
        for p in pairs:
            all_symbols.append(p.underlying)
            all_symbols.append(p.tick_symbol)
        quotes = self.client.get_quotes(list(dict.fromkeys(all_symbols)))

        # Check time-stop (close any open positions at TICK_CLOSE_BY_ET)
        time_stop_opps = self._time_stop_opportunities(now, pairs, quotes)
        opportunities.extend(time_stop_opps)

        for pair in pairs:
            u_quote = quotes.get(pair.underlying)
            t_quote = quotes.get(pair.tick_symbol)
            if u_quote is None or t_quote is None:
                continue
            current_underlying = u_quote.last or 0.0
            current_tick = t_quote.last or 0.0
            if current_underlying <= 0:
                continue

            state = self._get_pair_state(pair.underlying)

            # Bootstrap mid-session if state is missing or stale
            if state.session_date != session_date and self.config.TICK_BOOTSTRAP_FROM_DAILY:
                bootstrapped = self._bootstrap_from_daily(pair, session_date)
                if bootstrapped is not None:
                    state = bootstrapped

            # Update running state with the new reading — this is the ONLY
            # place state mutates during a scan.
            prior_state = state
            new_state = update_daily_state(state, session_date, current_tick, current_underlying)
            self._pair_states[pair.underlying] = new_state

            # Compute signal against the prior state — NOT the state that
            # already includes the new reading (otherwise we'd never see
            # a "new extreme" since it's already the current extreme).
            signal_result = classify_tick_signal(
                prior_state=prior_state,
                current_tick=current_tick,
                current_underlying=current_underlying,
                extreme_threshold=self.config.TICK_EXTREME_THRESHOLD,
                confirmation_bps=self.config.TICK_CONFIRMATION_BPS,
            )

            # Also evaluate TP/SL on any existing position for this underlying
            exit_opp = self._evaluate_exits(pair, current_underlying)
            if exit_opp is not None:
                opportunities.append(exit_opp)
                continue  # don't also consider a new entry on the same pair this tick

            if signal_result.signal == Signal.HOLD:
                continue

            # Cooldown gate — skip if we fired an entry within TICK_ENTRY_COOLDOWN_MIN
            if is_within_cooldown(new_state, now, self.config.TICK_ENTRY_COOLDOWN_MIN):
                logger.debug("[%s] %s within cooldown, skipping", self.name, pair.underlying)
                continue

            opp = self._build_opportunity(pair, signal_result, current_underlying, current_tick)
            if opp is not None:
                opportunities.append(opp)

        self._save_state()
        return opportunities

    def _build_opportunity(
        self,
        pair: TickPair,
        signal_result: TickSignalResult,
        current_underlying: float,
        current_tick: float,
    ) -> dict | None:
        """Convert a TickSignalResult into a runner-compatible opportunity dict."""
        sig = signal_result.signal
        # If the pair is observe-only, we still emit the opportunity but
        # tag it so execute() skips the order placement entirely.
        observe_only = not pair.trade_enabled

        # Long-only gate: a SELL on a pair we don't currently hold is a no-op
        # in (a) close-only mode. Still emit it for logging if observe-only.
        held_quantity = self._held_quantity(pair.underlying)
        if sig == Signal.SELL and held_quantity == 0 and not observe_only:
            # Nothing to close — but we do want to record the signal for analysis.
            return {
                "strategy": self.name,
                "symbol": pair.underlying,
                "signal": Signal.HOLD,
                "score": 0.0,
                "price": current_underlying,
                "tick": current_tick,
                "tick_signal_kind": signal_result.kind,
                "reason": f"[log-only] {signal_result.reason} (no position held)",
                "_signal_only": True,
            }

        return {
            "strategy": self.name,
            "symbol": pair.underlying,
            "signal": sig,
            "score": signal_result.score,
            "price": current_underlying,
            "tick": current_tick,
            "tick_signal_kind": signal_result.kind,
            "tick_new_extreme": signal_result.new_tick_extreme,
            "tick_prior_extreme": signal_result.prior_tick_extreme,
            "underlying_gap_bps": signal_result.underlying_gap_bps,
            "tick_pair": pair.tick_symbol,
            "observe_only": observe_only,
            "reason": signal_result.reason,
        }

    def _held_quantity(self, symbol: str) -> int:
        account = self._account
        if account is None:
            return 0
        pos = next((p for p in account.positions if p.symbol == symbol), None)
        return int(pos.quantity) if pos else 0

    # ── exit management ──────────────────────────────────────────────────────

    def _evaluate_exits(self, pair: TickPair, current_price: float) -> dict | None:
        """Check TP and SL on an open position. Returns a SELL opportunity or None."""
        if not pair.trade_enabled:
            return None
        account = self._account
        if account is None:
            return None
        held = next((p for p in account.positions if p.symbol == pair.underlying and p.quantity > 0), None)
        if held is None or held.quantity <= 0:
            return None

        entry = held.avg_price
        if entry <= 0:
            return None
        move_pct = (current_price - entry) / entry * 100

        if move_pct >= self.config.TICK_TAKE_PROFIT_PCT:
            return self._exit_opportunity(pair, current_price, "take_profit", move_pct)
        if move_pct <= -self.config.TICK_STOP_LOSS_PCT:
            return self._exit_opportunity(pair, current_price, "stop_loss", move_pct)
        return None

    def _exit_opportunity(self, pair: TickPair, price: float, reason_kind: str, move_pct: float) -> dict:
        return {
            "strategy": self.name,
            "symbol": pair.underlying,
            "signal": Signal.SELL,
            "score": 0.0,
            "price": price,
            "reason": f"{reason_kind} move={move_pct:+.2f}%",
            "exit_kind": reason_kind,
            "observe_only": False,
        }

    def _time_stop_opportunities(self, now: datetime, pairs: list[TickPair], quotes: dict) -> list[dict]:
        """If we're past TICK_CLOSE_BY_ET, flush any open positions on our pairs."""
        if not self._is_past_close_by(now):
            return []
        opps: list[dict] = []
        account = self._account
        if account is None:
            return opps
        for pair in pairs:
            if not pair.trade_enabled:
                continue
            held = next(
                (p for p in account.positions if p.symbol == pair.underlying and p.quantity > 0),
                None,
            )
            if held is None:
                continue
            quote = quotes.get(pair.underlying)
            price = (quote.last if quote else 0.0) or held.avg_price
            opps.append(self._exit_opportunity(pair, price, "time_stop", 0.0))
        return opps

    # ── execute ──────────────────────────────────────────────────────────────

    def execute(self, opportunity: dict) -> dict | None:
        if opportunity.get("_signal_only"):
            logger.info(
                "[%s] signal-only: %s %s  %s",
                self.name,
                opportunity.get("symbol"),
                opportunity.get("tick_signal_kind"),
                opportunity.get("reason"),
            )
            return None

        signal = opportunity["signal"]
        symbol = opportunity["symbol"]
        price = opportunity.get("price", 0.0)

        account = self._account
        if account is None:
            logger.warning("[%s] no account set — skipping %s", self.name, symbol)
            return None

        if price <= 0:
            quote = self.client.get_quotes([symbol]).get(symbol)
            price = quote.last if quote else 0.0
        if price <= 0:
            return None

        if opportunity.get("observe_only"):
            # Observe-only pair — log the signal, do not route any order
            logger.info(
                "[%s] observe-only: %s %s @ $%.2f  %s",
                self.name, symbol, signal.value, price, opportunity.get("reason"),
            )
            return {
                "strategy": self.name,
                "symbol": symbol,
                "signal": signal.value,
                "price": price,
                "dry_run": True,
                "status": "observed",
                "reason": opportunity.get("reason"),
                **{k: v for k, v in opportunity.items() if not k.startswith("_")},
            }

        if signal == Signal.BUY:
            result = self._buy(symbol, price, account, opportunity)
        elif signal == Signal.SELL:
            result = self._sell(symbol, price, account, opportunity)
        else:
            return None

        # Record entry timestamp in state for cooldown purposes
        if result and result.get("side") == "BUY":
            state = self._pair_states.get(symbol)
            if state is not None:
                state.last_entry_ts = self._now_et().isoformat()
                self._save_state()

        return result

    def _buy(
        self,
        symbol: str,
        price: float,
        account: AccountSummary,
        opp: dict,
    ) -> dict | None:
        order_value = min(self.config.MAX_ORDER_VALUE, account.cash_available * 0.95)
        order_value = self._autotune_sizing(order_value, opp)
        if order_value < self.config.MIN_ORDER_VALUE:
            return None

        quantity = max(1, int(order_value / price))
        allowed, reason = self.risk.can_buy(symbol, quantity, price, account)
        if not allowed:
            logger.debug("[%s] BUY blocked for %s: %s", self.name, symbol, reason)
            return None

        if self._should_execute(opp):
            result = self.client.place_order(account.account_hash, symbol, "BUY", quantity)
            if result.get("status") != "ok":
                logger.error("[%s] BUY failed for %s: %s", self.name, symbol, result.get("error"))
                return None
        else:
            result = {"status": "dry_run", "order_id": "dry"}

        self.risk.record_trade(symbol, "BUY", quantity, price, strategy=self.name)
        trade = {
            "strategy": self.name,
            "symbol": symbol,
            "side": "BUY",
            "signal": Signal.BUY.value,
            "quantity": quantity,
            "price": price,
            "value": quantity * price,
            "dry_run": not self._should_execute(opp),
            **{k: v for k, v in opp.items() if not k.startswith("_")},
            **result,
        }
        logger.info(
            "[%s] BUY %d %s @ $%.2f  %s",
            self.name, quantity, symbol, price, opp.get("reason"),
        )
        return trade

    def _sell(
        self,
        symbol: str,
        price: float,
        account: AccountSummary,
        opp: dict,
    ) -> dict | None:
        held = next((p for p in account.positions if p.symbol == symbol and p.quantity > 0), None)
        if held is None:
            return None
        quantity = int(held.quantity)
        if quantity <= 0:
            return None

        if self._should_execute(opp):
            result = self.client.place_order(account.account_hash, symbol, "SELL", quantity)
            if result.get("status") != "ok":
                logger.error("[%s] SELL failed for %s: %s", self.name, symbol, result.get("error"))
                return None
        else:
            result = {"status": "dry_run", "order_id": "dry"}

        pnl = (price - held.avg_price) * quantity
        self.session_pnl += pnl
        self.state.update_strategy_pnl(self.name, pnl, win=pnl > 0)
        self.risk.record_trade(symbol, "SELL", quantity, price, strategy=self.name)

        trade = {
            "strategy": self.name,
            "symbol": symbol,
            "side": "SELL",
            "signal": Signal.SELL.value,
            "quantity": quantity,
            "price": price,
            "value": quantity * price,
            "realized_pnl": round(pnl, 4),
            "dry_run": not self._should_execute(opp),
            **{k: v for k, v in opp.items() if not k.startswith("_")},
            **result,
        }
        logger.info(
            "[%s] SELL %d %s @ $%.2f pnl=$%.2f  %s",
            self.name, quantity, symbol, price, pnl, opp.get("reason"),
        )
        return trade
