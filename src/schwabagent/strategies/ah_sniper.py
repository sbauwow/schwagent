"""AhSniperStrategy — after-hours deep-limit sniper.

Thesis: AH books (16:00-20:00 ET) are thin. An occasional forced-liquidation
print or fat-finger market order can sweep well below the RTH close. If a
deep buy limit is already parked there, it gets filled at a price the RTH
book would never honour. Next RTH session the book usually snaps back
toward a sane level, so the sniped fill can be flipped for a few percent.

Long-only, fixed $ per ticker. One snipe per symbol per AH session.
Placement, cancellation, and exit are all driven by ET wall-clock time so
the strategy does the right thing no matter how often the runner calls
`run_once()`:

  16:05-19:55 ET   → place snipe BUY limits for AH_SNIPER_SYMBOLS at
                     close − AH_SNIPER_ATR_MULT × ATR, session=SEAMLESS.
  19:55-20:00 ET   → cancel any still-open snipes (belt-and-suspenders
                     even though DAY duration would expire them at 20:00).
  Next RTH         → held positions are evaluated for TP / SL, and any
                     still-open at AH_SNIPER_EXIT_BY_ET are flushed.

State lives at ~/.schwagent/ah_sniper_state.json so repeated scans in
the same AH session don't re-place snipes:

    {
      "session_date": "2026-04-13",
      "open_snipes": {
         "AAPL": {"order_id": "1234", "limit": 165.22, "qty": 6,
                  "close": 180.00, "placed_at": "2026-04-13T16:05:32-04:00"}
      }
    }
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, time as dtime
from pathlib import Path
from zoneinfo import ZoneInfo

from schwabagent.config import Config
from schwabagent.indicators import atr
from schwabagent.persistence import StateStore
from schwabagent.risk import RiskManager
from schwabagent.schwab_client import AccountSummary, SchwabClient
from schwabagent.strategies.base import Signal, Strategy

logger = logging.getLogger(__name__)

ET = ZoneInfo("America/New_York")
RTH_OPEN = dtime(9, 30)
RTH_CLOSE = dtime(16, 0)
AH_CLOSE = dtime(20, 0)


def _parse_hhmm(raw: str, default: dtime) -> dtime:
    try:
        h, m = str(raw).split(":")
        return dtime(int(h), int(m))
    except (ValueError, AttributeError):
        return default


class AhSniperStrategy(Strategy):
    """Deep-limit buy sniper for the after-hours session."""

    name = "ah_sniper"

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
        self._state_path = Path(config.STATE_DIR).expanduser() / "ah_sniper_state.json"
        self._session_date: str | None = None
        self._open_snipes: dict[str, dict] = {}
        self._load_state()

    def set_account(self, account: AccountSummary) -> None:
        self._account = account

    # ── session / state ──────────────────────────────────────────────────────

    def _now_et(self) -> datetime:
        return datetime.now(ET)

    def _session_key(self, now: datetime) -> str:
        return now.date().isoformat()

    def _start_time(self) -> dtime:
        return _parse_hhmm(self.config.AH_SNIPER_START_ET, dtime(16, 5))

    def _cancel_time(self) -> dtime:
        return _parse_hhmm(self.config.AH_SNIPER_CANCEL_ET, dtime(19, 55))

    def _exit_by_time(self) -> dtime:
        return _parse_hhmm(self.config.AH_SNIPER_EXIT_BY_ET, dtime(15, 45))

    def _in_snipe_window(self, now: datetime) -> bool:
        if now.weekday() >= 5:
            return False
        t = now.timetz().replace(tzinfo=None)
        return self._start_time() <= t < self._cancel_time()

    def _past_cancel_time(self, now: datetime) -> bool:
        if now.weekday() >= 5:
            return False
        t = now.timetz().replace(tzinfo=None)
        return self._cancel_time() <= t < AH_CLOSE

    def _in_rth(self, now: datetime) -> bool:
        if now.weekday() >= 5:
            return False
        t = now.timetz().replace(tzinfo=None)
        return RTH_OPEN <= t <= RTH_CLOSE

    def _past_exit_by(self, now: datetime) -> bool:
        if now.weekday() >= 5:
            return False
        t = now.timetz().replace(tzinfo=None)
        return t >= self._exit_by_time()

    def _load_state(self) -> None:
        if not self._state_path.exists():
            return
        try:
            raw = json.loads(self._state_path.read_text())
            self._session_date = raw.get("session_date")
            self._open_snipes = dict(raw.get("open_snipes") or {})
        except (OSError, ValueError) as e:
            logger.warning("[%s] could not load state: %s", self.name, e)

    def _save_state(self) -> None:
        try:
            self._state_path.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                "session_date": self._session_date,
                "open_snipes": self._open_snipes,
            }
            self._state_path.write_text(json.dumps(payload, indent=2))
        except OSError as e:
            logger.warning("[%s] could not save state: %s", self.name, e)

    # ── scan ─────────────────────────────────────────────────────────────────

    def scan(self) -> list[dict]:
        opportunities: list[dict] = []
        now = self._now_et()
        session_key = self._session_key(now)

        # Phase 1: cancel leftover snipes once we're past the cancel cutoff.
        if self._past_cancel_time(now) and self._open_snipes:
            for symbol, info in list(self._open_snipes.items()):
                opportunities.append({
                    "strategy": self.name,
                    "symbol": symbol,
                    "signal": Signal.HOLD,
                    "score": 0.0,
                    "price": info.get("limit", 0.0),
                    "action": "cancel",
                    "order_id": info.get("order_id"),
                    "reason": f"AH session closing — cancelling snipe (limit ${info.get('limit', 0):.2f})",
                })

        # Phase 2: exit any held snipes during next RTH.
        if self._in_rth(now):
            opportunities.extend(self._rth_exit_opportunities(now))

        # Phase 3: place new snipes inside the AH window, once per session.
        if self._in_snipe_window(now) and self._session_date != session_key:
            opportunities.extend(self._build_snipe_entries(session_key))

        return opportunities

    # ── exit logic (runs during next RTH) ────────────────────────────────────

    def _rth_exit_opportunities(self, now: datetime) -> list[dict]:
        opps: list[dict] = []
        account = self._account
        if account is None:
            return opps
        tracked = {s.upper() for s in self._open_snipes.keys()}
        # Only exit positions we opened ourselves (by symbol match + position size
        # growth). Runner may also hold unrelated positions on the same tickers.
        if not tracked:
            return opps

        force_close = self._past_exit_by(now)
        held_syms = [p.symbol for p in account.positions if p.symbol in tracked and p.quantity > 0]
        if not held_syms:
            return opps

        quotes = self.client.get_quotes(held_syms)
        for pos in account.positions:
            if pos.symbol not in tracked or pos.quantity <= 0:
                continue
            quote = quotes.get(pos.symbol)
            last = (quote.last if quote else 0.0) or pos.avg_price
            if last <= 0:
                continue
            pnl_pct = (last - pos.avg_price) / pos.avg_price * 100 if pos.avg_price > 0 else 0.0

            reason_kind: str | None = None
            if force_close:
                reason_kind = "time_stop"
            elif pnl_pct >= self.config.AH_SNIPER_TAKE_PROFIT_PCT:
                reason_kind = "take_profit"
            elif pnl_pct <= -self.config.AH_SNIPER_STOP_LOSS_PCT:
                reason_kind = "stop_loss"

            if reason_kind is None:
                continue

            opps.append({
                "strategy": self.name,
                "symbol": pos.symbol,
                "signal": Signal.SELL,
                "score": 0.0,
                "price": last,
                "action": "exit",
                "exit_kind": reason_kind,
                "reason": f"{reason_kind} move={pnl_pct:+.2f}%",
                "snipe_entry": pos.avg_price,
            })
        return opps

    # ── entry logic (runs once per AH session) ───────────────────────────────

    def _build_snipe_entries(self, session_key: str) -> list[dict]:
        universe = self.config.ah_sniper_symbols
        if not universe:
            return []
        universe = universe[: max(1, int(self.config.AH_SNIPER_MAX_SYMBOLS))]

        opps: list[dict] = []
        for symbol in universe:
            df = self._fetch_ohlcv(symbol, days=max(40, self.config.AH_SNIPER_ATR_PERIOD * 3))
            if df is None:
                continue
            try:
                last_close = float(df["close"].iloc[-1])
            except (KeyError, IndexError):
                continue
            if last_close <= 0:
                continue

            atr_val = atr(df["high"], df["low"], df["close"], self.config.AH_SNIPER_ATR_PERIOD)
            if atr_val != atr_val or atr_val <= 0:  # NaN check
                continue

            raw_offset = self.config.AH_SNIPER_ATR_MULT * atr_val
            min_offset = last_close * self.config.AH_SNIPER_MIN_OFFSET_PCT / 100.0
            max_offset = last_close * self.config.AH_SNIPER_MAX_OFFSET_PCT / 100.0
            offset = min(max(raw_offset, min_offset), max_offset)
            limit_price = round(last_close - offset, 2)
            if limit_price <= 0:
                continue
            offset_pct = offset / last_close * 100

            opps.append({
                "strategy": self.name,
                "symbol": symbol,
                "signal": Signal.BUY,
                "score": 1.0,
                "price": last_close,
                "action": "snipe_place",
                "session_key": session_key,
                "limit_price": limit_price,
                "atr": round(atr_val, 4),
                "offset_pct": round(offset_pct, 2),
                "reason": (
                    f"snipe at ${limit_price:.2f} "
                    f"({offset_pct:.1f}% below close ${last_close:.2f}, "
                    f"ATR={atr_val:.2f})"
                ),
            })
        return opps

    # ── execute ──────────────────────────────────────────────────────────────

    def execute(self, opportunity: dict) -> dict | None:
        action = opportunity.get("action")
        if action == "snipe_place":
            return self._place_snipe(opportunity)
        if action == "cancel":
            return self._cancel_snipe(opportunity)
        if action == "exit":
            return self._exit_snipe(opportunity)
        logger.debug("[%s] unknown action %s — ignoring", self.name, action)
        return None

    # Entry placement
    def _place_snipe(self, opp: dict) -> dict | None:
        account = self._account
        if account is None:
            logger.warning("[%s] no account set — skipping %s", self.name, opp.get("symbol"))
            return None

        symbol = opp["symbol"]
        limit_price = float(opp["limit_price"])
        session_key = opp["session_key"]

        order_value = self.config.AH_SNIPER_POSITION_USD
        order_value = self._autotune_sizing(order_value, opp)
        if order_value < self.config.MIN_ORDER_VALUE:
            return None
        quantity = max(1, int(order_value / limit_price))

        allowed, reason = self.risk.can_buy(symbol, quantity, limit_price, account)
        if not allowed:
            logger.debug("[%s] BUY blocked for %s: %s", self.name, symbol, reason)
            return None

        if self._should_execute(opp):
            result = self.client.place_order(
                account.account_hash,
                symbol,
                "BUY",
                quantity,
                order_type="LIMIT",
                limit_price=limit_price,
                session="SEAMLESS",
                duration="DAY",
            )
            if result.get("status") != "ok":
                logger.error("[%s] snipe BUY failed for %s: %s", self.name, symbol, result.get("error"))
                return None
            order_id = result.get("order_id")
        else:
            result = {"status": "dry_run", "order_id": "dry"}
            order_id = "dry"

        self._open_snipes[symbol] = {
            "order_id": order_id,
            "limit": limit_price,
            "qty": quantity,
            "close": opp.get("price"),
            "placed_at": self._now_et().isoformat(),
        }
        self._session_date = session_key
        self._save_state()

        self.risk.record_trade(symbol, "BUY", quantity, limit_price, strategy=self.name)
        # Unpack opp extras first so the explicit fields below win — opp["price"]
        # is the RTH close used to compute the limit, not the order price itself.
        trade = {
            **{k: v for k, v in opp.items() if not k.startswith("_")},
            "strategy": self.name,
            "symbol": symbol,
            "side": "BUY",
            "signal": Signal.BUY.value,
            "quantity": quantity,
            "price": limit_price,
            "value": quantity * limit_price,
            "rth_close": opp.get("price"),
            "dry_run": not self._should_execute(opp),
            **result,
        }
        logger.info(
            "[%s] snipe placed: %d %s @ $%.2f (limit, SEAMLESS) — %s",
            self.name, quantity, symbol, limit_price, opp.get("reason"),
        )
        return trade

    # Cancel leftover snipes at session end
    def _cancel_snipe(self, opp: dict) -> dict | None:
        account = self._account
        if account is None:
            return None
        symbol = opp["symbol"]
        order_id = opp.get("order_id")
        if not order_id or order_id == "dry":
            self._open_snipes.pop(symbol, None)
            self._save_state()
            return None

        ok = True
        if self._should_execute(opp):
            ok = self.client.cancel_order(account.account_hash, order_id)

        if ok:
            self._open_snipes.pop(symbol, None)
            self._save_state()
            logger.info("[%s] snipe cancelled: %s order_id=%s", self.name, symbol, order_id)
            return {
                "strategy": self.name,
                "symbol": symbol,
                "side": "CANCEL",
                "signal": Signal.HOLD.value,
                "quantity": 0,
                "price": opp.get("price", 0.0),
                "value": 0.0,
                "dry_run": not self._should_execute(opp),
                "status": "cancelled",
                "order_id": order_id,
                "reason": opp.get("reason"),
            }
        logger.warning("[%s] cancel failed for %s order_id=%s", self.name, symbol, order_id)
        return None

    # Exit filled snipes next RTH
    def _exit_snipe(self, opp: dict) -> dict | None:
        account = self._account
        if account is None:
            return None
        symbol = opp["symbol"]
        held = next((p for p in account.positions if p.symbol == symbol and p.quantity > 0), None)
        if held is None:
            return None
        quantity = int(held.quantity)
        if quantity <= 0:
            return None

        price = float(opp.get("price") or held.avg_price)

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

        # Clean up snipe tracking so next AH session gets a fresh slot.
        self._open_snipes.pop(symbol, None)
        self._save_state()

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
            "[%s] snipe exit %d %s @ $%.2f pnl=$%.2f  %s",
            self.name, quantity, symbol, price, pnl, opp.get("reason"),
        )
        return trade
