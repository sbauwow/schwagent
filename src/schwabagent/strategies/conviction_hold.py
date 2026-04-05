"""Conviction Hold strategy — compliance-gated positions with minimum hold period.

Designed for situations where:
- You have high conviction on specific names (e.g., RKLB, space/AI sector)
- Compliance requires pre-clearance before trading
- Positions must be held for a minimum period (e.g., 30 days)

Flow:
  1. MONITOR: Track universe daily — technicals, fundamentals, news
  2. SIGNAL: When entry conditions are met, create a compliance request
  3. APPROVE: Compliance approval via Telegram or manual flag
  4. EXECUTE: Buy after approval, record hold-start date
  5. HOLD: Position is locked — no sells until hold period expires
  6. EXIT: After hold period, apply trailing stop or target exit

The hold period is a hard constraint. The strategy will NOT generate
sell signals before the minimum hold days have elapsed, regardless of
price action. This protects against compliance violations.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

from schwabagent.config import Config
from schwabagent.indicators import (
    atr, ema, efficiency_ratio, kama, rsi, rsi_series,
    kst, cmf, ichimoku, aroon, chandelier_exit, detect_divergence,
)
from schwabagent.persistence import StateStore
from schwabagent.risk import RiskManager
from schwabagent.schwab_client import AccountSummary, SchwabClient
from schwabagent.strategies.base import SIGNAL_SCORE, Signal, Strategy

logger = logging.getLogger(__name__)


@dataclass
class ConvictionPosition:
    """A compliance-tracked position with hold period."""
    symbol: str
    entry_price: float
    quantity: int
    entry_date: str            # ISO date
    hold_until: str            # ISO date (entry + min hold days)
    compliance_approved: bool = True
    compliance_id: str = ""    # reference ID from compliance system
    target_price: float = 0.0  # optional take-profit target
    trailing_stop: float = 0.0 # ATR-based trailing stop (updated daily)
    peak_price: float = 0.0    # highest price since entry (for trailing)
    notes: str = ""


class ConvictionHoldStrategy(Strategy):
    """Buy high-conviction names with compliance approval and minimum hold period."""

    name = "conviction_hold"

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
        self._positions: dict[str, ConvictionPosition] = {}
        self._pending_approvals: dict[str, dict] = {}
        self._load_positions()

    def set_account(self, account: AccountSummary) -> None:
        self._account = account

    # ── scan ─────────────────────────────────────────────────────────────

    def scan(self) -> list[dict]:
        """Scan conviction universe for entry signals and exit conditions."""
        universe = self.config.conviction_symbols
        if not universe:
            return []

        opportunities = []

        for symbol in universe:
            # Check if we already hold this
            held = symbol in self._positions

            if held:
                # Check exit conditions (only after hold period)
                exit_signal = self._check_exit(symbol)
                if exit_signal:
                    opportunities.append(exit_signal)
            else:
                # Check entry conditions
                entry_signal = self._check_entry(symbol)
                if entry_signal:
                    opportunities.append(entry_signal)

        # Also generate monitoring data for all symbols
        self._update_monitoring(universe)

        return opportunities

    def _check_entry(self, symbol: str) -> dict | None:
        """Evaluate entry conditions for a symbol."""
        df = self._fetch_ohlcv(symbol, days=200)
        if df is None:
            return None

        close = df["close"]
        high = df["high"]
        low = df["low"]
        volume = df["volume"]
        price = float(close.iloc[-1])

        # ── Technical confirmation ───────────────────────────────────
        # Don't buy into overbought — wait for a pullback or base

        rsi_val = rsi(close, 14)
        kama_val = kama(close, 10, 2, 30)
        er = efficiency_ratio(close, 10)
        kst_val, kst_sig = kst(close)
        money_flow = cmf(high, low, close, volume, 20)
        ichi = ichimoku(high, low, close)
        atr_val = atr(high, low, close, 14)

        # Entry conditions (need 3 of 5 for BUY, 4+ for STRONG_BUY):
        conditions = {
            "kama_trending": price > kama_val if not _isnan(kama_val) else False,
            "kst_bullish": kst_val > kst_sig if not _isnan(kst_val) else False,
            "rsi_not_overbought": 30 < rsi_val < 70 if not _isnan(rsi_val) else False,
            "money_flow_positive": money_flow > 0 if not _isnan(money_flow) else False,
            "ichimoku_bullish": ichi.get("signal") == "bullish",
        }
        confirmed = sum(1 for v in conditions.values() if v)

        if confirmed < 3:
            return None

        signal = Signal.STRONG_BUY if confirmed >= 4 else Signal.BUY
        score = SIGNAL_SCORE[signal]

        # Compute initial trailing stop and target
        if not _isnan(atr_val):
            stop = price - 3 * atr_val
            target = price + 5 * atr_val  # 5:3 reward-to-risk
        else:
            stop = price * 0.90  # 10% stop
            target = price * 1.15  # 15% target

        reason_parts = [f"{k}={'Y' if v else 'N'}" for k, v in conditions.items()]

        return {
            "symbol": symbol,
            "signal": signal,
            "score": score,
            "price": price,
            "strategy": self.name,
            "reason": f"conviction entry ({confirmed}/5): " + ", ".join(reason_parts),
            "atr": round(atr_val, 4) if not _isnan(atr_val) else None,
            "trailing_stop": round(stop, 2),
            "target": round(target, 2),
            "rsi": round(rsi_val, 2) if not _isnan(rsi_val) else None,
            "kama": round(kama_val, 2) if not _isnan(kama_val) else None,
            "er": round(er, 3) if not _isnan(er) else None,
            "money_flow": round(money_flow, 3) if not _isnan(money_flow) else None,
            "_requires_compliance": True,
        }

    def _check_exit(self, symbol: str) -> dict | None:
        """Check exit conditions for a held position."""
        pos = self._positions.get(symbol)
        if not pos:
            return None

        # ── Hold period check — HARD CONSTRAINT ──────────────────────
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if today < pos.hold_until:
            days_remaining = (
                datetime.fromisoformat(pos.hold_until) -
                datetime.fromisoformat(today)
            ).days
            logger.debug(
                "[conviction_hold] %s hold locked — %d days remaining (until %s)",
                symbol, days_remaining, pos.hold_until,
            )
            return None

        # ── Post-hold-period: check trailing stop and target ─────────
        df = self._fetch_ohlcv(symbol, days=50)
        if df is None:
            return None

        close = df["close"]
        high = df["high"]
        low = df["low"]
        price = float(close.iloc[-1])

        # Update peak and trailing stop
        if price > pos.peak_price:
            pos.peak_price = price
            atr_val = atr(high, low, close, 14)
            if not _isnan(atr_val):
                pos.trailing_stop = pos.peak_price - 3 * atr_val
            self._save_positions()

        pnl_pct = (price - pos.entry_price) / pos.entry_price * 100
        hold_days = (
            datetime.fromisoformat(today) -
            datetime.fromisoformat(pos.entry_date)
        ).days

        # Exit conditions (post hold period)
        if pos.target_price > 0 and price >= pos.target_price:
            return self._make_exit_signal(
                symbol, price, Signal.SELL,
                f"TARGET HIT: {pnl_pct:+.1f}% after {hold_days}d (target=${pos.target_price:.2f})",
            )

        if pos.trailing_stop > 0 and price <= pos.trailing_stop:
            return self._make_exit_signal(
                symbol, price, Signal.SELL,
                f"TRAILING STOP: {pnl_pct:+.1f}% after {hold_days}d (stop=${pos.trailing_stop:.2f}, peak=${pos.peak_price:.2f})",
            )

        # RSI divergence warning
        rsi_s = rsi_series(close, 14)
        div = detect_divergence(close, rsi_s, lookback=20)
        if div == "bearish":
            return self._make_exit_signal(
                symbol, price, Signal.SELL,
                f"BEARISH DIVERGENCE: {pnl_pct:+.1f}% after {hold_days}d — price rising but RSI falling",
            )

        return None

    def _make_exit_signal(self, symbol: str, price: float, signal: Signal, reason: str) -> dict:
        return {
            "symbol": symbol,
            "signal": signal,
            "score": SIGNAL_SCORE[signal],
            "price": price,
            "strategy": self.name,
            "reason": reason,
            "_conviction_exit": True,
        }

    # ── execute ──────────────────────────────────────────────────────────

    def execute(self, opportunity: dict) -> dict | None:
        signal = opportunity["signal"]
        symbol = opportunity["symbol"]
        price = opportunity.get("price", 0.0)

        account = self._account
        if account is None:
            return None

        if price <= 0:
            quote = self.client.get_quotes([symbol]).get(symbol)
            price = quote.last if quote else 0.0
        if price <= 0:
            return None

        # Exit path
        if opportunity.get("_conviction_exit"):
            return self._execute_sell(symbol, price, signal, account, opportunity)

        # Entry path — requires compliance
        if signal in (Signal.BUY, Signal.STRONG_BUY):
            if opportunity.get("_requires_compliance"):
                return self._execute_compliance_buy(symbol, price, signal, account, opportunity)

        return None

    def _execute_compliance_buy(
        self, symbol: str, price: float, signal: Signal,
        account: AccountSummary, opp: dict,
    ) -> dict | None:
        # Position sizing
        order_value = min(
            self.config.CONVICTION_MAX_POSITION,
            account.cash_available * 0.20,  # max 20% of cash per conviction name
        )
        if order_value < self.config.MIN_ORDER_VALUE:
            return None

        quantity = max(1, int(order_value / price))

        allowed, reason = self.risk.can_buy(symbol, quantity, price, account)
        if not allowed:
            logger.debug("[conviction_hold] BUY blocked for %s: %s", symbol, reason)
            return None

        if self._should_execute(opp):
            result = self.client.place_order(account.account_hash, symbol, "BUY", quantity)
            if result.get("status") != "ok":
                logger.error("[conviction_hold] BUY failed for %s: %s", symbol, result.get("error"))
                return None
        else:
            result = {"status": "dry_run", "order_id": "dry"}

        # Record the conviction position with hold period
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        hold_until = (datetime.now(timezone.utc) + timedelta(days=self.config.CONVICTION_HOLD_DAYS)).strftime("%Y-%m-%d")

        pos = ConvictionPosition(
            symbol=symbol,
            entry_price=price,
            quantity=quantity,
            entry_date=today,
            hold_until=hold_until,
            target_price=opp.get("target", 0),
            trailing_stop=opp.get("trailing_stop", 0),
            peak_price=price,
            notes=f"Signal: {signal.value}, {opp.get('reason', '')}",
        )
        self._positions[symbol] = pos
        self._save_positions()

        self.risk.record_trade(symbol, "BUY", quantity, price, strategy=self.name)
        trade = {
            "strategy": self.name,
            "symbol": symbol,
            "side": "BUY",
            "signal": signal.value,
            "quantity": quantity,
            "price": price,
            "value": quantity * price,
            "dry_run": not self._should_execute(opp),
            "hold_until": hold_until,
            "target": opp.get("target"),
            "trailing_stop": opp.get("trailing_stop"),
            **{k: v for k, v in opp.items() if not k.startswith("_")},
            **result,
        }
        logger.info(
            "[conviction_hold] BUY %d %s @ $%.2f — hold until %s (target=$%.2f, stop=$%.2f)",
            quantity, symbol, price, hold_until,
            opp.get("target", 0), opp.get("trailing_stop", 0),
        )
        return trade

    def _execute_sell(
        self, symbol: str, price: float, signal: Signal,
        account: AccountSummary, opp: dict,
    ) -> dict | None:
        pos = self._positions.get(symbol)
        if not pos:
            return None

        # Double-check hold period
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if today < pos.hold_until:
            logger.warning("[conviction_hold] BLOCKED sell of %s — hold period not expired", symbol)
            return None

        quantity = pos.quantity

        if self._should_execute(opp):
            result = self.client.place_order(account.account_hash, symbol, "SELL", quantity)
            if result.get("status") != "ok":
                logger.error("[conviction_hold] SELL failed for %s: %s", symbol, result.get("error"))
                return None
        else:
            result = {"status": "dry_run", "order_id": "dry"}

        pnl = (price - pos.entry_price) * quantity
        hold_days = (datetime.fromisoformat(today) - datetime.fromisoformat(pos.entry_date)).days

        self.session_pnl += pnl
        self.state.update_strategy_pnl(self.name, pnl, win=pnl > 0)
        self.risk.record_trade(symbol, "SELL", quantity, price, strategy=self.name)

        # Remove position
        del self._positions[symbol]
        self._save_positions()

        trade = {
            "strategy": self.name,
            "symbol": symbol,
            "side": "SELL",
            "signal": signal.value,
            "quantity": quantity,
            "price": price,
            "value": quantity * price,
            "realized_pnl": round(pnl, 4),
            "hold_days": hold_days,
            "entry_price": pos.entry_price,
            "dry_run": not self._should_execute(opp),
            **{k: v for k, v in opp.items() if not k.startswith("_")},
            **result,
        }
        logger.info(
            "[conviction_hold] SELL %d %s @ $%.2f pnl=$%.2f held=%dd reason=%s",
            quantity, symbol, price, pnl, hold_days, opp.get("reason", ""),
        )
        return trade

    # ── Monitoring ───────────────────────────────────────────────────────

    def _update_monitoring(self, universe: list[str]) -> None:
        """Update trailing stops for held positions."""
        for symbol in universe:
            pos = self._positions.get(symbol)
            if not pos:
                continue
            df = self._fetch_ohlcv(symbol, days=30)
            if df is None:
                continue
            price = float(df["close"].iloc[-1])
            if price > pos.peak_price:
                pos.peak_price = price
                atr_val = atr(df["high"], df["low"], df["close"], 14)
                if not _isnan(atr_val):
                    pos.trailing_stop = pos.peak_price - 3 * atr_val
        self._save_positions()

    def conviction_status(self) -> list[dict]:
        """Return status of all conviction positions."""
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        result = []
        for sym, pos in self._positions.items():
            days_held = (datetime.fromisoformat(today) - datetime.fromisoformat(pos.entry_date)).days
            days_remaining = max(0, (datetime.fromisoformat(pos.hold_until) - datetime.fromisoformat(today)).days)
            result.append({
                "symbol": sym,
                "entry_price": pos.entry_price,
                "quantity": pos.quantity,
                "entry_date": pos.entry_date,
                "hold_until": pos.hold_until,
                "days_held": days_held,
                "days_remaining": days_remaining,
                "locked": days_remaining > 0,
                "target": pos.target_price,
                "trailing_stop": round(pos.trailing_stop, 2),
                "peak": pos.peak_price,
            })
        return result

    # ── Persistence ──────────────────────────────────────────────────────

    def _save_positions(self) -> None:
        path = Path(self.config.STATE_DIR).expanduser() / "conviction_positions.json"
        data = {}
        for sym, pos in self._positions.items():
            data[sym] = {
                "symbol": pos.symbol, "entry_price": pos.entry_price,
                "quantity": pos.quantity, "entry_date": pos.entry_date,
                "hold_until": pos.hold_until, "target_price": pos.target_price,
                "trailing_stop": pos.trailing_stop, "peak_price": pos.peak_price,
                "compliance_approved": pos.compliance_approved,
                "compliance_id": pos.compliance_id, "notes": pos.notes,
            }
        path.write_text(json.dumps(data, indent=2))

    def _load_positions(self) -> None:
        path = Path(self.config.STATE_DIR).expanduser() / "conviction_positions.json"
        if not path.exists():
            return
        try:
            data = json.loads(path.read_text())
            for sym, d in data.items():
                self._positions[sym] = ConvictionPosition(**d)
            if self._positions:
                logger.info("[conviction_hold] Restored %d positions", len(self._positions))
        except Exception as e:
            logger.warning("Failed to load conviction positions: %s", e)


def _isnan(v) -> bool:
    import math
    try:
        return math.isnan(v)
    except (TypeError, ValueError):
        return True
