"""ETF Scalp Strategy — volume/price breakout with tight take-profit/stop-loss.

Designed for cash accounts with $200k+ capital. Targets +0.15% profit per
trade with a -0.10% stop loss (1.5:1 R:R). Uses 3-minute bars.

Entry requires confluence of:
  1. Volume spike: current bar volume > 2× the 20-bar average
  2. Price breakout: close > high of prior 3 bars
  3. Trend filter: EMA(9) > EMA(21) and price > VWAP

Exit conditions (first one hit wins):
  - Take profit: +0.15% from entry
  - Stop loss: -0.10% from entry
  - Time stop: 30 minutes elapsed
  - Session end: 15:45 ET — close all positions

Capital is split into tranches for settlement management in cash accounts.
After a sell, that tranche's capital is locked T+1.
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from datetime import datetime, time, timezone

import numpy as np
import pandas as pd

from schwabagent.config import Config
from schwabagent.persistence import StateStore
from schwabagent.risk import RiskManager
from schwabagent.schwab_client import AccountSummary, SchwabClient
from schwabagent.strategies.base import Signal, Strategy

logger = logging.getLogger(__name__)


@dataclass
class ScalpPosition:
    """Tracks an open scalp position."""
    symbol: str
    entry_price: float
    quantity: float
    entry_time: datetime
    tranche_id: int
    take_profit: float   # target price
    stop_loss: float     # stop price
    time_stop: datetime  # close by this time


@dataclass
class Tranche:
    """A capital tranche for settlement management."""
    id: int
    capital: float
    available: bool = True      # False if capital is locked (unsettled)
    locked_until: str = ""      # date string when capital frees up


class ETFScalpStrategy(Strategy):
    """Intraday ETF scalp with volume/price confluence entry."""

    name = "etf_scalp"

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
        self._open_positions: list[ScalpPosition] = []
        self._bar_cache: dict[str, pd.DataFrame] = {}
        self._tranches: list[Tranche] = []

    def set_account(self, account: AccountSummary) -> None:
        self._account = account
        if not self._tranches:
            self._init_tranches(account)

    def _init_tranches(self, account: AccountSummary) -> None:
        """Split available capital into tranches."""
        n = self.config.SCALP_TRANCHES
        settled = account.cash_available - account.unsettled_cash
        per_tranche = settled / n if n > 0 else settled
        self._tranches = [
            Tranche(id=i, capital=per_tranche) for i in range(n)
        ]
        logger.info(
            "[etf_scalp] Initialized %d tranches of $%.2f each (settled=$%.2f)",
            n, per_tranche, settled,
        )

    def _get_available_tranche(self) -> Tranche | None:
        """Return the first available (settled) tranche, or None."""
        for t in self._tranches:
            if t.available:
                return t
        return None

    # ── scan ─────────────────────────────────────────────────────────────────

    def scan(self) -> list[dict]:
        """Scan the scalp universe for entry signals on 3-min bars."""
        now_et = datetime.now(timezone.utc).astimezone(
            __import__("zoneinfo").ZoneInfo("US/Eastern")
        )

        # Check session bounds
        session_start = time(*map(int, self.config.SCALP_SESSION_START.split(":")))
        session_end = time(*map(int, self.config.SCALP_SESSION_END.split(":")))
        current_time = now_et.time()

        if current_time < session_start or current_time >= session_end:
            logger.debug("[etf_scalp] Outside session window (%s)", current_time)
            return self._check_exits_only()

        # Don't open more than max concurrent positions
        if len(self._open_positions) >= self.config.SCALP_MAX_POSITIONS:
            logger.debug("[etf_scalp] At max positions (%d)", len(self._open_positions))
            return self._check_exits_only()

        # Need an available tranche
        tranche = self._get_available_tranche()
        if tranche is None:
            logger.debug("[etf_scalp] No available tranches — all capital unsettled")
            return self._check_exits_only()

        universe = self.config.scalp_universe

        # Pre-fetch quotes for liquidity filtering
        quotes = self.client.get_quotes(universe) if universe else {}

        opportunities = []

        for symbol in universe:
            # Skip if already in a position for this symbol
            if any(p.symbol == symbol for p in self._open_positions):
                continue

            # ── Liquidity filter ──────────────────────────────────────────
            quote = quotes.get(symbol)
            if quote is not None:
                if quote.avg_10d_volume < self.config.SCALP_MIN_AVG_VOLUME:
                    logger.info(
                        "[etf_scalp] SKIP %s: avg volume %s < %s",
                        symbol, f"{quote.avg_10d_volume:,}",
                        f"{self.config.SCALP_MIN_AVG_VOLUME:,}",
                    )
                    continue
                if quote.spread_pct > self.config.SCALP_MAX_SPREAD_PCT:
                    logger.info(
                        "[etf_scalp] SKIP %s: spread %.4f%% > %.4f%%",
                        symbol, quote.spread_pct, self.config.SCALP_MAX_SPREAD_PCT,
                    )
                    continue

            signal = self._evaluate_entry(symbol)
            if signal is not None:
                # Attach liquidity data to signal output
                if quote is not None:
                    signal["avg_10d_volume"] = quote.avg_10d_volume
                    signal["spread_pct"] = round(quote.spread_pct, 4)
                    signal["spread"] = round(quote.spread, 4)
                opportunities.append(signal)

        # Add exit signals for open positions
        exits = self._check_exits()
        opportunities.extend(exits)

        opportunities.sort(key=lambda o: abs(o.get("score", 0)), reverse=True)
        return opportunities

    def _evaluate_entry(self, symbol: str) -> dict | None:
        """Evaluate whether a symbol has an entry signal."""
        interval = self.config.SCALP_INTERVAL_MINUTES
        df = self.client.get_intraday_ohlcv(symbol, interval_minutes=interval, days=2)

        if df.empty or len(df) < max(self.config.SCALP_EMA_SLOW + 5, 25):
            return None

        self._bar_cache[symbol] = df

        close = df["close"]
        high = df["high"]
        volume = df["volume"]
        n = len(df)

        current_close = float(close.iloc[-1])
        current_volume = int(volume.iloc[-1])

        # ── 1. Volume spike: current bar > 2× 20-bar average ────────────
        vol_avg_20 = float(volume.iloc[-21:-1].mean()) if n > 21 else float(volume.mean())
        if vol_avg_20 <= 0:
            return None
        vol_ratio = current_volume / vol_avg_20
        if vol_ratio < self.config.SCALP_VOLUME_SPIKE_MULT:
            return None

        # ── 2. Price breakout: close > high of prior N bars ─────────────
        lookback = self.config.SCALP_LOOKBACK_BARS
        if n < lookback + 1:
            return None
        prior_high = float(high.iloc[-(lookback + 1):-1].max())
        if current_close <= prior_high:
            return None

        # ── 3. Trend filter: EMA(fast) > EMA(slow) and price > VWAP ────
        ema_fast = float(close.ewm(span=self.config.SCALP_EMA_FAST, adjust=False).mean().iloc[-1])
        ema_slow = float(close.ewm(span=self.config.SCALP_EMA_SLOW, adjust=False).mean().iloc[-1])
        if ema_fast <= ema_slow:
            return None

        # VWAP calculation
        typical_price = (df["high"] + df["low"] + df["close"]) / 3
        # Filter to today only for VWAP
        today = df.index[-1].normalize()
        today_mask = df.index >= today
        if today_mask.sum() > 0:
            tp_today = typical_price[today_mask]
            vol_today = volume[today_mask]
            cumvol = vol_today.cumsum()
            vwap = float((tp_today * vol_today).cumsum().iloc[-1] / cumvol.iloc[-1]) if cumvol.iloc[-1] > 0 else current_close
        else:
            vwap = current_close

        if current_close <= vwap:
            return None

        # ── All three confirmed — generate BUY signal ───────────────────
        score = min(vol_ratio / self.config.SCALP_VOLUME_SPIKE_MULT, 3.0)  # 1.0-3.0 scale

        return {
            "symbol": symbol,
            "signal": Signal.BUY,
            "score": score,
            "price": current_close,
            "strategy": self.name,
            "reason": (
                f"vol={current_volume:,} ({vol_ratio:.1f}× avg)  "
                f"breakout={current_close:.2f}>{prior_high:.2f}  "
                f"EMA {ema_fast:.2f}/{ema_slow:.2f}  VWAP={vwap:.2f}"
            ),
            "vol_ratio": round(vol_ratio, 2),
            "vwap": round(vwap, 2),
            "ema_fast": round(ema_fast, 2),
            "ema_slow": round(ema_slow, 2),
        }

    # ── Exit management ──────────────────────────────────────────────────────

    def _check_exits(self) -> list[dict]:
        """Check all open positions for exit conditions."""
        exits = []
        now = datetime.now(timezone.utc)

        for pos in list(self._open_positions):
            quote = self.client.get_quotes([pos.symbol]).get(pos.symbol)
            if quote is None:
                continue

            current_price = quote.last
            pnl_pct = (current_price - pos.entry_price) / pos.entry_price * 100

            exit_reason = None
            exit_signal = Signal.SELL

            if current_price >= pos.take_profit:
                exit_reason = f"TAKE PROFIT: {pnl_pct:+.3f}% (target +{self.config.SCALP_TAKE_PROFIT_PCT}%)"
            elif current_price <= pos.stop_loss:
                exit_reason = f"STOP LOSS: {pnl_pct:+.3f}% (limit -{self.config.SCALP_STOP_LOSS_PCT}%)"
                exit_signal = Signal.STRONG_SELL
            elif now >= pos.time_stop:
                exit_reason = f"TIME STOP: {pnl_pct:+.3f}% after {self.config.SCALP_TIME_STOP_MINUTES}min"
            elif self._is_session_closing():
                exit_reason = f"SESSION END: {pnl_pct:+.3f}% — closing before end of day"

            if exit_reason:
                exits.append({
                    "symbol": pos.symbol,
                    "signal": exit_signal,
                    "score": -2.0 if exit_signal == Signal.STRONG_SELL else -1.0,
                    "price": current_price,
                    "strategy": self.name,
                    "reason": exit_reason,
                    "_scalp_exit": True,
                    "_scalp_position": pos,
                })

        return exits

    def _check_exits_only(self) -> list[dict]:
        """When outside session or at capacity, only check for exits."""
        if not self._open_positions:
            return []
        return self._check_exits()

    def _is_session_closing(self) -> bool:
        now_et = datetime.now(timezone.utc).astimezone(
            __import__("zoneinfo").ZoneInfo("US/Eastern")
        )
        session_end = time(*map(int, self.config.SCALP_SESSION_END.split(":")))
        return now_et.time() >= session_end

    # ── execute ──────────────────────────────────────────────────────────────

    def execute(self, opportunity: dict) -> dict | None:
        signal = opportunity["signal"]
        symbol = opportunity["symbol"]
        price = opportunity.get("price", 0.0)

        account = self._account
        if account is None:
            logger.warning("[etf_scalp] no account set — skipping %s", symbol)
            return None

        if price <= 0:
            quote = self.client.get_quotes([symbol]).get(symbol)
            price = quote.last if quote else 0.0
        if price <= 0:
            return None

        # Exit path
        if opportunity.get("_scalp_exit"):
            return self._execute_sell(symbol, price, signal, account, opportunity)

        # Entry path
        if signal in (Signal.BUY, Signal.STRONG_BUY):
            return self._execute_buy(symbol, price, signal, account, opportunity)

        return None

    def _execute_buy(
        self, symbol: str, price: float, signal: Signal,
        account: AccountSummary, opp: dict,
    ) -> dict | None:
        tranche = self._get_available_tranche()
        if tranche is None:
            return None

        if len(self._open_positions) >= self.config.SCALP_MAX_POSITIONS:
            return None

        # Size: use full tranche capital
        order_value = min(tranche.capital, account.cash_available * 0.95)
        if order_value < self.config.MIN_ORDER_VALUE:
            return None

        quantity = max(1, int(order_value / price))

        allowed, reason = self.risk.can_buy(symbol, quantity, price, account)
        if not allowed:
            logger.debug("[etf_scalp] BUY blocked for %s: %s", symbol, reason)
            return None

        if self._should_execute(opp):
            result = self.client.place_order(account.account_hash, symbol, "BUY", quantity)
            if result.get("status") != "ok":
                logger.error("[etf_scalp] BUY failed for %s: %s", symbol, result.get("error"))
                return None
        else:
            result = {"status": "dry_run", "order_id": "dry"}

        # Track the position
        now = datetime.now(timezone.utc)
        from datetime import timedelta
        pos = ScalpPosition(
            symbol=symbol,
            entry_price=price,
            quantity=quantity,
            entry_time=now,
            tranche_id=tranche.id,
            take_profit=price * (1 + self.config.SCALP_TAKE_PROFIT_PCT / 100),
            stop_loss=price * (1 - self.config.SCALP_STOP_LOSS_PCT / 100),
            time_stop=now + timedelta(minutes=self.config.SCALP_TIME_STOP_MINUTES),
        )
        self._open_positions.append(pos)

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
            "take_profit": round(pos.take_profit, 4),
            "stop_loss": round(pos.stop_loss, 4),
            "tranche": tranche.id,
            **{k: v for k, v in opp.items() if not k.startswith("_")},
            **result,
        }
        logger.info(
            "[etf_scalp] BUY %d %s @ $%.2f  TP=$%.2f  SL=$%.2f  tranche=%d  reason=%s",
            quantity, symbol, price, pos.take_profit, pos.stop_loss, tranche.id, opp.get("reason", ""),
        )
        return trade

    def _execute_sell(
        self, symbol: str, price: float, signal: Signal,
        account: AccountSummary, opp: dict,
    ) -> dict | None:
        pos: ScalpPosition | None = opp.get("_scalp_position")
        if pos is None:
            return None

        quantity = int(pos.quantity)
        if quantity <= 0:
            return None

        if self._should_execute(opp):
            result = self.client.place_order(account.account_hash, symbol, "SELL", quantity)
            if result.get("status") != "ok":
                logger.error("[etf_scalp] SELL failed for %s: %s", symbol, result.get("error"))
                return None
        else:
            result = {"status": "dry_run", "order_id": "dry"}

        pnl = (price - pos.entry_price) * quantity
        pnl_pct = (price - pos.entry_price) / pos.entry_price * 100
        self.session_pnl += pnl
        self.state.update_strategy_pnl(self.name, pnl, win=pnl > 0)
        self.risk.record_trade(symbol, "SELL", quantity, price, strategy=self.name)

        # Remove from open positions
        self._open_positions = [p for p in self._open_positions if p is not pos]

        # Lock the tranche (T+1 settlement)
        for t in self._tranches:
            if t.id == pos.tranche_id:
                t.available = False
                # Capital returns next business day
                from datetime import timedelta
                tomorrow = (datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%d")
                t.locked_until = tomorrow
                logger.info("[etf_scalp] Tranche %d locked until %s (T+1)", t.id, tomorrow)
                break

        hold_minutes = (datetime.now(timezone.utc) - pos.entry_time).total_seconds() / 60
        trade = {
            "strategy": self.name,
            "symbol": symbol,
            "side": "SELL",
            "signal": signal.value,
            "quantity": quantity,
            "price": price,
            "value": quantity * price,
            "realized_pnl": round(pnl, 4),
            "pnl_pct": round(pnl_pct, 4),
            "hold_minutes": round(hold_minutes, 1),
            "entry_price": pos.entry_price,
            "dry_run": not self._should_execute(opp),
            "tranche": pos.tranche_id,
            **{k: v for k, v in opp.items() if not k.startswith("_")},
            **result,
        }
        emoji = "WIN" if pnl > 0 else "LOSS"
        logger.info(
            "[etf_scalp] SELL %d %s @ $%.2f  pnl=$%.2f (%+.3f%%)  held=%.0fmin  %s  reason=%s",
            quantity, symbol, price, pnl, pnl_pct, hold_minutes, emoji, opp.get("reason", ""),
        )
        return trade

    # ── Status ───────────────────────────────────────────────────────────────

    def scalp_status(self) -> dict:
        """Return current scalp state for display."""
        available_tranches = sum(1 for t in self._tranches if t.available)
        return {
            "open_positions": len(self._open_positions),
            "max_positions": self.config.SCALP_MAX_POSITIONS,
            "tranches_available": available_tranches,
            "tranches_total": len(self._tranches),
            "positions": [
                {
                    "symbol": p.symbol,
                    "entry": p.entry_price,
                    "tp": round(p.take_profit, 2),
                    "sl": round(p.stop_loss, 2),
                    "tranche": p.tranche_id,
                    "age_min": round(
                        (datetime.now(timezone.utc) - p.entry_time).total_seconds() / 60, 1
                    ),
                }
                for p in self._open_positions
            ],
        }
