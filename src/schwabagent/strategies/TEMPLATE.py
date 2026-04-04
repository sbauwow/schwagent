"""
Strategy Template — copy this file to create a new strategy.

=== CHECKLIST ===

Before writing any code, answer these questions. They define the strategy
and determine what infrastructure it needs.

1. IDENTITY
   - Strategy name (snake_case, used in config/logs):
   - One-line description:

2. MARKET REGIME
   - Asset class: equities / ETFs / options / mixed
   - Universe: specific symbols or configurable list?
   - Any symbols to always exclude?

3. TIMEFRAME
   - Data frequency: daily / intraday (1m, 3m, 5m, 15m)
   - Holding period: seconds / minutes / hours / days / weeks
   - Session hours: regular only (9:30-16:00 ET) / extended / 24h

4. ENTRY SIGNAL
   - What triggers a BUY? (be specific — which indicators, thresholds)
   - How many conditions must confirm? (confluence required?)
   - Minimum signal strength to act?

5. EXIT SIGNAL
   - Take profit: fixed % / trailing / indicator-based
   - Stop loss: fixed % / ATR-based / indicator-based
   - Time stop: close after N minutes/bars if no exit hit?
   - Session stop: close all before market close?
   - Any other exit conditions?

6. POSITION SIZING
   - How much capital per trade? (fixed $ / % of portfolio / Kelly)
   - Max concurrent positions?
   - Scale in/out or all-at-once?

7. RISK CONSTRAINTS
   - Max loss per trade:
   - Max loss per day:
   - Max drawdown before halting:
   - Any correlation limits? (e.g., don't hold SPY and QQQ simultaneously)

8. ACCOUNT REQUIREMENTS
   - Account type: cash / margin / either
   - Minimum capital needed:
   - Settlement awareness needed? (cash accounts = T+1 lockup)
   - Separate account from other strategies? (multi-account)

9. DATA REQUIREMENTS
   - Daily OHLCV: how many days of history?
   - Intraday bars: what interval? how many days?
   - Live quotes: how frequently?
   - Fundamentals: any (P/E, dividend dates, earnings)?
   - Options chain data?

10. DEPENDENCIES
    - New indicators needed? (add to indicators.py)
    - New config fields needed? (add to config.py)
    - New client methods needed? (add to schwab_client.py)
    - LLM overlay? (optional Ollama integration)


=== WIRING CHECKLIST (after writing the strategy) ===

Files to update:
  [ ] config.py — add any new config fields + LIVE_<NAME> flag + _STRATEGY_LIVE_FLAGS entry
  [ ] runner.py — import strategy, add to _build_strategies()
  [ ] runner.py — if multi-account, update _inject_account()
  [ ] .env      — add config values + LIVE_<NAME>=false
  [ ] .env      — add strategy name to STRATEGIES=
  [ ] run.sh    — add LIVE_<NAME> to status display loop


=== CODE TEMPLATE ===
"""
from __future__ import annotations

import logging

import pandas as pd

from schwabagent.config import Config
from schwabagent.persistence import StateStore
from schwabagent.risk import RiskManager
from schwabagent.schwab_client import AccountSummary, SchwabClient
from schwabagent.strategies.base import SIGNAL_SCORE, Signal, Strategy

logger = logging.getLogger(__name__)


class MyStrategy(Strategy):
    """One-line description of what this strategy does."""

    # Must be unique, snake_case, matches config LIVE_<NAME> and STRATEGIES list
    name = "my_strategy"

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

    def set_account(self, account: AccountSummary) -> None:
        """Called by the runner before each cycle with the current account state."""
        self._account = account

    # ── scan ─────────────────────────────────────────────────────────────────

    def scan(self) -> list[dict]:
        """Scan the universe for entry/exit signals.

        Must return a list of opportunity dicts. Each dict needs at minimum:
            symbol:   str       — ticker
            signal:   Signal    — STRONG_BUY / BUY / HOLD / SELL / STRONG_SELL
            score:    float     — numeric strength (used for ranking/filtering)
            price:    float     — current price
            strategy: str       — self.name
            reason:   str       — human-readable explanation of why

        You can add any extra keys — they'll be passed through to execute()
        and included in the trade record.
        """
        # universe = self.config.watchlist  # or a custom config field
        # account = self._account

        opportunities = []

        # for symbol in universe:
        #     # 1. Fetch data
        #     df = self._fetch_ohlcv(symbol, days=100)  # daily bars
        #     # or: df = self.client.get_intraday_ohlcv(symbol, interval_minutes=3)
        #     if df is None:
        #         continue
        #
        #     # 2. Compute indicators
        #     close = df["close"]
        #     # ... your indicator logic here ...
        #
        #     # 3. Generate signal
        #     signal = Signal.HOLD
        #     score = 0.0
        #     reason = ""
        #     # ... your signal logic here ...
        #
        #     if signal == Signal.HOLD:
        #         continue
        #
        #     opportunities.append({
        #         "symbol": symbol,
        #         "signal": signal,
        #         "score": score,
        #         "price": float(close.iloc[-1]),
        #         "strategy": self.name,
        #         "reason": reason,
        #     })

        return opportunities

    # ── execute ──────────────────────────────────────────────────────────────

    def execute(self, opportunity: dict) -> dict | None:
        """Act on a single opportunity from scan().

        This is where you:
        - Determine position size
        - Check risk limits via self.risk.can_buy() / self.risk.can_sell()
        - Place the order (or dry-run) via self._should_execute()
        - Record the trade
        - Return a trade result dict (or None to skip)
        """
        signal = opportunity["signal"]
        symbol = opportunity["symbol"]
        price = opportunity.get("price", 0.0)

        account = self._account
        if account is None:
            logger.warning("[%s] no account set — skipping %s", self.name, symbol)
            return None

        # Get a live price if we don't have one
        if price <= 0:
            quote = self.client.get_quotes([symbol]).get(symbol)
            price = quote.last if quote else 0.0
        if price <= 0:
            return None

        if signal in (Signal.STRONG_BUY, Signal.BUY):
            return self._buy(symbol, price, signal, account, opportunity)
        if signal in (Signal.STRONG_SELL, Signal.SELL):
            return self._sell(symbol, price, signal, account, opportunity)
        return None

    def _buy(
        self,
        symbol: str,
        price: float,
        signal: Signal,
        account: AccountSummary,
        opp: dict,
    ) -> dict | None:
        # ── Position sizing ──────────────────────────────────────────────
        order_value = min(self.config.MAX_ORDER_VALUE, account.cash_available * 0.95)
        if order_value < self.config.MIN_ORDER_VALUE:
            return None

        quantity = max(1, int(order_value / price))

        # ── Risk check ───────────────────────────────────────────────────
        allowed, reason = self.risk.can_buy(symbol, quantity, price, account)
        if not allowed:
            logger.debug("[%s] BUY blocked for %s: %s", self.name, symbol, reason)
            return None

        # ── Execute or dry-run ───────────────────────────────────────────
        if self._should_execute(opp):
            result = self.client.place_order(account.account_hash, symbol, "BUY", quantity)
            if result.get("status") != "ok":
                logger.error("[%s] BUY failed for %s: %s", self.name, symbol, result.get("error"))
                return None
        else:
            result = {"status": "dry_run", "order_id": "dry"}

        # ── Record ───────────────────────────────────────────────────────
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
            # Include any extra fields from the opportunity
            **{k: v for k, v in opp.items() if not k.startswith("_")},
            **result,
        }
        logger.info("[%s] BUY %d %s @ $%.2f", self.name, quantity, symbol, price)
        return trade

    def _sell(
        self,
        symbol: str,
        price: float,
        signal: Signal,
        account: AccountSummary,
        opp: dict,
    ) -> dict | None:
        # Only sell if we hold this symbol
        held = next((p for p in account.positions if p.symbol == symbol and p.quantity > 0), None)
        if held is None:
            return None

        quantity = int(held.quantity)
        if quantity <= 0:
            return None

        # ── Execute or dry-run ───────────────────────────────────────────
        if self._should_execute(opp):
            result = self.client.place_order(account.account_hash, symbol, "SELL", quantity)
            if result.get("status") != "ok":
                logger.error("[%s] SELL failed for %s: %s", self.name, symbol, result.get("error"))
                return None
        else:
            result = {"status": "dry_run", "order_id": "dry"}

        # ── P&L + record ────────────────────────────────────────────────
        pnl = (price - held.avg_price) * quantity
        self.session_pnl += pnl
        self.state.update_strategy_pnl(self.name, pnl, win=pnl > 0)
        self.risk.record_trade(symbol, "SELL", quantity, price, strategy=self.name)

        trade = {
            "strategy": self.name,
            "symbol": symbol,
            "side": "SELL",
            "signal": signal.value,
            "quantity": quantity,
            "price": price,
            "value": quantity * price,
            "realized_pnl": round(pnl, 4),
            "dry_run": not self._should_execute(opp),
            **{k: v for k, v in opp.items() if not k.startswith("_")},
            **result,
        }
        logger.info("[%s] SELL %d %s @ $%.2f pnl=$%.2f", self.name, quantity, symbol, price, pnl)
        return trade
