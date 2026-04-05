"""Momentum strategy — SMA crossover + RSI + MACD."""
from __future__ import annotations

import logging
import math

from schwabagent.config import Config
from schwabagent.indicators import macd, rsi, sma
from schwabagent.persistence import StateStore
from schwabagent.risk import RiskManager
from schwabagent.schwab_client import AccountSummary, SchwabClient
from schwabagent.strategies.base import SIGNAL_SCORE, Signal, Strategy

logger = logging.getLogger(__name__)


class MomentumStrategy(Strategy):
    """Buy when price > SMA20 > SMA50 with confirming RSI and MACD.

    Signal rules:
      STRONG_BUY  — price > SMA20 > SMA50 AND RSI in [40, 70] AND MACD hist > 0
      BUY         — price > SMA50 AND RSI < 65
      SELL        — price < SMA20 AND RSI > 60
      STRONG_SELL — price < SMA50 AND MACD hist < 0 AND RSI > 60
      HOLD        — everything else
    """

    name = "momentum"

    def __init__(
        self,
        client: SchwabClient,
        config: Config,
        risk: RiskManager,
        state: StateStore,
        account: AccountSummary | None = None,
    ):
        super().__init__(client, config, risk, state)
        self._account = account  # injected by runner each cycle

    def set_account(self, account: AccountSummary) -> None:
        self._account = account

    # ── scan ─────────────────────────────────────────────────────────────────

    def scan(self) -> list[dict]:
        opportunities = []
        universe = self.config.momentum_symbols
        quotes = self.client.get_quotes(universe)

        for symbol in universe:
            df = self._fetch_ohlcv(symbol)
            if df is None:
                continue

            close = df["close"]
            price = float(close.iloc[-1])

            sma20 = sma(close, 20)
            sma50 = sma(close, 50)
            rsi_val = rsi(close, 14)
            _, _, macd_hist = macd(close)

            if any(math.isnan(v) for v in [sma20, sma50, rsi_val, macd_hist]):
                continue

            signal = self._classify(price, sma20, sma50, rsi_val, macd_hist)
            score = SIGNAL_SCORE[signal]
            quote = quotes.get(symbol)

            opportunities.append({
                "symbol": symbol,
                "signal": signal,
                "score": score,
                "price": quote.last if quote else price,
                "sma20": round(sma20, 4),
                "sma50": round(sma50, 4),
                "rsi": round(rsi_val, 2),
                "macd_hist": round(macd_hist, 4),
                "strategy": self.name,
                "reason": self._reason(signal, price, sma20, sma50, rsi_val, macd_hist),
            })

        # Sort by absolute score descending
        opportunities.sort(key=lambda o: abs(o["score"]), reverse=True)
        logger.info("[momentum] scan: %d symbols → %d opportunities", len(universe), len(opportunities))
        return opportunities

    # ── execute ───────────────────────────────────────────────────────────────

    def execute(self, opportunity: dict) -> dict | None:
        signal = opportunity["signal"]
        symbol = opportunity["symbol"]
        score = opportunity["score"]
        price = opportunity["price"]

        if abs(score) < self.config.MIN_SIGNAL_SCORE:
            return None

        account = self._account
        if account is None:
            logger.warning("[momentum] no account set — skipping %s", symbol)
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
        # Size: use MAX_ORDER_VALUE, clipped by cash
        order_value = min(self.config.MAX_ORDER_VALUE, account.cash_available * 0.95)
        if order_value < self.config.MIN_ORDER_VALUE:
            return None

        quantity = max(1, int(order_value / price))
        allowed, reason = self.risk.can_buy(symbol, quantity, price, account)
        if not allowed:
            logger.debug("[momentum] BUY blocked for %s: %s", symbol, reason)
            return None

        if self._should_execute(opp):
            result = self.client.place_order(account.account_hash, symbol, "BUY", quantity)
            if result.get("status") != "ok":
                logger.error("[momentum] Order failed for %s: %s", symbol, result.get("error"))
                return None
        else:
            result = {"status": "dry_run", "order_id": "dry"}

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
            **opp,
            **result,
        }
        logger.info("[momentum] BUY %d %s @ $%.2f (signal=%s)", quantity, symbol, price, signal.value)
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

        if self._should_execute(opp):
            result = self.client.place_order(account.account_hash, symbol, "SELL", quantity)
            if result.get("status") != "ok":
                logger.error("[momentum] SELL order failed for %s: %s", symbol, result.get("error"))
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
            "signal": signal.value,
            "quantity": quantity,
            "price": price,
            "value": quantity * price,
            "realized_pnl": round(pnl, 4),
            "dry_run": not self._should_execute(opp),
            **opp,
            **result,
        }
        logger.info("[momentum] SELL %d %s @ $%.2f pnl=$%.2f", quantity, symbol, price, pnl)
        return trade

    # ── classification helpers ────────────────────────────────────────────────

    def _classify(
        self,
        price: float,
        sma20: float,
        sma50: float,
        rsi_val: float,
        macd_hist: float,
    ) -> Signal:
        # STRONG_BUY: price > SMA20 > SMA50 AND RSI in [40,70] AND MACD positive
        if price > sma20 > sma50 and 40 <= rsi_val <= 70 and macd_hist > 0:
            return Signal.STRONG_BUY
        # BUY: price > SMA50 AND RSI < 65
        if price > sma50 and rsi_val < 65:
            return Signal.BUY
        # STRONG_SELL: price < SMA50 AND MACD negative AND RSI > 60
        if price < sma50 and macd_hist < 0 and rsi_val > 60:
            return Signal.STRONG_SELL
        # SELL: price < SMA20 AND RSI > 60
        if price < sma20 and rsi_val > 60:
            return Signal.SELL
        return Signal.HOLD

    def _reason(
        self,
        signal: Signal,
        price: float,
        sma20: float,
        sma50: float,
        rsi_val: float,
        macd_hist: float,
    ) -> str:
        return (
            f"price={price:.2f} SMA20={sma20:.2f} SMA50={sma50:.2f} "
            f"RSI={rsi_val:.1f} MACD_hist={macd_hist:.4f} → {signal.value}"
        )
