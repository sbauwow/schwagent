"""Trend following strategy — EMA alignment + ADX strength."""
from __future__ import annotations

import logging
import math

from schwabagent.config import Config
from schwabagent.indicators import adx, ema
from schwabagent.persistence import StateStore
from schwabagent.risk import RiskManager
from schwabagent.schwab_client import AccountSummary, SchwabClient
from schwabagent.strategies.base import SIGNAL_SCORE, Signal, Strategy

logger = logging.getLogger(__name__)


class TrendFollowingStrategy(Strategy):
    """Enter when EMA alignment confirms a trend and ADX validates its strength.

    Signal rules:
      STRONG_BUY  — EMA20 > EMA50 > EMA200 AND ADX > 25
      BUY         — EMA20 > EMA50 AND ADX > 20
      SELL        — EMA20 < EMA50
      STRONG_SELL — EMA20 < EMA50 < EMA200 AND ADX > 25
      HOLD        — everything else
    """

    name = "trend_following"

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
        self._account = account

    # ── scan ─────────────────────────────────────────────────────────────────

    def scan(self) -> list[dict]:
        opportunities = []
        quotes = self.client.get_quotes(self.config.watchlist)

        for symbol in self.config.watchlist:
            # Need at least 200+ bars for EMA200
            df = self._fetch_ohlcv(symbol, days=260)
            if df is None or len(df) < 60:
                continue

            close = df["close"]
            high = df["high"]
            low = df["low"]
            price = float(close.iloc[-1])

            ema20 = ema(close, 20)
            ema50 = ema(close, 50)
            # EMA200 only if we have enough bars
            ema200 = ema(close, 200) if len(close) >= 200 else float("nan")
            adx_val = adx(high, low, close, 14)

            if any(math.isnan(v) for v in [ema20, ema50, adx_val]):
                continue

            signal = self._classify(ema20, ema50, ema200, adx_val)
            score = SIGNAL_SCORE[signal]
            quote = quotes.get(symbol)

            opportunities.append({
                "symbol": symbol,
                "signal": signal,
                "score": score,
                "price": quote.last if quote else price,
                "ema20": round(ema20, 4),
                "ema50": round(ema50, 4),
                "ema200": round(ema200, 4) if not math.isnan(ema200) else None,
                "adx": round(adx_val, 2),
                "strategy": self.name,
                "reason": self._reason(signal, price, ema20, ema50, ema200, adx_val),
            })

        opportunities.sort(key=lambda o: abs(o["score"]), reverse=True)
        logger.info(
            "[trend_following] scan: %d symbols → %d opportunities",
            len(self.config.watchlist), len(opportunities),
        )
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
            logger.warning("[trend_following] no account set — skipping %s", symbol)
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
        order_value = min(self.config.MAX_ORDER_VALUE, account.cash_available * 0.95)
        if order_value < self.config.MIN_ORDER_VALUE:
            return None

        quantity = max(1, int(order_value / price))
        allowed, reason = self.risk.can_buy(symbol, quantity, price, account)
        if not allowed:
            logger.debug("[trend_following] BUY blocked for %s: %s", symbol, reason)
            return None

        if self._should_execute(opp):
            result = self.client.place_order(account.account_hash, symbol, "BUY", quantity)
            if result.get("status") != "ok":
                logger.error("[trend_following] Order failed for %s: %s", symbol, result.get("error"))
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
        logger.info("[trend_following] BUY %d %s @ $%.2f (signal=%s)", quantity, symbol, price, signal.value)
        return trade

    def _sell(
        self,
        symbol: str,
        price: float,
        signal: Signal,
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
                logger.error("[trend_following] SELL failed for %s: %s", symbol, result.get("error"))
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
        logger.info("[trend_following] SELL %d %s @ $%.2f pnl=$%.2f", quantity, symbol, price, pnl)
        return trade

    # ── classification ────────────────────────────────────────────────────────

    def _classify(
        self,
        ema20: float,
        ema50: float,
        ema200: float,
        adx_val: float,
    ) -> Signal:
        has_200 = not math.isnan(ema200)

        # STRONG_BUY: full bull alignment + strong trend
        if has_200 and ema20 > ema50 > ema200 and adx_val > 25:
            return Signal.STRONG_BUY
        # BUY: short-term bull with moderate trend
        if ema20 > ema50 and adx_val > 20:
            return Signal.BUY
        # STRONG_SELL: full bear alignment + strong trend
        if has_200 and ema20 < ema50 < ema200 and adx_val > 25:
            return Signal.STRONG_SELL
        # SELL: short-term bear
        if ema20 < ema50:
            return Signal.SELL
        return Signal.HOLD

    def _reason(
        self,
        signal: Signal,
        price: float,
        ema20: float,
        ema50: float,
        ema200: float,
        adx_val: float,
    ) -> str:
        ema200_str = f"{ema200:.2f}" if not math.isnan(ema200) else "N/A"
        return (
            f"price={price:.2f} EMA20={ema20:.2f} EMA50={ema50:.2f} "
            f"EMA200={ema200_str} ADX={adx_val:.1f} → {signal.value}"
        )
