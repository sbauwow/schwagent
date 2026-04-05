"""Mean reversion strategy — Bollinger Bands + RSI + z-score."""
from __future__ import annotations

import logging
import math

from schwabagent.config import Config
from schwabagent.indicators import bollinger_bands, rsi, zscore
from schwabagent.persistence import StateStore
from schwabagent.risk import RiskManager
from schwabagent.schwab_client import AccountSummary, SchwabClient
from schwabagent.strategies.base import SIGNAL_SCORE, Signal, Strategy

logger = logging.getLogger(__name__)


class MeanReversionStrategy(Strategy):
    """Buy at lower Bollinger Band when oversold; sell at upper when overbought.

    Signal rules:
      STRONG_BUY  — price < lower_band AND RSI < 30
      BUY         — price < lower_band OR RSI < 35
      SELL        — price > upper_band OR RSI > 65
      STRONG_SELL — price > upper_band AND RSI > 70
      HOLD        — everything else
    """

    name = "mean_reversion"

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
        universe = self.config.mean_reversion_symbols
        quotes = self.client.get_quotes(universe)

        for symbol in universe:
            df = self._fetch_ohlcv(symbol)
            if df is None:
                continue

            close = df["close"]
            price = float(close.iloc[-1])

            upper, middle, lower = bollinger_bands(close, 20, 2.0)
            rsi_val = rsi(close, 14)
            z = zscore(close, 20)

            if any(math.isnan(v) for v in [upper, middle, lower, rsi_val, z]):
                continue

            signal = self._classify(price, upper, lower, rsi_val)
            score = SIGNAL_SCORE[signal]
            quote = quotes.get(symbol)

            opportunities.append({
                "symbol": symbol,
                "signal": signal,
                "score": score,
                "price": quote.last if quote else price,
                "bb_upper": round(upper, 4),
                "bb_middle": round(middle, 4),
                "bb_lower": round(lower, 4),
                "rsi": round(rsi_val, 2),
                "zscore": round(z, 4),
                "strategy": self.name,
                "reason": self._reason(signal, price, upper, lower, rsi_val, z),
            })

        opportunities.sort(key=lambda o: abs(o["score"]), reverse=True)
        logger.info(
            "[mean_reversion] scan: %d symbols → %d opportunities",
            len(universe), len(opportunities),
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
            logger.warning("[mean_reversion] no account set — skipping %s", symbol)
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
            logger.debug("[mean_reversion] BUY blocked for %s: %s", symbol, reason)
            return None

        if self._should_execute(opp):
            result = self.client.place_order(account.account_hash, symbol, "BUY", quantity)
            if result.get("status") != "ok":
                logger.error("[mean_reversion] Order failed for %s: %s", symbol, result.get("error"))
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
        logger.info("[mean_reversion] BUY %d %s @ $%.2f (signal=%s)", quantity, symbol, price, signal.value)
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
                logger.error("[mean_reversion] SELL failed for %s: %s", symbol, result.get("error"))
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
        logger.info("[mean_reversion] SELL %d %s @ $%.2f pnl=$%.2f", quantity, symbol, price, pnl)
        return trade

    # ── classification ────────────────────────────────────────────────────────

    def _classify(
        self,
        price: float,
        upper: float,
        lower: float,
        rsi_val: float,
    ) -> Signal:
        # STRONG_BUY: price below lower band AND oversold
        if price < lower and rsi_val < 30:
            return Signal.STRONG_BUY
        # BUY: price below lower band OR moderately oversold
        if price < lower or rsi_val < 35:
            return Signal.BUY
        # STRONG_SELL: price above upper band AND overbought
        if price > upper and rsi_val > 70:
            return Signal.STRONG_SELL
        # SELL: price above upper band OR moderately overbought
        if price > upper or rsi_val > 65:
            return Signal.SELL
        return Signal.HOLD

    def _reason(
        self,
        signal: Signal,
        price: float,
        upper: float,
        lower: float,
        rsi_val: float,
        z: float,
    ) -> str:
        return (
            f"price={price:.2f} BB_upper={upper:.2f} BB_lower={lower:.2f} "
            f"RSI={rsi_val:.1f} z={z:.2f} → {signal.value}"
        )
