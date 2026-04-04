"""Composite strategy — averages signals from all three sub-strategies."""
from __future__ import annotations

import logging

from schwabagent.config import Config
from schwabagent.persistence import StateStore
from schwabagent.risk import RiskManager
from schwabagent.schwab_client import AccountSummary, SchwabClient
from schwabagent.strategies.base import SIGNAL_SCORE, Signal, Strategy
from schwabagent.strategies.mean_reversion import MeanReversionStrategy
from schwabagent.strategies.momentum import MomentumStrategy
from schwabagent.strategies.trend_following import TrendFollowingStrategy

logger = logging.getLogger(__name__)

# Score thresholds for composite signal
_STRONG_BUY_THRESH = 1.5
_BUY_THRESH = 0.5
_SELL_THRESH = -0.5
_STRONG_SELL_THRESH = -1.5


class CompositeStrategy(Strategy):
    """Run Momentum + MeanReversion + TrendFollowing, average their scores.

    Score mapping:
      STRONG_BUY = 2, BUY = 1, HOLD = 0, SELL = -1, STRONG_SELL = -2

    Composite thresholds (average score):
      >= 1.5  → STRONG_BUY
      >= 0.5  → BUY
      > -0.5  → HOLD
      > -1.5  → SELL
      <= -1.5 → STRONG_SELL
    """

    name = "composite"

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

        # Sub-strategies share the same client/config/risk/state
        self._momentum = MomentumStrategy(client, config, risk, state, account)
        self._mean_rev = MeanReversionStrategy(client, config, risk, state, account)
        self._trend = TrendFollowingStrategy(client, config, risk, state, account)

    def set_account(self, account: AccountSummary) -> None:
        self._account = account
        self._momentum.set_account(account)
        self._mean_rev.set_account(account)
        self._trend.set_account(account)

    # ── scan ─────────────────────────────────────────────────────────────────

    def scan(self) -> list[dict]:
        """Scan all symbols, collect sub-strategy signals, and average scores."""
        # Run each sub-strategy's scan
        mom_opps = {o["symbol"]: o for o in self._momentum.scan()}
        mr_opps = {o["symbol"]: o for o in self._mean_rev.scan()}
        tf_opps = {o["symbol"]: o for o in self._trend.scan()}

        # Merge all symbols seen by any strategy
        all_symbols = set(mom_opps) | set(mr_opps) | set(tf_opps)

        opportunities = []
        for symbol in all_symbols:
            scores = []
            sub_signals: dict[str, str] = {}

            for name, opps in [
                ("momentum", mom_opps),
                ("mean_reversion", mr_opps),
                ("trend_following", tf_opps),
            ]:
                if symbol in opps:
                    s = opps[symbol]["score"]
                    scores.append(s)
                    sub_signals[name] = opps[symbol]["signal"].value if isinstance(opps[symbol]["signal"], Signal) else opps[symbol]["signal"]

            if not scores:
                continue

            avg_score = sum(scores) / len(scores)
            signal = _score_to_signal(avg_score)

            # Use last known price from any sub-strategy
            price = (
                mom_opps.get(symbol, mr_opps.get(symbol, tf_opps.get(symbol, {})))
                .get("price", 0.0)
            )

            opportunities.append({
                "symbol": symbol,
                "signal": signal,
                "score": round(avg_score, 4),
                "price": price,
                "sub_signals": sub_signals,
                "sub_scores": {
                    "momentum": mom_opps[symbol]["score"] if symbol in mom_opps else None,
                    "mean_reversion": mr_opps[symbol]["score"] if symbol in mr_opps else None,
                    "trend_following": tf_opps[symbol]["score"] if symbol in tf_opps else None,
                },
                "strategy": self.name,
                "reason": (
                    f"avg_score={avg_score:.2f} from "
                    + ", ".join(f"{k}={v}" for k, v in sub_signals.items())
                    + f" → {signal.value}"
                ),
            })

        opportunities.sort(key=lambda o: abs(o["score"]), reverse=True)
        logger.info(
            "[composite] scan: %d symbols → %d opportunities",
            len(all_symbols), len(opportunities),
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
            logger.warning("[composite] no account set — skipping %s", symbol)
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
            logger.debug("[composite] BUY blocked for %s: %s", symbol, reason)
            return None

        if not self.config.DRY_RUN:
            result = self.client.place_order(account.account_hash, symbol, "BUY", quantity)
            if result.get("status") != "ok":
                logger.error("[composite] Order failed for %s: %s", symbol, result.get("error"))
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
            "dry_run": self.config.DRY_RUN,
            **opp,
            **result,
        }
        logger.info("[composite] BUY %d %s @ $%.2f (signal=%s score=%.2f)", quantity, symbol, price, signal.value, opp["score"])
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

        if not self.config.DRY_RUN:
            result = self.client.place_order(account.account_hash, symbol, "SELL", quantity)
            if result.get("status") != "ok":
                logger.error("[composite] SELL failed for %s: %s", symbol, result.get("error"))
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
            "dry_run": self.config.DRY_RUN,
            **opp,
            **result,
        }
        logger.info("[composite] SELL %d %s @ $%.2f pnl=$%.2f", quantity, symbol, price, pnl)
        return trade


def _score_to_signal(score: float) -> Signal:
    if score >= _STRONG_BUY_THRESH:
        return Signal.STRONG_BUY
    if score >= _BUY_THRESH:
        return Signal.BUY
    if score > _SELL_THRESH:
        return Signal.HOLD
    if score > _STRONG_SELL_THRESH:
        return Signal.SELL
    return Signal.STRONG_SELL
