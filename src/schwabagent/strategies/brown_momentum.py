"""Brown Momentum — Constance Brown's methodology distilled into one strategy.

Composes three pieces from `brown_indicators`:

1. **RSI range shift** (regime filter)
   - Bull regime: RSI holds above 40 → enable long setups
   - Bear regime: RSI holds below 60 → enable short/exit setups
   - Neutral: skip all entries — the signal is too ambiguous

2. **Composite Index divergence** (entry trigger)
   - In bull regime: classic bullish or hidden bullish divergence → BUY
   - In bear regime: classic bearish or hidden bearish divergence → SELL
   - Brown considers Composite Index divergences more reliable than
     price/RSI divergences because the Composite Index leads RSI itself.

3. **Derivative Oscillator** (trend confirmation)
   - Positive DO in a bull regime → stronger BUY signal
   - Negative DO in a bear regime → stronger SELL signal
   - Opposing DO demotes the signal back to HOLD (exit/skip)

Signal classification:
    STRONG_BUY   bull + bullish divergence + DO > 0
    BUY          bull + bullish divergence + DO ≤ 0
    HOLD         any combo not matching a directional signal
    SELL         bear + bearish divergence + DO ≥ 0
    STRONG_SELL  bear + bearish divergence + DO < 0

Execution follows the pattern of the existing equity strategies:
fixed order-value sizing, risk-check, dry-run by default,
per-strategy LIVE flag gate before any real order goes out.
"""
from __future__ import annotations

import logging
import math

from schwabagent.brown_indicators import (
    composite_index,
    derivative_oscillator,
    rsi_range_shift,
)
from schwabagent.config import Config
from schwabagent.indicators import detect_divergence
from schwabagent.persistence import StateStore
from schwabagent.risk import RiskManager
from schwabagent.schwab_client import AccountSummary, SchwabClient
from schwabagent.strategies.base import Signal, Strategy

logger = logging.getLogger(__name__)


class BrownMomentumStrategy(Strategy):
    """Constance Brown's range-shift + Composite Index + Derivative Oscillator combo."""

    name = "brown_momentum"

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
        opportunities: list[dict] = []
        universe = self.config.brown_momentum_symbols
        if not universe:
            return opportunities

        quotes = self.client.get_quotes(universe)

        for symbol in universe:
            df = self._fetch_ohlcv(symbol, days=200)
            if df is None:
                continue

            close = df["close"]
            if len(close) < 100:
                continue

            # Regime filter
            regime = rsi_range_shift(
                close,
                rsi_period=self.config.BROWN_RSI_PERIOD,
                lookback=self.config.BROWN_REGIME_LOOKBACK,
            )
            if regime.regime == "neutral":
                continue

            # Composite Index for divergence detection
            ci = composite_index(close)
            if ci.dropna().empty:
                continue

            # Divergence classification on price vs Composite Index
            div_kind = detect_divergence(
                close,
                ci,
                lookback=self.config.BROWN_DIVERGENCE_LOOKBACK,
            )
            if div_kind == "none":
                continue

            # Derivative Oscillator for trend confirmation
            do = derivative_oscillator(close)
            do_val = do.dropna().iloc[-1] if not do.dropna().empty else float("nan")
            if math.isnan(do_val):
                continue

            signal = self._classify(regime.regime, div_kind, do_val)
            if signal == Signal.HOLD:
                continue

            # Score = regime confidence × direction × DO-alignment bonus
            direction = 1.0 if signal in (Signal.BUY, Signal.STRONG_BUY) else -1.0
            strength_bonus = 0.5 if signal in (Signal.STRONG_BUY, Signal.STRONG_SELL) else 0.0
            score = direction * (regime.confidence + strength_bonus)

            quote = quotes.get(symbol)
            opportunities.append({
                "symbol": symbol,
                "signal": signal,
                "score": round(score, 3),
                "price": quote.last if quote else float(close.iloc[-1]),
                "regime": regime.regime,
                "regime_confidence": round(regime.confidence, 3),
                "rsi_floor": round(regime.floor, 2),
                "rsi_ceiling": round(regime.ceiling, 2),
                "rsi_current": round(regime.current, 2),
                "divergence": div_kind,
                "composite_index": round(float(ci.dropna().iloc[-1]), 2),
                "derivative_oscillator": round(float(do_val), 3),
                "strategy": self.name,
                "reason": self._format_reason(regime.regime, div_kind, do_val),
            })

        return opportunities

    def _classify(self, regime: str, divergence: str, do_val: float) -> Signal:
        """Map (regime, divergence, DO) → Signal.

        See the module docstring for the full classification table.
        """
        if regime == "bull":
            if divergence in ("bullish", "hidden_bullish"):
                return Signal.STRONG_BUY if do_val > 0 else Signal.BUY
            return Signal.HOLD
        if regime == "bear":
            if divergence in ("bearish", "hidden_bearish"):
                return Signal.STRONG_SELL if do_val < 0 else Signal.SELL
            return Signal.HOLD
        return Signal.HOLD

    def _format_reason(self, regime: str, divergence: str, do_val: float) -> str:
        do_dir = "+" if do_val >= 0 else "−"
        return f"regime={regime} div={divergence} DO={do_dir}{abs(do_val):.2f}"

    # ── execute ──────────────────────────────────────────────────────────────

    def execute(self, opportunity: dict) -> dict | None:
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
            "signal": signal.value,
            "quantity": quantity,
            "price": price,
            "value": quantity * price,
            "dry_run": not self._should_execute(opp),
            **{k: v for k, v in opp.items() if not k.startswith("_")},
            **result,
        }
        logger.info(
            "[%s] BUY %d %s @ $%.2f regime=%s div=%s",
            self.name, quantity, symbol, price,
            opp.get("regime"), opp.get("divergence"),
        )
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
            "signal": signal.value,
            "quantity": quantity,
            "price": price,
            "value": quantity * price,
            "realized_pnl": round(pnl, 4),
            "dry_run": not self._should_execute(opp),
            **{k: v for k, v in opp.items() if not k.startswith("_")},
            **result,
        }
        logger.info(
            "[%s] SELL %d %s @ $%.2f pnl=$%.2f regime=%s div=%s",
            self.name, quantity, symbol, price, pnl,
            opp.get("regime"), opp.get("divergence"),
        )
        return trade
