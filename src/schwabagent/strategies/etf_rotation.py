"""ETF Rotation Strategy — dual momentum with bear-market filter and optional LLM overlay.

Logic
-----
1. Fetch OHLCV for each ETF in the universe (default: SPY QQQ IWM EFA EEM TLT IEF HYG TIP GLD VNQ SHY).
2. Compute a weighted momentum score from 1-, 3-, 6-, and 12-month returns.
3. Bear-market filter: if SPY < SMA(200), skip risky assets and rotate to ETF_SAFE_HAVEN.
4. Rank ETFs by score. The top ETF_TOP_N are BUY candidates; everything else is HOLD or SELL.
5. If an ETF we currently hold drops out of the top tier, generate a SELL signal.
6. Optional LLM overlay: query Ollama for macro commentary and a confidence modifier
   that scales position size for top-ranked ETFs.

Scoring
-------
  score = 0.4 × ret_1m  + 0.2 × ret_3m  + 0.2 × ret_6m  + 0.2 × ret_12m

All returns are percentage returns (e.g. +5.2 for +5.2%). The weights emphasise
recent momentum while retaining medium-term context.
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass

import pandas as pd

from schwabagent.config import Config
from schwabagent.indicators import sma
from schwabagent.persistence import StateStore
from schwabagent.risk import RiskManager
from schwabagent.schwab_client import AccountSummary, SchwabClient
from schwabagent.strategies.base import SIGNAL_SCORE, Signal, Strategy

logger = logging.getLogger(__name__)

# Momentum period weights — must sum to 1.0
_PERIOD_WEIGHTS = {1: 0.4, 3: 0.2, 6: 0.2, 12: 0.2}

# Approximate trading days per calendar month
_TRADING_DAYS_PER_MONTH = 21

# Human-readable descriptions for the default ETF universe (used by LLM prompts)
_ETF_DESCRIPTIONS: dict[str, str] = {
    "SPY": "S&P 500 large-cap US equities",
    "QQQ": "Nasdaq-100 technology/growth",
    "IWM": "Russell 2000 small-cap US equities",
    "VTV": "Vanguard US value equities",
    "VUG": "Vanguard US growth equities",
    "EFA": "Developed-market international equities (ex-US/CA)",
    "EEM": "Emerging-market equities",
    "TLT": "20+ year US Treasury bonds",
    "IEF": "7-10 year US Treasury bonds",
    "HYG": "US high-yield (junk) corporate bonds",
    "TIP": "US Treasury inflation-protected securities",
    "GLD": "Gold bullion",
    "SLV": "Silver bullion",
    "VNQ": "US real estate investment trusts (REITs)",
    "USO": "Crude oil futures",
    "SHY": "1-3 year US Treasury bonds (near-cash safe haven)",
}


@dataclass
class ETFScore:
    symbol: str
    score: float
    ret_1m: float
    ret_3m: float
    ret_6m: float
    ret_12m: float
    rank: int = 0
    signal: Signal = Signal.HOLD
    llm_confidence: float = 1.0
    llm_commentary: str = ""


class ETFRotationStrategy(Strategy):
    """Rotate into top-momentum ETFs; exit when they fall out of the top tier.

    This is the primary strategy for long-term capital allocation. It runs
    on a slower cadence (daily is sufficient) and does not day-trade.
    """

    name = "etf_rotation"

    def __init__(
        self,
        client: SchwabClient,
        config: Config,
        risk: RiskManager,
        state: StateStore,
        account: AccountSummary | None = None,
        llm=None,  # OllamaClient | None
    ):
        super().__init__(client, config, risk, state)
        self._account = account
        self._llm = llm
        self._last_scores: list[ETFScore] = []

    def set_account(self, account: AccountSummary) -> None:
        self._account = account

    # ── scan ─────────────────────────────────────────────────────────────────

    def scan(self) -> list[dict]:
        """Score all ETFs in the universe and return ranked opportunities."""
        universe = self.config.etf_universe
        if not universe:
            logger.warning("[etf_rotation] ETF_UNIVERSE is empty")
            return []

        # Determine required lookback: max period in months + buffer
        max_months = max(self.config.etf_momentum_periods, default=12)
        days_needed = max_months * _TRADING_DAYS_PER_MONTH + 10

        # Bear-market filter — check SPY vs SMA(200)
        bear_market = self._is_bear_market()
        if bear_market:
            logger.info("[etf_rotation] Bear-market filter active (SPY < SMA200) — rotating to %s",
                        self.config.ETF_SAFE_HAVEN)

        # Score each ETF
        scores: list[ETFScore] = []
        for symbol in universe:
            score = self._score_etf(symbol, days_needed)
            if score is not None:
                scores.append(score)

        if not scores:
            logger.warning("[etf_rotation] No ETFs could be scored")
            return []

        # Rank by score descending
        scores.sort(key=lambda s: s.score, reverse=True)
        for i, s in enumerate(scores):
            s.rank = i + 1

        top_n = self.config.ETF_TOP_N
        safe_haven = self.config.ETF_SAFE_HAVEN.upper()

        # Assign signals
        for s in scores:
            if bear_market:
                # In bear market: only safe haven is BUY, everything else SELL/HOLD
                if s.symbol == safe_haven:
                    s.signal = Signal.STRONG_BUY
                elif self._holding(s.symbol):
                    s.signal = Signal.SELL
                else:
                    s.signal = Signal.HOLD
            else:
                if s.rank <= top_n:
                    s.signal = Signal.BUY if s.rank > 1 else Signal.STRONG_BUY
                elif self._holding(s.symbol) and s.rank > top_n + 2:
                    # Held but fallen well out of top tier → sell
                    s.signal = Signal.SELL
                else:
                    s.signal = Signal.HOLD

        # LLM overlay on top-N (if enabled and available)
        if self.config.LLM_ENABLED and self._llm is not None:
            for s in scores[:top_n]:
                if s.signal in (Signal.BUY, Signal.STRONG_BUY):
                    self._apply_llm(s)

        self._last_scores = scores

        # Convert to opportunity dicts (filter out HOLD with no position)
        opps = []
        for s in scores:
            if s.signal == Signal.HOLD and not self._holding(s.symbol):
                continue
            opps.append({
                "symbol": s.symbol,
                "signal": s.signal,
                "score": SIGNAL_SCORE[s.signal],
                "momentum_score": round(s.score, 4),
                "rank": s.rank,
                "ret_1m": round(s.ret_1m, 2),
                "ret_3m": round(s.ret_3m, 2),
                "ret_6m": round(s.ret_6m, 2),
                "ret_12m": round(s.ret_12m, 2),
                "llm_confidence": s.llm_confidence,
                "llm_commentary": s.llm_commentary,
                "strategy": self.name,
                "reason": self._reason(s, bear_market),
                "price": self._last_price(s.symbol),
            })

        opps.sort(key=lambda o: abs(o["score"]), reverse=True)
        logger.info(
            "[etf_rotation] scan: %d ETFs scored, top=%s, bear=%s",
            len(scores),
            ", ".join(f"{s.symbol}({s.score:+.2f})" for s in scores[:top_n]),
            bear_market,
        )
        return opps

    # ── execute ───────────────────────────────────────────────────────────────

    def execute(self, opportunity: dict) -> dict | None:
        signal = opportunity["signal"]
        symbol = opportunity["symbol"]
        price = opportunity.get("price", 0.0)

        account = self._account
        if account is None:
            logger.warning("[etf_rotation] no account set — skipping %s", symbol)
            return None

        if price <= 0:
            quote = self.client.get_quotes([symbol]).get(symbol)
            price = quote.last if quote else 0.0
        if price <= 0:
            logger.warning("[etf_rotation] could not get price for %s", symbol)
            return None

        if signal in (Signal.STRONG_BUY, Signal.BUY):
            return self._buy(symbol, price, signal, account, opportunity)
        if signal in (Signal.SELL, Signal.STRONG_SELL):
            return self._sell(symbol, price, signal, account, opportunity)
        return None

    # ── helpers ───────────────────────────────────────────────────────────────

    def _score_etf(self, symbol: str, days_needed: int) -> ETFScore | None:
        """Compute the weighted momentum score for one ETF."""
        df = self._fetch_ohlcv(symbol, days=days_needed)
        if df is None or len(df) < _TRADING_DAYS_PER_MONTH:
            return None

        close = df["close"]
        n = len(close)
        last = float(close.iloc[-1])

        def _ret(months: int) -> float:
            lookback = min(months * _TRADING_DAYS_PER_MONTH, n - 1)
            past = float(close.iloc[-lookback - 1])
            if past <= 0:
                return 0.0
            return (last / past - 1) * 100

        ret_map = {p: _ret(p) for p in self.config.etf_momentum_periods}

        # Weighted sum — use period_weights for configured periods, uniform fallback
        score = 0.0
        weights = _PERIOD_WEIGHTS
        total_w = sum(weights.get(p, 1.0) for p in ret_map)
        for period, ret in ret_map.items():
            w = weights.get(period, 1.0) / total_w
            score += w * ret

        return ETFScore(
            symbol=symbol,
            score=score,
            ret_1m=ret_map.get(1, 0.0),
            ret_3m=ret_map.get(3, 0.0),
            ret_6m=ret_map.get(6, 0.0),
            ret_12m=ret_map.get(12, 0.0),
        )

    def _is_bear_market(self) -> bool:
        """Return True if SPY is below its 200-day SMA."""
        if not self.config.ETF_BEAR_FILTER:
            return False
        df = self._fetch_ohlcv("SPY", days=220)
        if df is None or len(df) < 200:
            return False
        close = df["close"]
        sma200 = sma(close, 200)
        if math.isnan(sma200):
            return False
        return float(close.iloc[-1]) < sma200

    def _holding(self, symbol: str) -> bool:
        """Return True if the account currently holds shares of *symbol*."""
        if self._account is None:
            return False
        return any(p.symbol == symbol and p.quantity > 0 for p in self._account.positions)

    def _last_price(self, symbol: str) -> float:
        """Return the last close price from cached OHLCV, or 0 if not available."""
        if self._account:
            pos = next((p for p in self._account.positions if p.symbol == symbol), None)
            if pos and pos.market_value > 0 and pos.quantity > 0:
                return pos.market_value / pos.quantity
        return 0.0

    def _apply_llm(self, s: ETFScore) -> None:
        """Query the LLM and update confidence + commentary on the ETFScore in place."""
        try:
            result = self._llm.etf_commentary(
                symbol=s.symbol,
                description=_ETF_DESCRIPTIONS.get(s.symbol, s.symbol),
                momentum_rank=s.rank,
                universe_size=len(self.config.etf_universe),
                return_12m=s.ret_12m,
                return_1m=s.ret_1m,
                signal=s.signal.value,
            )
            s.llm_confidence = result.get("confidence", 0.7)
            s.llm_commentary = result.get("commentary", "")
            logger.debug("[etf_rotation] LLM %s confidence=%.2f: %s",
                         s.symbol, s.llm_confidence, s.llm_commentary)
        except Exception as e:
            logger.warning("[etf_rotation] LLM overlay failed for %s: %s", s.symbol, e)

    def _buy(self, symbol: str, price: float, signal: Signal, account: AccountSummary, opp: dict) -> dict | None:
        # Position size: equal-weight allocation across top_n positions
        # Size = portfolio_value / top_n, clipped to MAX_ORDER_VALUE and available cash
        top_n = self.config.ETF_TOP_N
        target_value = min(
            account.total_value / top_n,
            self.config.MAX_ORDER_VALUE,
            account.cash_available * 0.95,
        )
        # Scale by LLM confidence if enabled
        if self.config.LLM_ENABLED and opp.get("llm_confidence", 1.0) < 1.0:
            target_value *= opp["llm_confidence"]

        if target_value < self.config.MIN_ORDER_VALUE:
            return None

        quantity = max(1, int(target_value / price))
        allowed, reason = self.risk.can_buy(symbol, quantity, price, account)
        if not allowed:
            logger.debug("[etf_rotation] BUY blocked for %s: %s", symbol, reason)
            return None

        if self._should_execute(opp):
            result = self.client.place_order(account.account_hash, symbol, "BUY", quantity)
            if result.get("status") != "ok":
                logger.error("[etf_rotation] BUY order failed for %s: %s", symbol, result.get("error"))
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
        logger.info("[etf_rotation] BUY %d %s @ $%.2f rank=%d score=%+.2f",
                    quantity, symbol, price, opp.get("rank", 0), opp.get("momentum_score", 0))
        return trade

    def _sell(self, symbol: str, price: float, signal: Signal, account: AccountSummary, opp: dict) -> dict | None:
        held = next((p for p in account.positions if p.symbol == symbol and p.quantity > 0), None)
        if held is None:
            return None

        quantity = int(held.quantity)
        if quantity <= 0:
            return None

        if self._should_execute(opp):
            result = self.client.place_order(account.account_hash, symbol, "SELL", quantity)
            if result.get("status") != "ok":
                logger.error("[etf_rotation] SELL order failed for %s: %s", symbol, result.get("error"))
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
        logger.info("[etf_rotation] SELL %d %s @ $%.2f pnl=$%.2f rank=%d",
                    quantity, symbol, price, pnl, opp.get("rank", 0))
        return trade

    @staticmethod
    def _reason(s: ETFScore, bear_market: bool) -> str:
        mode = "BEAR MARKET → safe haven" if bear_market else f"rank={s.rank}"
        parts = [
            f"{mode}",
            f"score={s.score:+.2f}",
            f"1m={s.ret_1m:+.1f}%",
            f"3m={s.ret_3m:+.1f}%",
            f"6m={s.ret_6m:+.1f}%",
            f"12m={s.ret_12m:+.1f}%",
        ]
        if s.llm_commentary:
            parts.append(f'llm="{s.llm_commentary}"')
        return "  ".join(parts)

    def scores_table(self) -> list[dict]:
        """Return the last computed scores as a list of dicts (for display)."""
        return [
            {
                "rank": s.rank,
                "symbol": s.symbol,
                "score": round(s.score, 3),
                "signal": s.signal.value,
                "1m": f"{s.ret_1m:+.1f}%",
                "3m": f"{s.ret_3m:+.1f}%",
                "6m": f"{s.ret_6m:+.1f}%",
                "12m": f"{s.ret_12m:+.1f}%",
                "llm": s.llm_commentary[:60] if s.llm_commentary else "",
            }
            for s in self._last_scores
        ]
