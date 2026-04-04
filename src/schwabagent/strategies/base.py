"""Base strategy interface and Signal enum."""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from enum import Enum

import pandas as pd

from schwabagent.config import Config
from schwabagent.persistence import StateStore
from schwabagent.risk import RiskManager
from schwabagent.schwab_client import SchwabClient

logger = logging.getLogger(__name__)


class Signal(str, Enum):
    STRONG_BUY = "STRONG_BUY"
    BUY = "BUY"
    HOLD = "HOLD"
    SELL = "SELL"
    STRONG_SELL = "STRONG_SELL"


SIGNAL_SCORE: dict[Signal, float] = {
    Signal.STRONG_BUY: 2.0,
    Signal.BUY: 1.0,
    Signal.HOLD: 0.0,
    Signal.SELL: -1.0,
    Signal.STRONG_SELL: -2.0,
}


class Strategy(ABC):
    """Abstract base for all trading strategies."""

    name: str = "base"

    def __init__(
        self,
        client: SchwabClient,
        config: Config,
        risk: RiskManager,
        state: StateStore,
    ):
        self.client = client
        self.config = config
        self.risk = risk
        self.state = state
        self.trades_executed: int = 0
        self.session_pnl: float = 0.0

    @abstractmethod
    def scan(self) -> list[dict]:
        """Scan watchlist for opportunities.

        Returns a list of opportunity dicts, each containing at minimum:
          symbol, signal, score, price, reason
        """
        ...

    @abstractmethod
    def execute(self, opportunity: dict) -> dict | None:
        """Act on a single opportunity. Returns a trade result dict or None."""
        ...

    @property
    def is_live(self) -> bool:
        """Whether this strategy is enabled for live trading."""
        return self.config.is_strategy_live(self.name)

    def run_once(self) -> list[dict]:
        """Run one scan→execute cycle. Returns list of executed trade results."""
        if self.risk.is_killed():
            return []

        try:
            opportunities = self.scan()
        except Exception as e:
            logger.error("[%s] scan() failed: %s", self.name, e)
            return []

        if not self.is_live and not self.config.DRY_RUN:
            logger.info("[%s] live trading disabled for this strategy — dry-run only", self.name)

        results = []
        for opp in opportunities:
            if self.risk.is_killed():
                break
            try:
                opp["_force_dry_run"] = not self.is_live
                result = self.execute(opp)
                if result:
                    self.trades_executed += 1
                    results.append(result)
            except Exception as e:
                logger.error("[%s] execute(%s) failed: %s", self.name, opp.get("symbol"), e)

        return results

    def stats(self) -> dict:
        """Return session and lifetime stats for this strategy."""
        persisted = self.state.get_strategy_pnl().get(self.name, {})
        trades_total = persisted.get("trades", 0)
        wins = persisted.get("wins", 0)
        return {
            "strategy": self.name,
            "trades_session": self.trades_executed,
            "trades_total": trades_total,
            "pnl_session": round(self.session_pnl, 4),
            "pnl_realized": round(persisted.get("realized_pnl", 0.0), 4),
            "wins": wins,
            "losses": persisted.get("losses", 0),
            "win_rate": round(wins / trades_total * 100, 1) if trades_total > 0 else 0.0,
        }

    # ── Shared helpers ────────────────────────────────────────────────────────

    def _should_execute(self, opportunity: dict | None = None) -> bool:
        """Return True if this strategy should place real orders.

        Checks both the global DRY_RUN flag and the per-strategy live toggle.
        """
        if self.config.DRY_RUN:
            return False
        if opportunity and opportunity.get("_force_dry_run"):
            return False
        return self.is_live

    def _fetch_ohlcv(self, symbol: str, days: int = 100) -> pd.DataFrame | None:
        """Fetch OHLCV, logging a warning if insufficient data is returned."""
        df = self.client.get_ohlcv(symbol, days=days)
        if df.empty or len(df) < 30:
            logger.warning("[%s] insufficient data for %s (%d bars)", self.name, symbol, len(df))
            return None
        return df

    def _signal_from_score(self, score: float) -> Signal:
        if score >= 1.5:
            return Signal.STRONG_BUY
        if score >= 0.5:
            return Signal.BUY
        if score > -0.5:
            return Signal.HOLD
        if score > -1.5:
            return Signal.SELL
        return Signal.STRONG_SELL
