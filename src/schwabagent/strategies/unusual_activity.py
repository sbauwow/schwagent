"""Unusual options activity scanner — flag contracts where volume dwarfs
open interest.

Scan-only strategy. For each watchlist symbol:
  1. Batch-fetch equity quotes (1 API call for all symbols) and skip any
     symbol whose total equity volume is below a configurable percentile
     of its 10-day average — if the stock itself is quiet, the options
     flow is unlikely to be meaningful.
  2. For survivors, fetch the full option chain (calls + puts in a single
     API call via get_option_chain_all).
  3. For each contract: compute volume / open_interest ratio.
  4. Filter: ratio must be >= threshold (default 5.0 = 500%).
  5. Apply minimum-volume floor to avoid noise from thin contracts.
  6. Rank descending by vol/OI ratio.

`execute()` is a no-op — this strategy never places orders. Opportunities
appear in `/scan` output, the `/unusual` Telegram command, and the
runner's normal scan logs.
"""
from __future__ import annotations

import logging

from schwabagent.config import Config
from schwabagent.persistence import StateStore
from schwabagent.risk import RiskManager
from schwabagent.schwab_client import OptionContract, SchwabClient
from schwabagent.strategies.base import Strategy

logger = logging.getLogger(__name__)


class UnusualActivityStrategy(Strategy):
    """Unusual options activity scanner. Scan-only: `execute()` returns None."""

    name = "unusual_activity"

    def __init__(
        self,
        client: SchwabClient,
        config: Config,
        risk: RiskManager,
        state: StateStore,
    ):
        super().__init__(client, config, risk, state)

    # ── scan ─────────────────────────────────────────────────────────────

    def scan(self) -> list[dict]:
        cfg = self.config
        universe = cfg.unusual_activity_symbols
        if not universe:
            return []

        # ── Pre-filter: 1 API call for all symbols ──────────────────────
        # Skip symbols where equity volume is below the configured
        # fraction of 10-day average. Quiet stock ≈ no unusual flow.
        active = self._prefilter(universe)
        logger.info(
            "[unusual_activity] pre-filter: %d/%d symbols active",
            len(active), len(universe),
        )
        if not active:
            return []

        # ── Chain scan: 1 API call per surviving symbol ─────────────────
        candidates: list[dict] = []
        for symbol in active:
            try:
                rows = self._scan_symbol(symbol)
            except Exception as e:
                logger.warning("[unusual_activity] %s failed: %s", symbol, e)
                continue
            candidates.extend(rows)

        # Rank by vol/OI ratio descending
        candidates.sort(key=lambda r: r["vol_oi_ratio"], reverse=True)
        top = candidates[: cfg.UNUSUAL_ACTIVITY_TOP_N]
        logger.info(
            "[unusual_activity] scan: %d symbols → %d hits (showing top %d)",
            len(active), len(candidates), len(top),
        )
        return top

    def _prefilter(self, universe: list[str]) -> list[str]:
        """Batch-quote the universe and keep only symbols with above-average
        equity volume today.  Costs 1 API call regardless of universe size.
        """
        quotes = self.client.get_quotes(universe)
        if not quotes:
            # Fail open — scan everything if quotes are unavailable
            logger.warning("[unusual_activity] pre-filter quotes failed, scanning all")
            return universe

        threshold = self.config.UNUSUAL_ACTIVITY_VOLUME_PREFILTER
        active: list[str] = []
        for sym in universe:
            q = quotes.get(sym)
            if q is None:
                continue
            # If no avg volume data, let the symbol through
            if q.avg_10d_volume <= 0:
                active.append(sym)
                continue
            if q.volume >= q.avg_10d_volume * threshold:
                active.append(sym)
        return active

    def _scan_symbol(self, symbol: str) -> list[dict]:
        """Fetch calls + puts in one API call and evaluate every contract."""
        cfg = self.config
        contracts = self.client.get_option_chain_all(
            symbol,
            dte_min=cfg.UNUSUAL_ACTIVITY_DTE_MIN,
            dte_max=cfg.UNUSUAL_ACTIVITY_DTE_MAX,
            strike_count=cfg.UNUSUAL_ACTIVITY_STRIKE_COUNT,
        )
        hits: list[dict] = []
        for c in contracts:
            row = self._evaluate_contract(c)
            if row is not None:
                hits.append(row)
        return hits

    def _evaluate_contract(self, c: OptionContract) -> dict | None:
        cfg = self.config

        # Need meaningful OI to compute a ratio
        if c.open_interest <= 0:
            return None
        # Floor on absolute volume to filter out noise
        if c.volume < cfg.UNUSUAL_ACTIVITY_MIN_VOLUME:
            return None

        ratio = c.volume / c.open_interest
        if ratio < cfg.UNUSUAL_ACTIVITY_MIN_RATIO:
            return None

        # Liquidity sanity: skip contracts with no market
        if c.bid <= 0 or c.ask <= 0:
            return None

        mid = (c.bid + c.ask) / 2
        spread_pct = ((c.ask - c.bid) / mid * 100) if mid > 0 else 999.0
        notional = c.volume * mid * 100  # approximate dollar volume

        reason = (
            f"{c.underlying} {c.expiration} ${c.strike:g} {c.side}  "
            f"vol={c.volume:,} OI={c.open_interest:,} "
            f"ratio={ratio:.1f}x  mid=${mid:.2f}  ~${notional:,.0f} notional  "
            f"DTE={c.dte}"
        )

        return {
            "strategy": self.name,
            "symbol": c.underlying,
            "signal": "BUY" if c.side == "CALL" else "SELL",
            "score": round(ratio, 2),
            "price": c.mark or mid,
            "strike": c.strike,
            "expiration": c.expiration,
            "dte": c.dte,
            "side": c.side,
            "option_symbol": c.symbol,
            "volume": c.volume,
            "open_interest": c.open_interest,
            "vol_oi_ratio": round(ratio, 2),
            "bid": c.bid,
            "ask": c.ask,
            "mid": round(mid, 2),
            "spread_pct": round(spread_pct, 2),
            "notional": round(notional, 2),
            "iv_pct": round(c.iv, 2),
            "delta": round(c.delta, 3),
            "reason": reason,
        }

    # ── execute ──────────────────────────────────────────────────────────

    def execute(self, opportunity: dict) -> dict | None:
        """Scan-only strategy — never places orders."""
        return None
