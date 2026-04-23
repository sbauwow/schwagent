"""Cheap-gamma scanner — surface long-straddle candidates where priced vol
is below recent realized vol.

Scan-only strategy. For each watchlist symbol:
  1. Compute 20-day close-to-close realized vol (annualized %).
  2. Fetch ATM straddles in the DTE window (21-45 by default).
  3. For each straddle: IV = avg(call IV, put IV), ratio = IV / RV.
  4. Filter: IV/RV below GAMMA_SCANNER_MAX_RATIO (default 1.0 — implied
     cheaper than realized), liquidity + open-interest floors, sane
     greeks.
  5. Rank ascending by IV/RV, tiebreak descending by gamma per dollar.

`execute()` is a no-op — this strategy never places orders. Opportunities
appear in `/scan` output and in the runner's normal scan logs.
"""
from __future__ import annotations

import logging
import math

import numpy as np

from schwabagent.config import Config
from schwabagent.persistence import StateStore
from schwabagent.risk import RiskManager
from schwabagent.schwab_client import SchwabClient, Straddle
from schwabagent.strategies.base import Strategy

logger = logging.getLogger(__name__)

_TRADING_DAYS_PER_YEAR = 252


class GammaScannerStrategy(Strategy):
    """Long-gamma scanner. Scan-only: `execute()` returns None."""

    name = "gamma_scanner"

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
        universe = cfg.gamma_scanner_symbols
        if not universe:
            return []

        candidates: list[dict] = []
        for symbol in universe:
            try:
                row = self._scan_symbol(symbol)
            except Exception as e:
                logger.warning("[gamma_scanner] %s failed: %s", symbol, e)
                continue
            if row is not None:
                candidates.append(row)

        # Rank: primary IV/RV asc, tiebreak γ/$ desc
        candidates.sort(
            key=lambda r: (r["iv_rv_ratio"], -r["gamma_per_dollar"]),
        )
        logger.info(
            "[gamma_scanner] scan: %d symbols → %d cheap candidates",
            len(universe), len(candidates),
        )
        return candidates

    def _scan_symbol(self, symbol: str) -> dict | None:
        cfg = self.config
        rv = self._realized_vol(symbol, cfg.GAMMA_SCANNER_RV_WINDOW)
        if rv is None or rv <= 0:
            return None

        straddles = self.client.get_atm_straddles(
            symbol, cfg.GAMMA_SCANNER_DTE_MIN, cfg.GAMMA_SCANNER_DTE_MAX,
        )
        if not straddles:
            return None

        # Score each straddle, keep the cheapest (lowest IV/RV)
        best: dict | None = None
        for s in straddles:
            row = self._score_straddle(s, rv)
            if row is None:
                continue
            if best is None or row["iv_rv_ratio"] < best["iv_rv_ratio"]:
                best = row
        return best

    def _realized_vol(self, symbol: str, window: int) -> float | None:
        """Annualized close-to-close realized vol in percent."""
        df = self.client.get_ohlcv(symbol, days=max(window + 10, 60))
        if df is None or df.empty or len(df) < window + 1:
            return None
        closes = df["close"].astype(float).values[-(window + 1):]
        if any(c <= 0 for c in closes):
            return None
        log_returns = np.diff(np.log(closes))
        if len(log_returns) < window:
            return None
        sigma = float(np.std(log_returns, ddof=1))
        return sigma * math.sqrt(_TRADING_DAYS_PER_YEAR) * 100.0

    def _score_straddle(self, s: Straddle, rv: float) -> dict | None:
        cfg = self.config
        # Liquidity + sanity gates
        if (
            s.call.bid <= 0 or s.put.bid <= 0
            or s.call.ask <= 0 or s.put.ask <= 0
            or s.call.open_interest < cfg.GAMMA_SCANNER_MIN_OI
            or s.put.open_interest < cfg.GAMMA_SCANNER_MIN_OI
        ):
            return None
        if s.gamma <= 0 or s.cost <= 0:
            return None

        iv = s.iv
        if iv <= 0:
            return None
        ratio = iv / rv
        if ratio > cfg.GAMMA_SCANNER_MAX_RATIO:
            return None

        cost = s.cost  # per-share (×100 per contract)
        premium_dollars = cost * 100  # per 1 contract
        break_up = s.strike + cost
        break_dn = s.strike - cost
        delta_sum = s.call.delta + s.put.delta  # near-zero for ATM

        reason = (
            f"{s.underlying} {s.expiration} {s.strike:g}-straddle  "
            f"IV={iv:.1f}% RV={rv:.1f}% ratio={ratio:.2f}  "
            f"cost=${cost:.2f} γ/$={s.gamma_per_dollar:.5f}  DTE={s.dte}"
        )

        return {
            "strategy": self.name,
            "symbol": s.underlying,
            "signal": "BUY",
            "score": -ratio,          # more negative = cheaper
            "price": s.underlying_price,
            "strike": s.strike,
            "expiration": s.expiration,
            "dte": s.dte,
            "straddle_cost_per_share": round(cost, 2),
            "straddle_cost_per_contract": round(premium_dollars, 2),
            "iv_pct": round(iv, 2),
            "rv_pct": round(rv, 2),
            "iv_rv_ratio": round(ratio, 3),
            "gamma": round(s.gamma, 5),
            "gamma_per_dollar": round(s.gamma_per_dollar, 6),
            "delta_sum": round(delta_sum, 3),
            "break_even_up": round(break_up, 2),
            "break_even_down": round(break_dn, 2),
            "call_symbol": s.call.symbol,
            "put_symbol": s.put.symbol,
            "call_ask": s.call.ask,
            "put_ask": s.put.ask,
            "reason": reason,
        }

    # ── execute ──────────────────────────────────────────────────────────

    def execute(self, opportunity: dict) -> dict | None:
        """Scan-only strategy — never places orders."""
        return None
