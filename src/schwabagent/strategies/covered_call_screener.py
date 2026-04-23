"""Covered call screener — dividend-paying stocks + calls >30 DTE.

Scan-only strategy. For each symbol in the configured universe:
  1. Fetch a live quote — need spot, next ex-dividend date, and the
     next cash dividend amount.
  2. Resolve an annual-dividend estimate, preferring the daily-cached
     `dividend_calendar.json` when the symbol is present (Nasdaq's
     `indicated_Annual_Dividend` is authoritative); fall back to
     `next_div_amount * 4` (quarterly assumption) from the Schwab quote.
  3. Skip non-dividend payers and symbols below the min yield floor.
  4. Fetch the call chain in the configured DTE window (default 30–60).
  5. Bucket the chain by expiry and, per bucket, pick the OTM strike
     nearest to `spot * (1 + TARGET_OTM_PCT/100)`.
  6. Apply liquidity gates (open interest, bid/ask spread).
  7. Compute static and if-called yields, annualize them, and add a
     dividend-capture bonus when `next_ex_date` falls inside the hold
     window (today → expiration).
  8. Rank by total annualized yield (if-called + dividend capture).

`execute()` is a no-op: schwagent has no multi-leg live-execution path
yet (see the wiki's "No live options execution" gap). The screener feeds
the normal scan / alert pipeline and surfaces opportunities for manual or
future-automated entry.
"""
from __future__ import annotations

import json
import logging
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from schwabagent.config import Config
from schwabagent.persistence import StateStore
from schwabagent.risk import RiskManager
from schwabagent.schwab_client import AccountSummary, OptionContract, Quote, SchwabClient
from schwabagent.strategies.base import Strategy

logger = logging.getLogger(__name__)

_ET = ZoneInfo("America/New_York")
_DIVIDEND_CACHE_NAME = "dividend_calendar.json"
_QUARTERLY_FREQ = 4  # fallback when only `next_div_amount` is known


class CoveredCallScreener(Strategy):
    """Dividend-stock covered-call screener with live buy-write execution.

    Gated by `LIVE_COVERED_CALL_SCREENER` — off by default (scan-only).
    When live, `execute()` submits a TRIGGER chain (equity BUY → option STO)
    via `SchwabClient.place_buy_write`.
    """

    name = "covered_call_screener"

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

    # ── scan ─────────────────────────────────────────────────────────────

    def scan(self) -> list[dict]:
        cfg = self.config
        universe = cfg.covered_call_symbols
        if not universe:
            logger.warning("[covered_call_screener] COVERED_CALL_SYMBOLS is empty")
            return []

        quotes = self.client.get_quotes(universe)
        div_index = self._load_dividend_index()

        candidates: list[dict] = []
        for symbol in universe:
            quote = quotes.get(symbol)
            if quote is None:
                continue
            try:
                row = self._scan_symbol(symbol, quote, div_index)
            except Exception as e:
                logger.warning("[covered_call_screener] %s failed: %s", symbol, e)
                continue
            if row is not None:
                candidates.append(row)

        candidates.sort(key=lambda r: r["total_annual_yield_pct"], reverse=True)
        logger.info(
            "[covered_call_screener] scan: %d symbols → %d candidates",
            len(universe), len(candidates),
        )
        top_n = cfg.COVERED_CALL_TOP_N
        return candidates[:top_n] if top_n > 0 else candidates

    # ── per-symbol pipeline ──────────────────────────────────────────────

    def _scan_symbol(
        self, symbol: str, quote: Quote, div_index: dict[str, dict],
    ) -> dict | None:
        cfg = self.config

        spot = self._pick_spot(quote)
        if spot <= 0:
            return None
        # Per-contract notional cap: 1 contract = 100 * spot shares of stock.
        # Filter early so ranking doesn't surface opportunities the account
        # can't size into.
        if cfg.COVERED_CALL_MAX_SPOT > 0 and spot > cfg.COVERED_CALL_MAX_SPOT:
            return None

        annual_div, ex_date, next_amount = self._resolve_dividend(
            symbol, quote, div_index,
        )
        if annual_div <= 0:
            return None

        div_yield_pct = annual_div / spot * 100.0
        if div_yield_pct < cfg.COVERED_CALL_MIN_DIV_YIELD_PCT:
            return None

        chain = self.client.get_option_chain(
            symbol=symbol,
            side="CALL",
            dte_min=cfg.COVERED_CALL_DTE_MIN,
            dte_max=cfg.COVERED_CALL_DTE_MAX,
            strike_count=30,
        )
        if not chain:
            return None

        target_strike = spot * (1.0 + cfg.COVERED_CALL_TARGET_OTM_PCT / 100.0)
        by_exp: dict[str, list[OptionContract]] = {}
        for c in chain:
            by_exp.setdefault(c.expiration, []).append(c)

        today = datetime.now(_ET).date()
        best: dict | None = None
        for expiration, contracts in by_exp.items():
            row = self._score_expiry(
                symbol=symbol,
                spot=spot,
                target_strike=target_strike,
                contracts=contracts,
                expiration=expiration,
                today=today,
                annual_div=annual_div,
                div_yield_pct=div_yield_pct,
                ex_date=ex_date,
                next_amount=next_amount,
            )
            if row is None:
                continue
            if row["total_annual_yield_pct"] < cfg.COVERED_CALL_MIN_ANNUAL_YIELD_PCT:
                continue
            if best is None or row["total_annual_yield_pct"] > best["total_annual_yield_pct"]:
                best = row
        return best

    def _score_expiry(
        self,
        *,
        symbol: str,
        spot: float,
        target_strike: float,
        contracts: list[OptionContract],
        expiration: str,
        today: date,
        annual_div: float,
        div_yield_pct: float,
        ex_date: str,
        next_amount: float,
    ) -> dict | None:
        cfg = self.config

        otm = [c for c in contracts if c.strike >= spot]
        if not otm:
            return None
        pick = min(otm, key=lambda c: abs(c.strike - target_strike))

        # Liquidity + sanity gates
        if pick.open_interest < cfg.COVERED_CALL_MIN_OI:
            return None
        if pick.bid <= 0 or pick.ask <= 0 or pick.ask < pick.bid:
            return None
        mid = (pick.bid + pick.ask) / 2.0
        if mid <= 0:
            return None
        spread_pct = (pick.ask - pick.bid) / mid * 100.0
        if spread_pct > cfg.COVERED_CALL_MAX_SPREAD_PCT:
            return None

        premium = mid
        dte = max(pick.dte, 1)

        static_return = premium / spot
        if_called_return = (pick.strike - spot + premium) / spot
        ann_factor = 365.0 / dte

        ann_static_pct = static_return * ann_factor * 100.0
        ann_if_called_pct = if_called_return * ann_factor * 100.0

        # Dividend capture: only if the next ex-date lands in the hold window.
        try:
            exp_date = date.fromisoformat(expiration)
        except ValueError:
            return None
        div_capture = 0.0
        div_in_hold = False
        if ex_date and next_amount > 0:
            try:
                ex = date.fromisoformat(ex_date)
                if today <= ex <= exp_date:
                    div_capture = next_amount
                    div_in_hold = True
            except ValueError:
                pass
        ann_div_capture_pct = (
            (div_capture / spot) * ann_factor * 100.0 if div_capture else 0.0
        )

        total_annual = ann_if_called_pct + ann_div_capture_pct
        downside_protection = premium / spot * 100.0
        breakeven = spot - premium
        otm_pct = (pick.strike - spot) / spot * 100.0

        reason = (
            f"{symbol} {expiration} ${pick.strike:g}C "
            f"prem=${premium:.2f} if-called={ann_if_called_pct:.1f}%/yr "
            f"div={div_yield_pct:.1f}% DTE={dte}"
            + (" +cap" if div_in_hold else "")
        )

        return {
            "strategy": self.name,
            "symbol": symbol,
            "signal": "BUY",
            "score": round(total_annual, 2),
            "price": round(spot, 2),
            "strike": pick.strike,
            "expiration": expiration,
            "dte": dte,
            "otm_pct": round(otm_pct, 2),
            "call_symbol": pick.symbol,
            "call_bid": pick.bid,
            "call_ask": pick.ask,
            "call_premium": round(premium, 4),
            "call_delta": round(pick.delta, 4),
            "call_iv_pct": round(pick.iv, 2),
            "call_oi": pick.open_interest,
            "spread_pct": round(spread_pct, 2),
            "static_yield_pct": round(ann_static_pct, 2),
            "if_called_yield_pct": round(ann_if_called_pct, 2),
            "dividend_yield_pct": round(div_yield_pct, 2),
            "dividend_capture": round(div_capture, 4),
            "dividend_in_hold": div_in_hold,
            "annual_dividend": round(annual_div, 4),
            "ex_date": ex_date,
            "total_annual_yield_pct": round(total_annual, 2),
            "downside_protection_pct": round(downside_protection, 2),
            "breakeven": round(breakeven, 2),
            "reason": reason,
        }

    # ── helpers ──────────────────────────────────────────────────────────

    @staticmethod
    def _pick_spot(quote: Quote) -> float:
        for candidate in (quote.last, (quote.bid + quote.ask) / 2 if quote.bid and quote.ask else 0, quote.ask, quote.bid):
            if candidate and candidate > 0:
                return float(candidate)
        return 0.0

    def _load_dividend_index(self) -> dict[str, dict]:
        """Build a {symbol: row} index from the dividend_calendar.json cache.

        Empty dict on missing / unreadable cache (we fall back to Schwab
        quote fields per symbol).
        """
        path = Path(self.config.STATE_DIR).expanduser() / _DIVIDEND_CACHE_NAME
        try:
            payload = json.loads(path.read_text())
        except (OSError, ValueError):
            return {}
        index: dict[str, dict] = {}
        for row in payload.get("rows", []):
            sym = (row.get("symbol") or "").upper()
            if sym and sym not in index:
                index[sym] = row
        return index

    @staticmethod
    def _resolve_dividend(
        symbol: str, quote: Quote, div_index: dict[str, dict],
    ) -> tuple[float, str, float]:
        """Return (annual_dividend, next_ex_date_iso, next_amount).

        Prefer the Nasdaq-sourced cache when available (authoritative annual
        figure). Fall back to the Schwab quote's `next_div_*` fields and
        assume quarterly frequency for the annual estimate.
        """
        cached = div_index.get(symbol.upper())
        if cached:
            annual = float(cached.get("annual_dividend") or 0.0)
            ex_date = cached.get("ex_date") or ""
            amount = float(cached.get("amount") or 0.0)
            if annual > 0:
                return annual, ex_date, amount

        if quote.next_div_ex_date and quote.next_div_amount > 0:
            return (
                float(quote.next_div_amount) * _QUARTERLY_FREQ,
                quote.next_div_ex_date,
                float(quote.next_div_amount),
            )

        return 0.0, "", 0.0

    # ── execute ──────────────────────────────────────────────────────────

    def execute(self, opportunity: dict) -> dict | None:
        """Submit a buy-write for one opportunity, gated by DRY_RUN + live flag.

        v1 sizes at a single contract (100 shares + 1 call). Contract sizing
        is a later iteration — see multi-leg-execution-plan.md.

        Flow:
          1. Dry-run guard (global DRY_RUN or LIVE_COVERED_CALL_SCREENER=false)
          2. Account availability check
          3. RiskManager.can_buy on the equity leg
          4. place_buy_write TRIGGER chain

        Returns a status dict; `run_once` counts a non-None result as a trade.
        """
        if not self._should_execute(opportunity):
            return {"status": "dry_run", "opportunity": opportunity}

        account = self._account
        if account is None:
            logger.warning("[covered_call_screener] No account set — skipping execute")
            return {"status": "error", "error": "no account set"}

        symbol = opportunity["symbol"]
        price = float(opportunity["price"])
        contracts = 1
        shares = contracts * 100

        allowed, reason = self.risk.can_buy(
            symbol=symbol,
            quantity=shares,
            price=price,
            account=account,
        )
        if not allowed:
            logger.info("[covered_call_screener] risk veto on %s: %s", symbol, reason)
            return {"status": "risk_veto", "reason": reason, "opportunity": opportunity}

        # Conservative option fill: sell at the bid. Falls back to premium
        # (mid) if bid is absent for any reason.
        call_limit = float(opportunity.get("call_bid") or opportunity.get("call_premium") or 0)
        if call_limit <= 0:
            return {"status": "error", "error": "opportunity missing call_bid/call_premium"}

        result = self.client.place_buy_write(
            account_hash=account.account_hash,
            stock_symbol=symbol,
            option_osi=opportunity["call_symbol"],
            contracts=contracts,
            call_limit=call_limit,
        )
        if result.get("status") == "ok":
            logger.info(
                "[covered_call_screener] %s buy-write submitted: parent=%s",
                symbol, result.get("parent_order_id"),
            )
        else:
            logger.error(
                "[covered_call_screener] %s buy-write failed: %s",
                symbol, result.get("error"),
            )
        return result
