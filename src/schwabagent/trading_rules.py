"""Brokerage trading rules engine.

Enforces regulatory and Schwab-specific constraints using account metadata
returned directly from the Schwab API:

- Pattern Day Trader (PDT) rule: margin accounts under $25k get 3 day-trade
  round trips per rolling 5 business days.  Uses Schwab's own `roundTrips`
  counter and `isDayTrader` flag.
- Closing-only restriction: if Schwab has flagged the account, block new buys.
- Settlement: tracks `unsettledCash` to avoid free-riding in cash accounts.
- Wash sale awareness: flags same-symbol buy within 30 days of a loss sale.
- Event blackout: warns or blocks BUYs within N days of a scheduled earnings
  release or ex-dividend date (from the Briefing / Nasdaq scrapers).

These rules run *before* the RiskManager position-size checks and can hard-
block an order regardless of strategy signal strength.
"""
from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from schwabagent.config import Config
from schwabagent.persistence import StateStore

logger = logging.getLogger(__name__)

_PDT_MAX_DAY_TRADES = 3
_PDT_EQUITY_THRESHOLD = 25_000.0
_WASH_SALE_DAYS = 30
_ET = ZoneInfo("America/New_York")


class TradingRules:
    """Evaluates brokerage-level trading rules before order placement."""

    def __init__(self, config: Config, state: StateStore):
        self.config = config
        self.state = state
        # Lazy-loaded blackout indexes: {symbol: set[date]}.
        # Populated on first check; refreshed when the cache file mtime changes.
        self._earnings_index: dict[str, set[date]] | None = None
        self._earnings_mtime: float = 0.0
        self._dividend_index: dict[str, set[date]] | None = None
        self._dividend_mtime: float = 0.0

    def check_order(
        self,
        symbol: str,
        side: str,
        quantity: int,
        price: float,
        account_value: float,
        account_type: str = "CASH",
        round_trips: int = 0,
        is_day_trader: bool = False,
        is_closing_only: bool = False,
    ) -> tuple[bool, str]:
        """Run all trading rules against a proposed order.

        Returns:
            (allowed, reason) — reason is empty when allowed.
        """
        checks = [
            self._check_closing_only(side, is_closing_only),
            self._check_pdt(symbol, side, account_value, account_type, round_trips, is_day_trader),
            self._check_wash_sale(symbol, side),
            self._check_event_blackout(symbol, side),
        ]

        for allowed, reason in checks:
            if not allowed:
                return False, reason

        return True, ""

    # ── Closing-only restriction ─────────────────────────────────────────

    def _check_closing_only(
        self, side: str, is_closing_only: bool,
    ) -> tuple[bool, str]:
        """If Schwab has restricted the account to closing-only, block new buys."""
        if is_closing_only and side.upper() == "BUY":
            return False, (
                "Account is restricted to closing-only orders by Schwab. "
                "New BUY orders are blocked."
            )
        return True, ""

    # ── PDT rule ─────────────────────────────────────────────────────────

    def _check_pdt(
        self,
        symbol: str,
        side: str,
        account_value: float,
        account_type: str,
        round_trips: int,
        is_day_trader: bool,
    ) -> tuple[bool, str]:
        """Pattern Day Trader rule using Schwab's own tracking.

        - `roundTrips`: Schwab's count of day trades in the rolling 5-day window.
        - `isDayTrader`: True if Schwab has flagged the account as PDT.
        """
        # PDT only applies to margin accounts under the threshold
        if account_type.upper() != "MARGIN":
            return True, ""
        if account_value >= _PDT_EQUITY_THRESHOLD:
            return True, ""

        # If Schwab has already flagged as PDT, warn but don't double-block
        # (isClosingOnlyRestricted handles the hard block)
        if is_day_trader:
            logger.warning(
                "Account is flagged as Pattern Day Trader by Schwab "
                "(equity=$%.0f, roundTrips=%d)", account_value, round_trips,
            )

        # Check if this order would create a new day trade
        if round_trips >= _PDT_MAX_DAY_TRADES:
            if self._would_be_day_trade(symbol, side):
                return False, (
                    f"PDT limit reached: {round_trips}/{_PDT_MAX_DAY_TRADES} day trades "
                    f"(Schwab roundTrips={round_trips}, equity=${account_value:,.0f} "
                    f"< ${_PDT_EQUITY_THRESHOLD:,.0f}). "
                    f"A {side} of {symbol} would create another day trade."
                )

        # Warn at 2/3
        if round_trips >= _PDT_MAX_DAY_TRADES - 1 and self._would_be_day_trade(symbol, side):
            logger.warning(
                "PDT warning: %d/%d day trades used (Schwab roundTrips) — "
                "this %s of %s would use the last one",
                round_trips, _PDT_MAX_DAY_TRADES, side, symbol,
            )

        return True, ""

    def _would_be_day_trade(self, symbol: str, side: str) -> bool:
        """Return True if placing this order would complete a day trade
        (the opposite side for the same symbol was already executed today)."""
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        opposite = "SELL" if side.upper() == "BUY" else "BUY"

        trades = self.state.get_trade_history(limit=200)
        for t in reversed(trades):
            ts = t.get("timestamp", "")
            if not ts:
                continue
            if not ts.startswith(today):
                break
            if t.get("symbol") == symbol and t.get("side", "").upper() == opposite:
                return True
        return False

    # ── Wash sale ────────────────────────────────────────────────────────

    def _check_wash_sale(self, symbol: str, side: str) -> tuple[bool, str]:
        """Warn (but don't block) if buying a symbol sold at a loss within 30 days."""
        if side.upper() != "BUY":
            return True, ""

        cutoff = datetime.now(timezone.utc) - timedelta(days=_WASH_SALE_DAYS)
        trades = self.state.get_trade_history(limit=500)

        for t in reversed(trades):
            ts = t.get("timestamp", "")
            if not ts:
                continue
            try:
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            except ValueError:
                continue
            if dt < cutoff:
                break

            if (
                t.get("symbol") == symbol
                and t.get("side", "").upper() == "SELL"
                and t.get("realized_pnl", 0) < 0
            ):
                loss = t["realized_pnl"]
                logger.warning(
                    "Wash sale alert: buying %s within 30 days of selling at a loss "
                    "(sold on %s, loss=$%.2f). Tax deduction will be deferred.",
                    symbol, dt.strftime("%Y-%m-%d"), loss,
                )
                break

        return True, ""

    # ── Event blackout (earnings / ex-dividend) ─────────────────────────

    def _check_event_blackout(
        self, symbol: str, side: str,
    ) -> tuple[bool, str]:
        """Warn or block BUYs within N days of a scheduled earnings release
        or ex-dividend date.

        Fail-open on missing cache: if the scraper hasn't populated the cache
        file yet, log once and allow the order. This prevents a Nasdaq or
        Briefing outage from stranding the bot.
        """
        if side.upper() != "BUY":
            return True, ""

        today = datetime.now(_ET).date()
        sym = symbol.upper()

        earn_days = self._days_until_next_event(
            sym, today, kind="earnings",
        ) if self.config.EARNINGS_BLACKOUT_ENABLED else None
        if earn_days is not None and earn_days <= self.config.EARNINGS_BLACKOUT_DAYS:
            msg = (
                f"Earnings blackout: {sym} reports in {earn_days}d "
                f"(threshold {self.config.EARNINGS_BLACKOUT_DAYS}d)."
            )
            if self.config.EARNINGS_BLACKOUT_MODE == "block":
                return False, msg
            logger.warning("%s [warn mode — order allowed]", msg)

        div_days = self._days_until_next_event(
            sym, today, kind="dividend",
        ) if self.config.DIVIDEND_BLACKOUT_ENABLED else None
        if div_days is not None and div_days <= self.config.DIVIDEND_BLACKOUT_DAYS:
            msg = (
                f"Ex-dividend blackout: {sym} goes ex-div in {div_days}d "
                f"(threshold {self.config.DIVIDEND_BLACKOUT_DAYS}d)."
            )
            if self.config.DIVIDEND_BLACKOUT_MODE == "block":
                return False, msg
            logger.warning("%s [warn mode — order allowed]", msg)

        return True, ""

    def _days_until_next_event(
        self, symbol: str, today: date, kind: str,
    ) -> int | None:
        """Return days until the next scheduled `kind` event for `symbol`,
        or None if no upcoming event is known (or the cache is missing)."""
        index = self._get_event_index(kind)
        if index is None:
            return None
        dates = index.get(symbol)
        if not dates:
            return None
        future = [d for d in dates if d >= today]
        if not future:
            return None
        return (min(future) - today).days

    def _get_event_index(self, kind: str) -> dict[str, set[date]] | None:
        """Load or refresh the per-symbol date index for earnings/dividends.

        Returns None on missing or unreadable cache (caller fails open).
        """
        if kind == "earnings":
            from schwabagent.scrapers.earnings_calendar import _cache_path
            path = _cache_path(self.config)
            cached_index = self._earnings_index
            cached_mtime = self._earnings_mtime
        elif kind == "dividend":
            from schwabagent.scrapers.dividend_calendar import _cache_path
            path = _cache_path(self.config)
            cached_index = self._dividend_index
            cached_mtime = self._dividend_mtime
        else:
            return None

        try:
            mtime = path.stat().st_mtime
        except FileNotFoundError:
            return None
        except OSError as e:
            logger.debug("event gate: cache stat failed for %s: %s", path, e)
            return None

        if cached_index is not None and mtime == cached_mtime:
            return cached_index

        index = self._load_event_index(path, kind)
        if kind == "earnings":
            self._earnings_index = index
            self._earnings_mtime = mtime
        else:
            self._dividend_index = index
            self._dividend_mtime = mtime
        return index

    @staticmethod
    def _load_event_index(path: Path, kind: str) -> dict[str, set[date]]:
        """Parse a scraper cache file into {symbol: set[date]}.

        `earnings` rows carry a `date` field; `dividend` rows carry `ex_date`.
        Malformed rows are silently skipped.
        """
        date_key = "date" if kind == "earnings" else "ex_date"
        try:
            payload = json.loads(path.read_text())
        except (OSError, ValueError) as e:
            logger.warning("event gate: failed to read %s: %s", path, e)
            return {}

        index: dict[str, set[date]] = {}
        for row in payload.get("rows", []):
            sym = (row.get("symbol") or "").upper()
            iso = row.get(date_key)
            if not sym or not iso:
                continue
            try:
                d = date.fromisoformat(iso)
            except ValueError:
                continue
            index.setdefault(sym, set()).add(d)
        return index

    # ── Status ───────────────────────────────────────────────────────────

    def status(
        self,
        account_value: float,
        account_type: str = "CASH",
        round_trips: int = 0,
        is_day_trader: bool = False,
        is_closing_only: bool = False,
    ) -> dict:
        """Return current trading rules status for display."""
        pdt_applies = (
            account_type.upper() == "MARGIN"
            and account_value < _PDT_EQUITY_THRESHOLD
        )

        return {
            "account_type": account_type,
            "pdt_applies": pdt_applies,
            "round_trips": round_trips,
            "round_trips_limit": _PDT_MAX_DAY_TRADES if pdt_applies else None,
            "round_trips_remaining": max(0, _PDT_MAX_DAY_TRADES - round_trips) if pdt_applies else None,
            "is_day_trader": is_day_trader,
            "is_closing_only": is_closing_only,
            "pdt_threshold": _PDT_EQUITY_THRESHOLD,
        }
