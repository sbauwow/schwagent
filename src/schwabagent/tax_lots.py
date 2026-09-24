"""Tax lots rebuilt from Schwab transaction history.

Schwab's Trader API reports one average price per position, not lots, so lots
are replayed from ``/transactions``:

* ``TRADE`` items: amount > 0 opens a lot, amount < 0 closes lots FIFO (Schwab's
  default disposal method for stocks and ETFs).
* ``RECEIVE_AND_DELIVER`` items (splits, mergers, spin-offs, transfers) move
  shares with ``cost == 0``. A same-day close/open pair is a conversion that
  carries each lot's basis and date across; anything else opens a lot of
  unknown basis and origin.
* "System transfer" trades (an account conversion) carry basis but not the
  original purchase date, so their lots are dated at the transfer.

History only reaches back about two years, so after replay each position is
reconciled against what Schwab says it holds: missing shares become an
estimated lot older than the history window (hence long-term), and unknown
bases are solved so total basis matches Schwab's ``averagePrice * quantity``.
Every lot says where its basis came from; anything not ``trade`` should be
confirmed on schwab.com before acting on it.
"""
from __future__ import annotations

import logging
import math
from collections import defaultdict, deque
from collections.abc import Iterable
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta

logger = logging.getLogger(__name__)

LOT_ASSET_TYPES = {"EQUITY", "COLLECTIVE_INVESTMENT", "MUTUAL_FUND"}
LONG_TERM_DAYS = 365
_QTY_EPS = 1e-6


@dataclass(frozen=True)
class Fill:
    """One share movement in one account. ``qty`` > 0 in, < 0 out."""

    account: str  # masked account number (last 4)
    symbol: str
    qty: float
    price: float | None  # per share; None when the basis is unknown
    day: date
    kind: str  # "trade" | "transfer" | "corporate_action"


@dataclass(frozen=True)
class Lot:
    account: str
    symbol: str
    qty: float
    cost_per_share: float
    acquired: date | None  # None = before the history window
    basis_source: str  # "trade" | "transfer" | "corporate_action" | "estimated"

    @property
    def cost(self) -> float:
        return self.qty * self.cost_per_share

    def is_long_term(self, asof: date) -> bool:
        if self.acquired is None:
            return True  # predates the ~2-year history window
        return (asof - self.acquired).days > LONG_TERM_DAYS

    def unrealized(self, price: float) -> float:
        return self.qty * (price - self.cost_per_share)


def _parse_day(raw: str | None) -> date | None:
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).date()
    except ValueError:
        return None


def parse_fills(account: str, transactions: Iterable[dict]) -> list[Fill]:
    """Turn raw Schwab transactions into share movements, oldest first."""
    fills: list[Fill] = []
    for tx in transactions:
        if tx.get("status") == "INVALID":
            continue
        tx_type = tx.get("type")
        if tx_type not in ("TRADE", "RECEIVE_AND_DELIVER"):
            continue
        day = _parse_day(tx.get("tradeDate") or tx.get("time"))
        if day is None:
            continue
        system_transfer = (tx.get("description") or "").lower() == "system transfer"
        for item in tx.get("transferItems", []):
            instrument = item.get("instrument", {})
            symbol = instrument.get("symbol")
            qty = float(item.get("amount") or 0)
            if instrument.get("assetType") not in LOT_ASSET_TYPES or not symbol or abs(qty) < _QTY_EPS:
                continue
            price = float(item.get("price") or 0)
            if tx_type == "RECEIVE_AND_DELIVER":
                kind, price_or_none = "corporate_action", (price or None)
            elif system_transfer:
                kind, price_or_none = "transfer", price
            else:
                kind, price_or_none = "trade", price
            fills.append(Fill(account, symbol, qty, price_or_none, day, kind))
    fills.sort(key=lambda f: f.day)
    return fills


def _consume(lots: deque[Lot], qty: float) -> list[Lot]:
    """Take ``qty`` shares off the front of ``lots`` (FIFO); return what was taken."""
    taken: list[Lot] = []
    while qty > _QTY_EPS and lots:
        head = lots[0]
        if head.qty <= qty + _QTY_EPS:
            taken.append(lots.popleft())
            qty -= head.qty
        else:
            taken.append(replace(head, qty=qty))
            lots[0] = replace(head, qty=head.qty - qty)
            qty = 0.0
    return taken


def replay(fills: Iterable[Fill]) -> dict[tuple[str, str], list[Lot]]:
    """FIFO-replay fills into open lots per (account, symbol).

    A corporate action that closes one position and opens one other on the same
    day in the same account (split, reverse split, stock-for-stock merger) is a
    conversion: each old lot becomes a new one, scaled by the share ratio, keeping
    its basis and purchase date. Shares with no traceable origin (unpaired
    transfers in, spin-offs) get ``acquired=None`` and ``cost_per_share = nan``
    until ``reconcile``.
    """
    fills = list(fills)
    actions: dict[tuple[str, date], list[Fill]] = defaultdict(list)
    for f in fills:
        if f.kind == "corporate_action":
            actions[(f.account, f.day)].append(f)
    conversions: dict[tuple[str, date], tuple[Fill, Fill]] = {}
    for key, group in actions.items():
        closes = [f for f in group if f.qty < 0]
        opens = [f for f in group if f.qty > 0]
        if len(closes) == 1 and len(opens) == 1:
            conversions[key] = (closes[0], opens[0])

    open_lots: dict[tuple[str, str], deque[Lot]] = defaultdict(deque)
    done: set[tuple[str, date]] = set()
    for f in fills:
        key = (f.account, f.symbol)
        action_key = (f.account, f.day)
        if f.kind == "corporate_action" and action_key in conversions:
            if action_key in done:
                continue
            done.add(action_key)
            closed, opened = conversions[action_key]
            ratio = opened.qty / -closed.qty
            old = _consume(open_lots[(f.account, closed.symbol)], -closed.qty)
            new = open_lots[(f.account, opened.symbol)]
            for lot in old:
                new.append(Lot(f.account, opened.symbol, lot.qty * ratio, lot.cost_per_share / ratio,
                               lot.acquired, lot.basis_source))
            untraced = opened.qty - sum(lot.qty for lot in old) * ratio
            if untraced > _QTY_EPS:  # old shares predate the history window
                new.appendleft(Lot(f.account, opened.symbol, untraced, float("nan"), None, "corporate_action"))
            continue
        if f.qty > 0:
            unknown_origin = f.kind == "corporate_action"
            open_lots[key].append(Lot(
                account=f.account, symbol=f.symbol, qty=f.qty,
                cost_per_share=float("nan") if unknown_origin or f.price is None else f.price,
                acquired=None if unknown_origin else f.day, basis_source=f.kind,
            ))
            continue
        # Shares sold beyond what replay knows were bought before the history
        # window; reconcile() recreates whatever of them is still held.
        _consume(open_lots[key], -f.qty)
    return {k: list(v) for k, v in open_lots.items() if v}


def reconcile(
    replayed: dict[tuple[str, str], list[Lot]],
    holdings: dict[tuple[str, str], tuple[float, float]],
) -> dict[tuple[str, str], list[Lot]]:
    """Match replayed lots to Schwab's positions.

    ``holdings`` maps (account, symbol) -> (quantity, averagePrice). Positions
    not held are dropped; excess replayed shares are trimmed oldest-first;
    missing shares become an estimated pre-history lot; unknown bases are
    solved so the position's total basis equals ``quantity * averagePrice``.
    """
    out: dict[tuple[str, str], list[Lot]] = {}
    for key, (held_qty, avg_price) in holdings.items():
        if held_qty <= _QTY_EPS:
            continue
        lots = list(replayed.get(key, []))

        excess = sum(lot.qty for lot in lots) - held_qty
        while excess > _QTY_EPS and lots:
            if lots[0].qty <= excess + _QTY_EPS:
                excess -= lots.pop(0).qty
            else:
                lots[0] = replace(lots[0], qty=lots[0].qty - excess)
                excess = 0.0

        missing = held_qty - sum(lot.qty for lot in lots)
        if missing > _QTY_EPS:
            lots.insert(0, Lot(key[0], key[1], missing, float("nan"), None, "estimated"))

        if any(math.isnan(lot.cost_per_share) for lot in lots):
            known_cost = sum(lot.cost for lot in lots if not math.isnan(lot.cost_per_share))
            unknown_qty = sum(lot.qty for lot in lots if math.isnan(lot.cost_per_share))
            solved = (held_qty * avg_price - known_cost) / unknown_qty
            if solved < 0:  # replayed trades disagree with Schwab; don't invent a negative basis
                logger.warning("%s %s: basis does not reconcile, using average price", *key)
                solved = avg_price
            lots = [replace(lot, cost_per_share=solved) if math.isnan(lot.cost_per_share) else lot
                    for lot in lots]
        out[key] = lots
    return out


def build_lots(
    transactions_by_account: dict[str, list[dict]],
    holdings: dict[tuple[str, str], tuple[float, float]],
) -> dict[tuple[str, str], list[Lot]]:
    fills: list[Fill] = []
    for account, txs in transactions_by_account.items():
        fills.extend(parse_fills(account, txs))
    fills.sort(key=lambda f: f.day)
    return reconcile(replay(fills), holdings)


def recent_buys(
    transactions_by_account: dict[str, list[dict]],
    asof: date,
    days: int = 30,
) -> list[Fill]:
    """Every share purchase in the last ``days`` days, in every account.

    Includes dividend reinvestments: they are purchases for wash-sale purposes.
    """
    cutoff = asof - timedelta(days=days)
    buys: list[Fill] = []
    for account, txs in transactions_by_account.items():
        buys.extend(f for f in parse_fills(account, txs)
                    if f.qty > 0 and f.kind == "trade" and f.day >= cutoff)
    return buys
