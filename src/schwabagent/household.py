"""One snapshot of every linked account, for tax-aware household decisions.

Tax-loss harvesting and cross-account rebalancing both need the same view:
each account's tax treatment, cash and positions, the tax lots behind those
positions, and every recent purchase anywhere (a buy in an IRA still triggers a
wash sale on a loss taken in a taxable account).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date

from schwabagent.config import Config
from schwabagent.tax_lots import Fill, Lot, build_lots, recent_buys

logger = logging.getLogger(__name__)

TAX_TYPES = ("taxable", "traditional", "roth")


@dataclass(frozen=True)
class Holding:
    qty: float
    avg_price: float
    market_value: float


@dataclass
class Account:
    number: str  # last 4 digits
    hash: str
    tax_type: str | None  # None = not mapped in ACCOUNT_TAX_TYPES
    total_value: float
    cash: float
    holdings: dict[str, Holding] = field(default_factory=dict)


@dataclass
class Household:
    accounts: list[Account]
    lots: dict[tuple[str, str], list[Lot]]
    recent_buys: list[Fill]
    prices: dict[str, float]
    asof: date

    def account(self, number: str) -> Account:
        return next(a for a in self.accounts if a.number == number)


def parse_pairs(raw: str) -> dict[str, str]:
    """``"A:x,B:y"`` -> ``{"A": "x", "B": "y"}`` (keys upper-cased, values stripped)."""
    out: dict[str, str] = {}
    for part in raw.split(","):
        if ":" not in part:
            continue
        key, value = part.split(":", 1)
        if key.strip() and value.strip():
            out[key.strip().upper()] = value.strip()
    return out


def parse_tax_types(raw: str) -> dict[str, str]:
    types = {k[-4:]: v.lower() for k, v in parse_pairs(raw).items()}
    bad = {k: v for k, v in types.items() if v not in TAX_TYPES}
    if bad:
        raise ValueError(f"ACCOUNT_TAX_TYPES: unknown tax type(s) {bad}; use {'/'.join(TAX_TYPES)}")
    return types


def parse_weights(raw: str) -> dict[str, float]:
    weights = {k: float(v) for k, v in parse_pairs(raw).items()}
    total = sum(weights.values())
    if weights and not 0.0 < total <= 1.0 + 1e-9:
        raise ValueError(f"REBALANCE_TARGETS weights sum to {total:.4f}; they must sum to at most 1")
    return weights


def load_household(
    client,
    config: Config,
    extra_symbols: list[str] | None = None,
    asof: date | None = None,
) -> Household:
    """Pull accounts, two years of transactions and prices from Schwab."""
    asof = asof or date.today()
    tax_types = parse_tax_types(config.ACCOUNT_TAX_TYPES)

    accounts: list[Account] = []
    transactions: dict[str, list[dict]] = {}
    for summary in client.get_all_accounts():
        number = summary.account_number[-4:]
        holdings = {
            p.symbol: Holding(p.quantity, p.avg_price, p.market_value)
            for p in summary.positions if p.quantity > 0
        }
        accounts.append(Account(
            number=number, hash=summary.account_hash, tax_type=tax_types.get(number),
            total_value=summary.total_value, cash=summary.cash_available, holdings=holdings,
        ))
        transactions[number] = client.get_transactions(summary.account_hash) if summary.account_hash else []

    unmapped = [a.number for a in accounts if a.tax_type is None and a.total_value > 0]
    if unmapped:
        logger.warning("Accounts without ACCOUNT_TAX_TYPES entry: %s", ", ".join(unmapped))

    prices = {
        sym: h.market_value / h.qty
        for a in accounts for sym, h in a.holdings.items() if h.qty > 0
    }
    missing = sorted({s.upper() for s in (extra_symbols or [])} - prices.keys())
    if missing:
        for sym, quote in client.get_quotes(missing).items():
            if quote and quote.last > 0:
                prices[sym] = quote.last

    holdings_index = {
        (a.number, sym): (h.qty, h.avg_price) for a in accounts for sym, h in a.holdings.items()
    }
    return Household(
        accounts=accounts,
        lots=build_lots(transactions, holdings_index),
        recent_buys=recent_buys(transactions, asof, days=config.WASH_SALE_DAYS),
        prices=prices,
        asof=asof,
    )
