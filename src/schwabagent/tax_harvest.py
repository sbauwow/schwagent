"""Tax-loss harvesting candidates across the household. Advisory only.

A candidate is every loss lot of one symbol in one taxable account. Selling it
realizes the loss; buying a *different* fund (``TLH_REPLACEMENTS``) keeps the
market exposure. The wash-sale rule disallows the loss on as many shares as were
bought within ``WASH_SALE_DAYS`` before or after the sale, in *any* account; a
loss washed by a purchase in an IRA or Roth is lost for good, because the basis
adjustment lands in the tax-advantaged account.

What's modelled: shares acquired inside the window that are still held and not
part of this sale are replacement shares (lookback). The lookahead half is on
you: don't buy the symbol anywhere, and turn off its dividend reinvestment,
until ``rebuy_after``.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta

from schwabagent.config import Config
from schwabagent.household import Household, parse_pairs
from schwabagent.tax_lots import Lot

DRIP_MAX_USD = 100.0  # recent buys this small are treated as dividend reinvestments


@dataclass
class HarvestIdea:
    account: str
    symbol: str
    price: float
    lots: list[Lot]
    loss: float  # positive dollars
    short_term_loss: float
    long_term_loss: float
    tax_value: float  # loss * rate, net of any wash-sale disallowance
    washed_loss: float  # disallowed portion
    replacement: str | None
    rebuy_after: date
    estimated_basis: bool
    warnings: list[str] = field(default_factory=list)

    @property
    def qty(self) -> float:
        return sum(lot.qty for lot in self.lots)


def find_harvests(hh: Household, config: Config) -> list[HarvestIdea]:
    replacements = parse_pairs(config.TLH_REPLACEMENTS)
    window = timedelta(days=config.WASH_SALE_DAYS)
    rebuy_after = hh.asof + window + timedelta(days=1)
    tax_type = {a.number: a.tax_type for a in hh.accounts}

    ideas: list[HarvestIdea] = []
    for account in hh.accounts:
        if account.tax_type != "taxable":
            continue
        for symbol in account.holdings:
            price = hh.prices.get(symbol)
            if not price:
                continue
            lots = hh.lots.get((account.number, symbol), [])
            selling = [lot for lot in lots if lot.unrealized(price) < 0]
            if not selling:
                continue
            loss = -sum(lot.unrealized(price) for lot in selling)
            basis = sum(lot.cost for lot in selling)
            if loss < config.TLH_MIN_LOSS_USD or loss < config.TLH_MIN_LOSS_PCT * basis:
                continue
            st_loss = -sum(lot.unrealized(price) for lot in selling if not lot.is_long_term(hh.asof))
            lt_loss = loss - st_loss

            # Lookback: shares bought in the window, anywhere, that this sale leaves held.
            replacement_lots = [
                lot for (acct, sym), held in hh.lots.items() if sym == symbol
                for lot in held
                if lot.acquired is not None and lot.acquired >= hh.asof - window
                and not (acct == account.number and lot in selling)
            ]
            sold_qty = sum(lot.qty for lot in selling)
            washed_qty = min(sum(lot.qty for lot in replacement_lots), sold_qty)
            washed = loss * washed_qty / sold_qty if sold_qty else 0.0

            kept = 1 - washed / loss
            idea = HarvestIdea(
                account=account.number, symbol=symbol, price=price, lots=selling,
                loss=loss, short_term_loss=st_loss, long_term_loss=lt_loss,
                tax_value=kept * (st_loss * config.TAX_RATE_SHORT + lt_loss * config.TAX_RATE_LONG),
                washed_loss=washed, replacement=replacements.get(symbol.upper()),
                rebuy_after=rebuy_after,
                estimated_basis=any(lot.basis_source != "trade" for lot in selling),
            )
            if washed > 0:
                where = sorted({lot.account for lot in replacement_lots})
                permanent = [a for a in where if tax_type.get(a) in ("traditional", "roth")]
                idea.warnings.append(
                    f"${washed:,.0f} of the loss is washed by {washed_qty:g} shares bought since "
                    f"{hh.asof - window} in ...{', ...'.join(where)}"
                    + (" — lost for good (IRA/Roth)" if permanent else " — added to their basis")
                )
            drip_accounts = sorted({
                b.account for b in hh.recent_buys
                if b.symbol == symbol and b.qty * (b.price or 0) <= DRIP_MAX_USD
            })
            if drip_accounts:
                idea.warnings.append(
                    f"dividend reinvestment looks on in ...{', ...'.join(drip_accounts)}: "
                    f"turn it off until {rebuy_after} or each reinvestment washes part of the loss"
                )
            ideas.append(idea)

    ideas.sort(key=lambda i: -i.tax_value)
    return ideas
