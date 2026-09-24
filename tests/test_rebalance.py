from datetime import date

import pytest

from schwabagent.config import Config
from schwabagent.household import Account, Holding, Household, parse_tax_types, parse_weights
from schwabagent.rebalance import plan_rebalance
from schwabagent.tax_lots import Lot

ASOF = date(2026, 9, 24)
PRICES = {"VTI": 100.0, "BND": 50.0}


def _cfg(**kw) -> Config:
    base = dict(TAX_RATE_SHORT=0.3, TAX_RATE_LONG=0.15, REBALANCE_BAND=0.01, REBALANCE_MIN_TRADE_USD=50,
                REBALANCE_CASH_BUFFER_USD=0, WASH_SALE_DAYS=30, TAX_INEFFICIENT="BND",
                LOCATION_DRAG_TAXABLE=0.004, LOCATION_DRAG_ROTH=0.002)
    base.update(kw)
    return Config(_env_file=None, **base)


def _account(number, tax_type, cash, lots):
    holdings = {}
    for lot in lots:
        prev = holdings.get(lot.symbol)
        qty = lot.qty + (prev.qty if prev else 0)
        holdings[lot.symbol] = Holding(qty, 0, qty * PRICES[lot.symbol])
    value = cash + sum(h.market_value for h in holdings.values())
    return Account(number, "h" + number, tax_type, value, cash, holdings), lots


def _household(*accounts_and_lots):
    index = {}
    for _, lots in accounts_and_lots:
        for lot in lots:
            index.setdefault((lot.account, lot.symbol), []).append(lot)
    return Household([a for a, _ in accounts_and_lots], index, [], dict(PRICES), ASOF)


def _trades(plan):
    return {(t.account, t.symbol, t.side): t.shares for t in plan.trades}


def test_deploys_cash_and_puts_bonds_in_the_ira():
    hh = _household(_account("tax", "taxable", 6000, []), _account("ira", "traditional", 4000, []))
    plan = plan_rebalance(hh, {"VTI": 0.6, "BND": 0.4}, _cfg())
    assert _trades(plan) == {("tax", "VTI", "BUY"): 60, ("ira", "BND", "BUY"): 80}
    assert plan.after["VTI"] == pytest.approx(0.6) and plan.after["BND"] == pytest.approx(0.4)
    assert plan.estimated_tax == 0


def test_rebalances_inside_the_ira_instead_of_realizing_gains():
    # Household is 80/20; target 60/40. The taxable VTI has big gains, so the
    # solver should sell VTI in the IRA and buy BND there.
    hh = _household(
        _account("tax", "taxable", 0, [Lot("tax", "VTI", 50, 20.0, date(2020, 1, 1), "trade")]),
        _account("ira", "traditional", 0, [Lot("ira", "VTI", 30, 90.0, date(2025, 1, 1), "trade"),
                                            Lot("ira", "BND", 40, 50.0, date(2025, 1, 1), "trade")]),
    )
    plan = plan_rebalance(hh, {"VTI": 0.6, "BND": 0.4}, _cfg())
    assert all(t.account == "ira" for t in plan.trades)
    assert plan.estimated_tax == 0
    assert plan.after["BND"] == pytest.approx(0.4, abs=0.011)


def test_taxable_sells_cheapest_lots_first():
    hh = _household(_account("tax", "taxable", 0, [
        Lot("tax", "VTI", 50, 20.0, date(2020, 1, 1), "trade"),   # big LT gain
        Lot("tax", "VTI", 50, 95.0, date(2026, 8, 1), "trade"),   # small ST gain, cheaper per $
    ]))
    plan = plan_rebalance(hh, {"VTI": 0.6, "BND": 0.4}, _cfg())
    sell = next(t for t in plan.trades if t.side == "SELL")
    assert sell.shares == 39  # to the band edge (61%), not all the way to 60%
    assert [(lot.cost_per_share, qty) for lot, qty in sell.lots] == [(95.0, 39)]
    assert sell.short_term_gain == pytest.approx(195) and sell.long_term_gain == 0


def test_within_band_means_no_trades():
    hh = _household(_account("ira", "roth", 0, [Lot("ira", "VTI", 60, 90.0, date(2025, 1, 1), "trade"),
                                                 Lot("ira", "BND", 80, 50.0, date(2025, 1, 1), "trade")]))
    plan = plan_rebalance(hh, {"VTI": 0.605, "BND": 0.395}, _cfg())
    assert plan.trades == []


def test_no_loss_sale_while_buying_same_symbol():
    # Taxable holds VTI at a loss; IRA must buy VTI. Selling taxable VTI at a
    # loss to fund BND there would be washed by the IRA buy, so it's avoided.
    hh = _household(
        _account("tax", "taxable", 0, [Lot("tax", "VTI", 40, 150.0, date(2025, 1, 1), "trade")]),
        _account("ira", "traditional", 6000, []),
    )
    plan = plan_rebalance(hh, {"VTI": 0.6, "BND": 0.4}, _cfg())
    buys_vti = any(t.symbol == "VTI" and t.side == "BUY" for t in plan.trades)
    sells_vti_at_loss = any(t.symbol == "VTI" and t.side == "SELL" and t.account == "tax" for t in plan.trades)
    assert not (buys_vti and sells_vti_at_loss)


def test_unmapped_accounts_are_left_out_and_warned():
    hh = _household(_account("ira", "roth", 1000, []))
    hh.accounts.append(Account("9999", "h", None, 5000, 5000, {}))
    plan = plan_rebalance(hh, {"VTI": 1.0}, _cfg(REBALANCE_BAND=0.02))
    assert plan.managed_value == pytest.approx(1000)
    assert any("9999" in w for w in plan.warnings)


def test_parsers_validate():
    assert parse_tax_types("xxxx1234:Taxable, 5678:roth") == {"1234": "taxable", "5678": "roth"}
    with pytest.raises(ValueError):
        parse_tax_types("1234:ira")
    with pytest.raises(ValueError):
        parse_weights("VTI:0.8,BND:0.4")


def test_intended_cash_is_kept():
    hh = _household(_account("ira", "roth", 10000, []))
    plan = plan_rebalance(hh, {"VTI": 0.5}, _cfg())
    assert _trades(plan) == {("ira", "VTI", "BUY"): 50}
    assert plan.after["CASH"] == pytest.approx(0.5)


def test_money_market_counts_as_cash_and_is_sold_to_fund_buys():
    PRICES["SWVXX"] = 1.0
    try:
        acct, lots = _account("ira", "roth", 1000, [Lot("ira", "SWVXX", 9000, 1.0, date(2025, 1, 1), "trade")])
        plan = plan_rebalance(_household((acct, lots)), {"VTI": 1.0}, _cfg(REBALANCE_BAND=0.02))
        assert _trades(plan) == {("ira", "VTI", "BUY"): 100, ("ira", "SWVXX", "SELL"): 9000}
        assert plan.unmanaged == {}
    finally:
        del PRICES["SWVXX"]


def test_warns_when_a_buy_blocks_a_harvest():
    hh = _household(
        _account("tax", "taxable", 5000, [Lot("tax", "VTI", 50, 150.0, date(2025, 1, 1), "trade")]),
    )
    plan = plan_rebalance(hh, {"VTI": 1.0}, _cfg(REBALANCE_BAND=0.02))
    assert any("blocks harvesting" in w for w in plan.warnings)
