from datetime import date

import pytest

from schwabagent.config import Config
from schwabagent.household import Account, Holding, Household
from schwabagent.tax_harvest import find_harvests
from schwabagent.tax_lots import Fill, Lot

ASOF = date(2026, 9, 24)


def _cfg(**kw) -> Config:
    base = dict(TAX_RATE_SHORT=0.3, TAX_RATE_LONG=0.15, TLH_MIN_LOSS_USD=200, TLH_MIN_LOSS_PCT=0.05,
                WASH_SALE_DAYS=30, TLH_REPLACEMENTS="VTI:SCHB")
    base.update(kw)
    return Config(_env_file=None, **base)


def _household(lots, tax_types, prices, recent=()):
    accounts = {}
    for lot in lots:
        acct = accounts.setdefault(lot.account, Account(lot.account, "h" + lot.account,
                                                         tax_types[lot.account], 0, 0))
        prev = acct.holdings.get(lot.symbol)
        qty = lot.qty + (prev.qty if prev else 0)
        acct.holdings[lot.symbol] = Holding(qty, 0, qty * prices[lot.symbol])
    index = {}
    for lot in lots:
        index.setdefault((lot.account, lot.symbol), []).append(lot)
    return Household(list(accounts.values()), index, list(recent), prices, ASOF)


def test_finds_loss_splits_terms_and_suggests_replacement():
    hh = _household([
        Lot("tax", "VTI", 10, 300, date(2024, 1, 1), "trade"),   # LT loss 1000
        Lot("tax", "VTI", 10, 250, date(2026, 6, 1), "trade"),   # ST loss 500
        Lot("tax", "VTI", 10, 150, date(2023, 1, 1), "trade"),   # gain, kept
    ], {"tax": "taxable"}, {"VTI": 200.0})
    [idea] = find_harvests(hh, _cfg())
    assert idea.qty == 20 and idea.loss == pytest.approx(1500)
    assert idea.long_term_loss == pytest.approx(1000) and idea.short_term_loss == pytest.approx(500)
    assert idea.tax_value == pytest.approx(1000 * 0.15 + 500 * 0.3)
    assert idea.replacement == "SCHB"
    assert idea.rebuy_after == date(2026, 10, 25)
    assert not idea.estimated_basis


def test_skips_ira_and_small_losses():
    hh = _household([
        Lot("ira", "VTI", 10, 300, date(2024, 1, 1), "trade"),
        Lot("tax", "XYZ", 10, 101, date(2024, 1, 1), "trade"),  # $10 loss
    ], {"ira": "roth", "tax": "taxable"}, {"VTI": 200.0, "XYZ": 100.0})
    assert find_harvests(hh, _cfg()) == []


def test_ira_purchase_in_window_washes_loss_permanently():
    hh = _household([
        Lot("tax", "VTI", 10, 300, date(2024, 1, 1), "trade"),
        Lot("ira", "VTI", 4, 210, date(2026, 9, 10), "trade"),
    ], {"tax": "taxable", "ira": "traditional"}, {"VTI": 200.0})
    [idea] = find_harvests(hh, _cfg())
    assert idea.washed_loss == pytest.approx(400)
    assert idea.tax_value == pytest.approx(600 * 0.15)
    assert any("lost for good" in w for w in idea.warnings)


def test_drip_and_missing_replacement_warn():
    drip = Fill("tax", "O", 0.004, 55.0, date(2026, 9, 15), "trade")
    hh = _household([
        Lot("tax", "O", 20, 70, None, "estimated"),
    ], {"tax": "taxable"}, {"O": 55.0}, recent=[drip])
    [idea] = find_harvests(hh, _cfg())
    assert idea.estimated_basis
    assert any("dividend reinvestment" in w for w in idea.warnings)
    assert idea.replacement is None
