from datetime import date

import pytest

from schwabagent.tax_lots import Fill, Lot, build_lots, parse_fills, recent_buys, reconcile, replay


def _tx(day, symbol, amount, price, *, type_="TRADE", asset="EQUITY", desc=None, status="VALID"):
    return {
        "type": type_, "status": status, "description": desc,
        "tradeDate": f"{day}T14:00:00+0000",
        "transferItems": [
            {"instrument": {"assetType": "CURRENCY", "symbol": "CURRENCY_USD"}, "amount": -1.0},
            {"instrument": {"assetType": asset, "symbol": symbol}, "amount": amount,
             "price": price, "cost": -amount * price},
        ],
    }


def test_parse_fills_kinds_and_filters():
    txs = [
        _tx("2025-05-17", "ET", 10, 12.0, desc="System transfer"),
        _tx("2025-06-01", "ET", 5, 15.0),
        _tx("2025-06-02", "ET", 1, 99.0, status="INVALID"),
        _tx("2025-06-03", "ET 250620C00020000", 1, 1.5, asset="OPTION"),
        _tx("2025-08-17", "EQR", 2.0, 0.0, type_="RECEIVE_AND_DELIVER"),
        {"type": "DIVIDEND_OR_INTEREST", "tradeDate": "2025-06-05T00:00:00+0000", "transferItems": []},
    ]
    fills = parse_fills("1234", txs)
    assert [(f.symbol, f.kind, f.price) for f in fills] == [
        ("ET", "transfer", 12.0), ("ET", "trade", 15.0), ("EQR", "corporate_action", None),
    ]


def test_replay_sells_fifo():
    fills = [
        Fill("a", "X", 10, 10.0, date(2024, 1, 1), "trade"),
        Fill("a", "X", 10, 20.0, date(2024, 6, 1), "trade"),
        Fill("a", "X", -15, 25.0, date(2025, 1, 1), "trade"),
    ]
    lots = replay(fills)[("a", "X")]
    assert len(lots) == 1
    assert lots[0].qty == pytest.approx(5)
    assert lots[0].cost_per_share == 20.0


def test_reconcile_adds_estimated_prehistory_lot():
    replayed = {("a", "X"): [Lot("a", "X", 4, 30.0, date(2026, 1, 1), "trade")]}
    # Schwab: 10 held at avg 24 -> basis 240; 4 @ 30 = 120 known, so 6 @ 20.
    lots = reconcile(replayed, {("a", "X"): (10, 24.0)})[("a", "X")]
    est = lots[0]
    assert est.basis_source == "estimated" and est.acquired is None
    assert est.qty == pytest.approx(6) and est.cost_per_share == pytest.approx(20.0)
    assert est.is_long_term(date(2026, 9, 24))


def test_reconcile_solves_merger_basis_and_trims_excess():
    replayed = {("a", "EQR"): [
        Lot("a", "EQR", 3, 50.0, date(2025, 1, 1), "trade"),
        Lot("a", "EQR", 2, float("nan"), date(2026, 8, 17), "corporate_action"),
    ]}
    lots = reconcile(replayed, {("a", "EQR"): (4, 60.0)})[("a", "EQR")]
    # Oldest share trimmed (sold before replay could see it) -> 2 @ 50 + 2 @ solved.
    assert [lot.qty for lot in lots] == pytest.approx([2, 2])
    assert lots[1].cost_per_share == pytest.approx((240 - 100) / 2)


def test_reconcile_never_invents_negative_basis():
    replayed = {("a", "X"): [Lot("a", "X", 1, float("nan"), date(2026, 1, 1), "corporate_action"),
                             Lot("a", "X", 1, 500.0, date(2026, 2, 1), "trade")]}
    lots = reconcile(replayed, {("a", "X"): (2, 10.0)})[("a", "X")]
    assert lots[0].cost_per_share == 10.0


def test_build_lots_drops_closed_positions_and_recent_buys_include_drip():
    txs = {"1234": [
        _tx("2026-09-01", "O", 0.004, 55.0),  # dividend reinvestment
        _tx("2026-01-01", "O", 10, 50.0),
        _tx("2026-02-01", "GONE", 5, 10.0),
        _tx("2026-03-01", "GONE", -5, 11.0),
    ]}
    lots = build_lots(txs, {("1234", "O"): (10.004, 50.0)})
    assert set(lots) == {("1234", "O")}
    buys = recent_buys(txs, date(2026, 9, 24), days=30)
    assert [(b.symbol, b.qty) for b in buys] == [("O", 0.004)]


def test_reverse_split_keeps_basis_and_holding_period():
    fills = [
        Fill("a", "PAVM", 100, 2.0, date(2024, 3, 1), "trade"),
        Fill("a", "PAVM", 50, 1.0, date(2025, 10, 1), "trade"),
        Fill("a", "PAVM", -150, None, date(2026, 1, 5), "corporate_action"),
        Fill("a", "PAVM1", 10, None, date(2026, 1, 5), "corporate_action"),  # 1-for-15
    ]
    lots = replay(fills)[("a", "PAVM1")]
    assert [(round(lot.qty, 4), lot.cost_per_share, lot.acquired) for lot in lots] == [
        (6.6667, 30.0, date(2024, 3, 1)), (3.3333, 15.0, date(2025, 10, 1)),
    ]
    assert lots[0].is_long_term(date(2026, 9, 24))


def test_unpaired_transfer_in_has_unknown_origin():
    lots = replay([Fill("a", "EQR", 2.0, None, date(2026, 8, 17), "corporate_action"),
                   Fill("a", "X", 1.0, None, date(2026, 8, 17), "corporate_action")])
    assert lots[("a", "EQR")][0].acquired is None
