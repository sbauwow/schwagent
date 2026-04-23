"""Tests for SchwabClient.place_buy_write — TRIGGER-chain buy-write.

Exercises the real schwab-py builders (equity_buy_limit, option_sell_to_open_limit,
first_triggers_second) and asserts on the final order dict passed to
the Schwab REST client. This catches any builder/enum path changes in
schwab-py without requiring credentials or network.
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from schwabagent.config import Config
from schwabagent.schwab_client import Quote, SchwabClient


def _make_config(**overrides) -> Config:
    defaults = dict(
        SCHWAB_API_KEY="test",
        SCHWAB_APP_SECRET="test",
        DRY_RUN=True,
        LIMIT_PRICE_BUFFER_BPS=25.0,
    )
    defaults.update(overrides)
    return Config(**defaults)


def _make_client_with_mock() -> tuple[SchwabClient, MagicMock]:
    """Return (SchwabClient, mocked Schwab REST client).

    The mocked client's `place_order` returns a response with a Location
    header that encodes a parent order id (mimics the real Schwab API).
    """
    cfg = _make_config()
    client = SchwabClient(cfg)

    schwab_rest = MagicMock()
    resp = MagicMock()
    resp.raise_for_status.return_value = None
    resp.headers = {"Location": "https://api.schwabapi.com/trader/v1/accounts/ACCT/orders/111222"}
    schwab_rest.place_order.return_value = resp
    client._client = schwab_rest
    return client, schwab_rest


# ── Happy path ───────────────────────────────────────────────────────────────


class TestHappyPath:
    def test_builds_trigger_chain_and_returns_parent_id(self):
        client, schwab_rest = _make_client_with_mock()

        result = client.place_buy_write(
            account_hash="HASH",
            stock_symbol="KO",
            option_osi="KO    260520C00063000",
            contracts=1,
            stock_limit=60.02,
            call_limit=1.10,
        )

        assert result["status"] == "ok"
        assert result["parent_order_id"] == "111222"
        assert result["child_order_id"] is None
        assert result["stock_symbol"] == "KO"
        assert result["option_symbol"] == "KO    260520C00063000"
        assert result["contracts"] == 1
        assert result["shares"] == 100
        assert result["stock_limit"] == 60.02
        assert result["call_limit"] == 1.10
        assert result["duration_child"] == "GOOD_TILL_CANCEL"

        # Exactly one REST call — the trigger chain posts as a single order.
        assert schwab_rest.place_order.call_count == 1
        account_hash_arg, order_dict = schwab_rest.place_order.call_args[0]
        assert account_hash_arg == "HASH"

        # Parent: equity BUY LIMIT, 100 shares of KO @ 60.02, DAY/NORMAL
        assert order_dict["orderType"] == "LIMIT"
        assert order_dict["orderStrategyType"] == "TRIGGER"
        assert order_dict["duration"] == "DAY"
        assert order_dict["session"] == "NORMAL"
        assert order_dict["price"] == "60.02"
        parent_legs = order_dict["orderLegCollection"]
        assert len(parent_legs) == 1
        assert parent_legs[0]["instruction"] == "BUY"
        assert parent_legs[0]["quantity"] == 100
        assert parent_legs[0]["instrument"]["symbol"] == "KO"
        assert parent_legs[0]["instrument"]["assetType"] == "EQUITY"

        # Child: option STO LIMIT, 1 contract, GTC/NORMAL
        children = order_dict["childOrderStrategies"]
        assert len(children) == 1
        child = children[0]
        assert child["orderType"] == "LIMIT"
        assert child["orderStrategyType"] == "SINGLE"
        assert child["duration"] == "GOOD_TILL_CANCEL"
        assert child["session"] == "NORMAL"
        assert child["price"] == "1.10"
        child_legs = child["orderLegCollection"]
        assert len(child_legs) == 1
        assert child_legs[0]["instruction"] == "SELL_TO_OPEN"
        assert child_legs[0]["quantity"] == 1
        assert child_legs[0]["instrument"]["symbol"] == "KO    260520C00063000"
        assert child_legs[0]["instrument"]["assetType"] == "OPTION"

    def test_multiple_contracts_scale_equity_leg(self):
        client, schwab_rest = _make_client_with_mock()

        result = client.place_buy_write(
            account_hash="HASH",
            stock_symbol="XOM",
            option_osi="XOM   260717C00130000",
            contracts=3,
            stock_limit=125.50,
            call_limit=2.40,
        )

        assert result["status"] == "ok"
        assert result["contracts"] == 3
        assert result["shares"] == 300

        _, order_dict = schwab_rest.place_order.call_args[0]
        assert order_dict["orderLegCollection"][0]["quantity"] == 300
        assert order_dict["childOrderStrategies"][0]["orderLegCollection"][0]["quantity"] == 3

    def test_auto_computes_stock_limit_from_quote(self):
        client, schwab_rest = _make_client_with_mock()

        # Mock get_quotes → live quote with ask=60.00; buffered BUY at 25 bps
        # rounds to 60.15 (60.00 * 1.0025).
        def _fake_get_quotes(symbols):
            return {
                "KO": Quote(symbol="KO", bid=59.95, ask=60.00, last=59.98,
                            volume=1_000_000, change_pct=0.0),
            }
        client.get_quotes = _fake_get_quotes  # type: ignore[method-assign]

        result = client.place_buy_write(
            account_hash="HASH",
            stock_symbol="KO",
            option_osi="KO    260520C00063000",
            contracts=1,
            stock_limit=None,
            call_limit=1.10,
        )

        assert result["status"] == "ok"
        assert result["stock_limit"] == 60.15  # 60.00 * 1.0025

        _, order_dict = schwab_rest.place_order.call_args[0]
        assert order_dict["price"] == "60.15"

    def test_explicit_day_child_duration(self):
        client, schwab_rest = _make_client_with_mock()

        result = client.place_buy_write(
            account_hash="HASH",
            stock_symbol="KO",
            option_osi="KO    260520C00063000",
            contracts=1,
            stock_limit=60.02,
            call_limit=1.10,
            duration_child="DAY",
        )

        assert result["status"] == "ok"
        assert result["duration_child"] == "DAY"
        _, order_dict = schwab_rest.place_order.call_args[0]
        assert order_dict["childOrderStrategies"][0]["duration"] == "DAY"


# ── Error paths ──────────────────────────────────────────────────────────────


class TestValidation:
    def test_empty_account_hash(self):
        client, schwab_rest = _make_client_with_mock()
        result = client.place_buy_write(
            account_hash="",
            stock_symbol="KO",
            option_osi="KO    260520C00063000",
            contracts=1,
            stock_limit=60.02,
            call_limit=1.10,
        )
        assert result["status"] == "error"
        assert "account_hash" in result["error"]
        schwab_rest.place_order.assert_not_called()

    def test_empty_stock_symbol(self):
        client, _ = _make_client_with_mock()
        result = client.place_buy_write(
            account_hash="HASH",
            stock_symbol="",
            option_osi="KO    260520C00063000",
            contracts=1,
            stock_limit=60.02,
            call_limit=1.10,
        )
        assert result["status"] == "error"
        assert "stock_symbol" in result["error"]

    def test_empty_option_osi(self):
        client, _ = _make_client_with_mock()
        result = client.place_buy_write(
            account_hash="HASH",
            stock_symbol="KO",
            option_osi="",
            contracts=1,
            stock_limit=60.02,
            call_limit=1.10,
        )
        assert result["status"] == "error"
        assert "option_osi" in result["error"]

    def test_contracts_below_one(self):
        client, _ = _make_client_with_mock()
        result = client.place_buy_write(
            account_hash="HASH",
            stock_symbol="KO",
            option_osi="KO    260520C00063000",
            contracts=0,
            stock_limit=60.02,
            call_limit=1.10,
        )
        assert result["status"] == "error"
        assert "contracts" in result["error"]

    def test_call_limit_missing(self):
        client, _ = _make_client_with_mock()
        result = client.place_buy_write(
            account_hash="HASH",
            stock_symbol="KO",
            option_osi="KO    260520C00063000",
            contracts=1,
            stock_limit=60.02,
            call_limit=None,
        )
        assert result["status"] == "error"
        assert "call_limit" in result["error"]

    def test_call_limit_zero(self):
        client, _ = _make_client_with_mock()
        result = client.place_buy_write(
            account_hash="HASH",
            stock_symbol="KO",
            option_osi="KO    260520C00063000",
            contracts=1,
            stock_limit=60.02,
            call_limit=0.0,
        )
        assert result["status"] == "error"
        assert "call_limit" in result["error"]

    def test_unknown_child_duration(self):
        client, _ = _make_client_with_mock()
        result = client.place_buy_write(
            account_hash="HASH",
            stock_symbol="KO",
            option_osi="KO    260520C00063000",
            contracts=1,
            stock_limit=60.02,
            call_limit=1.10,
            duration_child="FOREVER",
        )
        assert result["status"] == "error"
        assert "duration_child" in result["error"]

    def test_no_quote_for_auto_price(self):
        client, _ = _make_client_with_mock()
        client.get_quotes = lambda symbols: {}  # type: ignore[method-assign]
        result = client.place_buy_write(
            account_hash="HASH",
            stock_symbol="UNKNOWN",
            option_osi="UNKNOWN260520C00063000",
            contracts=1,
            stock_limit=None,
            call_limit=1.10,
        )
        assert result["status"] == "error"
        assert "No quote" in result["error"]

    def test_rest_error_returns_error_status(self):
        client, schwab_rest = _make_client_with_mock()
        schwab_rest.place_order.side_effect = RuntimeError("Schwab rejected: margin")
        result = client.place_buy_write(
            account_hash="HASH",
            stock_symbol="KO",
            option_osi="KO    260520C00063000",
            contracts=1,
            stock_limit=60.02,
            call_limit=1.10,
        )
        assert result["status"] == "error"
        assert "margin" in result["error"]
