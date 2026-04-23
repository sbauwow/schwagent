"""Tests for SchwabClient.place_vertical — 4 spreads × open/close dispatch.

Drives the real schwab-py vertical builders (bull_call / bear_call / bull_put
/ bear_put × open / close) and asserts on the final order dict passed to
the Schwab REST client. Catches any builder/enum path changes in schwab-py
without requiring credentials or network.
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from schwabagent.config import Config
from schwabagent.schwab_client import SchwabClient


def _make_config() -> Config:
    return Config(
        SCHWAB_API_KEY="test",
        SCHWAB_APP_SECRET="test",
        DRY_RUN=True,
    )


def _make_client_with_mock() -> tuple[SchwabClient, MagicMock]:
    """Return (SchwabClient, mocked Schwab REST client).

    The mocked client's `place_order` returns a response with a Location
    header encoding a fake order id, mimicking the real Schwab API.
    """
    client = SchwabClient(_make_config())
    rest = MagicMock()
    resp = MagicMock()
    resp.raise_for_status.return_value = None
    resp.headers = {
        "Location": "https://api.schwabapi.com/trader/v1/accounts/A/orders/99001",
    }
    rest.place_order.return_value = resp
    client._client = rest
    return client, rest


_LONG = "SPY   260515C00450000"
_SHORT = "SPY   260515C00455000"


# Expected leg ordering + net order type per (spread_type, action). The
# first leg's OSI is the long leg for bull_* and the short leg for bear_*
# (schwab-py's bear builders flip the arg order internally).
#
# (spread_type, action, net_kind, first_leg_instruction, first_leg_is_long)
_CASES = [
    ("bull_call", "open",  "NET_DEBIT",  "BUY_TO_OPEN",   True),
    ("bull_call", "close", "NET_CREDIT", "SELL_TO_CLOSE", True),
    ("bear_call", "open",  "NET_CREDIT", "SELL_TO_OPEN",  False),
    ("bear_call", "close", "NET_DEBIT",  "BUY_TO_CLOSE",  False),
    ("bull_put",  "open",  "NET_CREDIT", "BUY_TO_OPEN",   True),
    ("bull_put",  "close", "NET_DEBIT",  "SELL_TO_CLOSE", True),
    ("bear_put",  "open",  "NET_DEBIT",  "SELL_TO_OPEN",  False),
    ("bear_put",  "close", "NET_CREDIT", "BUY_TO_CLOSE",  False),
]


class TestHappyPath:
    @pytest.mark.parametrize(
        "spread_type,action,net_kind,first_instruction,first_is_long", _CASES,
    )
    def test_builder_and_legs(
        self, spread_type, action, net_kind, first_instruction, first_is_long,
    ):
        client, rest = _make_client_with_mock()

        result = client.place_vertical(
            account_hash="HASH",
            spread_type=spread_type,
            action=action,
            long_osi=_LONG,
            short_osi=_SHORT,
            quantity=5,
            net_price=1.20,
        )

        assert result["status"] == "ok", result
        assert result["order_id"] == "99001"
        assert result["spread_type"] == spread_type
        assert result["action"] == action
        assert result["long_osi"] == _LONG
        assert result["short_osi"] == _SHORT
        assert result["quantity"] == 5
        assert result["net_price"] == 1.20
        assert result["net_kind"] == net_kind
        assert result["duration"] == "DAY"
        assert result["session"] == "NORMAL"

        assert rest.place_order.call_count == 1
        account_hash_arg, order_dict = rest.place_order.call_args[0]
        assert account_hash_arg == "HASH"
        assert order_dict["orderType"] == net_kind
        assert order_dict["complexOrderStrategyType"] == "VERTICAL"
        assert order_dict["orderStrategyType"] == "SINGLE"
        assert order_dict["price"] == "1.20"
        assert order_dict["quantity"] == 5
        assert order_dict["duration"] == "DAY"
        assert order_dict["session"] == "NORMAL"

        legs = order_dict["orderLegCollection"]
        assert len(legs) == 2
        assert legs[0]["instruction"] == first_instruction
        assert legs[0]["quantity"] == 5
        assert legs[0]["instrument"]["assetType"] == "OPTION"

        first_osi = _LONG if first_is_long else _SHORT
        second_osi = _SHORT if first_is_long else _LONG
        assert legs[0]["instrument"]["symbol"] == first_osi
        assert legs[1]["instrument"]["symbol"] == second_osi
        assert legs[1]["quantity"] == 5
        assert legs[1]["instrument"]["assetType"] == "OPTION"

    def test_gtc_duration(self):
        client, rest = _make_client_with_mock()
        result = client.place_vertical(
            account_hash="HASH",
            spread_type="bull_put",
            action="open",
            long_osi="SPY   260515P00440000",
            short_osi="SPY   260515P00445000",
            quantity=1,
            net_price=0.50,
            duration="GOOD_TILL_CANCEL",
        )
        assert result["status"] == "ok"
        assert result["duration"] == "GOOD_TILL_CANCEL"
        _, order_dict = rest.place_order.call_args[0]
        assert order_dict["duration"] == "GOOD_TILL_CANCEL"

    def test_seamless_session(self):
        client, rest = _make_client_with_mock()
        result = client.place_vertical(
            account_hash="HASH",
            spread_type="bull_call",
            action="open",
            long_osi=_LONG,
            short_osi=_SHORT,
            quantity=1,
            net_price=1.10,
            session="SEAMLESS",
        )
        assert result["status"] == "ok"
        _, order_dict = rest.place_order.call_args[0]
        assert order_dict["session"] == "SEAMLESS"


class TestValidation:
    def _base(self) -> dict:
        return dict(
            account_hash="HASH",
            spread_type="bull_call",
            action="open",
            long_osi=_LONG,
            short_osi=_SHORT,
            quantity=1,
            net_price=1.00,
        )

    def _call(self, client, **overrides) -> dict:
        kwargs = self._base()
        kwargs.update(overrides)
        return client.place_vertical(**kwargs)

    def test_empty_account_hash(self):
        client, rest = _make_client_with_mock()
        result = self._call(client, account_hash="")
        assert result["status"] == "error"
        assert "account_hash" in result["error"]
        rest.place_order.assert_not_called()

    def test_empty_long_osi(self):
        client, _ = _make_client_with_mock()
        result = self._call(client, long_osi="")
        assert result["status"] == "error"
        assert "long_osi" in result["error"]

    def test_empty_short_osi(self):
        client, _ = _make_client_with_mock()
        result = self._call(client, short_osi="")
        assert result["status"] == "error"
        assert "short_osi" in result["error"]

    def test_quantity_zero(self):
        client, _ = _make_client_with_mock()
        result = self._call(client, quantity=0)
        assert result["status"] == "error"
        assert "quantity" in result["error"]

    def test_quantity_negative(self):
        client, _ = _make_client_with_mock()
        result = self._call(client, quantity=-1)
        assert result["status"] == "error"
        assert "quantity" in result["error"]

    def test_net_price_zero(self):
        client, _ = _make_client_with_mock()
        result = self._call(client, net_price=0.0)
        assert result["status"] == "error"
        assert "net_price" in result["error"]

    def test_net_price_negative(self):
        client, _ = _make_client_with_mock()
        result = self._call(client, net_price=-0.50)
        assert result["status"] == "error"
        assert "net_price" in result["error"]

    def test_unknown_spread_type(self):
        client, _ = _make_client_with_mock()
        result = self._call(client, spread_type="calendar")
        assert result["status"] == "error"
        assert "spread_type" in result["error"]

    def test_unknown_action(self):
        client, _ = _make_client_with_mock()
        result = self._call(client, action="roll")
        assert result["status"] == "error"
        assert "action" in result["error"]

    def test_unknown_duration(self):
        client, _ = _make_client_with_mock()
        result = self._call(client, duration="FOREVER")
        assert result["status"] == "error"
        assert "duration" in result["error"]

    def test_unknown_session(self):
        client, _ = _make_client_with_mock()
        result = self._call(client, session="OVERNIGHT")
        assert result["status"] == "error"
        assert "session" in result["error"]

    def test_rest_error_returns_error_status(self):
        client, rest = _make_client_with_mock()
        rest.place_order.side_effect = RuntimeError(
            "Schwab rejected: insufficient buying power",
        )
        result = self._call(client)
        assert result["status"] == "error"
        assert "insufficient buying power" in result["error"]
