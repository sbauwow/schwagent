"""Tests for SchwabClient.place_iron_condor — 4-leg hand-built OrderBuilder.

Asserts on the final order dict passed to the Schwab REST client. Covers
both open (net credit, BTO/STO) and close (net debit, STC/BTC) paths.
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
    client = SchwabClient(_make_config())
    rest = MagicMock()
    resp = MagicMock()
    resp.raise_for_status.return_value = None
    resp.headers = {
        "Location": "https://api.schwabapi.com/trader/v1/accounts/A/orders/77010",
    }
    rest.place_order.return_value = resp
    client._client = rest
    return client, rest


_LP = "SPY   260515P00440000"  # long put (wing, farthest OTM put)
_SP = "SPY   260515P00445000"  # short put (body)
_SC = "SPY   260515C00465000"  # short call (body)
_LC = "SPY   260515C00470000"  # long call (wing, farthest OTM call)


class TestHappyPath:
    def test_open_builds_net_credit_four_legs(self):
        client, rest = _make_client_with_mock()

        result = client.place_iron_condor(
            account_hash="HASH",
            action="open",
            long_put_osi=_LP,
            short_put_osi=_SP,
            short_call_osi=_SC,
            long_call_osi=_LC,
            quantity=2,
            net_price=1.50,
        )

        assert result["status"] == "ok", result
        assert result["order_id"] == "77010"
        assert result["action"] == "open"
        assert result["long_put_osi"] == _LP
        assert result["short_put_osi"] == _SP
        assert result["short_call_osi"] == _SC
        assert result["long_call_osi"] == _LC
        assert result["quantity"] == 2
        assert result["net_price"] == 1.50
        assert result["net_kind"] == "NET_CREDIT"
        assert result["duration"] == "DAY"
        assert result["session"] == "NORMAL"

        assert rest.place_order.call_count == 1
        account_hash_arg, order_dict = rest.place_order.call_args[0]
        assert account_hash_arg == "HASH"
        assert order_dict["orderType"] == "NET_CREDIT"
        assert order_dict["complexOrderStrategyType"] == "IRON_CONDOR"
        assert order_dict["orderStrategyType"] == "SINGLE"
        assert order_dict["price"] == "1.50"
        assert order_dict["quantity"] == 2
        assert order_dict["duration"] == "DAY"
        assert order_dict["session"] == "NORMAL"

        legs = order_dict["orderLegCollection"]
        assert len(legs) == 4
        expected = [
            ("BUY_TO_OPEN",  _LP),
            ("SELL_TO_OPEN", _SP),
            ("SELL_TO_OPEN", _SC),
            ("BUY_TO_OPEN",  _LC),
        ]
        for leg, (instr, osi) in zip(legs, expected, strict=True):
            assert leg["instruction"] == instr
            assert leg["quantity"] == 2
            assert leg["instrument"]["symbol"] == osi
            assert leg["instrument"]["assetType"] == "OPTION"

    def test_close_builds_net_debit_four_legs(self):
        client, rest = _make_client_with_mock()

        result = client.place_iron_condor(
            account_hash="HASH",
            action="close",
            long_put_osi=_LP,
            short_put_osi=_SP,
            short_call_osi=_SC,
            long_call_osi=_LC,
            quantity=1,
            net_price=0.40,
        )

        assert result["status"] == "ok"
        assert result["action"] == "close"
        assert result["net_kind"] == "NET_DEBIT"
        assert result["net_price"] == 0.40

        _, order_dict = rest.place_order.call_args[0]
        assert order_dict["orderType"] == "NET_DEBIT"
        assert order_dict["complexOrderStrategyType"] == "IRON_CONDOR"
        assert order_dict["price"] == "0.40"

        legs = order_dict["orderLegCollection"]
        assert len(legs) == 4
        expected = [
            ("SELL_TO_CLOSE", _LP),
            ("BUY_TO_CLOSE",  _SP),
            ("BUY_TO_CLOSE",  _SC),
            ("SELL_TO_CLOSE", _LC),
        ]
        for leg, (instr, osi) in zip(legs, expected, strict=True):
            assert leg["instruction"] == instr
            assert leg["quantity"] == 1
            assert leg["instrument"]["symbol"] == osi

    def test_gtc_and_seamless(self):
        client, rest = _make_client_with_mock()
        result = client.place_iron_condor(
            account_hash="HASH",
            action="open",
            long_put_osi=_LP,
            short_put_osi=_SP,
            short_call_osi=_SC,
            long_call_osi=_LC,
            quantity=1,
            net_price=1.00,
            duration="GOOD_TILL_CANCEL",
            session="SEAMLESS",
        )
        assert result["status"] == "ok"
        _, order_dict = rest.place_order.call_args[0]
        assert order_dict["duration"] == "GOOD_TILL_CANCEL"
        assert order_dict["session"] == "SEAMLESS"


class TestValidation:
    def _base(self) -> dict:
        return dict(
            account_hash="HASH",
            action="open",
            long_put_osi=_LP,
            short_put_osi=_SP,
            short_call_osi=_SC,
            long_call_osi=_LC,
            quantity=1,
            net_price=1.00,
        )

    def _call(self, client, **overrides) -> dict:
        kwargs = self._base()
        kwargs.update(overrides)
        return client.place_iron_condor(**kwargs)

    def test_empty_account_hash(self):
        client, rest = _make_client_with_mock()
        result = self._call(client, account_hash="")
        assert result["status"] == "error"
        assert "account_hash" in result["error"]
        rest.place_order.assert_not_called()

    @pytest.mark.parametrize(
        "field",
        ["long_put_osi", "short_put_osi", "short_call_osi", "long_call_osi"],
    )
    def test_empty_osi_field(self, field):
        client, _ = _make_client_with_mock()
        result = self._call(client, **{field: ""})
        assert result["status"] == "error"
        assert field in result["error"]

    def test_quantity_zero(self):
        client, _ = _make_client_with_mock()
        result = self._call(client, quantity=0)
        assert result["status"] == "error"
        assert "quantity" in result["error"]

    def test_quantity_negative(self):
        client, _ = _make_client_with_mock()
        result = self._call(client, quantity=-2)
        assert result["status"] == "error"
        assert "quantity" in result["error"]

    def test_net_price_zero(self):
        client, _ = _make_client_with_mock()
        result = self._call(client, net_price=0.0)
        assert result["status"] == "error"
        assert "net_price" in result["error"]

    def test_net_price_negative(self):
        client, _ = _make_client_with_mock()
        result = self._call(client, net_price=-1.0)
        assert result["status"] == "error"
        assert "net_price" in result["error"]

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
            "Schwab rejected: not enough margin for 4-leg",
        )
        result = self._call(client)
        assert result["status"] == "error"
        assert "not enough margin" in result["error"]
