"""Tests for the Telegram approval-message renderer.

Covers both single-leg (default) and multi-leg buy-write branches. Pure
string-building tests — no bot, no network.
"""
from __future__ import annotations

from schwabagent.telegram import _render_approval_text


class TestSingleLegDefault:
    def test_renders_equity_trade_header_and_body(self):
        trade = {
            "side": "BUY",
            "symbol": "SPY",
            "quantity": 10,
            "price": 500.25,
            "strategy": "momentum",
        }
        text = _render_approval_text(trade, timeout=60)
        assert "Trade Approval Required" in text
        assert "BUY SPY" in text
        assert "momentum" in text
        assert "Qty: 10 @ $500.25" in text
        assert "Value: $5,002.50" in text
        assert "Expires in 60s" in text


class TestBuyWrite:
    def _base(self) -> dict:
        return {
            "trade_type": "buy_write",
            "strategy": "covered_call_screener",
            "symbol": "KO",
            "contracts": 1,
            "stock_limit": 60.02,
            "strike": 63.0,
            "expiration": "2026-05-20",
            "call_limit": 1.10,
            "if_called_yield_pct": 72.1,
            "dividend_capture": 0.48,
        }

    def test_renders_two_legs_net_debit_and_max_profit(self):
        text = _render_approval_text(self._base(), timeout=90)

        # Header (strategy name has underscores → MarkdownV2 escapes them)
        assert "Buy-Write proposal" in text
        assert "KO" in text
        assert r"covered\_call\_screener" in text

        # Legs — expiration "2026-05-20" is passed through _escape_md → dashes escaped
        assert "BUY 100 KO @ LIMIT $60.02" in text
        assert r"STO  1 KO 2026\-05\-20 $63.0 CALL @ LIMIT $1.10" in text

        # Economics: net debit = 60.02*100 - 1.10*100 = 5892.00
        assert "Net debit: $5,892.00" in text
        # Max profit if called = (63 - 60.02 + 1.10) * 100 = 408.00
        assert "Max profit" in text
        assert "$408.00" in text

        # Optional lines (hyphen in "if-called" is escaped)
        assert r"Annualized if\-called" in text
        assert "72.1%" in text
        assert "Div in hold: $48.00" in text

        # Footer
        assert "Expires in 90s" in text

    def test_scales_with_multiple_contracts(self):
        trade = self._base()
        trade["contracts"] = 3
        text = _render_approval_text(trade, timeout=60)

        assert "BUY 300 KO @ LIMIT $60.02" in text
        assert "STO  3 KO" in text
        # Net debit = 60.02*300 - 1.10*300 = 17676.00
        assert "Net debit: $17,676.00" in text
        # Max profit = (63 - 60.02 + 1.10) * 300 = 1224.00
        assert "$1,224.00" in text

    def test_omits_optional_lines_when_absent(self):
        trade = self._base()
        trade.pop("if_called_yield_pct")
        trade.pop("dividend_capture")
        text = _render_approval_text(trade, timeout=60)
        assert "Annualized if" not in text
        assert "Div in hold" not in text
        # Core lines still present
        assert "Buy-Write proposal" in text
        assert "Net debit:" in text

    def test_falls_back_to_call_bid_when_call_limit_missing(self):
        trade = self._base()
        trade.pop("call_limit")
        trade["call_bid"] = 1.05
        text = _render_approval_text(trade, timeout=60)
        assert "@ LIMIT $1.05" in text
