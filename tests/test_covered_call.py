"""Tests for the covered call screener.

Pure-logic tests with a mocked SchwabClient: no network, no real chain data.
"""
from __future__ import annotations

import json
from datetime import date, timedelta
from unittest.mock import MagicMock

import pytest

from schwabagent.config import Config
from schwabagent.schwab_client import OptionContract, Quote
from schwabagent.strategies.covered_call_screener import CoveredCallScreener


def _make_config(tmp_dir: str, **overrides) -> Config:
    defaults = dict(
        SCHWAB_API_KEY="test",
        SCHWAB_APP_SECRET="test",
        STATE_DIR=tmp_dir,
        DRY_RUN=True,
        COVERED_CALL_SYMBOLS="KO",
        COVERED_CALL_DTE_MIN=30,
        COVERED_CALL_DTE_MAX=60,
        COVERED_CALL_TARGET_OTM_PCT=5.0,
        COVERED_CALL_MIN_DIV_YIELD_PCT=1.5,
        COVERED_CALL_MIN_OI=100,
        COVERED_CALL_MAX_SPREAD_PCT=10.0,
        COVERED_CALL_MIN_ANNUAL_YIELD_PCT=5.0,
        COVERED_CALL_TOP_N=20,
    )
    defaults.update(overrides)
    return Config(**defaults)


def _mock_quote(**kw) -> Quote:
    defaults = dict(
        symbol="KO",
        bid=59.95,
        ask=60.05,
        last=60.0,
        volume=1_000_000,
        change_pct=0.5,
    )
    defaults.update(kw)
    return Quote(**defaults)


def _contract(
    strike: float,
    expiration: str,
    dte: int,
    bid: float = 1.20,
    ask: float = 1.30,
    delta: float = 0.30,
    oi: int = 500,
) -> OptionContract:
    return OptionContract(
        symbol=f"KO_{expiration}_{strike}C",
        underlying="KO",
        side="CALL",
        strike=strike,
        expiration=expiration,
        dte=dte,
        bid=bid,
        ask=ask,
        mark=(bid + ask) / 2,
        delta=delta,
        gamma=0.05,
        iv=22.0,
        open_interest=oi,
        volume=100,
    )


@pytest.fixture
def tmp_dir(tmp_path):
    return str(tmp_path)


@pytest.fixture
def config(tmp_dir):
    return _make_config(tmp_dir)


@pytest.fixture
def screener(config):
    client = MagicMock()
    risk = MagicMock()
    state = MagicMock()
    return CoveredCallScreener(client, config, risk, state)


def _write_div_cache(tmp_dir: str, symbol: str, annual: float, ex_date: str, amount: float):
    path = tmp_dir + "/dividend_calendar.json"
    payload = {
        "fetched_at": "2026-04-15T10:00:00-04:00",
        "rows": [{
            "symbol": symbol,
            "company": "Test Co",
            "ex_date": ex_date,
            "annual_dividend": annual,
            "amount": amount,
            "payment_date": ex_date,
            "record_date": ex_date,
            "announce_date": ex_date,
        }],
    }
    with open(path, "w") as f:
        json.dump(payload, f)


class TestScanPipeline:
    """End-to-end scan() behaviour against a mocked client."""

    def test_empty_universe_returns_nothing(self, tmp_dir):
        cfg = _make_config(tmp_dir, COVERED_CALL_SYMBOLS="")
        client = MagicMock()
        s = CoveredCallScreener(client, cfg, MagicMock(), MagicMock())
        assert s.scan() == []

    def test_non_dividend_payer_skipped(self, screener):
        """No dividend fields on quote + no cache entry → skip."""
        screener.client.get_quotes.return_value = {
            "KO": _mock_quote(next_div_ex_date="", next_div_amount=0.0),
        }
        # Chain doesn't matter — we should bail before fetching it.
        screener.client.get_option_chain.return_value = []
        assert screener.scan() == []

    def test_low_dividend_yield_skipped(self, tmp_dir, screener):
        """Dividend yield below the floor → skip."""
        # $0.10 quarterly on a $60 stock = 0.67% annual — below default 1.5%.
        screener.client.get_quotes.return_value = {
            "KO": _mock_quote(next_div_ex_date="2026-05-15", next_div_amount=0.10),
        }
        screener.client.get_option_chain.return_value = [
            _contract(63.0, "2026-05-15", 30),
        ]
        assert screener.scan() == []

    def test_happy_path_returns_ranked_candidate(self, tmp_dir, screener):
        """A clean setup yields one candidate with sensible metrics."""
        _write_div_cache(tmp_dir, "KO", annual=1.92, ex_date="2026-05-15", amount=0.48)
        screener.client.get_quotes.return_value = {"KO": _mock_quote()}
        # Chain around spot=60, target strike = 60 * 1.05 = 63
        screener.client.get_option_chain.return_value = [
            _contract(62.5, "2026-05-20", 35, bid=1.40, ask=1.50),
            _contract(63.0, "2026-05-20", 35, bid=1.10, ask=1.20),  # closest to 63
            _contract(65.0, "2026-05-20", 35, bid=0.50, ask=0.60),
        ]

        opps = screener.scan()
        assert len(opps) == 1
        row = opps[0]
        assert row["symbol"] == "KO"
        assert row["strike"] == 63.0
        # premium = (1.10 + 1.20) / 2 = 1.15
        assert row["call_premium"] == pytest.approx(1.15)
        # if-called return = (63 - 60 + 1.15) / 60 = 0.06917
        # annualized over 35 days: 0.06917 * 365/35 ≈ 72.13%
        assert row["if_called_yield_pct"] == pytest.approx(72.13, rel=1e-3)
        # dividend_yield_pct = 1.92 / 60 * 100 = 3.2%
        assert row["dividend_yield_pct"] == pytest.approx(3.2, rel=1e-3)
        # ex_date 2026-05-15 lands inside hold window (today → 2026-05-20)
        assert row["dividend_in_hold"] is True
        # downside protection = 1.15 / 60 = 1.9167% (stored rounded to 2 dp)
        assert row["downside_protection_pct"] == pytest.approx(1.92, abs=0.01)
        assert row["breakeven"] == pytest.approx(58.85)
        assert row["score"] == row["total_annual_yield_pct"]

    def test_picks_strike_nearest_target_otm(self, tmp_dir, screener):
        _write_div_cache(tmp_dir, "KO", annual=1.92, ex_date="2030-01-01", amount=0.48)
        screener.client.get_quotes.return_value = {"KO": _mock_quote()}
        # Target = 63. Two candidates; the exact 63 must win over the 62.5.
        screener.client.get_option_chain.return_value = [
            _contract(62.5, "2026-05-20", 35, bid=1.40, ask=1.50),
            _contract(63.0, "2026-05-20", 35, bid=1.10, ask=1.20),
        ]
        opps = screener.scan()
        assert opps[0]["strike"] == 63.0

    def test_liquidity_gate_rejects_thin_oi(self, tmp_dir, screener):
        _write_div_cache(tmp_dir, "KO", annual=1.92, ex_date="2030-01-01", amount=0.48)
        screener.client.get_quotes.return_value = {"KO": _mock_quote()}
        screener.client.get_option_chain.return_value = [
            _contract(63.0, "2026-05-20", 35, bid=1.10, ask=1.20, oi=50),
        ]
        assert screener.scan() == []

    def test_spread_gate_rejects_wide_bidask(self, tmp_dir, screener):
        _write_div_cache(tmp_dir, "KO", annual=1.92, ex_date="2030-01-01", amount=0.48)
        screener.client.get_quotes.return_value = {"KO": _mock_quote()}
        # mid = 1.00, spread = 0.40 → 40% → above 10% default
        screener.client.get_option_chain.return_value = [
            _contract(63.0, "2026-05-20", 35, bid=0.80, ask=1.20),
        ]
        assert screener.scan() == []

    def test_min_yield_filter(self, tmp_dir, screener):
        """A low-premium call that produces <min annual yield is dropped."""
        _write_div_cache(tmp_dir, "KO", annual=1.92, ex_date="2030-01-01", amount=0.48)
        screener.client.get_quotes.return_value = {"KO": _mock_quote()}
        # Premium ~0.05 → annualized yield tiny, below MIN_ANNUAL_YIELD_PCT=5%.
        screener.client.get_option_chain.return_value = [
            _contract(63.0, "2026-05-20", 35, bid=0.04, ask=0.06),
        ]
        assert screener.scan() == []

    def test_dividend_outside_hold_not_counted(self, tmp_dir, screener):
        """Ex-date after expiry → no dividend capture bonus."""
        _write_div_cache(tmp_dir, "KO", annual=1.92, ex_date="2027-01-01", amount=0.48)
        screener.client.get_quotes.return_value = {"KO": _mock_quote()}
        screener.client.get_option_chain.return_value = [
            _contract(63.0, "2026-05-20", 35, bid=1.10, ask=1.20),
        ]
        opps = screener.scan()
        assert len(opps) == 1
        assert opps[0]["dividend_in_hold"] is False
        assert opps[0]["dividend_capture"] == 0.0

    def test_picks_best_expiry_per_symbol(self, tmp_dir, screener):
        """When multiple expiries qualify, the one with highest annual yield wins."""
        _write_div_cache(tmp_dir, "KO", annual=1.92, ex_date="2030-01-01", amount=0.48)
        screener.client.get_quotes.return_value = {"KO": _mock_quote()}
        # Two expiries. The shorter DTE produces a higher annualized yield on
        # the same premium, so it should be chosen.
        screener.client.get_option_chain.return_value = [
            _contract(63.0, "2026-05-20", 35, bid=1.10, ask=1.20),
            _contract(63.0, "2026-06-20", 66, bid=1.10, ask=1.20),
        ]
        opps = screener.scan()
        assert len(opps) == 1
        assert opps[0]["expiration"] == "2026-05-20"
        assert opps[0]["dte"] == 35

    def test_quote_fallback_when_no_div_cache(self, screener):
        """No cache entry → fall back to quarterly-frequency quote estimate."""
        screener.client.get_quotes.return_value = {
            "KO": _mock_quote(next_div_ex_date="2026-05-15", next_div_amount=0.48),
        }
        screener.client.get_option_chain.return_value = [
            _contract(63.0, "2026-05-20", 35, bid=1.10, ask=1.20),
        ]
        opps = screener.scan()
        assert len(opps) == 1
        # Quarterly fallback: 0.48 * 4 = 1.92 annual → 3.2% yield
        assert opps[0]["dividend_yield_pct"] == pytest.approx(3.2, rel=1e-3)
        assert opps[0]["annual_dividend"] == pytest.approx(1.92)

    def test_max_spot_filter_drops_pricey_stocks(self, tmp_dir):
        """Spots above COVERED_CALL_MAX_SPOT are filtered before ranking."""
        cfg = _make_config(tmp_dir, COVERED_CALL_SYMBOLS="GOOG", COVERED_CALL_MAX_SPOT=250.0)
        _write_div_cache(tmp_dir, "GOOG", annual=1.20, ex_date="2030-01-01", amount=0.30)
        client = MagicMock()
        # $500 stock — above the $250 cap, should be filtered.
        client.get_quotes.return_value = {
            "GOOG": Quote(
                symbol="GOOG", bid=499.95, ask=500.05, last=500.0,
                volume=1_000_000, change_pct=0.0,
            ),
        }
        client.get_option_chain.return_value = [
            _contract(525.0, "2026-05-20", 35, bid=3.00, ask=3.20),
        ]
        s = CoveredCallScreener(client, cfg, MagicMock(), MagicMock())
        assert s.scan() == []
        # Chain never fetched because filter bails first.
        client.get_option_chain.assert_not_called()


def _opportunity(symbol: str = "KO") -> dict:
    """Minimal opportunity dict shaped like scan() output."""
    return {
        "strategy": "covered_call_screener",
        "symbol": symbol,
        "signal": "BUY",
        "score": 72.0,
        "price": 60.02,
        "strike": 63.0,
        "expiration": "2026-05-20",
        "dte": 35,
        "call_symbol": "KO    260520C00063000",
        "call_bid": 1.10,
        "call_ask": 1.20,
        "call_premium": 1.15,
    }


class TestExecute:
    """Buy-write execution path — DRY_RUN, risk veto, and live submission."""

    def _setup(self, tmp_dir, *, dry_run: bool = True, risk_allow: bool = True):
        cfg = _make_config(tmp_dir, DRY_RUN=dry_run, LIVE_COVERED_CALL_SCREENER=not dry_run)
        client = MagicMock()
        client.place_buy_write.return_value = {
            "status": "ok",
            "parent_order_id": "1001",
            "child_order_id": None,
        }
        risk = MagicMock()
        risk.can_buy.return_value = (risk_allow, "" if risk_allow else "position cap")
        account = MagicMock()
        account.account_hash = "HASH"
        s = CoveredCallScreener(client, cfg, risk, MagicMock(), account=account)
        return s, client, risk

    def test_dry_run_does_not_call_client(self, tmp_dir):
        s, client, _ = self._setup(tmp_dir, dry_run=True)
        result = s.execute(_opportunity())
        assert result["status"] == "dry_run"
        client.place_buy_write.assert_not_called()

    def test_force_dry_run_flag(self, tmp_dir):
        """`_force_dry_run` marker (set by run_once when LIVE flag off) bypasses execution."""
        s, client, _ = self._setup(tmp_dir, dry_run=False)
        opp = _opportunity()
        opp["_force_dry_run"] = True
        result = s.execute(opp)
        assert result["status"] == "dry_run"
        client.place_buy_write.assert_not_called()

    def test_risk_veto_short_circuits(self, tmp_dir):
        s, client, risk = self._setup(tmp_dir, dry_run=False, risk_allow=False)
        result = s.execute(_opportunity())
        assert result["status"] == "risk_veto"
        assert "position cap" in result["reason"]
        client.place_buy_write.assert_not_called()
        risk.can_buy.assert_called_once()

    def test_missing_account_errors(self, tmp_dir):
        cfg = _make_config(tmp_dir, DRY_RUN=False, LIVE_COVERED_CALL_SCREENER=True)
        s = CoveredCallScreener(MagicMock(), cfg, MagicMock(), MagicMock(), account=None)
        result = s.execute(_opportunity())
        assert result["status"] == "error"
        assert "account" in result["error"]

    def test_live_happy_path(self, tmp_dir):
        s, client, risk = self._setup(tmp_dir, dry_run=False)
        result = s.execute(_opportunity())
        assert result["status"] == "ok"
        assert result["parent_order_id"] == "1001"
        # Risk checked on the equity leg (100 shares).
        risk.can_buy.assert_called_once()
        kwargs = risk.can_buy.call_args.kwargs
        assert kwargs["symbol"] == "KO"
        assert kwargs["quantity"] == 100
        assert kwargs["price"] == 60.02
        # place_buy_write called with sell-at-bid conservative pricing.
        client.place_buy_write.assert_called_once()
        call_kwargs = client.place_buy_write.call_args.kwargs
        assert call_kwargs["account_hash"] == "HASH"
        assert call_kwargs["stock_symbol"] == "KO"
        assert call_kwargs["option_osi"] == "KO    260520C00063000"
        assert call_kwargs["contracts"] == 1
        assert call_kwargs["call_limit"] == 1.10  # call_bid

    def test_missing_call_bid_falls_back_to_premium(self, tmp_dir):
        s, client, _ = self._setup(tmp_dir, dry_run=False)
        opp = _opportunity()
        opp["call_bid"] = 0.0  # missing bid → fall back to premium
        s.execute(opp)
        assert client.place_buy_write.call_args.kwargs["call_limit"] == 1.15  # call_premium

    def test_missing_call_pricing_errors(self, tmp_dir):
        s, client, _ = self._setup(tmp_dir, dry_run=False)
        opp = _opportunity()
        opp["call_bid"] = 0.0
        opp["call_premium"] = 0.0
        result = s.execute(opp)
        assert result["status"] == "error"
        assert "call_bid" in result["error"] or "call_premium" in result["error"]
        client.place_buy_write.assert_not_called()

    def test_client_error_is_propagated(self, tmp_dir):
        s, client, _ = self._setup(tmp_dir, dry_run=False)
        client.place_buy_write.return_value = {"status": "error", "error": "Schwab margin check failed"}
        result = s.execute(_opportunity())
        assert result["status"] == "error"
        assert "margin" in result["error"]
