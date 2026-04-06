"""Tests for liquidity and dividend filters.

Covers:
- Quote spread calculation
- Liquidity filter in ETF Scalp (volume, spread)
- Dividend filter in ETF Rotation (near/far ex-date, score reduction)
- Config defaults for new fields
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch, PropertyMock

import numpy as np
import pandas as pd
import pytest

from schwabagent.config import Config
from schwabagent.schwab_client import Quote
from schwabagent.strategies.base import Signal, SIGNAL_SCORE


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_config(**overrides) -> Config:
    """Create a Config with safe defaults for testing."""
    defaults = {
        "DRY_RUN": True,
        "STATE_DIR": "/tmp/schwab-test-filters",
    }
    defaults.update(overrides)
    return Config(**defaults)


def _mock_client():
    client = MagicMock()
    client.get_quotes.return_value = {}
    client.get_ohlcv.return_value = pd.DataFrame()
    client.get_intraday_ohlcv.return_value = pd.DataFrame()
    return client


def _mock_risk():
    risk = MagicMock()
    risk.can_buy.return_value = (True, "")
    risk.is_killed.return_value = False
    return risk


def _mock_state():
    state = MagicMock()
    state.get_strategy_pnl.return_value = {}
    return state


@dataclass
class FakeAccount:
    account_hash: str = "test_hash"
    cash_available: float = 100_000.0
    total_value: float = 200_000.0
    unsettled_cash: float = 0.0
    positions: list = None

    def __post_init__(self):
        if self.positions is None:
            self.positions = []


def _make_quote(symbol="SPY", bid=500.0, ask=500.05, last=500.02,
                volume=50_000_000, avg_10d_volume=40_000_000,
                next_div_ex_date="", next_div_amount=0.0, pe_ratio=0.0):
    """Build a Quote with computed spread fields."""
    spread = ask - bid
    mid = (bid + ask) / 2 if (bid + ask) > 0 else 0.0
    spread_pct = (spread / mid * 100) if mid > 0 else 0.0
    return Quote(
        symbol=symbol,
        bid=bid,
        ask=ask,
        last=last,
        volume=volume,
        change_pct=0.5,
        spread=round(spread, 6),
        spread_pct=round(spread_pct, 6),
        avg_10d_volume=avg_10d_volume,
        next_div_ex_date=next_div_ex_date,
        next_div_amount=next_div_amount,
        pe_ratio=pe_ratio,
    )


def _make_ohlcv(n=260, base_price=150.0, trend=0.1):
    """Generate a synthetic OHLCV DataFrame for scoring."""
    dates = pd.date_range("2023-01-01", periods=n, freq="B", tz="UTC")
    close = base_price + trend * np.arange(n) + np.random.default_rng(42).normal(0, 0.5, n).cumsum()
    close = np.maximum(close, 1.0)
    return pd.DataFrame({
        "open": close * 0.999,
        "high": close * 1.005,
        "low": close * 0.995,
        "close": close,
        "volume": np.random.default_rng(42).integers(1_000_000, 10_000_000, n),
    }, index=dates)


# ═══════════════════════════════════════════════════════════════════════════════
# 1. Quote spread calculation tests
# ═══════════════════════════════════════════════════════════════════════════════

class TestSpreadCalculation:

    def test_spread_basic(self):
        """Spread = ask - bid."""
        q = _make_quote(bid=100.0, ask=100.10)
        assert abs(q.spread - 0.10) < 1e-4

    def test_spread_pct_basic(self):
        """spread_pct = spread / mid * 100."""
        q = _make_quote(bid=100.0, ask=100.10)
        # mid = 100.05, spread = 0.10 → spread_pct = 0.10/100.05*100 ≈ 0.0999%
        assert abs(q.spread_pct - 0.0999) < 0.01

    def test_spread_zero_when_equal(self):
        """If bid == ask, spread is 0."""
        q = _make_quote(bid=200.0, ask=200.0)
        assert q.spread == 0.0
        assert q.spread_pct == 0.0

    def test_spread_pct_wide(self):
        """A wide spread should have a large spread_pct."""
        q = _make_quote(bid=50.0, ask=51.0)
        # mid = 50.5, spread = 1.0 → spread_pct ≈ 1.98%
        assert q.spread_pct > 1.0

    def test_quote_fundamental_defaults(self):
        """Quote fields default to 0/empty when not provided."""
        q = Quote(symbol="TEST", bid=10.0, ask=10.05, last=10.02, volume=100, change_pct=0.1)
        assert q.avg_10d_volume == 0
        assert q.next_div_ex_date == ""
        assert q.next_div_amount == 0.0
        assert q.pe_ratio == 0.0
        assert q.spread == 0.0
        assert q.spread_pct == 0.0


# ═══════════════════════════════════════════════════════════════════════════════
# 2. Config defaults tests
# ═══════════════════════════════════════════════════════════════════════════════

class TestConfigDefaults:

    def test_scalp_min_avg_volume_default(self):
        cfg = _make_config()
        assert cfg.SCALP_MIN_AVG_VOLUME == 1_000_000

    def test_scalp_max_spread_pct_default(self):
        cfg = _make_config()
        assert cfg.SCALP_MAX_SPREAD_PCT == 0.10

    def test_etf_dividend_lookforward_days_default(self):
        cfg = _make_config()
        assert cfg.ETF_DIVIDEND_LOOKFORWARD_DAYS == 3

    def test_config_overrides(self):
        cfg = _make_config(
            SCALP_MIN_AVG_VOLUME=500_000,
            SCALP_MAX_SPREAD_PCT=0.05,
            ETF_DIVIDEND_LOOKFORWARD_DAYS=5,
        )
        assert cfg.SCALP_MIN_AVG_VOLUME == 500_000
        assert cfg.SCALP_MAX_SPREAD_PCT == 0.05
        assert cfg.ETF_DIVIDEND_LOOKFORWARD_DAYS == 5


# ═══════════════════════════════════════════════════════════════════════════════
# 3. Liquidity filter tests (ETF Scalp)
# ═══════════════════════════════════════════════════════════════════════════════

class TestLiquidityFilter:

    def _make_scalp_strategy(self, config=None, quotes=None):
        from schwabagent.strategies.etf_scalp import ETFScalpStrategy

        cfg = config or _make_config(SCALP_UNIVERSE="SPY,QQQ,IWM")
        client = _mock_client()
        if quotes is not None:
            client.get_quotes.return_value = quotes
        risk = _mock_risk()
        state = _mock_state()
        strat = ETFScalpStrategy(client, cfg, risk, state, account=FakeAccount())
        strat._init_tranches(FakeAccount())
        return strat, client

    def _patch_session_time(self, hour=10, minute=0):
        """Return a context manager that patches datetime in etf_scalp to return a valid trading time."""
        import zoneinfo
        eastern = zoneinfo.ZoneInfo("US/Eastern")
        # Build a real datetime that's within session hours
        fake_now = datetime(2025, 6, 15, hour, minute, tzinfo=eastern)

        return patch(
            "schwabagent.strategies.etf_scalp.datetime",
            wraps=datetime,
            **{"now.return_value": fake_now},
        )

    def test_low_volume_filtered(self):
        """Symbol with avg volume below threshold gets skipped."""
        low_vol_quote = _make_quote("SPY", avg_10d_volume=500_000)  # below 1M default
        good_quote = _make_quote("QQQ", avg_10d_volume=5_000_000)
        iwm_quote = _make_quote("IWM", avg_10d_volume=3_000_000)
        quotes = {"SPY": low_vol_quote, "QQQ": good_quote, "IWM": iwm_quote}

        strat, client = self._make_scalp_strategy(quotes=quotes)

        with self._patch_session_time():
            strat.scan()

        # SPY should have been filtered — _evaluate_entry should NOT be called for SPY
        called_symbols = [
            call.args[0] for call in client.get_intraday_ohlcv.call_args_list
        ]
        assert "SPY" not in called_symbols

    def test_wide_spread_filtered(self):
        """Symbol with spread_pct above threshold gets skipped."""
        wide_spread = _make_quote("SPY", bid=100.0, ask=101.0, avg_10d_volume=5_000_000)
        # spread_pct ≈ 0.995% — above 0.10% default
        assert wide_spread.spread_pct > 0.10

        narrow_spread = _make_quote("QQQ", bid=400.0, ask=400.02, avg_10d_volume=5_000_000)
        iwm_quote = _make_quote("IWM", bid=200.0, ask=200.01, avg_10d_volume=5_000_000)

        quotes = {"SPY": wide_spread, "QQQ": narrow_spread, "IWM": iwm_quote}
        strat, client = self._make_scalp_strategy(quotes=quotes)

        with self._patch_session_time():
            strat.scan()

        called_symbols = [
            call.args[0] for call in client.get_intraday_ohlcv.call_args_list
        ]
        assert "SPY" not in called_symbols

    def test_good_liquidity_passes(self):
        """Symbol that passes both checks is evaluated for entry."""
        good_quote = _make_quote("SPY", bid=500.0, ask=500.02, avg_10d_volume=50_000_000)
        quotes = {"SPY": good_quote}

        cfg = _make_config(SCALP_UNIVERSE="SPY")
        strat, client = self._make_scalp_strategy(config=cfg, quotes=quotes)

        with self._patch_session_time():
            strat.scan()

        # SPY should have been evaluated (get_intraday_ohlcv called)
        assert client.get_intraday_ohlcv.called
        called_symbols = [call.args[0] for call in client.get_intraday_ohlcv.call_args_list]
        assert "SPY" in called_symbols

    def test_custom_volume_threshold(self):
        """Custom SCALP_MIN_AVG_VOLUME overrides default."""
        # 800k volume — above 500k custom threshold, below 1M default
        quote = _make_quote("SPY", avg_10d_volume=800_000)
        quotes = {"SPY": quote}

        cfg = _make_config(SCALP_UNIVERSE="SPY", SCALP_MIN_AVG_VOLUME=500_000)
        strat, client = self._make_scalp_strategy(config=cfg, quotes=quotes)

        with self._patch_session_time():
            strat.scan()

        called_symbols = [call.args[0] for call in client.get_intraday_ohlcv.call_args_list]
        assert "SPY" in called_symbols

    def test_custom_spread_threshold(self):
        """Custom SCALP_MAX_SPREAD_PCT overrides default."""
        # spread_pct ≈ 0.04% — below 0.05 custom threshold
        quote = _make_quote("SPY", bid=500.0, ask=500.20, avg_10d_volume=5_000_000)
        # spread = 0.20, mid = 500.10, pct = 0.04%
        assert quote.spread_pct < 0.05

        quotes = {"SPY": quote}
        cfg = _make_config(SCALP_UNIVERSE="SPY", SCALP_MAX_SPREAD_PCT=0.05)
        strat, client = self._make_scalp_strategy(config=cfg, quotes=quotes)

        with self._patch_session_time():
            strat.scan()

        called_symbols = [call.args[0] for call in client.get_intraday_ohlcv.call_args_list]
        assert "SPY" in called_symbols


# ═══════════════════════════════════════════════════════════════════════════════
# 4. Dividend filter tests (ETF Rotation)
# ═══════════════════════════════════════════════════════════════════════════════

class TestDividendFilter:

    def _make_rotation_strategy(self, config=None, quotes=None, ohlcv=None):
        from schwabagent.strategies.etf_rotation import ETFRotationStrategy

        cfg = config or _make_config(
            ETF_UNIVERSE="SPY,QQQ,IWM",
            ETF_TOP_N=2,
            ETF_BEAR_FILTER=False,
        )
        client = _mock_client()
        if quotes is not None:
            client.get_quotes.return_value = quotes
        if ohlcv is not None:
            client.get_ohlcv.return_value = ohlcv
        else:
            client.get_ohlcv.return_value = _make_ohlcv()
        risk = _mock_risk()
        state = _mock_state()
        strat = ETFRotationStrategy(client, cfg, risk, state, account=FakeAccount())
        return strat, client

    def test_near_exdiv_score_reduced(self):
        """ETF near ex-div date gets score reduced by 50%."""
        from schwabagent.strategies.etf_rotation import ETFRotationStrategy

        tomorrow = (datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%d")
        quotes = {
            "SPY": _make_quote("SPY", next_div_ex_date=tomorrow, avg_10d_volume=50_000_000),
            "QQQ": _make_quote("QQQ", avg_10d_volume=30_000_000),
            "IWM": _make_quote("IWM", avg_10d_volume=20_000_000),
        }

        strat, client = self._make_rotation_strategy(quotes=quotes)
        opps = strat.scan()

        # Find SPY in results — it should have a div_note
        spy_opp = next((o for o in opps if o["symbol"] == "SPY"), None)
        if spy_opp:
            assert "div_note" in spy_opp
            assert "Near ex-div" in spy_opp["div_note"]

    def test_far_exdiv_unaffected(self):
        """ETF with ex-div date far in the future is not penalized."""
        far_date = (datetime.now(timezone.utc) + timedelta(days=30)).strftime("%Y-%m-%d")
        quotes = {
            "SPY": _make_quote("SPY", next_div_ex_date=far_date, avg_10d_volume=50_000_000),
            "QQQ": _make_quote("QQQ", avg_10d_volume=30_000_000),
            "IWM": _make_quote("IWM", avg_10d_volume=20_000_000),
        }

        strat, client = self._make_rotation_strategy(quotes=quotes)
        opps = strat.scan()

        spy_opp = next((o for o in opps if o["symbol"] == "SPY"), None)
        if spy_opp:
            assert "div_note" not in spy_opp or spy_opp.get("div_note", "") == ""

    def test_no_exdiv_date_unaffected(self):
        """ETF with no ex-div date is not penalized."""
        quotes = {
            "SPY": _make_quote("SPY", next_div_ex_date="", avg_10d_volume=50_000_000),
            "QQQ": _make_quote("QQQ", avg_10d_volume=30_000_000),
            "IWM": _make_quote("IWM", avg_10d_volume=20_000_000),
        }

        strat, client = self._make_rotation_strategy(quotes=quotes)
        opps = strat.scan()

        spy_opp = next((o for o in opps if o["symbol"] == "SPY"), None)
        if spy_opp:
            assert "div_note" not in spy_opp or spy_opp.get("div_note", "") == ""

    def test_exdiv_today_reduces_score(self):
        """Ex-div date today (0 days away) should reduce score."""
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        quotes = {
            "SPY": _make_quote("SPY", next_div_ex_date=today, avg_10d_volume=50_000_000),
            "QQQ": _make_quote("QQQ", avg_10d_volume=30_000_000),
            "IWM": _make_quote("IWM", avg_10d_volume=20_000_000),
        }

        strat, client = self._make_rotation_strategy(quotes=quotes)
        opps = strat.scan()

        spy_opp = next((o for o in opps if o["symbol"] == "SPY"), None)
        if spy_opp:
            assert "div_note" in spy_opp
            assert "score reduced" in spy_opp["div_note"]

    def test_exdiv_boundary_day(self):
        """Ex-div date exactly ETF_DIVIDEND_LOOKFORWARD_DAYS away should be caught."""
        boundary = (datetime.now(timezone.utc) + timedelta(days=3)).strftime("%Y-%m-%d")
        quotes = {
            "SPY": _make_quote("SPY", next_div_ex_date=boundary, avg_10d_volume=50_000_000),
            "QQQ": _make_quote("QQQ", avg_10d_volume=30_000_000),
            "IWM": _make_quote("IWM", avg_10d_volume=20_000_000),
        }

        strat, client = self._make_rotation_strategy(quotes=quotes)
        opps = strat.scan()

        spy_opp = next((o for o in opps if o["symbol"] == "SPY"), None)
        if spy_opp:
            assert "div_note" in spy_opp

    def test_exdiv_just_past_boundary(self):
        """Ex-div date 4 days away (default lookforward=3) should NOT be caught."""
        past_boundary = (datetime.now(timezone.utc) + timedelta(days=4)).strftime("%Y-%m-%d")
        quotes = {
            "SPY": _make_quote("SPY", next_div_ex_date=past_boundary, avg_10d_volume=50_000_000),
            "QQQ": _make_quote("QQQ", avg_10d_volume=30_000_000),
            "IWM": _make_quote("IWM", avg_10d_volume=20_000_000),
        }

        strat, client = self._make_rotation_strategy(quotes=quotes)
        opps = strat.scan()

        spy_opp = next((o for o in opps if o["symbol"] == "SPY"), None)
        if spy_opp:
            assert "div_note" not in spy_opp or spy_opp.get("div_note", "") == ""

    def test_div_filter_disabled_when_zero(self):
        """When ETF_DIVIDEND_LOOKFORWARD_DAYS=0, dividend filter is skipped."""
        tomorrow = (datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%d")
        quotes = {
            "SPY": _make_quote("SPY", next_div_ex_date=tomorrow, avg_10d_volume=50_000_000),
            "QQQ": _make_quote("QQQ", avg_10d_volume=30_000_000),
            "IWM": _make_quote("IWM", avg_10d_volume=20_000_000),
        }

        cfg = _make_config(
            ETF_UNIVERSE="SPY,QQQ,IWM",
            ETF_TOP_N=2,
            ETF_BEAR_FILTER=False,
            ETF_DIVIDEND_LOOKFORWARD_DAYS=0,
        )
        strat, client = self._make_rotation_strategy(config=cfg, quotes=quotes)
        opps = strat.scan()

        # get_quotes should NOT be called for dividend check when disabled
        # (it may still be called 0 times for div filter since div_lookforward=0)
        spy_opp = next((o for o in opps if o["symbol"] == "SPY"), None)
        if spy_opp:
            assert "div_note" not in spy_opp or spy_opp.get("div_note", "") == ""


# ═══════════════════════════════════════════════════════════════════════════════
# 5. Quote parsing from API response tests
# ═══════════════════════════════════════════════════════════════════════════════

class TestQuoteParsing:

    def test_quote_with_fundamentals(self):
        """get_quotes correctly extracts fundamental data from nested response."""
        from schwabagent.schwab_client import SchwabClient

        mock_resp = MagicMock()
        mock_resp.raise_for_status.return_value = None
        mock_resp.json.return_value = {
            "SPY": {
                "quote": {
                    "bidPrice": 500.0,
                    "askPrice": 500.10,
                    "lastPrice": 500.05,
                    "totalVolume": 80_000_000,
                    "netPercentChangeInDouble": 0.5,
                },
                "fundamental": {
                    "avg10DaysVolume": 75_000_000,
                    "nextDivExDate": "2025-03-15",
                    "nextDivAmount": 1.50,
                    "peRatio": 22.5,
                },
            }
        }

        cfg = _make_config()
        client = SchwabClient(cfg)
        client._market_client = MagicMock()
        client._market_client.get_quotes.return_value = mock_resp

        quotes = client.get_quotes(["SPY"])
        q = quotes["SPY"]
        assert q.avg_10d_volume == 75_000_000
        assert q.next_div_ex_date == "2025-03-15"
        assert q.next_div_amount == 1.50
        assert q.pe_ratio == 22.5
        assert abs(q.spread - 0.10) < 1e-4
        assert q.spread_pct > 0

    def test_quote_without_fundamentals(self):
        """get_quotes handles missing fundamental section gracefully."""
        from schwabagent.schwab_client import SchwabClient

        mock_resp = MagicMock()
        mock_resp.raise_for_status.return_value = None
        mock_resp.json.return_value = {
            "SPY": {
                "quote": {
                    "bidPrice": 500.0,
                    "askPrice": 500.10,
                    "lastPrice": 500.05,
                    "totalVolume": 80_000_000,
                    "netPercentChangeInDouble": 0.5,
                },
                # No "fundamental" key
            }
        }

        cfg = _make_config()
        client = SchwabClient(cfg)
        client._market_client = MagicMock()
        client._market_client.get_quotes.return_value = mock_resp

        quotes = client.get_quotes(["SPY"])
        q = quotes["SPY"]
        assert q.avg_10d_volume == 0
        assert q.next_div_ex_date == ""
        assert q.next_div_amount == 0.0
        assert q.pe_ratio == 0.0

    def test_quote_flat_schema(self):
        """get_quotes handles flat response (no nested 'quote' key)."""
        from schwabagent.schwab_client import SchwabClient

        mock_resp = MagicMock()
        mock_resp.raise_for_status.return_value = None
        mock_resp.json.return_value = {
            "SPY": {
                "bidPrice": 500.0,
                "askPrice": 500.10,
                "lastPrice": 500.05,
                "totalVolume": 80_000_000,
                "netPercentChangeInDouble": 0.5,
            }
        }

        cfg = _make_config()
        client = SchwabClient(cfg)
        client._market_client = MagicMock()
        client._market_client.get_quotes.return_value = mock_resp

        quotes = client.get_quotes(["SPY"])
        q = quotes["SPY"]
        assert q.bid == 500.0
        assert q.ask == 500.10
        assert abs(q.spread - 0.10) < 1e-4
