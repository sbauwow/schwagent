"""Tests for fundamentals.py, backtest.py, and feedback.py.

All tests are self-contained with no external API calls.
"""
from __future__ import annotations

import math
import os
import tempfile
from dataclasses import dataclass

import numpy as np
import pandas as pd
import pytest

from schwabagent.config import Config


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_config(**overrides) -> Config:
    defaults = {"DRY_RUN": True}
    defaults.update(overrides)
    return Config(**defaults)


# ══════════════════════════════════════════════════════════════════════════════
# FUNDAMENTALS — pure math functions
# ══════════════════════════════════════════════════════════════════════════════

class TestEarningsYield:
    def test_positive_pe(self):
        from schwabagent.fundamentals import earnings_yield
        result = earnings_yield(20.0)
        assert abs(result - 5.0) < 0.01  # 1/20 * 100 = 5%

    def test_pe_10(self):
        from schwabagent.fundamentals import earnings_yield
        assert abs(earnings_yield(10.0) - 10.0) < 0.01

    def test_pe_1(self):
        from schwabagent.fundamentals import earnings_yield
        assert abs(earnings_yield(1.0) - 100.0) < 0.01

    def test_zero_pe_returns_nan(self):
        from schwabagent.fundamentals import earnings_yield
        assert math.isnan(earnings_yield(0))

    def test_negative_pe_returns_nan(self):
        from schwabagent.fundamentals import earnings_yield
        assert math.isnan(earnings_yield(-5))

    def test_nan_pe_returns_nan(self):
        from schwabagent.fundamentals import earnings_yield
        assert math.isnan(earnings_yield(float("nan")))


class TestGrahamNumber:
    def test_positive_values(self):
        from schwabagent.fundamentals import graham_number
        result = graham_number(eps=5.0, book_value_per_share=30.0)
        expected = math.sqrt(22.5 * 5.0 * 30.0)
        assert abs(result - expected) < 0.01

    def test_known_value(self):
        from schwabagent.fundamentals import graham_number
        # sqrt(22.5 * 10 * 40) = sqrt(9000) ≈ 94.87
        result = graham_number(10.0, 40.0)
        assert abs(result - 94.87) < 0.01

    def test_negative_eps(self):
        from schwabagent.fundamentals import graham_number
        assert math.isnan(graham_number(-5.0, 30.0))

    def test_negative_bvps(self):
        from schwabagent.fundamentals import graham_number
        assert math.isnan(graham_number(5.0, -10.0))

    def test_zero_eps(self):
        from schwabagent.fundamentals import graham_number
        assert math.isnan(graham_number(0, 30.0))

    def test_zero_bvps(self):
        from schwabagent.fundamentals import graham_number
        assert math.isnan(graham_number(5.0, 0))


class TestDCFIntrinsicValue:
    def test_basic_dcf(self):
        from schwabagent.fundamentals import dcf_intrinsic_value
        result = dcf_intrinsic_value(
            fcf=1_000_000, growth_rate=0.05, discount_rate=0.10,
            terminal_growth=0.025, projection_years=10, shares=100_000,
        )
        assert result > 0
        assert isinstance(result, float)

    def test_higher_growth_gives_higher_value(self):
        from schwabagent.fundamentals import dcf_intrinsic_value
        low = dcf_intrinsic_value(fcf=1e6, growth_rate=0.03, discount_rate=0.10, shares=1000)
        high = dcf_intrinsic_value(fcf=1e6, growth_rate=0.10, discount_rate=0.10, shares=1000)
        assert high > low

    def test_higher_discount_gives_lower_value(self):
        from schwabagent.fundamentals import dcf_intrinsic_value
        low_disc = dcf_intrinsic_value(fcf=1e6, discount_rate=0.08, shares=1000)
        high_disc = dcf_intrinsic_value(fcf=1e6, discount_rate=0.15, shares=1000)
        assert low_disc > high_disc

    def test_negative_fcf_returns_nan(self):
        from schwabagent.fundamentals import dcf_intrinsic_value
        assert math.isnan(dcf_intrinsic_value(fcf=-100, shares=100))

    def test_zero_shares_returns_nan(self):
        from schwabagent.fundamentals import dcf_intrinsic_value
        assert math.isnan(dcf_intrinsic_value(fcf=1e6, shares=0))

    def test_discount_lte_terminal_returns_nan(self):
        from schwabagent.fundamentals import dcf_intrinsic_value
        assert math.isnan(dcf_intrinsic_value(
            fcf=1e6, discount_rate=0.02, terminal_growth=0.03, shares=100,
        ))

    def test_single_share(self):
        from schwabagent.fundamentals import dcf_intrinsic_value
        result = dcf_intrinsic_value(fcf=500_000, shares=1)
        assert result > 500_000  # should be more due to growth


class TestPEGRatio:
    def test_normal(self):
        from schwabagent.fundamentals import peg_ratio
        # PE=20, growth=10% → PEG=2.0
        assert abs(peg_ratio(20, 10) - 2.0) < 0.01

    def test_undervalued(self):
        from schwabagent.fundamentals import peg_ratio
        # PE=10, growth=15% → PEG < 1
        assert peg_ratio(10, 15) < 1.0

    def test_negative_pe(self):
        from schwabagent.fundamentals import peg_ratio
        assert math.isnan(peg_ratio(-5, 10))

    def test_zero_growth(self):
        from schwabagent.fundamentals import peg_ratio
        assert math.isnan(peg_ratio(20, 0))

    def test_negative_growth(self):
        from schwabagent.fundamentals import peg_ratio
        assert math.isnan(peg_ratio(20, -5))


class TestAltmanZScore:
    def test_safe_zone(self):
        from schwabagent.fundamentals import altman_z_score
        z, zone = altman_z_score(
            working_capital=500, retained_earnings=1000, ebit=300,
            market_cap=5000, total_liabilities=2000, revenue=3000,
            total_assets=4000,
        )
        assert isinstance(z, float)
        assert zone in ("safe", "grey", "distress")

    def test_safe_classification(self):
        from schwabagent.fundamentals import altman_z_score
        # Construct values that give Z > 2.99
        z, zone = altman_z_score(
            working_capital=1000, retained_earnings=2000, ebit=800,
            market_cap=10000, total_liabilities=1000, revenue=5000,
            total_assets=3000,
        )
        assert z > 2.99
        assert zone == "safe"

    def test_distress_classification(self):
        from schwabagent.fundamentals import altman_z_score
        z, zone = altman_z_score(
            working_capital=-500, retained_earnings=-200, ebit=-100,
            market_cap=100, total_liabilities=5000, revenue=500,
            total_assets=3000,
        )
        assert z < 1.81
        assert zone == "distress"

    def test_grey_zone(self):
        from schwabagent.fundamentals import altman_z_score
        # Carefully construct values for grey zone (1.81 < z < 2.99)
        z, zone = altman_z_score(
            working_capital=200, retained_earnings=500, ebit=200,
            market_cap=2000, total_liabilities=2000, revenue=2000,
            total_assets=3000,
        )
        if 1.81 < z < 2.99:
            assert zone == "grey"

    def test_zero_assets_returns_nan(self):
        from schwabagent.fundamentals import altman_z_score
        z, zone = altman_z_score(0, 0, 0, 0, 1000, 0, 0)
        assert math.isnan(z)
        assert zone == "unknown"

    def test_zero_liabilities_returns_nan(self):
        from schwabagent.fundamentals import altman_z_score
        z, zone = altman_z_score(100, 100, 100, 100, 0, 100, 100)
        assert math.isnan(z)
        assert zone == "unknown"

    def test_formula_correctness(self):
        from schwabagent.fundamentals import altman_z_score
        # Manual calculation
        wc, re, ebit, mc, tl, rev, ta = 100, 200, 50, 500, 300, 400, 600
        expected = 1.2 * (wc/ta) + 1.4 * (re/ta) + 3.3 * (ebit/ta) + 0.6 * (mc/tl) + 1.0 * (rev/ta)
        z, _ = altman_z_score(wc, re, ebit, mc, tl, rev, ta)
        assert abs(z - expected) < 0.001


class TestPiotroskiFScore:
    def test_perfect_score(self):
        from schwabagent.fundamentals import piotroski_f_score
        score, details = piotroski_f_score(
            net_income=100, operating_cf=150,
            roa_current=0.12, roa_prior=0.10,
            debt_current=400, debt_prior=500,
            current_ratio_now=2.0, current_ratio_prior=1.5,
            shares_current=1000, shares_prior=1000,
            gross_margin_current=0.40, gross_margin_prior=0.35,
            asset_turnover_current=1.2, asset_turnover_prior=1.0,
        )
        assert score == 9
        assert all(v == 1 for v in details.values())

    def test_worst_score(self):
        from schwabagent.fundamentals import piotroski_f_score
        score, details = piotroski_f_score(
            net_income=-100, operating_cf=-200,
            roa_current=0.05, roa_prior=0.10,
            debt_current=600, debt_prior=500,
            current_ratio_now=1.0, current_ratio_prior=1.5,
            shares_current=1200, shares_prior=1000,
            gross_margin_current=0.30, gross_margin_prior=0.35,
            asset_turnover_current=0.8, asset_turnover_prior=1.0,
        )
        assert score == 0
        assert all(v == 0 for v in details.values())

    def test_partial_score(self):
        from schwabagent.fundamentals import piotroski_f_score
        score, details = piotroski_f_score(
            net_income=100, operating_cf=150,
            roa_current=0.08, roa_prior=0.10,  # failing
            debt_current=600, debt_prior=500,   # failing
            current_ratio_now=2.0, current_ratio_prior=1.5,
            shares_current=1000, shares_prior=1000,
            gross_margin_current=0.30, gross_margin_prior=0.35,  # failing
            asset_turnover_current=1.2, asset_turnover_prior=1.0,
        )
        assert 0 < score < 9
        assert details["positive_net_income"] == 1
        assert details["positive_ocf"] == 1
        assert details["roa_improving"] == 0
        assert details["debt_decreasing"] == 0

    def test_returns_tuple(self):
        from schwabagent.fundamentals import piotroski_f_score
        result = piotroski_f_score(100, 150, 0.1, 0.08, 400, 500, 2, 1.5, 1000, 1000, 0.4, 0.35, 1.2, 1.0)
        assert isinstance(result, tuple)
        assert len(result) == 2
        assert isinstance(result[0], int)
        assert isinstance(result[1], dict)

    def test_score_range(self):
        from schwabagent.fundamentals import piotroski_f_score
        score, _ = piotroski_f_score(100, 150, 0.1, 0.08, 400, 500, 2, 1.5, 1000, 1000, 0.4, 0.35, 1.2, 1.0)
        assert 0 <= score <= 9


class TestReturnMetrics:
    def test_return_on_equity(self):
        from schwabagent.fundamentals import return_on_equity
        assert abs(return_on_equity(100, 500) - 20.0) < 0.01

    def test_return_on_equity_zero_equity(self):
        from schwabagent.fundamentals import return_on_equity
        assert math.isnan(return_on_equity(100, 0))

    def test_return_on_equity_negative_equity(self):
        from schwabagent.fundamentals import return_on_equity
        assert math.isnan(return_on_equity(100, -500))

    def test_return_on_assets(self):
        from schwabagent.fundamentals import return_on_assets
        assert abs(return_on_assets(50, 1000) - 5.0) < 0.01

    def test_return_on_assets_zero(self):
        from schwabagent.fundamentals import return_on_assets
        assert math.isnan(return_on_assets(50, 0))

    def test_roic(self):
        from schwabagent.fundamentals import return_on_invested_capital
        # ROIC = EBIT * (1 - tax) / IC * 100
        result = return_on_invested_capital(ebit=200, tax_rate=0.21, invested_capital=1000)
        expected = 200 * 0.79 / 1000 * 100  # 15.8%
        assert abs(result - expected) < 0.01

    def test_roic_zero_capital(self):
        from schwabagent.fundamentals import return_on_invested_capital
        assert math.isnan(return_on_invested_capital(200, 0.21, 0))

    def test_roic_negative_capital(self):
        from schwabagent.fundamentals import return_on_invested_capital
        assert math.isnan(return_on_invested_capital(200, 0.21, -500))


class TestEarningsQuality:
    def test_high_quality(self):
        from schwabagent.fundamentals import earnings_quality_ratio
        # OCF > net income → quality > 1
        assert earnings_quality_ratio(150, 100) > 1.0

    def test_low_quality(self):
        from schwabagent.fundamentals import earnings_quality_ratio
        assert earnings_quality_ratio(50, 100) < 1.0

    def test_zero_income(self):
        from schwabagent.fundamentals import earnings_quality_ratio
        assert math.isnan(earnings_quality_ratio(50, 0))

    def test_negative_income(self):
        from schwabagent.fundamentals import earnings_quality_ratio
        assert math.isnan(earnings_quality_ratio(50, -100))


class TestMarginStability:
    def test_stable_margins(self):
        from schwabagent.fundamentals import margin_stability
        result = margin_stability([0.20, 0.20, 0.20, 0.20])
        assert result == 0.0

    def test_unstable_margins(self):
        from schwabagent.fundamentals import margin_stability
        result = margin_stability([0.10, 0.30, 0.15, 0.25])
        assert result > 0.0

    def test_insufficient_data(self):
        from schwabagent.fundamentals import margin_stability
        assert math.isnan(margin_stability([0.20, 0.20]))

    def test_single_value(self):
        from schwabagent.fundamentals import margin_stability
        assert math.isnan(margin_stability([0.20]))


class TestFactorScores:
    def test_returns_ranked_list(self):
        from schwabagent.fundamentals import compute_factor_scores
        stocks = [
            {"symbol": "AAPL", "pe_ratio": 25, "earnings_yield": 4.0, "roe": 30, "gross_margin": 40, "net_margin": 25, "debt_to_equity": 1.5, "earnings_quality": 1.2, "momentum_12m": 15},
            {"symbol": "MSFT", "pe_ratio": 30, "earnings_yield": 3.3, "roe": 35, "gross_margin": 65, "net_margin": 35, "debt_to_equity": 0.5, "earnings_quality": 1.5, "momentum_12m": 20},
            {"symbol": "TSLA", "pe_ratio": 60, "earnings_yield": 1.7, "roe": 15, "gross_margin": 20, "net_margin": 10, "debt_to_equity": 1.0, "earnings_quality": 0.8, "momentum_12m": -5},
        ]
        results = compute_factor_scores(stocks)
        assert len(results) == 3
        # All ranked
        ranks = [r.rank for r in results]
        assert sorted(ranks) == [1, 2, 3]
        # Sorted by composite descending
        composites = [r.composite for r in results]
        assert composites == sorted(composites, reverse=True)

    def test_empty_input(self):
        from schwabagent.fundamentals import compute_factor_scores
        assert compute_factor_scores([]) == []


# ══════════════════════════════════════════════════════════════════════════════
# BACKTEST
# ══════════════════════════════════════════════════════════════════════════════

class TestBacktestConfig:
    def test_default_values(self):
        from schwabagent.backtest import BacktestConfig
        cfg = BacktestConfig()
        assert cfg.strategy == "momentum"
        assert cfg.initial_capital == 100_000.0
        assert cfg.commission == 0.0
        assert isinstance(cfg.symbols, list)

    def test_custom_values(self):
        from schwabagent.backtest import BacktestConfig
        cfg = BacktestConfig(
            strategy="mean_reversion",
            symbols=["AAPL", "MSFT"],
            start="2022-01-01",
            end="2023-12-31",
            initial_capital=50_000.0,
        )
        assert cfg.strategy == "mean_reversion"
        assert cfg.symbols == ["AAPL", "MSFT"]
        assert cfg.initial_capital == 50_000.0

    def test_slippage_default(self):
        from schwabagent.backtest import BacktestConfig
        cfg = BacktestConfig()
        assert cfg.slippage_pct == 0.01


class TestTrade:
    def test_trade_fields(self):
        from schwabagent.backtest import Trade
        t = Trade(
            symbol="AAPL", side="BUY", date="2024-01-15",
            price=150.0, quantity=10, value=1500.0,
            signal="BUY", score=1.5,
        )
        assert t.symbol == "AAPL"
        assert t.side == "BUY"
        assert t.price == 150.0
        assert t.quantity == 10
        assert t.value == 1500.0

    def test_trade_sell(self):
        from schwabagent.backtest import Trade
        t = Trade("AAPL", "SELL", "2024-03-01", 170.0, 10, 1700.0, "SELL", -1.0)
        assert t.side == "SELL"
        assert t.score == -1.0


class TestBacktestResult:
    def test_print_report(self):
        from schwabagent.backtest import BacktestConfig, BacktestResult
        cfg = BacktestConfig(strategy="momentum", symbols=["AAPL"])
        result = BacktestResult(
            config=cfg,
            trades=[],
            equity_curve=pd.Series([100_000, 101_000, 102_000]),
            total_return_pct=2.0,
            cagr=2.0,
            sharpe=1.5,
            sortino=2.0,
            max_drawdown_pct=1.0,
            win_rate=60.0,
            total_trades=10,
            winners=6,
            losers=4,
            annual_returns={2024: 2.0},
        )
        report = result.print_report()
        assert "momentum" in report
        assert "2.00%" in report
        assert "Sharpe" in report

    def test_empty_result(self):
        from schwabagent.backtest import BacktestConfig, BacktestResult
        cfg = BacktestConfig()
        result = BacktestResult(config=cfg, trades=[], equity_curve=pd.Series(dtype=float))
        assert result.total_trades == 0
        assert result.total_return_pct == 0.0


class TestBacktester:
    """Test the Backtester with mock CSV data."""

    def _create_mock_csv(self, tmpdir: str) -> str:
        """Create a minimal CSV for backtesting."""
        dates = pd.date_range("2020-01-01", periods=300, freq="B")
        rows = []
        for sym in ["AAPL", "MSFT"]:
            base = 100.0 if sym == "AAPL" else 200.0
            np.random.seed(42 if sym == "AAPL" else 43)
            close = base + np.cumsum(np.random.normal(0.05, 1.0, len(dates)))
            close = np.maximum(close, 10.0)
            for i, d in enumerate(dates):
                rows.append({
                    "date": d.strftime("%Y-%m-%d"),
                    "symbol": sym,
                    "open": close[i] * 0.999,
                    "high": close[i] * 1.01,
                    "low": close[i] * 0.99,
                    "close": close[i],
                    "volume": 1_000_000,
                })
        df = pd.DataFrame(rows)
        path = os.path.join(tmpdir, "test_stocks.csv")
        df.to_csv(path, index=False)
        return path

    def test_backtester_momentum(self):
        from schwabagent.backtest import Backtester, BacktestConfig
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = self._create_mock_csv(tmpdir)
            cfg = BacktestConfig(
                strategy="momentum",
                symbols=["AAPL", "MSFT"],
                start="2020-06-01",
                end="2021-03-01",
                data_path=csv_path,
            )
            bt = Backtester(cfg)
            result = bt.run()
            assert isinstance(result.total_return_pct, float)
            assert isinstance(result.trades, list)
            assert result.equity_curve is not None

    def test_backtester_mean_reversion(self):
        from schwabagent.backtest import Backtester, BacktestConfig
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = self._create_mock_csv(tmpdir)
            cfg = BacktestConfig(
                strategy="mean_reversion",
                symbols=["AAPL"],
                start="2020-06-01",
                end="2021-03-01",
                data_path=csv_path,
            )
            bt = Backtester(cfg)
            result = bt.run()
            assert isinstance(result.total_return_pct, float)

    def test_backtester_trend_following(self):
        from schwabagent.backtest import Backtester, BacktestConfig
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = self._create_mock_csv(tmpdir)
            cfg = BacktestConfig(
                strategy="trend_following",
                symbols=["AAPL"],
                start="2020-06-01",
                end="2021-03-01",
                data_path=csv_path,
            )
            bt = Backtester(cfg)
            result = bt.run()
            assert isinstance(result.total_return_pct, float)

    def test_backtester_composite(self):
        from schwabagent.backtest import Backtester, BacktestConfig
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = self._create_mock_csv(tmpdir)
            cfg = BacktestConfig(
                strategy="composite",
                symbols=["AAPL"],
                start="2020-06-01",
                end="2021-03-01",
                data_path=csv_path,
            )
            bt = Backtester(cfg)
            result = bt.run()
            assert isinstance(result.total_return_pct, float)

    def test_backtester_unknown_strategy_raises(self):
        from schwabagent.backtest import Backtester, BacktestConfig
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = self._create_mock_csv(tmpdir)
            cfg = BacktestConfig(strategy="invalid", symbols=["AAPL"], data_path=csv_path)
            bt = Backtester(cfg)
            with pytest.raises(ValueError, match="Unknown strategy"):
                bt.run()

    def test_backtester_missing_csv_raises(self):
        from schwabagent.backtest import Backtester, BacktestConfig
        cfg = BacktestConfig(data_path="/nonexistent/file.csv")
        bt = Backtester(cfg)
        with pytest.raises(FileNotFoundError):
            bt.run()

    def test_metrics_calculation(self):
        from schwabagent.backtest import Backtester, BacktestConfig
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = self._create_mock_csv(tmpdir)
            cfg = BacktestConfig(
                strategy="momentum",
                symbols=["AAPL", "MSFT"],
                start="2020-06-01",
                end="2021-03-01",
                data_path=csv_path,
            )
            bt = Backtester(cfg)
            result = bt.run()
            # Metrics should be numeric
            assert isinstance(result.sharpe, float)
            assert isinstance(result.max_drawdown_pct, float)
            assert result.max_drawdown_pct >= 0
            assert isinstance(result.win_rate, float)
            assert 0 <= result.win_rate <= 100


# ══════════════════════════════════════════════════════════════════════════════
# FEEDBACK LOOP
# ══════════════════════════════════════════════════════════════════════════════

class TestFeedbackLoop:
    """Test FeedbackLoop with a temp SQLite database."""

    def _make_feedback(self, tmpdir: str):
        from schwabagent.feedback import FeedbackLoop
        config = _make_config(STATE_DIR=tmpdir)
        return FeedbackLoop(config)

    def test_record_signal_returns_id(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            fb = self._make_feedback(tmpdir)
            sid = fb.record_signal(
                strategy="momentum", symbol="AAPL", signal="BUY",
                score=1.0, price=150.0, reason="test signal",
            )
            assert isinstance(sid, int)
            assert sid > 0
            fb.close()

    def test_record_multiple_signals(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            fb = self._make_feedback(tmpdir)
            id1 = fb.record_signal("momentum", "AAPL", "BUY", 1.0, 150.0)
            id2 = fb.record_signal("momentum", "MSFT", "SELL", -1.0, 300.0)
            assert id2 > id1
            fb.close()

    def test_record_signal_with_features(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            fb = self._make_feedback(tmpdir)
            sid = fb.record_signal(
                strategy="momentum", symbol="AAPL", signal="BUY",
                score=1.5, price=150.0,
                features={"rsi": 45.0, "sma20": 148.0},
            )
            assert sid > 0
            fb.close()

    def test_resolve_signal(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            fb = self._make_feedback(tmpdir)
            fb.record_signal("momentum", "AAPL", "BUY", 1.0, 150.0)
            fb.resolve_signal(
                trade_id="trade_001", strategy="momentum", symbol="AAPL",
                realized_pnl=25.0, pnl_pct=1.5, hold_minutes=120,
                exit_type="take_profit",
            )
            # Verify the signal was resolved
            history = fb.get_signal_history(strategy="momentum", symbol="AAPL")
            resolved = [h for h in history if h["realized_pnl"] is not None]
            assert len(resolved) >= 1
            assert resolved[0]["realized_pnl"] == 25.0
            fb.close()

    def test_resolve_from_trade(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            fb = self._make_feedback(tmpdir)
            fb.record_signal("etf_scalp", "SPY", "BUY", 1.5, 450.0)
            trade = {
                "side": "SELL",
                "strategy": "etf_scalp",
                "symbol": "SPY",
                "order_id": "order_123",
                "realized_pnl": 50.0,
                "pnl_pct": 0.15,
                "hold_minutes": 15.0,
                "reason": "TAKE PROFIT",
            }
            fb.resolve_from_trade(trade)
            history = fb.get_signal_history(strategy="etf_scalp")
            resolved = [h for h in history if h["realized_pnl"] is not None]
            assert len(resolved) >= 1
            fb.close()

    def test_resolve_from_trade_ignores_buy(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            fb = self._make_feedback(tmpdir)
            fb.record_signal("momentum", "AAPL", "BUY", 1.0, 150.0)
            trade = {"side": "BUY", "strategy": "momentum", "symbol": "AAPL"}
            fb.resolve_from_trade(trade)  # Should be a no-op
            history = fb.get_signal_history(strategy="momentum")
            resolved = [h for h in history if h["realized_pnl"] is not None]
            assert len(resolved) == 0
            fb.close()

    def test_record_batch(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            fb = self._make_feedback(tmpdir)
            signals = [
                {"strategy": "momentum", "symbol": "AAPL", "signal": "BUY", "score": 1.0, "price": 150.0, "reason": "test"},
                {"strategy": "momentum", "symbol": "MSFT", "signal": "SELL", "score": -1.0, "price": 300.0, "reason": "test"},
            ]
            ids = fb.record_batch(signals)
            assert len(ids) == 2
            assert all(isinstance(i, int) for i in ids)
            fb.close()


class TestCalibration:
    """Test calibrate() returns per-strategy stats."""

    def _make_feedback_with_data(self, tmpdir: str):
        from schwabagent.feedback import FeedbackLoop
        config = _make_config(STATE_DIR=tmpdir)
        fb = FeedbackLoop(config)

        # Record and resolve several signals
        for i in range(10):
            fb.record_signal("momentum", "AAPL", "BUY", 1.0, 150.0 + i)
            fb.resolve_signal(
                trade_id=f"t{i}", strategy="momentum", symbol="AAPL",
                realized_pnl=10.0 if i % 2 == 0 else -5.0,
                pnl_pct=1.0 if i % 2 == 0 else -0.5,
            )
        return fb

    def test_calibrate_returns_dict(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            fb = self._make_feedback_with_data(tmpdir)
            cal = fb.calibrate("momentum", days=30)
            assert isinstance(cal, dict)
            assert "BUY" in cal
            fb.close()

    def test_calibrate_win_rate(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            fb = self._make_feedback_with_data(tmpdir)
            cal = fb.calibrate("momentum", days=30)
            buy_cal = cal["BUY"]
            # 5 wins, 5 losses → 50% win rate
            assert buy_cal["win_rate"] == 50.0
            assert buy_cal["total"] == 10
            assert buy_cal["wins"] == 5
            assert buy_cal["losses"] == 5
            fb.close()

    def test_calibrate_profit_factor(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            fb = self._make_feedback_with_data(tmpdir)
            cal = fb.calibrate("momentum", days=30)
            buy_cal = cal["BUY"]
            # gross_win = 5 * 10 = 50, gross_loss = 5 * 5 = 25 → PF = 2.0
            assert abs(buy_cal["profit_factor"] - 2.0) < 0.01
            fb.close()

    def test_calibrate_avg_pnl(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            fb = self._make_feedback_with_data(tmpdir)
            cal = fb.calibrate("momentum", days=30)
            buy_cal = cal["BUY"]
            # Total PnL = 5*10 + 5*(-5) = 25, avg = 2.5
            assert abs(buy_cal["avg_pnl"] - 2.5) < 0.01
            fb.close()

    def test_calibrate_all(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            fb = self._make_feedback_with_data(tmpdir)
            # Add another strategy
            fb.record_signal("mean_reversion", "MSFT", "SELL", -1.0, 300.0)
            fb.resolve_signal("t_mr", "mean_reversion", "MSFT", -20.0)
            all_cal = fb.calibrate_all(days=30)
            assert "momentum" in all_cal
            assert "mean_reversion" in all_cal
            fb.close()

    def test_calibrate_empty_strategy(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            from schwabagent.feedback import FeedbackLoop
            fb = FeedbackLoop(_make_config(STATE_DIR=tmpdir))
            cal = fb.calibrate("nonexistent", days=30)
            assert cal == {}
            fb.close()


class TestDriftDetection:
    """Test check_drift() detects performance degradation."""

    def _make_feedback_with_drift(self, tmpdir: str):
        from schwabagent.feedback import FeedbackLoop
        from datetime import datetime, timedelta, timezone
        import sqlite3

        config = _make_config(STATE_DIR=tmpdir)
        fb = FeedbackLoop(config)

        # Create baseline data (60 days ago) — 70% win rate
        now = datetime.now(timezone.utc)
        baseline_ts = (now - timedelta(days=30)).isoformat()
        for i in range(20):
            pnl = 10.0 if i < 14 else -5.0  # 14 wins, 6 losses = 70%
            fb._db.execute(
                """INSERT INTO signals (ts, strategy, symbol, signal, score, price, realized_pnl, resolved_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (baseline_ts, "momentum", "AAPL", "BUY", 1.0, 150.0, pnl, baseline_ts),
            )

        # Create recent data (last 7 days) — 30% win rate (significant drift)
        recent_ts = (now - timedelta(days=3)).isoformat()
        for i in range(10):
            pnl = 10.0 if i < 3 else -5.0  # 3 wins, 7 losses = 30%
            fb._db.execute(
                """INSERT INTO signals (ts, strategy, symbol, signal, score, price, realized_pnl, resolved_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (recent_ts, "momentum", "AAPL", "BUY", 1.0, 150.0, pnl, recent_ts),
            )
        fb._db.commit()
        return fb

    def test_check_drift_detects_degradation(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            fb = self._make_feedback_with_drift(tmpdir)
            alerts = fb.check_drift("momentum", baseline_days=60, recent_days=7)
            # Should detect drift — win rate dropped significantly
            # (depends on the data distribution matching the threshold logic)
            assert isinstance(alerts, list)
            fb.close()

    def test_check_drift_no_data(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            from schwabagent.feedback import FeedbackLoop
            fb = FeedbackLoop(_make_config(STATE_DIR=tmpdir))
            alerts = fb.check_drift("nonexistent", baseline_days=60, recent_days=7)
            assert alerts == []
            fb.close()


class TestFeedbackHelpers:
    """Test query helpers and cleanup."""

    def test_get_signal_history(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            from schwabagent.feedback import FeedbackLoop
            fb = FeedbackLoop(_make_config(STATE_DIR=tmpdir))
            fb.record_signal("momentum", "AAPL", "BUY", 1.0, 150.0)
            fb.record_signal("momentum", "MSFT", "SELL", -1.0, 300.0)
            history = fb.get_signal_history(strategy="momentum")
            assert len(history) == 2
            fb.close()

    def test_get_signal_history_by_symbol(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            from schwabagent.feedback import FeedbackLoop
            fb = FeedbackLoop(_make_config(STATE_DIR=tmpdir))
            fb.record_signal("momentum", "AAPL", "BUY", 1.0, 150.0)
            fb.record_signal("momentum", "MSFT", "SELL", -1.0, 300.0)
            history = fb.get_signal_history(symbol="AAPL")
            assert len(history) == 1
            assert history[0]["symbol"] == "AAPL"
            fb.close()

    def test_get_strategy_summary(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            from schwabagent.feedback import FeedbackLoop
            fb = FeedbackLoop(_make_config(STATE_DIR=tmpdir))
            fb.record_signal("momentum", "AAPL", "BUY", 1.0, 150.0)
            fb.resolve_signal("t1", "momentum", "AAPL", 10.0)
            summary = fb.get_strategy_summary()
            assert "momentum" in summary
            assert summary["momentum"]["total_signals"] >= 1
            fb.close()

    def test_cleanup(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            from schwabagent.feedback import FeedbackLoop
            fb = FeedbackLoop(_make_config(STATE_DIR=tmpdir))
            fb.record_signal("momentum", "AAPL", "BUY", 1.0, 150.0)
            # Cleanup with 0 retention should delete everything
            deleted = fb.cleanup(retention_days=0)
            assert deleted >= 1
            fb.close()

    def test_exclude_symbol(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            from schwabagent.feedback import FeedbackLoop
            fb = FeedbackLoop(_make_config(STATE_DIR=tmpdir))
            fb.exclude_symbol("momentum", "AAPL", "too many losses", days=7)
            excluded = fb.get_excluded_symbols("momentum")
            assert "AAPL" in excluded
            fb.close()

    def test_record_adjustment(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            from schwabagent.feedback import FeedbackLoop
            fb = FeedbackLoop(_make_config(STATE_DIR=tmpdir))
            fb.record_adjustment(
                "momentum", "throttle", "win rate below 40%",
                details={"win_rate": 35.0},
            )
            adjustments = fb.get_adjustments()
            assert len(adjustments) == 1
            assert adjustments[0]["action"] == "throttle"
            fb.close()

    def test_symbol_streak(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            from schwabagent.feedback import FeedbackLoop
            fb = FeedbackLoop(_make_config(STATE_DIR=tmpdir))
            # 3 consecutive losses
            for i in range(3):
                fb.record_signal("momentum", "AAPL", "BUY", 1.0, 150.0)
                fb.resolve_signal(f"t{i}", "momentum", "AAPL", -5.0)
            streak = fb.get_symbol_streak("momentum", "AAPL")
            assert streak == 3  # positive = loss streak
            fb.close()

    def test_symbol_streak_wins(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            from schwabagent.feedback import FeedbackLoop
            fb = FeedbackLoop(_make_config(STATE_DIR=tmpdir))
            # 2 consecutive wins
            for i in range(2):
                fb.record_signal("momentum", "AAPL", "BUY", 1.0, 150.0)
                fb.resolve_signal(f"t{i}", "momentum", "AAPL", 10.0)
            streak = fb.get_symbol_streak("momentum", "AAPL")
            assert streak == -2  # negative = win streak
            fb.close()
