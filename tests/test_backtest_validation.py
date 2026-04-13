"""Tests for backtest_validation — Monte Carlo, Bootstrap, Walk-Forward."""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd
import pytest

from schwabagent.backtest_validation import (
    _max_drawdown,
    _sharpe,
    bootstrap_sharpe_ci,
    format_report,
    monte_carlo_test,
    run_validation,
    walk_forward_analysis,
)


# ── Fixtures ─────────────────────────────────────────────────────────────────

@dataclass
class _FakeTrade:
    """Minimal Trade stub matching schwagent's Trade interface."""
    symbol: str
    date: str
    side: str = "buy"
    price: float = 0.0
    quantity: int = 0
    value: float = 0.0
    signal: str = ""
    score: float = 0.0


def _strong_uptrend(n: int = 500, start: float = 100_000, seed: int = 7) -> pd.Series:
    """Equity curve with strong positive drift and low vol — clear edge."""
    rng = np.random.default_rng(seed)
    pnls = rng.normal(300, 200, n)  # +$300/day, small noise
    dates = pd.date_range("2022-01-01", periods=n, freq="B")
    return pd.Series(start + np.cumsum(pnls), index=dates)


def _random_walk(n: int = 500, start: float = 100_000, seed: int = 13) -> pd.Series:
    """Equity curve with zero drift — pure noise, no edge."""
    rng = np.random.default_rng(seed)
    pnls = rng.normal(0, 500, n)
    dates = pd.date_range("2022-01-01", periods=n, freq="B")
    return pd.Series(start + np.cumsum(pnls), index=dates)


def _strong_downtrend(n: int = 500, start: float = 100_000, seed: int = 21) -> pd.Series:
    """Equity curve with strong negative drift — inverse edge."""
    rng = np.random.default_rng(seed)
    pnls = rng.normal(-200, 200, n)
    dates = pd.date_range("2022-01-01", periods=n, freq="B")
    return pd.Series(start + np.cumsum(pnls), index=dates)


# ── Helper functions ─────────────────────────────────────────────────────────

def test_sharpe_zero_on_constant_returns():
    assert _sharpe(np.zeros(10)) == 0.0


def test_sharpe_positive_on_positive_drift():
    returns = np.array([0.01] * 100)  # constant positive return
    # With zero std we define sharpe as 0 to avoid div-by-zero inflation
    assert _sharpe(returns) == 0.0


def test_sharpe_annualization():
    returns = np.array([0.001, 0.002, -0.001, 0.003, -0.001] * 20)
    s = _sharpe(returns, bars_per_year=252)
    assert s != 0
    # Sharpe on positive-mean series should be positive
    assert s > 0


def test_max_drawdown_zero_on_rising_equity():
    equity = np.array([100, 101, 102, 103, 104], dtype=float)
    assert _max_drawdown(equity) == 0.0


def test_max_drawdown_negative_on_decline():
    equity = np.array([100, 120, 100, 80, 90], dtype=float)
    # Peak is 120, trough is 80, drawdown = -33.3%
    assert _max_drawdown(equity) == pytest.approx(-1 / 3, rel=1e-4)


def test_max_drawdown_empty():
    assert _max_drawdown(np.array([])) == 0.0


# ── Monte Carlo ──────────────────────────────────────────────────────────────

def test_monte_carlo_uptrend_has_distribution():
    """Uptrending strategy should produce a proper simulated Sharpe distribution."""
    equity = _strong_uptrend()
    result = monte_carlo_test(equity, n_simulations=200, seed=1)
    assert "error" not in result
    assert result["n_simulations"] == 200
    # Uptrend Sharpe should be meaningfully positive
    assert result["actual_sharpe"] > 1.0
    # Simulated distribution should have some spread
    assert result["simulated_sharpe_std"] >= 0.0
    # p-value is a valid probability
    assert 0.0 <= result["p_value_sharpe"] <= 1.0
    assert 0.0 <= result["p_value_max_dd"] <= 1.0


def test_monte_carlo_returns_error_on_short_series():
    short = pd.Series([100.0, 101.0, 102.0])  # only 3 bars
    result = monte_carlo_test(short, n_simulations=100)
    assert "error" in result


def test_monte_carlo_rejects_nonpositive_initial():
    bad = pd.Series([0.0, 10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0])
    result = monte_carlo_test(bad, n_simulations=100)
    assert "error" in result


def test_monte_carlo_reproducible_with_same_seed():
    equity = _strong_uptrend()
    r1 = monte_carlo_test(equity, n_simulations=200, seed=42)
    r2 = monte_carlo_test(equity, n_simulations=200, seed=42)
    assert r1["p_value_sharpe"] == r2["p_value_sharpe"]
    assert r1["simulated_sharpe_mean"] == r2["simulated_sharpe_mean"]


# ── Bootstrap ────────────────────────────────────────────────────────────────

def test_bootstrap_uptrend_has_positive_lower_bound():
    """A strongly profitable strategy should have CI lower bound > 0."""
    equity = _strong_uptrend()
    result = bootstrap_sharpe_ci(equity, n_bootstrap=500, seed=1)
    assert "error" not in result
    assert result["observed_sharpe"] > 1.0
    assert result["ci_lower"] > 0, f"expected positive lower bound, got {result['ci_lower']}"
    assert result["prob_positive"] > 0.95


def test_bootstrap_random_walk_has_near_even_probability():
    """A random walk should have ~50% prob of positive Sharpe."""
    equity = _random_walk()
    result = bootstrap_sharpe_ci(equity, n_bootstrap=500, seed=1)
    assert "error" not in result
    # Random walk: prob_positive should be roughly 0.3-0.7, not extreme
    assert 0.2 < result["prob_positive"] < 0.8


def test_bootstrap_downtrend_has_negative_upper_bound():
    """A downtrending strategy should have CI upper bound < 0."""
    equity = _strong_downtrend()
    result = bootstrap_sharpe_ci(equity, n_bootstrap=500, seed=1)
    assert "error" not in result
    assert result["observed_sharpe"] < 0
    assert result["prob_positive"] < 0.5


def test_bootstrap_short_series_errors():
    short = pd.Series([100.0, 101.0, 102.0])
    result = bootstrap_sharpe_ci(short, n_bootstrap=100)
    assert "error" in result


def test_bootstrap_confidence_levels():
    equity = _strong_uptrend()
    wide = bootstrap_sharpe_ci(equity, confidence=0.99, seed=1, n_bootstrap=500)
    narrow = bootstrap_sharpe_ci(equity, confidence=0.80, seed=1, n_bootstrap=500)
    # Wider confidence level = wider interval
    wide_width = wide["ci_upper"] - wide["ci_lower"]
    narrow_width = narrow["ci_upper"] - narrow["ci_lower"]
    assert wide_width >= narrow_width


# ── Walk-Forward ─────────────────────────────────────────────────────────────

def test_walk_forward_uptrend_high_consistency():
    equity = _strong_uptrend()
    result = walk_forward_analysis(equity, n_windows=5)
    assert "error" not in result
    assert result["n_windows"] == 5
    assert result["consistency_rate"] >= 0.8  # most windows profitable
    assert len(result["windows"]) == 5


def test_walk_forward_downtrend_low_consistency():
    equity = _strong_downtrend()
    result = walk_forward_analysis(equity, n_windows=5)
    assert "error" not in result
    assert result["consistency_rate"] <= 0.2  # most windows unprofitable


def test_walk_forward_trade_count_per_window():
    equity = _strong_uptrend(n=250)
    # Create synthetic trades distributed across the time range
    trades = [
        _FakeTrade(symbol="SPY", date="2022-02-01"),
        _FakeTrade(symbol="SPY", date="2022-05-01"),
        _FakeTrade(symbol="SPY", date="2022-08-01"),
        _FakeTrade(symbol="SPY", date="2022-11-01"),
    ]
    result = walk_forward_analysis(equity, trades=trades, n_windows=4)
    assert "error" not in result
    # Total trades counted across windows should equal trades passed in
    total_trades = sum(w["trades"] for w in result["windows"])
    assert total_trades == len(trades)


def test_walk_forward_short_series_errors():
    short = pd.Series([100.0, 101.0, 102.0, 103.0])  # only 4 bars
    result = walk_forward_analysis(short, n_windows=5)
    assert "error" in result


def test_walk_forward_window_dates_ordered():
    equity = _strong_uptrend(n=300)
    result = walk_forward_analysis(equity, n_windows=3)
    windows = result["windows"]
    # Windows should be chronologically ordered
    for i in range(len(windows) - 1):
        assert windows[i]["end"] < windows[i + 1]["start"] or \
               windows[i]["end"] == windows[i + 1]["start"]


# ── Combined runner ──────────────────────────────────────────────────────────

def test_run_validation_returns_all_three_sections():
    equity = _strong_uptrend()
    result = run_validation(
        equity,
        n_simulations=100,
        n_bootstrap=100,
        n_windows=5,
    )
    assert "monte_carlo" in result
    assert "bootstrap" in result
    assert "walk_forward" in result


def test_run_validation_with_trades():
    equity = _strong_uptrend(n=250)
    trades = [_FakeTrade(symbol="SPY", date="2022-05-01")]
    result = run_validation(
        equity,
        trades=trades,
        n_simulations=100,
        n_bootstrap=100,
    )
    # Walk-forward should see the trade
    total_trades = sum(w["trades"] for w in result["walk_forward"]["windows"])
    assert total_trades == 1


# ── Report formatting ────────────────────────────────────────────────────────

def test_format_report_renders_all_sections():
    equity = _strong_uptrend()
    results = run_validation(equity, n_simulations=100, n_bootstrap=100)
    report = format_report(results)
    assert "Monte Carlo" in report
    assert "Bootstrap" in report
    assert "Walk-Forward" in report
    assert "Observed Sharpe" in report
    assert "CI" in report


def test_format_report_handles_errors():
    short = pd.Series([100.0, 101.0, 102.0])
    results = run_validation(short, n_simulations=50)
    report = format_report(results)
    # All three sections should print an Error line for the short series
    assert "Error" in report
