"""Tests for the PyPortfolioOpt wrapper."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from schwabagent.portfolio_optimizer import (
    OptimizationResult,
    _build_price_frame,
    format_report,
    optimize_portfolio,
)


# ── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture
def synthetic_prices() -> pd.DataFrame:
    """Four assets, 300 days, different drift and vol profiles."""
    rng = np.random.default_rng(42)
    n = 300
    dates = pd.date_range("2022-01-01", periods=n, freq="B")
    # Mix of growth, bond proxy, gold proxy, high-vol equity
    trend = 400 * np.cumprod(1 + rng.normal(0.0005, 0.012, n))
    bond = 100 * np.cumprod(1 + rng.normal(0.0001, 0.008, n))
    gold = 180 * np.cumprod(1 + rng.normal(0.0003, 0.010, n))
    growth = 350 * np.cumprod(1 + rng.normal(0.0007, 0.015, n))
    return pd.DataFrame(
        {"SPY": trend, "TLT": bond, "GLD": gold, "QQQ": growth}, index=dates
    )


@pytest.fixture
def ohlcv_dict() -> dict[str, pd.DataFrame]:
    """Two assets in schwab-agent's OHLCV format."""
    rng = np.random.default_rng(7)
    n = 120
    dates = pd.date_range("2023-01-01", periods=n, freq="B")
    prices_a = 100 * np.cumprod(1 + rng.normal(0.0004, 0.011, n))
    prices_b = 50 * np.cumprod(1 + rng.normal(0.0002, 0.009, n))
    return {
        "AAA": pd.DataFrame({"close": prices_a}, index=dates),
        "BBB": pd.DataFrame({"close": prices_b}, index=dates),
    }


# ── _build_price_frame ───────────────────────────────────────────────────────

def test_build_price_frame_from_dataframe(synthetic_prices):
    frame = _build_price_frame(synthetic_prices)
    assert list(frame.columns) == ["SPY", "TLT", "GLD", "QQQ"]
    assert len(frame) == len(synthetic_prices)


def test_build_price_frame_from_ohlcv_dict(ohlcv_dict):
    frame = _build_price_frame(ohlcv_dict)
    assert set(frame.columns) == {"AAA", "BBB"}
    assert len(frame) == 120


def test_build_price_frame_too_few_symbols():
    df = pd.DataFrame({"ONLY": np.arange(100, dtype=float)})
    with pytest.raises(ValueError, match="at least 2 symbols"):
        _build_price_frame(df)


def test_build_price_frame_too_few_rows():
    df = pd.DataFrame(
        {"A": np.arange(20, dtype=float), "B": np.arange(20, dtype=float)}
    )
    with pytest.raises(ValueError, match="at least 30 rows"):
        _build_price_frame(df)


def test_build_price_frame_drops_empty_columns():
    df = pd.DataFrame(
        {
            "A": np.arange(100, dtype=float),
            "B": np.arange(100, dtype=float),
            "C": np.full(100, np.nan),
        }
    )
    frame = _build_price_frame(df)
    # C has all NaN and should be dropped
    assert "C" not in frame.columns
    assert len(frame.columns) == 2


def test_build_price_frame_skips_dict_entries_without_close():
    df_bad = pd.DataFrame({"not_close": np.arange(100, dtype=float)})
    df_good1 = pd.DataFrame({"close": np.arange(100, dtype=float) + 100})
    df_good2 = pd.DataFrame({"close": np.arange(100, dtype=float) + 50})
    frame = _build_price_frame({"BAD": df_bad, "A": df_good1, "B": df_good2})
    assert "BAD" not in frame.columns
    assert set(frame.columns) == {"A", "B"}


# ── Max Sharpe ───────────────────────────────────────────────────────────────

def test_max_sharpe_weights_valid(synthetic_prices):
    result = optimize_portfolio(
        synthetic_prices, method="max_sharpe", total_value=100_000
    )
    assert isinstance(result, OptimizationResult)
    assert result.method == "max_sharpe"
    # Weights should be non-negative (default bounds) and sum to ~1
    for w in result.weights.values():
        assert w >= -1e-6
    total = sum(result.weights.values())
    assert total == pytest.approx(1.0, abs=1e-4)


def test_max_sharpe_produces_discrete_allocation(synthetic_prices):
    result = optimize_portfolio(
        synthetic_prices, method="max_sharpe", total_value=100_000
    )
    assert result.total_value == 100_000
    # At least one holding should be allocated
    assert len(result.discrete_allocation) > 0
    assert result.leftover_cash >= 0
    # Leftover should be a small fraction of total value (greedy allocator
    # can leave up to roughly the sum of max share prices).
    assert result.leftover_cash < 0.05 * result.total_value


def test_max_sharpe_performance_metrics(synthetic_prices):
    result = optimize_portfolio(
        synthetic_prices, method="max_sharpe", total_value=100_000
    )
    assert result.expected_return != 0
    assert result.expected_volatility > 0
    # Sharpe should be computed
    assert result.sharpe_ratio != 0


# ── Min volatility ───────────────────────────────────────────────────────────

def test_min_volatility_lower_vol_than_max_sharpe(synthetic_prices):
    ms = optimize_portfolio(synthetic_prices, method="max_sharpe")
    mv = optimize_portfolio(synthetic_prices, method="min_volatility")
    # Min vol should have lower (or equal) volatility than max sharpe
    assert mv.expected_volatility <= ms.expected_volatility + 1e-6


def test_min_volatility_weights_valid(synthetic_prices):
    result = optimize_portfolio(synthetic_prices, method="min_volatility")
    total = sum(result.weights.values())
    assert total == pytest.approx(1.0, abs=1e-4)


# ── HRP ──────────────────────────────────────────────────────────────────────

def test_hrp_weights_valid(synthetic_prices):
    result = optimize_portfolio(
        synthetic_prices, method="hrp", total_value=50_000
    )
    assert result.method == "hrp"
    total = sum(result.weights.values())
    assert total == pytest.approx(1.0, abs=1e-4)
    # HRP should hold all 4 assets (not concentrate in 1)
    nonzero = [w for w in result.weights.values() if w > 0.01]
    assert len(nonzero) >= 3


def test_hrp_performance_metrics_positive(synthetic_prices):
    result = optimize_portfolio(synthetic_prices, method="hrp")
    assert result.expected_volatility > 0
    # Return could be any sign, just verify it's computed
    assert result.expected_return != 0 or result.expected_volatility > 0


# ── Target methods ───────────────────────────────────────────────────────────

def test_efficient_risk_requires_target(synthetic_prices):
    with pytest.raises(ValueError, match="target_volatility is required"):
        optimize_portfolio(synthetic_prices, method="efficient_risk")


def test_efficient_return_requires_target(synthetic_prices):
    with pytest.raises(ValueError, match="target_return is required"):
        optimize_portfolio(synthetic_prices, method="efficient_return")


def test_efficient_risk_hits_target(synthetic_prices):
    # Find a feasible target
    baseline = optimize_portfolio(synthetic_prices, method="min_volatility")
    target_vol = baseline.expected_volatility * 1.5
    result = optimize_portfolio(
        synthetic_prices, method="efficient_risk", target_volatility=target_vol
    )
    # Actual vol should be close to target (within 5%)
    assert result.expected_volatility == pytest.approx(target_vol, rel=0.05)


# ── Unknown method ───────────────────────────────────────────────────────────

def test_unknown_method_raises(synthetic_prices):
    with pytest.raises(ValueError, match="unknown optimization method"):
        optimize_portfolio(synthetic_prices, method="magic")  # type: ignore[arg-type]


def test_unknown_returns_model_raises(synthetic_prices):
    with pytest.raises(ValueError, match="unknown returns model"):
        optimize_portfolio(
            synthetic_prices,
            method="max_sharpe",
            returns_model="magic",  # type: ignore[arg-type]
        )


def test_unknown_risk_model_raises(synthetic_prices):
    with pytest.raises(ValueError, match="unknown risk model"):
        optimize_portfolio(
            synthetic_prices,
            method="max_sharpe",
            risk_model="magic",  # type: ignore[arg-type]
        )


# ── OHLCV dict input path ────────────────────────────────────────────────────

def test_ohlcv_dict_input(ohlcv_dict):
    # Use min_volatility so this test passes regardless of the synthetic
    # drift vs risk-free rate — the point here is the dict → frame path.
    result = optimize_portfolio(
        ohlcv_dict, method="min_volatility", total_value=10_000
    )
    assert set(result.symbols) == {"AAA", "BBB"}
    total = sum(result.weights.values())
    assert total == pytest.approx(1.0, abs=1e-4)


# ── to_dict + format_report ──────────────────────────────────────────────────

def test_to_dict_is_jsonable(synthetic_prices):
    import json
    result = optimize_portfolio(synthetic_prices, method="max_sharpe")
    d = result.to_dict()
    # Should serialize without errors
    s = json.dumps(d)
    assert "max_sharpe" in s
    assert "weights" in d
    assert "discrete_allocation" in d


def test_format_report_renders_sections(synthetic_prices):
    result = optimize_portfolio(synthetic_prices, method="max_sharpe")
    report = format_report(result)
    assert "Portfolio Optimization" in report
    assert "Expected return" in report
    assert "Continuous weights" in report
    assert "Discrete allocation" in report
    assert "Leftover cash" in report
