"""Portfolio optimization via PyPortfolioOpt.

Thin wrapper around https://github.com/robertmartin8/PyPortfolioOpt that
integrates with schwab-agent's SchwabClient for historical OHLCV fetching
and returns results in a format the ETF rotation strategy, swarm presets,
and web dashboard can consume.

Supported objectives:
    max_sharpe          — maximum Sharpe ratio on the efficient frontier
    min_volatility      — minimum variance portfolio
    efficient_risk      — maximum return for a target volatility
    efficient_return    — minimum volatility for a target return
    hrp                 — hierarchical risk parity (Lopez de Prado)

Expected-returns estimators:
    mean_historical     — simple historical mean
    ema_historical      — exponentially-weighted historical mean
    capm                — CAPM-implied returns given a market benchmark

Risk models:
    sample_cov          — sample covariance
    ledoit_wolf         — Ledoit-Wolf shrinkage
    exp_cov             — exponentially-weighted covariance
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Literal

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


OptimizeMethod = Literal[
    "max_sharpe",
    "min_volatility",
    "efficient_risk",
    "efficient_return",
    "hrp",
]

ReturnsModel = Literal["mean_historical", "ema_historical", "capm"]
RiskModel = Literal["sample_cov", "ledoit_wolf", "exp_cov"]


@dataclass
class OptimizationResult:
    """Output of a portfolio optimization run.

    Attributes:
        method: Objective used (e.g. "max_sharpe").
        weights: Symbol → weight (sums to 1.0 for a fully-invested portfolio).
        expected_return: Annualized expected return of the portfolio.
        expected_volatility: Annualized expected volatility.
        sharpe_ratio: Expected Sharpe ratio (0 risk-free rate assumed unless set).
        discrete_allocation: Symbol → share count (rounded whole shares).
        leftover_cash: Capital not allocated to whole shares.
        total_value: Total portfolio capital used for discrete allocation.
        symbols: Original symbol list (deduplicated, order-preserved).
    """

    method: str
    weights: dict[str, float] = field(default_factory=dict)
    expected_return: float = 0.0
    expected_volatility: float = 0.0
    sharpe_ratio: float = 0.0
    discrete_allocation: dict[str, int] = field(default_factory=dict)
    leftover_cash: float = 0.0
    total_value: float = 0.0
    symbols: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "method": self.method,
            "weights": {k: round(v, 6) for k, v in self.weights.items()},
            "expected_return": round(self.expected_return, 6),
            "expected_volatility": round(self.expected_volatility, 6),
            "sharpe_ratio": round(self.sharpe_ratio, 6),
            "discrete_allocation": self.discrete_allocation,
            "leftover_cash": round(self.leftover_cash, 2),
            "total_value": round(self.total_value, 2),
            "symbols": self.symbols,
        }


# ── Helpers ──────────────────────────────────────────────────────────────────

def _build_price_frame(
    prices: dict[str, pd.DataFrame] | pd.DataFrame,
) -> pd.DataFrame:
    """Normalize price input into a DataFrame with one column per symbol.

    Accepts either a pre-built DataFrame (columns = symbols, values = close)
    or a dict of {symbol: OHLCV DataFrame}. In the dict case the 'close'
    column is extracted per symbol.

    Raises:
        ValueError: If the result has fewer than 2 columns or 30 rows.
    """
    if isinstance(prices, pd.DataFrame):
        frame = prices.copy()
    else:
        cols = {}
        for sym, df in prices.items():
            if df is None or df.empty:
                logger.warning("Skipping %s — no price data", sym)
                continue
            # Try 'close' (schwab-agent standard), fall back to 'Close'
            if "close" in df.columns:
                cols[sym] = df["close"]
            elif "Close" in df.columns:
                cols[sym] = df["Close"]
            else:
                logger.warning("Skipping %s — no close column in %s", sym, list(df.columns))
                continue
        if not cols:
            raise ValueError("no usable price data in the input")
        frame = pd.DataFrame(cols)

    frame = frame.dropna(how="all")
    # Forward-fill small gaps, then drop any symbol still with NaN
    frame = frame.ffill().dropna(axis=1, how="any")

    if frame.shape[1] < 2:
        raise ValueError(
            f"need at least 2 symbols with clean price data, got {frame.shape[1]}"
        )
    if len(frame) < 30:
        raise ValueError(
            f"need at least 30 rows of price history, got {len(frame)}"
        )

    return frame


def _expected_returns(
    prices: pd.DataFrame, model: ReturnsModel, market_prices: pd.Series | None = None
) -> pd.Series:
    """Compute expected returns using the requested model."""
    from pypfopt import expected_returns as _er  # lazy import

    if model == "mean_historical":
        return _er.mean_historical_return(prices)
    if model == "ema_historical":
        return _er.ema_historical_return(prices)
    if model == "capm":
        return _er.capm_return(prices, market_prices=market_prices)
    raise ValueError(f"unknown returns model: {model!r}")


def _risk_model(prices: pd.DataFrame, model: RiskModel) -> pd.DataFrame:
    """Compute a covariance matrix using the requested model."""
    from pypfopt import risk_models as _rm  # lazy import

    if model == "sample_cov":
        return _rm.sample_cov(prices)
    if model == "ledoit_wolf":
        return _rm.CovarianceShrinkage(prices).ledoit_wolf()
    if model == "exp_cov":
        return _rm.exp_cov(prices)
    raise ValueError(f"unknown risk model: {model!r}")


# ── Main entry point ─────────────────────────────────────────────────────────

def optimize_portfolio(
    prices: dict[str, pd.DataFrame] | pd.DataFrame,
    method: OptimizeMethod = "max_sharpe",
    returns_model: ReturnsModel = "mean_historical",
    risk_model: RiskModel = "ledoit_wolf",
    risk_free_rate: float = 0.04,
    target_volatility: float | None = None,
    target_return: float | None = None,
    weight_bounds: tuple[float, float] = (0.0, 1.0),
    total_value: float = 100_000.0,
    market_prices: pd.Series | None = None,
) -> OptimizationResult:
    """Run a portfolio optimization and return weights + discrete allocation.

    Args:
        prices: Either a DataFrame of close prices (columns = symbols) or a
            dict of {symbol: OHLCV DataFrame}.
        method: Optimization objective.
        returns_model: Expected-returns estimator (ignored for HRP).
        risk_model: Covariance estimator (ignored for HRP).
        risk_free_rate: Annualized risk-free rate used for max_sharpe.
        target_volatility: Required for method="efficient_risk".
        target_return: Required for method="efficient_return".
        weight_bounds: (min, max) per-asset weight bounds (not used by HRP).
        total_value: Total capital for discrete allocation.
        market_prices: Market benchmark series, required for returns_model="capm".

    Returns:
        OptimizationResult with continuous weights, discrete allocation,
        leftover cash, and performance metrics.

    Raises:
        ValueError: On bad inputs (too few symbols, missing target for
            efficient_risk/return, insufficient data, unknown method).
    """
    # Lazy import so schwab-agent can still import without pypfopt installed
    from pypfopt import EfficientFrontier, HRPOpt
    from pypfopt.discrete_allocation import DiscreteAllocation, get_latest_prices

    frame = _build_price_frame(prices)
    symbols = list(frame.columns)

    if method == "hrp":
        returns = frame.pct_change().dropna()
        hrp = HRPOpt(returns=returns)
        hrp.optimize()
        weights = hrp.clean_weights()
        # HRP does not produce standard portfolio_performance; compute manually
        exp_ret, exp_vol, sharpe = _hrp_performance(
            frame, weights, risk_free_rate=risk_free_rate
        )
    else:
        mu = _expected_returns(frame, returns_model, market_prices=market_prices)
        S = _risk_model(frame, risk_model)
        ef = EfficientFrontier(mu, S, weight_bounds=weight_bounds)

        if method == "max_sharpe":
            ef.max_sharpe(risk_free_rate=risk_free_rate)
        elif method == "min_volatility":
            ef.min_volatility()
        elif method == "efficient_risk":
            if target_volatility is None:
                raise ValueError("target_volatility is required for efficient_risk")
            ef.efficient_risk(target_volatility=target_volatility)
        elif method == "efficient_return":
            if target_return is None:
                raise ValueError("target_return is required for efficient_return")
            ef.efficient_return(target_return=target_return)
        else:
            raise ValueError(f"unknown optimization method: {method!r}")

        weights = ef.clean_weights()
        exp_ret, exp_vol, sharpe = ef.portfolio_performance(
            risk_free_rate=risk_free_rate, verbose=False
        )

    # Discrete allocation: rounds continuous weights to whole shares
    latest_prices = get_latest_prices(frame)
    da = DiscreteAllocation(
        weights=weights,
        latest_prices=latest_prices,
        total_portfolio_value=total_value,
    )
    allocation, leftover = da.greedy_portfolio()

    return OptimizationResult(
        method=method,
        weights=dict(weights),
        expected_return=float(exp_ret),
        expected_volatility=float(exp_vol),
        sharpe_ratio=float(sharpe),
        discrete_allocation=dict(allocation),
        leftover_cash=float(leftover),
        total_value=float(total_value),
        symbols=symbols,
    )


def _hrp_performance(
    prices: pd.DataFrame,
    weights: dict[str, float],
    risk_free_rate: float,
) -> tuple[float, float, float]:
    """Compute annualized return/vol/Sharpe for an HRP result."""
    daily_returns = prices.pct_change().dropna()
    w = np.array([weights.get(c, 0.0) for c in daily_returns.columns])
    portfolio_daily = daily_returns.to_numpy() @ w
    annual_ret = float(portfolio_daily.mean() * 252)
    annual_vol = float(portfolio_daily.std(ddof=0) * np.sqrt(252))
    sharpe = (annual_ret - risk_free_rate) / annual_vol if annual_vol > 0 else 0.0
    return annual_ret, annual_vol, sharpe


# ── Pretty printing ──────────────────────────────────────────────────────────

def format_report(result: OptimizationResult) -> str:
    """Render an OptimizationResult as a human-readable multi-line report."""
    lines: list[str] = []
    lines.append(f"=== Portfolio Optimization: {result.method} ===")
    lines.append("")
    lines.append(
        f"Expected return:      {result.expected_return:+.2%}"
    )
    lines.append(
        f"Expected volatility:  {result.expected_volatility:.2%}"
    )
    lines.append(
        f"Sharpe ratio:         {result.sharpe_ratio:+.3f}"
    )
    lines.append("")
    lines.append("Continuous weights:")
    nonzero = {k: v for k, v in result.weights.items() if v > 1e-4}
    for sym, w in sorted(nonzero.items(), key=lambda x: -x[1]):
        lines.append(f"  {sym:<8} {w:6.2%}")
    if not nonzero:
        lines.append("  (no positions)")
    lines.append("")
    lines.append(f"Discrete allocation (${result.total_value:,.0f} capital):")
    for sym, qty in sorted(
        result.discrete_allocation.items(), key=lambda x: -x[1]
    ):
        lines.append(f"  {sym:<8} {qty:>4} shares")
    if not result.discrete_allocation:
        lines.append("  (no positions)")
    lines.append(f"  Leftover cash:  ${result.leftover_cash:,.2f}")
    lines.append("")
    return "\n".join(lines)
