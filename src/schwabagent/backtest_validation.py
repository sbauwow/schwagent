"""Statistical validation for backtest results.

Three independent tests that quantify how much of a strategy's observed
performance is signal vs luck:

    Monte Carlo permutation  — is the Sharpe better than random ordering?
    Bootstrap Sharpe CI      — how stable is the risk-adjusted return?
    Walk-Forward analysis    — is performance consistent across time?

Adapted from HKUDS/vibe-trading (MIT licensed). Unlike the original which
operates on round-trip TradeRecords, this version uses schwagent's
BacktestResult (equity_curve + Trade list) directly. Daily equity deltas
replace per-trade PnLs — simpler and more general.
"""
from __future__ import annotations

import logging
from typing import Any

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

BARS_PER_YEAR = 252


# ── Helpers ──────────────────────────────────────────────────────────────────

def _sharpe(returns: np.ndarray, bars_per_year: int = BARS_PER_YEAR) -> float:
    """Annualized Sharpe ratio from a return series."""
    if len(returns) == 0:
        return 0.0
    std = float(returns.std())
    if std < 1e-12:
        return 0.0
    return float(returns.mean() / std * np.sqrt(bars_per_year))


def _max_drawdown(equity: np.ndarray) -> float:
    """Maximum drawdown as a negative fraction (e.g. -0.23 = -23%)."""
    if len(equity) == 0:
        return 0.0
    peak = np.maximum.accumulate(equity)
    safe_peak = np.where(peak > 0, peak, 1.0)
    dd = (equity - peak) / safe_peak
    return float(dd.min())


def _returns_from_equity(equity_curve: pd.Series) -> np.ndarray:
    """Convert an equity curve to a daily return array, dropping NaNs."""
    return equity_curve.pct_change().dropna().to_numpy()


# ── Monte Carlo permutation ──────────────────────────────────────────────────

def monte_carlo_test(
    equity_curve: pd.Series,
    n_simulations: int = 1000,
    seed: int = 42,
    bars_per_year: int = BARS_PER_YEAR,
) -> dict[str, Any]:
    """Permutation test on the ordering of daily dollar PnL.

    Null hypothesis: the observed Sharpe and max drawdown are no better
    than what we'd see if the same daily dollar PnLs had been experienced
    in a random order. Because the equity base shifts with path, this
    produces meaningful variance in both Sharpe and drawdown — a strategy
    with real edge will score low p-values (observed beats most shuffles).

    We operate on dollar PnLs (diff of equity) rather than percentage
    returns because percentage returns have mean/std invariant under
    permutation, making the Sharpe test degenerate.

    Args:
        equity_curve: Daily equity time series (indexed by date).
        n_simulations: Number of random permutations to run.
        seed: Random seed for reproducibility.
        bars_per_year: Annualization factor (252 for daily equity data).

    Returns:
        Dict with actual_sharpe, actual_max_dd, p_value_sharpe,
        p_value_max_dd, and simulated distribution stats.
    """
    equity_values = equity_curve.to_numpy(dtype=float)
    if len(equity_values) < 6:
        return {
            "error": "need at least 6 equity observations",
            "n_observations": len(equity_values),
            "p_value_sharpe": 1.0,
            "p_value_max_dd": 1.0,
        }

    initial = float(equity_values[0])
    if initial <= 0:
        return {
            "error": "initial equity must be positive",
            "n_observations": len(equity_values),
        }

    pnls = np.diff(equity_values)  # dollar PnL per bar

    def _metrics(shuffled_pnls: np.ndarray) -> tuple[float, float]:
        sim_equity = initial + np.cumsum(shuffled_pnls)
        sim_returns = np.diff(sim_equity) / np.where(
            sim_equity[:-1] > 0, sim_equity[:-1], 1.0
        )
        return _sharpe(sim_returns, bars_per_year), _max_drawdown(sim_equity)

    actual_sharpe, actual_max_dd = _metrics(pnls)

    rng = np.random.default_rng(seed)
    sharpe_better = 0
    dd_better = 0
    sim_sharpes: list[float] = []

    for _ in range(n_simulations):
        shuffled = rng.permutation(pnls)
        sim_sharpe, sim_max_dd = _metrics(shuffled)
        sim_sharpes.append(sim_sharpe)
        if sim_sharpe >= actual_sharpe:
            sharpe_better += 1
        # max_dd is negative; "better" means less negative (closer to 0)
        if sim_max_dd >= actual_max_dd:
            dd_better += 1

    sim_arr = np.array(sim_sharpes)
    return {
        "actual_sharpe": round(actual_sharpe, 4),
        "actual_max_dd": round(actual_max_dd, 4),
        "p_value_sharpe": round(sharpe_better / n_simulations, 4),
        "p_value_max_dd": round(dd_better / n_simulations, 4),
        "simulated_sharpe_mean": round(float(sim_arr.mean()), 4),
        "simulated_sharpe_std": round(float(sim_arr.std()), 4),
        "simulated_sharpe_p5": round(float(np.percentile(sim_arr, 5)), 4),
        "simulated_sharpe_p95": round(float(np.percentile(sim_arr, 95)), 4),
        "n_simulations": n_simulations,
        "n_observations": len(pnls),
    }


# ── Bootstrap Sharpe CI ──────────────────────────────────────────────────────

def bootstrap_sharpe_ci(
    equity_curve: pd.Series,
    n_bootstrap: int = 1000,
    confidence: float = 0.95,
    seed: int = 42,
    bars_per_year: int = BARS_PER_YEAR,
) -> dict[str, Any]:
    """Bootstrap confidence interval for the Sharpe ratio.

    Resamples daily returns with replacement to estimate how much the
    Sharpe ratio depends on the specific sample. A wide CI means the
    Sharpe is sensitive to a few lucky days; a tight CI means the
    result is robust.

    Args:
        equity_curve: Daily equity time series.
        n_bootstrap: Number of resamples.
        confidence: Confidence level (e.g. 0.95 for 95% CI).
        seed: Random seed.
        bars_per_year: Annualization factor.

    Returns:
        Dict with observed_sharpe, ci_lower, ci_upper, median_sharpe,
        prob_positive (fraction of resamples with Sharpe > 0).
    """
    returns = _returns_from_equity(equity_curve)
    if len(returns) < 5:
        return {
            "error": "need at least 5 return observations",
            "n_observations": len(returns),
        }

    observed = _sharpe(returns, bars_per_year)

    rng = np.random.default_rng(seed)
    boot_sharpes: list[float] = []
    for _ in range(n_bootstrap):
        sample = rng.choice(returns, size=len(returns), replace=True)
        boot_sharpes.append(_sharpe(sample, bars_per_year))

    arr = np.array(boot_sharpes)
    alpha = (1 - confidence) / 2
    lower = float(np.percentile(arr, alpha * 100))
    upper = float(np.percentile(arr, (1 - alpha) * 100))
    prob_positive = float(np.mean(arr > 0))

    return {
        "observed_sharpe": round(observed, 4),
        "ci_lower": round(lower, 4),
        "ci_upper": round(upper, 4),
        "median_sharpe": round(float(np.median(arr)), 4),
        "prob_positive": round(prob_positive, 4),
        "confidence": confidence,
        "n_bootstrap": n_bootstrap,
        "n_observations": len(returns),
    }


# ── Walk-Forward analysis ────────────────────────────────────────────────────

def walk_forward_analysis(
    equity_curve: pd.Series,
    trades: list[Any] | None = None,
    n_windows: int = 5,
    bars_per_year: int = BARS_PER_YEAR,
) -> dict[str, Any]:
    """Split the backtest into sequential windows and compare consistency.

    A strategy with true edge should be profitable in most windows; a
    strategy that was lucky on one big run will fail most windows.

    Args:
        equity_curve: Daily equity time series.
        trades: Optional list of Trade objects (must have .date attribute).
            If provided, per-window trade counts are reported.
        n_windows: Number of non-overlapping windows.
        bars_per_year: Annualization factor.

    Returns:
        Dict with per-window stats and overall consistency metrics.
    """
    if len(equity_curve) < n_windows * 2:
        return {
            "error": f"need at least {n_windows * 2} bars for {n_windows} windows",
            "n_observations": len(equity_curve),
        }

    indices = equity_curve.index
    window_size = len(indices) // n_windows
    windows: list[dict[str, Any]] = []

    for i in range(n_windows):
        start_idx = i * window_size
        end_idx = (i + 1) * window_size if i < n_windows - 1 else len(indices)
        win_eq = equity_curve.iloc[start_idx:end_idx]
        if len(win_eq) < 2:
            continue

        win_start = indices[start_idx]
        win_end = indices[end_idx - 1]
        start_value = float(win_eq.iloc[0])
        end_value = float(win_eq.iloc[-1])

        ret = (end_value / start_value - 1) if start_value > 0 else 0.0
        win_returns = win_eq.pct_change().dropna().to_numpy()
        sharpe = _sharpe(win_returns, bars_per_year)
        max_dd = _max_drawdown(win_eq.to_numpy())

        trade_count = 0
        if trades:
            for t in trades:
                t_date = _trade_date(t)
                if t_date is None:
                    continue
                if win_start <= t_date <= win_end:
                    trade_count += 1

        windows.append({
            "window": i + 1,
            "start": _format_date(win_start),
            "end": _format_date(win_end),
            "return": round(ret, 6),
            "sharpe": round(sharpe, 4),
            "max_dd": round(max_dd, 6),
            "trades": trade_count,
        })

    if not windows:
        return {"error": "no valid windows", "n_observations": len(equity_curve)}

    returns_list = [w["return"] for w in windows]
    sharpes_list = [w["sharpe"] for w in windows]
    profitable = sum(1 for r in returns_list if r > 0)

    return {
        "n_windows": len(windows),
        "windows": windows,
        "profitable_windows": profitable,
        "consistency_rate": round(profitable / len(windows), 4),
        "return_mean": round(float(np.mean(returns_list)), 6),
        "return_std": round(float(np.std(returns_list)), 6),
        "sharpe_mean": round(float(np.mean(sharpes_list)), 4),
        "sharpe_std": round(float(np.std(sharpes_list)), 4),
    }


def _trade_date(trade: Any) -> pd.Timestamp | None:
    """Extract a pandas Timestamp from a Trade object.

    Trade.date is typically a string like '2024-03-15'; coerce to Timestamp.
    Returns None if the date can't be parsed.
    """
    date_attr = getattr(trade, "date", None)
    if date_attr is None:
        return None
    try:
        return pd.Timestamp(date_attr)
    except (ValueError, TypeError):
        return None


def _format_date(ts: Any) -> str:
    """Format a pandas Timestamp as YYYY-MM-DD, or fall back to str()."""
    if hasattr(ts, "date"):
        try:
            return str(ts.date())
        except Exception:
            pass
    return str(ts)


# ── Combined runner ──────────────────────────────────────────────────────────

def run_validation(
    equity_curve: pd.Series,
    trades: list[Any] | None = None,
    n_simulations: int = 1000,
    n_bootstrap: int = 1000,
    n_windows: int = 5,
    confidence: float = 0.95,
    seed: int = 42,
    bars_per_year: int = BARS_PER_YEAR,
) -> dict[str, Any]:
    """Run all three validation tests and return the combined results.

    This is the main entry point for callers who want a full validation
    report. Each test is independent, so a missing field (e.g., trades=None)
    still produces results for the equity-based tests.

    Returns:
        Dict with keys 'monte_carlo', 'bootstrap', 'walk_forward'.
    """
    return {
        "monte_carlo": monte_carlo_test(
            equity_curve,
            n_simulations=n_simulations,
            seed=seed,
            bars_per_year=bars_per_year,
        ),
        "bootstrap": bootstrap_sharpe_ci(
            equity_curve,
            n_bootstrap=n_bootstrap,
            confidence=confidence,
            seed=seed,
            bars_per_year=bars_per_year,
        ),
        "walk_forward": walk_forward_analysis(
            equity_curve,
            trades=trades,
            n_windows=n_windows,
            bars_per_year=bars_per_year,
        ),
    }


# ── Pretty printing ──────────────────────────────────────────────────────────

def format_report(results: dict[str, Any]) -> str:
    """Render validation results as a human-readable multi-line report."""
    lines: list[str] = []
    lines.append("=== Backtest Validation ===\n")

    mc = results.get("monte_carlo", {})
    lines.append("Monte Carlo Permutation Test:")
    if "error" in mc:
        lines.append(f"  Error: {mc['error']}")
    else:
        p_sharpe = mc["p_value_sharpe"]
        verdict = _mc_verdict(p_sharpe)
        lines.append(f"  Observed Sharpe:       {mc['actual_sharpe']:.3f}")
        lines.append(f"  Observed Max DD:       {mc['actual_max_dd']:.2%}")
        lines.append(f"  p-value (Sharpe):      {p_sharpe:.4f}  {verdict}")
        lines.append(f"  p-value (Max DD):      {mc['p_value_max_dd']:.4f}")
        lines.append(
            f"  Random Sharpe range:   "
            f"[{mc['simulated_sharpe_p5']:.3f}, {mc['simulated_sharpe_p95']:.3f}] "
            f"(mean {mc['simulated_sharpe_mean']:.3f})"
        )
    lines.append("")

    bs = results.get("bootstrap", {})
    lines.append("Bootstrap Sharpe CI:")
    if "error" in bs:
        lines.append(f"  Error: {bs['error']}")
    else:
        verdict = _bs_verdict(bs["ci_lower"], bs["prob_positive"])
        lines.append(f"  Observed Sharpe:       {bs['observed_sharpe']:.3f}")
        lines.append(
            f"  {int(bs['confidence'] * 100)}% CI:                "
            f"[{bs['ci_lower']:.3f}, {bs['ci_upper']:.3f}]"
        )
        lines.append(f"  Median Sharpe:         {bs['median_sharpe']:.3f}")
        lines.append(
            f"  P(Sharpe > 0):         {bs['prob_positive']:.2%}  {verdict}"
        )
    lines.append("")

    wf = results.get("walk_forward", {})
    lines.append("Walk-Forward Analysis:")
    if "error" in wf:
        lines.append(f"  Error: {wf['error']}")
    else:
        verdict = _wf_verdict(wf["consistency_rate"])
        lines.append(
            f"  Profitable windows:    "
            f"{wf['profitable_windows']}/{wf['n_windows']} "
            f"({wf['consistency_rate']:.0%})  {verdict}"
        )
        lines.append(
            f"  Per-window return:     "
            f"mean {wf['return_mean']:+.2%}, std {wf['return_std']:.2%}"
        )
        lines.append(
            f"  Per-window Sharpe:     "
            f"mean {wf['sharpe_mean']:.2f}, std {wf['sharpe_std']:.2f}"
        )
        for w in wf.get("windows", []):
            lines.append(
                f"    [{w['window']}] {w['start']} → {w['end']} "
                f"ret {w['return']:+.2%} sharpe {w['sharpe']:+.2f} "
                f"maxdd {w['max_dd']:.2%} trades {w['trades']}"
            )
    lines.append("")
    return "\n".join(lines)


def _mc_verdict(p: float) -> str:
    """Short verbal rating for a Monte Carlo p-value."""
    if p < 0.05:
        return "(significant)"
    if p < 0.10:
        return "(marginal)"
    return "(not significant)"


def _bs_verdict(lower: float, prob_pos: float) -> str:
    """Short verbal rating for a bootstrap CI."""
    if lower > 0 and prob_pos > 0.95:
        return "(robust)"
    if lower > 0:
        return "(positive)"
    if prob_pos > 0.80:
        return "(likely positive)"
    return "(unreliable)"


def _wf_verdict(rate: float) -> str:
    """Short verbal rating for walk-forward consistency."""
    if rate >= 0.80:
        return "(very consistent)"
    if rate >= 0.60:
        return "(consistent)"
    if rate >= 0.40:
        return "(mixed)"
    return "(inconsistent)"
