"""Fundamental analysis — valuation, financial health, quality, and factor scoring.

Three data sources:
1. Schwab quotes API (real-time: P/E, EPS, dividend yield, shares outstanding)
2. Financial features dataset (historical: 210 columns per company per year)
3. S&P 500 companies dataset (sector classification for peer comparison)

Implements CFA-grade analysis:
- Valuation: DCF, Graham Number, earnings yield, relative P/E
- Financial health: Altman Z-Score, Piotroski F-Score
- Quality: ROIC, ROE, margin stability, earnings quality
- Factor model: composite Value + Quality + Momentum score
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# DATA MODELS
# ══════════════════════════════════════════════════════════════════════════════


@dataclass
class ValuationResult:
    symbol: str
    price: float = 0.0
    # From Schwab API
    pe_ratio: float = float("nan")
    eps: float = float("nan")
    dividend_yield: float = float("nan")
    shares_outstanding: int = 0
    # Computed
    earnings_yield: float = float("nan")    # 1/PE — compare to bond yields
    graham_number: float = float("nan")     # sqrt(22.5 × EPS × BVPS)
    graham_upside_pct: float = float("nan") # (graham - price) / price
    dcf_value: float = float("nan")         # discounted cash flow intrinsic value
    dcf_upside_pct: float = float("nan")
    margin_of_safety: float = float("nan")  # discount from intrinsic value
    peg_ratio: float = float("nan")         # P/E divided by growth rate
    fcf_yield: float = float("nan")         # free cash flow / market cap


@dataclass
class HealthResult:
    symbol: str
    altman_z: float = float("nan")          # >2.99 safe, <1.81 distress
    altman_zone: str = ""                    # "safe", "grey", "distress"
    piotroski_f: int = 0                     # 0-9 (higher = stronger)
    piotroski_details: dict = field(default_factory=dict)
    current_ratio: float = float("nan")
    debt_to_equity: float = float("nan")
    interest_coverage: float = float("nan")


@dataclass
class QualityResult:
    symbol: str
    roe: float = float("nan")               # return on equity
    roa: float = float("nan")               # return on assets
    roic: float = float("nan")              # return on invested capital
    gross_margin: float = float("nan")
    net_margin: float = float("nan")
    margin_stability: float = float("nan")  # std of net_margin over time (lower = better)
    earnings_quality: float = float("nan")  # operating CF / net income (>1 = high quality)
    revenue_growth_3y: float = float("nan")
    earnings_growth_3y: float = float("nan")


@dataclass
class FactorScore:
    """Multi-factor composite score for stock ranking."""
    symbol: str
    value_score: float = 0.0        # -1 to +1 (cheap to expensive)
    quality_score: float = 0.0      # -1 to +1 (weak to strong)
    momentum_score: float = 0.0     # -1 to +1 (lagging to leading)
    composite: float = 0.0          # weighted average
    rank: int = 0
    details: dict = field(default_factory=dict)


# ══════════════════════════════════════════════════════════════════════════════
# VALUATION
# ══════════════════════════════════════════════════════════════════════════════


def earnings_yield(pe_ratio: float) -> float:
    """Earnings yield = 1/PE. Compare to 10-year Treasury yield.
    If earnings yield > bond yield, stocks are relatively cheap."""
    if pe_ratio <= 0 or math.isnan(pe_ratio):
        return float("nan")
    return 1.0 / pe_ratio * 100


def graham_number(eps: float, book_value_per_share: float) -> float:
    """Benjamin Graham's intrinsic value formula.
    Graham Number = sqrt(22.5 × EPS × BVPS).
    Buy when price < Graham Number."""
    if eps <= 0 or book_value_per_share <= 0:
        return float("nan")
    return math.sqrt(22.5 * eps * book_value_per_share)


def dcf_intrinsic_value(
    fcf: float,
    growth_rate: float = 0.05,
    discount_rate: float = 0.10,
    terminal_growth: float = 0.025,
    projection_years: int = 10,
    shares: int = 1,
) -> float:
    """Two-stage DCF model.

    Stage 1: Project FCF at growth_rate for projection_years.
    Stage 2: Terminal value using Gordon Growth Model.
    Discount everything back at discount_rate.

    Returns per-share intrinsic value.
    """
    if fcf <= 0 or shares <= 0:
        return float("nan")
    if discount_rate <= terminal_growth:
        return float("nan")

    pv_fcf = 0.0
    projected_fcf = fcf
    for year in range(1, projection_years + 1):
        projected_fcf *= (1 + growth_rate)
        pv_fcf += projected_fcf / (1 + discount_rate) ** year

    # Terminal value
    terminal_fcf = projected_fcf * (1 + terminal_growth)
    terminal_value = terminal_fcf / (discount_rate - terminal_growth)
    pv_terminal = terminal_value / (1 + discount_rate) ** projection_years

    enterprise_value = pv_fcf + pv_terminal
    return enterprise_value / shares


def peg_ratio(pe: float, earnings_growth_pct: float) -> float:
    """PEG = P/E / earnings growth rate. PEG < 1 = undervalued for its growth."""
    if pe <= 0 or earnings_growth_pct <= 0:
        return float("nan")
    return pe / earnings_growth_pct


# ══════════════════════════════════════════════════════════════════════════════
# FINANCIAL HEALTH
# ══════════════════════════════════════════════════════════════════════════════


def altman_z_score(
    working_capital: float,
    retained_earnings: float,
    ebit: float,
    market_cap: float,
    total_liabilities: float,
    revenue: float,
    total_assets: float,
) -> tuple[float, str]:
    """Altman Z-Score — bankruptcy predictor.

    Z = 1.2×A + 1.4×B + 3.3×C + 0.6×D + 1.0×E
    where:
      A = Working Capital / Total Assets
      B = Retained Earnings / Total Assets
      C = EBIT / Total Assets
      D = Market Cap / Total Liabilities
      E = Revenue / Total Assets

    Z > 2.99 = safe zone
    1.81 < Z < 2.99 = grey zone
    Z < 1.81 = distress zone
    """
    if total_assets <= 0 or total_liabilities <= 0:
        return float("nan"), "unknown"

    a = working_capital / total_assets
    b = retained_earnings / total_assets
    c = ebit / total_assets
    d = market_cap / total_liabilities
    e = revenue / total_assets

    z = 1.2 * a + 1.4 * b + 3.3 * c + 0.6 * d + 1.0 * e

    if z > 2.99:
        zone = "safe"
    elif z > 1.81:
        zone = "grey"
    else:
        zone = "distress"

    return z, zone


def piotroski_f_score(
    net_income: float,
    operating_cf: float,
    roa_current: float,
    roa_prior: float,
    debt_current: float,
    debt_prior: float,
    current_ratio_now: float,
    current_ratio_prior: float,
    shares_current: float,
    shares_prior: float,
    gross_margin_current: float,
    gross_margin_prior: float,
    asset_turnover_current: float,
    asset_turnover_prior: float,
) -> tuple[int, dict]:
    """Piotroski F-Score — fundamental strength (0-9).

    9 binary tests across profitability (4), leverage (3), efficiency (2).
    Score ≥ 7 = strong fundamentals. Score ≤ 3 = weak.

    Returns (score, details_dict).
    """
    details = {}

    # Profitability (4 points)
    details["positive_net_income"] = p1 = int(net_income > 0)
    details["positive_ocf"] = p2 = int(operating_cf > 0)
    details["roa_improving"] = p3 = int(roa_current > roa_prior)
    details["ocf_gt_net_income"] = p4 = int(operating_cf > net_income)  # earnings quality

    # Leverage/Liquidity (3 points)
    details["debt_decreasing"] = l1 = int(debt_current < debt_prior)
    details["current_ratio_improving"] = l2 = int(current_ratio_now > current_ratio_prior)
    details["no_dilution"] = l3 = int(shares_current <= shares_prior)

    # Efficiency (2 points)
    details["gross_margin_improving"] = e1 = int(gross_margin_current > gross_margin_prior)
    details["asset_turnover_improving"] = e2 = int(asset_turnover_current > asset_turnover_prior)

    score = p1 + p2 + p3 + p4 + l1 + l2 + l3 + e1 + e2
    return score, details


# ══════════════════════════════════════════════════════════════════════════════
# QUALITY METRICS
# ══════════════════════════════════════════════════════════════════════════════


def return_on_equity(net_income: float, equity: float) -> float:
    if equity <= 0:
        return float("nan")
    return net_income / equity * 100


def return_on_assets(net_income: float, assets: float) -> float:
    if assets <= 0:
        return float("nan")
    return net_income / assets * 100


def return_on_invested_capital(ebit: float, tax_rate: float, invested_capital: float) -> float:
    """ROIC = NOPAT / Invested Capital.
    Invested Capital = Total Assets - Cash - Current Liabilities.
    The true measure of capital efficiency — compare to WACC."""
    if invested_capital <= 0:
        return float("nan")
    nopat = ebit * (1 - tax_rate)
    return nopat / invested_capital * 100


def earnings_quality_ratio(operating_cf: float, net_income: float) -> float:
    """Earnings quality = Operating CF / Net Income.
    > 1.0 = high quality (cash earnings exceed accounting earnings).
    < 1.0 = low quality (earnings may be driven by accruals, not cash).
    Watch for declining trend — Enron's was 0.3 before collapse."""
    if net_income <= 0:
        return float("nan")
    return operating_cf / net_income


def margin_stability(margins: list[float]) -> float:
    """Standard deviation of margin over time. Lower = more predictable business.
    Stable margins → reliable earnings → lower risk premium in DCF."""
    if len(margins) < 3:
        return float("nan")
    return float(np.std(margins))


# ══════════════════════════════════════════════════════════════════════════════
# FACTOR MODEL
# ══════════════════════════════════════════════════════════════════════════════


def compute_factor_scores(
    stocks: list[dict],
    value_weight: float = 0.35,
    quality_weight: float = 0.35,
    momentum_weight: float = 0.30,
) -> list[FactorScore]:
    """Multi-factor ranking model.

    Each stock dict should contain:
        symbol, pe_ratio, earnings_yield, roe, roa, gross_margin,
        net_margin, debt_to_equity, earnings_quality, momentum_12m

    Returns ranked list of FactorScores (best first).
    """
    if not stocks:
        return []

    df = pd.DataFrame(stocks)
    results = []

    def _rank_percentile(series: pd.Series, ascending: bool = True) -> pd.Series:
        """Rank values to 0-1 percentile. ascending=True means lower raw = higher rank."""
        if ascending:
            return series.rank(pct=True, ascending=True)
        return series.rank(pct=True, ascending=False)

    # Value factor: earnings yield (higher = cheaper), inverse P/E
    v_score = pd.Series(0.0, index=df.index)
    if "earnings_yield" in df.columns:
        ey = pd.to_numeric(df["earnings_yield"], errors="coerce")
        v_score += _rank_percentile(ey, ascending=False)  # higher yield = cheaper = better
    if "pe_ratio" in df.columns:
        pe = pd.to_numeric(df["pe_ratio"], errors="coerce")
        v_score += _rank_percentile(pe, ascending=True)  # lower PE = cheaper = better
    v_score = (v_score / v_score.abs().max()).fillna(0) if v_score.abs().max() > 0 else v_score

    # Quality factor: ROE, margins, low debt, earnings quality
    q_score = pd.Series(0.0, index=df.index)
    for col, asc in [("roe", False), ("gross_margin", False), ("net_margin", False),
                      ("earnings_quality", False), ("debt_to_equity", True)]:
        if col in df.columns:
            s = pd.to_numeric(df[col], errors="coerce")
            q_score += _rank_percentile(s, ascending=asc)
    q_score = (q_score / q_score.abs().max()).fillna(0) if q_score.abs().max() > 0 else q_score

    # Momentum factor: 12-month return
    m_score = pd.Series(0.0, index=df.index)
    if "momentum_12m" in df.columns:
        m = pd.to_numeric(df["momentum_12m"], errors="coerce")
        m_score = _rank_percentile(m, ascending=False).fillna(0)

    for i, row in df.iterrows():
        v = float(v_score.iloc[i]) * 2 - 1  # scale to -1..+1
        q = float(q_score.iloc[i]) * 2 - 1
        m = float(m_score.iloc[i]) * 2 - 1
        composite = value_weight * v + quality_weight * q + momentum_weight * m

        results.append(FactorScore(
            symbol=row.get("symbol", ""),
            value_score=round(v, 3),
            quality_score=round(q, 3),
            momentum_score=round(m, 3),
            composite=round(composite, 3),
            details={
                "pe_ratio": row.get("pe_ratio"),
                "earnings_yield": row.get("earnings_yield"),
                "roe": row.get("roe"),
                "gross_margin": row.get("gross_margin"),
                "debt_to_equity": row.get("debt_to_equity"),
            },
        ))

    results.sort(key=lambda x: x.composite, reverse=True)
    for i, r in enumerate(results):
        r.rank = i + 1

    return results


# ══════════════════════════════════════════════════════════════════════════════
# SECTOR ANALYSIS
# ══════════════════════════════════════════════════════════════════════════════


def load_sector_map(path: str = "data/sp500_companies.csv") -> dict[str, str]:
    """Load symbol → sector mapping from S&P 500 companies CSV."""
    p = Path(path)
    if not p.exists():
        return {}
    df = pd.read_csv(p, usecols=["symbol", "sector"])
    return dict(zip(df["symbol"], df["sector"]))


def sector_relative(
    value: float,
    sector_values: list[float],
) -> tuple[float, float]:
    """Compare a metric to its sector distribution.

    Returns (percentile, z_score_vs_sector).
    """
    if not sector_values or math.isnan(value):
        return float("nan"), float("nan")
    arr = np.array([v for v in sector_values if not math.isnan(v)])
    if len(arr) < 3:
        return float("nan"), float("nan")
    percentile = float(np.sum(arr <= value) / len(arr) * 100)
    mean = float(arr.mean())
    std = float(arr.std())
    z = (value - mean) / std if std > 0 else 0.0
    return percentile, z


# ══════════════════════════════════════════════════════════════════════════════
# CONVENIENCE — analyze a symbol using Schwab API data
# ══════════════════════════════════════════════════════════════════════════════


def analyze_from_quote(quote_data: dict, price: float) -> ValuationResult:
    """Build a ValuationResult from Schwab quote fundamental data.

    The quote_data dict should be the raw Schwab quote response for a symbol,
    containing the 'fundamental' section.
    """
    fund = quote_data.get("fundamental", {})
    result = ValuationResult(symbol=quote_data.get("symbol", ""))
    result.price = price
    result.pe_ratio = fund.get("peRatio", float("nan"))
    result.eps = fund.get("eps", float("nan"))
    result.dividend_yield = fund.get("divYield", float("nan"))
    result.shares_outstanding = fund.get("sharesOutstanding", 0)

    if result.pe_ratio and result.pe_ratio > 0:
        result.earnings_yield = earnings_yield(result.pe_ratio)

    return result
