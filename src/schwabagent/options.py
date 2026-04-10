"""Options pricing and multi-leg strategy analysis.

Pure-math module — no external dependencies beyond numpy (scipy is not
required; normal CDF/PDF are computed from the stdlib ``math.erf``).

Provides:
    Black-Scholes pricing + Greeks          (call, put)
    Implied volatility solver               (bisection, robust to wide ranges)
    Multi-leg strategy representation       (Leg dataclass)
    Payoff-at-expiry computation            (per-price and vectorized)
    Strategy metrics                        (max profit, max loss, breakevens)
    Common strategy constructors            (covered call, vertical spreads,
                                             iron condor, straddle, strangle,
                                             butterfly, etc.)

Adapted from HKUDS/vibe-trading (MIT licensed) and extended with IV
solver, multi-leg payoff analysis, and strategy constructors tailored
to the options strategies a Schwab retail account can run.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Literal

import numpy as np

OptionType = Literal["call", "put"]
Side = Literal["long", "short"]


# ── Normal distribution (stdlib erf, no scipy dependency) ────────────────────

def _norm_cdf(x: float) -> float:
    """Standard normal CDF via math.erf — matches scipy.stats.norm.cdf."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _norm_pdf(x: float) -> float:
    """Standard normal PDF."""
    return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)


# ── Black-Scholes pricing and Greeks ─────────────────────────────────────────

def bs_price_and_greeks(
    spot: float,
    strike: float,
    T: float,
    r: float,
    sigma: float,
    option_type: OptionType,
) -> dict[str, float]:
    """Compute Black-Scholes price and Greeks for a single option.

    Args:
        spot: Current underlying price.
        strike: Option strike price.
        T: Time to expiry in years (e.g. 30 days → 30/365).
        r: Risk-free rate as a decimal (e.g. 0.05 for 5%).
        sigma: Annualized implied volatility as a decimal (e.g. 0.25 for 25%).
        option_type: "call" or "put".

    Returns:
        Dict with keys: price, delta, gamma, theta (per day), vega (per 1% IV).
    """
    option_type = option_type.lower()  # type: ignore[assignment]
    if option_type not in ("call", "put"):
        raise ValueError(f"option_type must be 'call' or 'put', got {option_type!r}")

    # Expiry or zero-vol edge case: collapse to intrinsic value
    if T <= 0 or sigma <= 0:
        if option_type == "call":
            price = max(spot - strike, 0.0)
            delta = 1.0 if spot > strike else 0.0
        else:
            price = max(strike - spot, 0.0)
            delta = -1.0 if spot < strike else 0.0
        return {
            "price": round(price, 6),
            "delta": round(delta, 6),
            "gamma": 0.0,
            "theta": 0.0,
            "vega": 0.0,
        }

    sqrt_T = math.sqrt(T)
    d1 = (math.log(spot / strike) + (r + 0.5 * sigma * sigma) * T) / (sigma * sqrt_T)
    d2 = d1 - sigma * sqrt_T
    pdf_d1 = _norm_pdf(d1)

    if option_type == "call":
        price = spot * _norm_cdf(d1) - strike * math.exp(-r * T) * _norm_cdf(d2)
        delta = _norm_cdf(d1)
    else:
        price = strike * math.exp(-r * T) * _norm_cdf(-d2) - spot * _norm_cdf(-d1)
        delta = _norm_cdf(d1) - 1.0

    gamma = pdf_d1 / (spot * sigma * sqrt_T)

    theta_common = -(spot * pdf_d1 * sigma) / (2.0 * sqrt_T)
    if option_type == "call":
        theta_annual = theta_common - r * strike * math.exp(-r * T) * _norm_cdf(d2)
    else:
        theta_annual = theta_common + r * strike * math.exp(-r * T) * _norm_cdf(-d2)
    theta_per_day = theta_annual / 365.0

    # Vega per 1% vol change (divide by 100 since sigma is a decimal)
    vega = spot * pdf_d1 * sqrt_T / 100.0

    return {
        "price": round(price, 6),
        "delta": round(delta, 6),
        "gamma": round(gamma, 6),
        "theta": round(theta_per_day, 6),
        "vega": round(vega, 6),
    }


# ── Implied volatility ───────────────────────────────────────────────────────

def implied_volatility(
    market_price: float,
    spot: float,
    strike: float,
    T: float,
    r: float,
    option_type: OptionType,
    tol: float = 1e-6,
    max_iter: int = 100,
) -> float | None:
    """Solve for implied volatility via bisection.

    Robust to wide parameter ranges (works where Newton-Raphson can diverge
    on deep OTM or near-expiry options). Returns None if the market price
    is outside the [intrinsic, spot] arbitrage bounds.

    Args:
        market_price: Observed option premium.
        spot: Current underlying price.
        strike: Strike price.
        T: Time to expiry in years.
        r: Risk-free rate.
        option_type: "call" or "put".
        tol: Absolute price tolerance for convergence.
        max_iter: Maximum bisection iterations.

    Returns:
        Implied volatility as a decimal, or None if the market price is
        outside the arbitrage bounds.
    """
    if T <= 0 or market_price <= 0:
        return None

    # Intrinsic value is the arbitrage lower bound
    if option_type == "call":
        intrinsic = max(spot - strike * math.exp(-r * T), 0.0)
        upper_bound = spot
    else:
        intrinsic = max(strike * math.exp(-r * T) - spot, 0.0)
        upper_bound = strike * math.exp(-r * T)

    if market_price < intrinsic - tol or market_price > upper_bound + tol:
        return None

    lo, hi = 1e-6, 5.0  # IV between ~0% and 500%

    # Verify the bracket brackets the root
    def _price_at(vol: float) -> float:
        return bs_price_and_greeks(spot, strike, T, r, vol, option_type)["price"]

    price_lo = _price_at(lo)
    price_hi = _price_at(hi)
    if not (price_lo - tol <= market_price <= price_hi + tol):
        return None

    for _ in range(max_iter):
        mid = (lo + hi) / 2.0
        price_mid = _price_at(mid)
        diff = price_mid - market_price
        if abs(diff) < tol:
            return round(mid, 6)
        if diff > 0:
            hi = mid
        else:
            lo = mid

    return round((lo + hi) / 2.0, 6)


# ── Multi-leg strategies ─────────────────────────────────────────────────────

@dataclass
class Leg:
    """A single leg in a multi-leg options strategy.

    Attributes:
        option_type: "call" or "put".
        strike: Strike price.
        side: "long" (buy) or "short" (sell).
        quantity: Number of contracts (always positive; side encodes direction).
        premium: Premium paid (long) or received (short) per contract.
    """

    option_type: OptionType
    strike: float
    side: Side
    quantity: int = 1
    premium: float = 0.0

    def payoff_at(self, spot_at_expiry: float) -> float:
        """Total P&L for this leg at expiry for a given spot price."""
        if self.option_type == "call":
            intrinsic = max(spot_at_expiry - self.strike, 0.0)
        else:
            intrinsic = max(self.strike - spot_at_expiry, 0.0)

        sign = 1 if self.side == "long" else -1
        # P&L = sign * (intrinsic - premium) * quantity
        return sign * (intrinsic - self.premium) * self.quantity


def payoff_curve(
    legs: list[Leg],
    spot_range: tuple[float, float] | None = None,
    n_points: int = 200,
) -> tuple[np.ndarray, np.ndarray]:
    """Compute the payoff curve for a strategy across a range of spot prices.

    Args:
        legs: List of Leg objects.
        spot_range: Tuple of (min_spot, max_spot). If omitted, defaults to
            ±50% of the median strike.
        n_points: Number of points in the curve.

    Returns:
        Tuple of (spot_prices, payoffs) as numpy arrays.
    """
    if not legs:
        raise ValueError("legs must not be empty")

    if spot_range is None:
        strikes = [leg.strike for leg in legs]
        median = float(np.median(strikes))
        spot_range = (median * 0.5, median * 1.5)

    spots = np.linspace(spot_range[0], spot_range[1], n_points)
    payoffs = np.array([
        sum(leg.payoff_at(float(s)) for leg in legs)
        for s in spots
    ])
    return spots, payoffs


def strategy_metrics(
    legs: list[Leg],
    spot_range: tuple[float, float] | None = None,
    n_points: int = 1000,
) -> dict[str, object]:
    """Compute max profit, max loss, and breakeven points for a strategy.

    Works by sampling the payoff curve at many points and extracting
    extrema and zero crossings. For vanilla strategies with clean
    breakevens this produces the right answer; for exotic structures
    increase n_points.

    Args:
        legs: List of Leg objects.
        spot_range: Optional spot range to analyze.
        n_points: Resolution of the payoff sampling (higher = more accurate).

    Returns:
        Dict with: max_profit, max_loss, breakevens (list of floats),
        net_debit (positive = paid, negative = credit received).
    """
    spots, payoffs = payoff_curve(legs, spot_range=spot_range, n_points=n_points)

    max_profit = float(payoffs.max())
    max_loss = float(payoffs.min())

    # Net debit = sum of long premiums paid minus short premiums received
    net_debit = sum(
        (leg.premium * leg.quantity * (1 if leg.side == "long" else -1))
        for leg in legs
    )

    # Find breakevens as zero crossings in the payoff curve
    breakevens: list[float] = []
    for i in range(len(payoffs) - 1):
        if payoffs[i] * payoffs[i + 1] < 0:  # sign change
            # Linear interpolation for the zero crossing
            x0, x1 = spots[i], spots[i + 1]
            y0, y1 = payoffs[i], payoffs[i + 1]
            zero_x = x0 - y0 * (x1 - x0) / (y1 - y0)
            breakevens.append(round(float(zero_x), 4))

    return {
        "max_profit": round(max_profit, 4),
        "max_loss": round(max_loss, 4),
        "breakevens": breakevens,
        "net_debit": round(float(net_debit), 4),
        "legs": len(legs),
    }


# ── Strategy constructors ────────────────────────────────────────────────────

def long_call(strike: float, premium: float, quantity: int = 1) -> list[Leg]:
    """Single long call."""
    return [Leg("call", strike, "long", quantity, premium)]


def long_put(strike: float, premium: float, quantity: int = 1) -> list[Leg]:
    """Single long put."""
    return [Leg("put", strike, "long", quantity, premium)]


def covered_call(
    strike: float,
    call_premium: float,
    stock_cost: float,
    quantity: int = 1,
) -> list[Leg]:
    """Covered call = long stock (modeled as an ATM long call at stock_cost) + short call.

    For accurate payoff modeling, we treat the long stock position as a
    deep-ITM long call at strike 0 with premium = stock_cost. This makes
    the payoff math identical to owning the shares.

    Args:
        strike: Strike of the short call.
        call_premium: Premium received for selling the call.
        stock_cost: Cost basis per share.
        quantity: Number of contracts (1 contract = 100 shares of stock).
    """
    return [
        Leg("call", 0.0, "long", quantity, stock_cost),  # synthetic long stock
        Leg("call", strike, "short", quantity, call_premium),
    ]


def protective_put(
    strike: float,
    put_premium: float,
    stock_cost: float,
    quantity: int = 1,
) -> list[Leg]:
    """Protective put = long stock + long put."""
    return [
        Leg("call", 0.0, "long", quantity, stock_cost),  # synthetic long stock
        Leg("put", strike, "long", quantity, put_premium),
    ]


def bull_call_spread(
    long_strike: float,
    short_strike: float,
    long_premium: float,
    short_premium: float,
    quantity: int = 1,
) -> list[Leg]:
    """Bull call (debit) spread: long lower-strike call, short higher-strike call."""
    if short_strike <= long_strike:
        raise ValueError("short_strike must be higher than long_strike for a bull call spread")
    return [
        Leg("call", long_strike, "long", quantity, long_premium),
        Leg("call", short_strike, "short", quantity, short_premium),
    ]


def bear_put_spread(
    long_strike: float,
    short_strike: float,
    long_premium: float,
    short_premium: float,
    quantity: int = 1,
) -> list[Leg]:
    """Bear put (debit) spread: long higher-strike put, short lower-strike put."""
    if short_strike >= long_strike:
        raise ValueError("short_strike must be lower than long_strike for a bear put spread")
    return [
        Leg("put", long_strike, "long", quantity, long_premium),
        Leg("put", short_strike, "short", quantity, short_premium),
    ]


def bull_put_spread(
    short_strike: float,
    long_strike: float,
    short_premium: float,
    long_premium: float,
    quantity: int = 1,
) -> list[Leg]:
    """Bull put (credit) spread: short higher-strike put, long lower-strike put."""
    if long_strike >= short_strike:
        raise ValueError("long_strike must be lower than short_strike for a bull put spread")
    return [
        Leg("put", short_strike, "short", quantity, short_premium),
        Leg("put", long_strike, "long", quantity, long_premium),
    ]


def bear_call_spread(
    short_strike: float,
    long_strike: float,
    short_premium: float,
    long_premium: float,
    quantity: int = 1,
) -> list[Leg]:
    """Bear call (credit) spread: short lower-strike call, long higher-strike call."""
    if long_strike <= short_strike:
        raise ValueError("long_strike must be higher than short_strike for a bear call spread")
    return [
        Leg("call", short_strike, "short", quantity, short_premium),
        Leg("call", long_strike, "long", quantity, long_premium),
    ]


def iron_condor(
    put_long_strike: float,
    put_short_strike: float,
    call_short_strike: float,
    call_long_strike: float,
    put_long_premium: float,
    put_short_premium: float,
    call_short_premium: float,
    call_long_premium: float,
    quantity: int = 1,
) -> list[Leg]:
    """Iron condor: short put spread + short call spread (net credit).

    Strikes must be ordered: put_long < put_short < call_short < call_long.
    """
    if not (put_long_strike < put_short_strike < call_short_strike < call_long_strike):
        raise ValueError(
            "iron condor strikes must satisfy: "
            "put_long < put_short < call_short < call_long"
        )
    return [
        Leg("put", put_long_strike, "long", quantity, put_long_premium),
        Leg("put", put_short_strike, "short", quantity, put_short_premium),
        Leg("call", call_short_strike, "short", quantity, call_short_premium),
        Leg("call", call_long_strike, "long", quantity, call_long_premium),
    ]


def long_straddle(
    strike: float,
    call_premium: float,
    put_premium: float,
    quantity: int = 1,
) -> list[Leg]:
    """Long straddle: long ATM call + long ATM put (volatility bet)."""
    return [
        Leg("call", strike, "long", quantity, call_premium),
        Leg("put", strike, "long", quantity, put_premium),
    ]


def long_strangle(
    call_strike: float,
    put_strike: float,
    call_premium: float,
    put_premium: float,
    quantity: int = 1,
) -> list[Leg]:
    """Long strangle: long OTM call + long OTM put (cheaper vol bet than straddle)."""
    if call_strike <= put_strike:
        raise ValueError("call_strike must exceed put_strike for a strangle")
    return [
        Leg("call", call_strike, "long", quantity, call_premium),
        Leg("put", put_strike, "long", quantity, put_premium),
    ]


def long_butterfly(
    lower_strike: float,
    middle_strike: float,
    upper_strike: float,
    lower_premium: float,
    middle_premium: float,
    upper_premium: float,
    quantity: int = 1,
) -> list[Leg]:
    """Long call butterfly (bullish, narrow pin): long 1 lower + short 2 middle + long 1 upper.

    Strikes must be evenly spaced for a classic butterfly.
    """
    if not (lower_strike < middle_strike < upper_strike):
        raise ValueError("butterfly strikes must be in ascending order")
    return [
        Leg("call", lower_strike, "long", quantity, lower_premium),
        Leg("call", middle_strike, "short", 2 * quantity, middle_premium),
        Leg("call", upper_strike, "long", quantity, upper_premium),
    ]
