"""Tests for schwab-agent options pricing and strategy analysis."""
from __future__ import annotations

import math

import numpy as np
import pytest

from schwabagent.options import (
    Leg,
    bear_call_spread,
    bear_put_spread,
    bs_price_and_greeks,
    bull_call_spread,
    bull_put_spread,
    covered_call,
    implied_volatility,
    iron_condor,
    long_butterfly,
    long_call,
    long_put,
    long_straddle,
    long_strangle,
    payoff_curve,
    protective_put,
    strategy_metrics,
)


# ── Black-Scholes pricing ────────────────────────────────────────────────────

def test_atm_call_price_positive():
    result = bs_price_and_greeks(
        spot=100, strike=100, T=30 / 365, r=0.05, sigma=0.20, option_type="call"
    )
    assert result["price"] > 0
    # ATM call delta should be ~0.5 + rate drift, in (0.5, 0.6)
    assert 0.50 <= result["delta"] <= 0.60


def test_atm_put_price_positive():
    result = bs_price_and_greeks(
        spot=100, strike=100, T=30 / 365, r=0.05, sigma=0.20, option_type="put"
    )
    assert result["price"] > 0
    # ATM put delta should be ~-0.5 + rate drift, in (-0.5, -0.4)
    assert -0.50 <= result["delta"] <= -0.40


def test_deep_itm_call_delta_near_one():
    result = bs_price_and_greeks(
        spot=150, strike=100, T=30 / 365, r=0.05, sigma=0.20, option_type="call"
    )
    assert result["delta"] > 0.95
    # Deep ITM call price ≈ intrinsic + small extrinsic
    intrinsic = 150 - 100 * math.exp(-0.05 * 30 / 365)
    assert result["price"] >= intrinsic - 0.1


def test_deep_otm_call_delta_near_zero():
    result = bs_price_and_greeks(
        spot=50, strike=100, T=30 / 365, r=0.05, sigma=0.20, option_type="call"
    )
    assert result["delta"] < 0.05
    assert 0 <= result["price"] < 0.5


def test_put_call_parity():
    """C - P = S - K*exp(-rT) for European options."""
    spot, strike, T, r, sigma = 100, 100, 30 / 365, 0.05, 0.20
    call = bs_price_and_greeks(spot, strike, T, r, sigma, "call")
    put = bs_price_and_greeks(spot, strike, T, r, sigma, "put")
    parity_lhs = call["price"] - put["price"]
    parity_rhs = spot - strike * math.exp(-r * T)
    assert parity_lhs == pytest.approx(parity_rhs, abs=1e-4)


def test_greeks_signs():
    """Delta signs and gamma/vega positivity."""
    result_call = bs_price_and_greeks(100, 100, 30 / 365, 0.05, 0.2, "call")
    result_put = bs_price_and_greeks(100, 100, 30 / 365, 0.05, 0.2, "put")
    # Call delta positive, put delta negative
    assert result_call["delta"] > 0
    assert result_put["delta"] < 0
    # Gamma and vega same for call and put, always positive
    assert result_call["gamma"] > 0
    assert result_put["gamma"] > 0
    assert result_call["gamma"] == result_put["gamma"]
    assert result_call["vega"] > 0
    assert result_call["vega"] == result_put["vega"]
    # Theta negative for long ATM options
    assert result_call["theta"] < 0
    assert result_put["theta"] < 0


def test_expiry_call_collapses_to_intrinsic():
    result = bs_price_and_greeks(110, 100, 0, 0.05, 0.20, "call")
    assert result["price"] == pytest.approx(10.0, abs=1e-6)
    assert result["gamma"] == 0.0
    assert result["vega"] == 0.0


def test_expiry_otm_call_is_zero():
    result = bs_price_and_greeks(90, 100, 0, 0.05, 0.20, "call")
    assert result["price"] == 0.0
    assert result["delta"] == 0.0


def test_invalid_option_type_raises():
    with pytest.raises(ValueError, match="must be 'call' or 'put'"):
        bs_price_and_greeks(100, 100, 0.1, 0.05, 0.20, "future")  # type: ignore[arg-type]


# ── Implied volatility solver ────────────────────────────────────────────────

def test_iv_round_trip_atm_call():
    """Price at known IV → solve IV → should match."""
    spot, strike, T, r, sigma = 100, 100, 60 / 365, 0.04, 0.25
    priced = bs_price_and_greeks(spot, strike, T, r, sigma, "call")
    iv = implied_volatility(priced["price"], spot, strike, T, r, "call")
    assert iv is not None
    assert iv == pytest.approx(sigma, abs=1e-4)


def test_iv_round_trip_otm_put():
    spot, strike, T, r, sigma = 100, 90, 90 / 365, 0.03, 0.30
    priced = bs_price_and_greeks(spot, strike, T, r, sigma, "put")
    iv = implied_volatility(priced["price"], spot, strike, T, r, "put")
    assert iv is not None
    assert iv == pytest.approx(sigma, abs=1e-4)


def test_iv_rejects_arbitrage_violating_price():
    """A price below intrinsic should return None."""
    # Intrinsic for an ITM call at spot=110, strike=100 is ~10
    iv = implied_volatility(
        market_price=0.50,  # way below intrinsic
        spot=110, strike=100, T=30 / 365, r=0.05, option_type="call",
    )
    assert iv is None


def test_iv_returns_none_for_zero_time():
    iv = implied_volatility(
        market_price=5.0, spot=100, strike=100, T=0, r=0.05, option_type="call"
    )
    assert iv is None


# ── Leg payoffs ──────────────────────────────────────────────────────────────

def test_long_call_payoff_at_strike():
    leg = Leg(option_type="call", strike=100, side="long", quantity=1, premium=5)
    # At strike: intrinsic = 0, paid premium, so P&L = -5
    assert leg.payoff_at(100) == pytest.approx(-5.0)


def test_long_call_payoff_above_strike():
    leg = Leg(option_type="call", strike=100, side="long", quantity=1, premium=5)
    # At 120: intrinsic = 20, premium = 5, P&L = 15
    assert leg.payoff_at(120) == pytest.approx(15.0)


def test_short_put_payoff_below_strike():
    leg = Leg(option_type="put", strike=100, side="short", quantity=1, premium=3)
    # Sold put at $3; at 90, intrinsic = 10, P&L = -(10 - 3) = -7
    assert leg.payoff_at(90) == pytest.approx(-7.0)


def test_short_put_payoff_above_strike():
    leg = Leg(option_type="put", strike=100, side="short", quantity=1, premium=3)
    # Sold put at $3; at 110, intrinsic = 0, P&L = +3 (keep premium)
    assert leg.payoff_at(110) == pytest.approx(3.0)


def test_quantity_multiplies_payoff():
    leg = Leg(option_type="call", strike=100, side="long", quantity=5, premium=2)
    assert leg.payoff_at(110) == pytest.approx(5 * (10 - 2))  # = 40


# ── Payoff curves and strategy metrics ──────────────────────────────────────

def test_long_call_breakeven():
    legs = long_call(strike=100, premium=5)
    m = strategy_metrics(legs, spot_range=(80, 120))
    assert len(m["breakevens"]) == 1
    assert m["breakevens"][0] == pytest.approx(105.0, abs=0.1)


def test_long_straddle_two_breakevens():
    legs = long_straddle(strike=100, call_premium=5, put_premium=5)
    m = strategy_metrics(legs, spot_range=(80, 120))
    # Breakevens should be at 100 ± 10 = 90 and 110
    assert len(m["breakevens"]) == 2
    low, high = sorted(m["breakevens"])
    assert low == pytest.approx(90.0, abs=0.2)
    assert high == pytest.approx(110.0, abs=0.2)


def test_long_straddle_max_loss_at_strike():
    legs = long_straddle(strike=100, call_premium=5, put_premium=5)
    m = strategy_metrics(legs, spot_range=(80, 120), n_points=10_001)
    # Total premium paid is 10; max loss should be about -10
    assert m["max_loss"] == pytest.approx(-10.0, abs=0.05)


def test_bull_call_spread_metrics():
    legs = bull_call_spread(
        long_strike=100, short_strike=110,
        long_premium=6, short_premium=2,
    )
    # Net debit = 4, max profit = width - debit = 6, max loss = 4
    m = strategy_metrics(legs, spot_range=(90, 120))
    assert m["net_debit"] == pytest.approx(4.0)
    assert m["max_profit"] == pytest.approx(6.0, abs=0.1)
    assert m["max_loss"] == pytest.approx(-4.0, abs=0.1)
    assert len(m["breakevens"]) == 1
    assert m["breakevens"][0] == pytest.approx(104.0, abs=0.1)


def test_bull_call_spread_wrong_strike_order_raises():
    with pytest.raises(ValueError, match="higher"):
        bull_call_spread(long_strike=110, short_strike=100, long_premium=5, short_premium=2)


def test_bear_put_spread_metrics():
    legs = bear_put_spread(
        long_strike=100, short_strike=90,
        long_premium=5, short_premium=1,
    )
    # Net debit = 4, width = 10, max profit = 6, max loss = 4
    m = strategy_metrics(legs, spot_range=(80, 110))
    assert m["net_debit"] == pytest.approx(4.0)
    assert m["max_profit"] == pytest.approx(6.0, abs=0.1)
    assert m["max_loss"] == pytest.approx(-4.0, abs=0.1)


def test_bull_put_spread_is_credit():
    legs = bull_put_spread(
        short_strike=100, long_strike=90,
        short_premium=4, long_premium=1,
    )
    # Net credit = 3 (so net_debit is -3)
    m = strategy_metrics(legs, spot_range=(80, 110))
    assert m["net_debit"] == pytest.approx(-3.0)
    assert m["max_profit"] == pytest.approx(3.0, abs=0.1)
    # Max loss = width - credit = 10 - 3 = 7
    assert m["max_loss"] == pytest.approx(-7.0, abs=0.1)


def test_iron_condor_credit_and_breakevens():
    legs = iron_condor(
        put_long_strike=90, put_short_strike=95,
        call_short_strike=105, call_long_strike=110,
        put_long_premium=0.5, put_short_premium=1.5,
        call_short_premium=1.5, call_long_premium=0.5,
    )
    # Net credit = 1.0 + 1.0 = 2.0
    m = strategy_metrics(legs, spot_range=(80, 120))
    assert m["net_debit"] == pytest.approx(-2.0)
    # Max profit = credit = 2
    assert m["max_profit"] == pytest.approx(2.0, abs=0.1)
    # Max loss = wing width - credit = 5 - 2 = 3
    assert m["max_loss"] == pytest.approx(-3.0, abs=0.1)
    # Two breakevens: short put - credit, short call + credit
    assert len(m["breakevens"]) == 2
    low, high = sorted(m["breakevens"])
    assert low == pytest.approx(93.0, abs=0.1)
    assert high == pytest.approx(107.0, abs=0.1)


def test_iron_condor_wrong_strike_order_raises():
    with pytest.raises(ValueError, match="put_long < put_short"):
        iron_condor(
            put_long_strike=95, put_short_strike=90,
            call_short_strike=105, call_long_strike=110,
            put_long_premium=0.5, put_short_premium=1.5,
            call_short_premium=1.5, call_long_premium=0.5,
        )


def test_long_strangle_two_breakevens():
    legs = long_strangle(
        call_strike=110, put_strike=90,
        call_premium=2, put_premium=2,
    )
    m = strategy_metrics(legs, spot_range=(70, 130))
    assert len(m["breakevens"]) == 2
    low, high = sorted(m["breakevens"])
    # Breakevens at put_strike - total_premium and call_strike + total_premium
    assert low == pytest.approx(86.0, abs=0.2)
    assert high == pytest.approx(114.0, abs=0.2)


def test_long_butterfly_has_three_strikes():
    legs = long_butterfly(
        lower_strike=95, middle_strike=100, upper_strike=105,
        lower_premium=6, middle_premium=3, upper_premium=1,
    )
    # 4 legs total: 1 long lower, 2 short middle, 1 long upper
    assert len(legs) == 3
    # The middle short has quantity 2
    assert legs[1].quantity == 2
    assert legs[1].side == "short"


def test_long_butterfly_payoff_peak_at_middle_strike():
    legs = long_butterfly(
        lower_strike=95, middle_strike=100, upper_strike=105,
        lower_premium=6, middle_premium=3, upper_premium=1,
    )
    # Payoff at middle strike should be the maximum
    spots, payoffs = payoff_curve(legs, spot_range=(90, 110), n_points=1001)
    idx_at_100 = int(np.argmin(np.abs(spots - 100)))
    # Max should be near (not exactly at due to sampling) the middle
    assert idx_at_100 == int(np.argmax(payoffs)) or abs(idx_at_100 - int(np.argmax(payoffs))) <= 2


# ── Covered call and protective put ─────────────────────────────────────────

def test_covered_call_capped_upside():
    # Bought stock at 100, sold 110-strike call for 2
    legs = covered_call(strike=110, call_premium=2, stock_cost=100)
    m = strategy_metrics(legs, spot_range=(80, 130))
    # Above 110, P&L is capped at (110 - 100 + 2) = 12
    assert m["max_profit"] == pytest.approx(12.0, abs=0.1)


def test_protective_put_limited_downside():
    # Bought stock at 100, bought 95-strike put for 3
    legs = protective_put(strike=95, put_premium=3, stock_cost=100)
    m = strategy_metrics(legs, spot_range=(70, 130))
    # Max loss below 95 is (100 - 95 + 3) = 8
    assert m["max_loss"] == pytest.approx(-8.0, abs=0.1)


# ── Empty / edge cases ───────────────────────────────────────────────────────

def test_empty_legs_raises():
    with pytest.raises(ValueError, match="legs must not be empty"):
        payoff_curve(legs=[])


def test_payoff_curve_default_spot_range():
    """If spot_range is None, it should default based on median strike."""
    legs = [Leg("call", 100, "long", 1, 5)]
    spots, payoffs = payoff_curve(legs)
    assert len(spots) == 200
    # Default range is ±50% of median strike
    assert spots[0] == pytest.approx(50.0)
    assert spots[-1] == pytest.approx(150.0)
