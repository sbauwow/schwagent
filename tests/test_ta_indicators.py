"""Tests for the `ta` library wrapper."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from schwabagent.ta_indicators import (
    _ensure_ohlcv,
    apply_all,
    compute,
    indicator_names,
    list_indicators,
)


# ── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture
def synthetic_ohlcv() -> pd.DataFrame:
    """300 days of synthetic OHLCV with realistic structure."""
    rng = np.random.default_rng(42)
    n = 300
    dates = pd.date_range("2023-01-01", periods=n, freq="B")
    close = 100 * np.cumprod(1 + rng.normal(0.0005, 0.015, n))
    high = close * (1 + np.abs(rng.normal(0, 0.005, n)))
    low = close * (1 - np.abs(rng.normal(0, 0.005, n)))
    open_ = close * (1 + rng.normal(0, 0.003, n))
    volume = rng.integers(100_000, 500_000, n)
    return pd.DataFrame(
        {"open": open_, "high": high, "low": low, "close": close, "volume": volume},
        index=dates,
    )


@pytest.fixture
def capitalized_ohlcv(synthetic_ohlcv: pd.DataFrame) -> pd.DataFrame:
    """Same data but with yfinance-style capitalized column names."""
    return synthetic_ohlcv.rename(
        columns={c: c.capitalize() for c in synthetic_ohlcv.columns}
    )


# ── Validation ───────────────────────────────────────────────────────────────

def test_ensure_ohlcv_lowercase_ok(synthetic_ohlcv):
    frame = _ensure_ohlcv(synthetic_ohlcv)
    assert set(["open", "high", "low", "close", "volume"]) <= set(frame.columns)


def test_ensure_ohlcv_normalizes_capitalized(capitalized_ohlcv):
    frame = _ensure_ohlcv(capitalized_ohlcv)
    # Should be lowercased
    assert "close" in frame.columns
    assert "Close" not in frame.columns


def test_ensure_ohlcv_missing_columns_raises():
    df = pd.DataFrame({"close": [1, 2, 3], "volume": [10, 20, 30]})
    with pytest.raises(ValueError, match="missing required columns"):
        _ensure_ohlcv(df)


def test_ensure_ohlcv_empty_raises():
    with pytest.raises(ValueError, match="empty"):
        _ensure_ohlcv(pd.DataFrame())


# ── Registry / catalog ───────────────────────────────────────────────────────

def test_list_indicators_categories():
    cats = list_indicators()
    assert set(cats.keys()) == {"trend", "momentum", "volatility", "volume"}
    assert len(cats["trend"]) >= 10
    assert len(cats["momentum"]) >= 8
    assert len(cats["volatility"]) >= 4
    assert len(cats["volume"]) >= 8


def test_indicator_names_is_flat_sorted():
    names = indicator_names()
    assert len(names) >= 30
    assert names == sorted(names)
    assert "rsi" in names
    assert "macd" in names
    assert "bollinger" in names


def test_unknown_indicator_raises(synthetic_ohlcv):
    with pytest.raises(ValueError, match="Unknown indicator"):
        compute(synthetic_ohlcv, "magic")


# ── Scalar indicators (single Series output) ────────────────────────────────

@pytest.mark.parametrize(
    "name",
    ["sma", "ema", "wma", "psar", "cci", "trix",
     "rsi", "williams_r", "roc", "tsi", "kama", "ultimate", "awesome",
     "atr", "ulcer",
     "obv", "mfi", "vwap", "cmf", "adi", "force_index", "eom", "nvi", "vpt"],
)
def test_scalar_indicator_returns_series(synthetic_ohlcv, name):
    result = compute(synthetic_ohlcv, name)
    assert isinstance(result, pd.Series)
    assert len(result) == len(synthetic_ohlcv)
    assert result.index.equals(synthetic_ohlcv.index)


# ── Multi-output indicators (DataFrame) ─────────────────────────────────────

@pytest.mark.parametrize(
    ("name", "expected_cols"),
    [
        ("macd", {"macd", "signal", "histogram"}),
        ("adx", {"adx", "plus_di", "minus_di"}),
        ("ichimoku", {"tenkan", "kijun", "senkou_a", "senkou_b"}),
        ("aroon", {"aroon_up", "aroon_down", "aroon_indicator"}),
        ("vortex", {"vortex_pos", "vortex_neg"}),
        ("kst", {"kst", "kst_signal"}),
        ("stoch", {"stoch_k", "stoch_d"}),
        ("stochrsi", {"stochrsi", "stochrsi_k", "stochrsi_d"}),
        ("bollinger", {"bb_upper", "bb_middle", "bb_lower", "bb_width", "bb_pct_b"}),
        ("keltner", {"kc_upper", "kc_middle", "kc_lower"}),
        ("donchian", {"dc_upper", "dc_middle", "dc_lower"}),
    ],
)
def test_multi_output_indicator_returns_expected_columns(
    synthetic_ohlcv, name, expected_cols
):
    result = compute(synthetic_ohlcv, name)
    assert isinstance(result, pd.DataFrame)
    assert set(result.columns) == expected_cols
    assert len(result) == len(synthetic_ohlcv)


# ── Parameter passthrough ────────────────────────────────────────────────────

def test_sma_window_affects_output(synthetic_ohlcv):
    short = compute(synthetic_ohlcv, "sma", window=5)
    long = compute(synthetic_ohlcv, "sma", window=50)
    # Different windows should produce different non-NaN rows
    assert short.dropna().iloc[-1] != long.dropna().iloc[-1]


def test_rsi_bounded_0_to_100(synthetic_ohlcv):
    rsi = compute(synthetic_ohlcv, "rsi")
    clean = rsi.dropna()
    assert clean.min() >= 0
    assert clean.max() <= 100


def test_bollinger_bands_ordering(synthetic_ohlcv):
    bb = compute(synthetic_ohlcv, "bollinger")
    clean = bb.dropna()
    # Upper band should always be >= middle >= lower
    assert (clean["bb_upper"] >= clean["bb_middle"]).all()
    assert (clean["bb_middle"] >= clean["bb_lower"]).all()


def test_atr_is_non_negative(synthetic_ohlcv):
    atr = compute(synthetic_ohlcv, "atr")
    clean = atr.dropna()
    assert (clean >= 0).all()


# ── Case-insensitive indicator names ────────────────────────────────────────

def test_compute_case_insensitive(synthetic_ohlcv):
    lower = compute(synthetic_ohlcv, "rsi")
    upper = compute(synthetic_ohlcv, "RSI")
    mixed = compute(synthetic_ohlcv, "  Rsi  ")
    # Should all produce identical results
    pd.testing.assert_series_equal(lower, upper)
    pd.testing.assert_series_equal(lower, mixed)


# ── apply_all feature engineering ───────────────────────────────────────────

def test_apply_all_default_produces_many_features(synthetic_ohlcv):
    features = apply_all(synthetic_ohlcv)
    assert features.shape[1] >= 20
    assert len(features) == len(synthetic_ohlcv)
    assert features.index.equals(synthetic_ohlcv.index)


def test_apply_all_include_filters(synthetic_ohlcv):
    features = apply_all(synthetic_ohlcv, include=["rsi", "atr"])
    # Should contain rsi and atr, nothing else
    assert set(features.columns) == {"rsi", "atr"}


def test_apply_all_exclude_filters(synthetic_ohlcv):
    default_features = apply_all(synthetic_ohlcv)
    reduced = apply_all(synthetic_ohlcv, exclude=["rsi", "macd"])
    assert "rsi" in default_features.columns
    assert "rsi" not in reduced.columns
    assert "macd" not in reduced.columns
    # Other indicators should still be present
    assert "atr" in reduced.columns


def test_apply_all_multi_output_expanded(synthetic_ohlcv):
    """Multi-output indicators should contribute one column per output."""
    features = apply_all(synthetic_ohlcv, include=["macd", "bollinger"])
    assert "macd" in features.columns
    assert "signal" in features.columns
    assert "histogram" in features.columns
    assert "bb_upper" in features.columns
    assert "bb_lower" in features.columns


def test_apply_all_handles_failed_indicators(synthetic_ohlcv):
    """Indicators that fail should be logged and skipped, not crash."""
    # Short frame that will produce NaNs for long-window indicators but
    # shouldn't raise — the wrapper should just skip failures.
    short = synthetic_ohlcv.head(15)
    features = apply_all(short)
    # Should still produce some columns even if many are NaN
    assert features.shape[0] == 15
    assert features.shape[1] > 0


# ── Capitalized column support ──────────────────────────────────────────────

def test_compute_accepts_capitalized_columns(capitalized_ohlcv):
    rsi = compute(capitalized_ohlcv, "rsi")
    assert isinstance(rsi, pd.Series)
    assert len(rsi) == len(capitalized_ohlcv)


def test_apply_all_accepts_capitalized_columns(capitalized_ohlcv):
    features = apply_all(capitalized_ohlcv, include=["rsi", "atr", "obv"])
    assert "rsi" in features.columns
    assert "atr" in features.columns
    assert "obv" in features.columns
