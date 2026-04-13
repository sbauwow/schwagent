"""Technical indicators wrapper around the `ta` library.

Thin schwagent-friendly layer over https://github.com/bukosabino/ta
that accepts a standard OHLCV DataFrame (columns: open, high, low, close,
volume, indexed by date) and returns either a single indicator Series or
a feature DataFrame with many indicators ready for ML/backtesting.

Why not the custom indicators.py that already exists?
    The existing module has ~31 hand-written indicators. This module
    wraps a battle-tested library with ~45 indicators (including
    Ichimoku, Vortex, Aroon, TSI, KAMA, Keltner, Donchian, MFI, CMF,
    Accumulation/Distribution, Force Index, Ultimate Oscillator, ADX
    with +DI/-DI breakdown, etc.). Both can coexist — strategies pick
    whichever fits.

Example:
    >>> from schwabagent.ta_indicators import compute, apply_all
    >>> rsi = compute(df, "rsi", window=14)
    >>> macd = compute(df, "macd")           # → DataFrame with 3 columns
    >>> features = apply_all(df)             # → DataFrame with 25+ columns
"""
from __future__ import annotations

import logging
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


# Required OHLCV columns (lowercase canonical form used by SchwabClient)
_OHLCV_COLS = ("open", "high", "low", "close", "volume")


# ── Validation ───────────────────────────────────────────────────────────────

def _ensure_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    """Validate and normalize an OHLCV DataFrame.

    Accepts either lowercase columns (open/high/low/close/volume — the
    schwagent convention) or capitalized (Open/High/Low/Close/Volume
    — the yfinance convention). Returns a copy with lowercase columns.

    Raises:
        ValueError: If required OHLCV columns are missing.
    """
    if df is None or df.empty:
        raise ValueError("OHLCV DataFrame is empty")

    out = df.copy()
    # Normalize capitalized columns to lowercase
    rename_map = {
        c: c.lower() for c in out.columns
        if isinstance(c, str) and c.lower() in _OHLCV_COLS and c != c.lower()
    }
    if rename_map:
        out = out.rename(columns=rename_map)

    missing = [c for c in _OHLCV_COLS if c not in out.columns]
    if missing:
        raise ValueError(
            f"OHLCV DataFrame is missing required columns: {missing}. "
            f"Got: {list(out.columns)}"
        )
    return out


# ── Single-indicator compute ─────────────────────────────────────────────────

def compute(
    df: pd.DataFrame,
    indicator: str,
    **kwargs: Any,
) -> pd.Series | pd.DataFrame:
    """Compute a single indicator on an OHLCV DataFrame.

    Args:
        df: OHLCV DataFrame (columns: open, high, low, close, volume).
        indicator: Indicator name (case-insensitive). See `list_indicators()`
            for the full catalog.
        **kwargs: Per-indicator parameters (e.g. window, fast, slow). Each
            indicator falls back to sensible defaults when omitted.

    Returns:
        A pd.Series for scalar indicators (e.g. rsi, cci, atr) or a
        pd.DataFrame when the indicator produces multiple columns (e.g.
        macd produces macd/signal/histogram, bollinger produces upper/
        middle/lower).

    Raises:
        ValueError: If the indicator name is unknown or the DataFrame
            is missing required OHLCV columns.
    """
    frame = _ensure_ohlcv(df)
    key = indicator.lower().strip()

    fn = _REGISTRY.get(key)
    if fn is None:
        available = ", ".join(sorted(_REGISTRY.keys()))
        raise ValueError(
            f"Unknown indicator: {indicator!r}. Available: {available}"
        )

    return fn(frame, **kwargs)


# ── Individual indicator wrappers ────────────────────────────────────────────
# Each returns a pd.Series for scalar indicators, or a pd.DataFrame for
# indicators that produce multiple outputs.

def _sma(df: pd.DataFrame, window: int = 20) -> pd.Series:
    from ta.trend import SMAIndicator
    return SMAIndicator(close=df["close"], window=window).sma_indicator()


def _ema(df: pd.DataFrame, window: int = 20) -> pd.Series:
    from ta.trend import EMAIndicator
    return EMAIndicator(close=df["close"], window=window).ema_indicator()


def _wma(df: pd.DataFrame, window: int = 20) -> pd.Series:
    from ta.trend import WMAIndicator
    return WMAIndicator(close=df["close"], window=window).wma()


def _macd(
    df: pd.DataFrame, fast: int = 12, slow: int = 26, signal: int = 9
) -> pd.DataFrame:
    from ta.trend import MACD
    ind = MACD(
        close=df["close"], window_fast=fast, window_slow=slow, window_sign=signal
    )
    return pd.DataFrame({
        "macd": ind.macd(),
        "signal": ind.macd_signal(),
        "histogram": ind.macd_diff(),
    })


def _adx(df: pd.DataFrame, window: int = 14) -> pd.DataFrame:
    from ta.trend import ADXIndicator
    ind = ADXIndicator(
        high=df["high"], low=df["low"], close=df["close"], window=window
    )
    return pd.DataFrame({
        "adx": ind.adx(),
        "plus_di": ind.adx_pos(),
        "minus_di": ind.adx_neg(),
    })


def _ichimoku(df: pd.DataFrame) -> pd.DataFrame:
    from ta.trend import IchimokuIndicator
    ind = IchimokuIndicator(high=df["high"], low=df["low"])
    return pd.DataFrame({
        "tenkan": ind.ichimoku_conversion_line(),
        "kijun": ind.ichimoku_base_line(),
        "senkou_a": ind.ichimoku_a(),
        "senkou_b": ind.ichimoku_b(),
    })


def _aroon(df: pd.DataFrame, window: int = 25) -> pd.DataFrame:
    from ta.trend import AroonIndicator
    ind = AroonIndicator(high=df["high"], low=df["low"], window=window)
    return pd.DataFrame({
        "aroon_up": ind.aroon_up(),
        "aroon_down": ind.aroon_down(),
        "aroon_indicator": ind.aroon_indicator(),
    })


def _psar(df: pd.DataFrame, step: float = 0.02, max_step: float = 0.2) -> pd.Series:
    from ta.trend import PSARIndicator
    result = PSARIndicator(
        high=df["high"], low=df["low"], close=df["close"], step=step, max_step=max_step
    ).psar()
    # PSAR can occasionally return a reindexed Series; snap back to the
    # input index so downstream code (including apply_all) works cleanly.
    return result.reindex(df.index)


def _cci(df: pd.DataFrame, window: int = 20) -> pd.Series:
    from ta.trend import CCIIndicator
    return CCIIndicator(
        high=df["high"], low=df["low"], close=df["close"], window=window
    ).cci()


def _vortex(df: pd.DataFrame, window: int = 14) -> pd.DataFrame:
    from ta.trend import VortexIndicator
    ind = VortexIndicator(
        high=df["high"], low=df["low"], close=df["close"], window=window
    )
    return pd.DataFrame({
        "vortex_pos": ind.vortex_indicator_pos(),
        "vortex_neg": ind.vortex_indicator_neg(),
    })


def _trix(df: pd.DataFrame, window: int = 15) -> pd.Series:
    from ta.trend import TRIXIndicator
    return TRIXIndicator(close=df["close"], window=window).trix()


def _kst(df: pd.DataFrame) -> pd.DataFrame:
    from ta.trend import KSTIndicator
    ind = KSTIndicator(close=df["close"])
    return pd.DataFrame({
        "kst": ind.kst(),
        "kst_signal": ind.kst_sig(),
    })


# ── Momentum ─────────────────────────────────────────────────────────────────

def _rsi(df: pd.DataFrame, window: int = 14) -> pd.Series:
    from ta.momentum import RSIIndicator
    return RSIIndicator(close=df["close"], window=window).rsi()


def _stoch(
    df: pd.DataFrame, window: int = 14, smooth_window: int = 3
) -> pd.DataFrame:
    from ta.momentum import StochasticOscillator
    ind = StochasticOscillator(
        high=df["high"], low=df["low"], close=df["close"],
        window=window, smooth_window=smooth_window,
    )
    return pd.DataFrame({"stoch_k": ind.stoch(), "stoch_d": ind.stoch_signal()})


def _stoch_rsi(df: pd.DataFrame, window: int = 14) -> pd.DataFrame:
    from ta.momentum import StochRSIIndicator
    ind = StochRSIIndicator(close=df["close"], window=window)
    return pd.DataFrame({
        "stochrsi": ind.stochrsi(),
        "stochrsi_k": ind.stochrsi_k(),
        "stochrsi_d": ind.stochrsi_d(),
    })


def _williams_r(df: pd.DataFrame, lbp: int = 14) -> pd.Series:
    from ta.momentum import WilliamsRIndicator
    return WilliamsRIndicator(
        high=df["high"], low=df["low"], close=df["close"], lbp=lbp
    ).williams_r()


def _roc(df: pd.DataFrame, window: int = 12) -> pd.Series:
    from ta.momentum import ROCIndicator
    return ROCIndicator(close=df["close"], window=window).roc()


def _tsi(df: pd.DataFrame, window_slow: int = 25, window_fast: int = 13) -> pd.Series:
    from ta.momentum import TSIIndicator
    return TSIIndicator(
        close=df["close"], window_slow=window_slow, window_fast=window_fast
    ).tsi()


def _kama(df: pd.DataFrame, window: int = 10) -> pd.Series:
    from ta.momentum import KAMAIndicator
    return KAMAIndicator(close=df["close"], window=window).kama()


def _ultimate(df: pd.DataFrame) -> pd.Series:
    from ta.momentum import UltimateOscillator
    return UltimateOscillator(
        high=df["high"], low=df["low"], close=df["close"]
    ).ultimate_oscillator()


def _awesome(df: pd.DataFrame) -> pd.Series:
    from ta.momentum import AwesomeOscillatorIndicator
    return AwesomeOscillatorIndicator(
        high=df["high"], low=df["low"]
    ).awesome_oscillator()


# ── Volatility ───────────────────────────────────────────────────────────────

def _bollinger(
    df: pd.DataFrame, window: int = 20, window_dev: int = 2
) -> pd.DataFrame:
    from ta.volatility import BollingerBands
    ind = BollingerBands(close=df["close"], window=window, window_dev=window_dev)
    return pd.DataFrame({
        "bb_upper": ind.bollinger_hband(),
        "bb_middle": ind.bollinger_mavg(),
        "bb_lower": ind.bollinger_lband(),
        "bb_width": ind.bollinger_wband(),
        "bb_pct_b": ind.bollinger_pband(),
    })


def _atr(df: pd.DataFrame, window: int = 14) -> pd.Series:
    from ta.volatility import AverageTrueRange
    return AverageTrueRange(
        high=df["high"], low=df["low"], close=df["close"], window=window
    ).average_true_range()


def _keltner(
    df: pd.DataFrame, window: int = 20, window_atr: int = 10
) -> pd.DataFrame:
    from ta.volatility import KeltnerChannel
    ind = KeltnerChannel(
        high=df["high"], low=df["low"], close=df["close"],
        window=window, window_atr=window_atr,
    )
    return pd.DataFrame({
        "kc_upper": ind.keltner_channel_hband(),
        "kc_middle": ind.keltner_channel_mband(),
        "kc_lower": ind.keltner_channel_lband(),
    })


def _donchian(df: pd.DataFrame, window: int = 20) -> pd.DataFrame:
    from ta.volatility import DonchianChannel
    ind = DonchianChannel(
        high=df["high"], low=df["low"], close=df["close"], window=window
    )
    return pd.DataFrame({
        "dc_upper": ind.donchian_channel_hband(),
        "dc_middle": ind.donchian_channel_mband(),
        "dc_lower": ind.donchian_channel_lband(),
    })


def _ulcer(df: pd.DataFrame, window: int = 14) -> pd.Series:
    from ta.volatility import UlcerIndex
    return UlcerIndex(close=df["close"], window=window).ulcer_index()


# ── Volume ───────────────────────────────────────────────────────────────────

def _obv(df: pd.DataFrame) -> pd.Series:
    from ta.volume import OnBalanceVolumeIndicator
    return OnBalanceVolumeIndicator(
        close=df["close"], volume=df["volume"]
    ).on_balance_volume()


def _mfi(df: pd.DataFrame, window: int = 14) -> pd.Series:
    from ta.volume import MFIIndicator
    return MFIIndicator(
        high=df["high"], low=df["low"], close=df["close"],
        volume=df["volume"], window=window,
    ).money_flow_index()


def _vwap(df: pd.DataFrame, window: int = 14) -> pd.Series:
    from ta.volume import VolumeWeightedAveragePrice
    return VolumeWeightedAveragePrice(
        high=df["high"], low=df["low"], close=df["close"],
        volume=df["volume"], window=window,
    ).volume_weighted_average_price()


def _cmf(df: pd.DataFrame, window: int = 20) -> pd.Series:
    from ta.volume import ChaikinMoneyFlowIndicator
    return ChaikinMoneyFlowIndicator(
        high=df["high"], low=df["low"], close=df["close"],
        volume=df["volume"], window=window,
    ).chaikin_money_flow()


def _adi(df: pd.DataFrame) -> pd.Series:
    from ta.volume import AccDistIndexIndicator
    return AccDistIndexIndicator(
        high=df["high"], low=df["low"], close=df["close"], volume=df["volume"],
    ).acc_dist_index()


def _force_index(df: pd.DataFrame, window: int = 13) -> pd.Series:
    from ta.volume import ForceIndexIndicator
    return ForceIndexIndicator(
        close=df["close"], volume=df["volume"], window=window,
    ).force_index()


def _eom(df: pd.DataFrame, window: int = 14) -> pd.Series:
    from ta.volume import EaseOfMovementIndicator
    return EaseOfMovementIndicator(
        high=df["high"], low=df["low"], volume=df["volume"], window=window,
    ).ease_of_movement()


def _nvi(df: pd.DataFrame) -> pd.Series:
    from ta.volume import NegativeVolumeIndexIndicator
    return NegativeVolumeIndexIndicator(
        close=df["close"], volume=df["volume"]
    ).negative_volume_index()


def _vpt(df: pd.DataFrame) -> pd.Series:
    from ta.volume import VolumePriceTrendIndicator
    return VolumePriceTrendIndicator(
        close=df["close"], volume=df["volume"]
    ).volume_price_trend()


# ── Registry (single source of truth for indicator names) ────────────────────

_REGISTRY: dict[str, Any] = {
    # trend
    "sma": _sma,
    "ema": _ema,
    "wma": _wma,
    "macd": _macd,
    "adx": _adx,
    "ichimoku": _ichimoku,
    "aroon": _aroon,
    "psar": _psar,
    "cci": _cci,
    "vortex": _vortex,
    "trix": _trix,
    "kst": _kst,
    # momentum
    "rsi": _rsi,
    "stoch": _stoch,
    "stochrsi": _stoch_rsi,
    "williams_r": _williams_r,
    "roc": _roc,
    "tsi": _tsi,
    "kama": _kama,
    "ultimate": _ultimate,
    "awesome": _awesome,
    # volatility
    "bollinger": _bollinger,
    "atr": _atr,
    "keltner": _keltner,
    "donchian": _donchian,
    "ulcer": _ulcer,
    # volume
    "obv": _obv,
    "mfi": _mfi,
    "vwap": _vwap,
    "cmf": _cmf,
    "adi": _adi,
    "force_index": _force_index,
    "eom": _eom,
    "nvi": _nvi,
    "vpt": _vpt,
}

_CATEGORIES: dict[str, list[str]] = {
    "trend": ["sma", "ema", "wma", "macd", "adx", "ichimoku", "aroon", "psar", "cci", "vortex", "trix", "kst"],
    "momentum": ["rsi", "stoch", "stochrsi", "williams_r", "roc", "tsi", "kama", "ultimate", "awesome"],
    "volatility": ["bollinger", "atr", "keltner", "donchian", "ulcer"],
    "volume": ["obv", "mfi", "vwap", "cmf", "adi", "force_index", "eom", "nvi", "vpt"],
}


# ── Catalog helpers ──────────────────────────────────────────────────────────

def list_indicators() -> dict[str, list[str]]:
    """Return the available indicators grouped by category."""
    return {cat: list(names) for cat, names in _CATEGORIES.items()}


def indicator_names() -> list[str]:
    """Return a flat sorted list of all available indicator names."""
    return sorted(_REGISTRY.keys())


# ── Bulk feature engineering ─────────────────────────────────────────────────

def apply_all(
    df: pd.DataFrame,
    include: list[str] | None = None,
    exclude: list[str] | None = None,
) -> pd.DataFrame:
    """Compute many indicators at once and return a wide feature DataFrame.

    Useful for ML feature engineering and backtest research. The default
    set is a curated "everything reasonable on a daily timeframe" bundle
    that produces ~25 feature columns. Multi-output indicators (macd,
    bollinger, etc.) contribute one column per output.

    Args:
        df: OHLCV DataFrame.
        include: Only compute these indicators (default: a curated set).
        exclude: Skip these indicators.

    Returns:
        DataFrame indexed by the source df index, with one column per
        indicator output.
    """
    default_set = [
        "sma", "ema", "macd", "adx", "cci", "trix",
        "rsi", "stoch", "williams_r", "roc", "tsi",
        "bollinger", "atr", "keltner", "donchian",
        "obv", "mfi", "cmf", "adi",
    ]
    names = include if include is not None else default_set
    if exclude:
        names = [n for n in names if n not in exclude]

    frame = _ensure_ohlcv(df)
    features: dict[str, pd.Series] = {}

    for name in names:
        try:
            result = compute(frame, name)
        except Exception as e:
            logger.warning("Skipping %s: %s", name, e)
            continue

        if isinstance(result, pd.Series):
            features[name] = result
        elif isinstance(result, pd.DataFrame):
            for col in result.columns:
                features[col] = result[col]

    return pd.DataFrame(features, index=frame.index)
