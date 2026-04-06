"""Intermarket regime detection — classifies macro environment from ETF proxies.

Uses cross-asset signals (equities, bonds, credit, gold, dollar, volatility)
to determine the current market regime and adjust strategy sizing accordingly.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any

import pandas as pd

from schwabagent.config import Config
from schwabagent.indicators import sma, roc

logger = logging.getLogger(__name__)

# Default reference ETF proxies for intermarket analysis
DEFAULT_REFERENCE_SYMBOLS = ["SPY", "TLT", "HYG", "GLD", "IWM", "UUP", "VIXY"]


# ── Regime Enum ────────────────────────────────────────────────────────────────

class Regime(Enum):
    """Market regime classification."""
    BULL = "bull"
    RECOVERY = "recovery"
    CORRECTION = "correction"
    BEAR = "bear"
    RISK_OFF = "risk_off"
    STAGFLATION = "stagflation"

    def label(self) -> str:
        """Human-readable label."""
        return {
            Regime.BULL: "Bull Market",
            Regime.RECOVERY: "Recovery",
            Regime.CORRECTION: "Correction",
            Regime.BEAR: "Bear Market",
            Regime.RISK_OFF: "Risk-Off",
            Regime.STAGFLATION: "Stagflation",
        }[self]

    def description(self) -> str:
        """Short description of the regime."""
        return {
            Regime.BULL: "Broad risk-on: equities trending up, low volatility, credit healthy",
            Regime.RECOVERY: "Early recovery: improving breadth, declining stress",
            Regime.CORRECTION: "Pullback in progress: mixed signals, elevated caution",
            Regime.BEAR: "Sustained downtrend: weak breadth, credit stress rising",
            Regime.RISK_OFF: "Crisis mode: flight to safety, high volatility, credit frozen",
            Regime.STAGFLATION: "Stagflation: gold rising, equities falling, strong dollar",
        }[self]

    def strategy_weights(self) -> dict[str, float]:
        """Strategy sizing multipliers for this regime."""
        return _STRATEGY_WEIGHTS[self]

    def color(self) -> str:
        """Rich color tag for terminal display."""
        return {
            Regime.BULL: "green",
            Regime.RECOVERY: "cyan",
            Regime.CORRECTION: "yellow",
            Regime.BEAR: "red",
            Regime.RISK_OFF: "bold red",
            Regime.STAGFLATION: "magenta",
        }[self]


# ── Strategy weight maps per regime ────────────────────────────────────────────

_STRATEGY_WEIGHTS: dict[Regime, dict[str, float]] = {
    Regime.BULL: {
        "momentum": 1.2, "etf_rotation": 1.2, "mean_reversion": 0.8,
        "trend_following": 0.9, "etf_scalp": 1.0, "composite": 1.0,
        "conviction_hold": 1.1,
    },
    Regime.RECOVERY: {
        "momentum": 1.0, "etf_rotation": 1.1, "mean_reversion": 1.0,
        "trend_following": 1.0, "etf_scalp": 1.0, "composite": 1.0,
        "conviction_hold": 1.0,
    },
    Regime.CORRECTION: {
        "momentum": 0.6, "etf_rotation": 0.8, "mean_reversion": 1.3,
        "trend_following": 0.7, "etf_scalp": 1.2, "composite": 1.0,
        "conviction_hold": 0.8,
    },
    Regime.BEAR: {
        "momentum": 0.3, "etf_rotation": 1.0, "mean_reversion": 1.0,
        "trend_following": 1.3, "etf_scalp": 0.8, "composite": 0.8,
        "conviction_hold": 0.5,
    },
    Regime.RISK_OFF: {
        "momentum": 0.0, "etf_rotation": 0.5, "mean_reversion": 0.5,
        "trend_following": 1.2, "etf_scalp": 0.5, "composite": 0.5,
        "conviction_hold": 0.3,
    },
    Regime.STAGFLATION: {
        "momentum": 0.4, "etf_rotation": 0.7, "mean_reversion": 0.8,
        "trend_following": 1.2, "etf_scalp": 0.7, "composite": 0.7,
        "conviction_hold": 0.5,
    },
}


# ── Data classes ───────────────────────────────────────────────────────────────

@dataclass
class RegimeSignal:
    """A single intermarket signal contributing to regime detection."""
    name: str
    value: float
    signal: int  # -1, 0, or +1
    weight: float = 1.0

    def indicator(self) -> str:
        """Return +/=/- indicator character."""
        if self.signal > 0:
            return "+"
        elif self.signal < 0:
            return "-"
        return "="


@dataclass
class RegimeResult:
    """Result of regime detection."""
    regime: Regime
    confidence: float
    signals: list[RegimeSignal]
    timestamp: str
    previous_regime: Regime | None = None
    composite_score: int = 0

    @property
    def changed(self) -> bool:
        """True if regime changed from previous detection."""
        return self.previous_regime is not None and self.previous_regime != self.regime

    def to_dict(self) -> dict[str, Any]:
        return {
            "regime": self.regime.value,
            "regime_label": self.regime.label(),
            "confidence": self.confidence,
            "composite_score": self.composite_score,
            "signals": [
                {"name": s.name, "value": s.value, "signal": s.signal, "weight": s.weight}
                for s in self.signals
            ],
            "timestamp": self.timestamp,
            "previous_regime": self.previous_regime.value if self.previous_regime else None,
            "changed": self.changed,
        }


# ── RegimeModel ────────────────────────────────────────────────────────────────

class RegimeModel:
    """Intermarket regime detection engine.

    Analyzes cross-asset ETF signals to classify the current macro regime
    and provide strategy sizing multipliers.
    """

    def __init__(self, config: Config):
        self.config = config
        self._enabled = getattr(config, "REGIME_ENABLED", True)
        ref_str = getattr(config, "REGIME_REFERENCE_SYMBOLS", "SPY,TLT,HYG,GLD,IWM,UUP,VIXY")
        self.reference_symbols = [s.strip().upper() for s in ref_str.split(",") if s.strip()]
        self._previous_regime: Regime | None = None
        self._regime_path = Path(getattr(config, "STATE_DIR", "~/.schwab-agent")).expanduser() / "regime.json"
        self._load_persisted()

    # ── Persistence ────────────────────────────────────────────────────────────

    def _load_persisted(self) -> None:
        """Load last known regime from disk."""
        try:
            if self._regime_path.exists():
                data = json.loads(self._regime_path.read_text())
                regime_val = data.get("regime")
                if regime_val:
                    self._previous_regime = Regime(regime_val)
                    logger.info("Loaded persisted regime: %s", self._previous_regime.label())
        except Exception as e:
            logger.warning("Could not load persisted regime: %s", e)

    def _save_regime(self, result: RegimeResult) -> None:
        """Persist current regime to disk."""
        try:
            self._regime_path.parent.mkdir(parents=True, exist_ok=True)
            self._regime_path.write_text(json.dumps({
                "regime": result.regime.value,
                "confidence": result.confidence,
                "composite_score": result.composite_score,
                "timestamp": result.timestamp,
            }, indent=2))
        except Exception as e:
            logger.warning("Could not save regime state: %s", e)

    # ── Signal computation ─────────────────────────────────────────────────────

    def _get_price(self, quotes: dict, symbol: str) -> float | None:
        """Extract last price from quotes dict."""
        q = quotes.get(symbol)
        if q is None:
            return None
        # Support both raw dicts and Quote-like objects
        if isinstance(q, dict):
            return q.get("lastPrice") or q.get("last_price") or q.get("mark")
        return getattr(q, "lastPrice", None) or getattr(q, "last_price", None)

    def _compute_spy_trend(self, quotes: dict, histories: dict | None) -> RegimeSignal:
        """SPY price vs SMA(200) — above = +1, below = -1."""
        price = self._get_price(quotes, "SPY")
        if price is None or histories is None or "SPY" not in histories:
            return RegimeSignal(name="SPY Trend", value=0.0, signal=0, weight=1.0)
        spy_hist = histories["SPY"]
        if isinstance(spy_hist, pd.DataFrame):
            prices = spy_hist["close"] if "close" in spy_hist.columns else spy_hist.iloc[:, 0]
        else:
            prices = spy_hist
        sma200 = sma(prices, 200)
        if pd.isna(sma200):
            return RegimeSignal(name="SPY Trend", value=price, signal=0, weight=1.0)
        sig = 1 if price > sma200 else -1
        return RegimeSignal(name="SPY Trend", value=round(price - sma200, 2), signal=sig, weight=1.0)

    def _compute_spy_momentum(self, quotes: dict, histories: dict | None) -> RegimeSignal:
        """SPY 20-day ROC — positive = +1, negative = -1."""
        if histories is None or "SPY" not in histories:
            return RegimeSignal(name="SPY Momentum", value=0.0, signal=0, weight=1.0)
        spy_hist = histories["SPY"]
        if isinstance(spy_hist, pd.DataFrame):
            prices = spy_hist["close"] if "close" in spy_hist.columns else spy_hist.iloc[:, 0]
        else:
            prices = spy_hist
        roc_val = roc(prices, 20)
        if pd.isna(roc_val):
            return RegimeSignal(name="SPY Momentum", value=0.0, signal=0, weight=1.0)
        sig = 1 if roc_val > 0 else -1
        return RegimeSignal(name="SPY Momentum", value=round(roc_val, 2), signal=sig, weight=1.0)

    def _compute_credit_stress(self, quotes: dict, histories: dict | None) -> RegimeSignal:
        """HYG vs TLT relative performance (30d) — HYG outperforming = +1."""
        if histories is None or "HYG" not in histories or "TLT" not in histories:
            return RegimeSignal(name="Credit Stress", value=0.0, signal=0, weight=1.0)
        hyg_hist = histories["HYG"]
        tlt_hist = histories["TLT"]
        if isinstance(hyg_hist, pd.DataFrame):
            hyg_prices = hyg_hist["close"] if "close" in hyg_hist.columns else hyg_hist.iloc[:, 0]
        else:
            hyg_prices = hyg_hist
        if isinstance(tlt_hist, pd.DataFrame):
            tlt_prices = tlt_hist["close"] if "close" in tlt_hist.columns else tlt_hist.iloc[:, 0]
        else:
            tlt_prices = tlt_hist

        hyg_roc = roc(hyg_prices, 30)
        tlt_roc = roc(tlt_prices, 30)
        if pd.isna(hyg_roc) or pd.isna(tlt_roc):
            return RegimeSignal(name="Credit Stress", value=0.0, signal=0, weight=1.0)
        rel_perf = hyg_roc - tlt_roc
        sig = 1 if rel_perf > 0 else -1
        return RegimeSignal(name="Credit Stress", value=round(rel_perf, 2), signal=sig, weight=1.0)

    def _compute_safe_haven(self, quotes: dict, histories: dict | None) -> RegimeSignal:
        """GLD trend — above SMA(50) = +1, below = -1."""
        price = self._get_price(quotes, "GLD")
        if price is None or histories is None or "GLD" not in histories:
            return RegimeSignal(name="Safe Haven (GLD)", value=0.0, signal=0, weight=1.0)
        gld_hist = histories["GLD"]
        if isinstance(gld_hist, pd.DataFrame):
            prices = gld_hist["close"] if "close" in gld_hist.columns else gld_hist.iloc[:, 0]
        else:
            prices = gld_hist
        sma50 = sma(prices, 50)
        if pd.isna(sma50):
            return RegimeSignal(name="Safe Haven (GLD)", value=0.0, signal=0, weight=1.0)
        sig = 1 if price > sma50 else -1
        return RegimeSignal(name="Safe Haven (GLD)", value=round(price - sma50, 2), signal=sig, weight=1.0)

    def _compute_breadth(self, quotes: dict, histories: dict | None) -> RegimeSignal:
        """IWM vs SPY relative performance (20d) — IWM outperforming = +1."""
        if histories is None or "IWM" not in histories or "SPY" not in histories:
            return RegimeSignal(name="Breadth (IWM/SPY)", value=0.0, signal=0, weight=1.0)
        iwm_hist = histories["IWM"]
        spy_hist = histories["SPY"]
        if isinstance(iwm_hist, pd.DataFrame):
            iwm_prices = iwm_hist["close"] if "close" in iwm_hist.columns else iwm_hist.iloc[:, 0]
        else:
            iwm_prices = iwm_hist
        if isinstance(spy_hist, pd.DataFrame):
            spy_prices = spy_hist["close"] if "close" in spy_hist.columns else spy_hist.iloc[:, 0]
        else:
            spy_prices = spy_hist

        iwm_roc = roc(iwm_prices, 20)
        spy_roc = roc(spy_prices, 20)
        if pd.isna(iwm_roc) or pd.isna(spy_roc):
            return RegimeSignal(name="Breadth (IWM/SPY)", value=0.0, signal=0, weight=1.0)
        rel_perf = iwm_roc - spy_roc
        sig = 1 if rel_perf > 0 else -1
        return RegimeSignal(name="Breadth (IWM/SPY)", value=round(rel_perf, 2), signal=sig, weight=1.0)

    def _compute_dollar(self, quotes: dict, histories: dict | None) -> RegimeSignal:
        """UUP trend — above SMA(50) = +1 (strong dollar), below = -1."""
        price = self._get_price(quotes, "UUP")
        if price is None or histories is None or "UUP" not in histories:
            return RegimeSignal(name="Dollar (UUP)", value=0.0, signal=0, weight=1.0)
        uup_hist = histories["UUP"]
        if isinstance(uup_hist, pd.DataFrame):
            prices = uup_hist["close"] if "close" in uup_hist.columns else uup_hist.iloc[:, 0]
        else:
            prices = uup_hist
        sma50 = sma(prices, 50)
        if pd.isna(sma50):
            return RegimeSignal(name="Dollar (UUP)", value=0.0, signal=0, weight=1.0)
        sig = 1 if price > sma50 else -1
        return RegimeSignal(name="Dollar (UUP)", value=round(price - sma50, 2), signal=sig, weight=1.0)

    def _compute_volatility(self, quotes: dict, histories: dict | None) -> RegimeSignal:
        """VIX proxy (VIXY) level — below 20 = +1, 20-30 = 0, above 30 = -1.

        Note: VIXY is an ETF that tracks VIX futures, not the VIX index directly.
        We use the raw price and interpret thresholds contextually. If the quote
        dict contains a 'VIX' key (e.g. from $VIX.X), we prefer that.
        """
        # Try VIX directly first, then VIXY ETF proxy
        vix_level = None
        for sym in ("VIX", "$VIX.X", "VIXY"):
            price = self._get_price(quotes, sym)
            if price is not None:
                vix_level = price
                break

        if vix_level is None:
            return RegimeSignal(name="Volatility (VIX)", value=0.0, signal=0, weight=1.0)

        if vix_level < 20:
            sig = 1
        elif vix_level > 30:
            sig = -1
        else:
            sig = 0
        return RegimeSignal(name="Volatility (VIX)", value=round(vix_level, 2), signal=sig, weight=1.0)

    # ── Regime classification ──────────────────────────────────────────────────

    def _classify(self, signals: list[RegimeSignal]) -> tuple[Regime, float]:
        """Classify regime from composite signal score.

        Returns (regime, confidence) where confidence is 0.0–1.0.
        """
        # Build signal lookup by name prefix
        signal_map: dict[str, int] = {}
        for s in signals:
            signal_map[s.name] = s.signal

        composite_score = sum(s.signal * s.weight for s in signals)
        score = int(round(composite_score))

        # Special case: Stagflation detection
        gld_sig = signal_map.get("Safe Haven (GLD)", 0)
        spy_sig = signal_map.get("SPY Trend", 0)
        dollar_sig = signal_map.get("Dollar (UUP)", 0)
        if gld_sig == 1 and spy_sig == -1 and dollar_sig == 1:
            # Confidence based on how many other signals confirm distress
            conf = min(1.0, 0.5 + 0.1 * abs(score))
            return Regime.STAGFLATION, conf

        # Standard regime from composite score
        if score >= 4:
            regime = Regime.BULL
        elif score >= 2:
            regime = Regime.RECOVERY
        elif score >= 0:
            regime = Regime.CORRECTION
        elif score >= -2:
            regime = Regime.BEAR
        else:
            regime = Regime.RISK_OFF

        # Confidence: how decisive the score is (max signals = 7)
        max_possible = sum(s.weight for s in signals) or 7.0
        confidence = min(1.0, abs(composite_score) / max_possible)
        return regime, round(confidence, 3)

    # ── Main detect method ─────────────────────────────────────────────────────

    def detect(
        self,
        quotes: dict,
        price_histories: dict | None = None,
    ) -> RegimeResult:
        """Detect the current market regime from quotes and optional price histories.

        Args:
            quotes: dict of symbol -> Quote (or dict with lastPrice).
            price_histories: dict of symbol -> pd.DataFrame or pd.Series
                with historical close prices. Required for SMA/ROC signals.

        Returns:
            RegimeResult with the detected regime, confidence, and signals.
        """
        if not self._enabled:
            return RegimeResult(
                regime=Regime.RECOVERY,
                confidence=0.0,
                signals=[],
                timestamp=datetime.now(timezone.utc).isoformat(),
                previous_regime=self._previous_regime,
                composite_score=0,
            )

        # Compute all 7 signals
        signals = [
            self._compute_spy_trend(quotes, price_histories),
            self._compute_spy_momentum(quotes, price_histories),
            self._compute_credit_stress(quotes, price_histories),
            self._compute_safe_haven(quotes, price_histories),
            self._compute_breadth(quotes, price_histories),
            self._compute_dollar(quotes, price_histories),
            self._compute_volatility(quotes, price_histories),
        ]

        regime, confidence = self._classify(signals)
        composite_score = int(round(sum(s.signal * s.weight for s in signals)))

        result = RegimeResult(
            regime=regime,
            confidence=confidence,
            signals=signals,
            timestamp=datetime.now(timezone.utc).isoformat(),
            previous_regime=self._previous_regime,
            composite_score=composite_score,
        )

        # Persist and update previous regime
        self._save_regime(result)
        self._previous_regime = regime

        return result

    # ── Display ────────────────────────────────────────────────────────────────

    @staticmethod
    def display_regime(result: RegimeResult) -> None:
        """Rich terminal output of the regime result."""
        from rich.console import Console
        from rich.table import Table

        console = Console()
        color = result.regime.color()

        console.print()
        console.print(f"  [{color} bold]■ Market Regime: {result.regime.label()}[/{color} bold]")
        console.print(f"    {result.regime.description()}")
        console.print(f"    Confidence: {result.confidence:.0%}  |  Score: {result.composite_score}")
        if result.changed:
            prev = result.previous_regime
            prev_label = prev.label() if prev else "Unknown"
            console.print(f"    [yellow]⚠ Regime changed from {prev_label}![/yellow]")
        console.print()

        table = Table(show_header=True, show_lines=False, padding=(0, 1))
        table.add_column("Signal", style="cyan", min_width=20)
        table.add_column("Dir", justify="center", min_width=3)
        table.add_column("Value", justify="right", min_width=10)

        for sig in result.signals:
            indicator = sig.indicator()
            if indicator == "+":
                dir_str = "[green]+[/green]"
            elif indicator == "-":
                dir_str = "[red]-[/red]"
            else:
                dir_str = "[yellow]=[/yellow]"
            table.add_row(sig.name, dir_str, f"{sig.value:+.2f}")

        console.print(table)
        console.print()

        # Show strategy weights for current regime
        weights = result.regime.strategy_weights()
        weight_parts = []
        for strat, w in sorted(weights.items()):
            if w >= 1.0:
                weight_parts.append(f"[green]{strat}:{w:.1f}[/green]")
            elif w >= 0.5:
                weight_parts.append(f"[yellow]{strat}:{w:.1f}[/yellow]")
            else:
                weight_parts.append(f"[red]{strat}:{w:.1f}[/red]")
        console.print(f"  Strategy weights: {' '.join(weight_parts)}")
        console.print()


# ── Module-level helpers ───────────────────────────────────────────────────────

def regime_sizing_factor(regime: Regime, strategy_name: str) -> float:
    """Return the sizing multiplier for a strategy in the given regime.

    Defaults to 1.0 for unknown strategies.
    """
    weights = regime.strategy_weights()
    return weights.get(strategy_name, 1.0)


def display_regime(result: RegimeResult) -> None:
    """Module-level convenience for RegimeModel.display_regime()."""
    RegimeModel.display_regime(result)
