"""Walk-forward backtester for schwagent strategies.

Replays historical OHLCV data through strategy signal logic and simulates
execution with configurable position sizing, commission, and slippage.

Usage:
    from schwabagent.backtest import Backtester, BacktestConfig
    bt = Backtester(BacktestConfig(
        strategy="momentum",
        symbols=["AAPL", "MSFT"],
        start="2020-01-01",
        end="2024-12-31",
    ))
    result = bt.run()
    result.print_report()
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from schwabagent.indicators import adx, bollinger_bands, ema, macd, rsi, sma

logger = logging.getLogger(__name__)

_TRADING_DAYS_PER_YEAR = 252
_RISK_FREE_RATE = 0.05


@dataclass
class BacktestConfig:
    strategy: str = "momentum"
    symbols: list[str] = field(default_factory=lambda: ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA"])
    start: str = "2020-01-01"
    end: str = "2024-12-31"
    initial_capital: float = 100_000.0
    max_position_pct: float = 0.10
    max_positions: int = 10
    commission: float = 0.0       # Schwab = $0
    slippage_pct: float = 0.01    # 1 basis point slippage per side
    data_path: str = "data/sp500_stocks.csv"


@dataclass
class Trade:
    symbol: str
    side: str
    date: str
    price: float
    quantity: int
    value: float
    signal: str
    score: float


@dataclass
class BacktestResult:
    config: BacktestConfig
    trades: list[Trade]
    equity_curve: pd.Series
    # Metrics
    total_return_pct: float = 0.0
    cagr: float = 0.0
    sharpe: float = 0.0
    sortino: float = 0.0
    max_drawdown_pct: float = 0.0
    max_drawdown_days: int = 0
    win_rate: float = 0.0
    profit_factor: float = 0.0
    avg_win: float = 0.0
    avg_loss: float = 0.0
    total_trades: int = 0
    winners: int = 0
    losers: int = 0
    avg_hold_days: float = 0.0
    # Per-year breakdown
    annual_returns: dict[int, float] = field(default_factory=dict)

    def print_report(self) -> str:
        lines = [
            f"=== Backtest: {self.config.strategy} ===",
            f"Period: {self.config.start} → {self.config.end}",
            f"Symbols: {', '.join(self.config.symbols[:10])}{'...' if len(self.config.symbols) > 10 else ''}",
            f"Initial capital: ${self.config.initial_capital:,.0f}",
            f"",
            f"Total return: {self.total_return_pct:+.2f}%",
            f"CAGR: {self.cagr:+.2f}%",
            f"Sharpe: {self.sharpe:.2f}",
            f"Sortino: {self.sortino:.2f}",
            f"Max drawdown: {self.max_drawdown_pct:.2f}%",
            f"Max DD duration: {self.max_drawdown_days} days",
            f"",
            f"Trades: {self.total_trades}",
            f"Winners: {self.winners} ({self.win_rate:.1f}%)",
            f"Losers: {self.losers}",
            f"Avg win: ${self.avg_win:,.2f}",
            f"Avg loss: ${self.avg_loss:,.2f}",
            f"Profit factor: {self.profit_factor:.2f}",
            f"Avg hold: {self.avg_hold_days:.1f} days",
            f"",
            f"Annual returns:",
        ]
        for year, ret in sorted(self.annual_returns.items()):
            lines.append(f"  {year}: {ret:+.2f}%")

        report = "\n".join(lines)
        print(report)
        return report


class Backtester:
    """Walk-forward backtester using historical CSV data."""

    def __init__(self, config: BacktestConfig):
        self.config = config
        self._data: dict[str, pd.DataFrame] = {}

    def run(self) -> BacktestResult:
        """Run the backtest and return results."""
        self._load_data()

        if self.config.strategy == "momentum":
            return self._run_signal_strategy(self._momentum_signals)
        elif self.config.strategy == "mean_reversion":
            return self._run_signal_strategy(self._mean_reversion_signals)
        elif self.config.strategy == "trend_following":
            return self._run_signal_strategy(self._trend_signals)
        elif self.config.strategy == "composite":
            return self._run_signal_strategy(self._composite_signals)
        else:
            raise ValueError(f"Unknown strategy: {self.config.strategy}")

    # ── Data loading ─────────────────────────────────────────────────────

    def _load_data(self) -> None:
        """Load OHLCV from the S&P 500 CSV for the configured symbols."""
        path = Path(self.config.data_path)
        if not path.exists():
            raise FileNotFoundError(f"Data file not found: {path}")

        logger.info("Loading data from %s...", path)
        df = pd.read_csv(
            path,
            parse_dates=["date"],
            usecols=["date", "open", "high", "low", "close", "volume", "symbol"],
        )

        start = pd.Timestamp(self.config.start)
        end = pd.Timestamp(self.config.end)
        # Load extra history before start for indicator warmup
        warmup_start = start - pd.Timedelta(days=300)

        for sym in self.config.symbols:
            sym_df = df[df["symbol"] == sym].copy()
            sym_df = sym_df[(sym_df["date"] >= warmup_start) & (sym_df["date"] <= end)]
            sym_df = sym_df.sort_values("date").set_index("date")
            if len(sym_df) > 50:
                self._data[sym] = sym_df
            else:
                logger.warning("Insufficient data for %s (%d bars), skipping", sym, len(sym_df))

        logger.info("Loaded %d symbols with data", len(self._data))

    # ── Signal generators ────────────────────────────────────────────────

    def _momentum_signals(self, close: pd.Series, high: pd.Series, low: pd.Series) -> pd.Series:
        """Generate momentum signals: +1=BUY, -1=SELL, 0=HOLD."""
        signals = pd.Series(0.0, index=close.index)
        if len(close) < 60:
            return signals

        sma20 = close.rolling(20).mean()
        sma50 = close.rolling(50).mean()
        rsi_s = self._rsi_series(close, 14)
        _, _, macd_hist = self._macd_series(close)

        buy = (close > sma20) & (close > sma50) & (rsi_s > 40) & (rsi_s < 70) & (macd_hist > 0)
        sell = (close < sma20) & (rsi_s > 60)

        signals[buy] = 1.0
        signals[sell] = -1.0
        return signals

    def _mean_reversion_signals(self, close: pd.Series, high: pd.Series, low: pd.Series) -> pd.Series:
        signals = pd.Series(0.0, index=close.index)
        if len(close) < 30:
            return signals

        mid = close.rolling(20).mean()
        std = close.rolling(20).std()
        upper = mid + 2 * std
        lower = mid - 2 * std
        rsi_s = self._rsi_series(close, 14)

        buy = (close < lower) & (rsi_s < 30)
        sell = (close > upper) & (rsi_s > 70)

        signals[buy] = 1.0
        signals[sell] = -1.0
        return signals

    def _trend_signals(self, close: pd.Series, high: pd.Series, low: pd.Series) -> pd.Series:
        signals = pd.Series(0.0, index=close.index)
        if len(close) < 220:
            return signals

        ema20 = close.ewm(span=20, adjust=False).mean()
        ema50 = close.ewm(span=50, adjust=False).mean()
        ema200 = close.ewm(span=200, adjust=False).mean()
        adx_s = self._adx_series(high, low, close, 14)

        buy = (ema20 > ema50) & (ema50 > ema200) & (adx_s > 25)
        sell = (ema20 < ema50)

        signals[buy] = 1.0
        signals[sell] = -1.0
        return signals

    def _composite_signals(self, close: pd.Series, high: pd.Series, low: pd.Series) -> pd.Series:
        m = self._momentum_signals(close, high, low)
        mr = self._mean_reversion_signals(close, high, low)
        t = self._trend_signals(close, high, low)
        avg = (m + mr + t) / 3.0

        signals = pd.Series(0.0, index=close.index)
        signals[avg >= 0.5] = 1.0
        signals[avg <= -0.5] = -1.0
        return signals

    # ── Indicator series helpers ─────────────────────────────────────────

    @staticmethod
    def _rsi_series(close: pd.Series, period: int = 14) -> pd.Series:
        delta = close.diff()
        gain = delta.clip(lower=0)
        loss = (-delta).clip(lower=0)
        avg_gain = gain.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
        avg_loss = loss.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
        rs = avg_gain / avg_loss.replace(0, np.nan)
        return 100.0 - (100.0 / (1.0 + rs))

    @staticmethod
    def _macd_series(close: pd.Series, fast: int = 12, slow: int = 26, sig: int = 9):
        ema_fast = close.ewm(span=fast, adjust=False).mean()
        ema_slow = close.ewm(span=slow, adjust=False).mean()
        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=sig, adjust=False).mean()
        histogram = macd_line - signal_line
        return macd_line, signal_line, histogram

    @staticmethod
    def _adx_series(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
        """Vectorized ADX series."""
        prev_close = close.shift(1)
        tr = pd.concat([
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ], axis=1).max(axis=1)

        up = high.diff()
        down = -low.diff()
        plus_dm = pd.Series(np.where((up > down) & (up > 0), up, 0.0), index=close.index)
        minus_dm = pd.Series(np.where((down > up) & (down > 0), down, 0.0), index=close.index)

        atr = tr.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
        smooth_plus = plus_dm.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
        smooth_minus = minus_dm.ewm(alpha=1/period, min_periods=period, adjust=False).mean()

        plus_di = 100 * smooth_plus / atr.replace(0, np.nan)
        minus_di = 100 * smooth_minus / atr.replace(0, np.nan)
        di_sum = plus_di + minus_di
        dx = 100 * (plus_di - minus_di).abs() / di_sum.replace(0, np.nan)
        adx_out = dx.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
        return adx_out

    # ── Simulation engine ────────────────────────────────────────────────

    def _run_signal_strategy(self, signal_fn) -> BacktestResult:
        """Walk-forward simulation of a signal-based long-only strategy."""
        start = pd.Timestamp(self.config.start)
        end = pd.Timestamp(self.config.end)
        capital = self.config.initial_capital
        cash = capital

        # Precompute signals for all symbols
        all_signals: dict[str, pd.Series] = {}
        for sym, df in self._data.items():
            sigs = signal_fn(df["close"], df["high"], df["low"])
            all_signals[sym] = sigs

        # Get all trading dates in range
        all_dates = set()
        for df in self._data.values():
            dates = df.index[(df.index >= start) & (df.index <= end)]
            all_dates.update(dates)
        trading_dates = sorted(all_dates)

        if not trading_dates:
            return self._empty_result()

        # Position tracking
        positions: dict[str, dict] = {}  # sym → {qty, entry_price, entry_date}
        trades: list[Trade] = []
        equity_values: list[float] = []
        equity_dates: list[pd.Timestamp] = []

        for date in trading_dates:
            # Mark to market
            portfolio_value = cash
            for sym, pos in positions.items():
                if date in self._data[sym].index:
                    price = float(self._data[sym].loc[date, "close"])
                    portfolio_value += pos["qty"] * price

            equity_values.append(portfolio_value)
            equity_dates.append(date)

            # Generate signals and execute
            for sym in self._data:
                if date not in all_signals[sym].index:
                    continue
                if date not in self._data[sym].index:
                    continue

                sig = float(all_signals[sym].loc[date])
                price = float(self._data[sym].loc[date, "close"])

                if sig > 0 and sym not in positions:
                    # BUY
                    if len(positions) >= self.config.max_positions:
                        continue
                    pos_value = min(
                        portfolio_value * self.config.max_position_pct,
                        cash * 0.95,
                    )
                    if pos_value < 100:
                        continue
                    fill_price = price * (1 + self.config.slippage_pct / 100)
                    qty = int(pos_value / fill_price)
                    if qty <= 0:
                        continue
                    cost = qty * fill_price + self.config.commission
                    if cost > cash:
                        continue
                    cash -= cost
                    positions[sym] = {"qty": qty, "entry_price": fill_price, "entry_date": date}
                    trades.append(Trade(sym, "BUY", str(date.date()), fill_price, qty, cost, "BUY", sig))

                elif sig < 0 and sym in positions:
                    # SELL
                    pos = positions.pop(sym)
                    fill_price = price * (1 - self.config.slippage_pct / 100)
                    revenue = pos["qty"] * fill_price - self.config.commission
                    cash += revenue
                    trades.append(Trade(sym, "SELL", str(date.date()), fill_price, pos["qty"], revenue, "SELL", sig))

        # Close remaining positions at end
        final_date = trading_dates[-1]
        for sym, pos in list(positions.items()):
            if final_date in self._data[sym].index:
                price = float(self._data[sym].loc[final_date, "close"])
                fill_price = price * (1 - self.config.slippage_pct / 100)
                revenue = pos["qty"] * fill_price
                cash += revenue
                trades.append(Trade(sym, "SELL", str(final_date.date()), fill_price, pos["qty"], revenue, "CLOSE", 0))
        positions.clear()

        # Build equity curve
        equity_curve = pd.Series(equity_values, index=equity_dates)

        # Compute metrics
        return self._compute_metrics(trades, equity_curve)

    def _compute_metrics(self, trades: list[Trade], equity_curve: pd.Series) -> BacktestResult:
        capital = self.config.initial_capital
        final_value = float(equity_curve.iloc[-1]) if len(equity_curve) > 0 else capital

        # Total return
        total_return = (final_value / capital - 1) * 100

        # CAGR
        days = (equity_curve.index[-1] - equity_curve.index[0]).days if len(equity_curve) > 1 else 1
        years = max(days / 365.25, 0.01)
        cagr = ((final_value / capital) ** (1 / years) - 1) * 100

        # Daily returns
        daily_returns = equity_curve.pct_change().dropna()

        # Sharpe
        if len(daily_returns) > 1 and daily_returns.std() > 0:
            excess = daily_returns.mean() - _RISK_FREE_RATE / _TRADING_DAYS_PER_YEAR
            sharpe = excess / daily_returns.std() * np.sqrt(_TRADING_DAYS_PER_YEAR)
        else:
            sharpe = 0.0

        # Sortino
        downside = daily_returns[daily_returns < 0]
        if len(downside) > 1 and downside.std() > 0:
            excess = daily_returns.mean() - _RISK_FREE_RATE / _TRADING_DAYS_PER_YEAR
            sortino = excess / downside.std() * np.sqrt(_TRADING_DAYS_PER_YEAR)
        else:
            sortino = 0.0

        # Max drawdown
        peak = equity_curve.expanding().max()
        drawdown = (equity_curve - peak) / peak * 100
        max_dd = float(drawdown.min())

        # Max drawdown duration
        underwater = drawdown < 0
        if underwater.any():
            groups = (~underwater).cumsum()
            dd_lengths = underwater.groupby(groups).sum()
            max_dd_days = int(dd_lengths.max()) if len(dd_lengths) > 0 else 0
        else:
            max_dd_days = 0

        # Trade P&L pairs
        buy_trades = {(t.symbol, t.date): t for t in trades if t.side == "BUY"}
        sell_trades = [t for t in trades if t.side in ("SELL", "CLOSE")]
        pnls = []
        hold_days_list = []

        for sell in sell_trades:
            # Find matching buy
            buys = [(d, bt) for (s, d), bt in buy_trades.items() if s == sell.symbol]
            if buys:
                _, buy = buys[-1]  # most recent
                pnl = (sell.price - buy.price) * sell.quantity
                pnls.append(pnl)
                try:
                    hold = (pd.Timestamp(sell.date) - pd.Timestamp(buy.date)).days
                    hold_days_list.append(hold)
                except Exception:
                    pass

        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p <= 0]
        gross_win = sum(wins) if wins else 0
        gross_loss = abs(sum(losses)) if losses else 0

        # Annual returns
        annual = {}
        for year in range(equity_curve.index[0].year, equity_curve.index[-1].year + 1):
            year_data = equity_curve[equity_curve.index.year == year]
            if len(year_data) >= 2:
                annual[year] = (float(year_data.iloc[-1]) / float(year_data.iloc[0]) - 1) * 100

        return BacktestResult(
            config=self.config,
            trades=trades,
            equity_curve=equity_curve,
            total_return_pct=round(total_return, 2),
            cagr=round(cagr, 2),
            sharpe=round(sharpe, 2),
            sortino=round(sortino, 2),
            max_drawdown_pct=round(abs(max_dd), 2),
            max_drawdown_days=max_dd_days,
            win_rate=round(len(wins) / len(pnls) * 100, 1) if pnls else 0.0,
            profit_factor=round(gross_win / gross_loss, 2) if gross_loss > 0 else float("inf"),
            avg_win=round(np.mean(wins), 2) if wins else 0.0,
            avg_loss=round(np.mean(losses), 2) if losses else 0.0,
            total_trades=len(pnls),
            winners=len(wins),
            losers=len(losses),
            avg_hold_days=round(np.mean(hold_days_list), 1) if hold_days_list else 0.0,
            annual_returns=annual,
        )

    def _empty_result(self) -> BacktestResult:
        return BacktestResult(
            config=self.config,
            trades=[],
            equity_curve=pd.Series(dtype=float),
        )
