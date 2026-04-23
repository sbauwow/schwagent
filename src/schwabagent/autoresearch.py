"""Auto-research pipeline for Schwab strategies.

Automates the evaluation loop for every strategy in the repo so shipping
a new strategy isn't "write the code, cross your fingers, flip LIVE".
Output is a per-strategy markdown report plus a ranked leaderboard that
gets updated on every run, so you can see Sharpe drift over time.

Pipeline steps per run:
  1. Ensure a fresh historical OHLCV CSV exists. If missing or >24h old,
     pull each symbol from Schwab via get_ohlcv and rewrite the file in
     the schema backtest.py expects (date, open, high, low, close,
     volume, symbol).
  2. For each backtestable strategy (momentum, mean_reversion,
     trend_following, composite), run Backtester then run_validation
     (Monte Carlo + bootstrap Sharpe CI + walk-forward).
  3. Compute SPY buy-and-hold baseline over the same period.
  4. LLM critique: feed metrics to the configured LLMClient and request
     a ≤200-word plain-English verdict.
  5. Write ~/.schwagent/research/<strategy>_<YYYY-MM-DD>.md for every
     strategy (including non-backtestable ones, which get a stub report).
  6. Update ~/.schwagent/research/leaderboard.md — a single table
     ranking all strategies by Sharpe with drift flags when the latest
     Sharpe deteriorates >20% from the previous run.
  7. Optional Telegram digest if AUTORESEARCH_TELEGRAM_DIGEST=true.

Not covered here:
  - etf_rotation, etf_scalp, conviction_hold, brown_momentum, tick_breadth
    do not have backtest engine support. They appear in the leaderboard
    as "unvalidated" with a stub report explaining how to add a backtest
    path later. Adding them is a separate per-strategy task.
"""
from __future__ import annotations

import json
import logging
import re
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from schwabagent.backtest import Backtester, BacktestConfig, BacktestResult
from schwabagent.backtest_validation import format_report as format_validation_report
from schwabagent.backtest_validation import run_validation
from schwabagent.config import Config
from schwabagent.llm import LLMClient
from schwabagent.schwab_client import SchwabClient

logger = logging.getLogger(__name__)


# Strategies that the Backtester class knows how to replay. Keep this in
# sync with backtest.Backtester.run() — if you add a new branch there,
# add the name here.
BACKTESTABLE_STRATEGIES = {
    "momentum",
    "mean_reversion",
    "trend_following",
    "composite",
}


@dataclass
class StrategyReport:
    """Everything autoresearch knows about one strategy after a run."""

    strategy: str
    run_date: str                   # YYYY-MM-DD
    backtestable: bool

    # Core metrics — populated for backtestable strategies
    total_return_pct: float | None = None
    cagr: float | None = None
    sharpe: float | None = None
    sortino: float | None = None
    max_drawdown_pct: float | None = None
    win_rate: float | None = None
    profit_factor: float | None = None
    total_trades: int | None = None

    # Validation results
    monte_carlo_p_sharpe: float | None = None
    bootstrap_ci_lower: float | None = None
    bootstrap_ci_upper: float | None = None
    walk_forward_avg_sharpe: float | None = None

    # Baseline comparison
    baseline_symbol: str = "SPY"
    baseline_return_pct: float | None = None
    alpha_pct: float | None = None      # strategy return − baseline return

    # LLM critique
    llm_verdict: str | None = None

    # Drift tracking
    sharpe_prior: float | None = None
    sharpe_drift_pct: float | None = None    # (current - prior) / |prior| * 100
    drift_flag: bool = False

    # Error bucket — any backtest/validation exceptions caught here
    errors: list[str] = field(default_factory=list)
    skipped_reason: str | None = None

    def to_dict(self) -> dict:
        return asdict(self)


# ── Historical data fetch ────────────────────────────────────────────────────


def _universe_for_fetch(config: Config) -> list[str]:
    """Union of all strategy symbol lists plus the baseline."""
    syms: set[str] = set()
    syms.update(config.momentum_symbols)
    syms.update(config.mean_reversion_symbols)
    syms.update(config.trend_following_symbols)
    syms.update(config.watchlist)
    syms.add("SPY")
    return sorted(syms)


def fetch_backtest_data(
    client: SchwabClient,
    config: Config,
    data_path: Path,
    years: int,
    force: bool = False,
) -> tuple[Path, int]:
    """Pull daily OHLCV for every strategy symbol from Schwab and write CSV.

    Writes a file compatible with backtest.py's `_load_data` loader:
        columns: date, open, high, low, close, volume, symbol
        one row per (symbol, trading day)

    Args:
        client:     Authenticated SchwabClient.
        config:     Live config (used for the symbol universe).
        data_path:  Target CSV path, e.g. data/sp500_stocks.csv
        years:      How many years of history to try for each symbol.
        force:      If True, fetch even when the file is fresh.

    Returns:
        (data_path, symbol_count) — the number of symbols that produced
        at least one bar.
    """
    data_path.parent.mkdir(parents=True, exist_ok=True)

    # Freshness check: skip the fetch if the CSV exists and was written
    # less than 24 hours ago.
    if not force and data_path.exists():
        age = datetime.now(timezone.utc).timestamp() - data_path.stat().st_mtime
        if age < 86400:
            logger.info("autoresearch: data file is %.1fh old, skipping refetch", age / 3600)
            existing = pd.read_csv(data_path, usecols=["symbol"])
            return data_path, int(existing["symbol"].nunique())

    universe = _universe_for_fetch(config)
    logger.info("autoresearch: fetching %d symbols × %d years from Schwab", len(universe), years)

    all_frames: list[pd.DataFrame] = []
    days = years * 365
    for i, sym in enumerate(universe, 1):
        try:
            df = client.get_ohlcv(sym, days=days)
        except Exception as e:
            logger.warning("autoresearch: %s fetch failed: %s", sym, e)
            continue
        if df is None or df.empty:
            logger.warning("autoresearch: %s returned no bars", sym)
            continue

        # Schwab's DataFrame is indexed by a tz-aware timestamp. Normalise
        # to the backtest.py schema: naive date in a 'date' column, plus
        # a 'symbol' column.
        df = df.reset_index().rename(columns={df.index.name or "index": "date"})
        if "date" not in df.columns:
            df["date"] = df.iloc[:, 0]
        df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None).dt.date
        df["symbol"] = sym
        df = df[["date", "open", "high", "low", "close", "volume", "symbol"]]
        all_frames.append(df)

        if i % 5 == 0:
            logger.info("autoresearch: %d/%d fetched", i, len(universe))

    if not all_frames:
        raise RuntimeError("autoresearch: no symbols returned data — cannot build CSV")

    combined = pd.concat(all_frames, ignore_index=True)
    combined = combined.sort_values(["symbol", "date"]).reset_index(drop=True)
    combined.to_csv(data_path, index=False)

    symbol_count = int(combined["symbol"].nunique())
    logger.info(
        "autoresearch: wrote %d rows for %d symbols to %s",
        len(combined), symbol_count, data_path,
    )
    return data_path, symbol_count


# ── Baseline ─────────────────────────────────────────────────────────────────


def compute_baseline_return(
    data_path: Path,
    symbol: str,
    start: str,
    end: str,
) -> float | None:
    """Return the buy-and-hold percentage return for `symbol` over [start, end]."""
    if not data_path.exists():
        return None
    try:
        df = pd.read_csv(data_path, parse_dates=["date"], usecols=["date", "symbol", "close"])
    except Exception as e:
        logger.warning("autoresearch: baseline read failed: %s", e)
        return None
    df = df[df["symbol"] == symbol]
    if df.empty:
        return None
    df = df[(df["date"] >= pd.Timestamp(start)) & (df["date"] <= pd.Timestamp(end))]
    if len(df) < 2:
        return None
    first = float(df.iloc[0]["close"])
    last = float(df.iloc[-1]["close"])
    if first <= 0:
        return None
    return round((last - first) / first * 100, 2)


# ── LLM critique ─────────────────────────────────────────────────────────────


_LLM_SYSTEM = (
    "You are a quantitative research analyst. Your job is to critique "
    "strategy backtests for a retail trader. You are skeptical of "
    "overfit results and you favor walk-forward / out-of-sample "
    "evidence over in-sample metrics. Your answers are plain English, "
    "under 200 words, and never financial advice."
)


_LLM_PROMPT_TMPL = """Review this backtest and answer three questions:

Strategy: {name}
Period: {start} to {end} ({years} years)
Total return: {total_return}%  (vs {baseline_symbol} buy-and-hold: {baseline_return}%)
CAGR: {cagr}%
Sharpe: {sharpe}
Sortino: {sortino}
Max drawdown: {max_dd}%
Win rate: {win_rate}%  Trades: {trades}  Profit factor: {pf}

Validation:
  Monte Carlo p-value (Sharpe vs random): {mc_p}
  Bootstrap 95% Sharpe CI: [{ci_lower}, {ci_upper}]
  Walk-forward avg Sharpe: {wf_sharpe}

Questions:
1. Does this strategy show real edge, or is it likely fit to noise?
2. What's the single biggest concern in these numbers?
3. Would you recommend deploying with real money? Yes/no and one-line reason.

Keep the whole answer under 200 words.
"""


def llm_critique(llm: LLMClient, report: StrategyReport, years: int, start: str, end: str) -> str:
    """Ask the configured LLM for a plain-English verdict.

    Returns an empty string on any failure — caller decides how to surface it.
    """
    def _fmt(v: Any, digits: int = 2) -> str:
        if v is None:
            return "n/a"
        try:
            return f"{float(v):.{digits}f}"
        except (TypeError, ValueError):
            return str(v)

    prompt = _LLM_PROMPT_TMPL.format(
        name=report.strategy,
        start=start,
        end=end,
        years=years,
        total_return=_fmt(report.total_return_pct),
        baseline_symbol=report.baseline_symbol,
        baseline_return=_fmt(report.baseline_return_pct),
        cagr=_fmt(report.cagr),
        sharpe=_fmt(report.sharpe, 3),
        sortino=_fmt(report.sortino, 3),
        max_dd=_fmt(report.max_drawdown_pct),
        win_rate=_fmt(report.win_rate, 1),
        trades=report.total_trades if report.total_trades is not None else "n/a",
        pf=_fmt(report.profit_factor),
        mc_p=_fmt(report.monte_carlo_p_sharpe, 4),
        ci_lower=_fmt(report.bootstrap_ci_lower, 3),
        ci_upper=_fmt(report.bootstrap_ci_upper, 3),
        wf_sharpe=_fmt(report.walk_forward_avg_sharpe, 3),
    )
    try:
        return llm.generate(prompt, system=_LLM_SYSTEM).strip()
    except Exception as e:
        logger.warning("autoresearch: LLM critique failed: %s", e)
        return ""


# ── Pipeline orchestrator ────────────────────────────────────────────────────


class AutoResearchPipeline:
    """Runs the full research cycle across every configured strategy."""

    def __init__(
        self,
        config: Config,
        client: SchwabClient | None = None,
        llm: LLMClient | None = None,
        report_dir: Path | None = None,
    ):
        self.config = config
        self.client = client
        self.llm = llm
        self.report_dir = (
            report_dir
            or Path(config.STATE_DIR).expanduser() / "research"
        )
        self.report_dir.mkdir(parents=True, exist_ok=True)
        self.data_path = Path(
            getattr(config, "AUTORESEARCH_DATA_PATH", "data/sp500_stocks.csv")
        )
        self.years = int(getattr(config, "AUTORESEARCH_PERIOD_YEARS", 3))

    # ── Top-level run ────────────────────────────────────────────────────────

    def run(
        self,
        strategy_names: list[str] | None = None,
        fetch_force: bool = False,
    ) -> list[StrategyReport]:
        """Run the full pipeline and return one StrategyReport per strategy.

        Args:
            strategy_names: Only evaluate these (default: all configured).
            fetch_force:    Re-fetch data even if the CSV is fresh.
        """
        # 1. Data freshness
        if self.client is not None:
            try:
                fetch_backtest_data(
                    self.client, self.config, self.data_path,
                    years=self.years, force=fetch_force,
                )
            except Exception as e:
                logger.error("autoresearch: data fetch failed: %s", e)

        # 2. Which strategies?
        to_run = strategy_names or self.config.strategies
        logger.info("autoresearch: evaluating %d strategies: %s", len(to_run), to_run)

        # Determine the evaluation window.
        end_date = datetime.now(timezone.utc).date()
        start_date = end_date - timedelta(days=self.years * 365)
        start_str = start_date.isoformat()
        end_str = end_date.isoformat()

        reports: list[StrategyReport] = []
        for name in to_run:
            report = self._evaluate_one(name, start_str, end_str)
            self._write_report(report, start_str, end_str)
            reports.append(report)

        self._update_leaderboard(reports)
        return reports

    # ── Per-strategy evaluation ──────────────────────────────────────────────

    def _evaluate_one(self, name: str, start: str, end: str) -> StrategyReport:
        run_date = datetime.now(timezone.utc).date().isoformat()
        backtestable = name in BACKTESTABLE_STRATEGIES

        report = StrategyReport(
            strategy=name,
            run_date=run_date,
            backtestable=backtestable,
        )

        if not backtestable:
            report.skipped_reason = (
                f"Strategy '{name}' does not have a Backtester.run() branch. "
                "Add a signal generator to backtest.py._run_signal_strategy "
                "or implement a bespoke backtest path to validate it."
            )
            return report

        try:
            bt_result = self._run_backtest(name, start, end)
        except Exception as e:
            logger.exception("autoresearch: backtest(%s) failed", name)
            report.errors.append(f"backtest: {e}")
            return report

        report.total_return_pct = round(bt_result.total_return_pct, 2)
        report.cagr = round(bt_result.cagr, 2)
        report.sharpe = round(bt_result.sharpe, 3)
        report.sortino = round(bt_result.sortino, 3)
        report.max_drawdown_pct = round(bt_result.max_drawdown_pct, 2)
        report.win_rate = round(bt_result.win_rate, 1)
        report.profit_factor = round(bt_result.profit_factor, 2)
        report.total_trades = bt_result.total_trades

        # Validation
        try:
            validation = run_validation(
                bt_result.equity_curve,
                trades=bt_result.trades,
                n_simulations=int(getattr(self.config, "AUTORESEARCH_MONTE_CARLO_SIMS", 500)),
                n_bootstrap=int(getattr(self.config, "AUTORESEARCH_BOOTSTRAP_ITERATIONS", 500)),
                n_windows=int(getattr(self.config, "AUTORESEARCH_WALK_FORWARD_WINDOWS", 5)),
            )
            mc = validation.get("monte_carlo", {})
            bs = validation.get("bootstrap", {})
            wf = validation.get("walk_forward", {})
            if "error" not in mc:
                report.monte_carlo_p_sharpe = round(mc.get("p_value_sharpe", 0.0), 4)
            if "error" not in bs:
                report.bootstrap_ci_lower = round(bs.get("ci_lower", 0.0), 3)
                report.bootstrap_ci_upper = round(bs.get("ci_upper", 0.0), 3)
            if "error" not in wf:
                # walk_forward_analysis returns per-window stats — average the sharpes
                windows = wf.get("windows") or []
                sharpes = [w.get("sharpe") for w in windows if w.get("sharpe") is not None]
                if sharpes:
                    report.walk_forward_avg_sharpe = round(sum(sharpes) / len(sharpes), 3)
        except Exception as e:
            logger.exception("autoresearch: validation(%s) failed", name)
            report.errors.append(f"validation: {e}")

        # Baseline
        try:
            baseline = compute_baseline_return(self.data_path, report.baseline_symbol, start, end)
            if baseline is not None:
                report.baseline_return_pct = baseline
                if report.total_return_pct is not None:
                    report.alpha_pct = round(report.total_return_pct - baseline, 2)
        except Exception as e:
            report.errors.append(f"baseline: {e}")

        # LLM critique
        if self.llm is not None and getattr(self.config, "AUTORESEARCH_LLM_ENABLED", True):
            report.llm_verdict = llm_critique(self.llm, report, years=self.years, start=start, end=end)

        return report

    def _run_backtest(self, strategy: str, start: str, end: str) -> BacktestResult:
        """Build a BacktestConfig and replay the strategy via Backtester."""
        symbols = self._backtest_symbols(strategy)
        bt_config = BacktestConfig(
            strategy=strategy,
            symbols=symbols,
            start=start,
            end=end,
            initial_capital=100_000.0,
            data_path=str(self.data_path),
        )
        bt = Backtester(bt_config)
        return bt.run()

    def _backtest_symbols(self, strategy: str) -> list[str]:
        """Return the symbol list to backtest each strategy against."""
        if strategy == "momentum":
            return self.config.momentum_symbols
        if strategy == "mean_reversion":
            return self.config.mean_reversion_symbols
        if strategy == "trend_following":
            return self.config.trend_following_symbols
        if strategy == "composite":
            # Composite blends everything — give it the momentum universe
            # as a sensible default; it overlaps with the other strategies.
            return self.config.momentum_symbols
        return self.config.watchlist

    # ── Report writing ───────────────────────────────────────────────────────

    def _write_report(self, report: StrategyReport, start: str, end: str) -> Path:
        path = self.report_dir / f"{report.strategy}_{report.run_date}.md"
        path.write_text(self._render_report(report, start, end))
        logger.info("autoresearch: wrote %s", path)
        return path

    def _render_report(self, report: StrategyReport, start: str, end: str) -> str:
        lines: list[str] = []
        lines.append(f"# Auto-research report — `{report.strategy}`")
        lines.append("")
        lines.append(f"- **Run date**: {report.run_date}")
        lines.append(f"- **Evaluation window**: {start} → {end} ({self.years} years)")
        lines.append(f"- **Backtestable**: {'yes' if report.backtestable else 'no'}")
        lines.append("")

        if not report.backtestable:
            lines.append("## Status: unvalidated")
            lines.append("")
            lines.append(report.skipped_reason or "No reason given.")
            lines.append("")
            lines.append("This strategy exists in the repo but cannot be replayed")
            lines.append("through the current backtest engine. Signals are generated")
            lines.append("live via the strategy's `scan()` method but are not")
            lines.append("reproducible against historical CSV data.")
            lines.append("")
            lines.append("**To add backtest support**: extend")
            lines.append("`src/schwabagent/backtest.py::Backtester.run` with a new")
            lines.append("branch and a `_<strategy>_signals` helper matching the")
            lines.append("existing ones (momentum, mean_reversion, trend_following,")
            lines.append("composite).")
            return "\n".join(lines) + "\n"

        if report.errors:
            lines.append("## Errors")
            for e in report.errors:
                lines.append(f"- {e}")
            lines.append("")

        lines.append("## Headline metrics")
        lines.append("")
        lines.append(f"| Metric | Value |")
        lines.append(f"|---|---|")
        lines.append(f"| Total return | {_pct(report.total_return_pct)} |")
        lines.append(f"| CAGR | {_pct(report.cagr)} |")
        lines.append(f"| Sharpe | {_num(report.sharpe, 3)} |")
        lines.append(f"| Sortino | {_num(report.sortino, 3)} |")
        lines.append(f"| Max drawdown | {_pct(report.max_drawdown_pct)} |")
        lines.append(f"| Win rate | {_pct(report.win_rate)} |")
        lines.append(f"| Profit factor | {_num(report.profit_factor, 2)} |")
        lines.append(f"| Total trades | {report.total_trades or 'n/a'} |")
        lines.append("")

        lines.append("## Validation")
        lines.append("")
        lines.append(f"| Test | Result |")
        lines.append(f"|---|---|")
        lines.append(
            f"| Monte Carlo p-value (Sharpe) | "
            f"{_num(report.monte_carlo_p_sharpe, 4)} "
            f"{_mc_verdict(report.monte_carlo_p_sharpe)} |"
        )
        ci_range = (
            f"[{_num(report.bootstrap_ci_lower, 3)}, "
            f"{_num(report.bootstrap_ci_upper, 3)}]"
            if report.bootstrap_ci_lower is not None and report.bootstrap_ci_upper is not None
            else "n/a"
        )
        lines.append(f"| Bootstrap 95% Sharpe CI | {ci_range} |")
        lines.append(f"| Walk-forward avg Sharpe | {_num(report.walk_forward_avg_sharpe, 3)} |")
        lines.append("")

        lines.append("## Baseline comparison")
        lines.append("")
        lines.append(
            f"- Strategy return: **{_pct(report.total_return_pct)}**"
        )
        lines.append(
            f"- {report.baseline_symbol} buy-and-hold: {_pct(report.baseline_return_pct)}"
        )
        lines.append(
            f"- Alpha (strategy − baseline): **{_pct(report.alpha_pct)}**"
        )
        lines.append("")

        if report.drift_flag and report.sharpe_prior is not None:
            lines.append("## Drift")
            lines.append("")
            lines.append(
                f"- Sharpe prior run: {_num(report.sharpe_prior, 3)}"
            )
            lines.append(
                f"- Sharpe this run: {_num(report.sharpe, 3)}"
            )
            lines.append(
                f"- Drift: **{_pct(report.sharpe_drift_pct)}** — "
                "flagged as significant deterioration."
            )
            lines.append("")

        if report.llm_verdict:
            lines.append("## LLM critique")
            lines.append("")
            lines.append(report.llm_verdict)
            lines.append("")

        return "\n".join(lines) + "\n"

    # ── Leaderboard ──────────────────────────────────────────────────────────

    def _update_leaderboard(self, reports: list[StrategyReport]) -> Path:
        """Update research/leaderboard.md with drift detection."""
        path = self.report_dir / "leaderboard.md"
        prior = self._load_prior_leaderboard(path)

        # Apply drift detection
        for r in reports:
            if r.sharpe is None:
                continue
            prior_sharpe = prior.get(r.strategy)
            if prior_sharpe is not None and prior_sharpe != 0:
                r.sharpe_prior = prior_sharpe
                r.sharpe_drift_pct = round(
                    (r.sharpe - prior_sharpe) / abs(prior_sharpe) * 100, 1
                )
                if r.sharpe_drift_pct <= -20:
                    r.drift_flag = True

        # Sort: backtestable with Sharpe desc first, then unvalidated
        def sort_key(r: StrategyReport) -> tuple[int, float]:
            if r.sharpe is None:
                return (1, 0.0)
            return (0, -r.sharpe)

        reports_sorted = sorted(reports, key=sort_key)

        lines: list[str] = []
        lines.append("# Strategy leaderboard")
        lines.append("")
        lines.append(
            f"Last updated: {datetime.now(timezone.utc).date().isoformat()}"
        )
        lines.append(
            f"Evaluation window: {self.years} years per strategy"
        )
        lines.append("")
        lines.append("| Rank | Strategy | Sharpe | CAGR | Max DD | Win % | Alpha vs SPY | Drift | Status |")
        lines.append("|---|---|---|---|---|---|---|---|---|")

        rank = 0
        for r in reports_sorted:
            rank += 1
            if r.backtestable and r.sharpe is not None:
                drift = (
                    f"{r.sharpe_drift_pct:+.1f}% ⚠️"
                    if r.drift_flag
                    else (f"{r.sharpe_drift_pct:+.1f}%" if r.sharpe_drift_pct is not None else "—")
                )
                status = "ok" if not r.errors else f"errors ({len(r.errors)})"
                lines.append(
                    f"| {rank} | `{r.strategy}` | "
                    f"{_num(r.sharpe, 2)} | "
                    f"{_pct(r.cagr)} | "
                    f"{_pct(r.max_drawdown_pct)} | "
                    f"{_pct(r.win_rate)} | "
                    f"{_pct(r.alpha_pct)} | "
                    f"{drift} | {status} |"
                )
            else:
                status = "unvalidated" if not r.backtestable else "error"
                lines.append(
                    f"| — | `{r.strategy}` | — | — | — | — | — | — | {status} |"
                )
        lines.append("")

        lines.append("## Reports")
        lines.append("")
        for r in reports_sorted:
            lines.append(f"- [`{r.strategy}`]({r.strategy}_{r.run_date}.md)")

        path.write_text("\n".join(lines) + "\n")
        logger.info("autoresearch: updated %s", path)
        return path

    def _load_prior_leaderboard(self, path: Path) -> dict[str, float]:
        """Parse a previous leaderboard.md and extract {strategy: sharpe}."""
        if not path.exists():
            return {}
        result: dict[str, float] = {}
        try:
            text = path.read_text()
        except OSError:
            return result
        # Match rows like: | N | `strategy` | 1.23 | ...
        pattern = re.compile(r"\|\s*\d+\s*\|\s*`([a-z_]+)`\s*\|\s*([-\d.]+)\s*\|")
        for line in text.splitlines():
            m = pattern.match(line)
            if m:
                name = m.group(1)
                try:
                    result[name] = float(m.group(2))
                except ValueError:
                    continue
        return result


# ── Formatting helpers ───────────────────────────────────────────────────────


def _num(v: Any, digits: int = 2) -> str:
    if v is None:
        return "n/a"
    try:
        return f"{float(v):.{digits}f}"
    except (TypeError, ValueError):
        return str(v)


def _pct(v: Any) -> str:
    if v is None:
        return "n/a"
    try:
        return f"{float(v):+.2f}%"
    except (TypeError, ValueError):
        return str(v)


def _mc_verdict(p: float | None) -> str:
    if p is None:
        return ""
    if p < 0.01:
        return "(highly significant)"
    if p < 0.05:
        return "(significant)"
    if p < 0.10:
        return "(marginal)"
    return "(not significant)"
