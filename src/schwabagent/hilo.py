"""High/low scanner — finds stocks hitting fresh N-day highs and lows.

Default windows: 5d, 20d, 60d, 252d (52w). Universe: union of the
watchlist, ah_sniper symbols, and momentum symbols. Daily OHLCV is pulled
via Schwab's pricehistory endpoint and cached to
~/.schwagent/hilo_cache.csv so repeat scans inside the same ET trading
day don't burn API quota. Pass --refresh to force a live refetch; useful
intraday when today's bar is still evolving.

A "new high" means today's intraday high exceeds the max high of the
prior N bars (excluding today). Same for lows with min low. We use today's
full OHLC bar, not just the close, so a symbol that printed a fresh
extreme and then reversed into the close still shows up.

Run as:
    python -m schwabagent.hilo [--refresh]
or via run.sh:
    ./run.sh hilo [--refresh]
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from schwabagent.config import Config
from schwabagent.schwab_client import SchwabClient

logger = logging.getLogger(__name__)

ET = ZoneInfo("America/New_York")
DEFAULT_WINDOWS: tuple[int, ...] = (5, 20, 60, 252)
WINDOW_LABELS: dict[int, str] = {5: "5d", 20: "20d", 60: "60d", 252: "52w"}


@dataclass
class HiloRow:
    """One row of scan output per symbol that hit at least one extreme."""

    symbol: str
    last_close: float
    pct_change: float        # today vs prior close, percent
    today_high: float
    today_low: float
    hits_high: set[int] = field(default_factory=set)
    hits_low: set[int] = field(default_factory=set)


# ── Cache ────────────────────────────────────────────────────────────────────


def _cache_path(config: Config) -> Path:
    return Path(config.STATE_DIR).expanduser() / "hilo_cache.csv"


def _universe(config: Config) -> list[str]:
    """Union of every symbol the user has configured as "interesting"."""
    syms: set[str] = set()
    syms.update(config.watchlist)
    syms.update(config.ah_sniper_symbols)
    syms.update(config.momentum_symbols)
    return sorted(syms)


def _cache_fresh(path: Path) -> bool:
    """True if the cache file was written on today's ET trading date."""
    if not path.exists():
        return False
    mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=ET)
    return mtime.date() == datetime.now(ET).date()


def load_or_fetch(
    config: Config,
    client: SchwabClient,
    force_refresh: bool = False,
) -> dict[str, pd.DataFrame]:
    """Return {symbol: daily-ohlcv DataFrame} — from cache when fresh, else Schwab.

    The cache stores the raw bars so re-computing with different windows
    later is free.
    """
    path = _cache_path(config)
    if not force_refresh and _cache_fresh(path):
        logger.info("hilo: loading fresh cache from %s", path)
        df = pd.read_csv(path, parse_dates=["date"])
        return {
            sym: g.drop_duplicates(subset=["date"], keep="last")
                  .sort_values("date")
                  .reset_index(drop=True)
            for sym, g in df.groupby("symbol")
        }

    universe = _universe(config)
    logger.info("hilo: fetching %d symbols (≈400 daily bars each) from Schwab", len(universe))

    frames: list[pd.DataFrame] = []
    result: dict[str, pd.DataFrame] = {}
    for sym in universe:
        try:
            df = client.get_ohlcv(sym, days=400)
        except Exception as e:
            logger.warning("hilo: %s fetch failed: %s", sym, e)
            continue
        if df is None or df.empty:
            logger.warning("hilo: %s returned no bars", sym)
            continue

        # Normalise to the same schema as autoresearch.
        df = df.reset_index().rename(columns={df.index.name or "index": "date"})
        if "date" not in df.columns:
            df["date"] = df.iloc[:, 0]
        df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None).dt.normalize()
        df["symbol"] = sym
        df = df[["date", "open", "high", "low", "close", "volume", "symbol"]]
        # Schwab sometimes returns two bars for the latest date (regular
        # close + post-session print). Collapse to a single row per date,
        # keeping the most recent one — otherwise the "today" bar and the
        # "prior" lookback share the same date and nothing ever prints a
        # fresh extreme.
        df = df.drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)
        result[sym] = df
        frames.append(df)

    if frames:
        path.parent.mkdir(parents=True, exist_ok=True)
        combined = pd.concat(frames, ignore_index=True).sort_values(["symbol", "date"])
        combined.to_csv(path, index=False)
        logger.info(
            "hilo: cached %d rows for %d symbols to %s",
            len(combined), len(result), path,
        )
    return result


# ── Scan ─────────────────────────────────────────────────────────────────────


def scan_hilo(
    data: dict[str, pd.DataFrame],
    windows: tuple[int, ...] = DEFAULT_WINDOWS,
) -> list[HiloRow]:
    """For each symbol, flag any window where today's bar set a fresh extreme.

    "Today" = the last row in the dataframe. "Fresh" = strictly greater
    than the max high (or strictly less than the min low) of the prior N
    bars. Ties don't count as new — required by convention.
    """
    rows: list[HiloRow] = []
    for sym, df in data.items():
        if df is None or len(df) < 2:
            continue
        today = df.iloc[-1]
        prior = df.iloc[:-1]
        try:
            last_close = float(today["close"])
            today_high = float(today["high"])
            today_low = float(today["low"])
            prior_close = float(prior.iloc[-1]["close"])
        except (KeyError, IndexError, ValueError):
            continue

        pct = (last_close - prior_close) / prior_close * 100 if prior_close > 0 else 0.0
        row = HiloRow(
            symbol=sym,
            last_close=last_close,
            pct_change=pct,
            today_high=today_high,
            today_low=today_low,
        )

        for w in windows:
            if len(prior) < w:
                continue
            lookback = prior.iloc[-w:]
            prior_max_high = float(lookback["high"].max())
            prior_min_low = float(lookback["low"].min())
            if today_high > prior_max_high:
                row.hits_high.add(w)
            if today_low < prior_min_low:
                row.hits_low.add(w)

        if row.hits_high or row.hits_low:
            rows.append(row)
    return rows


# ── Render ───────────────────────────────────────────────────────────────────


def _label(w: int) -> str:
    return WINDOW_LABELS.get(w, f"{w}d")


def _build_table(
    title: str,
    data: list[HiloRow],
    hit_attr: str,
    marker_color: str,
    windows: tuple[int, ...],
):
    from rich.table import Table

    t = Table(title=title, show_lines=False, title_style=f"bold {marker_color}")
    t.add_column("Symbol", style="bold")
    t.add_column("Close", justify="right")
    t.add_column("Δ vs prior", justify="right")
    for w in windows:
        t.add_column(_label(w), justify="center")

    for r in data:
        pct_color = "green" if r.pct_change >= 0 else "red"
        cells: list[str] = [
            r.symbol,
            f"${r.last_close:,.2f}",
            f"[{pct_color}]{r.pct_change:+.2f}%[/{pct_color}]",
        ]
        hits = getattr(r, hit_attr)
        for w in windows:
            cells.append(f"[{marker_color}]●[/{marker_color}]" if w in hits else "[dim]—[/dim]")
        t.add_row(*cells)
    return t


def render_tables(
    rows: list[HiloRow],
    windows: tuple[int, ...] = DEFAULT_WINDOWS,
):
    """Return (highs_table, lows_table) sorted by deepest window hit."""
    highs = sorted(
        (r for r in rows if r.hits_high),
        key=lambda r: (-max(r.hits_high), -r.pct_change),
    )
    lows = sorted(
        (r for r in rows if r.hits_low),
        key=lambda r: (-max(r.hits_low), r.pct_change),
    )
    return (
        _build_table("New highs", highs, "hits_high", "green", windows),
        _build_table("New lows", lows, "hits_low", "red", windows),
    )


# ── CLI entry point ──────────────────────────────────────────────────────────


def main() -> None:
    import argparse
    import logging as _logging

    from rich.console import Console

    parser = argparse.ArgumentParser(
        prog="hilo",
        description="Scan the configured universe for stocks hitting new N-day highs and lows.",
    )
    parser.add_argument("--refresh", action="store_true", help="Force cache refresh")
    parser.add_argument(
        "--windows", type=str, default="",
        help=f"Comma-separated lookback days (default: {','.join(str(w) for w in DEFAULT_WINDOWS)})",
    )
    args = parser.parse_args()

    config = Config()
    _logging.basicConfig(
        level=getattr(_logging, config.log_level, _logging.INFO),
        format="%(asctime)s %(name)-24s %(levelname)-8s %(message)s",
        datefmt="%H:%M:%S",
    )

    console = Console()
    console.print()
    console.rule("[cyan]High/Low Scanner[/cyan]")
    console.print()

    windows: tuple[int, ...] = DEFAULT_WINDOWS
    if args.windows.strip():
        try:
            windows = tuple(int(w.strip()) for w in args.windows.split(",") if w.strip())
        except ValueError:
            console.print(f"  [red]✗[/red] Bad --windows: {args.windows}")
            return

    client = SchwabClient(config)
    if not client.authenticate():
        console.print("  [red]✗[/red] Schwab API not reachable — run `./run.sh enroll` first")
        return
    console.print("  [green]✓[/green] Schwab API connected")

    data = load_or_fetch(config, client, force_refresh=args.refresh)
    if not data:
        console.print("  [red]✗[/red] No data loaded")
        return

    rows = scan_hilo(data, windows=windows)
    universe_symbols = sorted(data.keys())
    window_label = ", ".join(_label(w) for w in windows)
    console.print(
        f"  Universe: {len(universe_symbols)} symbols   Windows: {window_label}"
    )
    console.print(f"  Cache: {_cache_path(config)}")
    console.print()

    if not rows:
        console.print("  [dim]No new highs or lows across the universe.[/dim]")
        console.print()
        return

    high_tbl, low_tbl = render_tables(rows, windows=windows)
    if any(r.hits_high for r in rows):
        console.print(high_tbl)
        console.print()
    if any(r.hits_low for r in rows):
        console.print(low_tbl)
        console.print()

    # One-line summary
    n_high = sum(1 for r in rows if r.hits_high)
    n_low = sum(1 for r in rows if r.hits_low)
    console.print(
        f"  [green]{n_high}[/green] new-high symbols, "
        f"[red]{n_low}[/red] new-low symbols, "
        f"{len(universe_symbols) - len(rows)} quiet."
    )
    console.print()


if __name__ == "__main__":
    main()
