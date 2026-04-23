"""Ex-dividend calendar scraper — Nasdaq public JSON API.

Source URL: api.nasdaq.com/api/calendar/dividends?date=YYYY-MM-DD

Briefing's Schwab portal has no dividend calendar (earnings/splits/econ only),
so we use Nasdaq's public JSON feed instead. One HTTP call per calendar date;
we loop forward ~5 weeks and cache the full window daily to STATE_DIR.

Row shape from the API:
    {"companyName": "…", "symbol": "…",
     "dividend_Ex_Date": "M/D/YYYY", "payment_Date": "M/D/YYYY",
     "record_Date": "M/D/YYYY", "announcement_Date": "M/D/YYYY",
     "dividend_Rate": 0.26, "indicated_Annual_Dividend": 1.04}

Usage:
    from schwabagent.scrapers.dividend_calendar import fetch_dividend_calendar
    rows = fetch_dividend_calendar(config)

CLI:
    python -m schwabagent.scrapers.dividend_calendar [--refresh] [--all]
                                                     [--days N] [--symbol X]
"""
from __future__ import annotations

import json
import logging
import time
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

from schwabagent.config import Config
from schwabagent.scrapers.earnings_calendar import agent_universe

logger = logging.getLogger(__name__)

ET = ZoneInfo("America/New_York")
API_URL = "https://api.nasdaq.com/api/calendar/dividends"
USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
CACHE_NAME = "dividend_calendar.json"
DEFAULT_WINDOW_DAYS = 35
REQUEST_SLEEP_SEC = 0.25


@dataclass
class DividendRow:
    """One scheduled ex-dividend event."""

    ex_date: str               # ISO YYYY-MM-DD
    symbol: str
    company: str
    amount: float | None       # per-payment dividend
    annual_dividend: float | None
    payment_date: str | None   # ISO or None
    record_date: str | None
    announce_date: str | None

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "DividendRow":
        return cls(**d)


# ── Cache ────────────────────────────────────────────────────────────────────


def _cache_path(config: Config) -> Path:
    return Path(config.STATE_DIR).expanduser() / CACHE_NAME


def _cache_fresh(path: Path) -> bool:
    """True if the cache file was written on today's ET trading date."""
    if not path.exists():
        return False
    mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=ET)
    return mtime.date() == datetime.now(ET).date()


def _save_cache(path: Path, rows: list[DividendRow]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "fetched_at": datetime.now(ET).isoformat(),
        "rows": [r.to_dict() for r in rows],
    }
    path.write_text(json.dumps(payload, indent=2))


def _load_cache(path: Path) -> list[DividendRow]:
    payload = json.loads(path.read_text())
    return [DividendRow.from_dict(r) for r in payload.get("rows", [])]


# ── Fetch + parse ────────────────────────────────────────────────────────────


def _parse_api_date(text: str | None) -> str | None:
    """Convert 'M/D/YYYY' from the Nasdaq API into ISO YYYY-MM-DD."""
    if not text:
        return None
    text = text.strip()
    if not text or text in {"--", "-", "N/A"}:
        return None
    try:
        return datetime.strptime(text, "%m/%d/%Y").date().isoformat()
    except ValueError:
        return None


def _num(value) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).strip().replace("$", "").replace(",", "")
    if not s or s in {"--", "-", "N/A"}:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _fetch_day(day: date, timeout: float = 15.0) -> list[dict]:
    """Return the raw row list for a single calendar date."""
    resp = requests.get(
        API_URL,
        params={"date": day.isoformat()},
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
        },
        timeout=timeout,
    )
    resp.raise_for_status()
    payload = resp.json()
    data = payload.get("data") or {}
    calendar = data.get("calendar") or {}
    return calendar.get("rows") or []


def _row_from_api(raw: dict) -> DividendRow | None:
    symbol = (raw.get("symbol") or "").strip().upper()
    company = (raw.get("companyName") or "").strip()
    ex_iso = _parse_api_date(raw.get("dividend_Ex_Date"))
    if not symbol or not ex_iso:
        return None
    return DividendRow(
        ex_date=ex_iso,
        symbol=symbol,
        company=company,
        amount=_num(raw.get("dividend_Rate")),
        annual_dividend=_num(raw.get("indicated_Annual_Dividend")),
        payment_date=_parse_api_date(raw.get("payment_Date")),
        record_date=_parse_api_date(raw.get("record_Date")),
        announce_date=_parse_api_date(raw.get("announcement_Date")),
    )


def fetch_dividend_calendar(
    config: Config,
    force_refresh: bool = False,
    days: int = DEFAULT_WINDOW_DAYS,
) -> list[DividendRow]:
    """Return parsed dividend rows over the next `days` calendar days,
    cached daily to STATE_DIR."""
    path = _cache_path(config)
    if not force_refresh and _cache_fresh(path):
        logger.info("dividends: loading fresh cache from %s", path)
        try:
            return _load_cache(path)
        except Exception as e:
            logger.warning("dividends: cache load failed (%s) — refetching", e)

    start = datetime.now(ET).date()
    end = start + timedelta(days=days)
    logger.info("dividends: fetching %s..%s from %s", start, end, API_URL)

    rows: list[DividendRow] = []
    seen: set[tuple[str, str]] = set()
    current = start
    while current <= end:
        try:
            raw_rows = _fetch_day(current)
        except requests.RequestException as e:
            logger.warning("dividends: fetch %s failed: %s", current, e)
            current += timedelta(days=1)
            time.sleep(REQUEST_SLEEP_SEC)
            continue
        for raw in raw_rows:
            row = _row_from_api(raw)
            if row is None:
                continue
            key = (row.symbol, row.ex_date)
            if key in seen:
                continue
            seen.add(key)
            rows.append(row)
        current += timedelta(days=1)
        time.sleep(REQUEST_SLEEP_SEC)

    logger.info("dividends: parsed %d rows", len(rows))
    try:
        _save_cache(path, rows)
    except OSError as e:
        logger.warning("dividends: cache write failed: %s", e)
    return rows


# ── Filtering helpers ────────────────────────────────────────────────────────


def filter_rows(
    rows: list[DividendRow],
    symbols: set[str] | None = None,
    start: date | None = None,
    end: date | None = None,
) -> list[DividendRow]:
    """Narrow the row list by ticker set and/or ex-date window."""
    out = []
    for r in rows:
        if symbols and r.symbol not in symbols:
            continue
        d = date.fromisoformat(r.ex_date)
        if start and d < start:
            continue
        if end and d > end:
            continue
        out.append(r)
    return out


# ── Render ───────────────────────────────────────────────────────────────────


def _fmt_amount(v: float | None) -> str:
    if v is None:
        return "—"
    return f"${v:.4f}" if v < 0.01 else f"${v:.3f}"


def _fmt_annual(v: float | None) -> str:
    if v is None or v == 0:
        return "—"
    return f"${v:.2f}"


def render_table(rows: list[DividendRow]):
    """Return a Rich Table for terminal output, sorted by ex-date then symbol."""
    from rich.table import Table

    t = Table(title="Ex-Dividend Calendar", show_lines=False, title_style="bold cyan")
    t.add_column("Ex Date", style="bold")
    t.add_column("Symbol", style="bold")
    t.add_column("Company")
    t.add_column("Amount", justify="right")
    t.add_column("Annual", justify="right")
    t.add_column("Pay Date")
    t.add_column("Record")

    rows = sorted(rows, key=lambda r: (r.ex_date, r.symbol))
    for r in rows:
        t.add_row(
            r.ex_date,
            r.symbol,
            (r.company[:32] + "…") if len(r.company) > 33 else r.company,
            _fmt_amount(r.amount),
            _fmt_annual(r.annual_dividend),
            r.payment_date or "—",
            r.record_date or "—",
        )
    return t


# ── CLI entry point ──────────────────────────────────────────────────────────


def main() -> None:
    import argparse
    import logging as _logging

    from rich.console import Console

    parser = argparse.ArgumentParser(
        prog="dividends",
        description="Scrape the Nasdaq ex-dividend calendar (next ~5 weeks).",
    )
    parser.add_argument("--refresh", action="store_true", help="Force cache refresh")
    parser.add_argument(
        "--all", action="store_true",
        help="Show every ticker (default: filter to watchlist∪ah_sniper∪momentum)",
    )
    parser.add_argument(
        "--days", type=int, default=0,
        help="Limit displayed rows to next N days (default: all ~35 days)",
    )
    parser.add_argument(
        "--window", type=int, default=DEFAULT_WINDOW_DAYS,
        help=f"Fetch window in days (default: {DEFAULT_WINDOW_DAYS})",
    )
    parser.add_argument(
        "--symbol", type=str, default="",
        help="Show only this ticker (overrides universe filter)",
    )
    args = parser.parse_args()

    config = Config()
    _logging.basicConfig(
        level=getattr(_logging, config.log_level, _logging.INFO),
        format="%(asctime)s %(name)-32s %(levelname)-8s %(message)s",
        datefmt="%H:%M:%S",
    )

    console = Console()
    console.print()
    console.rule("[cyan]Ex-Dividend Calendar[/cyan]")
    console.print()

    try:
        rows = fetch_dividend_calendar(
            config, force_refresh=args.refresh, days=args.window,
        )
    except requests.RequestException as e:
        console.print(f"  [red]✗[/red] Fetch failed: {e}")
        return

    if not rows:
        console.print("  [red]✗[/red] No rows parsed — Nasdaq API layout may have changed")
        return

    filter_syms: set[str] | None
    if args.symbol:
        filter_syms = {args.symbol.upper()}
    elif args.all:
        filter_syms = None
    else:
        filter_syms = agent_universe(config)

    start = datetime.now(ET).date()
    end = start + timedelta(days=args.days) if args.days > 0 else None

    filtered = filter_rows(rows, symbols=filter_syms, start=start, end=end)
    total = len(rows)
    shown = len(filtered)

    scope = "all" if filter_syms is None else (
        f"symbol={args.symbol.upper()}" if args.symbol else f"{len(filter_syms)}-symbol universe"
    )
    window = f"next {args.days}d" if args.days > 0 else f"all ~{args.window} days"
    console.print(f"  Source: {API_URL}")
    console.print(f"  Scope: {scope}   Window: {window}")
    console.print(f"  Cache: {_cache_path(config)}")
    console.print(f"  Rows: {shown}/{total}")
    console.print()

    if not filtered:
        console.print("  [dim]No rows match the current filter.[/dim]")
        console.print()
        return

    console.print(render_table(filtered))
    console.print()


if __name__ == "__main__":
    main()
