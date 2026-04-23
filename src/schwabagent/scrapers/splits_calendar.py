"""Splits calendar scraper — Briefing.com (hosted under Schwab).

Source URL: hosting.briefing.com/cschwab/Calendars/SplitsCalendar.htm

Returns upcoming and recently-effective US stock splits with:
  payable date, ex-date, company, symbol, split ratio (and direction),
  announced date, and whether the name is optionable.

HTML structure (no formal schema, parsed by CSS class):
  <td class="sectionTitle">April</td>                — month header (no year)
  <tr> … rH cells …                                   — column header row
  <tr> … rD / rL / rDa cells …                        — one data row,
    7 cells: [company, symbol, ratio, payable,
              ex-date, announced, optionable]

The page prints dates without a year. We resolve years against the page's
"Updated: DD-MMM-YY" header the same way `earnings_calendar.py` does.

Usage:
    from schwabagent.scrapers.splits_calendar import fetch_splits_calendar
    rows = fetch_splits_calendar(config)

CLI:
    python -m schwabagent.scrapers.splits_calendar [--refresh] [--all]
                                                   [--days N] [--symbol X]
"""
from __future__ import annotations

import json
import logging
import re
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup

from schwabagent.config import Config
from schwabagent.scrapers.earnings_calendar import agent_universe

logger = logging.getLogger(__name__)

ET = ZoneInfo("America/New_York")
URL = "https://hosting.briefing.com/cschwab/Calendars/SplitsCalendar.htm"
USER_AGENT = "Mozilla/5.0 (schwagent splits scraper)"
CACHE_NAME = "splits_calendar.json"

_MONTH_NAMES = {
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
}


@dataclass
class SplitRow:
    """One upcoming or recently-effective stock split."""

    ex_date: str               # ISO YYYY-MM-DD
    payable_date: str | None   # ISO or None
    symbol: str
    company: str
    ratio: str                 # raw "N-M" as printed ("4-1", "1-10")
    ratio_num: int | None      # numerator of N-for-M
    ratio_den: int | None      # denominator
    is_reverse: bool           # True when ratio_num < ratio_den
    announced: str | None      # ISO or None
    optionable: bool

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "SplitRow":
        return cls(**d)


# ── Cache ────────────────────────────────────────────────────────────────────


def _cache_path(config: Config) -> Path:
    return Path(config.STATE_DIR).expanduser() / CACHE_NAME


def _cache_fresh(path: Path) -> bool:
    if not path.exists():
        return False
    mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=ET)
    return mtime.date() == datetime.now(ET).date()


def _save_cache(path: Path, rows: list[SplitRow]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "fetched_at": datetime.now(ET).isoformat(),
        "rows": [r.to_dict() for r in rows],
    }
    path.write_text(json.dumps(payload, indent=2))


def _load_cache(path: Path) -> list[SplitRow]:
    payload = json.loads(path.read_text())
    return [SplitRow.from_dict(r) for r in payload.get("rows", [])]


# ── Fetch + parse ────────────────────────────────────────────────────────────


def _fetch_html(timeout: float = 20.0) -> str:
    resp = requests.get(URL, headers={"User-Agent": USER_AGENT}, timeout=timeout)
    resp.raise_for_status()
    return resp.text


_UPDATED_RE = re.compile(r"Updated:\s*(\d{1,2})-([A-Za-z]{3})-(\d{2,4})")
_SHORT_DATE_RE = re.compile(r"^([A-Za-z]{3,9})\s+(\d{1,2})$")
_RATIO_RE = re.compile(r"^(\d+)\s*-\s*(\d+)$")


def _parse_reference_date(html: str) -> date:
    """Extract 'Updated: DD-MMM-YY' from the page header (fallback: today ET)."""
    m = _UPDATED_RE.search(html)
    if not m:
        return datetime.now(ET).date()
    day, mon, yr = m.group(1), m.group(2), m.group(3)
    year = int(yr)
    if year < 100:
        year += 2000
    try:
        return datetime.strptime(f"{day} {mon} {year}", "%d %b %Y").date()
    except ValueError:
        return datetime.now(ET).date()


def _naive_parse(text: str, year: int) -> date | None:
    text = text.strip()
    if not text:
        return None
    m = _SHORT_DATE_RE.match(text)
    if not m:
        return None
    mon_abbr, day = m.group(1)[:3], m.group(2)
    try:
        return datetime.strptime(f"{mon_abbr} {day} {year}", "%b %d %Y").date()
    except ValueError:
        return None


def _resolve_future_date(text: str, ref: date) -> date | None:
    """Ex-date / payable-date resolver: forward-biased.

    If the naive parse lands more than 14 days in the past, roll year + 1.
    14-day grace catches very recent ex-dates that slipped into history.
    """
    parsed = _naive_parse(text, ref.year)
    if parsed is None:
        return None
    if (parsed - ref).days < -14:
        parsed = parsed.replace(year=ref.year + 1)
    return parsed


def _resolve_past_date(text: str, ref: date) -> date | None:
    """Announced-date resolver: backward-biased.

    Splits are typically announced weeks-to-months before their ex-date, so
    the announced cell is always in the past relative to the page fetch.
    If the naive parse lands more than 14 days in the future, roll year - 1.
    """
    parsed = _naive_parse(text, ref.year)
    if parsed is None:
        return None
    if (parsed - ref).days > 14:
        parsed = parsed.replace(year=ref.year - 1)
    return parsed


def _cell_text(td) -> str:
    return re.sub(r"\s+", " ", td.get_text(" ", strip=True)).strip()


def _parse_ratio(text: str) -> tuple[str, int | None, int | None, bool]:
    """Parse '4-1', '1-10', '5-for-1' into (raw, num, den, is_reverse)."""
    raw = text.strip()
    if not raw:
        return "", None, None, False
    # Normalise 'A-for-B' → 'A-B'
    norm = raw.replace(" for ", "-").replace("-for-", "-").replace(" ", "")
    m = _RATIO_RE.match(norm)
    if not m:
        return raw, None, None, False
    num = int(m.group(1))
    den = int(m.group(2))
    return raw, num, den, num < den


def parse_splits(html: str) -> list[SplitRow]:
    """Walk the Briefing HTML and yield one SplitRow per listed split."""
    soup = BeautifulSoup(html, "html.parser")
    ref = _parse_reference_date(html)

    rows: list[SplitRow] = []
    current_month: str | None = None

    content = soup.find("div", id="Content") or soup
    for tr in content.find_all("tr"):
        cells = tr.find_all("td", recursive=False)
        if not cells:
            continue

        # Month header row
        section_td = tr.find("td", class_="sectionTitle")
        if section_td is not None:
            label = _cell_text(section_td)
            if label in _MONTH_NAMES:
                current_month = label
            continue

        # Data row — rD / rL / rDa
        first = cells[0]
        row_class = first.get("class") or []
        if not any(c in ("rD", "rL", "rDa") for c in row_class):
            continue
        if len(cells) < 7:
            continue

        company = _cell_text(cells[0])
        symbol = _cell_text(cells[1]).upper()
        ratio_raw = _cell_text(cells[2])
        payable_text = _cell_text(cells[3])
        ex_text = _cell_text(cells[4])
        announced_text = _cell_text(cells[5])
        optionable_text = _cell_text(cells[6]).lower()

        if not symbol or not company:
            continue

        ex_date = _resolve_future_date(ex_text, ref)
        if ex_date is None:
            continue

        payable_date = _resolve_future_date(payable_text, ref)
        announced_date = _resolve_past_date(announced_text, ref)
        ratio, num, den, is_reverse = _parse_ratio(ratio_raw)

        rows.append(SplitRow(
            ex_date=ex_date.isoformat(),
            payable_date=payable_date.isoformat() if payable_date else None,
            symbol=symbol,
            company=company,
            ratio=ratio,
            ratio_num=num,
            ratio_den=den,
            is_reverse=is_reverse,
            announced=announced_date.isoformat() if announced_date else None,
            optionable=optionable_text.startswith("y"),
        ))

    return rows


def fetch_splits_calendar(
    config: Config,
    force_refresh: bool = False,
) -> list[SplitRow]:
    """Return parsed split rows, cached daily to STATE_DIR."""
    path = _cache_path(config)
    if not force_refresh and _cache_fresh(path):
        logger.info("splits: loading fresh cache from %s", path)
        try:
            return _load_cache(path)
        except Exception as e:
            logger.warning("splits: cache load failed (%s) — refetching", e)

    logger.info("splits: fetching %s", URL)
    html = _fetch_html()
    rows = parse_splits(html)
    logger.info("splits: parsed %d rows", len(rows))
    try:
        _save_cache(path, rows)
    except OSError as e:
        logger.warning("splits: cache write failed: %s", e)
    return rows


# ── Filtering helpers ────────────────────────────────────────────────────────


def filter_rows(
    rows: list[SplitRow],
    symbols: set[str] | None = None,
    start: date | None = None,
    end: date | None = None,
    reverse_only: bool = False,
) -> list[SplitRow]:
    """Narrow the row list by ticker set, ex-date window, and/or direction."""
    out = []
    for r in rows:
        if symbols and r.symbol not in symbols:
            continue
        d = date.fromisoformat(r.ex_date)
        if start and d < start:
            continue
        if end and d > end:
            continue
        if reverse_only and not r.is_reverse:
            continue
        out.append(r)
    return out


# ── Render ───────────────────────────────────────────────────────────────────


def render_table(rows: list[SplitRow]):
    from rich.table import Table

    t = Table(title="Splits Calendar", show_lines=False, title_style="bold cyan")
    t.add_column("Ex Date", style="bold")
    t.add_column("Symbol", style="bold")
    t.add_column("Company")
    t.add_column("Ratio")
    t.add_column("Dir")
    t.add_column("Payable")
    t.add_column("Announced")
    t.add_column("Opt", justify="center")

    rows = sorted(rows, key=lambda r: (r.ex_date, r.symbol))
    for r in rows:
        direction = "[red]reverse[/red]" if r.is_reverse else "[green]forward[/green]"
        t.add_row(
            r.ex_date,
            r.symbol,
            (r.company[:30] + "…") if len(r.company) > 31 else r.company,
            r.ratio,
            direction,
            r.payable_date or "—",
            r.announced or "—",
            "✓" if r.optionable else "·",
        )
    return t


# ── CLI entry point ──────────────────────────────────────────────────────────


def main() -> None:
    import argparse
    import logging as _logging

    from rich.console import Console

    parser = argparse.ArgumentParser(
        prog="splits",
        description="Scrape the Briefing.com splits calendar.",
    )
    parser.add_argument("--refresh", action="store_true", help="Force cache refresh")
    parser.add_argument(
        "--all", action="store_true",
        help="Show every ticker (default: filter to watchlist∪ah_sniper∪momentum)",
    )
    parser.add_argument(
        "--days", type=int, default=0,
        help="Limit to next N days from today (default: all)",
    )
    parser.add_argument(
        "--symbol", type=str, default="",
        help="Show only this ticker (overrides universe filter)",
    )
    parser.add_argument(
        "--reverse", action="store_true",
        help="Show reverse splits only (distress signal)",
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
    console.rule("[cyan]Splits Calendar[/cyan]")
    console.print()

    try:
        rows = fetch_splits_calendar(config, force_refresh=args.refresh)
    except requests.RequestException as e:
        console.print(f"  [red]✗[/red] Fetch failed: {e}")
        return

    if not rows:
        console.print("  [red]✗[/red] No rows parsed — Briefing HTML layout may have changed")
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

    filtered = filter_rows(
        rows, symbols=filter_syms, start=start, end=end, reverse_only=args.reverse,
    )
    total = len(rows)
    shown = len(filtered)

    scope = "all" if filter_syms is None else (
        f"symbol={args.symbol.upper()}" if args.symbol else f"{len(filter_syms)}-symbol universe"
    )
    console.print(f"  Source: {URL}")
    console.print(f"  Scope: {scope}   Reverse-only: {args.reverse}")
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
