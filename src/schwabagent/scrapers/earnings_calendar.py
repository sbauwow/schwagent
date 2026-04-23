"""Earnings calendar scraper — Briefing.com (hosted under Schwab).

Source URL: hosting.briefing.com/cschwab/Calendars/EarningsCalendar5Weeks.htm

Returns ~5 weeks of upcoming and recently-reported US earnings with:
  date, BMO/AMC session, company, ticker, actual EPS (if reported),
  consensus EPS, year-ago EPS, year-over-year revenue change %, and
  a "confirmed" flag (Briefing marks confirmed release dates with a
  check-box image; blank icon = tentative).

HTML structure (no formal schema, so we parse by CSS class):
  <td class="sectionTitle">Thursday, April 09</td>    — date header
  <td class="rH">Before The Open</td>                 — session header
  <td class="rH">After The Close</td>
  <tr> … rD/rL cells …                                — one data row,
    9 cells: [confirm-img, company, ticker, actual,
              a/e marker, consensus, yr-ago, blank, yr/yr-rev]

Usage:
    from schwabagent.scrapers.earnings_calendar import fetch_earnings_calendar
    rows = fetch_earnings_calendar(config)

CLI:
    python -m schwabagent.scrapers.earnings_calendar [--refresh] [--all]
                                                     [--days N] [--symbol X]
"""
from __future__ import annotations

import json
import logging
import re
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup

from schwabagent.config import Config

logger = logging.getLogger(__name__)

ET = ZoneInfo("America/New_York")
URL = "https://hosting.briefing.com/cschwab/Calendars/EarningsCalendar5Weeks.htm"
USER_AGENT = "Mozilla/5.0 (schwagent earnings scraper)"
CACHE_NAME = "earnings_calendar.json"


@dataclass
class EarningsRow:
    """One scheduled or reported earnings release."""

    date: str              # ISO YYYY-MM-DD
    session: str           # "BMO" (before open) | "AMC" (after close)
    company: str
    symbol: str
    confirmed: bool        # Briefing shows the check-box image
    reported: bool         # True = actual result present, False = forecast only
    actual_eps: float | None
    consensus_eps: float | None
    year_ago_eps: float | None
    yoy_rev_pct: float | None

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "EarningsRow":
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


def _save_cache(path: Path, rows: list[EarningsRow]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "fetched_at": datetime.now(ET).isoformat(),
        "rows": [r.to_dict() for r in rows],
    }
    path.write_text(json.dumps(payload, indent=2))


def _load_cache(path: Path) -> list[EarningsRow]:
    payload = json.loads(path.read_text())
    return [EarningsRow.from_dict(r) for r in payload.get("rows", [])]


# ── Fetch + parse ────────────────────────────────────────────────────────────


def _fetch_html(timeout: float = 20.0) -> str:
    resp = requests.get(URL, headers={"User-Agent": USER_AGENT}, timeout=timeout)
    resp.raise_for_status()
    return resp.text


_UPDATED_RE = re.compile(r"Updated:\s*(\d{1,2})-([A-Za-z]{3})-(\d{2,4})")
_SECTION_DATE_RE = re.compile(r"^[A-Za-z]+,\s*([A-Za-z]+)\s+(\d{1,2})$")


def _parse_reference_date(html: str) -> date:
    """Extract 'Updated: DD-MMM-YY' from the page header.

    Falls back to today's ET date if the header is missing or malformed.
    """
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


def _resolve_year(section_text: str, ref: date) -> date | None:
    """Turn 'Thursday, April 09' into an absolute date near `ref`.

    The page never prints the year, so we assume the closest year to
    the reference date. If the naive parse lands more than 90 days
    before ref, it's next year (handles Dec→Jan rollovers).
    """
    m = _SECTION_DATE_RE.match(section_text.strip())
    if not m:
        return None
    month_name, day = m.group(1), m.group(2)
    try:
        parsed = datetime.strptime(f"{month_name} {day} {ref.year}", "%B %d %Y").date()
    except ValueError:
        return None
    if (ref - parsed).days > 90:
        parsed = parsed.replace(year=ref.year + 1)
    elif (parsed - ref).days > 300:
        parsed = parsed.replace(year=ref.year - 1)
    return parsed


def _num(text: str) -> float | None:
    """Parse a numeric cell, tolerating '%', parentheses, dashes, blanks."""
    if text is None:
        return None
    s = text.strip().replace(",", "").replace("%", "")
    if not s or s in {"--", "-", "n/a", "N/A"}:
        return None
    neg = s.startswith("(") and s.endswith(")")
    if neg:
        s = s[1:-1]
    try:
        val = float(s)
    except ValueError:
        return None
    return -val if neg else val


def _cell_text(td) -> str:
    """Extract visible text from a <td>, collapsing whitespace."""
    return re.sub(r"\s+", " ", td.get_text(" ", strip=True)).strip()


def _row_has_confirm_icon(td) -> bool:
    """Briefing shows Cfm_D.gif / Cfm_L.gif for confirmed rows; else no img."""
    img = td.find("img")
    if img is None:
        return False
    src = img.get("src", "")
    return "Cfm_" in src


def parse_earnings(html: str) -> list[EarningsRow]:
    """Walk the Briefing HTML and yield one EarningsRow per scheduled release."""
    soup = BeautifulSoup(html, "html.parser")
    ref = _parse_reference_date(html)

    rows: list[EarningsRow] = []
    current_date: date | None = None
    current_session: str | None = None

    # Walk every <tr> in the content div in document order.
    content = soup.find("div", id="Content") or soup
    for tr in content.find_all("tr"):
        cells = tr.find_all("td", recursive=False)
        if not cells:
            continue

        # Date header row
        section_td = tr.find("td", class_="sectionTitle")
        if section_td is not None:
            dt = _resolve_year(_cell_text(section_td), ref)
            if dt is not None:
                current_date = dt
                current_session = None
            continue

        # Session header row (Before The Open / After The Close)
        header_td = tr.find("td", class_="rH")
        if header_td is not None:
            label = _cell_text(header_td).lower()
            if "before" in label:
                current_session = "BMO"
            elif "after" in label:
                current_session = "AMC"
            continue

        # Data row — must have rD or rL class on its cells
        first = cells[0]
        row_class = first.get("class") or []
        if not any(c in ("rD", "rL") for c in row_class):
            continue
        if current_date is None or current_session is None:
            continue
        if len(cells) < 9:
            continue

        confirmed = _row_has_confirm_icon(first)
        company = _cell_text(cells[1])
        symbol = _cell_text(cells[2]).upper()
        actual_eps = _num(_cell_text(cells[3]))
        marker = _cell_text(cells[4]).lower()
        consensus_eps = _num(_cell_text(cells[5]))
        year_ago_eps = _num(_cell_text(cells[6]))
        yoy_rev_pct = _num(_cell_text(cells[8])) if len(cells) > 8 else None

        if not symbol or not company:
            continue

        rows.append(EarningsRow(
            date=current_date.isoformat(),
            session=current_session,
            company=company,
            symbol=symbol,
            confirmed=confirmed,
            reported=marker == "a" or actual_eps is not None,
            actual_eps=actual_eps,
            consensus_eps=consensus_eps,
            year_ago_eps=year_ago_eps,
            yoy_rev_pct=yoy_rev_pct,
        ))

    return rows


def fetch_earnings_calendar(
    config: Config,
    force_refresh: bool = False,
) -> list[EarningsRow]:
    """Return parsed earnings rows, cached daily to STATE_DIR."""
    path = _cache_path(config)
    if not force_refresh and _cache_fresh(path):
        logger.info("earnings: loading fresh cache from %s", path)
        try:
            return _load_cache(path)
        except Exception as e:
            logger.warning("earnings: cache load failed (%s) — refetching", e)

    logger.info("earnings: fetching %s", URL)
    html = _fetch_html()
    rows = parse_earnings(html)
    logger.info("earnings: parsed %d rows", len(rows))
    try:
        _save_cache(path, rows)
    except OSError as e:
        logger.warning("earnings: cache write failed: %s", e)
    return rows


# ── Filtering helpers ────────────────────────────────────────────────────────


def filter_rows(
    rows: list[EarningsRow],
    symbols: set[str] | None = None,
    start: date | None = None,
    end: date | None = None,
) -> list[EarningsRow]:
    """Narrow the row list by ticker set and/or date window."""
    out = []
    for r in rows:
        if symbols and r.symbol not in symbols:
            continue
        d = date.fromisoformat(r.date)
        if start and d < start:
            continue
        if end and d > end:
            continue
        out.append(r)
    return out


def agent_universe(config: Config) -> set[str]:
    """Union of every ticker the agent treats as 'interesting'."""
    syms: set[str] = set()
    syms.update(config.watchlist)
    syms.update(config.ah_sniper_symbols)
    syms.update(config.momentum_symbols)
    return syms


# ── Render ───────────────────────────────────────────────────────────────────


def _fmt_eps(v: float | None) -> str:
    if v is None:
        return "—"
    return f"{v:+.2f}" if v < 0 else f"{v:.2f}"


def _fmt_pct(v: float | None) -> str:
    if v is None:
        return "—"
    return f"{v:+.1f}%"


def render_table(rows: list[EarningsRow]):
    """Group rows by date and return a Rich Table for terminal output."""
    from rich.table import Table

    t = Table(title="Earnings Calendar", show_lines=False, title_style="bold cyan")
    t.add_column("Date", style="bold")
    t.add_column("Sess", justify="center")
    t.add_column("Symbol", style="bold")
    t.add_column("Company")
    t.add_column("Actual", justify="right")
    t.add_column("Cons.", justify="right")
    t.add_column("YrAgo", justify="right")
    t.add_column("YoY Rev", justify="right")
    t.add_column("Conf", justify="center")

    rows = sorted(rows, key=lambda r: (r.date, 0 if r.session == "BMO" else 1, r.symbol))
    for r in rows:
        actual = _fmt_eps(r.actual_eps)
        if r.actual_eps is not None and r.consensus_eps is not None:
            color = "green" if r.actual_eps >= r.consensus_eps else "red"
            actual = f"[{color}]{actual}[/{color}]"
        yoy_color = "dim"
        if r.yoy_rev_pct is not None:
            yoy_color = "green" if r.yoy_rev_pct >= 0 else "red"
        t.add_row(
            r.date,
            r.session,
            r.symbol,
            (r.company[:28] + "…") if len(r.company) > 29 else r.company,
            actual,
            _fmt_eps(r.consensus_eps),
            _fmt_eps(r.year_ago_eps),
            f"[{yoy_color}]{_fmt_pct(r.yoy_rev_pct)}[/{yoy_color}]",
            "[green]✓[/green]" if r.confirmed else "[dim]·[/dim]",
        )
    return t


# ── CLI entry point ──────────────────────────────────────────────────────────


def main() -> None:
    import argparse
    import logging as _logging

    from rich.console import Console

    parser = argparse.ArgumentParser(
        prog="earnings",
        description="Scrape the Briefing.com earnings calendar (next ~5 weeks).",
    )
    parser.add_argument("--refresh", action="store_true", help="Force cache refresh")
    parser.add_argument(
        "--all", action="store_true",
        help="Show every ticker (default: filter to watchlist∪ah_sniper∪momentum)",
    )
    parser.add_argument(
        "--days", type=int, default=0,
        help="Limit to next N days from today (default: all ~35 days)",
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
    console.rule("[cyan]Earnings Calendar[/cyan]")
    console.print()

    try:
        rows = fetch_earnings_calendar(config, force_refresh=args.refresh)
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

    filtered = filter_rows(rows, symbols=filter_syms, start=start, end=end)
    total = len(rows)
    shown = len(filtered)

    scope = "all" if filter_syms is None else (
        f"symbol={args.symbol.upper()}" if args.symbol else f"{len(filter_syms)}-symbol universe"
    )
    window = f"next {args.days}d" if args.days > 0 else "all ~5 weeks"
    console.print(f"  Source: {URL}")
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
