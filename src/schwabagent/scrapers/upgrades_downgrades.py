"""Analyst ratings scraper — Briefing.com (hosted under Schwab).

Source URL: hosting.briefing.com/cschwab/Calendars/UpgradesDowngrades.htm

Returns the current day's analyst upgrades and downgrades with:
  action (upgrade/downgrade), company, symbol, brokerage firm,
  from-rating → to-rating, price target.

Unlike the earnings / dividend / splits pages, this one has no dates on
the rows themselves — the page is a live snapshot of today's ratings
activity. We cache it daily like the others; `fetched_at` carries the
effective timestamp.

HTML structure:
  <td class="sectionTitle">Upgrades</td>          — section header
  <tr> … rH cells …                                — column header row
  <tr> … rD / rL / rDa cells …                     — data row,
    5 cells: [company, ticker, firm, change, target]
  <td class="sectionTitle">Downgrades</td>
  …

Usage:
    from schwabagent.scrapers.upgrades_downgrades import fetch_ratings
    rows = fetch_ratings(config)

CLI:
    python -m schwabagent.scrapers.upgrades_downgrades [--refresh] [--all]
                                                       [--symbol X] [--down]
"""
from __future__ import annotations

import json
import logging
import re
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup

from schwabagent.config import Config
from schwabagent.scrapers.earnings_calendar import agent_universe

logger = logging.getLogger(__name__)

ET = ZoneInfo("America/New_York")
URL = "https://hosting.briefing.com/cschwab/Calendars/UpgradesDowngrades.htm"
USER_AGENT = "Mozilla/5.0 (schwagent ratings scraper)"
CACHE_NAME = "ratings.json"

_ACTION_UPGRADE = "upgrade"
_ACTION_DOWNGRADE = "downgrade"


@dataclass
class RatingRow:
    """One analyst rating change."""

    action: str            # "upgrade" | "downgrade"
    symbol: str
    company: str
    firm: str              # brokerage firm making the call
    from_rating: str       # prior rating ("Hold", "Neutral", …)
    to_rating: str         # new rating
    price_target: float | None

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "RatingRow":
        return cls(**d)


# ── Cache ────────────────────────────────────────────────────────────────────


def _cache_path(config: Config) -> Path:
    return Path(config.STATE_DIR).expanduser() / CACHE_NAME


def _cache_fresh(path: Path) -> bool:
    if not path.exists():
        return False
    mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=ET)
    return mtime.date() == datetime.now(ET).date()


def _save_cache(path: Path, rows: list[RatingRow]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "fetched_at": datetime.now(ET).isoformat(),
        "rows": [r.to_dict() for r in rows],
    }
    path.write_text(json.dumps(payload, indent=2))


def _load_cache(path: Path) -> list[RatingRow]:
    payload = json.loads(path.read_text())
    return [RatingRow.from_dict(r) for r in payload.get("rows", [])]


# ── Fetch + parse ────────────────────────────────────────────────────────────


def _fetch_html(timeout: float = 20.0) -> str:
    resp = requests.get(URL, headers={"User-Agent": USER_AGENT}, timeout=timeout)
    resp.raise_for_status()
    return resp.text


def _cell_text(td) -> str:
    return re.sub(r"\s+", " ", td.get_text(" ", strip=True)).strip()


_ARROW_RE = re.compile(r"\s*»\s*|\s*->\s*|\s*→\s*")
_TARGET_RE = re.compile(r"\$\s*([\d,]+(?:\.\d+)?)")


def _parse_ratings_change(text: str) -> tuple[str, str]:
    """Split 'Hold » Buy' into ('Hold', 'Buy'). Returns ('', text) on failure."""
    parts = _ARROW_RE.split(text, maxsplit=1)
    if len(parts) != 2:
        return "", text.strip()
    return parts[0].strip(), parts[1].strip()


def _parse_target(text: str) -> float | None:
    """Extract the first dollar figure from a price-target cell."""
    m = _TARGET_RE.search(text)
    if not m:
        return None
    try:
        return float(m.group(1).replace(",", ""))
    except ValueError:
        return None


def parse_ratings(html: str) -> list[RatingRow]:
    """Walk the Briefing HTML and yield one RatingRow per listed change."""
    soup = BeautifulSoup(html, "html.parser")

    rows: list[RatingRow] = []
    current_action: str | None = None

    content = soup.find("div", id="Content") or soup
    for tr in content.find_all("tr"):
        cells = tr.find_all("td", recursive=False)
        if not cells:
            continue

        # Section header row
        section_td = tr.find("td", class_="sectionTitle")
        if section_td is not None:
            label = _cell_text(section_td).lower()
            if "upgrade" in label:
                current_action = _ACTION_UPGRADE
            elif "downgrade" in label:
                current_action = _ACTION_DOWNGRADE
            else:
                current_action = None
            continue

        # Data row — rD / rL / rDa
        first = cells[0]
        row_class = first.get("class") or []
        if not any(c in ("rD", "rL", "rDa") for c in row_class):
            continue
        if current_action is None or len(cells) < 5:
            continue

        company = _cell_text(cells[0])
        symbol = _cell_text(cells[1]).upper()
        firm = _cell_text(cells[2])
        change_text = _cell_text(cells[3])
        target_text = _cell_text(cells[4])

        if not symbol or not company:
            continue

        from_rating, to_rating = _parse_ratings_change(change_text)
        rows.append(RatingRow(
            action=current_action,
            symbol=symbol,
            company=company,
            firm=firm,
            from_rating=from_rating,
            to_rating=to_rating,
            price_target=_parse_target(target_text),
        ))

    return rows


def fetch_ratings(
    config: Config,
    force_refresh: bool = False,
) -> list[RatingRow]:
    """Return parsed rating rows, cached daily to STATE_DIR."""
    path = _cache_path(config)
    if not force_refresh and _cache_fresh(path):
        logger.info("ratings: loading fresh cache from %s", path)
        try:
            return _load_cache(path)
        except Exception as e:
            logger.warning("ratings: cache load failed (%s) — refetching", e)

    logger.info("ratings: fetching %s", URL)
    html = _fetch_html()
    rows = parse_ratings(html)
    logger.info("ratings: parsed %d rows", len(rows))
    try:
        _save_cache(path, rows)
    except OSError as e:
        logger.warning("ratings: cache write failed: %s", e)
    return rows


# ── Filtering helpers ────────────────────────────────────────────────────────


def filter_rows(
    rows: list[RatingRow],
    symbols: set[str] | None = None,
    action: str | None = None,
) -> list[RatingRow]:
    """Narrow the row list by ticker set and/or action ('upgrade'/'downgrade')."""
    out = []
    for r in rows:
        if symbols and r.symbol not in symbols:
            continue
        if action and r.action != action:
            continue
        out.append(r)
    return out


# ── Render ───────────────────────────────────────────────────────────────────


def _fmt_target(v: float | None) -> str:
    if v is None:
        return "—"
    return f"${v:,.2f}" if v < 1000 else f"${v:,.0f}"


def render_table(rows: list[RatingRow]):
    from rich.table import Table

    t = Table(title="Analyst Ratings", show_lines=False, title_style="bold cyan")
    t.add_column("Action")
    t.add_column("Symbol", style="bold")
    t.add_column("Company")
    t.add_column("Firm")
    t.add_column("Change")
    t.add_column("Target", justify="right")

    rows = sorted(rows, key=lambda r: (r.action, r.symbol))
    for r in rows:
        colour = "green" if r.action == _ACTION_UPGRADE else "red"
        t.add_row(
            f"[{colour}]{r.action}[/{colour}]",
            r.symbol,
            (r.company[:28] + "…") if len(r.company) > 29 else r.company,
            (r.firm[:24] + "…") if len(r.firm) > 25 else r.firm,
            f"{r.from_rating} → {r.to_rating}" if r.from_rating else r.to_rating,
            _fmt_target(r.price_target),
        )
    return t


# ── CLI entry point ──────────────────────────────────────────────────────────


def main() -> None:
    import argparse
    import logging as _logging

    from rich.console import Console

    parser = argparse.ArgumentParser(
        prog="ratings",
        description="Scrape today's Briefing.com analyst upgrades / downgrades.",
    )
    parser.add_argument("--refresh", action="store_true", help="Force cache refresh")
    parser.add_argument(
        "--all", action="store_true",
        help="Show every ticker (default: filter to watchlist∪ah_sniper∪momentum)",
    )
    parser.add_argument("--up", action="store_true", help="Upgrades only")
    parser.add_argument("--down", action="store_true", help="Downgrades only")
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
    console.rule("[cyan]Analyst Ratings[/cyan]")
    console.print()

    try:
        rows = fetch_ratings(config, force_refresh=args.refresh)
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

    action: str | None = None
    if args.up and not args.down:
        action = _ACTION_UPGRADE
    elif args.down and not args.up:
        action = _ACTION_DOWNGRADE

    filtered = filter_rows(rows, symbols=filter_syms, action=action)
    total = len(rows)
    shown = len(filtered)

    scope = "all" if filter_syms is None else (
        f"symbol={args.symbol.upper()}" if args.symbol else f"{len(filter_syms)}-symbol universe"
    )
    action_label = action or "both"
    console.print(f"  Source: {URL}")
    console.print(f"  Scope: {scope}   Action: {action_label}")
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
