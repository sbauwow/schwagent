"""Quant / finance-journal research scraper.

Pulls recent paper listings and long-form blog posts from a fixed set of
public Atom/RSS feeds, dedups against a local SQLite store, scores each
item for relevance against the running strategies and watchlist, and
returns new rows. The DreamCycle `research_papers` phase calls this once
per cycle and forwards the top-scoring new items to Telegram.

Sources (all public, no auth):
  - arXiv q-fin (CP + TR + ST categories) — arxiv.org export API (Atom)
  - SSRN FEN (Financial Economics Network) — SSRN RSS
  - Papers With Backtests — paperswithbacktest.com/feed
  - Alpha Architect — alphaarchitect.com/feed/
  - Hudson & Thames — hudsonthames.com/feed/

Each feed is best-effort: a fetch or parse failure is logged and the
other sources continue. Parsed rows land in
`~/.schwagent/quant_research.db` table `quant_papers` with URL as the
dedup key.

Public API:
    fetch_new_papers(config, llm=None) -> list[PaperRow]
    top_unread(config, limit=5) -> list[PaperRow]
    mark_notified(config, paper_ids) -> None
    render_table(rows) -> rich Table     (CLI helper)

CLI:
    python -m schwabagent.scrapers.quant_research [--refresh] [--top N]
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
import sqlite3
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable
from xml.etree import ElementTree as ET

import requests

from schwabagent.config import Config

logger = logging.getLogger(__name__)

USER_AGENT = "Mozilla/5.0 (schwagent quant-research scraper)"
DB_NAME = "quant_research.db"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS quant_papers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    source_id TEXT NOT NULL,
    title TEXT NOT NULL,
    authors TEXT DEFAULT '',
    abstract TEXT DEFAULT '',
    url TEXT NOT NULL UNIQUE,
    published TEXT DEFAULT '',
    fetched_at TEXT NOT NULL,
    relevance_score REAL DEFAULT 0,
    relevance_tags TEXT DEFAULT '',
    summary TEXT DEFAULT '',
    notified INTEGER DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_qpapers_published ON quant_papers(published DESC);
CREATE INDEX IF NOT EXISTS idx_qpapers_score     ON quant_papers(relevance_score DESC);
CREATE INDEX IF NOT EXISTS idx_qpapers_notified  ON quant_papers(notified, relevance_score DESC);
"""

# Keyword -> weight, matched against title+abstract (case-insensitive).
# Weights are additive; a paper mentioning "momentum" and "volatility"
# gets 2.0 before symbol/strategy matches.
_KEYWORDS: dict[str, float] = {
    "momentum": 1.0, "mean reversion": 1.0, "trend following": 1.0,
    "pairs trading": 1.0, "statistical arbitrage": 1.5, "stat arb": 1.5,
    "volatility": 0.8, "implied volatility": 1.0, "vix": 0.8,
    "options": 0.7, "covered call": 1.0, "cash secured put": 1.0,
    "theta": 0.7, "gamma": 0.7, "delta hedg": 0.8,
    "factor model": 1.0, "risk parity": 1.0, "carry trade": 0.8,
    "microstructure": 1.0, "limit order book": 1.0, "market making": 1.0,
    "backtest": 0.6, "walk-forward": 0.8, "overfit": 0.5,
    "sharpe": 0.4, "drawdown": 0.4, "kelly": 0.8,
    "regime": 0.8, "drift": 0.4,
    "etf": 0.5, "sector rotation": 1.2, "asset allocation": 0.7,
    "machine learning": 0.5, "reinforcement learning": 0.8,
    "earnings drift": 1.0, "post-earnings announcement drift": 1.2, "pead": 1.2,
    "breadth": 0.6, "tick": 0.4,
}


@dataclass
class PaperRow:
    source: str
    source_id: str
    title: str
    authors: str = ""
    abstract: str = ""
    url: str = ""
    published: str = ""
    fetched_at: str = ""
    relevance_score: float = 0.0
    relevance_tags: str = ""
    summary: str = ""
    notified: int = 0
    id: int | None = None

    def to_dict(self) -> dict:
        return asdict(self)


# ── DB ───────────────────────────────────────────────────────────────────────


def _db_path(config: Config) -> Path:
    return Path(config.STATE_DIR).expanduser() / DB_NAME


def _connect(config: Config) -> sqlite3.Connection:
    path = _db_path(config)
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(str(path), check_same_thread=False)
    db.row_factory = sqlite3.Row
    db.executescript(_SCHEMA)
    db.execute("PRAGMA journal_mode=WAL")
    return db


def _row_to_paper(r: sqlite3.Row) -> PaperRow:
    return PaperRow(
        id=r["id"],
        source=r["source"],
        source_id=r["source_id"],
        title=r["title"],
        authors=r["authors"] or "",
        abstract=r["abstract"] or "",
        url=r["url"],
        published=r["published"] or "",
        fetched_at=r["fetched_at"],
        relevance_score=r["relevance_score"] or 0.0,
        relevance_tags=r["relevance_tags"] or "",
        summary=r["summary"] or "",
        notified=r["notified"] or 0,
    )


# ── Feed fetchers ────────────────────────────────────────────────────────────


def _http_get(url: str, timeout: float = 20.0) -> str:
    resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=timeout)
    resp.raise_for_status()
    return resp.text


_NS = {"atom": "http://www.w3.org/2005/Atom"}


def _fetch_arxiv(max_results: int) -> list[PaperRow]:
    """arXiv q-fin.CP + q-fin.TR + q-fin.ST, newest first."""
    url = (
        "http://export.arxiv.org/api/query?"
        "search_query=cat:q-fin.CP+OR+cat:q-fin.TR+OR+cat:q-fin.ST"
        f"&sortBy=submittedDate&sortOrder=descending&max_results={max_results}"
    )
    xml = _http_get(url)
    root = ET.fromstring(xml)
    rows: list[PaperRow] = []
    for entry in root.findall("atom:entry", _NS):
        arxiv_id = (entry.findtext("atom:id", "", _NS) or "").strip()
        title = re.sub(r"\s+", " ", entry.findtext("atom:title", "", _NS) or "").strip()
        abstract = re.sub(r"\s+", " ", entry.findtext("atom:summary", "", _NS) or "").strip()
        published = (entry.findtext("atom:published", "", _NS) or "").strip()
        authors = ", ".join(
            (a.findtext("atom:name", "", _NS) or "").strip()
            for a in entry.findall("atom:author", _NS)
        )
        # Prefer the abstract page URL over the id-URN
        link_url = arxiv_id
        for link in entry.findall("atom:link", _NS):
            if link.get("rel") == "alternate" and link.get("type") == "text/html":
                link_url = link.get("href") or arxiv_id
        source_id = arxiv_id.rsplit("/", 1)[-1] if arxiv_id else link_url
        if not title or not link_url:
            continue
        rows.append(PaperRow(
            source="arxiv",
            source_id=source_id,
            title=title,
            authors=authors,
            abstract=abstract,
            url=link_url,
            published=published,
        ))
    return rows


def _fetch_rss(url: str, source: str, max_results: int) -> list[PaperRow]:
    """Generic RSS 2.0 / Atom parser for the blog/forum feeds."""
    xml = _http_get(url)
    root = ET.fromstring(xml)
    rows: list[PaperRow] = []

    # RSS 2.0: channel > item
    items = root.findall(".//item")
    if items:
        for item in items[:max_results]:
            title = re.sub(r"\s+", " ", (item.findtext("title") or "")).strip()
            link = (item.findtext("link") or "").strip()
            pub = (item.findtext("pubDate") or "").strip()
            desc = item.findtext("description") or ""
            # Strip HTML tags from description
            desc = re.sub(r"<[^>]+>", " ", desc)
            desc = re.sub(r"\s+", " ", desc).strip()
            authors = (
                item.findtext("{http://purl.org/dc/elements/1.1/}creator")
                or item.findtext("author") or ""
            ).strip()
            if not title or not link:
                continue
            source_id = hashlib.sha1(link.encode()).hexdigest()[:16]
            rows.append(PaperRow(
                source=source,
                source_id=source_id,
                title=title,
                authors=authors,
                abstract=desc[:1500],
                url=link,
                published=pub,
            ))
        return rows

    # Atom: feed > entry
    for entry in root.findall("atom:entry", _NS)[:max_results]:
        title = re.sub(r"\s+", " ", entry.findtext("atom:title", "", _NS) or "").strip()
        published = (entry.findtext("atom:published", "", _NS) or "").strip()
        abstract = re.sub(r"\s+", " ", entry.findtext("atom:summary", "", _NS) or "").strip()
        authors = ", ".join(
            (a.findtext("atom:name", "", _NS) or "").strip()
            for a in entry.findall("atom:author", _NS)
        )
        link_url = ""
        for link in entry.findall("atom:link", _NS):
            if link.get("rel") in (None, "alternate"):
                link_url = link.get("href") or ""
                if link_url:
                    break
        if not title or not link_url:
            continue
        source_id = hashlib.sha1(link_url.encode()).hexdigest()[:16]
        rows.append(PaperRow(
            source=source,
            source_id=source_id,
            title=title,
            authors=authors,
            abstract=abstract[:1500],
            url=link_url,
            published=published,
        ))
    return rows


_FEEDS: dict[str, tuple[str, str]] = {
    # source_key -> (url, pretty_name)
    "arxiv":             ("__arxiv__", "arXiv q-fin"),
    "ssrn":              ("https://papers.ssrn.com/sol3/JELJOUR_Results.cfm?form_name=journalBrowse&journal_id=203&Network=no&lim=false&rss=1", "SSRN FEN"),
    "paperswithbacktest":("https://paperswithbacktest.com/rss.xml", "Papers With Backtests"),
    "alpha_architect":   ("https://alphaarchitect.com/feed/", "Alpha Architect"),
    "hudsonthames":      ("https://hudsonthames.com/feed/", "Hudson & Thames"),
}


def _fetch_source(name: str, url: str, max_results: int) -> list[PaperRow]:
    if name == "arxiv":
        return _fetch_arxiv(max_results)
    return _fetch_rss(url, name, max_results)


# ── Relevance scoring ────────────────────────────────────────────────────────


def _universe_symbols(config: Config) -> set[str]:
    syms: set[str] = set()
    try:
        syms |= set(config.all_symbols)
    except Exception:
        pass
    for attr in ("WATCHLIST", "MOMENTUM_SYMBOLS", "CONVICTION_SYMBOLS", "ETF_UNIVERSE", "THETA_SYMBOLS"):
        val = getattr(config, attr, "") or ""
        for tok in val.split(","):
            tok = tok.strip().upper()
            if tok:
                syms.add(tok)
    # Strip very short/ambiguous tickers that false-match common words
    return {s for s in syms if len(s) >= 3 or s in {"GS", "GE", "HP", "BA"}}


def _strategy_names(config: Config) -> set[str]:
    raw = getattr(config, "STRATEGIES", "") or ""
    return {s.strip().lower().replace("_", " ") for s in raw.split(",") if s.strip()}


def _score_paper(row: PaperRow, symbols: set[str], strategies: set[str]) -> tuple[float, list[str]]:
    haystack = f"{row.title} {row.abstract}".lower()
    score = 0.0
    tags: list[str] = []

    for kw, weight in _KEYWORDS.items():
        if kw in haystack:
            score += weight
            tags.append(kw)

    for strat in strategies:
        if strat and strat in haystack:
            score += 1.0
            tags.append(f"strat:{strat}")

    # Whole-word ticker match, case-sensitive on original title/abstract
    text_case = f"{row.title} {row.abstract}"
    for sym in symbols:
        if re.search(rf"(?<![A-Z0-9]){re.escape(sym)}(?![A-Z0-9])", text_case):
            score += 0.5
            tags.append(f"sym:{sym}")

    return score, tags


# ── Public API ───────────────────────────────────────────────────────────────


def fetch_new_papers(config: Config, llm=None) -> list[PaperRow]:
    """Poll all configured sources, dedup, score, optionally summarize,
    persist new rows, and return them."""
    if not getattr(config, "QUANT_RESEARCH_ENABLED", False):
        return []

    sources_raw = getattr(config, "QUANT_RESEARCH_SOURCES", "") or ""
    wanted = [s.strip() for s in sources_raw.split(",") if s.strip()]
    max_per = int(getattr(config, "QUANT_RESEARCH_MAX_PER_SOURCE", 30))
    summarize = bool(getattr(config, "QUANT_RESEARCH_LLM_SUMMARIES", False))
    top_summarize_n = int(getattr(config, "QUANT_RESEARCH_DIGEST_TOP_N", 5))

    all_fetched: list[PaperRow] = []
    for name in wanted:
        feed = _FEEDS.get(name)
        if not feed:
            logger.warning("quant_research: unknown source '%s'", name)
            continue
        url, pretty = feed
        try:
            rows = _fetch_source(name, url, max_per)
            logger.info("quant_research: %s -> %d items", pretty, len(rows))
            all_fetched.extend(rows)
        except Exception as e:
            logger.warning("quant_research: %s fetch failed: %s", pretty, e)

    if not all_fetched:
        return []

    symbols = _universe_symbols(config)
    strategies = _strategy_names(config)
    now_iso = datetime.now(timezone.utc).isoformat()

    # Score everything first — we want top-N summaries only
    scored: list[PaperRow] = []
    for row in all_fetched:
        score, tags = _score_paper(row, symbols, strategies)
        row.relevance_score = round(score, 2)
        row.relevance_tags = ",".join(tags)
        row.fetched_at = now_iso
        scored.append(row)

    # LLM summaries for the top N by score, before persisting
    if summarize and llm is not None and top_summarize_n > 0:
        try:
            if llm.is_available():
                top = sorted(scored, key=lambda r: r.relevance_score, reverse=True)[:top_summarize_n]
                for r in top:
                    if not r.abstract:
                        continue
                    r.summary = _one_sentence_summary(llm, r)
        except Exception as e:
            logger.warning("quant_research: LLM summarize failed: %s", e)

    # Persist — INSERT OR IGNORE on the unique url constraint
    inserted: list[PaperRow] = []
    db = _connect(config)
    try:
        for r in scored:
            cur = db.execute(
                """INSERT OR IGNORE INTO quant_papers
                   (source, source_id, title, authors, abstract, url, published,
                    fetched_at, relevance_score, relevance_tags, summary, notified)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)""",
                (r.source, r.source_id, r.title, r.authors, r.abstract, r.url,
                 r.published, r.fetched_at, r.relevance_score, r.relevance_tags, r.summary),
            )
            if cur.rowcount > 0:
                r.id = cur.lastrowid
                inserted.append(r)
        db.commit()
    finally:
        db.close()

    logger.info("quant_research: %d new papers stored (of %d fetched)", len(inserted), len(scored))
    return inserted


def top_unread(config: Config, limit: int = 5, min_score: float | None = None) -> list[PaperRow]:
    """Return top unread (notified=0) papers by relevance_score."""
    if min_score is None:
        min_score = float(getattr(config, "QUANT_RESEARCH_MIN_RELEVANCE", 1.0))
    db = _connect(config)
    try:
        rows = db.execute(
            """SELECT * FROM quant_papers
               WHERE notified = 0 AND relevance_score >= ?
               ORDER BY relevance_score DESC, published DESC
               LIMIT ?""",
            (min_score, limit),
        ).fetchall()
        return [_row_to_paper(r) for r in rows]
    finally:
        db.close()


def mark_notified(config: Config, paper_ids: Iterable[int]) -> None:
    ids = [i for i in paper_ids if i is not None]
    if not ids:
        return
    db = _connect(config)
    try:
        db.executemany(
            "UPDATE quant_papers SET notified = 1 WHERE id = ?",
            [(i,) for i in ids],
        )
        db.commit()
    finally:
        db.close()


def cleanup(config: Config, retention_days: int | None = None) -> int:
    """Prune rows older than retention_days (by fetched_at). Returns rows deleted."""
    if retention_days is None:
        retention_days = int(getattr(config, "QUANT_RESEARCH_RETENTION_DAYS", 180))
    cutoff = (datetime.now(timezone.utc) - timedelta(days=retention_days)).isoformat()
    db = _connect(config)
    try:
        cur = db.execute("DELETE FROM quant_papers WHERE fetched_at < ?", (cutoff,))
        db.commit()
        return cur.rowcount or 0
    finally:
        db.close()


# ── LLM summary helper ──────────────────────────────────────────────────────


def _one_sentence_summary(llm, row: PaperRow) -> str:
    prompt = (
        "Summarize the following quant-finance paper in one sentence (<=30 words). "
        "Focus on the concrete trading or market-structure claim, not methodology.\n\n"
        f"Title: {row.title}\n\nAbstract: {row.abstract[:1500]}"
    )
    try:
        out = llm.generate(prompt, system="You write terse, concrete, one-sentence summaries for quant traders.", max_tokens=100)
    except Exception as e:
        logger.debug("quant_research: summarize error: %s", e)
        return ""
    out = re.sub(r"\s+", " ", out or "").strip()
    # Clip to first sentence in case the model over-produces
    m = re.match(r"(.+?[\.!?])\s", out + " ")
    return (m.group(1) if m else out).strip()[:400]


# ── Rendering ────────────────────────────────────────────────────────────────


def render_table(rows: list[PaperRow]):
    from rich.table import Table

    t = Table(title="Quant Research — latest", show_lines=False, title_style="bold cyan")
    t.add_column("Src", style="dim")
    t.add_column("Score", justify="right")
    t.add_column("Title", style="bold")
    t.add_column("Tags", style="yellow")
    t.add_column("Published", style="dim")

    for r in rows:
        t.add_row(
            r.source,
            f"{r.relevance_score:.1f}",
            (r.title[:78] + "…") if len(r.title) > 79 else r.title,
            (r.relevance_tags[:48] + "…") if len(r.relevance_tags) > 49 else r.relevance_tags,
            r.published[:10],
        )
    return t


# ── CLI ──────────────────────────────────────────────────────────────────────


def main() -> None:
    import argparse
    import logging as _logging
    from rich.console import Console

    parser = argparse.ArgumentParser(
        prog="quant_research",
        description="Fetch and score recent quant-finance papers and blog posts.",
    )
    parser.add_argument("--refresh", action="store_true", help="Fetch from sources now")
    parser.add_argument("--top", type=int, default=10, help="Show top N by relevance_score")
    parser.add_argument("--min-score", type=float, default=0.0, help="Minimum score to show")
    parser.add_argument("--all", action="store_true", help="Include already-notified")
    parser.add_argument("--cleanup", action="store_true", help="Prune rows older than retention window")
    args = parser.parse_args()

    config = Config()
    _logging.basicConfig(
        level=getattr(_logging, getattr(config, "log_level", "INFO"), _logging.INFO),
        format="%(asctime)s %(name)-34s %(levelname)-8s %(message)s",
        datefmt="%H:%M:%S",
    )
    console = Console()
    console.print()
    console.rule("[cyan]Quant Research[/cyan]")
    console.print()

    if args.cleanup:
        n = cleanup(config)
        console.print(f"  Pruned {n} old rows")

    if args.refresh:
        from schwabagent.llm import LLMClient  # noqa: F401 — side-effect import check
        rows = fetch_new_papers(config, llm=None)
        console.print(f"  Fetched {len(rows)} new rows")

    db = _connect(config)
    try:
        q = "SELECT * FROM quant_papers WHERE relevance_score >= ?"
        params: list = [args.min_score]
        if not args.all:
            q += " AND notified = 0"
        q += " ORDER BY relevance_score DESC, published DESC LIMIT ?"
        params.append(args.top)
        rows = [_row_to_paper(r) for r in db.execute(q, params).fetchall()]
    finally:
        db.close()

    if not rows:
        console.print("  [dim]No rows match[/dim]")
        console.print()
        return

    console.print(render_table(rows))
    console.print()


if __name__ == "__main__":
    main()
