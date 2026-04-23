"""Tests for the quant_research scraper.

Network fetches are stubbed — we cover the DB schema, scoring,
dedup, top_unread filtering, mark_notified, and cleanup.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from schwabagent.config import Config
from schwabagent.scrapers import quant_research as qr


@pytest.fixture
def config(tmp_path) -> Config:
    return Config(
        SCHWAB_API_KEY="test",
        SCHWAB_APP_SECRET="test",
        STATE_DIR=str(tmp_path),
        DRY_RUN=True,
        QUANT_RESEARCH_ENABLED=True,
        QUANT_RESEARCH_SOURCES="arxiv",
        QUANT_RESEARCH_MAX_PER_SOURCE=5,
        QUANT_RESEARCH_MIN_RELEVANCE=0.5,
        QUANT_RESEARCH_DIGEST_TOP_N=3,
        STRATEGIES="momentum,mean_reversion",
        WATCHLIST="SPY,QQQ,NVDA",
    )


def _paper(title: str, abstract: str = "", source: str = "arxiv", url: str | None = None) -> qr.PaperRow:
    return qr.PaperRow(
        source=source,
        source_id=title[:16].replace(" ", "_"),
        title=title,
        authors="Doe, Smith",
        abstract=abstract,
        url=url or f"https://example.test/{title[:30].replace(' ', '-')}",
        published="2026-04-20T00:00:00Z",
    )


# ── Scoring ─────────────────────────────────────────────────────────────────


def test_score_keyword_hits(config):
    row = _paper("Momentum and mean reversion on SPY", "A study of volatility regimes.")
    symbols = {"SPY", "QQQ", "NVDA"}
    strategies = {"momentum", "mean reversion"}

    score, tags = qr._score_paper(row, symbols, strategies)

    assert score > 0
    # keywords
    assert "momentum" in tags
    assert "mean reversion" in tags
    assert "volatility" in tags
    # strategy hits
    assert any(t.startswith("strat:") for t in tags)
    # ticker hit (SPY — case sensitive whole-word match)
    assert "sym:SPY" in tags


def test_score_no_false_positive_on_embedded_ticker(config):
    # "SPYING" should not match SPY
    row = _paper("SPYING trends in factor models")
    score, tags = qr._score_paper(row, {"SPY"}, set())
    assert "sym:SPY" not in tags


# ── Persistence + dedup ──────────────────────────────────────────────────────


def test_fetch_and_dedup(monkeypatch, config):
    rows_batch = [
        _paper("Momentum decay in mean reversion", "volatility study on SPY"),
        _paper("Pairs trading with Kelly sizing", "cointegration on factor exposures"),
        _paper("Random unrelated topic", "nothing of interest here"),
    ]

    calls = {"n": 0}

    def fake_fetch(name, url, max_results):
        calls["n"] += 1
        return list(rows_batch)

    monkeypatch.setattr(qr, "_fetch_source", fake_fetch)

    first = qr.fetch_new_papers(config)
    assert len(first) == 3
    assert all(r.id is not None for r in first)
    assert all(r.fetched_at for r in first)

    # Second call: same URLs -> all deduped, 0 new
    second = qr.fetch_new_papers(config)
    assert second == []
    assert calls["n"] == 2


def test_fetch_disabled_returns_empty(monkeypatch, tmp_path):
    cfg = Config(
        SCHWAB_API_KEY="x",
        SCHWAB_APP_SECRET="x",
        STATE_DIR=str(tmp_path),
        QUANT_RESEARCH_ENABLED=False,
    )
    called = {"n": 0}
    monkeypatch.setattr(qr, "_fetch_source", lambda *a, **k: (called.__setitem__("n", called["n"] + 1) or []))

    assert qr.fetch_new_papers(cfg) == []
    assert called["n"] == 0


def test_fetch_source_failure_is_isolated(monkeypatch, config):
    config.QUANT_RESEARCH_SOURCES = "arxiv,alpha_architect"

    def fake_fetch(name, url, max_results):
        if name == "arxiv":
            raise RuntimeError("arxiv down")
        return [_paper("Volatility term structure", source="alpha_architect")]

    monkeypatch.setattr(qr, "_fetch_source", fake_fetch)

    rows = qr.fetch_new_papers(config)
    assert len(rows) == 1
    assert rows[0].source == "alpha_architect"


# ── top_unread / mark_notified ───────────────────────────────────────────────


def test_top_unread_filters_by_score_and_notified(monkeypatch, config):
    monkeypatch.setattr(
        qr,
        "_fetch_source",
        lambda *_: [
            _paper("High relevance: momentum and volatility on SPY"),
            _paper("Medium relevance: backtest of a drift signal"),
            _paper("Low relevance: nothing financial here"),
        ],
    )

    inserted = qr.fetch_new_papers(config)
    assert len(inserted) == 3

    top = qr.top_unread(config, limit=5)
    # Low-relevance row filtered by MIN_RELEVANCE=0.5
    assert all(r.relevance_score >= config.QUANT_RESEARCH_MIN_RELEVANCE for r in top)
    assert any("momentum" in r.title.lower() for r in top)

    # Sorted descending by score
    scores = [r.relevance_score for r in top]
    assert scores == sorted(scores, reverse=True)

    # Mark first as notified and it should drop off
    qr.mark_notified(config, [top[0].id])
    top_after = qr.top_unread(config, limit=5)
    assert all(r.id != top[0].id for r in top_after)


def test_mark_notified_handles_empty(config):
    # Should be a no-op, not raise
    qr.mark_notified(config, [])
    qr.mark_notified(config, [None])


# ── LLM summary gating ───────────────────────────────────────────────────────


class _FakeLLM:
    def __init__(self, available=True, reply="Short one-sentence verdict."):
        self._available = available
        self.reply = reply
        self.calls = 0

    def is_available(self) -> bool:
        return self._available

    def generate(self, prompt, system=None, max_tokens=None):
        self.calls += 1
        return self.reply


def test_llm_summary_populated_when_enabled(monkeypatch, config):
    config.QUANT_RESEARCH_LLM_SUMMARIES = True
    config.QUANT_RESEARCH_DIGEST_TOP_N = 2

    monkeypatch.setattr(
        qr,
        "_fetch_source",
        lambda *_: [
            _paper("Momentum on SPY", abstract="We study momentum."),
            _paper("Volatility on QQQ", abstract="IV vs RV."),
            _paper("Unrelated topic", abstract=""),
        ],
    )

    llm = _FakeLLM()
    rows = qr.fetch_new_papers(config, llm=llm)

    # Top 2 by score get summaries, the third (empty abstract + low score) does not
    summarized = [r for r in rows if r.summary]
    assert 1 <= len(summarized) <= 2
    assert llm.calls == len(summarized)


def test_llm_unavailable_skips_summary(monkeypatch, config):
    config.QUANT_RESEARCH_LLM_SUMMARIES = True
    monkeypatch.setattr(
        qr,
        "_fetch_source",
        lambda *_: [_paper("Momentum on SPY", abstract="x")],
    )

    llm = _FakeLLM(available=False)
    rows = qr.fetch_new_papers(config, llm=llm)

    assert llm.calls == 0
    assert all(not r.summary for r in rows)


# ── Cleanup ──────────────────────────────────────────────────────────────────


def test_cleanup_prunes_old_rows(monkeypatch, config):
    monkeypatch.setattr(
        qr,
        "_fetch_source",
        lambda *_: [_paper("Momentum on SPY", abstract="x")],
    )
    qr.fetch_new_papers(config)

    # Manually backdate fetched_at beyond retention
    db = qr._connect(config)
    try:
        old_ts = (datetime.now(timezone.utc) - timedelta(days=400)).isoformat()
        db.execute("UPDATE quant_papers SET fetched_at = ?", (old_ts,))
        db.commit()
    finally:
        db.close()

    deleted = qr.cleanup(config, retention_days=180)
    assert deleted == 1

    db = qr._connect(config)
    try:
        remaining = db.execute("SELECT COUNT(*) FROM quant_papers").fetchone()[0]
    finally:
        db.close()
    assert remaining == 0


# ── Unknown source is logged, not fatal ──────────────────────────────────────


def test_unknown_source_is_skipped(monkeypatch, config, caplog):
    config.QUANT_RESEARCH_SOURCES = "arxiv,totally_made_up"

    monkeypatch.setattr(
        qr, "_fetch_source",
        lambda name, *_: [_paper(f"Paper from {name}")] if name == "arxiv" else [],
    )

    caplog.set_level("WARNING", logger="schwabagent.scrapers.quant_research")
    rows = qr.fetch_new_papers(config)

    assert len(rows) == 1
    assert any("unknown source" in r.message for r in caplog.records)
