"""Unit tests for the Briefing-hosted scrapers.

Only the parse + cache round-trip are covered here — network fetches are
exercised by running the scraper CLIs manually against the live source.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from schwabagent.config import Config
from schwabagent.scrapers.splits_calendar import (
    SplitRow,
    _load_cache as _load_splits_cache,
    _save_cache as _save_splits_cache,
    fetch_splits_calendar,
    parse_splits,
)
from schwabagent.scrapers.upgrades_downgrades import (
    RatingRow,
    _load_cache as _load_ratings_cache,
    _save_cache as _save_ratings_cache,
    fetch_ratings,
    parse_ratings,
)


@pytest.fixture
def tmp_dir(tmp_path):
    return str(tmp_path)


@pytest.fixture
def config(tmp_dir) -> Config:
    return Config(
        SCHWAB_API_KEY="test",
        SCHWAB_APP_SECRET="test",
        STATE_DIR=tmp_dir,
        DRY_RUN=True,
    )


# ── Fixtures: trimmed HTML that mirrors Briefing's real structure ────────────


SPLITS_HTML = """
<html><body>
<p>Updated: 14-Apr-26 16:30 ET</p>
<div id="Content">
<table>
  <tr><td class="sectionTitle">April</td></tr>
  <tr>
    <td class="rH">Company</td><td class="rH">Symbol</td>
    <td class="rH">Ratio</td><td class="rH">Payable</td>
    <td class="rH">Ex-Date*</td><td class="rH">Announced</td>
    <td class="rH">Optionable</td>
  </tr>
  <tr>
    <td class="rD">Vanguard Growth ETF</td>
    <td class="rD">VUG</td>
    <td class="rD">6-1</td>
    <td class="rD">Apr 20</td>
    <td class="rD">Apr 21</td>
    <td class="rD">Mar 26</td>
    <td class="rD">Yes</td>
  </tr>
  <tr>
    <td class="rL">Tuniu</td>
    <td class="rL">TOUR</td>
    <td class="rL">1-10</td>
    <td class="rL">Apr 21</td>
    <td class="rL">Apr 22</td>
    <td class="rL">Mar 20</td>
    <td class="rL">Yes</td>
  </tr>
  <tr><td class="sectionTitle">January</td></tr>
  <tr>
    <td class="rH">Company</td><td class="rH">Symbol</td>
    <td class="rH">Ratio</td><td class="rH">Payable</td>
    <td class="rH">Ex-Date*</td><td class="rH">Announced</td>
    <td class="rH">Optionable</td>
  </tr>
  <tr>
    <td class="rD">Next Year Co</td>
    <td class="rD">NYCO</td>
    <td class="rD">3-2</td>
    <td class="rD">Jan 14</td>
    <td class="rD">Jan 15</td>
    <td class="rD">Dec 10</td>
    <td class="rD"></td>
  </tr>
</table>
</div>
</body></html>
"""


RATINGS_HTML = """
<html><body>
<div id="Content">
<table>
  <tr><td class="sectionTitle">Upgrades</td></tr>
  <tr>
    <td class="rH">Company</td><td class="rH">Ticker</td>
    <td class="rH">Brokerage Firm</td><td class="rH">Ratings Change</td>
    <td class="rH">Price Target</td>
  </tr>
  <tr>
    <td class="rD">Cloudflare</td>
    <td class="rD">NET</td>
    <td class="rD">Piper Sandler</td>
    <td class="rD">Neutral » Overweight</td>
    <td class="rD">$222</td>
  </tr>
  <tr>
    <td class="rL">Alector</td>
    <td class="rL">ALEC</td>
    <td class="rL">Cantor Fitzgerald</td>
    <td class="rL">Neutral » Overweight</td>
    <td class="rL"></td>
  </tr>
  <tr><td class="sectionTitle">Downgrades</td></tr>
  <tr>
    <td class="rH">Company</td><td class="rH">Ticker</td>
    <td class="rH">Brokerage Firm</td><td class="rH">Ratings Change</td>
    <td class="rH">Price Target</td>
  </tr>
  <tr>
    <td class="rD">SolarEdge</td>
    <td class="rD">SEDG</td>
    <td class="rD">Goldman</td>
    <td class="rD">Neutral » Sell</td>
    <td class="rD">$31</td>
  </tr>
</table>
</div>
</body></html>
"""


# ── Splits parser ────────────────────────────────────────────────────────────


class TestSplitsParser:
    def test_parses_all_data_rows(self):
        rows = parse_splits(SPLITS_HTML)
        symbols = {r.symbol for r in rows}
        assert symbols == {"VUG", "TOUR", "NYCO"}

    def test_forward_split_flagged_correctly(self):
        rows = {r.symbol: r for r in parse_splits(SPLITS_HTML)}
        vug = rows["VUG"]
        assert vug.ratio == "6-1"
        assert vug.ratio_num == 6 and vug.ratio_den == 1
        assert vug.is_reverse is False
        assert vug.optionable is True

    def test_reverse_split_flagged_correctly(self):
        rows = {r.symbol: r for r in parse_splits(SPLITS_HTML)}
        tour = rows["TOUR"]
        assert tour.ratio == "1-10"
        assert tour.is_reverse is True

    def test_ex_date_year_resolved_from_updated_header(self):
        rows = {r.symbol: r for r in parse_splits(SPLITS_HTML)}
        # Updated: 14-Apr-26 → April entries are 2026
        assert rows["VUG"].ex_date == "2026-04-21"
        assert rows["TOUR"].ex_date == "2026-04-22"

    def test_future_month_rolls_year_forward(self):
        """January section appears after April. Reference date is Apr 14 2026;
        January should resolve to 2027 (next January, not prior one)."""
        rows = {r.symbol: r for r in parse_splits(SPLITS_HTML)}
        nyco = rows["NYCO"]
        assert nyco.ex_date == "2027-01-15"

    def test_unannounced_optionable_cell_parses(self):
        rows = {r.symbol: r for r in parse_splits(SPLITS_HTML)}
        assert rows["NYCO"].optionable is False

    def test_payable_date_populated(self):
        rows = {r.symbol: r for r in parse_splits(SPLITS_HTML)}
        assert rows["VUG"].payable_date == "2026-04-20"


class TestSplitsCache:
    def test_round_trip(self, tmp_path):
        path = tmp_path / "splits_calendar.json"
        rows = parse_splits(SPLITS_HTML)
        _save_splits_cache(path, rows)
        loaded = _load_splits_cache(path)
        assert len(loaded) == len(rows)
        assert {r.symbol for r in loaded} == {r.symbol for r in rows}
        assert all(isinstance(r, SplitRow) for r in loaded)


class TestSplitsFetchUsesCache:
    def test_fresh_cache_skips_network(self, config, monkeypatch):
        """If STATE_DIR holds a today-mtimed cache, fetch_splits_calendar
        must load it without calling the HTTP layer."""
        from schwabagent.scrapers.splits_calendar import _cache_path

        rows = parse_splits(SPLITS_HTML)
        _save_splits_cache(_cache_path(config), rows)

        def _boom(*_a, **_k):
            raise AssertionError("network layer should not be called")

        monkeypatch.setattr(
            "schwabagent.scrapers.splits_calendar._fetch_html", _boom,
        )

        loaded = fetch_splits_calendar(config)
        assert {r.symbol for r in loaded} == {"VUG", "TOUR", "NYCO"}


# ── Ratings parser ───────────────────────────────────────────────────────────


class TestRatingsParser:
    def test_both_sections_parsed(self):
        rows = parse_ratings(RATINGS_HTML)
        upgrades = [r for r in rows if r.action == "upgrade"]
        downgrades = [r for r in rows if r.action == "downgrade"]
        assert {r.symbol for r in upgrades} == {"NET", "ALEC"}
        assert {r.symbol for r in downgrades} == {"SEDG"}

    def test_ratings_change_split(self):
        rows = {r.symbol: r for r in parse_ratings(RATINGS_HTML)}
        net = rows["NET"]
        assert net.from_rating == "Neutral"
        assert net.to_rating == "Overweight"

    def test_price_target_parsed(self):
        rows = {r.symbol: r for r in parse_ratings(RATINGS_HTML)}
        assert rows["NET"].price_target == 222.0
        assert rows["SEDG"].price_target == 31.0
        assert rows["ALEC"].price_target is None

    def test_firm_preserved(self):
        rows = {r.symbol: r for r in parse_ratings(RATINGS_HTML)}
        assert rows["SEDG"].firm == "Goldman"


class TestRatingsCache:
    def test_round_trip(self, tmp_path):
        path = tmp_path / "ratings.json"
        rows = parse_ratings(RATINGS_HTML)
        _save_ratings_cache(path, rows)
        loaded = _load_ratings_cache(path)
        assert len(loaded) == len(rows)
        assert all(isinstance(r, RatingRow) for r in loaded)


class TestRatingsFetchUsesCache:
    def test_fresh_cache_skips_network(self, config, monkeypatch):
        from schwabagent.scrapers.upgrades_downgrades import _cache_path

        rows = parse_ratings(RATINGS_HTML)
        _save_ratings_cache(_cache_path(config), rows)

        def _boom(*_a, **_k):
            raise AssertionError("network layer should not be called")

        monkeypatch.setattr(
            "schwabagent.scrapers.upgrades_downgrades._fetch_html", _boom,
        )

        loaded = fetch_ratings(config)
        actions = {r.symbol: r.action for r in loaded}
        assert actions == {"NET": "upgrade", "ALEC": "upgrade", "SEDG": "downgrade"}
