"""Tests for Config loading and validation."""
from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pytest

from schwabagent.config import Config


# ── Default values ────────────────────────────────────────────────────────────

class TestConfigDefaults:
    def test_dry_run_default_true(self):
        c = Config()
        assert c.DRY_RUN is True

    def test_default_watchlist_parsed(self):
        c = Config()
        wl = c.watchlist
        assert isinstance(wl, list)
        assert len(wl) > 0
        assert all(s == s.upper() for s in wl)

    def test_default_strategies_parsed(self):
        c = Config()
        strats = c.strategies
        assert isinstance(strats, list)
        assert len(strats) > 0
        assert all(s == s.lower() for s in strats)

    def test_default_risk_values(self):
        c = Config()
        assert 0 < c.MAX_POSITION_PCT <= 1.0
        assert c.MAX_POSITION_VALUE > 0
        assert c.MAX_TOTAL_EXPOSURE > 0
        assert 0 < c.MAX_DRAWDOWN_PCT <= 100

    def test_state_dir_default(self):
        c = Config()
        assert "schwab" in c.STATE_DIR.lower() or "~" in c.STATE_DIR

    def test_log_level_property(self):
        c = Config()
        assert c.log_level in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")


# ── Override via constructor ──────────────────────────────────────────────────

class TestConfigOverrides:
    def test_override_dry_run(self):
        c = Config(DRY_RUN=False)
        assert c.DRY_RUN is False
        assert c.dry_run is False

    def test_override_watchlist(self):
        c = Config(WATCHLIST="AAPL,TSLA,NVDA")
        assert c.watchlist == ["AAPL", "TSLA", "NVDA"]

    def test_watchlist_strips_spaces(self):
        c = Config(WATCHLIST=" AAPL , MSFT , GOOGL ")
        assert c.watchlist == ["AAPL", "MSFT", "GOOGL"]

    def test_watchlist_uppercases(self):
        c = Config(WATCHLIST="aapl,msft")
        assert c.watchlist == ["AAPL", "MSFT"]

    def test_override_strategies(self):
        c = Config(STRATEGIES="momentum,composite")
        assert c.strategies == ["momentum", "composite"]

    def test_strategies_lowercased(self):
        c = Config(STRATEGIES="Momentum,COMPOSITE")
        assert c.strategies == ["momentum", "composite"]

    def test_set_dry_run_via_property(self):
        c = Config()
        c.dry_run = False
        assert c.DRY_RUN is False
        assert c.dry_run is False


# ── Validation ────────────────────────────────────────────────────────────────

class TestConfigValidation:
    def test_missing_api_key_is_error(self):
        c = Config(SCHWAB_API_KEY="", SCHWAB_APP_SECRET="secret")
        errors = c.validate()
        assert any("API_KEY" in e for e in errors)

    def test_missing_app_secret_is_error(self):
        c = Config(SCHWAB_API_KEY="key", SCHWAB_APP_SECRET="")
        errors = c.validate()
        assert any("APP_SECRET" in e for e in errors)

    def test_valid_credentials_no_errors(self):
        c = Config(SCHWAB_API_KEY="key123", SCHWAB_APP_SECRET="secret456")
        errors = c.validate()
        # Should have no credential errors (may have other warnings)
        assert not any("KEY" in e or "SECRET" in e for e in errors)

    def test_invalid_position_pct_too_high(self):
        c = Config(
            SCHWAB_API_KEY="k",
            SCHWAB_APP_SECRET="s",
            MAX_POSITION_PCT=1.5,
        )
        errors = c.validate()
        assert any("MAX_POSITION_PCT" in e for e in errors)

    def test_invalid_position_pct_zero(self):
        c = Config(
            SCHWAB_API_KEY="k",
            SCHWAB_APP_SECRET="s",
            MAX_POSITION_PCT=0.0,
        )
        errors = c.validate()
        assert any("MAX_POSITION_PCT" in e for e in errors)

    def test_invalid_drawdown_pct(self):
        c = Config(
            SCHWAB_API_KEY="k",
            SCHWAB_APP_SECRET="s",
            MAX_DRAWDOWN_PCT=0.0,
        )
        errors = c.validate()
        assert any("MAX_DRAWDOWN_PCT" in e for e in errors)

    def test_empty_watchlist_is_error(self):
        c = Config(
            SCHWAB_API_KEY="k",
            SCHWAB_APP_SECRET="s",
            WATCHLIST="",
        )
        errors = c.validate()
        assert any("WATCHLIST" in e for e in errors)


# ── .env file loading ─────────────────────────────────────────────────────────

class TestEnvFileLoading:
    def test_load_from_env_file(self, tmp_path, monkeypatch):
        env_file = tmp_path / ".env"
        env_file.write_text(
            "SCHWAB_API_KEY=my_key\n"
            "SCHWAB_APP_SECRET=my_secret\n"
            "DRY_RUN=false\n"
            "WATCHLIST=AAPL,NVDA\n"
        )
        monkeypatch.chdir(tmp_path)
        c = Config()
        assert c.SCHWAB_API_KEY == "my_key"
        assert c.SCHWAB_APP_SECRET == "my_secret"
        assert c.DRY_RUN is False
        assert c.watchlist == ["AAPL", "NVDA"]
