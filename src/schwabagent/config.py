"""Configuration — loaded from .env via pydantic-settings."""
from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Config(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # ── Schwab credentials ────────────────────────────────────────────────
    SCHWAB_API_KEY: str = ""
    SCHWAB_APP_SECRET: str = ""
    SCHWAB_TOKEN_PATH: str = "~/.schwab-agent/token.json"
    # If empty, the agent will use the first account returned by the API
    SCHWAB_ACCOUNT_HASH: str = ""

    # ── Watchlist / strategies ────────────────────────────────────────────
    WATCHLIST: str = "AAPL,MSFT,GOOGL,AMZN,NVDA,META,TSLA,JPM,V,UNH"
    STRATEGIES: str = "momentum,mean_reversion,trend_following,composite"
    SCAN_INTERVAL_SECONDS: int = 300

    # ── Risk ──────────────────────────────────────────────────────────────
    MAX_POSITION_PCT: float = 0.10       # max 10% of portfolio per position
    MAX_POSITION_VALUE: float = 5000.0   # max $ per position
    MAX_TOTAL_EXPOSURE: float = 50000.0  # max total $ deployed
    MAX_DRAWDOWN_PCT: float = 15.0       # kill switch at -15% drawdown
    MIN_SIGNAL_SCORE: float = 1.0        # min composite score to trade

    # ── Execution ─────────────────────────────────────────────────────────
    DRY_RUN: bool = True
    MIN_ORDER_VALUE: float = 100.0    # minimum order size in $
    MAX_ORDER_VALUE: float = 2000.0   # maximum single order in $

    # ── State / logging ───────────────────────────────────────────────────
    STATE_DIR: str = "~/.schwab-agent"
    LOG_LEVEL: str = "INFO"

    # ── LLM (optional) ───────────────────────────────────────────────────
    LLM_ENABLED: bool = False
    OLLAMA_HOST: str = "http://localhost:11434"
    OLLAMA_MODEL: str = "qwen2.5:14b-instruct-q5_K_M"

    # ── Derived properties ────────────────────────────────────────────────

    @property
    def watchlist(self) -> list[str]:
        return [s.strip().upper() for s in self.WATCHLIST.split(",") if s.strip()]

    @property
    def strategies(self) -> list[str]:
        return [s.strip().lower() for s in self.STRATEGIES.split(",") if s.strip()]

    @property
    def dry_run(self) -> bool:
        return self.DRY_RUN

    @dry_run.setter
    def dry_run(self, value: bool) -> None:
        self.DRY_RUN = value

    @property
    def log_level(self) -> str:
        return self.LOG_LEVEL.upper()

    @property
    def state_dir(self) -> str:
        return self.STATE_DIR

    def validate(self) -> list[str]:
        """Return list of validation errors (empty = all good)."""
        errors = []
        if not self.SCHWAB_API_KEY:
            errors.append("SCHWAB_API_KEY is not set")
        if not self.SCHWAB_APP_SECRET:
            errors.append("SCHWAB_APP_SECRET is not set")
        if not self.watchlist:
            errors.append("WATCHLIST is empty")
        if self.MAX_POSITION_PCT <= 0 or self.MAX_POSITION_PCT > 1:
            errors.append("MAX_POSITION_PCT must be between 0 and 1")
        if self.MAX_DRAWDOWN_PCT <= 0 or self.MAX_DRAWDOWN_PCT > 100:
            errors.append("MAX_DRAWDOWN_PCT must be between 0 and 100")
        return errors
