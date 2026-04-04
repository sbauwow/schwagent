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
    # Callback URL registered in Schwab developer portal (must match exactly)
    SCHWAB_CALLBACK_URL: str = "https://127.0.0.1"
    # If empty, the agent will use the first account returned by the API
    SCHWAB_ACCOUNT_HASH: str = ""

    # ── Watchlist / strategies ────────────────────────────────────────────
    WATCHLIST: str = "AAPL,MSFT,GOOGL,AMZN,NVDA,META,TSLA,JPM,V,UNH"
    STRATEGIES: str = "etf_rotation,momentum,mean_reversion,trend_following,composite"
    SCAN_INTERVAL_SECONDS: int = 300

    # ── ETF rotation strategy ─────────────────────────────────────────────
    # Comma-separated ETF universe for the rotation strategy
    ETF_UNIVERSE: str = "SPY,QQQ,IWM,EFA,EEM,TLT,IEF,HYG,TIP,GLD,VNQ,SHY"
    # Hold top N ETFs at any time
    ETF_TOP_N: int = 3
    # Lookback periods (months) used for momentum scoring
    ETF_MOMENTUM_PERIODS: str = "1,3,6,12"
    # If SPY is below its SMA200, move to safe-haven ETF
    ETF_SAFE_HAVEN: str = "SHY"
    # Bear market filter: True = enable SPY/SMA200 check
    ETF_BEAR_FILTER: bool = True
    # ETFs to permanently exclude regardless of universe setting
    # Includes all restricted issuer ETFs by default
    ETF_BLOCKLIST: str = "MINT,LDUR,SMUR,HYIN,ZROZ,BOND,PDBC,HYLS,LOWV,EMPW,MUNI,INFU,PFFD,REGL"

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
    OLLAMA_TIMEOUT: int = 60

    # ── Derived properties ────────────────────────────────────────────────

    @property
    def watchlist(self) -> list[str]:
        return [s.strip().upper() for s in self.WATCHLIST.split(",") if s.strip()]

    @property
    def strategies(self) -> list[str]:
        return [s.strip().lower() for s in self.STRATEGIES.split(",") if s.strip()]

    @property
    def etf_blocklist(self) -> set[str]:
        return {s.strip().upper() for s in self.ETF_BLOCKLIST.split(",") if s.strip()}

    @property
    def etf_universe(self) -> list[str]:
        blocked = self.etf_blocklist
        return [
            s.strip().upper()
            for s in self.ETF_UNIVERSE.split(",")
            if s.strip() and s.strip().upper() not in blocked
        ]

    @property
    def etf_momentum_periods(self) -> list[int]:
        return [int(p.strip()) for p in self.ETF_MOMENTUM_PERIODS.split(",") if p.strip()]

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
