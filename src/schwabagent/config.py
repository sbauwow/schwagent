"""Configuration — loaded from .env via pydantic-settings."""
from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Config(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # ── Schwab credentials — Account/Trading API ───────────────────────
    SCHWAB_API_KEY: str = ""
    SCHWAB_APP_SECRET: str = ""
    SCHWAB_TOKEN_PATH: str = "~/.schwagent/token.json"
    SCHWAB_CALLBACK_URL: str = "https://127.0.0.1"
    # If empty, the agent will use the first account returned by the API
    SCHWAB_ACCOUNT_HASH: str = ""

    # ── Schwab credentials — Market Data API ─────────────────────────
    SCHWAB_MARKET_API_KEY: str = ""
    SCHWAB_MARKET_APP_SECRET: str = ""
    SCHWAB_MARKET_TOKEN_PATH: str = "~/.schwagent/market_token.json"
    SCHWAB_MARKET_CALLBACK_URL: str = "https://127.0.0.1"

    # ── Watchlist (monitoring — all symbols the agent tracks) ────────────
    WATCHLIST: str = (
        "AAPL,MSFT,GOOGL,AMZN,NVDA,META,TSLA,JPM,V,UNH,"
        "SPY,QQQ,IWM,DIA,EFA,EEM,TLT,IEF,HYG,TIP,GLD,VNQ,SHY,"
        "XLE,XOM,CVX,XLU,XLP,SLV,DBC"
    )
    STRATEGIES: str = "etf_rotation,momentum,mean_reversion,trend_following,composite,etf_scalp"
    SCAN_INTERVAL_SECONDS: int = 300

    # ── Per-strategy symbol lists ─────────────────────────────────────────
    MOMENTUM_SYMBOLS: str = "AAPL,MSFT,GOOGL,AMZN,NVDA,META,TSLA,JPM,V,UNH"
    MEAN_REVERSION_SYMBOLS: str = "AAPL,MSFT,GOOGL,AMZN,NVDA,META,TSLA,JPM,V,UNH"
    TREND_FOLLOWING_SYMBOLS: str = "AAPL,MSFT,GOOGL,AMZN,NVDA,META,TSLA,JPM,V,UNH,XLE,XOM,CVX"

    # ── ETF rotation strategy ─────────────────────────────────────────────
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

    # Default order type for every strategy that calls place_order without
    # an explicit override. "LIMIT" is safer — it caps slippage. "MARKET"
    # fills immediately at whatever the book shows.
    ORDER_TYPE: str = "LIMIT"
    # Basis-point buffer applied when auto-computing a limit price from a
    # live quote. Positive values are "aggressive" — BUY limit = ask + bps,
    # SELL limit = bid - bps, so a round trip costs (2 × bps) + spread. At
    # 25 bps (0.25%) on a $100 stock the buffer adds $0.25 each side, which
    # is enough to fill through normal spread wobble without giving away
    # meaningful edge. Raise for thin symbols; lower for tight liquid ETFs.
    LIMIT_PRICE_BUFFER_BPS: float = 25.0

    # Order time-in-force.
    #   DAY               = expires at today's session close
    #   GOOD_TILL_CANCEL  = stays live across sessions until filled or cancelled
    # DAY is the conservative default for strategy-driven trading — you want
    # today's signal acted on today, not three sessions from now when the
    # setup has decayed.
    ORDER_DURATION: str = "DAY"

    # Market session the order can route during.
    #   NORMAL    = regular trading hours only (09:30-16:00 ET)
    #   SEAMLESS  = regular + pre-market (04:00) + post-market (20:00 ET)
    # IMPORTANT: Schwab auto-cancels NORMAL/DAY orders submitted outside
    # regular hours, because there's no session left today to fill them in.
    # If you run `./run.sh live` overnight or after-hours, set this to
    # SEAMLESS so Schwab queues the order for the next reachable session.
    ORDER_SESSION: str = "NORMAL"

    # ── Per-strategy live trading toggle ──────────────────────────────
    # Each strategy must be explicitly enabled for live trading.
    # Even with DRY_RUN=false / --live, a strategy with its flag set to
    # false will remain in dry-run mode.
    LIVE_ETF_ROTATION: bool = False
    LIVE_MOMENTUM: bool = False
    LIVE_MEAN_REVERSION: bool = False
    LIVE_TREND_FOLLOWING: bool = False
    LIVE_COMPOSITE: bool = False
    LIVE_ETF_SCALP: bool = False
    LIVE_CONVICTION_HOLD: bool = False

    # ── Auto-tune (self-improvement loop) ────────────────────────────
    AUTOTUNE_ENABLED: bool = True
    AUTOTUNE_MIN_TRADES: int = 20           # min resolved trades before tuning
    AUTOTUNE_EVAL_WINDOW_DAYS: int = 14     # rolling window for evaluation
    AUTOTUNE_WIN_RATE_WARN: float = 40.0    # warn below this win rate %
    AUTOTUNE_WIN_RATE_PAUSE: float = 30.0   # pause strategy below this %
    AUTOTUNE_PROFIT_FACTOR_WARN: float = 1.0  # warn below this PF
    AUTOTUNE_PROFIT_FACTOR_PAUSE: float = 0.5 # pause below this PF
    AUTOTUNE_THROTTLE_FACTOR: float = 0.5   # scale sizing to this when warned
    AUTOTUNE_RECOVERY_WINDOW_DAYS: int = 7  # must sustain recovery for N days
    AUTOTUNE_SYMBOL_MAX_LOSS_STREAK: int = 5  # auto-exclude symbol after N consecutive losses

    # ── Account type ──────────────────────────────────────────────────
    # "margin" or "cash" — affects PDT rule enforcement
    ACCOUNT_TYPE: str = "margin"

    # ── Multi-account ────────────────────────────────────────────────
    # Account hash for the scalp strategy (separate from SCHWAB_ACCOUNT_HASH)
    # Leave empty to use the same account as other strategies
    SCALP_ACCOUNT_HASH: str = ""

    # ── Conviction Hold strategy ─────────────────────────────────────
    CONVICTION_SYMBOLS: str = "RKLB,LUNR,ASTS,RDW,MNTS,BKSY,PLTR,PATH,AI,BBAI,IONQ,RGTI,QUBT"
    CONVICTION_HOLD_DAYS: int = 30
    CONVICTION_MAX_POSITION: float = 10000.0

    # ── Liquidity & dividend filters ─────────────────────────────────
    SCALP_MIN_AVG_VOLUME: int = 1_000_000    # skip ETFs with <1M avg daily volume
    SCALP_MAX_SPREAD_PCT: float = 0.10       # skip ETFs with >0.10% spread
    ETF_DIVIDEND_LOOKFORWARD_DAYS: int = 3   # avoid buying within N days of ex-date

    # ── ETF Scalp strategy ───────────────────────────────────────────
    # Liquid broad ETFs only — no restricted issuer, no leveraged, no inverse
    SCALP_UNIVERSE: str = "SPY,QQQ,IWM,DIA,EFA,EEM,TLT,IEF,GLD,VNQ"
    SCALP_INTERVAL_MINUTES: int = 3           # bar size for signals
    SCALP_TAKE_PROFIT_PCT: float = 0.15       # +0.15% take profit
    SCALP_STOP_LOSS_PCT: float = 0.10         # -0.10% stop loss
    SCALP_TIME_STOP_MINUTES: int = 30         # close if no exit hit in N min
    SCALP_SESSION_START: str = "09:33"        # skip first 3 min of chaos
    SCALP_SESSION_END: str = "15:45"          # close all 15 min before close
    SCALP_TRANCHES: int = 5                   # split capital into N tranches
    SCALP_MAX_POSITIONS: int = 3              # max simultaneous open positions
    SCALP_VOLUME_SPIKE_MULT: float = 2.0      # volume > N × 20-bar avg
    SCALP_LOOKBACK_BARS: int = 3              # breakout lookback (prior N bars)
    SCALP_EMA_FAST: int = 9                   # fast EMA for trend filter
    SCALP_EMA_SLOW: int = 21                  # slow EMA for trend filter
    SCALP_SCAN_INTERVAL_SECONDS: int = 15     # how often to check for entries

    # ── Intermarket regime detection ──────────────────────────────────────
    REGIME_ENABLED: bool = True
    REGIME_REFERENCE_SYMBOLS: str = "SPY,TLT,HYG,GLD,IWM,UUP,VIXY"

    # ── State / logging ───────────────────────────────────────────────────
    STATE_DIR: str = "~/.schwagent"
    LOG_LEVEL: str = "INFO"

    # ── Telegram ──────────────────────────────────────────────────────────
    TELEGRAM_ENABLED: bool = False
    TELEGRAM_BOT_TOKEN: str = ""
    TELEGRAM_CHAT_ID: str = ""           # your user/group chat ID
    TELEGRAM_REQUIRE_APPROVAL: bool = True  # require approval for live trades
    TELEGRAM_APPROVAL_TIMEOUT: int = 300    # seconds to wait for approval

    # ── LLM (optional) ───────────────────────────────────────────────────
    LLM_ENABLED: bool = False
    # Provider: "ollama" (local), "anthropic" (Claude), "openai" (GPT/compatible)
    LLM_PROVIDER: str = "ollama"
    LLM_MODEL: str = ""              # override model (empty = provider default)
    LLM_API_KEY: str = ""            # for anthropic/openai
    LLM_BASE_URL: str = ""           # override base URL
    LLM_TEMPERATURE: float = 0.2
    LLM_MAX_TOKENS: int = 1024
    LLM_TIMEOUT: int = 60
    # Legacy Ollama-specific (still work, overridden by LLM_* if set)
    OLLAMA_HOST: str = "http://localhost:11434"
    OLLAMA_MODEL: str = "qwen2.5:14b-instruct-q5_K_M"
    OLLAMA_TIMEOUT: int = 60
    # Provider-specific API keys (fallbacks if LLM_API_KEY not set)
    ANTHROPIC_API_KEY: str = ""
    OPENAI_API_KEY: str = ""

    # ── Derived properties ────────────────────────────────────────────────

    @property
    def watchlist(self) -> list[str]:
        return [s.strip().upper() for s in self.WATCHLIST.split(",") if s.strip()]

    @property
    def momentum_symbols(self) -> list[str]:
        return [s.strip().upper() for s in self.MOMENTUM_SYMBOLS.split(",") if s.strip()]

    @property
    def mean_reversion_symbols(self) -> list[str]:
        return [s.strip().upper() for s in self.MEAN_REVERSION_SYMBOLS.split(",") if s.strip()]

    @property
    def trend_following_symbols(self) -> list[str]:
        return [s.strip().upper() for s in self.TREND_FOLLOWING_SYMBOLS.split(",") if s.strip()]

    @property
    def regime_reference_symbols(self) -> list[str]:
        return [s.strip().upper() for s in self.REGIME_REFERENCE_SYMBOLS.split(",") if s.strip()]

    @property
    def all_symbols(self) -> list[str]:
        """All unique symbols across watchlist and all strategy universes."""
        syms = set(self.watchlist)
        syms.update(self.momentum_symbols)
        syms.update(self.mean_reversion_symbols)
        syms.update(self.trend_following_symbols)
        syms.update(self.etf_universe)
        syms.update(self.scalp_universe)
        syms.update(self.conviction_symbols)
        if self.REGIME_ENABLED:
            syms.update(self.regime_reference_symbols)
        return sorted(syms)

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

    _STRATEGY_LIVE_FLAGS: dict[str, str] = {
        "etf_rotation": "LIVE_ETF_ROTATION",
        "momentum": "LIVE_MOMENTUM",
        "mean_reversion": "LIVE_MEAN_REVERSION",
        "trend_following": "LIVE_TREND_FOLLOWING",
        "composite": "LIVE_COMPOSITE",
        "etf_scalp": "LIVE_ETF_SCALP",
        "conviction_hold": "LIVE_CONVICTION_HOLD",
    }

    @property
    def conviction_symbols(self) -> list[str]:
        return [s.strip().upper() for s in self.CONVICTION_SYMBOLS.split(",") if s.strip()]

    @property
    def scalp_universe(self) -> list[str]:
        blocked = self.etf_blocklist
        return [
            s.strip().upper()
            for s in self.SCALP_UNIVERSE.split(",")
            if s.strip() and s.strip().upper() not in blocked
        ]

    def is_strategy_live(self, strategy_name: str) -> bool:
        """Check if a strategy is enabled for live trading.

        Returns True only when BOTH:
        1. Global DRY_RUN is False (i.e. --live mode)
        2. The per-strategy LIVE_<name> flag is True
        """
        if self.DRY_RUN:
            return False
        attr = self._STRATEGY_LIVE_FLAGS.get(strategy_name)
        if attr is None:
            return False
        return bool(getattr(self, attr, False))

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
