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
    LIVE_BROWN_MOMENTUM: bool = False
    LIVE_TICK_BREADTH: bool = False
    LIVE_AH_SNIPER: bool = False
    LIVE_THETA: bool = False

    # ── Tick Breadth strategy ────────────────────────────────────────
    # Intraday $TICK-vs-price non-confirmation divergence. See
    # src/schwabagent/breadth.py and strategies/tick_breadth.py for the
    # signal math. Close-only SELLs (long-only), one position per
    # underlying, LIMIT orders via the shared order-routing config.
    #
    # Pairs are specified as "UNDERLYING:TICK" strings, comma separated.
    # TICK_PAIRS are traded; TICK_OBSERVE_PAIRS are logged signal-only
    # so you can evaluate new pairs before committing capital.
    TICK_PAIRS: str = "SPY:$TICK"
    TICK_OBSERVE_PAIRS: str = ""  # e.g. "QQQ:$TICKQ" when Nasdaq breadth is available
    TICK_SCAN_INTERVAL_SECONDS: int = 60
    # Minimum |TICK| for an extreme to count. Small values aren't meaningful.
    # 500 is conventional "first extreme zone", 800-1000 is "strong".
    TICK_EXTREME_THRESHOLD: float = 500.0
    # How close to its own daily extreme the underlying must be to "confirm"
    # a TICK extreme. 5 bps = 0.05% — tight enough that normal spread wobble
    # doesn't fake-confirm a weak move.
    TICK_CONFIRMATION_BPS: float = 5.0
    # Minutes between entries — prevents firing on every new TICK print.
    TICK_ENTRY_COOLDOWN_MIN: float = 10.0
    # Exit percentages — asymmetric because this is a reversal play, not trend.
    TICK_TAKE_PROFIT_PCT: float = 0.30
    TICK_STOP_LOSS_PCT: float = 0.15
    # Force-close any open position at this ET time (HH:MM, 24h). No overnight.
    TICK_CLOSE_BY_ET: str = "15:45"
    # Bootstrap running high/low from today's daily $TICK bar at session start.
    # If false, running state begins at whatever the first poll sees, which
    # misses any extreme that happened before the loop started.
    TICK_BOOTSTRAP_FROM_DAILY: bool = True

    # ── Brown Momentum strategy ──────────────────────────────────────
    # Constance Brown's RSI range-shift + Composite Index divergence +
    # Derivative Oscillator combo. See src/schwabagent/brown_indicators.py
    # and src/schwabagent/strategies/brown_momentum.py for the math and
    # signal classification.
    BROWN_MOMENTUM_SYMBOLS: str = (
        "AAPL,MSFT,GOOGL,AMZN,NVDA,META,TSLA,JPM,V,UNH,"
        "SPY,QQQ,IWM,TLT,GLD"
    )
    # Wilder RSI period used by all Brown indicators. Brown uses 14.
    BROWN_RSI_PERIOD: int = 14
    # Lookback window for RSI range-shift floor/ceiling detection.
    # 60 bars (~3 months of daily data) is a Brown-recommended default.
    BROWN_REGIME_LOOKBACK: int = 60
    # Lookback for divergence swing comparison on Composite Index.
    BROWN_DIVERGENCE_LOOKBACK: int = 40

    # ── After-hours sniper strategy ──────────────────────────────────
    # Places deep-discount LIMIT buy orders in the after-hours session
    # (16:00-20:00 ET) at prices N × ATR below the RTH close. Thin AH
    # books + occasional forced prints = occasional great fills. Orders
    # are cancelled before the AH session closes, and any fill is
    # exited the next trading day via TP/SL or a session-close time
    # stop. See src/schwabagent/strategies/ah_sniper.py.
    AH_SNIPER_SYMBOLS: str = (
        "AAPL,MSFT,GOOGL,AMZN,NVDA,META,TSLA,SPY,QQQ,IWM"
    )
    AH_SNIPER_ATR_PERIOD: int = 14
    AH_SNIPER_ATR_MULT: float = 2.0       # limit = close − N × ATR
    AH_SNIPER_MIN_OFFSET_PCT: float = 3.0  # floor: never snipe closer than this
    AH_SNIPER_MAX_OFFSET_PCT: float = 15.0  # ceiling: never snipe deeper than this
    AH_SNIPER_POSITION_USD: float = 1000.0  # fixed $ per ticker per snipe
    AH_SNIPER_MAX_SYMBOLS: int = 10        # hard cap on simultaneous snipes
    AH_SNIPER_START_ET: str = "16:05"      # earliest ET time to place snipes
    AH_SNIPER_CANCEL_ET: str = "19:55"     # cancel any still-open snipes after this ET
    AH_SNIPER_TAKE_PROFIT_PCT: float = 3.0  # exit filled snipe at +N% next RTH
    AH_SNIPER_STOP_LOSS_PCT: float = 5.0    # exit filled snipe at −N% next RTH
    AH_SNIPER_EXIT_BY_ET: str = "15:45"    # force-flatten next RTH at this ET

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

    # ── Auto-research pipeline ───────────────────────────────────────
    # Automated backtest + validation + LLM critique across every
    # backtestable strategy. See src/schwabagent/autoresearch.py.
    AUTORESEARCH_PERIOD_YEARS: int = 3
    AUTORESEARCH_DATA_PATH: str = "data/sp500_stocks.csv"
    AUTORESEARCH_BASELINE: str = "SPY"
    AUTORESEARCH_LLM_ENABLED: bool = True
    AUTORESEARCH_MONTE_CARLO_SIMS: int = 500
    AUTORESEARCH_BOOTSTRAP_ITERATIONS: int = 500
    AUTORESEARCH_WALK_FORWARD_WINDOWS: int = 5
    AUTORESEARCH_TELEGRAM_DIGEST: bool = False

    # ── Quant research feed (dreamcycle RESEARCH_PAPERS phase) ───────
    # Scrapes public Atom/RSS feeds from quant-finance journals and
    # practitioner blogs, scores items against active strategies and
    # symbol universes, and forwards the top-N to Telegram once per
    # dreamcycle. See src/schwabagent/scrapers/quant_research.py.
    QUANT_RESEARCH_ENABLED: bool = False
    # Comma-separated subset of supported sources. Any of:
    #   arxiv, ssrn, paperswithbacktest, alpha_architect, hudsonthames
    QUANT_RESEARCH_SOURCES: str = (
        "arxiv,paperswithbacktest,alpha_architect,hudsonthames"
    )
    # Max items pulled per feed per cycle. Dedup then trims further.
    QUANT_RESEARCH_MAX_PER_SOURCE: int = 30
    # Relevance score floor for the Telegram digest. Keyword weights in
    # quant_research._KEYWORDS; a single "momentum" hit = 1.0.
    QUANT_RESEARCH_MIN_RELEVANCE: float = 1.0
    # Top-N rows surfaced in the Telegram digest each cycle.
    QUANT_RESEARCH_DIGEST_TOP_N: int = 5
    # If true + LLM_ENABLED, generate a one-sentence summary for each
    # of the top-N before persisting. Off by default (saves tokens).
    QUANT_RESEARCH_LLM_SUMMARIES: bool = False
    # Prune rows older than this by fetched_at. 180 days = ~6 months
    # of paper history, enough for backtest idea recall without the
    # SQLite file bloating.
    QUANT_RESEARCH_RETENTION_DAYS: int = 180

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

    # ── Theta / wheel strategy ───────────────────────────────────────
    # Sells cash-secured puts, rolls into covered calls on assignment.
    # Universe should be tickers you're genuinely willing to own.
    THETA_SYMBOLS: str = "SPY,QQQ,IWM"
    THETA_DTE_MIN: int = 30               # earliest expiry window
    THETA_DTE_MAX: int = 45               # latest expiry window
    THETA_TARGET_DELTA: float = 0.25      # ~25 delta short
    THETA_DELTA_TOLERANCE: float = 0.08   # accept [target-tol, target+tol]
    THETA_PROFIT_TAKE_PCT: float = 0.50   # buy-to-close when 50% of credit captured
    THETA_DEFENSIVE_DTE: int = 7          # close at ≤7 DTE regardless of P&L
    THETA_MIN_CREDIT: float = 0.30        # skip contracts with bid < $0.30
    THETA_MIN_OPEN_INTEREST: int = 100    # liquidity floor
    THETA_MAX_CONTRACTS_PER_SYMBOL: int = 1  # hard cap on concurrent contracts
    THETA_LIMIT_BUFFER_PCT: float = 0.05  # STO at mid - 5% of width (give up a bit for fill)

    # ── Cheap-gamma scanner (scan-only) ──────────────────────────────
    # Surfaces long-straddle candidates where implied vol is cheap vs
    # recent realized vol. Never places orders.
    GAMMA_SCANNER_SYMBOLS: str = "SPY,QQQ,IWM,AAPL,MSFT,NVDA,META,GOOGL,AMZN,TSLA"
    GAMMA_SCANNER_DTE_MIN: int = 21
    GAMMA_SCANNER_DTE_MAX: int = 45
    GAMMA_SCANNER_RV_WINDOW: int = 20        # trailing days for realized vol
    GAMMA_SCANNER_MAX_RATIO: float = 1.0     # IV/RV must be below this
    GAMMA_SCANNER_MIN_OI: int = 100          # per-leg open interest floor

    # ── Covered call screener (scan-only) ────────────────────────────────
    # Buy dividend-paying stock + write an OTM call >30 DTE out. Ranks by
    # annualized if-called yield + captured dividend during the hold.
    COVERED_CALL_SYMBOLS: str = (
        "KO,PG,JNJ,VZ,XOM,CVX,MO,T,IBM,PFE,ABBV,MRK,PEP,MMM,KMB,"
        "SO,DUK,HD,LMT,CAT,MCD,BMY,GIS,TGT,WBA"
    )
    COVERED_CALL_DTE_MIN: int = 30
    COVERED_CALL_DTE_MAX: int = 60
    COVERED_CALL_TARGET_OTM_PCT: float = 5.0    # pick strike ~N% above spot
    COVERED_CALL_MIN_DIV_YIELD_PCT: float = 1.5 # skip non-dividend payers
    COVERED_CALL_MIN_OI: int = 100              # liquidity floor
    COVERED_CALL_MAX_SPREAD_PCT: float = 10.0   # bid/ask spread as % of mid
    COVERED_CALL_MIN_ANNUAL_YIELD_PCT: float = 8.0  # total annualized yield cutoff
    COVERED_CALL_TOP_N: int = 20
    # Per-contract notional cap — filter opportunities where 100*spot would
    # bust the account's MAX_POSITION_VALUE before ranking. A $300 stock =
    # $30k per contract, too big for a $50k account under a 10% position cap.
    COVERED_CALL_MAX_SPOT: float = 250.0
    # Live-execution switch. Off by default — scan-only until flipped.
    LIVE_COVERED_CALL_SCREENER: bool = False

    # ── Unusual options activity scanner (scan-only) ───────────────────
    # Flags option contracts where today's volume exceeds open interest
    # by a configurable multiple. High vol/OI = new money flooding in,
    # potential informed flow. Never places orders.
    UNUSUAL_ACTIVITY_SYMBOLS: str = (
        # Mega-cap + high options volume
        "SPY,QQQ,IWM,DIA,AAPL,MSFT,NVDA,META,GOOGL,AMZN,TSLA,"
        # Semis / AI
        "AMD,SMCI,ARM,MRVL,AVGO,MU,INTC,"
        # Growth / momentum
        "NFLX,CRM,SNOW,PLTR,COIN,SHOP,SQ,UBER,ABNB,"
        # Financials
        "JPM,GS,BAC,C,MS,"
        # Energy
        "XOM,CVX,OXY,"
        # Sector ETFs
        "XLE,XLF,XLK,XLV,XLI,XLP,XLU,XLY,XLRE,XBI,"
        # Volatility / rates / commodities
        "TLT,GLD,SLV,USO,HYG,UVXY"
    )
    UNUSUAL_ACTIVITY_DTE_MIN: int = 1
    UNUSUAL_ACTIVITY_DTE_MAX: int = 45
    UNUSUAL_ACTIVITY_STRIKE_COUNT: int = 30
    UNUSUAL_ACTIVITY_MIN_RATIO: float = 5.0      # vol/OI >= 5.0 (500%)
    UNUSUAL_ACTIVITY_MIN_VOLUME: int = 500        # skip low-volume noise
    UNUSUAL_ACTIVITY_TOP_N: int = 25
    UNUSUAL_ACTIVITY_VOLUME_PREFILTER: float = 0.5  # equity vol must be >= N × 10d avg

    # ── Cash sweep (SWVXX / money market) ────────────────────────────
    # Manual /sweep telegram command moves idle cash into a money market
    # mutual fund. SWVXX is Schwab's prime money fund (NAV $1.00, ~4.5% yield).
    SWEEP_SYMBOL: str = "SWVXX"
    SWEEP_CASH_BUFFER: float = 1000.0        # dollars to leave unsweeped
    SWEEP_MIN_AMOUNT: float = 100.0          # skip sweep if excess < this
    # ETF sweep — SGOV trades intraday, uses equity order flow
    SWEEP_ETF_SYMBOL: str = "SGOV"

    # ── Liquidity & dividend filters ─────────────────────────────────
    SCALP_MIN_AVG_VOLUME: int = 1_000_000    # skip ETFs with <1M avg daily volume
    SCALP_MAX_SPREAD_PCT: float = 0.10       # skip ETFs with >0.10% spread
    ETF_DIVIDEND_LOOKFORWARD_DAYS: int = 3   # avoid buying within N days of ex-date

    # ── Event blackout gate (earnings / ex-dividend) ─────────────────
    # Uses cached calendars from scrapers/earnings_calendar + dividend_calendar.
    # Only affects BUYs. Missing cache fails open (warn + allow) so a scraper
    # outage cannot strand the bot.
    EARNINGS_BLACKOUT_ENABLED: bool = True
    EARNINGS_BLACKOUT_DAYS: int = 2          # block/warn if earnings within N days
    EARNINGS_BLACKOUT_MODE: str = "warn"     # "warn" | "block"
    DIVIDEND_BLACKOUT_ENABLED: bool = False  # off by default — etf_rotation has its own
    DIVIDEND_BLACKOUT_DAYS: int = 1
    DIVIDEND_BLACKOUT_MODE: str = "warn"     # "warn" | "block"

    # ── ETF Scalp strategy ───────────────────────────────────────────
    # Liquid broad ETFs only — no leveraged or inverse products
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
    REGIME_REFERENCE_SYMBOLS: str = "SPY,TLT,HYG,GLD,IWM,UUP,VIXY,$TNX,$TYX,$FVX,$IRX"

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
        syms.update(self.theta_symbols)
        syms.update(self.gamma_scanner_symbols)
        syms.update(self.unusual_activity_symbols)
        syms.update(self.brown_momentum_symbols)
        syms.update(self.ah_sniper_symbols)
        if self.REGIME_ENABLED:
            syms.update(self.regime_reference_symbols)
        return sorted(syms)

    @property
    def strategies(self) -> list[str]:
        return [s.strip().lower() for s in self.STRATEGIES.split(",") if s.strip()]

    @property
    def etf_universe(self) -> list[str]:
        return [s.strip().upper() for s in self.ETF_UNIVERSE.split(",") if s.strip()]

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
        "brown_momentum": "LIVE_BROWN_MOMENTUM",
        "tick_breadth": "LIVE_TICK_BREADTH",
        "ah_sniper": "LIVE_AH_SNIPER",
        "theta": "LIVE_THETA",
        "covered_call_screener": "LIVE_COVERED_CALL_SCREENER",
    }

    @property
    def ah_sniper_symbols(self) -> list[str]:
        return [s.strip().upper() for s in self.AH_SNIPER_SYMBOLS.split(",") if s.strip()]

    @property
    def brown_momentum_symbols(self) -> list[str]:
        return [s.strip().upper() for s in self.BROWN_MOMENTUM_SYMBOLS.split(",") if s.strip()]

    @property
    def tick_pairs_traded(self) -> list[str]:
        """Parsed TICK_PAIRS specs, traded."""
        return [s.strip() for s in self.TICK_PAIRS.split(",") if s.strip()]

    @property
    def tick_pairs_observed(self) -> list[str]:
        """Parsed TICK_OBSERVE_PAIRS specs, signal-only."""
        return [s.strip() for s in self.TICK_OBSERVE_PAIRS.split(",") if s.strip()]

    @property
    def conviction_symbols(self) -> list[str]:
        return [s.strip().upper() for s in self.CONVICTION_SYMBOLS.split(",") if s.strip()]

    @property
    def theta_symbols(self) -> list[str]:
        return [s.strip().upper() for s in self.THETA_SYMBOLS.split(",") if s.strip()]

    @property
    def gamma_scanner_symbols(self) -> list[str]:
        return [s.strip().upper() for s in self.GAMMA_SCANNER_SYMBOLS.split(",") if s.strip()]

    @property
    def unusual_activity_symbols(self) -> list[str]:
        return [s.strip().upper() for s in self.UNUSUAL_ACTIVITY_SYMBOLS.split(",") if s.strip()]

    @property
    def covered_call_symbols(self) -> list[str]:
        return [s.strip().upper() for s in self.COVERED_CALL_SYMBOLS.split(",") if s.strip()]

    @property
    def scalp_universe(self) -> list[str]:
        return [s.strip().upper() for s in self.SCALP_UNIVERSE.split(",") if s.strip()]

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
