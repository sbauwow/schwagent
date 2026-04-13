# schwagent Roadmap

Last updated: 2026-04-13

## Current State

15k+ LOC Python, 544 tests passing, 7 strategies, 5 Schwab accounts connected.

**What works today:**
- Dual Schwab API (Account + Market Data), OAuth2 enrolled
- 7 strategies: ETF Rotation, ETF Scalp, Momentum, Mean Reversion, Trend Following, Composite, Conviction Hold
- 31 CMT-grade technical indicators
- CFA-grade fundamental analysis (DCF, Graham, Piotroski, Altman, multi-factor)
- SEC EDGAR filing retrieval + LLM analysis
- ML feedback loop with auto-tuner (throttle → pause → restore)
- Dreamcycle (7-phase autonomous research loop)
- Backtesting with parameter sweep (800-run optimization)
- **Backtest validation** — Monte Carlo, Bootstrap Sharpe CI, Walk-Forward
- **Options analysis** — Black-Scholes pricing + Greeks, implied vol solver, multi-leg strategy builders (iron condor, spreads, straddles, butterflies, covered calls)
- **Portfolio optimization** — PyPortfolioOpt wrapper with max Sharpe, min volatility, hierarchical risk parity, efficient frontier, discrete share allocation, and pluggable returns/risk models (mean, EMA, CAPM, sample cov, Ledoit-Wolf, exp cov)
- **Technical indicators library** — `ta`-based wrapper with 35+ indicators (trend, momentum, volatility, volume) plus `apply_all()` for bulk feature engineering. Complements the existing `indicators.py` used by the built-in strategies
- WebSocket streaming, order fill tracking, cron scheduler
- Telegram bot (alerts, commands, trade approval gate)
- Multi-provider LLM (Ollama, Anthropic, OpenAI)
- **Intelligence layer (vibe-trading port):**
  - 23 reference skills with progressive disclosure (`./run.sh ref`)
  - Swarm multi-agent orchestration with 3 bundled presets (`./run.sh swarm`)
- Risk management (PDT, wash sale, position limits, drawdown kill)
- **Web dashboard** (FastAPI + vanilla JS) — accounts, positions, trades, P&L
- Point & Figure charting
- DRY_RUN=true default with per-strategy live toggles

**Known issues:**
- No earnings calendar avoidance
- No regime detection beyond SPY/SMA200 filter (all strategies run in most conditions)
- No cross-account portfolio view (5 accounts treated independently)
- No live options execution yet (options pricing, IV, and multi-leg strategy analysis exist; Schwab order placement for options not wired)

---

## Completed

- Phase 0: Core infrastructure (dual API, risk engine, trading rules, 5 accounts)
- Phase 1: Real-time data (WebSocket streaming, order fill tracking, cron scheduler)
- 7 strategies implemented and backtested
- Full analysis toolkit (31 indicators, fundamentals, SEC filings, LLM)
- Self-improvement loop (ML feedback, auto-tuner, dreamcycle)
- Telegram bot with trade approval
- **S1**: Test coverage 70 → 544 passing, failing `test_total_exposure_cap` fixed
- **S3**: Intermarket regime model — `intermarket.py`, 7-signal macro (SPY/TLT/HYG/GLD/IWM/DXY/VIX), regime classifier
- **S4**: Regime-aware strategy weighting wired into `runner.py`
- **S5**: Liquidity + dividend filters for scalp and rotation strategies
- **M2 → shipped early**: Portfolio optimizer (PyPortfolioOpt) — `portfolio_optimizer.py`, max Sharpe, min vol, HRP, efficient frontier
- **L1 → shipped early**: Web dashboard (FastAPI) — `web/`, accounts, positions, trades, P&L at `localhost:8898`
- Backtest validation (Monte Carlo, Bootstrap Sharpe CI, Walk-Forward)
- Options analysis (Black-Scholes, IV solver, multi-leg strategy builders)
- Technical indicators library (`ta_indicators.py`, 35+ indicators)
- Intelligence layer: 23 reference skills + swarm multi-agent presets

---

## Short Term — Make It Profitable (next 2 weeks)

*Focus: the agent has tools but no intelligence about WHEN to use them.
S1, S3, S4, S5 are done. S2 (earnings avoidance) is the only remaining near-term item.*

### S2. Earnings Calendar Avoidance
- [ ] Build earnings calendar from Schwab `fundamental.lastEarningsDate` + SEC 8-K
- [ ] Auto-reduce position size in 48h pre-earnings window
- [ ] Auto-skip new entries in earnings window
- [ ] Add earnings dates to Telegram daily digest
- [ ] **~150 lines** — `src/schwabagent/earnings.py`

### S6. GitHub Actions CI
- [ ] ruff + pytest on push/PR (carried over from S1)

---

## Medium Term — Portfolio Intelligence (1-3 months)

*Focus: treat 5 accounts as one portfolio, optimize sizing, add new edges.*

### M1. Unified Portfolio View
- [ ] Aggregate positions across all 5 accounts
- [ ] Net exposure per symbol, per sector, per asset class
- [ ] Account-type constraints:
  - HSA: no wash sale concern, long-term focus
  - Taxable: tax-lot awareness, wash sale tracking
  - IRA: growth-focused, no tax on rotation
- [ ] Per-account P&L + aggregate P&L
- [ ] **~300 lines** — `src/schwabagent/portfolio.py`

### M2. Portfolio Optimizer — runner integration
- [x] Core library shipped (`portfolio_optimizer.py`, PyPortfolioOpt: max Sharpe, min vol, HRP, efficient frontier)
- [ ] Integrate into runner: optimizer suggests target weights, strategies execute toward them
- [ ] Rebalance recommendations with trade list
- [ ] Constraints: max position size, sector limits, min diversification

### M3. Tax-Aware Execution
- [ ] Route trades to optimal account (HSA for active, taxable for long-term)
- [ ] Tax lot selection for sells (specific ID for tax-loss harvesting, FIFO default)
- [ ] Wash sale window tracking across all 5 accounts (31-day lookback)
- [ ] Year-end tax-loss harvesting sweep (identify losers, sell, replace with correlated ETF)
- [ ] Estimated tax impact per trade in Telegram approval messages
- [ ] **~250 lines** — update `trading_rules.py` + `runner.py`

### M4. Value / Fundamental Strategy
- [ ] Screen for cheap + high-quality stocks using existing factor model
- [ ] Entry: fundamental screen passes + technical confirmation (KAMA trend, RSI oversold)
- [ ] Hold: weeks to months, re-evaluate on earnings or factor score change
- [ ] Universe: S&P 500 stocks (use existing `data/sp500.csv`)
- [ ] Backtest against 2000-2026 data
- [ ] **~300 lines** — `src/schwabagent/strategies/value.py`

### M5. Options Overlay (Covered Calls)
- [ ] Start simple: covered call writing on ETF rotation holdings
- [ ] Strike selection: 0.30 delta, 30-45 DTE, minimum $0.50 premium
- [ ] Auto-roll when approaching expiration (7 DTE threshold)
- [ ] Track premium collected as income
- [ ] Greeks monitoring (delta, theta, gamma from Schwab options chain)
- [ ] Protective puts on large positions during Risk-Off regime
- [ ] **~500 lines** — `src/schwabagent/strategies/options_overlay.py`

### M6. Pairs Trading
- [ ] Cointegration-based pair selection from ETF universe
- [ ] Candidate pairs: XLF/KBE, XLE/OIH, QQQ/ARKK, GDX/GLD, TLT/IEF
- [ ] Z-score entry/exit with mean-reversion logic
- [ ] Market-neutral: dollar-neutral long/short
- [ ] **~400 lines** — `src/schwabagent/strategies/pairs.py`

---

## Long Term — Autonomous Fund (3-12 months)

*Focus: run this like a real portfolio with institutional-grade process.*

### L1. Web Dashboard — phase 2
- [x] Core dashboard shipped (`web/`, FastAPI, port 8898): accounts, positions, trades, P&L
- [ ] Risk dashboard: drawdown chart, exposure heatmap, regime indicator
- [ ] Backtest viewer with parameter sweep results
- [ ] SEC filing browser
- [ ] Point & Figure chart viewer
- [ ] Strategy config editor (adjust parameters without code changes)

### L2. Advanced Regime Intelligence
- [ ] Macro factor model (yield curve slope, credit spreads, ISM PMI, unemployment claims)
- [ ] Regime transition probabilities (hidden Markov model)
- [ ] Forward-looking regime signals (options skew, VIX term structure)
- [ ] Sector rotation model (early/mid/late cycle sector preferences)
- [ ] International regime signals (DXY, EM spreads, China PMI)

### L3. Smarter Self-Improvement
- [ ] Walk-forward optimization on live data (monthly recalibration of all strategy parameters)
- [ ] Strategy tournament: run all strategies in paper mode, promote top performers to live
- [ ] Genetic algorithm for strategy parameter evolution
- [ ] LLM-powered trade review: "Why did this trade lose? What signal was wrong?"
- [ ] Correlation analysis between strategies (reduce correlated bets)

### L4. Advanced Order Execution
- [ ] TWAP/VWAP for larger positions (>$5K)
- [ ] Limit order management: place limits, adjust, cancel stale
- [ ] Dark pool / alternative routing awareness (TDA vs Schwab routing quality)
- [ ] Slippage tracking: expected fill vs actual fill, per-strategy slippage cost

### L5. Risk Evolution
- [ ] VaR model (historical simulation across portfolio)
- [ ] Stress testing: 2008, 2020 March, 2022 bear scenarios
- [ ] Maximum correlation constraint (don't hold 5 correlated tech stocks)
- [ ] Tail risk hedging (auto-buy VIX calls or put spreads when risk-off)
- [ ] Intraday risk monitoring (not just scan-time)

### L6. Reporting & Compliance
- [ ] Daily performance email/Telegram with attribution (which strategy, which position drove P&L)
- [ ] Monthly tearsheet (Sharpe, Sortino, max drawdown, win rate, avg win/loss)
- [ ] Yearly tax report export (cost basis, realized gains/losses, wash sale adjustments)
- [ ] Trade journal: searchable log of every trade with rationale from LLM
- [ ] Benchmark comparison (vs SPY, vs 60/40)

---

## Strategy Pipeline (research before building)

*Evaluate these. Pull into medium term when backtests show edge.*

- [ ] **Sector momentum:** Monthly rotation across 11 SPDR sector ETFs based on 3/6/12 month returns
- [ ] **Dividend capture:** Buy before ex-date, sell after, repeat across high-yield universe
- [ ] **Volatility targeting:** Scale position sizes to target constant portfolio volatility (e.g., 12% annualized)
- [ ] **Gap trading:** Identify overnight gaps, trade mean-reversion in first 30 minutes
- [ ] **Seasonal patterns:** "Sell in May", Santa Claus rally, January effect — backtest and automate
- [ ] **LEAPS/Poor man's covered calls:** Use long-dated options instead of stock for capital efficiency
- [ ] **Iron condor selling:** On low-vol ETFs, sell monthly iron condors for theta decay

---

## What was considered and skipped

| Feature | Reason skipped |
|---------|----------------|
| Full hermes/prowler gateway | Too heavyweight — Telegram bot covers messaging |
| Browser-based scrapers | Fragile Selenium; SEC EDGAR API + Schwab fundamentals cover it |
| Crypto / Polymarket | Out of scope — separate agents handle those |
| Android automation | Not applicable |
| Full MCP server | Unnecessary complexity |
| yfinance dependency | Schwab API + SEC EDGAR cover all data needs |

---

## Design Principles

1. **DRY_RUN first** — every feature works in simulation before going live
2. **Regime awareness** — no strategy should run blind to macro conditions
3. **Account-aware** — every trade considers which of 5 accounts to use
4. **Self-correcting** — the feedback loop throttles, pauses, and restores automatically
5. **Telegram approval** — no live trade without human confirmation (until trust is earned)
6. **Tax efficiency** — route to the right account, harvest losses, avoid wash sales
