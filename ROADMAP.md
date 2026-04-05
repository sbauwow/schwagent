# schwab-agent Roadmap

Ranked by ROI — highest value, lowest effort first.

---

## Completed

### Core infrastructure
- **Dual Schwab API architecture** — separate OAuth apps for Account (trading) and Market Data APIs
- **Per-strategy live trading toggles** — two-layer safety: global `DRY_RUN` + per-strategy `LIVE_<name>` flags
- **Trading rules engine** — auto-detects account type (CASH/MARGIN) from API, enforces PDT, closing-only, wash sale
- **Multi-account support** — 5 accounts connected (1 cash HSA, 4 margin), scalp strategy targets separate account
- **API rate limiting** — sliding window throttle (120 req/60s) on all Schwab API calls
- **Price anomaly detection** — flags >15% deviations from rolling average
- **Position reconciliation** — compares expected vs actual positions from Schwab API

### Strategies (6)
- **ETF Rotation** — dual momentum across 12 ETFs, bear-market filter (SPY < SMA200), LLM overlay
- **ETF Scalp** — intraday 3-min bar volume/price breakout, tight TP/SL, settlement-aware tranches
- **Momentum** — SMA(20/50) + RSI(14) + MACD
- **Mean Reversion** — Bollinger Bands + RSI + z-score
- **Trend Following** — EMA(20/50/200) + ADX(35), parameter-optimized via 800-backtest sweep
- **Composite** — multi-strategy consensus averaging
- **Strategy template** — `TEMPLATE.py` with 10-question design checklist

### Analysis & research
- **31 CMT-grade technical indicators** — KAMA, KST, Ichimoku, Keltner/Donchian, Stochastic, CCI, Elder Ray, Aroon, Chandelier/SAR, Fibonacci, pivots, OBV, CMF, MFI, VWAP, divergence detection
- **CFA-grade fundamental analysis** — DCF, Graham Number, PEG, Altman Z-Score, Piotroski F-Score, ROIC, ROE, ROA, earnings quality, margin stability, multi-factor model (Value+Quality+Momentum)
- **SEC EDGAR filing tool** — retrieve 10-K/10-Q/8-K filings, LLM-powered analysis (summary, risks, sentiment, insights), compare consecutive filings, multi-symbol 8-K scan
- **Point & Figure charting** — pypf integration powered by Schwab market data
- **Backtesting framework** — walk-forward simulation using S&P 500 historical data (2000–2026), parameter sweep capability

### Self-improvement loop
- **ML feedback loop** — records every signal to SQLite, links trade outcomes, per-strategy calibration (win rate, profit factor by signal type), drift detection
- **Auto-tuner** — throttle (50% sizing) → pause → restore cycle based on rolling performance. Auto-excludes symbols with 5+ consecutive losses. Telegram alerts on all state changes.
- **Dreamcycle** — 7-phase autonomous background loop: scan, calibrate, research, reconcile, digest, improve, cleanup

### Interface
- **Telegram bot** — alerts (trades, kill switch, errors), commands (/status /pnl /positions /kill /resume), inline-button trade approval
- **Skills framework** — declarative SKILL.md files, 9 starter skills across analysis/trading/research
- **Schwab API reference** — full field-level documentation of every endpoint response

---

## Phase 1 — Real-time data

### 1. WebSocket streaming
**Why:** The scalp strategy polls quotes every 15 seconds. At 0.15% targets, that's too slow. Streaming gives real-time tick data for instant TP/SL triggers.

Use `streamerInfo` from Schwab user preferences (`wss://streamer-api.schwab.com/ws`).

**~400 lines** — `src/schwabagent/streaming.py`

### 2. Order fill tracking
**Why:** We place market orders and assume the fill price equals the quoted price. Actual fills can differ, making TP/SL inaccurate.

Poll `GET /accounts/{hash}/orders` after placement, update entry price, recalculate exit levels.

**~150 lines** — update `schwab_client.py` + `strategies/etf_scalp.py`

### 3. Cron scheduler
**Why:** Replace manual `./run.sh loop` with persistent scheduled jobs. Survives restarts.

```
09:30  weekdays  → scalp starts
09:35  weekdays  → daily scan + execute
15:00  weekdays  → ETF rotation check
15:45  weekdays  → scalp session close
16:00  weekdays  → daily P&L → Telegram
08:00  monday    → weekly report + dreamcycle
```

**~300 lines** — `src/schwabagent/scheduler.py`

---

## Phase 2 — Intelligence

### 4. Earnings calendar
**Why:** Earnings reports cause 5-10% overnight moves. Auto-reduce or skip in 48-hour window.

Use Schwab `fundamental.lastEarningsDate` + SEC 8-K scan for confirmed dates.

**~150 lines** — `src/schwabagent/earnings.py`

### 5. Intermarket regime model
**Why:** Backtest proved no single strategy works in all regimes. The agent needs to detect the regime and weight strategies accordingly.

7-signal macro model from cross-asset reference symbols (SPY, TLT, HYG, GLD, IWM). Port from rebalancer's `intermarket.py`.

**~400 lines** — `src/schwabagent/intermarket.py`

### 6. Regime-aware strategy weighting
**Why:** In stagflation, trend following should dominate. In bull markets, momentum. The auto-tuner handles individual strategy tuning, but regime weighting handles the portfolio level.

Map regime → strategy weight overrides. Feed into the runner's strategy execution order and position sizing.

**~200 lines** — update `runner.py` + `config.py`

### 7. Liquidity + dividend filters
**Why:** Scalp needs tight spreads (skip illiquid ETFs). ETF rotation needs dividend awareness (ex-date timing).

Use `fundamental.avg10DaysVolume` and `fundamental.nextDivExDate` from quotes.

**~100 lines** — integrate into scalp + ETF rotation scans

---

## Phase 3 — Portfolio optimization

### 8. Unified portfolio view
**Why:** 5 accounts should be treated as one portfolio. Concentration, correlation, and risk need to be measured across accounts.

Aggregate positions across all accounts. Per-account constraints (cash vs margin, tax-advantaged vs taxable). Account-aware execution routing.

**~300 lines** — `src/schwabagent/portfolio.py`

### 9. Portfolio optimizer (MPT)
**Why:** Position sizing is naive (equal weight or fixed dollar). Mean-variance optimization improves risk-adjusted returns.

Max Sharpe, Min Volatility, Efficient Risk, HRP strategies. Uses `pypfopt`.

**~300 lines** — `src/schwabagent/optimizer.py`

### 10. Tax-aware execution
**Why:** HSA = no tax events. Taxable = wash sale risk, lot selection matters. IRA = growth-focused, no tax on rotation.

Route trades to the optimal account. Select tax lots (FIFO, specific ID) for sells. Track wash sale windows across accounts.

**~250 lines** — update `trading_rules.py` + `runner.py`

---

## Phase 4 — Advanced strategies

### 11. Value / fundamental strategy
**Why:** We have DCF, Graham Number, Piotroski, factor model — but no strategy uses them. A fundamental screen + technical entry would combine both lenses.

Screen for cheap + high-quality stocks (factor model), then enter on technical confirmation (KAMA trend, RSI oversold). Hold for weeks/months.

**~300 lines** — `src/schwabagent/strategies/value.py`

### 12. Options overlay
**Why:** Schwab returns full options chain with greeks. Covered calls on held positions generate income, protective puts limit downside.

Start with covered call writing on ETF rotation holdings. Use delta/theta to select strikes.

**~500 lines** — `src/schwabagent/strategies/options_overlay.py`

### 13. Pairs / spread trading
**Why:** Market-neutral strategy profiting from relative moves between correlated ETFs.

Cointegration-based entry, mean-reversion exit. Lower correlation to market direction.

**~400 lines** — `src/schwabagent/strategies/pairs.py`

---

## Phase 5 — Interface

### 14. Web dashboard
FastAPI + Jinja2 at `http://localhost:5000`.

Pages: portfolio overview, ETF rankings, signals, scalp positions, P&L, risk state, P&F charts, SEC filings, feedback/calibration.

**~500 lines** — `src/schwabagent/web/`

### 15. Telegram UX polish
Wire `request_approval()` into all live strategies. Add inline position management (close, adjust TP/SL). Daily digest formatting. P&F chart images via Telegram.

**~200 lines** — update strategies + `telegram.py`

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

## Dependency additions by phase

| Phase | New dependencies |
|-------|-----------------|
| 1 — Streaming | schwab-py streaming (built-in) |
| 1 — Scheduler | `croniter` |
| 3 — Optimizer | `pypfopt` |
| 5 — Dashboard | `jinja2` (fastapi/uvicorn already in deps) |
