# schwab-agent Roadmap

Ranked by ROI — highest value, lowest effort first.

---

## Completed

### Dual Schwab API architecture
Separate OAuth apps for Account (trading) and Market Data APIs. Each has independent credentials, tokens, and enrollment flow.

### Per-strategy live trading toggles
Two-layer safety: global `DRY_RUN` + per-strategy `LIVE_<name>` flags. All default to off.

### Trading rules engine
Auto-detects account type (CASH/MARGIN) from Schwab API. Enforces PDT rule using Schwab's own `roundTrips` counter, closing-only restrictions, and wash sale warnings.

### ETF Scalp strategy
Intraday 3-min bar strategy: volume spike + price breakout + trend filter entry, tight TP/SL exits, settlement-aware tranche management for cash accounts.

### Telegram integration
Bot with alerts (trades, kill switch, errors), commands (/status, /pnl, /positions, /kill, /resume), and inline-button trade approval flow.

### Point & Figure charting
`pypf` integration powered by Schwab market data. Terminal P&F charts via `./run.sh pf SYMBOL`.

### Multi-account support
Scalp strategy can target a separate account hash from daily strategies.

### Schwab API reference
Full field-level documentation of every endpoint response in `docs/schwab-api-reference.md`.

### Strategy template
`TEMPLATE.py` with 10-question design checklist and wiring checklist for new strategies.

---

## Phase 1 — Data and reliability

### 1. WebSocket streaming
**Why:** The scalp strategy polls quotes every 15 seconds. At 0.15% targets on SPY, that's too slow — price can move $1 in seconds. Streaming fixes this.

Use `streamerInfo` from the Schwab user preferences endpoint (`wss://streamer-api.schwab.com/ws`) for real-time tick data. Replace polling in the scalp strategy with push-based price updates for instant TP/SL triggers.

**~400 lines** — `src/schwabagent/streaming.py`

### 2. Order fill tracking
**Why:** We place market orders and assume the fill price equals the quoted price. The actual fill can differ, which makes TP/SL levels inaccurate.

After placing an order, poll `GET /accounts/{hash}/orders` to get the actual fill price, then update the position's entry price and recalculate exit levels.

**~150 lines** — update `schwab_client.py` + `strategies/etf_scalp.py`

### 3. Cron scheduler
**Why:** Replace manual `./run.sh loop` with a persistent scheduled job queue. Survives restarts.

Example schedule:
```
09:30  weekdays  → scalp strategy starts
09:35  weekdays  → daily scan + execute
15:00  weekdays  → ETF rotation check
15:45  weekdays  → scalp session close
16:00  weekdays  → daily P&L → Telegram
08:00  monday    → weekly performance report
```

Job queue persisted at `~/.schwab-agent/cron.json`. Parsed by `croniter`.

**~300 lines** — `src/schwabagent/scheduler.py`

---

## Phase 2 — Intelligence

### 4. Earnings calendar
**Why:** Earnings reports cause 5-10% overnight moves. The agent should not hold positions through earnings unless explicitly told to.

Pull upcoming earnings dates via yfinance or Schwab fundamentals (`fundamental.lastEarningsDate`). Auto-reduce position size or skip trading in the 48-hour window around an earnings report.

**~150 lines** — `src/schwabagent/earnings.py`

### 5. Dividend awareness
**Why:** ETF rotation buys/sells around ex-dividend dates can cause unintended tax events or price-drop surprises.

Use `fundamental.nextDivExDate` from the quotes endpoint. Flag symbols approaching ex-date. Optionally defer sells until after ex-date to capture the dividend, or defer buys to avoid buying the drop.

**~100 lines** — integrate into ETF rotation scan

### 6. Liquidity filter
**Why:** The scalp strategy assumes tight spreads. Illiquid ETFs will eat the 0.15% target in spread alone.

Use `fundamental.avg10DaysVolume` from quotes. Skip symbols below a configurable volume threshold. Log when a symbol is excluded for liquidity.

**~50 lines** — integrate into scalp scan

### 7. Intermarket regime model
**Why:** Strategy performance varies by market regime. A macro overlay can adjust strategy weights or pause strategies in unfavorable conditions.

Port `intermarket.py` from the rebalancer: 7-signal macro model (risk appetite, credit conditions, inflation pressure, yield curve, etc.) from cross-asset reference symbols (SPY, TLT, HYG, GLD, IWM).

**~400 lines** — `src/schwabagent/intermarket.py`

---

## Phase 3 — Validation

### 8. Backtesting framework
**Why:** Every parameter in every strategy is an untested assumption. Need walk-forward replay against historical data before going live.

Metrics: CAGR, Sharpe, Sortino, max drawdown, win rate, profit factor. Per-year breakdown. CLI: `./run.sh backtest --strategy etf_rotation --start 2020-01-01 --end 2024-12-31`.

Port and adapt `backtest.py` from the rebalancer.

**~600 lines** — `src/schwabagent/backtest.py`

### 9. Portfolio optimizer
**Why:** Current position sizing is naive (equal weight or fixed dollar). MPT optimization can improve risk-adjusted returns.

Port `optimizer/engine.py` from the rebalancer. Strategies: Max Sharpe, Min Volatility, Efficient Risk, HRP. Uses `pypfopt`.

**~300 lines** — `src/schwabagent/optimizer.py`

---

## Phase 4 — Auditability

### 10. SQLite audit trail
**Why:** JSONL files are append-only and hard to query. A proper database enables analytics.

Tables: `trades`, `signals`, `risk_events`, `etf_scores`. Existing JSONL kept as backup during migration.

**~400 lines** — `src/schwabagent/db.py`

### 11. Strategy memory
**Why:** The agent has no memory across runs. It can't learn from patterns like "HYG underperformed 3 consecutive bear-filter cycles" or "momentum win rate drops in high-vol months."

Markdown file at `~/.schwab-agent/memory.md`. LLM writes entries when it notices patterns (if enabled). Human-editable.

**~100 lines** — `src/schwabagent/memory.py`

---

## Phase 5 — Interface

### 12. Web dashboard
Minimal FastAPI + Jinja2 dashboard at `http://localhost:5000`.

Pages:
- `/` — current positions, cash, total value, unrealized P&L
- `/etf` — ETF rotation rankings table
- `/signals` — latest scan results per strategy
- `/scalp` — live scalp positions, tranches, TP/SL levels
- `/pnl` — realized P&L by strategy
- `/risk` — risk state, trading rules status, PDT counter
- `/pf/:symbol` — Point & Figure chart (rendered in browser)

**~500 lines** — `src/schwabagent/web/`

### 13. Telegram trade approval UX polish
Wire `request_approval()` into the strategy execute path so every live trade requires a button tap. Add inline position management (close position, adjust TP/SL from Telegram).

**~200 lines** — update strategies + `telegram.py`

---

## Phase 6 — Advanced strategies

### 14. Options overlay
**Why:** The Schwab API returns full options chain data with greeks. Covered calls on held positions can generate income, and protective puts can limit downside.

Start with covered call writing on ETF rotation holdings. Use delta/theta from the chain to select strikes.

**~500 lines** — `src/schwabagent/strategies/options_overlay.py`

### 15. Pairs / spread trading
**Why:** Market-neutral strategy that profits from relative moves between correlated ETFs (e.g., SPY vs QQQ, TLT vs IEF).

Cointegration-based entry, mean-reversion exit. Lower correlation to market direction than directional strategies.

**~400 lines** — `src/schwabagent/strategies/pairs.py`

---

## What was considered and skipped

| Feature | Reason skipped |
|---------|----------------|
| Full hermes/prowler gateway | Too heavyweight — only Telegram alerting is needed |
| Browser-based scrapers (earnings, dividends) | Fragile Selenium automation; prefer API data |
| Crypto / Polymarket integration | Out of scope for Schwab agent |
| Android automation | Not applicable |
| Full MCP server | Unnecessary complexity |

---

## Dependency additions by phase

| Phase | New dependencies |
|-------|-----------------|
| 1 — Streaming | schwab-py streaming (built-in) |
| 1 — Scheduler | `croniter` |
| 2 — Earnings | `yfinance` (or Schwab fundamentals only) |
| 2 — Intermarket | none (numpy/pandas only) |
| 3 — Backtest | none (or `vectorbt` for advanced) |
| 3 — Optimizer | `pypfopt` |
| 4 — SQLite | stdlib `sqlite3` |
| 5 — Dashboard | `jinja2` (fastapi/uvicorn already in deps) |
