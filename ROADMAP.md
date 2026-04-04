# schwab-agent Roadmap

Features sourced from hermes-agent, prowler-agent, and the rebalancer.
Ranked by ROI — highest value, lowest effort first.

---

## Phase 1 — Autonomous operation (do first)

### 1. Alerting
**Source:** `hermes-agent/tools/send_message_tool.py`

Push notifications when things happen so you don't need to watch the terminal.

Covers:
- Kill switch triggered (drawdown exceeded)
- Live trade executed (symbol, side, quantity, price, P&L)
- Daily P&L summary (every strategy, realized + unrealized)
- Bear-market filter activated / deactivated
- Any error that stops the agent loop

Platforms: Telegram first, Discord and Slack as optional extras.
Config additions: `ALERT_TELEGRAM_TOKEN`, `ALERT_TELEGRAM_CHAT_ID`, `ALERT_DISCORD_WEBHOOK`.

**~150 lines** — `src/schwabagent/alerts.py`

---

### 2. Cron scheduler
**Source:** `hermes-agent/cron/scheduler.py`

Replace manual `./run.sh loop` with a persistent scheduled job queue. Survives restarts — next run time is written before execution so a crash doesn't skip a cycle.

Example schedule:
```
09:30  weekdays   → scan + execute
15:00  weekdays   → ETF rotation check
16:00  weekdays   → daily P&L summary → alerts
08:00  monday     → weekly performance report
```

Job queue persisted at `~/.schwab-agent/cron.json`.
Cron expressions parsed by `croniter`.

**~300 lines** — `src/schwabagent/scheduler.py` + `schwab-agent schedule` CLI subcommand

---

### 3. Earnings calendar
**Source:** `rebalancer/app/earnings/scraper.py`

Pull upcoming earnings dates for watchlist symbols via yfinance (no browser needed).
Auto-reduce position size or skip trading in the 48-hour window around an earnings report to avoid surprise moves.

Behaviour:
- `earnings_today()` → list of symbols reporting today or tomorrow
- Strategy `execute()` skips BUY signals for symbols in earnings window
- Existing positions flagged but not force-sold (configurable)

**~150 lines** — `src/schwabagent/earnings.py`

---

## Phase 2 — Visibility and auditability

### 4. SQLite audit trail
**Source:** `hermes-agent/hermes_state.py`

Replace current JSONL files with a queryable SQLite database at `~/.schwab-agent/agent.db`.

Tables:
- `trades` — every executed order with full context (signal, score, indicators, strategy)
- `signals` — every scan result, whether traded or not
- `risk_events` — drawdown checks, kill switch triggers, exposure cap hits
- `etf_scores` — per-cycle ETF ranking snapshots

Enables queries like:
```sql
SELECT * FROM trades WHERE symbol='SPY' AND realized_pnl < 0;
SELECT * FROM signals WHERE strategy='momentum' AND signal='STRONG_BUY' ORDER BY ts DESC LIMIT 20;
```

Existing JSONL files kept as backup during migration.

**~400 lines** — `src/schwabagent/db.py`

---

### 5. Persistent strategy memory
**Source:** `hermes-agent/tools/memory_tool.py`

Markdown file at `~/.schwab-agent/memory.md` where the agent accumulates notes across runs.

Examples of what gets written:
- "HYG underperformed 3 consecutive bear-filter cycles — consider removing from universe"
- "Momentum strategy win rate dropped to 38% in high-volatility months"
- "ETF_TOP_N=3 produced better Sharpe than TOP_N=5 in 2024"

LLM writes entries when it notices patterns (if `LLM_ENABLED=true`).
Human-editable — add your own notes and the agent will respect them.

**~100 lines** — `src/schwabagent/memory.py`

---

## Phase 3 — Validation and robustness

### 6. Backtesting framework
**Source:** Build from scratch (nothing to copy)

Walk-forward replay of any strategy against historical OHLCV pulled via the Schwab API or yfinance.

Metrics:
- CAGR, Sharpe ratio, Sortino ratio
- Max drawdown, max drawdown duration
- Win rate, average win / average loss, profit factor
- Per-year breakdown

CLI: `./run.sh backtest --strategy etf_rotation --start 2020-01-01 --end 2024-12-31`

Output: markdown report + CSV of all simulated trades.

**~600 lines** — `src/schwabagent/backtest.py`

---

### 7. Webhook receiver
**Source:** `hermes-agent/gateway/platforms/webhook.py`

Lightweight aiohttp server (separate from the web dashboard) that accepts HTTP POST triggers.

Routes:
- `POST /scan` — run one scan cycle immediately
- `POST /pnl` — return current P&L as JSON
- `POST /kill` — manually trigger kill switch
- `POST /resume` — clear kill switch

Useful for integrating external volatility monitors, TradingView alerts, or a mobile shortcut.
HMAC signature validation on all routes.

**~250 lines** — `src/schwabagent/webhook.py`

---

## Phase 4 — Interface

### 8. Web dashboard
**Source:** `rebalancer/app/portfolio/` + `app/market/`

Minimal FastAPI + Jinja2 dashboard at `http://localhost:5000`.

Pages:
- `/` — current positions, cash, total value, unrealized P&L
- `/etf` — ETF rotation rankings table (rank, score, 1m/3m/6m/12m returns, signal, LLM commentary)
- `/signals` — latest scan results per strategy
- `/pnl` — realized P&L by strategy, win rate, trade count
- `/risk` — risk state, drawdown from peak, kill switch status

Run alongside the agent: `./run.sh web` starts uvicorn on port 5000.

**~400 lines** — `src/schwabagent/web/`

---

## What was considered and skipped

| Feature | Reason skipped |
|---------|----------------|
| Android automation | Not applicable to equity trading |
| Full hermes gateway/MCP | Too heavyweight; only messaging and memory are useful |
| Blockchain/Solana tools | Out of scope |
| Polymarket prediction market data | Low priority; LLM macro overlay covers this use case |
| Full skills hub | Unnecessary complexity for a focused trading agent |

---

## Dependency additions (when each phase lands)

| Phase | New dependencies |
|-------|-----------------|
| 1 — Alerts | `python-telegram-bot` or `telebot`, optional `discord.py` |
| 1 — Scheduler | `croniter` |
| 1 — Earnings | already have `yfinance` via rebalancer pattern; add if not present |
| 2 — SQLite | stdlib `sqlite3` only |
| 3 — Backtest | `vectorbt` or plain pandas |
| 3 — Webhook | `aiohttp` |
| 4 — Dashboard | `fastapi`, `uvicorn`, `jinja2` (already in pyproject.toml) |
