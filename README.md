# schwab-agent

Automated equity and ETF trading agent for Charles Schwab. Connects via dual OAuth2 apps (account + market data), scans a configurable watchlist with multiple quantitative strategies, and places orders through the Schwab Trader API.

**`DRY_RUN=true` is the default.** Each strategy also has its own `LIVE_<name>=false` toggle. No real orders are placed until both layers are explicitly enabled.

---

## Quick start

```bash
cd ~/Projects/schwab-agent

# Install dependencies (requires uv)
uv sync

# Configure
cp .env.example .env
$EDITOR .env   # set Schwab credentials at minimum

# Authenticate — opens browser for OAuth
./run.sh enroll

# Dry-run scan — see signals, no trades
./run.sh scan
```

---

## run.sh commands

| Command | Description |
|---------|-------------|
| `./run.sh enroll` | Authenticate with Schwab (OAuth browser flow) |
| `./run.sh status` | Check connectivity, config, balances, per-strategy live flags |
| `./run.sh scan` | Scan watchlist + ETF universe, show signals — no trades |
| `./run.sh once` | Dry-run one full scan + execute cycle |
| `./run.sh loop` | Dry-run continuous loop (interval from `.env`) |
| `./run.sh live` | **Live trading** — real orders, requires confirmation |
| `./run.sh pnl` | Show realized P&L by strategy |
| `./run.sh pf AAPL` | Point & Figure chart (powered by pypf + Schwab data) |

### Point & Figure charts

```bash
./run.sh pf SPY                        # default: 1% box, 3-box reversal, 1yr chart
./run.sh pf AAPL -b 0.02 -r 2         # 2% box, 2-box reversal
./run.sh pf QQQ -d 0.5 -p 1           # 6-month chart, 1yr data
./run.sh pf GLD --no-style --meta      # plain output + JSON metadata
```

---

## Dual API architecture

Schwab exposes two separate API products requiring separate app registrations:

| API | Purpose | Config keys |
|-----|---------|-------------|
| **Trader API** | Accounts, positions, balances, orders | `SCHWAB_API_KEY`, `SCHWAB_APP_SECRET` |
| **Market Data API** | Quotes, price history, options chains | `SCHWAB_MARKET_API_KEY`, `SCHWAB_MARKET_APP_SECRET` |

Each has its own OAuth token (`token.json` / `market_token.json`). The `enroll` command lets you authenticate each separately.

---

## Configuration

All settings live in `.env`. See `.env.example` for the full reference.

### Schwab credentials

```bash
# Account/Trading API — from developer.schwab.com → My Apps
SCHWAB_API_KEY=
SCHWAB_APP_SECRET=
SCHWAB_CALLBACK_URL=https://127.0.0.1:8182/auth/callback

# Market Data API — separate app registration
SCHWAB_MARKET_API_KEY=
SCHWAB_MARKET_APP_SECRET=
SCHWAB_MARKET_CALLBACK_URL=https://127.0.0.1:8182/auth/callback
```

### Multi-account

The default `SCHWAB_ACCOUNT_HASH` is used for daily strategies. The scalp strategy can target a separate account:

```bash
SCHWAB_ACCOUNT_HASH=           # default account (leave empty for first)
SCALP_ACCOUNT_HASH=            # separate account for scalp strategy
```

### Two-layer safety model

**Layer 1 — Global:** `DRY_RUN=true` is the master switch. Nothing trades when on.

**Layer 2 — Per-strategy:** Even with `DRY_RUN=false` (via `./run.sh live`), each strategy must be individually enabled:

```bash
LIVE_ETF_ROTATION=false
LIVE_MOMENTUM=false
LIVE_MEAN_REVERSION=false
LIVE_TREND_FOLLOWING=false
LIVE_COMPOSITE=false
LIVE_ETF_SCALP=false
```

A strategy only places real orders when **both** layers allow it.

---

## Strategies

### ETF Rotation (`etf_rotation`)

Dual momentum rotation across a configurable ETF universe. Holds the top-N ETFs by momentum score and rotates out of laggards.

**Scoring** — weighted momentum across four lookback periods:

| Period | Weight |
|--------|--------|
| 1 month | 40% |
| 3 months | 20% |
| 6 months | 20% |
| 12 months | 20% |

**Bear-market filter** — if SPY falls below its 200-day SMA, all risky positions are sold and the portfolio rotates entirely to `ETF_SAFE_HAVEN` (default: `SHY`).

**LLM overlay** — when `LLM_ENABLED=true`, the local Ollama model provides a macro confidence score (0-1) that scales position size for top-ranked ETFs.

### ETF Scalp (`etf_scalp`)

Intraday scalping on liquid ETFs. Designed for cash accounts with $200k+ capital.

**Entry** — requires all three:
1. Volume spike: bar volume > 2x the 20-bar average
2. Price breakout: close > high of prior 3 bars
3. Trend filter: EMA(9) > EMA(21) and price > VWAP

**Exit** — first condition hit wins:
- +0.15% take profit
- -0.10% stop loss
- 30 min time stop
- 15:45 ET session close

**Capital management** — splits capital into 5 tranches for T+1 settlement management. Max 3 concurrent positions. Uses 3-minute bars aggregated from 1-minute Schwab data.

### Momentum (`momentum`)

SMA(20/50) + RSI(14) + MACD. Buys when price is above both moving averages with confirming RSI and positive MACD histogram.

### Mean Reversion (`mean_reversion`)

Bollinger Bands(20,2) + RSI(14) + z-score. Buys at the lower band when oversold; sells at the upper band when overbought.

### Trend Following (`trend_following`)

EMA(20/50/200) alignment + ADX(14). Requires strong trend confirmation (ADX > 25) for the strongest signals.

### Composite (`composite`)

Runs Momentum, Mean Reversion, and Trend Following simultaneously and averages their scores. Only trades when the consensus is clear.

### Writing new strategies

Copy `src/schwabagent/strategies/TEMPLATE.py` — it includes a 10-question design checklist and a wiring checklist for the files you need to update.

---

## Trading rules engine

Brokerage constraints are auto-enforced using data from the Schwab API:

| Rule | Source | Behavior |
|------|--------|----------|
| **PDT** (Pattern Day Trader) | `securitiesAccount.roundTrips`, `isDayTrader` | Blocks day trade #4 on margin accounts < $25k |
| **Closing-only** | `isClosingOnlyRestricted` | Blocks all new BUY orders |
| **Wash sale** | Trade history (30-day lookback) | Warns but allows (tax implication only) |

Account type (`CASH` / `MARGIN`) is auto-detected from the API. PDT does not apply to cash accounts.

---

## Telegram integration

Push alerts and interactive trade management via Telegram bot.

### Setup

1. Create a bot via [@BotFather](https://t.me/BotFather)
2. Get your chat ID via [@userinfobot](https://t.me/userinfobot)
3. Configure in `.env`:

```bash
TELEGRAM_ENABLED=true
TELEGRAM_BOT_TOKEN=123456:ABC-DEF...
TELEGRAM_CHAT_ID=your_numeric_id
TELEGRAM_REQUIRE_APPROVAL=true
TELEGRAM_APPROVAL_TIMEOUT=300
```

### Alerts (agent → you)

- Trade executed (symbol, side, qty, price, P&L, strategy)
- Kill switch triggered
- Daily P&L summary
- Agent errors

### Bot commands (you → agent)

| Command | Description |
|---------|-------------|
| `/status` | Account value, cash, positions, kill switch |
| `/pnl` | P&L by strategy with win rates |
| `/positions` | Current holdings with unrealized P&L |
| `/kill` | Activate kill switch remotely |
| `/resume` | Clear kill switch |

### Trade approval

When `TELEGRAM_REQUIRE_APPROVAL=true`, live trades show **Approve** / **Reject** inline buttons. The agent blocks until you respond or the timeout expires (default 5 min). Unapproved trades are not executed.

---

## Risk management

| Control | Setting | Default |
|---------|---------|---------|
| Position size cap | `MAX_POSITION_PCT` / `MAX_POSITION_VALUE` | 10% / $5,000 |
| Total exposure cap | `MAX_TOTAL_EXPOSURE` | $50,000 |
| Drawdown kill switch | `MAX_DRAWDOWN_PCT` | 15% |
| Minimum signal score | `MIN_SIGNAL_SCORE` | 1.0 |
| Order size floor | `MIN_ORDER_VALUE` | $100 |
| Order size ceiling | `MAX_ORDER_VALUE` | $2,000 |

The kill switch halts all execution until manually cleared from Telegram (`/resume`) or by editing `~/.schwab-agent/risk_state.json`.

---

## Local LLM support

Optional [Ollama](https://ollama.com) integration for macro commentary on ETF rotation signals.

```bash
LLM_ENABLED=true
OLLAMA_HOST=http://localhost:11434
OLLAMA_MODEL=qwen2.5:14b-instruct-q5_K_M
```

---

## State files

All state is persisted in `~/.schwab-agent/`:

| File | Contents |
|------|----------|
| `token.json` | Schwab Account API OAuth token (chmod 0600) |
| `market_token.json` | Schwab Market Data API OAuth token |
| `risk_state.json` | Peak portfolio value, kill-switch status |
| `trade_history.jsonl` | One trade record per line |
| `strategy_pnl.json` | Per-strategy cumulative P&L, win rate |
| `audit.jsonl` | Full audit trail of all actions |

---

## Project structure

```
src/schwabagent/
  config.py           Configuration (pydantic-settings, loaded from .env)
  schwab_client.py    Schwab API wrapper (dual client: account + market)
  runner.py           Main orchestrator (scan → execute loop)
  risk.py             Risk management + trading rules integration
  trading_rules.py    Brokerage rules engine (PDT, wash sale, closing-only)
  persistence.py      JSON/JSONL state storage
  indicators.py       Technical indicators (SMA, EMA, RSI, MACD, etc.)
  telegram.py         Telegram bot (alerts, commands, trade approval)
  pf.py               Point & Figure charting (pypf + Schwab data)
  llm.py              Ollama LLM client
  cli.py              CLI entry point
  strategies/
    base.py           Abstract strategy interface + Signal enum
    TEMPLATE.py       Strategy template with design checklist
    etf_rotation.py   Dual momentum ETF rotation
    etf_scalp.py      Intraday ETF scalping (volume/price breakout)
    momentum.py       SMA/RSI/MACD momentum
    mean_reversion.py Bollinger Bands mean reversion
    trend_following.py EMA alignment + ADX trend
    composite.py      Multi-strategy consensus
docs/
  schwab-api-reference.md   Full Schwab API field reference
```

---

## Development

```bash
uv sync --dev
uv run pytest          # run tests
uv run ruff check src  # lint
```
