# schwab-agent

Automated equity and ETF trading agent for Charles Schwab. Connects via `schwab-py` OAuth2, scans a configurable watchlist with multiple quantitative strategies, and places orders through the Schwab API.

**`DRY_RUN=true` is the default** — no real orders until you explicitly pass `--live`.

---

## Quick start

```bash
cd ~/Projects/schwab-agent

# Install dependencies (requires uv)
uv sync

# Configure
cp .env.example .env
$EDITOR .env   # set SCHWAB_API_KEY and SCHWAB_APP_SECRET at minimum

# Authenticate (opens browser for Schwab OAuth on first run)
./run.sh status

# Dry-run scan — see signals, no trades placed
./run.sh scan
```

---

## run.sh commands

| Command | Description |
|---------|-------------|
| `./run.sh status` | Check Schwab connectivity, config, and Ollama |
| `./run.sh scan` | Scan watchlist — show signals and ETF rankings, no trades |
| `./run.sh once` | Dry-run one full scan + execute cycle |
| `./run.sh loop` | Dry-run continuous loop (interval from `.env`) |
| `./run.sh live` | **Live trading** — real orders, requires confirmation |
| `./run.sh pnl` | Show realized P&L by strategy |

---

## Configuration

All settings live in `.env`. Copy `.env.example` for the full reference.

```bash
# Schwab — get from developer.schwab.com → My Apps
SCHWAB_API_KEY=
SCHWAB_APP_SECRET=
SCHWAB_CALLBACK_URL=https://127.0.0.1   # must match your app's redirect URI

# Strategies to run (comma-separated)
STRATEGIES=etf_rotation,momentum,mean_reversion,trend_following,composite

# Symbols scanned by momentum/mean_reversion/trend_following strategies
WATCHLIST=AAPL,MSFT,GOOGL,AMZN,NVDA,META,TSLA,JPM,V,UNH

# Risk limits
MAX_TOTAL_EXPOSURE=50000.0   # hard cap on total $ deployed
MAX_POSITION_VALUE=5000.0    # max $ per single position
MAX_DRAWDOWN_PCT=15.0        # kill switch at -15% drawdown from peak

# Safety
DRY_RUN=true                 # set false or use ./run.sh live for real orders
```

---

## Strategies

### ETF Rotation (`etf_rotation`) — primary

Dual momentum rotation across a configurable ETF universe. Holds the top-N ETFs by momentum score and rotates out of laggards.

**Scoring** — weighted momentum across four lookback periods:

| Period | Weight |
|--------|--------|
| 1 month | 40% |
| 3 months | 20% |
| 6 months | 20% |
| 12 months | 20% |

**Bear-market filter** — if SPY falls below its 200-day SMA, all risky positions are sold and the portfolio rotates entirely to `ETF_SAFE_HAVEN` (default: `SHY`).

**Position sizing** — equal weight: `portfolio_value / ETF_TOP_N`, clipped to risk limits.

**LLM overlay** — when `LLM_ENABLED=true`, the local Ollama model provides a macro confidence score (0–1) that scales position size for top-ranked ETFs.

**restricted issuer blocklist** — all restricted issuer ETFs are excluded by default via `ETF_BLOCKLIST`. They can never appear in the universe even if added to `ETF_UNIVERSE`.

ETF rotation config:

```bash
ETF_UNIVERSE=SPY,QQQ,IWM,EFA,EEM,TLT,IEF,HYG,TIP,GLD,VNQ,SHY
ETF_TOP_N=3                  # hold top 3 ETFs at any time
ETF_MOMENTUM_PERIODS=1,3,6,12
ETF_SAFE_HAVEN=SHY           # cash-equivalent ETF for bear market
ETF_BEAR_FILTER=true
ETF_BLOCKLIST=MINT,LDUR,SMUR,HYIN,ZROZ,BOND,PDBC,HYLS,LOWV,EMPW,MUNI,INFU,PFFD,REGL
```

### Momentum (`momentum`)

SMA(20/50) + RSI(14) + MACD. Buys when price is above both moving averages with confirming RSI and positive MACD histogram. Sells into weakness.

### Mean Reversion (`mean_reversion`)

Bollinger Bands(20,2) + RSI(14) + z-score. Buys at the lower band when oversold; sells at the upper band when overbought.

### Trend Following (`trend_following`)

EMA(20/50/200) alignment + ADX(14). Requires strong trend confirmation (ADX > 25) for the strongest signals.

### Composite (`composite`)

Runs Momentum, Mean Reversion, and Trend Following simultaneously and averages their scores. Only trades when the consensus is clear.

| Score | Signal |
|-------|--------|
| ≥ 1.5 | STRONG_BUY |
| ≥ 0.5 | BUY |
| > −0.5 | HOLD |
| > −1.5 | SELL |
| ≤ −1.5 | STRONG_SELL |

---

## Local LLM support

The agent optionally uses a local [Ollama](https://ollama.com) model to add macro commentary and a confidence modifier to ETF rotation signals.

```bash
LLM_ENABLED=true
OLLAMA_HOST=http://localhost:11434
OLLAMA_MODEL=qwen2.5:14b-instruct-q5_K_M
OLLAMA_TIMEOUT=60
```

When enabled, the top-N ETF candidates are sent to the model with their momentum rank and return history. The model returns a `confidence` (0–1) that scales the target position size — high conviction increases allocation, low conviction reduces it. The agent runs fine with `LLM_ENABLED=false`.

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

The kill switch permanently halts all execution until manually cleared from `~/.schwab-agent/risk_state.json`.

---

## State files

All state is persisted in `~/.schwab-agent/`:

| File | Contents |
|------|----------|
| `token.json` | Schwab OAuth token (chmod 0600) |
| `risk_state.json` | Peak portfolio value, kill-switch status |
| `trade_history.jsonl` | One trade record per line |
| `strategy_pnl.json` | Per-strategy cumulative P&L, win rate |
| `audit.jsonl` | Full audit trail of all actions |

---

## Schwab app setup

1. Go to [developer.schwab.com](https://developer.schwab.com) → My Apps → Create App
2. Set the callback/redirect URL to `https://127.0.0.1` (or whatever you set in `SCHWAB_CALLBACK_URL`)
3. Copy the API key and app secret into `.env`
4. Run `./run.sh status` — it will open a browser for OAuth on first run, then save `~/.schwab-agent/token.json`

The token refreshes automatically. If it expires, delete `~/.schwab-agent/token.json` and re-run `./run.sh status`.

---

## Development

```bash
uv sync --dev
uv run pytest          # run tests
uv run pytest -v       # verbose
```
