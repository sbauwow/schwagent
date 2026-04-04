# schwab-agent

Automated equity trading agent for Charles Schwab. Scans a configurable watchlist using multiple quantitative strategies (momentum, mean reversion, trend following, composite) and places orders via the `schwab-py` library.

All risk controls are enabled by default. **`DRY_RUN=true` is the default** — no real orders are placed until you explicitly pass `--live`.

---

## Quick start

```bash
# 1. Clone / enter the project
cd ~/Projects/schwab-agent

# 2. Install dependencies (requires uv)
uv sync

# 3. Configure
cp .env.example .env
$EDITOR .env   # set SCHWAB_API_KEY, SCHWAB_APP_SECRET at minimum

# 4. Authenticate (opens browser for OAuth the first time)
uv run python -c "
from schwabagent.config import Config
from schwabagent.schwab_client import SchwabClient
SchwabClient(Config()).authenticate()
"

# 5. Dry-run scan
./run.sh scan
```

---

## run.sh commands

| Command | Description |
|---------|-------------|
| `./run.sh` | Dry-run one cycle (default) |
| `./run.sh status` | Check Schwab connectivity and config |
| `./run.sh scan` | Scan watchlist — show signals, no trades |
| `./run.sh once` | Dry-run one full scan + execute cycle |
| `./run.sh loop` | Dry-run continuous loop (interval from `.env`) |
| `./run.sh live` | **Live trading** — real orders, requires confirmation |
| `./run.sh pnl` | Show realized P&L by strategy |

---

## Configuration

All settings live in `.env` (see `.env.example` for full reference).

Key settings:

```bash
SCHWAB_API_KEY=...           # from developer.schwab.com
SCHWAB_APP_SECRET=...
WATCHLIST=AAPL,MSFT,GOOGL   # symbols to scan
STRATEGIES=composite         # which strategies to run
DRY_RUN=true                 # set false for live trading
MAX_TOTAL_EXPOSURE=50000.0   # hard cap on total $ deployed
MAX_POSITION_VALUE=5000.0    # max $ per single position
MAX_DRAWDOWN_PCT=15.0        # kill switch at -15% drawdown
```

---

## Strategies

### Momentum (`momentum`)
Uses SMA(20), SMA(50), RSI(14), and MACD. Buys when price is above both moving averages with confirming RSI and positive MACD. Sells into weakness.

### Mean Reversion (`mean_reversion`)
Uses Bollinger Bands(20,2), RSI(14), and z-score of returns. Buys at lower band when oversold; sells at upper band when overbought.

### Trend Following (`trend_following`)
Uses EMA(20/50/200) alignment and ADX(14) trend strength. Requires strong ADX confirmation for strongest signals.

### Composite (`composite`)
Runs all three strategies and averages their scores (STRONG_BUY=2 … STRONG_SELL=−2). Final composite score ≥ 1.5 → STRONG_BUY, ≥ 0.5 → BUY, etc.

---

## Risk management

- **Position size limit** — max `MAX_POSITION_PCT` of portfolio or `MAX_POSITION_VALUE` $, whichever is lower.
- **Total exposure cap** — agent stops buying when total deployed capital exceeds `MAX_TOTAL_EXPOSURE`.
- **Drawdown kill switch** — all trading halts if portfolio drops more than `MAX_DRAWDOWN_PCT` from peak.
- **Minimum signal score** — only trades with composite score ≥ `MIN_SIGNAL_SCORE` are executed.
- **Min / max order size** — orders below `MIN_ORDER_VALUE` or above `MAX_ORDER_VALUE` are skipped.

---

## State files

All state is stored in `~/.schwab-agent/`:

| File | Contents |
|------|----------|
| `token.json` | Schwab OAuth token |
| `risk_state.json` | Peak value, kill-switch status |
| `trade_history.jsonl` | One trade per line |
| `strategy_pnl.json` | Per-strategy cumulative P&L |
| `audit.jsonl` | Full audit trail |

---

## Safety defaults

- `DRY_RUN=true` in `.env.example` — must explicitly set `false` or pass `--live`
- `./run.sh live` requires typing `yes` at a confirmation prompt
- Kill switch permanently halts execution until manually cleared from state
