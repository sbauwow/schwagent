# schwab-agent

## ⚠️  DISCLAIMER — READ THIS FIRST

> **NOT AFFILIATED WITH CHARLES SCHWAB.** This is an unofficial, independent, personal project. It is **not** endorsed by, sponsored by, associated with, or in any way connected to Charles Schwab & Co., Inc. or any of its subsidiaries or affiliates. "Schwab" and any related marks are the property of their respective owners and are used here only to describe which third-party API this code talks to.
>
> **NOT TESTED. NOT AUDITED. USE AT YOUR OWN RISK.** This code has **not** been validated in production, has **not** been reviewed for correctness or safety, and may contain bugs that cause it to place wrong orders, lose money, leak credentials, corrupt state, or fail in ways I have not anticipated. I make **no** guarantees that anything in this repository works as described — or at all.
>
> **NOT FINANCIAL ADVICE.** Nothing in this repository constitutes financial, investment, trading, tax, or legal advice. Options trading involves substantial risk of loss and is not suitable for every investor. You can lose more than you invest.
>
> **NO WARRANTY. NO LIABILITY.** This software is provided "AS IS", without warranty of any kind, express or implied. In no event shall the author be liable for any claim, damages, or other liability — including but not limited to trading losses, account suspension, API misuse, data loss, or any direct, indirect, incidental, special, exemplary, or consequential damages — arising from, out of, or in connection with this software or its use.
>
> **If you run this code against a real brokerage account, you are doing so entirely on your own responsibility.** Paper trade first. Read the source. Understand what every order-placing function does before you enable it. If you are not comfortable auditing this yourself, **do not run it.**

---

Automated equity, ETF, and options trading agent for Charles Schwab. Connects via dual OAuth2 apps (account + market data), scans a configurable watchlist with multiple quantitative strategies, and places orders through the Schwab Trader API.

Beyond core trading, the agent includes:

- **Web dashboard** — real-time view of accounts, positions, trades, and P&L
- **Backtest validation** — Monte Carlo, Bootstrap Sharpe CI, Walk-Forward
- **Options analysis** — Black-Scholes + Greeks, implied vol solver, multi-leg payoff analysis
- **Portfolio optimization** — mean-variance, HRP, and discrete allocation via PyPortfolioOpt
- **Technical indicators** — 35+ trend/momentum/volatility/volume indicators via the `ta` library
- **Reference skill library** — 23 curated analysis methodologies for the LLM overlay
- **Swarm workflows** — DAG-based multi-agent committees (investment, TA panel, ETF allocation)
- **Telegram bot** — push alerts, `/status` `/pnl` `/kill`, trade approval
- **Point & Figure charting** — classic charting on Schwab OHLC data

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
| `./run.sh backtest <strategy>` | Historical backtest against CSV data |
| `./run.sh validate <strategy>` | Backtest + Monte Carlo + Bootstrap + Walk-Forward validation |
| `./run.sh options price\|iv\|strategy` | Black-Scholes pricing, IV solver, multi-leg strategy analysis |
| `./run.sh optimize <tickers>` | Portfolio optimization (max Sharpe, min vol, HRP) via PyPortfolioOpt |
| `./run.sh ta [list\|<indicator> <symbol>]` | 35+ technical indicators via the `ta` library |
| `./run.sh sec SYMBOL [filings\|analyze\|risks\|compare\|scan]` | SEC EDGAR filings lookup, risk extraction, 10-Q comparison, 8-K scan |
| `./run.sh dream` | Dreamcycle — one autonomous research cycle (scan, drift, auto-tune, digest) |
| `./run.sh feedback [days]` | Signal feedback: win rate, P&L, drift alerts, calibration by signal type |
| `./run.sh skills` | List user skills in `~/.schwab-agent/skills/` grouped by category |
| `./run.sh web` | Web dashboard at http://localhost:8898 |
| `./run.sh ref [skill]` | Reference skill library for the LLM overlay |
| `./run.sh swarm [preset]` | Multi-agent committee workflows |

### Point & Figure charts

```bash
./run.sh pf SPY                        # default: 1% box, 3-box reversal, 1yr chart
./run.sh pf AAPL -b 0.02 -r 2         # 2% box, 2-box reversal
./run.sh pf QQQ -d 0.5 -p 1           # 6-month chart, 1yr data
./run.sh pf GLD --no-style --meta      # plain output + JSON metadata
```

### Web dashboard

A FastAPI + vanilla-JS dashboard for real-time account and portfolio visibility.

```bash
./run.sh web                           # starts on http://localhost:8898
```

Four tabs:
- **Dashboard** — KPI cards (total value, cash, invested, kill switch, peak, mode), per-account cards with PDT/closing-only/unsettled flags, stacked portfolio allocation bar.
- **Positions** — all positions across all linked accounts, sorted by market value.
- **Trades** — recent trade history from `trade_history.jsonl`.
- **P&L** — per-strategy P&L with win rates, total realized P&L, and trade counts.

Data is fetched from Schwab on demand; a WebSocket heartbeat keeps the UI live-aware.

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

## Intelligence layer

The `src/schwabagent/intelligence/` module adds LLM-friendly reasoning tooling
adapted from [HKUDS/vibe-trading](https://github.com/hkuds/vibe-trading) (MIT licensed).

### Reference skill library

23 curated `SKILL.md` methodology documents cover equity, ETF, and options
analysis. The `SkillsLoader` uses progressive disclosure: only one-line
summaries are injected into the system prompt; full content is loaded on
demand via `llm.load_skill(name)`.

```bash
./run.sh ref                           # list all skills by category
./run.sh ref options-strategy          # show full methodology for one skill
```

Categories and examples:

| Category | Skills |
|----------|--------|
| **Strategy** | `technical-basic`, `multi-factor`, `ml-strategy`, `pair-trading`, `seasonal`, `candlestick`, `execution-model`, `strategy-generate` |
| **Analysis** | `factor-research`, `correlation-analysis`, `behavioral-finance`, `valuation-model`, `earnings-forecast`, `market-microstructure` |
| **Asset class** | `asset-allocation`, `options-strategy`, `options-advanced`, `options-payoff`, `etf-analysis`, `sector-rotation`, `hedging-strategy` |
| **Flow** | `financial-statement`, `fundamental-filter` |

The active `LLMClient` exposes:

```python
llm.skills                             # lazy-loaded SkillsLoader
llm.with_skills(system_prompt)         # augment system prompt with skill catalog
llm.load_skill("options-strategy")     # fetch full content on demand
```

### Swarm multi-agent workflows

DAG-based committee workflows where multiple agents argue, analyze, or
synthesize in parallel before a final decision. Each agent has its own
system prompt and whitelisted skills. Tasks run in topological layers
— independent tasks execute in parallel via a `ThreadPoolExecutor`,
then dependent tasks consume upstream outputs.

```bash
./run.sh swarm                                          # list all presets
./run.sh swarm investment_committee                     # describe one preset
./run.sh swarm investment_committee target=SPY          # execute
./run.sh swarm technical_analysis_panel target=AAPL timeframe=daily
./run.sh swarm etf_allocation_desk universe="SPY,QQQ,IWM,EFA,EEM,TLT,GLD,VNQ"
```

Bundled presets:

| Preset | Agents | Purpose |
|--------|--------|---------|
| `investment_committee` | bull advocate → bear advocate → risk officer → portfolio manager | Position sizing decision on a single ticker |
| `technical_analysis_panel` | trend + momentum + pattern + volume → aggregator | Multi-dimensional TA consensus with composite score |
| `etf_allocation_desk` | rotation + correlation + factor → allocation strategist | Build a 4-6 ETF weighted portfolio from a universe |

All presets are YAML files in `src/schwabagent/intelligence/swarm/presets/` —
add new ones without touching code. Each preset defines agents (role,
system prompt, skills), tasks (DAG with `depends_on` and `input_from`
mapping), and user variables. See
`src/schwabagent/intelligence/swarm/presets/investment_committee.yaml`
for a complete example.

---

## Options analysis

Pure-math options module in `src/schwabagent/options.py` — Black-Scholes
pricing, Greeks, implied volatility solver, multi-leg payoff analysis, and
a library of common strategy constructors. No scipy dependency (normal
CDF/PDF from stdlib `math.erf`).

### Single-option pricing

```bash
./run.sh options price spot=500 strike=505 dte=30 iv=0.20 type=call
```

```
  CALL spot=$500.00 strike=$505.00 dte=30d iv=20.00%

    price  = 10.064226
    delta  = 0.470825
    gamma  = 0.013878
    theta  = -0.220982    (per day)
    vega   = 0.570336     (per 1% IV change)
```

### Implied volatility

Bisection solver that returns `None` if the market price is outside the
arbitrage bounds (below intrinsic or above spot).

```bash
./run.sh options iv price=3.50 spot=500 strike=505 dte=30 type=call
# → Implied volatility: 0.0841 (8.41%)
```

### Multi-leg strategies

Built-in strategy constructors produce a list of `Leg` objects, which
`strategy_metrics()` analyzes for max profit, max loss, breakevens, and
net debit/credit.

| Strategy | CLI name | Notes |
|----------|----------|-------|
| Long call / put | `long_call`, `long_put` | Single-leg directional |
| Long straddle / strangle | `long_straddle`, `long_strangle` | Volatility bets |
| Vertical spreads | `bull_call`, `bear_put`, `bull_put`, `bear_call` | Debit and credit |
| Iron condor | `iron_condor` | Short put spread + short call spread |
| Butterfly | `long_butterfly` | Narrow pin on middle strike |
| Covered call | `covered_call` | Long stock + short call |
| Protective put | `protective_put` | Long stock + long put |

```bash
./run.sh options strategy iron_condor \
    pl=460 ps=470 cs=530 cl=540 \
    pl_prem=0.80 ps_prem=1.50 cs_prem=1.60 cl_prem=0.75
```

```
  iron_condor

    Legs (4):
      + 1x put  strike=$460.00 @ $0.80
      - 1x put  strike=$470.00 @ $1.50
      - 1x call strike=$530.00 @ $1.60
      + 1x call strike=$540.00 @ $0.75

    Net credit  $1.55
    Max profit    $1.55
    Max loss      $-8.45
    Breakevens    $468.45, $531.55
```

Python API:

```python
from schwabagent.options import (
    bs_price_and_greeks, implied_volatility,
    iron_condor, strategy_metrics,
)

legs = iron_condor(
    put_long_strike=460, put_short_strike=470,
    call_short_strike=530, call_long_strike=540,
    put_long_premium=0.80, put_short_premium=1.50,
    call_short_premium=1.60, call_long_premium=0.75,
)
print(strategy_metrics(legs, spot_range=(440, 560)))
# {'max_profit': 1.55, 'max_loss': -8.45,
#  'breakevens': [468.45, 531.55], 'net_debit': -1.55, 'legs': 4}
```

---

## Technical indicators

Thin wrapper in `src/schwabagent/ta_indicators.py` around the
[`ta` library](https://github.com/bukosabino/ta) — pure Python, no
C dependency, 35+ indicators across four categories. Complements
(not replaces) the existing `indicators.py` module which has 31
hand-written indicators used by the built-in strategies.

### Available indicators

| Category | Indicators |
|----------|------------|
| **Trend** (12) | `sma`, `ema`, `wma`, `macd`, `adx`, `ichimoku`, `aroon`, `psar`, `cci`, `vortex`, `trix`, `kst` |
| **Momentum** (9) | `rsi`, `stoch`, `stochrsi`, `williams_r`, `roc`, `tsi`, `kama`, `ultimate`, `awesome` |
| **Volatility** (5) | `bollinger`, `atr`, `keltner`, `donchian`, `ulcer` |
| **Volume** (9) | `obv`, `mfi`, `vwap`, `cmf`, `adi`, `force_index`, `eom`, `nvi`, `vpt` |

### CLI

```bash
./run.sh ta                       # list all indicators by category
./run.sh ta rsi SPY               # RSI(14) on SPY, last 10 values
./run.sh ta macd AAPL days=365    # MACD on AAPL over 1 year
./run.sh ta bollinger TLT         # Bollinger bands on TLT
```

Multi-output indicators (`macd`, `adx`, `bollinger`, `ichimoku`, etc.)
print one column per output.

### Python API

The module exposes two primary entry points:

```python
from schwabagent.ta_indicators import compute, apply_all
from schwabagent.schwab_client import SchwabClient
from schwabagent.config import Config

client = SchwabClient(Config())
client.authenticate()
df = client.get_ohlcv("SPY", days=365)

# Single indicator — scalar (Series) or multi-output (DataFrame)
rsi = compute(df, "rsi", window=14)                     # → Series
macd = compute(df, "macd", fast=12, slow=26, signal=9)  # → DataFrame with 3 cols
bb = compute(df, "bollinger", window=20, window_dev=2)  # → DataFrame with 5 cols

# Bulk feature engineering — 25+ indicator columns in one call
features = apply_all(df)                                # → wide DataFrame
features = apply_all(df, include=["rsi", "atr", "macd"])
features = apply_all(df, exclude=["williams_r"])
```

Both `compute` and `apply_all` accept either lowercase OHLCV columns
(schwab-agent convention) or capitalized columns (yfinance convention).
Multi-output indicators are automatically expanded into individual
columns in `apply_all` so the result is ready to feed to an ML model
or a backtest.

---

## Portfolio optimization

Wrapper around [PyPortfolioOpt](https://github.com/robertmartin8/PyPortfolioOpt) in
`src/schwabagent/portfolio_optimizer.py`. Supports mean-variance, hierarchical
risk parity, and discrete share allocation — feeds off Schwab historical OHLCV
so any universe you have market-data access to can be optimized.

### Supported objectives

| Method | Description |
|--------|-------------|
| `max_sharpe` | Maximum Sharpe ratio on the efficient frontier |
| `min_volatility` | Minimum variance portfolio |
| `efficient_risk` | Max return for a target volatility |
| `efficient_return` | Min volatility for a target return |
| `hrp` | Hierarchical risk parity (Lopez de Prado) |

Expected returns estimators: `mean_historical`, `ema_historical`, `capm`.
Risk models: `sample_cov`, `ledoit_wolf`, `exp_cov`.

### CLI

```bash
./run.sh optimize SPY,QQQ,IWM,EFA,EEM,TLT,GLD,VNQ
./run.sh optimize SPY,QQQ,TLT,GLD method=min_volatility capital=50000
./run.sh optimize SPY,QQQ,TLT,GLD method=hrp days=500
```

Output includes continuous weights, annualized return/vol/Sharpe, and a
discrete share allocation rounded to whole shares with leftover cash:

```
=== Portfolio Optimization: max_sharpe ===

Expected return:      +14.23%
Expected volatility:  11.45%
Sharpe ratio:         +0.893

Continuous weights:
  QQQ      42.18%
  GLD      28.56%
  TLT      18.92%
  SPY      10.34%

Discrete allocation ($100,000 capital):
  QQQ        86 shares
  GLD       123 shares
  TLT       191 shares
  SPY        17 shares
  Leftover cash:  $214.56
```

### Python API

```python
from schwabagent.portfolio_optimizer import optimize_portfolio, format_report
from schwabagent.schwab_client import SchwabClient
from schwabagent.config import Config

client = SchwabClient(Config())
client.authenticate()

tickers = ["SPY", "QQQ", "IWM", "EFA", "TLT", "GLD", "VNQ"]
prices = {t: client.get_ohlcv(t, days=365) for t in tickers}

result = optimize_portfolio(
    prices,
    method="max_sharpe",
    returns_model="ema_historical",
    risk_model="ledoit_wolf",
    total_value=100_000,
    risk_free_rate=0.04,
)

print(format_report(result))
# result.weights             → {"SPY": 0.10, "QQQ": 0.42, ...}
# result.discrete_allocation → {"SPY": 17, "QQQ": 86, ...}
# result.leftover_cash       → 214.56
# result.sharpe_ratio        → 0.893
```

The dict-of-OHLCV input format matches what `SchwabClient.get_ohlcv()`
returns, so integration with the rest of the agent is zero-friction.
HRP doesn't require a `returns_model` or `risk_model` (it uses raw
daily returns and hierarchical clustering), and HRP skips the
efficient-frontier optimizer entirely.

---

## Backtest validation

Statistical tests that quantify how much of a strategy's backtest
performance is signal vs luck. Runs on top of the regular backtester.

```bash
./run.sh validate momentum 2020-01-01 2024-12-31
```

Produces three independent verdicts:

| Test | What it asks | Verdict rating |
|------|--------------|----------------|
| **Monte Carlo permutation** | Is the observed Sharpe better than a random reordering of the same PnL? | `(significant)` `(marginal)` `(not significant)` |
| **Bootstrap Sharpe CI** | How stable is the Sharpe under resampling? | `(robust)` `(positive)` `(likely positive)` `(unreliable)` |
| **Walk-Forward** | Is the strategy profitable in most time windows, not just one lucky run? | `(very consistent)` `(consistent)` `(mixed)` `(inconsistent)` |

Monte Carlo operates on daily dollar PnLs (not percentage returns) so the
Sharpe test has real variance — the equity base shifts with path, and
strategies that depend on sequencing will score differently from random.

You can also call the validator directly:

```python
from schwabagent.backtest import Backtester, BacktestConfig
from schwabagent.backtest_validation import run_validation, format_report

result = Backtester(BacktestConfig(strategy="momentum")).run()
validation = run_validation(
    equity_curve=result.equity_curve,
    trades=result.trades,
    n_simulations=1000,
    n_bootstrap=1000,
    n_windows=5,
)
print(format_report(validation))
```

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

## LLM support

The `LLMClient` routes to one of three providers via `LLM_PROVIDER`:

| Provider | Config | Use case |
|----------|--------|----------|
| `ollama` | `OLLAMA_HOST`, `OLLAMA_MODEL` | Local, free, private |
| `anthropic` | `ANTHROPIC_API_KEY` or `LLM_API_KEY` | Claude API |
| `openai` | `OPENAI_API_KEY` or `LLM_API_KEY` | OpenAI or any compatible endpoint |

Common config:

```bash
LLM_ENABLED=true
LLM_PROVIDER=ollama                    # or: anthropic, openai
LLM_MODEL=qwen2.5:14b-instruct-q5_K_M  # override per provider
LLM_TEMPERATURE=0.2
LLM_MAX_TOKENS=1024
LLM_TIMEOUT=60
```

The LLM is used for ETF rotation commentary, signal reasoning, the
`intelligence/skills` reference library, and `swarm` multi-agent
workflows described above.

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
  config.py              Configuration (pydantic-settings, loaded from .env)
  schwab_client.py       Schwab API wrapper (dual client: account + market)
  runner.py              Main orchestrator (scan → execute loop)
  risk.py                Risk management + trading rules integration
  trading_rules.py       Brokerage rules engine (PDT, wash sale, closing-only)
  persistence.py         JSON/JSONL state storage
  indicators.py          Technical indicators (SMA, EMA, RSI, MACD, etc.)
  telegram.py            Telegram bot (alerts, commands, trade approval)
  pf.py                  Point & Figure charting (pypf + Schwab data)
  llm.py                 Multi-provider LLM client (Ollama/Anthropic/OpenAI)
  backtest.py            Historical backtester
  backtest_validation.py Monte Carlo + Bootstrap + Walk-Forward validation
  options.py             Black-Scholes pricing, IV solver, multi-leg strategies
  portfolio_optimizer.py PyPortfolioOpt wrapper (mean-variance, HRP, discrete alloc)
  ta_indicators.py       35+ technical indicators via the `ta` library (trend, momentum, volatility, volume)
  cli.py                 CLI entry point
  strategies/
    base.py              Abstract strategy interface + Signal enum
    TEMPLATE.py          Strategy template with design checklist
    etf_rotation.py      Dual momentum ETF rotation
    etf_scalp.py         Intraday ETF scalping (volume/price breakout)
    momentum.py          SMA/RSI/MACD momentum
    mean_reversion.py    Bollinger Bands mean reversion
    trend_following.py   EMA alignment + ADX trend
    composite.py         Multi-strategy consensus
    conviction_hold.py   Long-term hold on speculative positions
  intelligence/          LLM reasoning layer (vibe-trading port)
    skills.py            SkillsLoader with progressive disclosure
    skills_lib/          23 reference SKILL.md methodology documents
    swarm/               DAG-based multi-agent orchestration
      models.py          Dataclass-based SwarmAgentSpec / SwarmTask / SwarmRun
      task_store.py      DAG cycle detection + topological layering
      preset_loader.py   YAML preset loader
      worker.py          Single-task worker with skill injection
      runtime.py         Parallel layer execution (ThreadPoolExecutor)
      presets/           investment_committee / technical_analysis_panel /
                         etf_allocation_desk YAML presets
  web/                   FastAPI dashboard (src/static HTML/JS/CSS)
    app.py               REST + WebSocket endpoints
    static/              index.html / app.js / style.css
docs/
  schwab-api-reference.md   Full Schwab API field reference
```

---

## Development

```bash
uv sync --dev
uv run pytest          # run tests (544 tests, all passing)
uv run ruff check src  # lint
```

---

## Credits

The `intelligence/` module (skills library + swarm orchestration) and the
`backtest_validation.py` module are adapted from
[HKUDS/vibe-trading](https://github.com/hkuds/vibe-trading) (MIT licensed).
The original project provides a much broader multi-agent finance workspace
for Chinese and global markets. This port extracts the portable patterns
and adapts them to schwab-agent's equity/ETF/options focus:

- **Skills loader** — dataclass-based `SkillsLoader` with YAML frontmatter
  and progressive disclosure.
- **Skill corpus** — 23 of the 68 original skills, curated for US equities.
- **Swarm orchestration** — DAG scheduling, parallel layer execution, YAML
  preset format. Simplified to single-call agents (no ReAct loop) to fit
  schwab-agent's existing LLM client.
- **Backtest validation** — Monte Carlo, Bootstrap, Walk-Forward.
  Monte Carlo reworked to operate on dollar PnLs (not percent returns)
  so the Sharpe test produces meaningful variance.
- **Options pricing** — Black-Scholes + Greeks adapted from
  vibe-trading's `options_pricing_tool.py`, extended with an implied
  volatility bisection solver, multi-leg payoff analysis, and strategy
  constructors. Pure stdlib + numpy; no scipy dependency.
