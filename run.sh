#!/bin/bash
#
# run.sh — Start the Schwab trading agent
#
# Usage:
#   ./run.sh              Dry-run one scan cycle, show signals, exit
#   ./run.sh scan         Quick scan — show signals without executing
#   ./run.sh once         Dry-run one full cycle (scan + execute)
#   ./run.sh loop         Dry-run continuous (scans every 5 min)
#   ./run.sh live         REAL MONEY — places actual orders on Schwab
#   ./run.sh pnl          Show P&L summary by strategy
#   ./run.sh status       Check Schwab connectivity + agent config
#   ./run.sh web          Start web dashboard (http://localhost:8898)
#

set -e
cd "$(dirname "$0")"

export PYTHONPATH="src${PYTHONPATH:+:$PYTHONPATH}"
VENV=".venv/bin/python"

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

# ---------- helpers ----------

check_ollama() {
    if curl -s http://localhost:11434/api/tags > /dev/null 2>&1; then
        MODEL=$(grep OLLAMA_MODEL .env 2>/dev/null | cut -d= -f2)
        echo -e "  ${GREEN}✓${NC} Ollama running — model: ${CYAN}${MODEL:-qwen2.5:14b-instruct-q5_K_M}${NC}"
        return 0
    else
        echo -e "  ${YELLOW}!${NC} Ollama not running — LLM features disabled"
        echo -e "    Start it with: ${CYAN}ollama serve${NC}"
        return 1
    fi
}

check_schwab() {
    RESULT=$($VENV -c "
from schwabagent.config import Config
from schwabagent.schwab_client import SchwabClient
c = SchwabClient(Config())
ok = c.authenticate()
acct = '✓' if c._client else '✗'
mkt = '✓' if c._market_client else '✗'
print(f'{acct}|{mkt}')
" 2>/dev/null)
    ACCT=$(echo "$RESULT" | cut -d'|' -f1)
    MKT=$(echo "$RESULT" | cut -d'|' -f2)

    if [ "$ACCT" = "✓" ]; then
        echo -e "  ${GREEN}✓${NC} Schwab Account API connected"
    else
        echo -e "  ${RED}✗${NC} Schwab Account API not reachable — run ${CYAN}./run.sh enroll${NC}"
    fi
    if [ "$MKT" = "✓" ]; then
        echo -e "  ${GREEN}✓${NC} Schwab Market Data API connected"
    else
        echo -e "  ${RED}✗${NC} Schwab Market Data API not reachable — run ${CYAN}./run.sh enroll${NC}"
    fi
    [ "$ACCT" = "✓" ] || [ "$MKT" = "✓" ]
}

# ---------- commands ----------

cmd_enroll() {
    echo ""
    echo -e "${CYAN}=== Schwab OAuth Enrollment ===${NC}"
    echo ""
    echo "  Which API to enroll?"
    echo ""
    echo "    1) Account API  — positions, balances, orders"
    echo "    2) Market API   — quotes, price history, OHLCV"
    echo "    3) Both"
    echo ""
    read -p "  Choose [1/2/3]: " choice

    case "$choice" in
        1) WHICH="account" ;;
        2) WHICH="market" ;;
        3) WHICH="both" ;;
        *) echo "  Invalid choice."; return ;;
    esac

    $VENV -c "
import sys
from schwabagent.config import Config
from schwabagent.schwab_client import SchwabClient
c = SchwabClient(Config())
ok = c.enroll('$WHICH')
if not ok:
    print('  \033[0;31m✗\033[0m Enrollment failed')
    sys.exit(1)
print()
# Verify by loading from token files
c2 = SchwabClient(Config())
c2.authenticate()
if c2._client:
    print('  \033[0;32m✓\033[0m Account API ready')
    accounts = c2.get_all_accounts()
    if accounts:
        for a in accounts:
            print(f'    {a.account_number}  value=\${a.total_value:,.2f}  cash=\${a.cash_available:,.2f}')
else:
    print('  \033[1;33m!\033[0m Account API not enrolled')
if c2._market_client and c2._market_client is not c2._client:
    print('  \033[0;32m✓\033[0m Market Data API ready')
elif not c2._client:
    print('  \033[1;33m!\033[0m Market Data API not enrolled')
"
    echo ""
}

cmd_status() {
    echo ""
    echo -e "${CYAN}=== Schwab Agent Status ===${NC}"
    echo ""
    check_ollama
    check_schwab
    echo ""

    echo -e "  Config:"
    echo -e "    DRY_RUN      = $(grep '^DRY_RUN' .env 2>/dev/null | cut -d= -f2)"
    echo -e "    WATCHLIST    = $(grep '^WATCHLIST' .env 2>/dev/null | cut -d= -f2)"
    echo -e "    STRATEGIES   = $(grep '^STRATEGIES' .env 2>/dev/null | cut -d= -f2)"
    echo -e "    MAX_EXPOSURE = \$$(grep '^MAX_TOTAL_EXPOSURE' .env 2>/dev/null | cut -d= -f2)"
    echo -e "    LLM          = $(grep '^LLM_ENABLED' .env 2>/dev/null | cut -d= -f2)"
    echo ""
    echo -e "  Live trading by strategy:"
    for STRAT in ETF_ROTATION MOMENTUM MEAN_REVERSION TREND_FOLLOWING COMPOSITE ETF_SCALP CONVICTION_HOLD; do
        VAL=$(grep "^LIVE_${STRAT}" .env 2>/dev/null | cut -d= -f2)
        if [ "$VAL" = "true" ]; then
            echo -e "    LIVE_${STRAT} = ${GREEN}true${NC}"
        else
            echo -e "    LIVE_${STRAT} = ${YELLOW}false${NC}"
        fi
    done
    echo ""

    # Show account summary if connected
    $VENV -c "
import sys
try:
    from schwabagent.config import Config
    from schwabagent.schwab_client import SchwabClient
    c = SchwabClient(Config())
    if not c.authenticate():
        print('  Accounts: not authenticated')
        sys.exit(0)
    accounts = c.get_all_accounts()
    print(f'  Accounts ({len(accounts)}):')
    for a in accounts:
        print(f'    {a.account_number}  value=\${a.total_value:,.2f}  cash=\${a.cash_available:,.2f}  positions={len(a.positions)}')
except Exception as e:
    print(f'  Account query failed: {e}')
" 2>/dev/null
    echo ""
}

cmd_scan() {
    echo ""
    echo -e "${CYAN}=== Scanning watchlist ===${NC}"
    echo ""
    $VENV -m schwabagent.cli --scan --dry-run
    echo ""
}

cmd_once() {
    echo ""
    echo -e "${CYAN}=== Dry-run: one scan cycle ===${NC}"
    echo ""
    $VENV -m schwabagent.cli --dry-run --once
}

cmd_loop() {
    echo ""
    echo -e "${CYAN}=== Dry-run: continuous mode (Ctrl+C to stop) ===${NC}"
    echo ""
    INTERVAL=$(grep '^SCAN_INTERVAL_SECONDS' .env 2>/dev/null | cut -d= -f2)
    INTERVAL=${INTERVAL:-300}
    $VENV -m schwabagent.cli --dry-run --interval="$INTERVAL"
}

cmd_live() {
    echo ""
    echo -e "${RED}╔═══════════════════════════════════════╗${NC}"
    echo -e "${RED}║  WARNING: LIVE TRADING — REAL MONEY   ║${NC}"
    echo -e "${RED}╚═══════════════════════════════════════╝${NC}"
    echo ""
    echo -e "This will place real orders on your Charles Schwab account."
    echo -e "Max exposure: \$$(grep MAX_TOTAL_EXPOSURE .env 2>/dev/null | cut -d= -f2)"
    echo -e "Max per position: \$$(grep MAX_POSITION_VALUE .env 2>/dev/null | cut -d= -f2)"
    echo ""
    read -p "Type 'yes' to continue: " confirm
    if [ "$confirm" != "yes" ]; then
        echo "Aborted."
        exit 0
    fi
    echo ""
    INTERVAL=$(grep '^SCAN_INTERVAL_SECONDS' .env 2>/dev/null | cut -d= -f2)
    INTERVAL=${INTERVAL:-300}
    $VENV -m schwabagent.cli --live --interval="$INTERVAL"
}

cmd_pnl() {
    echo ""
    echo -e "${CYAN}=== Schwab Agent P&L Summary ===${NC}"
    echo ""
    $VENV -c "
import json, os
from pathlib import Path

state_dir = Path(os.environ.get('STATE_DIR', '~/.schwab-agent')).expanduser()

# Strategy P&L
pnl_file = state_dir / 'strategy_pnl.json'
if pnl_file.exists():
    data = json.loads(pnl_file.read_text())
    print(f'  {\"Strategy\":<22} {\"Trades\":>8} {\"Realized P&L\":>14} {\"Wins\":>6} {\"Losses\":>7} {\"Win%\":>6}')
    print('  ' + '-' * 68)
    total_pnl = 0.0
    for strat, d in sorted(data.items()):
        wr = d['wins'] / d['trades'] * 100 if d['trades'] > 0 else 0
        sign = '+' if d['realized_pnl'] >= 0 else ''
        print(f'  {strat:<22} {d[\"trades\"]:>8} {sign}{d[\"realized_pnl\"]:>13.2f} {d[\"wins\"]:>6} {d[\"losses\"]:>7} {wr:>5.1f}%')
        total_pnl += d['realized_pnl']
    print('  ' + '-' * 68)
    sign = '+' if total_pnl >= 0 else ''
    print(f'  {\"TOTAL\":<22} {\"\":>8} {sign}{total_pnl:>13.2f}')
else:
    print('  No P&L data yet.')

# Recent trades
hist = state_dir / 'trade_history.jsonl'
if hist.exists():
    lines = [l for l in hist.read_text().strip().split('\n') if l]
    print(f'\n  Recent trades ({len(lines)} total):')
    print(f'  {\"Time\":<20} {\"Strategy\":<18} {\"Symbol\":<8} {\"Side\":<5} {\"Qty\":>5} {\"Price\":>8} {\"Value\":>10}')
    print('  ' + '-' * 80)
    for line in lines[-20:]:
        t = json.loads(line)
        ts = t.get('timestamp', '')[:19].replace('T', ' ')
        value = t.get('quantity', 0) * t.get('price', 0)
        print(f'  {ts:<20} {t.get(\"strategy\",\"\"):<18} {t.get(\"symbol\",\"\"):<8} {t.get(\"side\",\"\"):<5} {t.get(\"quantity\",0):>5} {t.get(\"price\",0):>8.2f} {value:>10.2f}')
else:
    print('\n  No trade history yet.')
print()
" 2>/dev/null
}

cmd_backtest() {
    STRATEGY="${2:-all}"
    START="${3:-2020-01-01}"
    END="${4:-2025-12-31}"
    echo ""
    echo -e "${CYAN}=== Backtesting: ${STRATEGY} (${START} → ${END}) ===${NC}"
    echo ""

    $VENV -c "
from schwabagent.backtest import Backtester, BacktestConfig

symbols = ['AAPL','MSFT','GOOGL','AMZN','NVDA','META','TSLA','JPM','V','UNH']
strategies = ['${STRATEGY}'] if '${STRATEGY}' != 'all' else ['momentum','mean_reversion','trend_following','composite']

for strat in strategies:
    config = BacktestConfig(
        strategy=strat,
        symbols=symbols,
        start='${START}',
        end='${END}',
        initial_capital=100000,
        data_path='data/sp500_stocks.csv',
    )
    bt = Backtester(config)
    result = bt.run()
    result.print_report()
    print()
"
}

cmd_optimize() {
    # Portfolio optimization via PyPortfolioOpt.
    # Usage:
    #   ./run.sh optimize <tickers>                             # max Sharpe, $100k, 1yr data
    #   ./run.sh optimize SPY,QQQ,TLT,GLD method=min_volatility
    #   ./run.sh optimize SPY,QQQ,TLT,GLD capital=50000 days=250
    #   ./run.sh optimize SPY,QQQ,TLT,GLD method=hrp
    TICKERS="${2:-}"
    shift 2 2>/dev/null || true

    if [ -z "$TICKERS" ]; then
        echo ""
        echo "  Portfolio optimization via PyPortfolioOpt"
        echo ""
        echo -e "  ${CYAN}./run.sh optimize <tickers> [method=<m>] [capital=<amount>] [days=<n>] [rate=<r>]${NC}"
        echo ""
        echo "    tickers   Comma-separated symbols (e.g. SPY,QQQ,TLT,GLD)"
        echo "    method    max_sharpe (default) | min_volatility | hrp"
        echo "    capital   Total portfolio value in dollars (default: 100000)"
        echo "    days      Calendar days of history to fetch (default: 365)"
        echo "    rate      Risk-free rate as decimal (default: 0.04)"
        echo ""
        echo "  Example:"
        echo "    ./run.sh optimize SPY,QQQ,IWM,EFA,TLT,GLD,VNQ method=max_sharpe capital=250000"
        echo ""
        return
    fi

    METHOD="max_sharpe"
    CAPITAL="100000"
    DAYS="365"
    RATE="0.04"
    for arg in "$@"; do
        KEY="${arg%%=*}"
        VAL="${arg#*=}"
        case "$KEY" in
            method)  METHOD="$VAL" ;;
            capital) CAPITAL="$VAL" ;;
            days)    DAYS="$VAL" ;;
            rate)    RATE="$VAL" ;;
        esac
    done

    echo ""
    echo -e "${CYAN}=== Portfolio optimization: ${TICKERS} (${METHOD}) ===${NC}"
    echo ""

    $VENV -c "
from schwabagent.config import Config
from schwabagent.schwab_client import SchwabClient
from schwabagent.portfolio_optimizer import optimize_portfolio, format_report

config = Config()
client = SchwabClient(config)
if not client.authenticate():
    print('  Schwab API not authenticated — run ./run.sh enroll first.')
    import sys; sys.exit(1)

tickers = [t.strip().upper() for t in '${TICKERS}'.split(',') if t.strip()]
days = int('${DAYS}')

print(f'  Fetching {days}d of history for {len(tickers)} symbols...')
prices = {}
for t in tickers:
    try:
        df = client.get_ohlcv(t, days=days)
        if df is not None and not df.empty:
            prices[t] = df
            print(f'    {t:<6} ok ({len(df)} bars)')
        else:
            print(f'    {t:<6} ${RED}empty${NC}')
    except Exception as e:
        print(f'    {t:<6} ${RED}error: {e}${NC}')

if len(prices) < 2:
    print('  Not enough data to optimize.')
    import sys; sys.exit(1)

print()
result = optimize_portfolio(
    prices,
    method='${METHOD}',
    total_value=float('${CAPITAL}'),
    risk_free_rate=float('${RATE}'),
)
print(format_report(result))
"
}

cmd_options() {
    # Options pricing and strategy analysis.
    # Usage:
    #   ./run.sh options price   spot=500 strike=500 dte=30 iv=0.20 type=call
    #   ./run.sh options iv      price=12.5 spot=500 strike=500 dte=30 type=call
    #   ./run.sh options strategy iron_condor puts=460,470 calls=530,540 premiums=0.8,1.5,1.6,0.75
    #
    # Strategies supported: long_call, long_put, long_straddle, long_strangle,
    #   bull_call, bear_put, bull_put, bear_call, iron_condor, long_butterfly,
    #   covered_call, protective_put
    SUB="${2:-help}"
    shift 2 2>/dev/null || true

    if [ "$SUB" = "help" ] || [ -z "$SUB" ]; then
        cat <<EOF

  Options pricing and strategy analysis

  ${CYAN}./run.sh options price spot=<S> strike=<K> dte=<days> iv=<vol> type=<call|put> [rate=<r>]${NC}
    Black-Scholes price and Greeks for a single option.
    Example: ./run.sh options price spot=500 strike=505 dte=30 iv=0.20 type=call

  ${CYAN}./run.sh options iv price=<P> spot=<S> strike=<K> dte=<days> type=<call|put> [rate=<r>]${NC}
    Implied volatility from an observed market price.
    Example: ./run.sh options iv price=3.50 spot=500 strike=505 dte=30 type=call

  ${CYAN}./run.sh options strategy <name> <args>${NC}
    Multi-leg strategy analysis (payoff, max profit/loss, breakevens).

    Named strategies (all take --premiums= as a comma list in leg order):
      long_call strike=100 premium=5
      long_put strike=100 premium=5
      long_straddle strike=100 call=5 put=5
      long_strangle call_strike=110 put_strike=90 call=2 put=2
      bull_call long=100 short=110 long_prem=6 short_prem=2
      bear_put long=100 short=90 long_prem=5 short_prem=1
      bull_put short=100 long=90 short_prem=4 long_prem=1
      bear_call short=100 long=110 short_prem=4 long_prem=1
      iron_condor pl=90 ps=95 cs=105 cl=110 pl_prem=0.5 ps_prem=1.5 cs_prem=1.5 cl_prem=0.5
      long_butterfly lower=95 mid=100 upper=105 lower_prem=6 mid_prem=3 upper_prem=1
      covered_call strike=110 call_prem=2 stock=100
      protective_put strike=95 put_prem=3 stock=100

EOF
        return
    fi

    # Build a Python one-liner that reads the positional args into kwargs
    ARGS_PY=""
    for arg in "$@"; do
        KEY="${arg%%=*}"
        VAL="${arg#*=}"
        ARGS_PY="${ARGS_PY}    '${KEY}': '${VAL}',\n"
    done

    case "$SUB" in
        price)
            $VENV -c "
from schwabagent.options import bs_price_and_greeks
args = {
$(printf "${ARGS_PY}")
}
spot = float(args['spot'])
strike = float(args['strike'])
dte = float(args['dte'])
iv = float(args['iv'])
opt_type = args['type'].lower()
rate = float(args.get('rate', '0.05'))
T = dte / 365.0
r = bs_price_and_greeks(spot, strike, T, rate, iv, opt_type)
print()
print(f'  {opt_type.upper()} spot=\${spot:.2f} strike=\${strike:.2f} dte={int(dte)}d iv={iv:.2%}')
print()
for k, v in r.items():
    print(f'    {k:6} = {v}')
print()
"
            ;;
        iv)
            $VENV -c "
from schwabagent.options import implied_volatility
args = {
$(printf "${ARGS_PY}")
}
price = float(args['price'])
spot = float(args['spot'])
strike = float(args['strike'])
dte = float(args['dte'])
opt_type = args['type'].lower()
rate = float(args.get('rate', '0.05'))
T = dte / 365.0
iv = implied_volatility(price, spot, strike, T, rate, opt_type)
print()
if iv is None:
    print('  Error: market price is outside arbitrage bounds')
else:
    print(f'  Implied volatility: {iv:.4f} ({iv*100:.2f}%)')
print()
"
            ;;
        strategy)
            # First positional arg after 'strategy' is the strategy name
            STRAT="${1:-}"
            shift || true
            ARGS_PY=""
            for arg in "$@"; do
                KEY="${arg%%=*}"
                VAL="${arg#*=}"
                ARGS_PY="${ARGS_PY}    '${KEY}': '${VAL}',\n"
            done
            $VENV -c "
from schwabagent import options as O
args = {
$(printf "${ARGS_PY}")
}
strat = '${STRAT}'
try:
    if strat == 'long_call':
        legs = O.long_call(float(args['strike']), float(args['premium']))
    elif strat == 'long_put':
        legs = O.long_put(float(args['strike']), float(args['premium']))
    elif strat == 'long_straddle':
        legs = O.long_straddle(float(args['strike']), float(args['call']), float(args['put']))
    elif strat == 'long_strangle':
        legs = O.long_strangle(float(args['call_strike']), float(args['put_strike']), float(args['call']), float(args['put']))
    elif strat == 'bull_call':
        legs = O.bull_call_spread(float(args['long']), float(args['short']), float(args['long_prem']), float(args['short_prem']))
    elif strat == 'bear_put':
        legs = O.bear_put_spread(float(args['long']), float(args['short']), float(args['long_prem']), float(args['short_prem']))
    elif strat == 'bull_put':
        legs = O.bull_put_spread(float(args['short']), float(args['long']), float(args['short_prem']), float(args['long_prem']))
    elif strat == 'bear_call':
        legs = O.bear_call_spread(float(args['short']), float(args['long']), float(args['short_prem']), float(args['long_prem']))
    elif strat == 'iron_condor':
        legs = O.iron_condor(
            float(args['pl']), float(args['ps']), float(args['cs']), float(args['cl']),
            float(args['pl_prem']), float(args['ps_prem']), float(args['cs_prem']), float(args['cl_prem']),
        )
    elif strat == 'long_butterfly':
        legs = O.long_butterfly(
            float(args['lower']), float(args['mid']), float(args['upper']),
            float(args['lower_prem']), float(args['mid_prem']), float(args['upper_prem']),
        )
    elif strat == 'covered_call':
        legs = O.covered_call(float(args['strike']), float(args['call_prem']), float(args['stock']))
    elif strat == 'protective_put':
        legs = O.protective_put(float(args['strike']), float(args['put_prem']), float(args['stock']))
    else:
        print(f'  Unknown strategy: {strat}')
        import sys; sys.exit(1)

    # Determine spot range from the legs
    strikes = [leg.strike for leg in legs if leg.strike > 0]
    if strikes:
        lo = min(strikes) * 0.85
        hi = max(strikes) * 1.15
    else:
        lo, hi = 80, 120

    m = O.strategy_metrics(legs, spot_range=(lo, hi), n_points=5001)

    print()
    print(f'  ${CYAN}{strat}${NC}')
    print()
    print(f'    Legs ({len(legs)}):')
    for leg in legs:
        sign = '+' if leg.side == 'long' else '-'
        print(f'      {sign} {leg.quantity}x {leg.option_type:4} strike=\${leg.strike:.2f} @ \${leg.premium:.2f}')
    print()
    credit_or_debit = 'debit' if m['net_debit'] > 0 else 'credit'
    print(f'    Net {credit_or_debit:7} \${abs(m[\"net_debit\"]):.2f}')
    print(f'    Max profit    \${m[\"max_profit\"]:.2f}')
    print(f'    Max loss      \${m[\"max_loss\"]:.2f}')
    if m['breakevens']:
        bes = ', '.join(f'\${b:.2f}' for b in m['breakevens'])
        print(f'    Breakevens    {bes}')
    print()
except (KeyError, ValueError) as e:
    print(f'  Error: {e}')
    import sys; sys.exit(1)
"
            ;;
        *)
            echo "  Unknown subcommand: $SUB"
            echo "  Try: ./run.sh options help"
            return 1
            ;;
    esac
}

cmd_validate() {
    # Backtest + statistical validation (Monte Carlo, Bootstrap, Walk-Forward).
    # Usage: ./run.sh validate <strategy> [start] [end]
    STRATEGY="${2:-momentum}"
    START="${3:-2020-01-01}"
    END="${4:-2025-12-31}"
    echo ""
    echo -e "${CYAN}=== Validating: ${STRATEGY} (${START} → ${END}) ===${NC}"
    echo ""

    $VENV -c "
from schwabagent.backtest import Backtester, BacktestConfig
from schwabagent.backtest_validation import run_validation, format_report

symbols = ['AAPL','MSFT','GOOGL','AMZN','NVDA','META','TSLA','JPM','V','UNH']
config = BacktestConfig(
    strategy='${STRATEGY}',
    symbols=symbols,
    start='${START}',
    end='${END}',
    initial_capital=100000,
    data_path='data/sp500_stocks.csv',
)
bt = Backtester(config)
result = bt.run()
result.print_report()

print()
print('-' * 60)
print()

if len(result.equity_curve) < 10:
    print('  Not enough equity observations for validation.')
else:
    validation_results = run_validation(
        equity_curve=result.equity_curve,
        trades=result.trades,
        n_simulations=1000,
        n_bootstrap=1000,
        n_windows=5,
    )
    print(format_report(validation_results))
"
}

cmd_dream() {
    echo ""
    echo -e "${CYAN}=== Dreamcycle — autonomous research cycle ===${NC}"
    echo ""
    $VENV -c "
from schwabagent.config import Config
from schwabagent.runner import AgentRunner

config = Config()
runner = AgentRunner(config)
result = runner.dreamcycle.run_once()

print(f'  Duration: {result.duration_seconds:.1f}s')
print(f'  Phases OK: {result.phases_completed}')
if result.phases_failed:
    print(f'  Phases FAILED: {result.phases_failed}')
print(f'  Signals recorded: {result.signals_recorded}')
print(f'  Drift alerts: {result.drift_alerts}')
print(f'  Auto-tune actions: {result.auto_tune_actions}')
print(f'  Position mismatches: {result.position_mismatches}')
print(f'  Digest sent: {result.digest_sent}')
if result.errors:
    print(f'  Errors:')
    for e in result.errors:
        print(f'    - {e}')
print()
"
}

cmd_feedback() {
    DAYS="${2:-30}"
    $VENV -c "
from schwabagent.config import Config
from schwabagent.feedback import FeedbackLoop

fb = FeedbackLoop(Config())

# Strategy summary
summary = fb.get_strategy_summary(days=${DAYS})
if not summary:
    print('  No signal data yet. Run scans to populate.')
    exit(0)

print()
print(f'  Signal Feedback — last ${DAYS} days')
print()
print(f'  {\"Strategy\":<22} {\"Signals\":>8} {\"Resolved\":>9} {\"Wins\":>6} {\"Losses\":>7} {\"Win%\":>6} {\"Avg P&L\":>10} {\"Total P&L\":>11}')
print('  ' + '-' * 88)
for strat, d in sorted(summary.items()):
    total = d['total_signals']
    resolved = d['resolved'] or 0
    wins = d['wins'] or 0
    losses = d['losses'] or 0
    wr = wins / resolved * 100 if resolved > 0 else 0
    avg = d['avg_pnl'] or 0
    total_pnl = d['total_pnl'] or 0
    print(f'  {strat:<22} {total:>8} {resolved:>9} {wins:>6} {losses:>7} {wr:>5.1f}% {avg:>+10.2f} {total_pnl:>+11.2f}')
print()

# Drift alerts
alerts = fb.get_drift_alerts(days=7)
if alerts:
    print('  Drift Alerts (last 7 days):')
    for a in alerts:
        print(f'    [{a[\"alert_level\"].upper()}] {a[\"strategy\"]}: {a[\"metric\"]} '
              f'baseline={a[\"baseline_value\"]:.1f} → current={a[\"current_value\"]:.1f} '
              f'({a[\"deviation_pct\"]:+.1f}%)')
    print()

# Calibration
print('  Calibration by signal type:')
cal = fb.calibrate_all(days=${DAYS})
for strat, signals in sorted(cal.items()):
    if not signals:
        continue
    print(f'    {strat}:')
    for sig, c in sorted(signals.items()):
        if c['total'] < 3:
            continue
        pf = f'{c[\"profit_factor\"]:.2f}' if c['profit_factor'] != float('inf') else 'inf'
        print(f'      {sig:<14} n={c[\"total\"]:>4}  wr={c[\"win_rate\"]:>5.1f}%  pf={pf:>6}  avg=\${c[\"avg_pnl\"]:>+8.2f}')
    print()
"
}

cmd_skills() {
    $VENV -c "
from schwabagent.skills import SkillsManager

mgr = SkillsManager()
skills = mgr.list_skills()

if not skills:
    print('  No skills found in ~/.schwab-agent/skills/')
    exit(0)

# Group by category
cats = {}
for s in skills:
    cats.setdefault(s.category or '(uncategorized)', []).append(s)

for cat, items in sorted(cats.items()):
    print(f'\n  {cat}/')
    for s in items:
        tags = ', '.join(s.tags[:3]) if s.tags else ''
        tag_str = f'  [{tags}]' if tags else ''
        print(f'    {s.name:<24} {s.description[:60]}{tag_str}')
print()
"
}

cmd_sec() {
    SYMBOL="${2:-AAPL}"
    ACTION="${3:-filings}"

    $VENV -c "
import sys
from schwabagent.sec import SECAnalyzer
from schwabagent.config import Config

sec = SECAnalyzer(Config())
symbol = '${SYMBOL}'.upper()
action = '${ACTION}'

if action == 'filings':
    for form in ['10-K', '10-Q', '8-K']:
        filings = sec.get_filings(symbol, form=form, limit=3)
        if filings:
            print(f'\n  {form}:')
            for f in filings:
                print(f'    {f.filing_date}  {f.description[:60] if f.description else f.accession_number}')

elif action == 'analyze':
    print(f'  Analyzing latest 10-K for {symbol}...')
    result = sec.analyze_filing(symbol, form='10-K')
    print(f'\n  Filing date: {result.filing_date}')
    print(f'  Text length: {result.raw_text_length:,} chars')
    print(f'  Sections: {result.sections_extracted}')
    print(f'\n  Summary: {result.summary}')
    print(f'\n  Financials: {result.key_financials}')
    print(f'\n  Risks: {result.risk_assessment}')
    print(f'\n  Sentiment: {result.sentiment}')
    if result.actionable_insights:
        print(f'\n  Insights:')
        for ins in result.actionable_insights:
            print(f'    - {ins}')

elif action == 'risks':
    print(f'  Extracting risk factors for {symbol}...')
    risks = sec.extract_risk_factors(symbol)
    print(f'\n{risks}')

elif action == 'compare':
    print(f'  Comparing last two 10-Q filings for {symbol}...')
    comparison = sec.compare_filings(symbol, form='10-Q')
    print(f'\n{comparison}')

elif action == 'scan':
    symbols = '${SYMBOL}'.split(',')
    print(f'  Scanning recent 8-K filings for {symbols}...')
    results = sec.quick_scan(symbols, form='8-K')
    for r in results[:20]:
        print(f'    {r[\"date\"]}  {r[\"symbol\"]}  {r[\"form\"]}  {r[\"description\"][:60]}')

else:
    print(f'  Unknown action: {action}')
    print(f'  Usage: ./run.sh sec SYMBOL [filings|analyze|risks|compare|scan]')
"
}

cmd_pf() {
    SYMBOL="${2:-SPY}"
    shift 2 2>/dev/null || shift 1 2>/dev/null || true

    # Parse optional flags
    BOX="0.01"; REV="3"; DUR="1.0"; PER="2.0"; METHOD="HL"; STYLE="True"; TRENDS="True"; META="False"
    while [ $# -gt 0 ]; do
        case "$1" in
            -b|--box-size)    BOX="$2"; shift 2 ;;
            -r|--reversal)    REV="$2"; shift 2 ;;
            -d|--duration)    DUR="$2"; shift 2 ;;
            -p|--period)      PER="$2"; shift 2 ;;
            -m|--method)      METHOD="$2"; shift 2 ;;
            --no-style)       STYLE="False"; shift ;;
            --no-trends)      TRENDS="False"; shift ;;
            --meta)           META="True"; shift ;;
            *)                shift ;;
        esac
    done

    $VENV -c "
from schwabagent.config import Config
from schwabagent.schwab_client import SchwabClient
from schwabagent.pf import print_pf_chart

config = Config()
client = SchwabClient(config)
if not client.authenticate():
    print('Auth failed')
    exit(1)

print_pf_chart(
    symbol='${SYMBOL}'.upper(),
    client=client,
    box_size=${BOX},
    reversal=${REV},
    duration=${DUR},
    period=${PER},
    method='${METHOD}',
    style=${STYLE},
    trend_lines=${TRENDS},
    show_meta=${META},
)
"
}

cmd_web() {
    echo -e "\n${CYAN}Starting web dashboard on port 8898...${NC}\n"
    $VENV -m schwabagent.cli --web
}

cmd_ref() {
    # Reference skills (bundled with package, ported from vibe-trading).
    # Usage: ./run.sh ref            → list all skills grouped by category
    #        ./run.sh ref <name>     → show full content for a specific skill
    NAME="${1:-}"
    $VENV -c "
import sys
from schwabagent.intelligence import SkillsLoader
loader = SkillsLoader()
name = '${NAME}'
if name:
    print(loader.get_content(name))
else:
    print(f'\n  {len(loader.skills)} reference skills loaded\n')
    print(loader.get_descriptions())
    print()
"
}

cmd_swarm() {
    # Multi-agent committee workflows (ported from vibe-trading).
    # Usage:
    #   ./run.sh swarm                        → list available presets
    #   ./run.sh swarm <preset>               → describe a preset
    #   ./run.sh swarm <preset> key=val ...   → execute a preset
    #
    # Example:
    #   ./run.sh swarm investment_committee target=SPY
    #   ./run.sh swarm technical_analysis_panel target=AAPL timeframe=daily
    PRESET="${1:-}"
    shift || true

    if [ -z "$PRESET" ]; then
        $VENV -c "
from schwabagent.intelligence.swarm import list_presets, load_preset_by_name
presets = list_presets()
if not presets:
    print('  No presets found.')
else:
    print(f'\n  {len(presets)} swarm presets available:\n')
    for name in presets:
        p = load_preset_by_name(name)
        print(f'  ${CYAN}{p.name}${NC}')
        print(f'    {p.title}')
        print(f'    {p.description}')
        print()
    print('  Usage: ./run.sh swarm <preset> key=value ...')
    print()
"
        return
    fi

    # If no key=value args, just describe the preset
    if [ $# -eq 0 ]; then
        $VENV -c "
from schwabagent.intelligence.swarm import load_preset_by_name, topological_layers
p = load_preset_by_name('${PRESET}')
print(f'\n  {p.title}')
print(f'  {p.description}\n')
print(f'  Agents ({len(p.agents)}):')
for a in p.agents:
    print(f'    - {a.id:<24} {a.role}')
print(f'\n  Tasks ({len(p.tasks)}):')
for t in p.tasks:
    deps = ', '.join(t.depends_on) if t.depends_on else '(no deps)'
    print(f'    - {t.id:<24} agent={t.agent_id:<24} deps=[{deps}]')
layers = topological_layers(p.tasks)
print(f'\n  Execution layers: {len(layers)}')
for i, layer in enumerate(layers):
    print(f'    layer {i+1}: {layer}')
print(f'\n  Variables:')
for v in p.variables:
    req = 'required' if v.get('required') else f\"default='{v.get('default', '')}'\"
    print(f\"    - {v['name']:<16} ({req}) — {v.get('description', '')}\")
print()
"
        return
    fi

    # Execute: build user_vars from key=value args
    VARS_PY=""
    for arg in "$@"; do
        KEY="${arg%%=*}"
        VAL="${arg#*=}"
        VARS_PY="${VARS_PY}    '${KEY}': '''${VAL}''',\n"
    done

    $VENV -c "
from schwabagent.intelligence.swarm import SwarmRuntime, load_preset_by_name
from schwabagent.config import Config
from schwabagent.llm import LLMClient

config = Config()
llm = LLMClient(
    provider=config.LLM_PROVIDER,
    model=config.LLM_MODEL or config.OLLAMA_MODEL,
    api_key=config.LLM_API_KEY or config.ANTHROPIC_API_KEY or config.OPENAI_API_KEY,
    base_url=config.LLM_BASE_URL or config.OLLAMA_HOST,
    timeout=config.LLM_TIMEOUT or config.OLLAMA_TIMEOUT,
    temperature=config.LLM_TEMPERATURE,
    max_tokens=config.LLM_MAX_TOKENS,
)

if not llm.is_available():
    print(f'  LLM provider {llm.provider} not available — check .env')
    import sys; sys.exit(1)

user_vars = {
$(printf "${VARS_PY}")
}

preset = load_preset_by_name('${PRESET}')
print(f'\n  Running {preset.title}...\n')
for k, v in user_vars.items():
    print(f'    {k} = {v}')
print()

runtime = SwarmRuntime(llm)
run = runtime.execute(preset, user_vars=user_vars)

print()
print(f'  Status: {run.status.value}')
print(f'  Tasks completed: {sum(1 for t in run.tasks if t.status.value == \"completed\")}/{len(run.tasks)}')
print()

for task in run.tasks:
    print(f'  ─── {task.id} ({task.agent_id}) ───')
    if task.error:
        print(f'  ERROR: {task.error}')
    elif task.summary:
        # Indent the summary
        for line in task.summary.split('\n'):
            print(f'    {line}')
    print()

if run.final_report:
    print('  ══════ FINAL REPORT ══════')
    for line in run.final_report.split('\n'):
        print(f'  {line}')
    print()
"
}

# ---------- main ----------

case "${1:-once}" in
    enroll)  cmd_enroll ;;
    status)  cmd_status ;;
    scan)    cmd_scan ;;
    once)    cmd_once ;;
    loop)    cmd_loop ;;
    live)    cmd_live ;;
    pnl)     cmd_pnl ;;
    pf)      cmd_pf "$@" ;;
    skills)  cmd_skills ;;
    feedback) cmd_feedback "$@" ;;
    backtest) cmd_backtest "$@" ;;
    validate) cmd_validate "$@" ;;
    options)  cmd_options "$@" ;;
    optimize) cmd_optimize "$@" ;;
    dream)   cmd_dream ;;
    sec)     cmd_sec "$@" ;;
    web)     cmd_web ;;
    ref)     shift; cmd_ref "$@" ;;
    swarm)   shift; cmd_swarm "$@" ;;
    *)
        echo "Usage: ./run.sh [enroll|status|scan|once|loop|live|pnl|pf|skills|feedback|backtest|validate|options|optimize|dream|sec|web|ref|swarm]"
        echo ""
        echo "  enroll   Authenticate with Schwab (OAuth browser flow)"
        echo "  status   Check Schwab connectivity + agent config"
        echo "  scan     Show signals for watchlist (no execution)"
        echo "  once     Dry-run one full scan+execute cycle (default)"
        echo "  loop     Dry-run continuous (scans on interval)"
        echo "  live     REAL MONEY mode — places actual orders"
        echo "  pnl      Show realized P&L by strategy"
        echo "  pf       Point & Figure chart (e.g. ./run.sh pf AAPL)"
        echo "  skills   List available skills"
        echo "  feedback Show signal accuracy, calibration, and drift alerts"
        echo "  backtest Run strategy backtest (e.g. ./run.sh backtest momentum 2020-01-01 2024-12-31)"
        echo "  validate Backtest + statistical validation (Monte Carlo + Bootstrap + Walk-Forward)"
        echo "  options  Options pricing (price|iv|strategy — Black-Scholes, IV solver, multi-leg)"
        echo "  optimize Portfolio optimization via PyPortfolioOpt (max Sharpe, min vol, HRP)"
        echo "  dream    Run one dreamcycle (autonomous research + calibration)"
        echo "  sec      SEC filings (e.g. ./run.sh sec AAPL [filings|analyze|risks|compare|scan])"
        echo "  web      Start web dashboard (http://localhost:8898)"
        echo "  ref      Reference skill library (./run.sh ref [skill-name])"
        echo "  swarm    Multi-agent committee workflows (./run.sh swarm [preset] [key=val ...])"
        ;;
esac
