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
#

set -e
cd "$(dirname "$0")"

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
    if $VENV -c "
from schwabagent.config import Config
from schwabagent.schwab_client import SchwabClient
c = SchwabClient(Config())
ok = c.authenticate()
print('ok' if ok else 'fail')
" 2>/dev/null | grep -q "ok"; then
        echo -e "  ${GREEN}✓${NC} Schwab API connected"
        return 0
    else
        echo -e "  ${RED}✗${NC} Schwab API not reachable — check .env credentials and token file"
        return 1
    fi
}

# ---------- commands ----------

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

# ---------- main ----------

case "${1:-once}" in
    status)  cmd_status ;;
    scan)    cmd_scan ;;
    once)    cmd_once ;;
    loop)    cmd_loop ;;
    live)    cmd_live ;;
    pnl)     cmd_pnl ;;
    *)
        echo "Usage: ./run.sh [status|scan|once|loop|live|pnl]"
        echo ""
        echo "  status   Check Schwab connectivity + agent config"
        echo "  scan     Show signals for watchlist (no execution)"
        echo "  once     Dry-run one full scan+execute cycle (default)"
        echo "  loop     Dry-run continuous (scans on interval)"
        echo "  live     REAL MONEY mode — places actual orders"
        echo "  pnl      Show realized P&L by strategy"
        ;;
esac
