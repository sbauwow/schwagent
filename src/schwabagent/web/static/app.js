// ── State ────────────────────────────────────────────────────────────────────

let ws = null;
let accountsData = null;

// ── Tabs ─────────────────────────────────────────────────────────────────────

document.querySelectorAll('.tab').forEach(tab => {
  tab.addEventListener('click', () => {
    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
    tab.classList.add('active');
    document.getElementById('panel-' + tab.dataset.tab).classList.add('active');
    loadTab(tab.dataset.tab);
  });
});

function loadTab(name) {
  switch (name) {
    case 'dashboard': loadDashboard(); break;
    case 'positions': loadPositions(); break;
    case 'trades': loadTrades(); break;
    case 'pnl': loadPnl(); break;
  }
}

// ── Fetch helper ─────────────────────────────────────────────────────────────

async function api(endpoint) {
  const res = await fetch(endpoint);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

// ── Format helpers ───────────────────────────────────────────────────────────

function $(val, decimals = 2) {
  if (val == null) return '-';
  return '$' + Number(val).toLocaleString('en-US', {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  });
}

function pct(val, decimals = 1) {
  if (val == null) return '-';
  return Number(val).toFixed(decimals) + '%';
}

function pnlClass(val) {
  if (val > 0) return 'text-green';
  if (val < 0) return 'text-red';
  return 'text-dim';
}

function pnlSign(val, decimals = 2) {
  if (val == null) return '-';
  const sign = val >= 0 ? '+' : '';
  return sign + $(val, decimals);
}

// ── Dashboard ────────────────────────────────────────────────────────────────

async function loadDashboard() {
  try {
    const [acctData, statusData] = await Promise.all([
      api('/api/accounts'),
      api('/api/status'),
    ]);

    if (acctData.error) {
      toast('Error: ' + acctData.error);
      return;
    }

    accountsData = acctData;
    const totals = acctData.totals;
    const risk = statusData.risk || {};

    // KPI cards
    const kpiGrid = document.getElementById('kpi-grid');
    kpiGrid.innerHTML = `
      <div class="kpi">
        <div class="kpi-label">Total Value</div>
        <div class="kpi-value">${$(totals.total_value)}</div>
        <div class="kpi-sub">${totals.account_count} account${totals.account_count !== 1 ? 's' : ''}</div>
      </div>
      <div class="kpi">
        <div class="kpi-label">Cash Available</div>
        <div class="kpi-value">${$(totals.cash_available)}</div>
        <div class="kpi-sub">${pct(totals.total_value > 0 ? totals.cash_available / totals.total_value * 100 : 0)} of portfolio</div>
      </div>
      <div class="kpi">
        <div class="kpi-label">Invested</div>
        <div class="kpi-value">${$(totals.invested)}</div>
        <div class="kpi-sub">${totals.position_count} position${totals.position_count !== 1 ? 's' : ''}</div>
      </div>
      <div class="kpi">
        <div class="kpi-label">Kill Switch</div>
        <div class="kpi-value ${risk.killed ? 'text-red' : 'text-green'}">${risk.killed ? 'ACTIVE' : 'Off'}</div>
        <div class="kpi-sub">Max DD: ${pct(risk.max_drawdown_pct)}</div>
      </div>
      <div class="kpi">
        <div class="kpi-label">Peak Value</div>
        <div class="kpi-value">${$(risk.peak_value)}</div>
        <div class="kpi-sub">Max exposure: ${$(risk.max_total_exposure)}</div>
      </div>
      <div class="kpi">
        <div class="kpi-label">Mode</div>
        <div class="kpi-value ${statusData.dry_run ? 'text-yellow' : 'text-green'}">${statusData.dry_run ? 'DRY RUN' : 'LIVE'}</div>
        <div class="kpi-sub">${(statusData.strategies || []).length} strategies</div>
      </div>
    `;

    // Account cards
    const grid = document.getElementById('accounts-grid');
    grid.innerHTML = acctData.accounts.map(a => `
      <div class="account-card">
        <div class="account-header">
          <span class="account-number">${a.account_number}</span>
          <span class="account-type">${a.account_type || 'Unknown'}</span>
        </div>
        <div class="account-metrics">
          <div>
            <div class="account-metric-label">Total Value</div>
            <div class="account-metric-value">${$(a.total_value)}</div>
          </div>
          <div>
            <div class="account-metric-label">Cash</div>
            <div class="account-metric-value">${$(a.cash_available)}</div>
          </div>
          <div>
            <div class="account-metric-label">Invested</div>
            <div class="account-metric-value">${$(a.invested)}</div>
          </div>
          <div>
            <div class="account-metric-label">Positions</div>
            <div class="account-metric-value">${a.position_count}</div>
          </div>
        </div>
        ${renderFlags(a)}
      </div>
    `).join('');

    // Allocation bar
    renderAllocation(acctData.accounts);

  } catch (e) {
    toast('Dashboard load failed: ' + e.message);
  }
}

function renderFlags(account) {
  const flags = [];
  if (account.is_closing_only) flags.push('<span class="flag danger">CLOSING ONLY</span>');
  if (account.is_day_trader) flags.push('<span class="flag warn">PDT</span>');
  if (account.round_trips > 0) flags.push(`<span class="flag ${account.round_trips >= 3 ? 'warn' : 'ok'}">${account.round_trips}/3 day trades</span>`);
  if (account.unsettled_cash > 0) flags.push(`<span class="flag warn">Unsettled: ${$(account.unsettled_cash)}</span>`);
  if (!flags.length) return '';
  return `<div class="account-flags">${flags.join('')}</div>`;
}

// ── Allocation bar ───────────────────────────────────────────────────────────

const COLORS = ['#58a6ff', '#3fb950', '#d29922', '#bc8cff', '#f85149', '#79c0ff', '#56d364', '#e3b341'];

function renderAllocation(accounts) {
  const section = document.getElementById('allocation-section');
  // Gather all positions across all accounts
  const holdings = [];
  for (const a of accounts) {
    for (const p of a.positions) {
      if (p.market_value > 0) {
        holdings.push({ symbol: p.symbol, value: p.market_value, account: a.account_number });
      }
    }
  }

  if (!holdings.length) {
    section.style.display = 'none';
    return;
  }

  section.style.display = '';
  // Merge by symbol
  const merged = {};
  for (const h of holdings) {
    merged[h.symbol] = (merged[h.symbol] || 0) + h.value;
  }
  const sorted = Object.entries(merged).sort((a, b) => b[1] - a[1]);
  const total = sorted.reduce((s, [, v]) => s + v, 0);

  const bar = document.getElementById('alloc-bar');
  const legend = document.getElementById('alloc-legend');

  bar.innerHTML = sorted.map(([sym, val], i) => {
    const w = (val / total * 100).toFixed(1);
    const color = COLORS[i % COLORS.length];
    return `<div class="alloc-segment" style="width:${w}%;background:${color}" title="${sym}: ${$(val)} (${w}%)">${w > 5 ? sym : ''}</div>`;
  }).join('');

  legend.innerHTML = sorted.map(([sym, val], i) => {
    const color = COLORS[i % COLORS.length];
    return `<span><span class="alloc-legend-dot" style="background:${color}"></span>${sym} ${$(val)} (${(val/total*100).toFixed(1)}%)</span>`;
  }).join('');
}

// ── Positions ────────────────────────────────────────────────────────────────

async function loadPositions() {
  try {
    const data = accountsData || await api('/api/accounts');
    accountsData = data;

    const body = document.getElementById('positions-body');
    const rows = [];
    for (const a of data.accounts) {
      for (const p of a.positions) {
        rows.push({ ...p, account: a.account_number });
      }
    }

    if (!rows.length) {
      body.innerHTML = '<tr><td colspan="7" class="text-dim" style="text-align:center;padding:24px">No positions</td></tr>';
      return;
    }

    rows.sort((a, b) => Math.abs(b.market_value) - Math.abs(a.market_value));
    body.innerHTML = rows.map(p => `
      <tr>
        <td class="mono text-dim">${p.account}</td>
        <td class="mono" style="font-weight:600">${p.symbol}</td>
        <td class="text-right mono">${p.quantity}</td>
        <td class="text-right mono">${$(p.avg_price)}</td>
        <td class="text-right mono">${$(p.market_value)}</td>
        <td class="text-right mono ${pnlClass(p.unrealized_pnl)}">${pnlSign(p.unrealized_pnl)}</td>
        <td class="text-right mono text-dim">${pct(p.weight * 100)}</td>
      </tr>
    `).join('');
  } catch (e) {
    toast('Positions load failed: ' + e.message);
  }
}

// ── Trades ────────────────────────────────────────────────────────────────────

async function loadTrades() {
  try {
    const data = await api('/api/trades?limit=100');
    const body = document.getElementById('trades-body');

    if (!data.trades.length) {
      body.innerHTML = '<tr><td colspan="8" class="text-dim" style="text-align:center;padding:24px">No trades yet</td></tr>';
      return;
    }

    body.innerHTML = data.trades.reverse().map(t => `
      <tr>
        <td class="mono text-dim">${(t.timestamp || '').slice(0, 19).replace('T', ' ')}</td>
        <td class="text-blue">${t.strategy || ''}</td>
        <td class="mono" style="font-weight:600">${t.symbol || ''}</td>
        <td class="${t.side === 'BUY' ? 'text-green' : 'text-red'}" style="font-weight:600">${t.side || ''}</td>
        <td class="text-right mono">${t.quantity || ''}</td>
        <td class="text-right mono">${$(t.price)}</td>
        <td class="text-right mono">${$(t.value)}</td>
        <td class="${t.dry_run ? 'text-yellow' : 'text-green'}">${t.dry_run ? 'DRY' : 'LIVE'}</td>
      </tr>
    `).join('');
  } catch (e) {
    toast('Trades load failed: ' + e.message);
  }
}

// ── P&L ──────────────────────────────────────────────────────────────────────

async function loadPnl() {
  try {
    const data = await api('/api/pnl');
    const body = document.getElementById('pnl-body');

    // KPIs
    const kpi = document.getElementById('pnl-kpi-grid');
    kpi.innerHTML = `
      <div class="kpi">
        <div class="kpi-label">Total Realized P&L</div>
        <div class="kpi-value ${pnlClass(data.total_pnl)}">${pnlSign(data.total_pnl)}</div>
      </div>
      <div class="kpi">
        <div class="kpi-label">Total Trades</div>
        <div class="kpi-value">${data.total_trades}</div>
      </div>
    `;

    if (!data.strategies.length) {
      body.innerHTML = '<tr><td colspan="6" class="text-dim" style="text-align:center;padding:24px">No P&L data yet</td></tr>';
      return;
    }

    body.innerHTML = data.strategies.map(s => `
      <tr>
        <td class="text-blue" style="font-weight:600">${s.strategy}</td>
        <td class="text-right mono">${s.trades}</td>
        <td class="text-right mono text-green">${s.wins}</td>
        <td class="text-right mono text-red">${s.losses}</td>
        <td class="text-right mono">${pct(s.win_rate)}</td>
        <td class="text-right mono ${pnlClass(s.realized_pnl)}" style="font-weight:600">${pnlSign(s.realized_pnl)}</td>
      </tr>
    `).join('');
  } catch (e) {
    toast('P&L load failed: ' + e.message);
  }
}

// ── WebSocket ────────────────────────────────────────────────────────────────

function connectWs() {
  const dot = document.getElementById('ws-dot');
  const label = document.getElementById('ws-status');

  ws = new WebSocket(`ws://${location.host}/ws/live`);

  ws.onopen = () => {
    dot.className = 'status-dot connected';
    label.textContent = 'Connected';
  };

  ws.onmessage = (e) => {
    try {
      const msg = JSON.parse(e.data);
      if (msg.type === 'trade') {
        toast(`Trade: ${msg.data.side} ${msg.data.symbol}`);
        // Refresh if on relevant tab
        const active = document.querySelector('.tab.active');
        if (active) loadTab(active.dataset.tab);
      }
    } catch {}
  };

  ws.onclose = () => {
    dot.className = 'status-dot error';
    label.textContent = 'Disconnected';
    setTimeout(connectWs, 3000);
  };

  ws.onerror = () => {
    dot.className = 'status-dot error';
    label.textContent = 'Error';
  };
}

// ── Toast ────────────────────────────────────────────────────────────────────

function toast(msg) {
  const container = document.getElementById('toasts');
  const el = document.createElement('div');
  el.className = 'toast';
  el.textContent = msg;
  container.appendChild(el);
  setTimeout(() => el.remove(), 5000);
}

// ── Refresh ──────────────────────────────────────────────────────────────────

async function refreshAll() {
  const btn = document.getElementById('refresh-btn');
  btn.disabled = true;
  btn.textContent = 'Loading...';
  accountsData = null;
  const active = document.querySelector('.tab.active');
  if (active) await loadTab(active.dataset.tab);
  btn.disabled = false;
  btn.textContent = 'Refresh';
}

// ── Init ─────────────────────────────────────────────────────────────────────

loadDashboard();
connectWs();
