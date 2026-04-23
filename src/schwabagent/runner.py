"""AgentRunner — orchestrates strategies in a loop."""
from __future__ import annotations

import logging
import signal
import time
from datetime import datetime
from pathlib import Path

from rich.console import Console
from rich.table import Table

from schwabagent.config import Config
from schwabagent.persistence import StateStore
from schwabagent.risk import RiskManager
from schwabagent.schwab_client import AccountSummary, SchwabClient
from schwabagent.intermarket import (
    RegimeModel, regime_sizing_factor,
    get_yield_curve, TREASURY_SYMBOLS,
    get_commodities, COMMODITY_SYMBOLS,
)
from schwabagent.strategies.base import Strategy
from schwabagent.strategies.brown_momentum import BrownMomentumStrategy
from schwabagent.strategies.composite import CompositeStrategy
from schwabagent.strategies.etf_rotation import ETFRotationStrategy
from schwabagent.strategies.mean_reversion import MeanReversionStrategy
from schwabagent.strategies.conviction_hold import ConvictionHoldStrategy
from schwabagent.strategies.etf_scalp import ETFScalpStrategy
from schwabagent.strategies.momentum import MomentumStrategy
from schwabagent.strategies.tick_breadth import TickBreadthStrategy
from schwabagent.strategies.ah_sniper import AhSniperStrategy
from schwabagent.strategies.gamma_scanner import GammaScannerStrategy
from schwabagent.strategies.covered_call_screener import CoveredCallScreener
from schwabagent.strategies.unusual_activity import UnusualActivityStrategy
from schwabagent.strategies.theta import ThetaStrategy
from schwabagent.strategies.trend_following import TrendFollowingStrategy

logger = logging.getLogger(__name__)


class AgentRunner:
    """Main loop: fetches account, runs strategies, records trades."""

    def __init__(self, config: Config):
        self.config = config
        self.console = Console()
        self._running = False

        self.state = StateStore(config.STATE_DIR)
        self.risk = RiskManager(config, self.state)
        self.client = self._init_client()
        self.llm = self._init_llm()
        self.telegram = self._init_telegram()
        self.feedback = self._init_feedback()
        self.strategies: list[Strategy] = self._build_strategies()
        self.regime_model = RegimeModel(config)
        self.current_regime = None
        self.autotuner = self._init_autotuner()
        self.dreamcycle = self._init_dreamcycle()
        self.order_tracker = self._init_order_tracker()
        self.stream = self._init_streaming()
        self.scheduler = self._init_scheduler()

    # ── Initialization ────────────────────────────────────────────────────────

    def _init_llm(self):
        """Initialize LLM client (Ollama, Anthropic, or OpenAI)."""
        if not self.config.LLM_ENABLED:
            return None
        from schwabagent.llm import LLMClient

        provider = self.config.LLM_PROVIDER
        # Resolve API key: LLM_API_KEY → provider-specific → empty
        api_key = self.config.LLM_API_KEY
        if not api_key and provider == "anthropic":
            api_key = self.config.ANTHROPIC_API_KEY
        if not api_key and provider == "openai":
            api_key = self.config.OPENAI_API_KEY

        # Resolve base URL and model (legacy Ollama config as fallback)
        base_url = self.config.LLM_BASE_URL
        model = self.config.LLM_MODEL
        timeout = self.config.LLM_TIMEOUT

        if provider == "ollama" and not base_url:
            base_url = self.config.OLLAMA_HOST
        if provider == "ollama" and not model:
            model = self.config.OLLAMA_MODEL
        if provider == "ollama" and timeout == 60:
            timeout = self.config.OLLAMA_TIMEOUT

        llm = LLMClient(
            provider=provider,
            model=model,
            api_key=api_key,
            base_url=base_url,
            timeout=timeout,
            temperature=self.config.LLM_TEMPERATURE,
            max_tokens=self.config.LLM_MAX_TOKENS,
        )

        if llm.is_available():
            info = llm.info()
            logger.info("LLM enabled: provider=%s model=%s", info["provider"], info["model"])
        else:
            logger.warning(
                "LLM_ENABLED=true but %s not reachable — disabling LLM overlay",
                provider,
            )
            return None
        return llm

    def _init_feedback(self):
        """Initialize the ML feedback loop and auto-tuner."""
        from schwabagent.feedback import FeedbackLoop
        return FeedbackLoop(self.config)

    def _init_autotuner(self):
        """Initialize the auto-tuner (after feedback and telegram are ready)."""
        from schwabagent.feedback import AutoTuner
        return AutoTuner(self.config, self.feedback, self.telegram)

    def _init_dreamcycle(self):
        """Initialize the dreamcycle (after everything else is ready)."""
        from schwabagent.dreamcycle import DreamCycle
        return DreamCycle(self)

    def _init_order_tracker(self):
        """Initialize order fill tracking."""
        from schwabagent.order_tracker import OrderTracker
        tracker = OrderTracker(self.config, self.state)
        # Log fill events
        def _on_fill(order):
            logger.info("Fill confirmed: %s %d %s @ $%.2f (expected $%.2f)",
                        order.side, order.fill_quantity, order.symbol,
                        order.fill_price, order.expected_price)
            if self.telegram:
                from schwabagent.telegram import _escape_md
                slippage = order.fill_price - order.expected_price
                self.telegram.send_alert(
                    f"*Order Filled*\n"
                    f"`{_escape_md(order.symbol)}` {order.side} {order.fill_quantity} "
                    f"@ ${order.fill_price:,.2f}\n"
                    f"Slippage: ${slippage:+,.4f}"
                )
        tracker.on_fill(_on_fill)
        return tracker

    def _init_streaming(self):
        """Initialize WebSocket streaming (optional — starts on demand)."""
        from schwabagent.streaming import StreamManager
        stream = StreamManager(self.config, self.client)
        # Subscribe to all symbols across watchlist + all strategy universes
        all_symbols = self.config.all_symbols
        stream.subscribe_quotes(all_symbols)
        stream.subscribe_account_activity(on_fill=self.order_tracker.handle_stream_fill)
        return stream

    def _init_scheduler(self):
        """Initialize the cron scheduler."""
        from schwabagent.scheduler import Scheduler
        return Scheduler(self.config)

    def _init_telegram(self):
        """Initialize Telegram bot if enabled."""
        if not self.config.TELEGRAM_ENABLED:
            return None
        from schwabagent.telegram import TelegramBot
        bot = TelegramBot(self.config)
        self._register_telegram_commands(bot)
        bot.start()
        return bot

    def _register_telegram_commands(self, bot) -> None:
        """Register bot command handlers that return HTML text.

        Each handler accepts a list of string args from the Telegram command
        line (e.g. `/toggle momentum on` → args=["momentum", "on"]) and returns
        Telegram-HTML formatted output. Only &, <, > need escaping — numbers,
        percents, dots, dashes, and equals are all literal.
        """
        import html

        def _e(s) -> str:
            """HTML-escape any string for safe interpolation."""
            return html.escape(str(s))

        def _safe(fn):
            def _wrapped(args):
                try:
                    return fn(args)
                except Exception as e:
                    logger.exception("Telegram handler failed")
                    return f"<b>Error:</b> <code>{_e(str(e))[:400]}</code>"
            return _wrapped

        def _status(args):
            account = self._get_account()
            risk = self.risk.status(account=account)
            tr = risk.get("trading_rules", {})
            lines = [
                "<b>Schwab Agent Status</b>",
                "",
                f"Account: <code>{_e(account.account_number)}</code> ({_e(tr.get('account_type', '?'))})",
                f"Value: ${account.total_value:,.2f}",
                f"Cash: ${account.cash_available:,.2f}",
                f"Positions: {len(account.positions)}",
                f"Kill switch: {'YES' if risk['killed'] else 'no'}",
                f"DRY_RUN: {self.config.DRY_RUN}",
            ]
            return "\n".join(lines)

        def _pnl(args):
            summary = self.get_pnl_summary()
            if not summary:
                return "No P&amp;L data yet."
            lines = ["<b>P&amp;L Summary</b>", ""]
            total = 0.0
            for strat, data in sorted(summary.items()):
                pnl = data.get("realized_pnl", 0)
                trades = data.get("trades", 0)
                wr = data.get("win_rate", 0)
                total += pnl
                sign = "+" if pnl >= 0 else ""
                lines.append(
                    f"<code>{_e(strat)}</code> {sign}${pnl:,.2f} ({trades}t, {wr:.0f}%)"
                )
            sign = "+" if total >= 0 else ""
            lines.append(f"\n<b>Total: {sign}${total:,.2f}</b>")
            return "\n".join(lines)

        def _positions(args):
            account = self._get_account()
            if not account.positions:
                return "No open positions."
            lines = ["<b>Current Positions</b>", ""]
            for p in account.positions:
                pnl_str = f" {('+' if p.unrealized_pnl >= 0 else '')}${p.unrealized_pnl:,.2f}" if p.unrealized_pnl else ""
                lines.append(
                    f"<code>{_e(p.symbol)}</code> {p.quantity:.0f} shares "
                    f"@ ${p.avg_price:,.2f}{pnl_str}"
                )
            return "\n".join(lines)

        def _kill(args):
            self.risk.kill("Manual kill via Telegram")
            return "<b>Kill switch activated.</b> All trading halted."

        def _resume(args):
            self.risk.unkill()
            return "<b>Kill switch cleared.</b> Trading may resume."

        def _scan(args):
            limit = int(args[0]) if args and args[0].isdigit() else 10
            signals = self.scan_only()
            if not signals:
                return "No signals."
            ranked = sorted(signals, key=lambda s: abs(s.get("score", 0)), reverse=True)[:limit]
            lines = [f"<b>Top {len(ranked)} signals</b>", ""]
            for s in ranked:
                sym = s.get("symbol", "?")
                strat = s.get("strategy", "?")
                score = s.get("score", 0)
                sig = s.get("signal", s.get("side", "?"))
                lines.append(
                    f"<code>{_e(sym)}</code> {_e(sig)} "
                    f"score={score:+.2f} <i>{_e(strat)}</i>"
                )
            return "\n".join(lines)

        def _yields(args):
            quotes = self.client.get_quotes(TREASURY_SYMBOLS)
            curve = get_yield_curve(quotes)
            if not curve:
                return "Treasury yield data unavailable."
            lines = ["<b>Treasury Yield Curve</b>", ""]
            for sym, label in [("irx_13w", "13-Wk"), ("fvx_5y", "5-Yr"),
                               ("tnx_10y", "10-Yr"), ("tyx_30y", "30-Yr")]:
                val = curve.get(sym)
                lines.append(f"  <code>{label:6s}</code>  {val:.3f}%" if val is not None else f"  <code>{label:6s}</code>  --")
            lines.append("")
            shape = curve.get("shape", "?")
            s10y13w = curve.get("spread_10y_13w")
            s30y10y = curve.get("spread_30y_10y")
            if shape == "Inverted":
                shape_str = f"⚠️ <b>{shape}</b>"
            else:
                shape_str = f"<b>{shape}</b>"
            lines.append(f"Shape: {shape_str}")
            if s10y13w is not None:
                lines.append(f"10Y-13W spread: <b>{s10y13w:+.3f}%</b>")
            if s30y10y is not None:
                lines.append(f"30Y-10Y spread: <b>{s30y10y:+.3f}%</b>")
            return "\n".join(lines)

        def _commodities(args):
            quotes = self.client.get_quotes(COMMODITY_SYMBOLS)
            rows = get_commodities(quotes)
            if not rows:
                return "Commodity data unavailable."
            lines = ["<b>Commodities</b> (sorted by |move|)", ""]
            for r in rows:
                chg = r["change_pct"]
                arrow = "▲" if chg > 0 else "▼" if chg < 0 else "–"
                sign = "+" if chg > 0 else ""
                lines.append(
                    f"  <code>{r['label']:14s}</code> "
                    f"${r['last']:<9.2f} {arrow} {sign}{chg:.2f}%"
                )
            return "\n".join(lines)

        def _regime(args):
            if not self.config.REGIME_ENABLED:
                return "Regime detection is disabled in config."
            info = self.get_regime()
            if not info:
                return "Regime detection unavailable."
            name = info.get("regime", "?")
            conf = info.get("confidence", 0)
            lines = ["<b>Intermarket Regime</b>", "", f"Current: <b>{_e(name)}</b>"]
            if isinstance(conf, (int, float)):
                lines.append(f"Confidence: {conf*100:.0f}%")
            if self.current_regime is not None:
                lines.append("\n<b>Sizing factors:</b>")
                for sname in sorted(self.config._STRATEGY_LIVE_FLAGS.keys()):
                    try:
                        f = regime_sizing_factor(self.current_regime, sname)
                        lines.append(f"  <code>{_e(sname)}</code>: {f:.2f}x")
                    except Exception:
                        pass
            sigs = info.get("signals") or {}
            if isinstance(sigs, dict) and sigs:
                lines.append("\n<b>Signals:</b>")
                for k, v in list(sigs.items())[:10]:
                    lines.append(f"  <code>{_e(k)}</code>: {_e(str(v))[:60]}")
            return "\n".join(lines)

        def _feedback(args):
            days = int(args[0]) if args and args[0].isdigit() else 30
            summary = self.feedback.get_strategy_summary(days=days)
            if not summary:
                return f"No feedback data in last {days}d."
            lines = [f"<b>Feedback — last {days}d</b>", ""]
            for strat, d in sorted(summary.items()):
                total = d.get("total_signals", 0)
                resolved = d.get("resolved") or 0
                wins = d.get("wins") or 0
                wr = (wins / resolved * 100) if resolved else 0
                pnl = d.get("total_pnl") or 0
                sign = "+" if pnl >= 0 else ""
                lines.append(
                    f"<code>{_e(strat)}</code> n={total} wr={wr:.0f}% pnl={sign}${pnl:,.2f}"
                )
            return "\n".join(lines)

        def _drift(args):
            alerts = self.feedback.get_drift_alerts(days=7)
            if not alerts:
                return "No drift alerts in last 7d."
            lines = ["<b>Drift Alerts (7d)</b>", ""]
            for a in alerts[:15]:
                level = str(a.get("alert_level", "?")).upper()
                strat = a.get("strategy", "?")
                metric = a.get("metric", "?")
                baseline = a.get("baseline_value", 0)
                current = a.get("current_value", 0)
                dev = a.get("deviation_pct", 0)
                lines.append(
                    f"<b>{_e(level)}</b> <code>{_e(strat)}</code> "
                    f"{_e(metric)}: {baseline:.1f}→{current:.1f} "
                    f"({dev:+.0f}%)"
                )
            return "\n".join(lines)

        def _accounts(args):
            accounts = self.client.get_all_accounts()
            if not accounts:
                return "No accounts."
            lines = ["<b>Accounts</b>", ""]
            total = 0.0
            for a in accounts:
                total += a.total_value
                lines.append(
                    f"<code>{_e(a.account_number)}</code> "
                    f"${a.total_value:,.0f} cash ${a.cash_available:,.0f} "
                    f"pos={len(a.positions)}"
                )
            lines.append(f"\n<b>Total: ${total:,.2f}</b>")
            return "\n".join(lines)

        def _risk(args):
            account = self._get_account()
            st = self.risk.status(account=account)
            lines = ["<b>Risk Status</b>", ""]
            lines.append(f"Killed: {'YES' if st.get('killed') else 'no'}")
            if "peak_value" in st:
                lines.append(f"Peak: ${st['peak_value']:,.2f}")
            if "current_drawdown_pct" in st:
                lines.append(f"Drawdown: {st['current_drawdown_pct']:.2f}%")
            if "max_drawdown_pct" in st:
                lines.append(f"Max DD limit: {st['max_drawdown_pct']:.1f}%")
            tr = st.get("trading_rules", {})
            if tr:
                lines.append(f"Account type: <code>{_e(tr.get('account_type', '?'))}</code>")
                if "day_trades_remaining" in tr:
                    lines.append(f"Day trades left: {tr['day_trades_remaining']}")
            return "\n".join(lines)

        def _recent(args):
            limit = int(args[0]) if args and args[0].isdigit() else 10
            trades = self.state.get_trade_history(limit=limit)
            if not trades:
                return "No trade history."
            lines = [f"<b>Recent {len(trades)} trades</b>", ""]
            for t in reversed(trades):
                sym = t.get("symbol", "?")
                side = t.get("side", "?")
                qty = t.get("quantity", 0)
                price = t.get("price", 0)
                strat = t.get("strategy", "?")
                pnl = t.get("realized_pnl")
                dry = "DRY" if t.get("dry_run", True) else "LIVE"
                pnl_str = f" pnl={('+' if pnl >= 0 else '')}${pnl:,.2f}" if pnl is not None else ""
                lines.append(
                    f"<code>{_e(sym)}</code> {_e(side)} {qty}@${price:,.2f} "
                    f"<i>{_e(strat)}</i> [{dry}]{pnl_str}"
                )
            return "\n".join(lines)

        def _strategies(args):
            lines = ["<b>Strategies</b>", "", f"DRY_RUN: {self.config.DRY_RUN}", ""]
            flag_map = self.config._STRATEGY_LIVE_FLAGS
            for name in sorted(flag_map.keys()):
                enabled = name in self.config.strategies
                live = bool(getattr(self.config, flag_map[name], False))
                if enabled and live and not self.config.DRY_RUN:
                    state = "LIVE"
                elif enabled:
                    state = "DRY" if self.config.DRY_RUN else "GATED"
                else:
                    state = "OFF"
                lines.append(
                    f"<code>{_e(name)}</code> "
                    f"enabled={'Y' if enabled else 'n'} "
                    f"live_flag={'Y' if live else 'n'} → <b>{state}</b>"
                )
            return "\n".join(lines)

        def _quote(args):
            if not args:
                return "Usage: <code>/quote SYMBOL</code>"
            sym = args[0].upper()
            quotes = self.client.get_quotes([sym])
            q = quotes.get(sym) if quotes else None
            if not q:
                return f"No quote for <code>{_e(sym)}</code>."
            last = q.last or 0
            bid = q.bid or 0
            ask = q.ask or 0
            chg_pct = q.change_pct or 0
            vol = q.volume or 0
            sign = "+" if chg_pct >= 0 else ""
            return (
                f"<b>{_e(sym)}</b>\n"
                f"Last: ${last:,.2f}\n"
                f"Bid/Ask: ${bid:,.2f} / ${ask:,.2f}\n"
                f"Change: {sign}{chg_pct:.2f}%\n"
                f"Volume: {vol:,.0f}\n"
                f"Spread: ${q.spread or 0:.4f} ({q.spread_pct or 0:.3f}%)"
            )

        def _toggle(args):
            if len(args) < 2 or args[1].lower() not in ("on", "off"):
                names = ", ".join(f"<code>{_e(n)}</code>" for n in self.config._STRATEGY_LIVE_FLAGS)
                return (
                    "Usage: <code>/toggle &lt;strategy&gt; on|off</code>\n"
                    f"Strategies: {names}"
                )
            name = args[0].lower()
            state = args[1].lower() == "on"
            attr = self.config._STRATEGY_LIVE_FLAGS.get(name)
            if attr is None:
                return f"Unknown strategy <code>{_e(name)}</code>."
            old = bool(getattr(self.config, attr, False))
            setattr(self.config, attr, state)
            self._persist_live_flag(attr, state)
            return (
                f"<b>{_e(name)}</b> live flag: "
                f"{'ON' if old else 'OFF'} → <b>{'ON' if state else 'OFF'}</b>\n"
                f"Global DRY_RUN={self.config.DRY_RUN} "
                f"(flag only takes effect in <code>./run.sh live</code>)"
            )

        def _sweep(args):
            """Sweep idle cash into SWVXX (or configured money market fund).

            Usage:
              /sweep                   preview — show cash, buffer, proposed amount
              /sweep confirm           place the BUY order
              /sweep 5000              override amount (preview)
              /sweep 5000 confirm      override amount, place order
              /sweep sell 5000 confirm sell SWVXX to raise cash
            """
            cfg = self.config
            symbol = cfg.SWEEP_SYMBOL
            side = "BUY"
            amount_override: float | None = None
            confirm = False

            for tok in args:
                t = tok.lower()
                if t == "confirm":
                    confirm = True
                elif t in ("buy", "sell"):
                    side = t.upper()
                else:
                    try:
                        amount_override = float(tok.replace(",", "").replace("$", ""))
                    except ValueError:
                        return f"Bad token: <code>{_e(tok)}</code>"

            account = self._get_account()
            cash = account.cash_available
            buffer_ = cfg.SWEEP_CASH_BUFFER

            # Compute amount to move
            if side == "BUY":
                if amount_override is not None:
                    amount = amount_override
                else:
                    amount = max(0.0, cash - buffer_)
            else:
                if amount_override is None:
                    return "Usage: <code>/sweep sell AMOUNT confirm</code>"
                amount = amount_override

            # Preview header
            held_swvxx = next(
                (p for p in account.positions if p.symbol == symbol and p.quantity > 0),
                None,
            )
            held_str = f"${held_swvxx.market_value:,.2f}" if held_swvxx else "$0"

            mode = "LIVE" if (confirm and not cfg.DRY_RUN) else (
                "DRY" if confirm else "PREVIEW"
            )
            lines = [
                f"<b>Sweep {side} {_e(symbol)}</b>  <i>{mode}</i>",
                "",
                f"Cash available: ${cash:,.2f}",
                f"Buffer: ${buffer_:,.2f}",
                f"Currently held: {held_str}",
                f"Amount: ${amount:,.2f}",
            ]

            # Validation
            if amount < cfg.SWEEP_MIN_AMOUNT:
                lines.append(
                    f"\nBelow minimum (${cfg.SWEEP_MIN_AMOUNT:,.2f}) — nothing to do."
                )
                return "\n".join(lines)
            if side == "BUY" and amount > cash:
                lines.append(f"\n<b>BLOCKED:</b> amount exceeds cash available.")
                return "\n".join(lines)
            if side == "SELL":
                if not held_swvxx:
                    lines.append(f"\n<b>BLOCKED:</b> no {_e(symbol)} position.")
                    return "\n".join(lines)
                if amount > held_swvxx.market_value:
                    lines.append(
                        f"\n<b>BLOCKED:</b> hold ${held_swvxx.market_value:,.2f}, "
                        f"cannot sell ${amount:,.2f}."
                    )
                    return "\n".join(lines)

            if not confirm:
                lines.append(
                    "\nAdd <code>confirm</code> to place.  "
                    f"<i>NAV fills at 4pm ET.</i>"
                )
                return "\n".join(lines)

            if cfg.DRY_RUN:
                lines.append("\nDRY_RUN=true — not placed. Flip DRY_RUN=false for live.")
                return "\n".join(lines)

            result = self.client.place_mutual_fund_order(
                account.account_hash, symbol, side, amount,
            )
            if result.get("status") != "ok":
                lines.append(
                    f"\n<b>ORDER FAILED:</b> <code>{_e(result.get('error', '?'))}</code>"
                )
                return "\n".join(lines)

            # Record trade
            self.risk.record_trade(symbol, side, int(amount), 1.00, strategy="sweep")
            trade = {
                "strategy": "sweep",
                "symbol": symbol,
                "side": side,
                "quantity": amount,
                "price": 1.00,
                "value": amount,
                "dry_run": False,
                "asset_type": "MUTUAL_FUND",
                **result,
            }
            self.state.append_trade(trade)
            if self.telegram:
                self.telegram.send_trade_alert(trade)
            lines.append(
                f"\n<b>PLACED</b> id=<code>{_e(result.get('order_id', '?'))}</code>\n"
                f"<i>Fills at next 4pm ET NAV.</i>"
            )
            return "\n".join(lines)

        def _sweepetf(args):
            """Sweep idle cash into SGOV (short-Treasury ETF).

            Usage:
              /sweepetf                    preview at current ask
              /sweepetf confirm            BUY (cash - buffer) / ask
              /sweepetf 5000               override dollar amount (preview)
              /sweepetf 5000 confirm       override + place
              /sweepetf sell 50 confirm    sell 50 shares
              /sweepetf sell all confirm   liquidate entire position
            """
            cfg = self.config
            symbol = cfg.SWEEP_ETF_SYMBOL
            side = "BUY"
            amount_override: float | None = None
            sell_qty_override: int | str | None = None
            confirm = False

            for tok in args:
                t = tok.lower()
                if t == "confirm":
                    confirm = True
                elif t == "buy":
                    side = "BUY"
                elif t == "sell":
                    side = "SELL"
                elif t == "all" and side == "SELL":
                    sell_qty_override = "all"
                elif side == "SELL" and t.isdigit():
                    sell_qty_override = int(t)
                else:
                    try:
                        amount_override = float(tok.replace(",", "").replace("$", ""))
                    except ValueError:
                        return f"Bad token: <code>{_e(tok)}</code>"

            # Get a fresh ask to size shares
            q = (self.client.get_quotes([symbol]) or {}).get(symbol)
            if q is None or not (q.ask or q.last):
                return f"No quote for <code>{_e(symbol)}</code>."
            ask = float(q.ask or q.last)
            bid = float(q.bid or q.last)

            account = self._get_account()
            cash = account.cash_available
            buffer_ = cfg.SWEEP_CASH_BUFFER
            held = next(
                (p for p in account.positions if p.symbol == symbol and p.quantity > 0),
                None,
            )

            # Resolve quantity
            if side == "BUY":
                if amount_override is not None:
                    amount = amount_override
                else:
                    amount = max(0.0, cash - buffer_)
                qty = int(amount // ask)
                est_cost = qty * ask
            else:  # SELL
                if sell_qty_override == "all":
                    if held is None:
                        return f"No position in <code>{_e(symbol)}</code>."
                    qty = int(held.quantity)
                elif isinstance(sell_qty_override, int):
                    qty = sell_qty_override
                else:
                    return "Usage: <code>/sweepetf sell QTY|all confirm</code>"
                amount = qty * bid
                est_cost = qty * bid

            held_str = (
                f"{held.quantity:.0f}sh @ ${held.avg_price:,.2f} "
                f"(${held.market_value:,.2f})" if held else "none"
            )
            mode = "LIVE" if (confirm and not cfg.DRY_RUN) else (
                "DRY" if confirm else "PREVIEW"
            )
            lines = [
                f"<b>SweepETF {side} {_e(symbol)}</b>  <i>{mode}</i>",
                "",
                f"Cash: ${cash:,.2f}  buffer: ${buffer_:,.2f}",
                f"Quote: bid ${bid:.4f} / ask ${ask:.4f}",
                f"Held: {held_str}",
                f"Qty: {qty}  est ${est_cost:,.2f}",
            ]

            if qty <= 0:
                lines.append("\nNothing to do.")
                return "\n".join(lines)
            if side == "BUY" and est_cost < cfg.SWEEP_MIN_AMOUNT:
                lines.append(
                    f"\nBelow minimum (${cfg.SWEEP_MIN_AMOUNT:,.2f})."
                )
                return "\n".join(lines)
            if side == "BUY" and est_cost > cash:
                lines.append(f"\n<b>BLOCKED:</b> est cost exceeds cash.")
                return "\n".join(lines)
            if side == "SELL":
                if held is None:
                    lines.append(f"\n<b>BLOCKED:</b> no {_e(symbol)} position.")
                    return "\n".join(lines)
                if qty > held.quantity:
                    lines.append(
                        f"\n<b>BLOCKED:</b> hold {held.quantity:.0f}, "
                        f"cannot sell {qty}."
                    )
                    return "\n".join(lines)

            # Risk gate — only on BUY side
            if side == "BUY":
                allowed, reason = self.risk.can_buy(symbol, qty, ask, account)
                if not allowed:
                    lines.append(f"\n<b>BLOCKED:</b> <code>{_e(reason)}</code>")
                    return "\n".join(lines)

            if not confirm:
                lines.append("\nAdd <code>confirm</code> to place.")
                return "\n".join(lines)

            if cfg.DRY_RUN:
                lines.append("\nDRY_RUN=true — not placed.")
                return "\n".join(lines)

            price = ask if side == "BUY" else bid
            result = self.client.place_order(
                account.account_hash, symbol, side, qty,
            )
            if result.get("status") != "ok":
                lines.append(
                    f"\n<b>ORDER FAILED:</b> <code>{_e(result.get('error', '?'))}</code>"
                )
                return "\n".join(lines)

            self.risk.record_trade(symbol, side, qty, price, strategy="sweepetf")
            trade = {
                "strategy": "sweepetf",
                "symbol": symbol,
                "side": side,
                "quantity": qty,
                "price": price,
                "value": qty * price,
                "dry_run": False,
                **result,
            }
            self.state.append_trade(trade)
            if self.telegram:
                self.telegram.send_trade_alert(trade)
            if result.get("order_id") and result["order_id"] != "dry":
                try:
                    self.order_tracker.track(
                        order_id=result["order_id"],
                        symbol=symbol, side=side, quantity=qty,
                        expected_price=price,
                        account_hash=account.account_hash,
                        strategy="sweepetf",
                    )
                except Exception:
                    logger.exception("order_tracker.track failed for sweepetf")
            lines.append(
                f"\n<b>PLACED</b> id=<code>{_e(result.get('order_id', '?'))}</code>"
            )
            return "\n".join(lines)

        def _gamma(args):
            gs = next((s for s in self.strategies if getattr(s, "name", "") == "gamma_scanner"), None)
            if gs is None:
                return "Gamma scanner not loaded. Add <code>gamma_scanner</code> to STRATEGIES."
            try:
                n = int(args[0]) if args and args[0].isdigit() else 5
            except ValueError:
                n = 5
            opps = gs.scan()
            if not opps:
                return "<b>Cheap gamma</b>\n\nNo candidates below threshold."
            lines = [f"<b>Cheap gamma — top {min(n, len(opps))}</b>", ""]
            for o in opps[:n]:
                lines.append(
                    f"<code>{_e(o['symbol'])}</code> {_e(o['expiration'])} "
                    f"K={o['strike']:g} IV={o['iv_pct']:.1f}% "
                    f"RV={o['rv_pct']:.1f}% ratio={o['iv_rv_ratio']:.2f}\n"
                    f"  cost=${o['straddle_cost_per_share']:.2f} "
                    f"γ/$={o['gamma_per_dollar']:.5f} DTE={o['dte']}"
                )
            return "\n".join(lines)

        def _unusual(args):
            ua = next((s for s in self.strategies if getattr(s, "name", "") == "unusual_activity"), None)
            if ua is None:
                return "Unusual activity scanner not loaded. Add <code>unusual_activity</code> to STRATEGIES."
            try:
                n = int(args[0]) if args and args[0].isdigit() else 10
            except ValueError:
                n = 10
            opps = ua.scan()
            if not opps:
                return "<b>Unusual options activity</b>\n\nNo hits above threshold."
            lines = [f"<b>Unusual options activity — top {min(n, len(opps))}</b>", ""]
            for o in opps[:n]:
                lines.append(
                    f"<code>{_e(o['symbol'])}</code> {_e(o['expiration'])} "
                    f"${o['strike']:g} {_e(o['side'])}  "
                    f"vol={o['volume']:,} OI={o['open_interest']:,} "
                    f"<b>{o['vol_oi_ratio']:.1f}x</b>\n"
                    f"  mid=${o['mid']:.2f} ~${o['notional']:,.0f} "
                    f"IV={o['iv_pct']:.1f}% DTE={o['dte']}"
                )
            return "\n".join(lines)

        def _covered(args):
            """Covered call screener — dividend stocks + calls >30 DTE.

            Usage:
              /covered                  top N (config: COVERED_CALL_TOP_N)
              /covered 10               top 10
              /covered KO               single symbol (ignores universe filter)
              /covered refresh          force scan (no persistent cache yet)
            """
            ccs = next(
                (s for s in self.strategies
                 if getattr(s, "name", "") == "covered_call_screener"),
                None,
            )
            if ccs is None:
                return (
                    "Covered call screener not loaded. "
                    "Add <code>covered_call_screener</code> to STRATEGIES."
                )
            n = self.config.COVERED_CALL_TOP_N
            symbol: str | None = None
            for tok in args:
                t = tok.lower()
                if t in ("refresh", "-r", "--refresh"):
                    pass  # no persistent cache yet — scan() always fresh
                elif t.isdigit():
                    n = int(t)
                elif re.fullmatch(r"[A-Za-z]{1,6}", tok):
                    symbol = tok.upper()

            opps = ccs.scan()
            if symbol:
                opps = [o for o in opps if o["symbol"] == symbol]
            if not opps:
                return "<b>Covered calls</b>\n\nNo candidates met the thresholds."

            shown = opps[:n] if n > 0 else opps
            lines = [
                f"<b>Covered calls — top {len(shown)}/{len(opps)}</b>",
                "",
            ]
            for o in shown:
                cap = " +cap" if o.get("dividend_in_hold") else ""
                lines.append(
                    f"<code>{_e(o['symbol'])}</code> ${o['price']:.2f} → "
                    f"${o['strike']:g}C {_e(o['expiration'])} "
                    f"<b>{o['total_annual_yield_pct']:.1f}%/yr</b>{cap}\n"
                    f"  prem ${o['call_premium']:.2f} · "
                    f"if-called {o['if_called_yield_pct']:.1f}%/yr · "
                    f"div {o['dividend_yield_pct']:.1f}% · "
                    f"DTE {o['dte']} · OTM {o['otm_pct']:.1f}% · "
                    f"Δ {o['call_delta']:.2f}"
                )

            out = "\n".join(lines)
            if len(out) > 3900:
                out = out[:3900] + "\n<i>… truncated</i>"
            return out

        def _ivrank(args):
            if not args:
                return "Usage: <code>/ivrank SYMBOL [dte=30]</code>"
            symbol = args[0].upper()
            dte_target = 30
            for tok in args[1:]:
                if tok.lower().startswith("dte="):
                    try:
                        dte_target = int(tok.split("=", 1)[1])
                    except ValueError:
                        return f"Bad dte: <code>{_e(tok)}</code>"
            dte_min = max(1, dte_target - 7)
            dte_max = dte_target + 14

            # Realized vol inline — 20d close-to-close annualized.
            import math
            df = self.client.get_ohlcv(symbol, days=60)
            if df is None or df.empty or len(df) < 21:
                return f"No OHLCV for <code>{_e(symbol)}</code>."
            closes = df["close"].astype(float).values[-21:]
            log_r = [math.log(closes[i] / closes[i-1]) for i in range(1, len(closes))]
            mean = sum(log_r) / len(log_r)
            var = sum((r - mean) ** 2 for r in log_r) / (len(log_r) - 1)
            rv = (var ** 0.5) * math.sqrt(252) * 100

            straddles = self.client.get_atm_straddles(symbol, dte_min, dte_max)
            if not straddles:
                return f"No straddles for <code>{_e(symbol)}</code> in DTE {dte_min}-{dte_max}."

            lines = [
                f"<b>{_e(symbol)} IV vs RV</b>",
                f"RV(20d): {rv:.1f}%  spot: ${straddles[0].underlying_price:.2f}",
                "",
                "<code>expiry     K       IV     ratio  cost    γ/$</code>",
            ]
            for s in straddles[:6]:
                iv = s.iv
                ratio = iv / rv if rv > 0 else 0
                tag = " *" if ratio < 1.0 else ""
                lines.append(
                    f"<code>{_e(s.expiration)} {s.strike:<7g} "
                    f"{iv:>5.1f}% {ratio:>5.2f}  "
                    f"${s.cost:>6.2f} {s.gamma_per_dollar:.5f}</code>{tag}"
                )
            lines.append("\n<i>* = cheap (IV &lt; RV)</i>")
            return "\n".join(lines)

        def _tracker(args):
            tr = getattr(self, "order_tracker", None)
            if tr is None:
                return "Order tracker not running."
            st = tr.status()
            pending = st.get("pending", 0)
            orders = st.get("orders") or []
            if not orders:
                return f"<b>Order tracker</b>\n\nPending: {pending}"
            lines = [f"<b>Order tracker — {pending} pending</b>", ""]
            for o in orders[:20]:
                lines.append(
                    f"<code>{_e(o.get('id', '?'))}</code> "
                    f"{_e(o.get('side', '?'))} {_e(o.get('symbol', '?'))} "
                    f"exp=${o.get('expected', 0):.2f} "
                    f"[{_e(o.get('status', '?'))}] attempts={o.get('attempts', 0)}"
                )
            return "\n".join(lines)

        def _sec(args):
            if not args:
                return (
                    "Usage: <code>/sec SYMBOL [form=10-K] [n=5]</code>\n"
                    "Forms: 10-K (annual), 10-Q (quarterly), 8-K (events)"
                )
            symbol = args[0].upper()
            form = "10-K"
            limit = 5
            for tok in args[1:]:
                t = tok.lower()
                if t.startswith("form="):
                    form = tok.split("=", 1)[1].upper()
                elif t.startswith("n="):
                    try:
                        limit = max(1, min(20, int(tok.split("=", 1)[1])))
                    except ValueError:
                        pass
            try:
                from schwabagent.sec import SECAnalyzer
                analyzer = SECAnalyzer(self.config)
                filings = analyzer.get_filings(symbol, form=form, limit=limit)
            except ImportError:
                return "SEC module unavailable — install <code>edgar</code> package."
            if not filings:
                return f"No <code>{_e(form)}</code> filings for <code>{_e(symbol)}</code>."
            lines = [f"<b>{_e(symbol)} {_e(form)} filings</b>", ""]
            for f in filings:
                desc = (f.description or "")[:60]
                lines.append(
                    f"<code>{_e(f.filing_date)}</code> {_e(f.form)}  "
                    f"{_e(f.accession_number)}" + (f"\n  {_e(desc)}" if desc else "")
                )
            return "\n".join(lines)

        def _universe(args):
            all_syms = self.config.all_symbols
            per_strategy = [
                ("watchlist", self.config.watchlist),
                ("etf_rotation", self.config.etf_universe),
                ("momentum", self.config.momentum_symbols),
                ("mean_reversion", self.config.mean_reversion_symbols),
                ("trend_following", self.config.trend_following_symbols),
                ("conviction_hold", self.config.conviction_symbols),
                ("theta", self.config.theta_symbols),
                ("gamma_scanner", self.config.gamma_scanner_symbols),
                ("scalp", self.config.scalp_universe),
            ]
            lines = [f"<b>Universe — {len(all_syms)} unique symbols</b>", ""]
            for name, syms in per_strategy:
                if syms:
                    lines.append(f"<code>{_e(name)}</code> ({len(syms)}): {_e(', '.join(syms[:12]))}{' …' if len(syms) > 12 else ''}")
            lines.append(f"\n<i>Total: {len(all_syms)}</i>")
            return "\n".join(lines)

        def _size(args):
            if not args:
                return (
                    "Usage: <code>/size SYMBOL [price=X]</code>\n"
                    "Shows max quantity allowed under current risk rules."
                )
            symbol = args[0].upper()
            price_override = None
            for tok in args[1:]:
                if tok.lower().startswith("price="):
                    try:
                        price_override = float(tok.split("=", 1)[1])
                    except ValueError:
                        return f"Bad price: <code>{_e(tok)}</code>"
            if price_override is not None:
                price = price_override
            else:
                q = (self.client.get_quotes([symbol]) or {}).get(symbol)
                if q is None or not (q.ask or q.last):
                    return f"No quote for <code>{_e(symbol)}</code>."
                price = float(q.ask or q.last)

            account = self._get_account()
            cfg = self.config
            # Hard caps from config
            by_max_order = cfg.MAX_ORDER_VALUE
            by_max_position = cfg.MAX_POSITION_VALUE
            by_pct_port = account.total_value * cfg.MAX_POSITION_PCT
            by_cash = account.cash_available
            exposure = sum(p.market_value for p in account.positions if p.quantity > 0)
            by_exposure = max(0.0, cfg.MAX_TOTAL_EXPOSURE - exposure)

            ceiling_dollars = min(by_max_order, by_max_position, by_pct_port, by_cash, by_exposure)
            max_qty = int(ceiling_dollars // price) if price > 0 else 0
            # Run the real risk check on that size to catch PDT / closing-only / etc.
            ok, reason = self.risk.can_buy(symbol, max(1, max_qty), price, account)

            lines = [
                f"<b>{_e(symbol)} sizing</b>  price=${price:,.2f}",
                "",
                f"max_order cap: ${by_max_order:,.0f}",
                f"max_position cap: ${by_max_position:,.0f}",
                f"pct_portfolio cap: ${by_pct_port:,.0f} ({cfg.MAX_POSITION_PCT:.0%})",
                f"cash available: ${by_cash:,.0f}",
                f"exposure headroom: ${by_exposure:,.0f} (used ${exposure:,.0f} / ${cfg.MAX_TOTAL_EXPOSURE:,.0f})",
                "",
                f"<b>Max qty: {max_qty}</b> (${max_qty * price:,.0f})",
            ]
            if not ok and max_qty > 0:
                lines.append(f"\n<b>BLOCKED:</b> <code>{_e(reason)}</code>")
            return "\n".join(lines)

        def _orders(args):
            account = self._get_account()
            orders = self.client.get_open_orders(account.account_hash)
            if not orders:
                return "<b>Open orders</b>\n\nNone."
            lines = [f"<b>Open orders ({len(orders)})</b>", ""]
            for o in orders[:25]:
                oid = o.get("orderId", "?")
                status = o.get("status", "?")
                otype = o.get("orderType", "?")
                duration = o.get("duration", "")
                price = o.get("price")
                filled = o.get("filledQuantity", 0)
                qty = o.get("quantity", 0)
                legs = o.get("orderLegCollection") or []
                if legs:
                    leg = legs[0]
                    sym = (leg.get("instrument") or {}).get("symbol", "?")
                    instr = leg.get("instruction", "?")
                else:
                    sym, instr = "?", "?"
                price_str = f"${price}" if price else "mkt"
                lines.append(
                    f"<code>{_e(oid)}</code> {_e(instr)} {qty} {_e(sym)} "
                    f"{_e(otype)} @ {_e(price_str)} [{_e(status)}] "
                    f"filled={filled}/{qty} {_e(duration)}"
                )
            return "\n".join(lines)

        def _cancel(args):
            if not args:
                return "Usage: <code>/cancel ORDER_ID [ORDER_ID …]</code>"
            account = self._get_account()
            lines = ["<b>Cancel</b>", ""]
            for oid in args:
                ok = self.client.cancel_order(account.account_hash, oid)
                lines.append(f"<code>{_e(oid)}</code> — {'OK' if ok else 'FAILED'}")
            return "\n".join(lines)

        def _jobs(args):
            if not self.scheduler:
                return "Scheduler not running."
            jobs = self.scheduler.list_jobs()
            if not jobs:
                return "No scheduled jobs."
            lines = [f"<b>Scheduled jobs ({len(jobs)})</b>", ""]
            for j in jobs:
                tag = "on" if j.get("enabled") else "OFF"
                next_run = (j.get("next_run") or "?")[:19]
                last_run = (j.get("last_run") or "—")[:19]
                runs = j.get("run_count", 0)
                err = j.get("last_error") or ""
                line = (
                    f"<code>{_e(j.get('name', '?'))}</code> [{tag}] "
                    f"<i>{_e(j.get('schedule', '?'))}</i>\n"
                    f"  next={_e(next_run)} last={_e(last_run)} runs={runs}"
                )
                if err:
                    line += f"\n  err=<code>{_e(err[:100])}</code>"
                lines.append(line)
            return "\n".join(lines)

        def _dream(args):
            if not self.dreamcycle:
                return "Dreamcycle not running."
            st = self.dreamcycle.status()
            running = "YES" if st.get("running") else "no"
            lines = [
                "<b>Dreamcycle</b>",
                "",
                f"Running: {running}",
                f"Cycles: {st.get('cycle_count', 0)}",
                f"Last run: <code>{_e((st.get('last_run') or '—')[:19])}</code>",
            ]
            last = st.get("last_result")
            if last:
                lines.append(f"Duration: {last.get('duration', 0):.1f}s")
                lines.append(f"Signals recorded: {last.get('signals', 0)}")
                lines.append(f"Drift alerts: {last.get('drift_alerts', 0)}")
                ok = last.get("phases_ok") or []
                failed = last.get("phases_failed") or []
                if ok:
                    lines.append(f"OK: <code>{_e(', '.join(ok))}</code>")
                if failed:
                    lines.append(f"<b>Failed:</b> <code>{_e(', '.join(failed))}</code>")
                errors = last.get("errors") or []
                if errors:
                    lines.append(f"Errors: <code>{_e(str(errors[:3])[:200])}</code>")
            return "\n".join(lines)

        def _ratelimit(args):
            st = self.client.rate_limit_stats()
            lines = ["<b>API rate limits</b>", ""]
            for label, s in st.items():
                util = s.get("utilization_pct", 0)
                cur = s.get("current", 0)
                mx = s.get("max", 0)
                total = s.get("total_calls", 0)
                thr = s.get("total_throttled", 0)
                lines.append(
                    f"<code>{_e(label)}</code> {cur}/{mx} ({util:.0f}%) "
                    f"total={total} throttled={thr}"
                )
            return "\n".join(lines)

        def _chain(args):
            if not args:
                return (
                    "Usage: <code>/chain SYMBOL [put|call] [dte=30] [strikes=5]</code>\n"
                    "Defaults: both sides, dte=30, 5 strikes each way"
                )
            symbol = args[0].upper()
            side = "BOTH"
            dte_target = 30
            strike_count = 10
            for tok in args[1:]:
                t = tok.lower()
                if t in ("put", "call"):
                    side = t.upper()
                elif t.startswith("dte="):
                    try:
                        dte_target = int(t.split("=", 1)[1])
                    except ValueError:
                        return f"Bad dte: <code>{_e(tok)}</code>"
                elif t.startswith("strikes="):
                    try:
                        strike_count = int(t.split("=", 1)[1]) * 2
                    except ValueError:
                        return f"Bad strikes: <code>{_e(tok)}</code>"

            dte_min = max(1, dte_target - 7)
            dte_max = dte_target + 14

            sides = ["PUT", "CALL"] if side == "BOTH" else [side]
            header = (
                f"<b>{_e(symbol)} chain</b>  "
                f"DTE {dte_min}-{dte_max}, {strike_count // 2} strikes/side"
            )
            blocks = [header]
            total = 0
            for s in sides:
                chain = self.client.get_option_chain(
                    symbol, s, dte_min, dte_max, strike_count=strike_count,
                )
                if not chain:
                    blocks.append(f"\n<b>{s}</b>: no data")
                    continue
                # Keep only the earliest expiration for readability
                first_exp = chain[0].expiration
                rows = [c for c in chain if c.expiration == first_exp]
                total += len(rows)
                blocks.append(
                    f"\n<b>{s} {_e(first_exp)}</b> (DTE={rows[0].dte})"
                )
                # Header line
                blocks.append(
                    "<code>strike    bid     ask    Δ      γ       IV    OI</code>"
                )
                for c in rows[:15]:
                    blocks.append(
                        f"<code>{c.strike:>7g} {c.bid:>6.2f} {c.ask:>6.2f} "
                        f"{c.delta:>+5.2f} {c.gamma:>6.4f} {c.iv:>5.1f} "
                        f"{c.open_interest:>5d}</code>"
                    )
            if total == 0:
                return f"No chain data for <code>{_e(symbol)}</code>."
            return "\n".join(blocks)

        def _theta(args):
            theta = next((s for s in self.strategies if getattr(s, "name", "") == "theta"), None)
            if theta is None:
                return "Theta strategy not loaded. Add <code>theta</code> to STRATEGIES."
            rows = theta.theta_status()  # type: ignore[attr-defined]
            if not rows:
                return f"<b>Theta wheel</b>\n\nNo tracked symbols. Universe: <code>{_e(','.join(self.config.theta_symbols))}</code>"
            lines = ["<b>Theta wheel</b>", ""]
            for r in rows:
                state = r["state"]
                sym = r["symbol"]
                if state == "CASH":
                    lines.append(f"<code>{_e(sym)}</code> CASH")
                elif state == "LONG_STOCK":
                    lines.append(f"<code>{_e(sym)}</code> LONG {r['shares']}sh @ ${r['cost_basis']:.2f}")
                elif state in ("SHORT_PUT", "SHORT_CALL"):
                    leg = r.get("leg", {})
                    lines.append(
                        f"<code>{_e(sym)}</code> {state} "
                        f"{leg.get('contracts', 0)}x {leg.get('side', '?')} "
                        f"{leg.get('strike', 0):g} {leg.get('expiration', '?')} "
                        f"credit=${leg.get('credit', 0):.2f}"
                    )
                else:
                    lines.append(f"<code>{_e(sym)}</code> {_e(state)}")
            return "\n".join(lines)

        def _hilo(args):
            """Scan watchlist ∪ ah_sniper ∪ momentum for new N-day extremes.

            Usage:
              /hilo                   cached daily OHLCV, default windows 5/20/60/252
              /hilo refresh           force-refetch from Schwab
              /hilo 5,20              only scan these windows
              /hilo refresh 5,20,60   both
            """
            from schwabagent.hilo import (
                DEFAULT_WINDOWS,
                WINDOW_LABELS,
                load_or_fetch,
                scan_hilo,
            )

            force_refresh = False
            windows: tuple[int, ...] = DEFAULT_WINDOWS
            for tok in args:
                t = tok.lower()
                if t in ("refresh", "-r", "--refresh"):
                    force_refresh = True
                elif "," in t or t.isdigit():
                    try:
                        windows = tuple(int(w) for w in t.split(",") if w.strip())
                    except ValueError:
                        return f"Bad windows: <code>{_e(tok)}</code>"

            data = load_or_fetch(self.config, self.client, force_refresh=force_refresh)
            if not data:
                return "No OHLCV data returned."
            rows = scan_hilo(data, windows=windows)

            def _label(w: int) -> str:
                return WINDOW_LABELS.get(w, f"{w}d")

            win_str = "/".join(_label(w) for w in windows)
            header = f"<b>Hi/Lo scan</b> — {len(data)} symbols, {_e(win_str)}"

            if not rows:
                return f"{header}\n\nNo new highs or lows."

            highs = sorted(
                (r for r in rows if r.hits_high),
                key=lambda r: (-max(r.hits_high), -r.pct_change),
            )
            lows = sorted(
                (r for r in rows if r.hits_low),
                key=lambda r: (-max(r.hits_low), r.pct_change),
            )

            lines = [header, ""]
            if highs:
                lines.append(f"<b>New highs ({len(highs)})</b>")
                for r in highs:
                    tags = " ".join(_label(w) for w in windows if w in r.hits_high)
                    sign = "+" if r.pct_change >= 0 else ""
                    lines.append(
                        f"<code>{_e(r.symbol)}</code> ${r.last_close:,.2f} "
                        f"{sign}{r.pct_change:.2f}% [{_e(tags)}]"
                    )
            if lows:
                if highs:
                    lines.append("")
                lines.append(f"<b>New lows ({len(lows)})</b>")
                for r in lows:
                    tags = " ".join(_label(w) for w in windows if w in r.hits_low)
                    sign = "+" if r.pct_change >= 0 else ""
                    lines.append(
                        f"<code>{_e(r.symbol)}</code> ${r.last_close:,.2f} "
                        f"{sign}{r.pct_change:.2f}% [{_e(tags)}]"
                    )

            quiet = len(data) - len(rows)
            lines.append(f"\n<i>{len(highs)} high, {len(lows)} low, {quiet} quiet</i>")
            return "\n".join(lines)

        def _earnings(args):
            """Upcoming earnings from Briefing.com — filtered to agent universe.

            Usage:
              /earnings                 universe, next 7 days
              /earnings all             all tickers, next 7 days
              /earnings 14              universe, next 14 days
              /earnings AAPL            specific ticker, full 5 weeks
              /earnings refresh         force recache
            """
            from datetime import datetime as _dt, timedelta as _td
            from zoneinfo import ZoneInfo as _ZI
            from schwabagent.scrapers.earnings_calendar import (
                agent_universe,
                fetch_earnings_calendar,
                filter_rows,
            )

            force_refresh = False
            show_all = False
            days = 7
            symbol: str | None = None
            for tok in args:
                t = tok.lower()
                if t in ("refresh", "-r", "--refresh"):
                    force_refresh = True
                elif t == "all":
                    show_all = True
                elif t.isdigit():
                    days = int(t)
                elif re.fullmatch(r"[A-Za-z]{1,6}", tok):
                    symbol = tok.upper()

            rows = fetch_earnings_calendar(self.config, force_refresh=force_refresh)
            if not rows:
                return "No earnings data (Briefing layout may have changed)."

            start = _dt.now(_ZI("America/New_York")).date()
            if symbol:
                filter_syms: set[str] | None = {symbol}
                window_end = None
            else:
                filter_syms = None if show_all else agent_universe(self.config)
                window_end = start + _td(days=days)

            filtered = filter_rows(
                rows, symbols=filter_syms, start=start, end=window_end,
            )

            if symbol:
                header = f"<b>Earnings — {_e(symbol)}</b>"
            elif show_all:
                header = f"<b>Earnings — all tickers, next {days}d</b>"
            else:
                header = f"<b>Earnings — universe, next {days}d</b>"

            if not filtered:
                return f"{header}\n\nNo matching releases."

            filtered = sorted(
                filtered,
                key=lambda r: (r.date, 0 if r.session == "BMO" else 1, r.symbol),
            )
            lines = [header, ""]
            current_date = None
            for r in filtered:
                if r.date != current_date:
                    current_date = r.date
                    lines.append(f"<b>{_e(current_date)}</b>")
                mark = "✓" if r.confirmed else "·"
                cons = (
                    f"cons ${r.consensus_eps:.2f}"
                    if r.consensus_eps is not None else "cons —"
                )
                if r.reported and r.actual_eps is not None:
                    tag = (
                        "BEAT"
                        if (r.consensus_eps is not None
                            and r.actual_eps >= r.consensus_eps)
                        else "MISS"
                    )
                    actual = f"act ${r.actual_eps:.2f} [{tag}]"
                else:
                    actual = "act —"
                lines.append(
                    f"{mark} <code>{_e(r.symbol)}</code> {r.session} "
                    f"{_e(r.company[:24])} · {actual} · {cons}"
                )

            out = "\n".join(lines)
            if len(out) > 3900:
                out = out[:3900] + "\n<i>… truncated</i>"
            return out

        def _dividends(args):
            """Upcoming ex-dividends from Nasdaq — filtered to agent universe.

            Usage:
              /dividends                universe, next 7 days
              /dividends all            all tickers, next 7 days
              /dividends 14             universe, next 14 days
              /dividends AAPL           specific ticker, full window
              /dividends refresh        force recache
            """
            from datetime import datetime as _dt, timedelta as _td
            from zoneinfo import ZoneInfo as _ZI
            from schwabagent.scrapers.dividend_calendar import (
                fetch_dividend_calendar,
                filter_rows as _div_filter_rows,
            )
            from schwabagent.scrapers.earnings_calendar import agent_universe

            force_refresh = False
            show_all = False
            days = 7
            symbol: str | None = None
            for tok in args:
                t = tok.lower()
                if t in ("refresh", "-r", "--refresh"):
                    force_refresh = True
                elif t == "all":
                    show_all = True
                elif t.isdigit():
                    days = int(t)
                elif re.fullmatch(r"[A-Za-z]{1,6}", tok):
                    symbol = tok.upper()

            rows = fetch_dividend_calendar(self.config, force_refresh=force_refresh)
            if not rows:
                return "No dividend data (Nasdaq API layout may have changed)."

            start = _dt.now(_ZI("America/New_York")).date()
            if symbol:
                filter_syms: set[str] | None = {symbol}
                window_end = None
            else:
                filter_syms = None if show_all else agent_universe(self.config)
                window_end = start + _td(days=days)

            filtered = _div_filter_rows(
                rows, symbols=filter_syms, start=start, end=window_end,
            )

            if symbol:
                header = f"<b>Ex-Dividends — {_e(symbol)}</b>"
            elif show_all:
                header = f"<b>Ex-Dividends — all tickers, next {days}d</b>"
            else:
                header = f"<b>Ex-Dividends — universe, next {days}d</b>"

            if not filtered:
                return f"{header}\n\nNo matching ex-dividends."

            filtered = sorted(filtered, key=lambda r: (r.ex_date, r.symbol))
            lines = [header, ""]
            current_date = None
            for r in filtered:
                if r.ex_date != current_date:
                    current_date = r.ex_date
                    lines.append(f"<b>{_e(current_date)}</b>")
                amt = f"${r.amount:.3f}" if r.amount is not None else "—"
                ann = (
                    f"ann ${r.annual_dividend:.2f}"
                    if r.annual_dividend else "ann —"
                )
                pay = f"pay {r.payment_date}" if r.payment_date else "pay —"
                lines.append(
                    f"· <code>{_e(r.symbol)}</code> "
                    f"{_e(r.company[:24])} · {amt} · {ann} · {pay}"
                )

            out = "\n".join(lines)
            if len(out) > 3900:
                out = out[:3900] + "\n<i>… truncated</i>"
            return out

        def _splits(args):
            """Upcoming stock splits from Briefing.com.

            Usage:
              /splits                   universe, full window
              /splits all               all tickers, full window
              /splits 14                universe, next 14 days
              /splits AAPL              specific ticker
              /splits reverse           reverse-splits only (distress signal)
              /splits refresh           force recache
            """
            from datetime import datetime as _dt, timedelta as _td
            from zoneinfo import ZoneInfo as _ZI
            from schwabagent.scrapers.splits_calendar import (
                fetch_splits_calendar,
                filter_rows as _split_filter_rows,
            )
            from schwabagent.scrapers.earnings_calendar import agent_universe

            force_refresh = False
            show_all = False
            reverse_only = False
            days = 0
            symbol: str | None = None
            for tok in args:
                t = tok.lower()
                if t in ("refresh", "-r", "--refresh"):
                    force_refresh = True
                elif t == "all":
                    show_all = True
                elif t in ("reverse", "rev", "-rv"):
                    reverse_only = True
                elif t.isdigit():
                    days = int(t)
                elif re.fullmatch(r"[A-Za-z]{1,6}", tok):
                    symbol = tok.upper()

            rows = fetch_splits_calendar(self.config, force_refresh=force_refresh)
            if not rows:
                return "No splits data (Briefing layout may have changed)."

            start = _dt.now(_ZI("America/New_York")).date()
            if symbol:
                filter_syms: set[str] | None = {symbol}
                window_end = None
            else:
                filter_syms = None if show_all else agent_universe(self.config)
                window_end = start + _td(days=days) if days > 0 else None

            filtered = _split_filter_rows(
                rows, symbols=filter_syms, start=start, end=window_end,
                reverse_only=reverse_only,
            )

            if symbol:
                header = f"<b>Splits — {_e(symbol)}</b>"
            elif reverse_only:
                header = "<b>Reverse splits — distress watch</b>"
            elif show_all:
                header = f"<b>Splits — all tickers{', next ' + str(days) + 'd' if days else ''}</b>"
            else:
                header = f"<b>Splits — universe{', next ' + str(days) + 'd' if days else ''}</b>"

            if not filtered:
                return f"{header}\n\nNo matching splits."

            filtered = sorted(filtered, key=lambda r: (r.ex_date, r.symbol))
            lines = [header, ""]
            current_date = None
            for r in filtered:
                if r.ex_date != current_date:
                    current_date = r.ex_date
                    lines.append(f"<b>{_e(current_date)}</b>")
                direction = "⮃" if r.is_reverse else "⮁"
                pay = f"pay {r.payable_date}" if r.payable_date else "pay —"
                lines.append(
                    f"{direction} <code>{_e(r.symbol)}</code> "
                    f"{_e(r.company[:24])} · {r.ratio} · {pay}"
                )

            out = "\n".join(lines)
            if len(out) > 3900:
                out = out[:3900] + "\n<i>… truncated</i>"
            return out

        def _ratings(args):
            """Today's analyst upgrades / downgrades from Briefing.com.

            Usage:
              /ratings                  universe, both directions
              /ratings all              all tickers
              /ratings up               upgrades only
              /ratings down             downgrades only
              /ratings AAPL             specific ticker
              /ratings refresh          force recache
            """
            from schwabagent.scrapers.upgrades_downgrades import (
                fetch_ratings,
                filter_rows as _rate_filter_rows,
            )
            from schwabagent.scrapers.earnings_calendar import agent_universe

            force_refresh = False
            show_all = False
            action: str | None = None
            symbol: str | None = None
            for tok in args:
                t = tok.lower()
                if t in ("refresh", "-r", "--refresh"):
                    force_refresh = True
                elif t == "all":
                    show_all = True
                elif t in ("up", "upgrades"):
                    action = "upgrade"
                elif t in ("down", "downgrades"):
                    action = "downgrade"
                elif re.fullmatch(r"[A-Za-z]{1,6}", tok):
                    symbol = tok.upper()

            rows = fetch_ratings(self.config, force_refresh=force_refresh)
            if not rows:
                return "No ratings data (Briefing layout may have changed)."

            if symbol:
                filter_syms: set[str] | None = {symbol}
            else:
                filter_syms = None if show_all else agent_universe(self.config)

            filtered = _rate_filter_rows(rows, symbols=filter_syms, action=action)

            if symbol:
                header = f"<b>Ratings — {_e(symbol)}</b>"
            elif action:
                scope = "all tickers" if show_all else "universe"
                header = f"<b>{action.capitalize()}s — {scope}</b>"
            elif show_all:
                header = "<b>Ratings — all tickers</b>"
            else:
                header = "<b>Ratings — universe</b>"

            if not filtered:
                return f"{header}\n\nNo matching ratings today."

            filtered = sorted(filtered, key=lambda r: (r.action, r.symbol))
            lines = [header, ""]
            current_action = None
            for r in filtered:
                if r.action != current_action:
                    current_action = r.action
                    lines.append(f"<b>— {r.action.capitalize()}s —</b>")
                tgt = f"${r.price_target:.0f}" if r.price_target is not None else "—"
                change = (
                    f"{r.from_rating} » {r.to_rating}"
                    if r.from_rating else r.to_rating
                )
                lines.append(
                    f"· <code>{_e(r.symbol)}</code> "
                    f"{_e(r.firm[:18])} · {_e(change[:32])} · tgt {tgt}"
                )

            out = "\n".join(lines)
            if len(out) > 3900:
                out = out[:3900] + "\n<i>… truncated</i>"
            return out

        def _parse_trade_args(args: list[str]) -> tuple[dict | str, ...]:
            """Parse shared /buy /sell syntax. Returns (opts, error_msg_or_None).

            Syntax: SYMBOL QTY|all [limit=X.XX] [market] [confirm]
            """
            if len(args) < 2:
                return {}, "missing args"
            opts: dict = {
                "symbol": args[0].upper(),
                "qty_raw": args[1].lower(),
                "order_type": None,
                "limit_price": None,
                "confirm": False,
            }
            for tok in args[2:]:
                t = tok.lower()
                if t == "confirm":
                    opts["confirm"] = True
                elif t == "market":
                    opts["order_type"] = "MARKET"
                elif t.startswith("limit="):
                    try:
                        opts["limit_price"] = float(t.split("=", 1)[1])
                        opts["order_type"] = "LIMIT"
                    except ValueError:
                        return {}, f"bad limit price: {_e(tok)}"
                else:
                    return {}, f"unknown token: {_e(tok)}"
            return opts, None

        def _trade_reply(side: str, opts: dict, quote_price: float, qty: int,
                          account, allowed: bool, reason: str, placed: dict | None) -> str:
            sym = opts["symbol"]
            value = qty * quote_price
            mode = "LIVE" if (opts["confirm"] and not self.config.DRY_RUN) else (
                "DRY" if opts["confirm"] else "PREVIEW"
            )
            lines = [
                f"<b>{side} {_e(sym)}</b>  <i>{mode}</i>",
                f"Qty: {qty} @ ~${quote_price:,.2f}",
                f"Value: ${value:,.2f}",
                f"Order type: {_e(opts['order_type'] or self.config.ORDER_TYPE or 'LIMIT')}",
            ]
            if opts["limit_price"] is not None:
                lines.append(f"Limit: ${opts['limit_price']:,.2f}")
            if not allowed:
                lines.append(f"<b>BLOCKED:</b> <code>{_e(reason)}</code>")
                return "\n".join(lines)
            if not opts["confirm"]:
                lines.append(f"\nAdd <code>confirm</code> to place. Cash: ${account.cash_available:,.2f}")
                return "\n".join(lines)
            if placed is None:
                lines.append("\nDRY_RUN=true — not placed. Flip DRY_RUN=false in .env for live.")
                return "\n".join(lines)
            if placed.get("status") != "ok":
                lines.append(f"<b>ORDER FAILED:</b> <code>{_e(placed.get('error', '?'))}</code>")
                return "\n".join(lines)
            lines.append(f"<b>PLACED</b> id=<code>{_e(placed.get('order_id', '?'))}</code>")
            return "\n".join(lines)

        def _execute_trade(side: str, opts: dict, qty: int, quote_price: float,
                            account) -> dict | None:
            """Call place_order and plumb the result through risk + state + alerts."""
            if self.config.DRY_RUN:
                return None
            result = self.client.place_order(
                account.account_hash, opts["symbol"], side, qty,
                order_type=opts["order_type"],
                limit_price=opts["limit_price"],
            )
            if result.get("status") != "ok":
                return result
            self.risk.record_trade(opts["symbol"], side, qty, quote_price, strategy="telegram")
            trade = {
                "strategy": "telegram",
                "symbol": opts["symbol"],
                "side": side,
                "quantity": qty,
                "price": quote_price,
                "value": qty * quote_price,
                "dry_run": False,
                **result,
            }
            self.state.append_trade(trade)
            if self.telegram:
                self.telegram.send_trade_alert(trade)
            if result.get("order_id") and result["order_id"] != "dry":
                try:
                    self.order_tracker.track(
                        order_id=result["order_id"],
                        symbol=opts["symbol"],
                        side=side,
                        quantity=qty,
                        expected_price=quote_price,
                        account_hash=account.account_hash,
                        strategy="telegram",
                    )
                except Exception:
                    logger.exception("order_tracker.track failed for telegram trade")
            return result

        def _buy(args):
            opts, err = _parse_trade_args(args)
            if err:
                return (
                    "Usage: <code>/buy SYMBOL QTY [limit=X.XX] [market] [confirm]</code>\n"
                    f"{_e(err)}"
                )
            try:
                qty = int(opts["qty_raw"])
            except ValueError:
                return f"Invalid qty: <code>{_e(opts['qty_raw'])}</code>"
            if qty <= 0:
                return "Quantity must be positive."

            quotes = self.client.get_quotes([opts["symbol"]])
            q = quotes.get(opts["symbol"]) if quotes else None
            if not q or not (q.ask or q.last):
                return f"No quote for <code>{_e(opts['symbol'])}</code>."
            price = float(q.ask or q.last)

            account = self._get_account()
            allowed, reason = self.risk.can_buy(opts["symbol"], qty, price, account)
            placed = None
            if allowed and opts["confirm"]:
                placed = _execute_trade("BUY", opts, qty, price, account)
            return _trade_reply("BUY", opts, price, qty, account, allowed, reason, placed)

        def _sell(args):
            opts, err = _parse_trade_args(args)
            if err:
                return (
                    "Usage: <code>/sell SYMBOL QTY|all [limit=X.XX] [market] [confirm]</code>\n"
                    f"{_e(err)}"
                )
            account = self._get_account()
            held = next(
                (p for p in account.positions if p.symbol == opts["symbol"] and p.quantity > 0),
                None,
            )
            if held is None:
                return f"No position in <code>{_e(opts['symbol'])}</code>."
            if opts["qty_raw"] == "all":
                qty = int(held.quantity)
            else:
                try:
                    qty = int(opts["qty_raw"])
                except ValueError:
                    return f"Invalid qty: <code>{_e(opts['qty_raw'])}</code>"
            if qty <= 0:
                return "Quantity must be positive."
            if qty > held.quantity:
                return f"Holding {held.quantity:.0f} — cannot sell {qty}."

            quotes = self.client.get_quotes([opts["symbol"]])
            q = quotes.get(opts["symbol"]) if quotes else None
            if not q or not (q.bid or q.last):
                return f"No quote for <code>{_e(opts['symbol'])}</code>."
            price = float(q.bid or q.last)

            allowed, reason = self.risk.can_sell(opts["symbol"], qty, price, account)
            placed = None
            if allowed and opts["confirm"]:
                placed = _execute_trade("SELL", opts, qty, price, account)
            return _trade_reply("SELL", opts, price, qty, account, allowed, reason, placed)

        def _pf(args):
            """Render a Point & Figure chart as a monospace Telegram message.

            Usage:
              /pf SYMBOL                       defaults: box=0.01 rev=3 dur=1 period=2
              /pf SYMBOL box=0.02 rev=2        key=value overrides
              /pf SYMBOL dur=0.5 period=1      shorter lookback
            """
            if not args:
                return (
                    "Usage: <code>/pf SYMBOL [box=0.01] [rev=3] [dur=1] [period=2]</code>\n"
                    "Example: <code>/pf SPY box=0.02 rev=2</code>"
                )
            symbol = args[0].upper()

            # Parse key=value overrides
            params: dict[str, float] = {"box": 0.01, "rev": 3, "dur": 1.0, "period": 2.0}
            for arg in args[1:]:
                if "=" not in arg:
                    continue
                k, v = arg.split("=", 1)
                k = k.strip().lower()
                if k in params:
                    try:
                        params[k] = float(v)
                    except ValueError:
                        return f"Invalid value for <code>{_e(k)}</code>: {_e(v)}"

            from schwabagent.pf import create_pf_chart
            chart = create_pf_chart(
                symbol=symbol,
                client=self.client,
                box_size=params["box"],
                reversal=int(params["rev"]),
                duration=params["dur"],
                period=params["period"],
                method="HL",
                style=False,          # no ANSI, Telegram can't render it
                trend_lines=True,
            )

            # Extract latest meta entry for the summary line
            meta = chart.chart_meta_data
            latest = list(meta.values())[-1] if meta else {}
            signal = str(latest.get("signal", "?"))
            status = str(latest.get("status", "?"))

            body = (chart.chart or "").strip("\n")
            # Cap at Telegram's 4096 char limit minus wrapper overhead
            max_body = 3600
            if len(body) > max_body:
                body = body[-max_body:]
                body = "... (truncated)\n" + body

            return (
                f"<b>P&amp;F {_e(symbol)}</b>  "
                f"box={params['box']:.2%} rev={int(params['rev'])} "
                f"dur={params['dur']:g}y period={params['period']:g}y\n"
                f"Signal: <b>{_e(signal)}</b>  Status: {_e(status)}\n"
                f"<pre>{_e(body)}</pre>"
            )

        bot.register_command("status", _safe(_status), "Account status and connectivity")
        bot.register_command("pnl", _safe(_pnl), "P&L summary by strategy")
        bot.register_command("positions", _safe(_positions), "Current holdings")
        bot.register_command("kill", _safe(_kill), "Activate kill switch")
        bot.register_command("resume", _safe(_resume), "Deactivate kill switch")
        bot.register_command("scan", _safe(_scan), "Read-only scan — top N signals")
        bot.register_command("yields", _safe(_yields), "Treasury yield curve + spreads")
        bot.register_command("commodities", _safe(_commodities), "Commodity ETF prices + daily moves")
        bot.register_command("regime", _safe(_regime), "Current intermarket regime")
        bot.register_command("feedback", _safe(_feedback), "Win-rate + P&L per strategy")
        bot.register_command("drift", _safe(_drift), "ML feedback drift alerts")
        bot.register_command("accounts", _safe(_accounts), "All Schwab account summaries")
        bot.register_command("risk", _safe(_risk), "Risk status + drawdown")
        bot.register_command("recent", _safe(_recent), "Recent executed trades")
        bot.register_command("strategies", _safe(_strategies), "Strategy enable + live flags")
        bot.register_command("quote", _safe(_quote), "Quote for SYMBOL")
        bot.register_command("pf", _safe(_pf), "Point & Figure chart (box=, rev=, dur=, period=)")
        bot.register_command("toggle", _safe(_toggle), "Flip a strategy's live flag")
        bot.register_command("buy", _safe(_buy), "Buy SYMBOL QTY [limit=X] [market] [confirm]")
        bot.register_command("sell", _safe(_sell), "Sell SYMBOL QTY|all [limit=X] [market] [confirm]")
        bot.register_command("theta", _safe(_theta), "Theta wheel state per symbol")
        bot.register_command("hilo", _safe(_hilo), "New N-day highs/lows across universe")
        bot.register_command("earnings", _safe(_earnings), "Upcoming earnings from Briefing.com")
        bot.register_command("dividends", _safe(_dividends), "Upcoming ex-dividends from Nasdaq")
        bot.register_command("splits", _safe(_splits), "Upcoming stock splits from Briefing.com")
        bot.register_command("ratings", _safe(_ratings), "Today's analyst upgrades/downgrades")
        bot.register_command("orders", _safe(_orders), "Open working orders")
        bot.register_command("cancel", _safe(_cancel), "Cancel ORDER_ID [ORDER_ID …]")
        bot.register_command("jobs", _safe(_jobs), "Scheduler jobs + next fire")
        bot.register_command("dream", _safe(_dream), "Dreamcycle status")
        bot.register_command("ratelimit", _safe(_ratelimit), "Schwab API rate limit usage")
        bot.register_command("chain", _safe(_chain), "Option chain SYMBOL [put|call] [dte=N] [strikes=N]")
        bot.register_command("gamma", _safe(_gamma), "Cheap gamma scanner — top N straddles")
        bot.register_command("unusual", _safe(_unusual), "Unusual options activity — vol/OI spikes")
        bot.register_command("covered", _safe(_covered), "Covered call screener — dividend stocks + calls >30 DTE")
        bot.register_command("ivrank", _safe(_ivrank), "Single-symbol IV/RV comparison")
        bot.register_command("tracker", _safe(_tracker), "Pending order fill tracker")
        bot.register_command("sec", _safe(_sec), "Recent SEC filings for SYMBOL")
        bot.register_command("universe", _safe(_universe), "Consolidated symbol universe per strategy")
        bot.register_command("size", _safe(_size), "Max position size under risk rules")
        bot.register_command("sweep", _safe(_sweep), "Sweep idle cash into SWVXX money market")
        bot.register_command("sweepetf", _safe(_sweepetf), "Sweep idle cash into SGOV short-Treasury ETF")

    def _persist_live_flag(self, attr: str, value: bool) -> None:
        """Rewrite .env in place so a runtime flag flip survives restart."""
        env_path = Path(__file__).resolve().parents[2] / ".env"
        if not env_path.exists():
            return
        try:
            lines = env_path.read_text().splitlines()
            updated = False
            for i, line in enumerate(lines):
                if line.startswith(f"{attr}="):
                    lines[i] = f"{attr}={'true' if value else 'false'}"
                    updated = True
                    break
            if not updated:
                lines.append(f"{attr}={'true' if value else 'false'}")
            env_path.write_text("\n".join(lines) + "\n")
        except OSError as e:
            logger.warning("Failed to persist %s to .env: %s", attr, e)

    def _init_client(self) -> SchwabClient:
        client = SchwabClient(self.config)
        if not client.authenticate():
            raise RuntimeError(
                "Schwab authentication failed — ensure SCHWAB_API_KEY, "
                "SCHWAB_APP_SECRET are set and token.json is valid"
            )
        return client

    def _build_strategies(self) -> list[Strategy]:
        """Instantiate the configured strategies."""
        enabled = set(self.config.strategies)
        strategies: list[Strategy] = []

        if "etf_rotation" in enabled:
            strategies.append(ETFRotationStrategy(
                self.client, self.config, self.risk, self.state, llm=self.llm
            ))
        if "momentum" in enabled:
            strategies.append(MomentumStrategy(self.client, self.config, self.risk, self.state))
        if "mean_reversion" in enabled:
            strategies.append(MeanReversionStrategy(self.client, self.config, self.risk, self.state))
        if "trend_following" in enabled:
            strategies.append(TrendFollowingStrategy(self.client, self.config, self.risk, self.state))
        if "composite" in enabled:
            strategies.append(CompositeStrategy(self.client, self.config, self.risk, self.state))
        if "etf_scalp" in enabled:
            strategies.append(ETFScalpStrategy(self.client, self.config, self.risk, self.state))
        if "conviction_hold" in enabled:
            strategies.append(ConvictionHoldStrategy(self.client, self.config, self.risk, self.state))
        if "brown_momentum" in enabled:
            strategies.append(BrownMomentumStrategy(self.client, self.config, self.risk, self.state))
        if "tick_breadth" in enabled:
            strategies.append(TickBreadthStrategy(self.client, self.config, self.risk, self.state))
        if "ah_sniper" in enabled:
            strategies.append(AhSniperStrategy(self.client, self.config, self.risk, self.state))
        if "theta" in enabled:
            strategies.append(ThetaStrategy(self.client, self.config, self.risk, self.state))
        if "gamma_scanner" in enabled:
            strategies.append(GammaScannerStrategy(self.client, self.config, self.risk, self.state))
        if "covered_call_screener" in enabled:
            strategies.append(CoveredCallScreener(self.client, self.config, self.risk, self.state))
        if "unusual_activity" in enabled:
            strategies.append(UnusualActivityStrategy(self.client, self.config, self.risk, self.state))

        if not strategies:
            logger.warning("No strategies loaded — check STRATEGIES in .env")
        else:
            logger.info("Loaded strategies: %s", [s.name for s in strategies])

        return strategies

    # ── Account ───────────────────────────────────────────────────────────────

    def _get_account(self) -> AccountSummary:
        """Fetch the configured account (or first account if hash not set)."""
        account_hash = self.config.SCHWAB_ACCOUNT_HASH
        if account_hash:
            acct = self.client.get_account_summary(account_hash)
            if acct is None:
                raise RuntimeError(f"Could not fetch account {account_hash[:8]}…")
            return acct

        accounts = self.client.get_all_accounts()
        if not accounts:
            raise RuntimeError("No accounts returned from Schwab API")
        if len(accounts) > 1:
            logger.info(
                "Multiple accounts found (%d) — using first. Set SCHWAB_ACCOUNT_HASH to pick one.",
                len(accounts),
            )
        return accounts[0]

    def _get_scalp_account(self) -> AccountSummary | None:
        """Fetch the scalp-specific account if configured, else return None."""
        scalp_hash = self.config.SCALP_ACCOUNT_HASH
        if not scalp_hash:
            return None
        return self.client.get_account_summary(scalp_hash)

    def _inject_account(self, account: AccountSummary) -> None:
        """Push the current account object into every strategy.

        The scalp strategy gets its own account if SCALP_ACCOUNT_HASH is set.
        """
        scalp_account = self._get_scalp_account()

        for s in self.strategies:
            if hasattr(s, "set_account"):
                if isinstance(s, ETFScalpStrategy) and scalp_account:
                    s.set_account(scalp_account)
                else:
                    s.set_account(account)

    # ── run_once ─────────────────────────────────────────────────────────────

    def run_once(self) -> list[dict]:
        """Run one full scan+execute cycle. Returns all trade results."""
        account = self._get_account()
        self.risk.update_peak(account.total_value)
        ok, drawdown = self.risk.check_drawdown(account.total_value)
        if not ok:
            self.console.print(
                f"[red]Kill switch triggered: drawdown={drawdown:.1f}%[/red]"
            )
            if self.telegram:
                self.telegram.send_kill_switch_alert(
                    f"Drawdown {drawdown:.1f}% exceeded limit "
                    f"{self.config.MAX_DRAWDOWN_PCT}%"
                )
            return []

        self._inject_account(account)

        # ── Regime detection ───────────────────────────────────────────────
        regime_result = None
        if self.config.REGIME_ENABLED:
            try:
                # Fetch quotes for reference symbols
                ref_quotes = self.client.get_quotes(self.config.regime_reference_symbols)
                # Try to get price histories for SMA/ROC signals
                ref_histories = {}
                for sym in self.config.regime_reference_symbols:
                    try:
                        hist = self.client.get_ohlcv(sym, period_type="year", period=1)
                        if hist is not None and not hist.empty:
                            ref_histories[sym] = hist
                    except Exception:
                        pass
                regime_result = self.regime_model.detect(ref_quotes, ref_histories or None)
                self.current_regime = regime_result.regime

                if regime_result.changed:
                    prev = regime_result.previous_regime
                    msg = (
                        f"Market regime changed: {prev.label() if prev else '?'} → "
                        f"{regime_result.regime.label()} "
                        f"(confidence: {regime_result.confidence:.0%})"
                    )
                    logger.warning(msg)
                    if self.telegram:
                        self.telegram.send_alert(f"🔄 {msg}")
            except Exception as e:
                logger.error("Regime detection failed: %s", e)

        all_trades: list[dict] = []
        for strategy in self.strategies:
            if self.risk.is_killed():
                break

            # Check auto-tuner state for this strategy
            tuner_state = self.autotuner.get_state(strategy.name)
            if tuner_state.state == "paused":
                logger.debug("[autotune] Skipping %s — paused", strategy.name)
                continue

            try:
                trades = strategy.run_once()
                for t in trades:
                    # Apply auto-tuner sizing adjustment
                    if tuner_state.sizing_factor < 1.0 and t.get("side") == "BUY":
                        t["_autotune_sizing"] = tuner_state.sizing_factor
                    # Apply regime sizing adjustment
                    if regime_result and t.get("side") == "BUY":
                        factor = regime_sizing_factor(regime_result.regime, strategy.name)
                        t["_regime_sizing"] = factor
                        if factor != 1.0 and "quantity" in t:
                            original_qty = t["quantity"]
                            t["quantity"] = max(1, int(t["quantity"] * factor))
                            if t["quantity"] != original_qty:
                                logger.info(
                                    "[regime] %s %s qty %d→%d (factor=%.2f, regime=%s)",
                                    strategy.name, t.get("symbol"),
                                    original_qty, t["quantity"],
                                    factor, regime_result.regime.label(),
                                )
                    # Record to feedback loop
                    if t.get("side") == "SELL":
                        self.feedback.resolve_from_trade(t)
                    if self.telegram:
                        self.telegram.send_trade_alert(t)
                    # Track live orders for fill confirmation
                    if not t.get("dry_run") and t.get("order_id") and t["order_id"] != "dry":
                        self.order_tracker.track(
                            order_id=t["order_id"],
                            symbol=t["symbol"],
                            side=t["side"],
                            quantity=t.get("quantity", 0),
                            expected_price=t.get("price", 0),
                            account_hash=account.account_hash,
                            strategy=t.get("strategy", ""),
                        )
                all_trades.extend(trades)
            except Exception as e:
                logger.error("Strategy %s failed: %s", strategy.name, e)
                if self.telegram:
                    self.telegram.send_error(f"Strategy {strategy.name}: {e}")

        # Check pending order fills
        if self.order_tracker.pending_count > 0:
            try:
                self.order_tracker.check_fills(self.client)
            except Exception as e:
                logger.error("Order fill check failed: %s", e)

        # Run auto-tuner evaluation after each cycle
        try:
            actions = self.autotuner.evaluate()
            if actions:
                logger.info("[autotune] %d actions taken: %s",
                            len(actions), [a["action"] for a in actions])
        except Exception as e:
            logger.error("[autotune] Evaluation failed: %s", e)

        return all_trades

    # ── scan_only ─────────────────────────────────────────────────────────────

    def get_regime(self) -> dict | None:
        """Return current regime info as a dict, or None if disabled."""
        if not self.config.REGIME_ENABLED:
            return None
        try:
            ref_quotes = self.client.get_quotes(self.config.regime_reference_symbols)
            ref_histories = {}
            for sym in self.config.regime_reference_symbols:
                try:
                    hist = self.client.get_ohlcv(sym, period_type="year", period=1)
                    if hist is not None and not hist.empty:
                        ref_histories[sym] = hist
                except Exception:
                    pass
            result = self.regime_model.detect(ref_quotes, ref_histories or None)
            self.current_regime = result.regime
            return result.to_dict()
        except Exception as e:
            logger.error("Regime detection failed: %s", e)
            return None

    def display_regime(self) -> None:
        """Display current regime in rich terminal format."""
        if not self.config.REGIME_ENABLED:
            self.console.print("[yellow]Regime detection is disabled[/yellow]")
            return
        try:
            ref_quotes = self.client.get_quotes(self.config.regime_reference_symbols)
            ref_histories = {}
            for sym in self.config.regime_reference_symbols:
                try:
                    hist = self.client.get_ohlcv(sym, period_type="year", period=1)
                    if hist is not None and not hist.empty:
                        ref_histories[sym] = hist
                except Exception:
                    pass
            result = self.regime_model.detect(ref_quotes, ref_histories or None)
            self.current_regime = result.regime
            RegimeModel.display_regime(result)
        except Exception as e:
            self.console.print(f"[red]Regime detection failed: {e}[/red]")

    def scan_only(self) -> list[dict]:
        """Run scan phase only across all strategies — no execution.

        Returns combined opportunity list, deduplicated by symbol, with
        the best absolute score per symbol kept.
        """
        account = self._get_account()
        self._inject_account(account)

        # Include regime in scan output
        regime_info = self.get_regime() if self.config.REGIME_ENABLED else None

        seen: dict[str, dict] = {}
        all_signals: list[dict] = []
        for strategy in self.strategies:
            try:
                opps = strategy.scan()
                all_signals.extend(opps)
                for opp in opps:
                    sym = opp["symbol"]
                    if sym not in seen or abs(opp["score"]) > abs(seen[sym]["score"]):
                        seen[sym] = opp
            except Exception as e:
                logger.error("Strategy %s scan failed: %s", strategy.name, e)

        # Record all signals to feedback loop
        if all_signals:
            self.feedback.record_batch(all_signals)

        return sorted(seen.values(), key=lambda o: abs(o["score"]), reverse=True)

    # ── run_loop ──────────────────────────────────────────────────────────────

    def run_loop(self, interval_seconds: int | None = None) -> None:
        """Run strategies in a loop until interrupted."""
        interval = interval_seconds or self.config.SCAN_INTERVAL_SECONDS
        self._running = True

        def _stop(sig, frame):
            self._running = False
            logger.info("Shutdown signal received")

        try:
            signal.signal(signal.SIGINT, _stop)
            signal.signal(signal.SIGTERM, _stop)
        except ValueError:
            pass  # not in main thread

        # Start background services
        self.stream.start()
        self.dreamcycle.start(interval_minutes=30)
        self.scheduler.setup_defaults(self)
        self.scheduler.start()

        mode = "DRY RUN" if self.config.DRY_RUN else "LIVE"
        self.console.rule(f"[bold green]Schwab Agent Started ({mode})[/bold green]")
        etf_strat = next((s for s in self.strategies if isinstance(s, ETFRotationStrategy)), None)
        if etf_strat:
            self.console.print(f"  ETF universe: {', '.join(self.config.etf_universe)}")
            self.console.print(f"  Top N: {self.config.ETF_TOP_N}  Bear filter: {self.config.ETF_BEAR_FILTER}")
        self.console.print(f"  Watchlist : {', '.join(self.config.watchlist)}")
        self.console.print(f"  Strategies: {', '.join(s.name for s in self.strategies)}")
        self.console.print(f"  Interval  : {interval}s")
        self.console.print(f"  Max expsr : ${self.config.MAX_TOTAL_EXPOSURE:,.0f}")
        self.console.print()

        cycle = 0
        while self._running:
            cycle += 1
            logger.info("=== Cycle %d ===", cycle)
            try:
                trades = self.run_once()
                if trades:
                    self._print_trades(trades)
            except Exception as e:
                logger.error("Cycle %d failed: %s", cycle, e)

            if cycle % 6 == 0:
                self._print_status()

            for _ in range(interval):
                if not self._running:
                    break
                time.sleep(1)

        self.scheduler.stop()
        self.stream.stop()
        self.dreamcycle.stop()
        self.console.rule("[bold red]Schwab Agent Stopped[/bold red]")
        self._print_status()

    # ── P&L summary ──────────────────────────────────────────────────────────

    def get_pnl_summary(self) -> dict:
        """Return a P&L summary dict (strategy → {trades, pnl, win_rate})."""
        pnl = self.state.get_strategy_pnl()
        summary = {}
        for strategy, data in pnl.items():
            trades = data.get("trades", 0)
            wins = data.get("wins", 0)
            summary[strategy] = {
                "trades": trades,
                "realized_pnl": data.get("realized_pnl", 0.0),
                "wins": wins,
                "losses": data.get("losses", 0),
                "win_rate": round(wins / trades * 100, 1) if trades > 0 else 0.0,
            }
        # Include session stats
        for s in self.strategies:
            st = s.stats()
            name = st["strategy"]
            if name in summary:
                summary[name]["session_trades"] = st["trades_session"]
                summary[name]["session_pnl"] = st["pnl_session"]
        return summary

    # ── Display helpers ───────────────────────────────────────────────────────

    def _print_trades(self, trades: list[dict]) -> None:
        table = Table(title="Trades Executed", show_lines=True)
        table.add_column("Strategy", style="cyan")
        table.add_column("Symbol", style="bold")
        table.add_column("Side")
        table.add_column("Qty", justify="right")
        table.add_column("Price", justify="right")
        table.add_column("Value", justify="right")
        table.add_column("Signal")
        table.add_column("Dry Run")

        for t in trades:
            side_color = "green" if t.get("side") == "BUY" else "red"
            table.add_row(
                t.get("strategy", ""),
                t.get("symbol", ""),
                f"[{side_color}]{t.get('side', '')}[/{side_color}]",
                str(t.get("quantity", "")),
                f"${t.get('price', 0):.2f}",
                f"${t.get('value', 0):.2f}",
                t.get("signal", ""),
                "yes" if t.get("dry_run") else "NO",
            )

        self.console.print(table)

    def _print_status(self) -> None:
        try:
            account = self._get_account()
        except Exception:
            account = None
        risk = self.risk.status(account=account)
        self.console.print(
            f"\n  [bold]Risk:[/bold] "
            f"peak=${risk['peak_value']:,.2f}  "
            f"max_dd={risk['max_drawdown_pct']}%  "
            f"killed={'[red]YES[/red]' if risk['killed'] else '[green]no[/green]'}"
        )
        tr = risk.get("trading_rules", {})
        acct_type = tr.get("account_type", "?")
        self.console.print(f"  [bold]Account:[/bold] type={acct_type}")
        if tr.get("is_closing_only"):
            self.console.print("  [red bold]CLOSING ONLY — new buys blocked by Schwab[/red bold]")
        if tr.get("pdt_applies"):
            remaining = tr.get("round_trips_remaining", "?")
            used = tr.get("round_trips", 0)
            color = "red" if remaining == 0 else "yellow" if remaining == 1 else "green"
            self.console.print(
                f"  [bold]PDT:[/bold] [{color}]{used}/3 day trades used[/{color}] "
                f"({remaining} remaining — from Schwab roundTrips)"
            )

        table = Table(title="Strategy Session Stats", show_lines=False)
        table.add_column("Strategy", style="cyan")
        table.add_column("Session trades", justify="right")
        table.add_column("Total trades", justify="right")
        table.add_column("Realized P&L", justify="right")
        table.add_column("Win rate", justify="right")

        for s in self.strategies:
            st = s.stats()
            pnl = st["pnl_realized"]
            color = "green" if pnl >= 0 else "red"
            table.add_row(
                st["strategy"],
                str(st["trades_session"]),
                str(st["trades_total"]),
                f"[{color}]${pnl:+.2f}[/{color}]",
                f"{st['win_rate']:.1f}%",
            )

        self.console.print(table)
        self.console.print()
