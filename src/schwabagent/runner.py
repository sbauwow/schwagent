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
from schwabagent.intermarket import RegimeModel, regime_sizing_factor
from schwabagent.strategies.base import Strategy
from schwabagent.strategies.composite import CompositeStrategy
from schwabagent.strategies.etf_rotation import ETFRotationStrategy
from schwabagent.strategies.mean_reversion import MeanReversionStrategy
from schwabagent.strategies.conviction_hold import ConvictionHoldStrategy
from schwabagent.strategies.etf_scalp import ETFScalpStrategy
from schwabagent.strategies.momentum import MomentumStrategy
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

        bot.register_command("status", _safe(_status), "Account status and connectivity")
        bot.register_command("pnl", _safe(_pnl), "P&L summary by strategy")
        bot.register_command("positions", _safe(_positions), "Current holdings")
        bot.register_command("kill", _safe(_kill), "Activate kill switch")
        bot.register_command("resume", _safe(_resume), "Deactivate kill switch")
        bot.register_command("scan", _safe(_scan), "Read-only scan — top N signals")
        bot.register_command("regime", _safe(_regime), "Current intermarket regime")
        bot.register_command("feedback", _safe(_feedback), "Win-rate + P&L per strategy")
        bot.register_command("drift", _safe(_drift), "ML feedback drift alerts")
        bot.register_command("accounts", _safe(_accounts), "All Schwab account summaries")
        bot.register_command("risk", _safe(_risk), "Risk status + drawdown")
        bot.register_command("recent", _safe(_recent), "Recent executed trades")
        bot.register_command("strategies", _safe(_strategies), "Strategy enable + live flags")
        bot.register_command("quote", _safe(_quote), "Quote for SYMBOL")
        bot.register_command("toggle", _safe(_toggle), "Flip a strategy's live flag")

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
