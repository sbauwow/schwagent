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
from schwabagent.strategies.base import Strategy
from schwabagent.strategies.composite import CompositeStrategy
from schwabagent.strategies.etf_rotation import ETFRotationStrategy
from schwabagent.strategies.mean_reversion import MeanReversionStrategy
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
        self.autotuner = self._init_autotuner()
        self.dreamcycle = self._init_dreamcycle()
        self.order_tracker = self._init_order_tracker()
        self.stream = self._init_streaming()
        self.scheduler = self._init_scheduler()

    # ── Initialization ────────────────────────────────────────────────────────

    def _init_llm(self):
        """Initialise Ollama client if LLM_ENABLED, else return None."""
        if not self.config.LLM_ENABLED:
            return None
        from schwabagent.llm import OllamaClient
        llm = OllamaClient(
            host=self.config.OLLAMA_HOST,
            model=self.config.OLLAMA_MODEL,
            timeout=self.config.OLLAMA_TIMEOUT,
        )
        if llm.is_available():
            logger.info("LLM enabled: %s @ %s", self.config.OLLAMA_MODEL, self.config.OLLAMA_HOST)
        else:
            logger.warning(
                "LLM_ENABLED=true but Ollama not reachable at %s — disabling LLM overlay",
                self.config.OLLAMA_HOST,
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
        """Register bot command handlers that return MarkdownV2 text."""
        from schwabagent.telegram import _escape_md

        def _status_handler() -> str:
            try:
                account = self._get_account()
                risk = self.risk.status(account=account)
                tr = risk.get("trading_rules", {})
                lines = [
                    "*Schwab Agent Status*\n",
                    f"Account: `{account.account_number}` \\({tr.get('account_type', '?')}\\)",
                    f"Value: ${account.total_value:,.2f}",
                    f"Cash: ${account.cash_available:,.2f}",
                    f"Positions: {len(account.positions)}",
                    f"Kill switch: {'YES' if risk['killed'] else 'no'}",
                    f"DRY\\_RUN: {self.config.DRY_RUN}",
                ]
                return "\n".join(lines)
            except Exception as e:
                return f"*Error:* `{_escape_md(str(e))}`"

        def _pnl_handler() -> str:
            try:
                summary = self.get_pnl_summary()
                if not summary:
                    return "No P&L data yet\\."
                lines = ["*P&L Summary*\n"]
                total = 0.0
                for strat, data in sorted(summary.items()):
                    pnl = data.get("realized_pnl", 0)
                    trades = data.get("trades", 0)
                    wr = data.get("win_rate", 0)
                    total += pnl
                    sign = "\\+" if pnl >= 0 else ""
                    lines.append(f"`{_escape_md(strat)}` {sign}${pnl:,.2f} \\({trades}t, {wr:.0f}%\\)")
                sign = "\\+" if total >= 0 else ""
                lines.append(f"\n*Total: {sign}${total:,.2f}*")
                return "\n".join(lines)
            except Exception as e:
                return f"*Error:* `{_escape_md(str(e))}`"

        def _positions_handler() -> str:
            try:
                account = self._get_account()
                if not account.positions:
                    return "No open positions\\."
                lines = ["*Current Positions*\n"]
                for p in account.positions:
                    pnl_str = f"${p.unrealized_pnl:+,.2f}" if p.unrealized_pnl else ""
                    lines.append(
                        f"`{_escape_md(p.symbol)}` {p.quantity:.0f} shares "
                        f"@ ${p.avg_price:,.2f} {_escape_md(pnl_str)}"
                    )
                return "\n".join(lines)
            except Exception as e:
                return f"*Error:* `{_escape_md(str(e))}`"

        def _kill_handler() -> str:
            self.risk.kill("Manual kill via Telegram")
            return "*Kill switch activated\\.* All trading halted\\."

        def _resume_handler() -> str:
            self.risk.unkill()
            return "*Kill switch cleared\\.* Trading may resume\\."

        bot.register_command("status", _status_handler)
        bot.register_command("pnl", _pnl_handler)
        bot.register_command("positions", _positions_handler)
        bot.register_command("kill", _kill_handler)
        bot.register_command("resume", _resume_handler)

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

    def scan_only(self) -> list[dict]:
        """Run scan phase only across all strategies — no execution.

        Returns combined opportunity list, deduplicated by symbol, with
        the best absolute score per symbol kept.
        """
        account = self._get_account()
        self._inject_account(account)

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
