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
from schwabagent.strategies.mean_reversion import MeanReversionStrategy
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
        self.strategies: list[Strategy] = self._build_strategies()

    # ── Initialization ────────────────────────────────────────────────────────

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

        if "momentum" in enabled:
            strategies.append(MomentumStrategy(self.client, self.config, self.risk, self.state))
        if "mean_reversion" in enabled:
            strategies.append(MeanReversionStrategy(self.client, self.config, self.risk, self.state))
        if "trend_following" in enabled:
            strategies.append(TrendFollowingStrategy(self.client, self.config, self.risk, self.state))
        if "composite" in enabled:
            strategies.append(CompositeStrategy(self.client, self.config, self.risk, self.state))

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

    def _inject_account(self, account: AccountSummary) -> None:
        """Push the current account object into every strategy."""
        for s in self.strategies:
            if hasattr(s, "set_account"):
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
            return []

        self._inject_account(account)

        all_trades: list[dict] = []
        for strategy in self.strategies:
            if self.risk.is_killed():
                break
            try:
                trades = strategy.run_once()
                all_trades.extend(trades)
            except Exception as e:
                logger.error("Strategy %s failed: %s", strategy.name, e)

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
        for strategy in self.strategies:
            try:
                opps = strategy.scan()
                for opp in opps:
                    sym = opp["symbol"]
                    if sym not in seen or abs(opp["score"]) > abs(seen[sym]["score"]):
                        seen[sym] = opp
            except Exception as e:
                logger.error("Strategy %s scan failed: %s", strategy.name, e)

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

        mode = "DRY RUN" if self.config.DRY_RUN else "LIVE"
        self.console.rule(f"[bold green]Schwab Agent Started ({mode})[/bold green]")
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
        risk = self.risk.status()
        self.console.print(
            f"\n  [bold]Risk:[/bold] "
            f"peak=${risk['peak_value']:,.2f}  "
            f"max_dd={risk['max_drawdown_pct']}%  "
            f"killed={'[red]YES[/red]' if risk['killed'] else '[green]no[/green]'}"
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
