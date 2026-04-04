"""CLI entry point for the Schwab trading agent."""
from __future__ import annotations

import argparse
import logging
import sys

from rich.console import Console
from rich.table import Table

from schwabagent.config import Config


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="schwab-agent",
        description="Automated equity trading agent for Charles Schwab",
    )
    parser.add_argument("--live", action="store_true", help="Enable live trading (overrides DRY_RUN)")
    parser.add_argument("--dry-run", action="store_true", default=False, help="Force dry-run mode")
    parser.add_argument("--once", action="store_true", help="Run one scan+execute cycle and exit")
    parser.add_argument("--scan", action="store_true", help="Scan only — show signals, no execution")
    parser.add_argument("--interval", type=int, default=None, help="Loop interval in seconds")
    parser.add_argument("--account", type=str, default=None, help="Schwab account hash override")
    parser.add_argument("--strategies", type=str, default=None, help="Comma-separated strategies to run")
    parser.add_argument("--pnl", action="store_true", help="Show P&L summary and exit")
    parser.add_argument("--status", action="store_true", help="Show agent status and exit")

    args = parser.parse_args()

    config = Config()
    console = Console()

    # Apply CLI overrides
    if args.live:
        config.DRY_RUN = False
    if args.dry_run:
        config.DRY_RUN = True
    if args.account:
        config.SCHWAB_ACCOUNT_HASH = args.account
    if args.strategies:
        config.STRATEGIES = args.strategies

    # Setup logging
    logging.basicConfig(
        level=getattr(logging, config.log_level, logging.INFO),
        format="%(asctime)s %(name)-24s %(levelname)-8s %(message)s",
        datefmt="%H:%M:%S",
    )

    # Validate config
    errors = config.validate()
    if errors:
        for e in errors:
            console.print(f"  [red]✗[/red] {e}")
        if not config.DRY_RUN:
            sys.exit(1)

    # ── Status ─────────────────────────────────────────────────────────────

    if args.status:
        _cmd_status(config, console)
        return

    # ── P&L summary ────────────────────────────────────────────────────────

    if args.pnl:
        _cmd_pnl(config, console)
        return

    # ── Build runner (requires auth) ────────────────────────────────────────

    from schwabagent.runner import AgentRunner

    try:
        runner = AgentRunner(config)
    except RuntimeError as e:
        console.print(f"[red]Startup error:[/red] {e}")
        sys.exit(1)

    # Optionally filter strategies
    if args.strategies:
        requested = set(s.strip().lower() for s in args.strategies.split(","))
        runner.strategies = [s for s in runner.strategies if s.name in requested]
        if not runner.strategies:
            console.print(f"[red]No strategies matched:[/red] {args.strategies}")
            sys.exit(1)

    # ── Scan only ──────────────────────────────────────────────────────────

    if args.scan:
        opps = runner.scan_only()
        _print_opportunities(opps, console)
        return

    # ── Once ───────────────────────────────────────────────────────────────

    if args.once:
        trades = runner.run_once()
        if trades:
            runner._print_trades(trades)
        else:
            console.print("  No trades executed this cycle.")
        runner._print_status()
        return

    # ── Loop ───────────────────────────────────────────────────────────────

    interval = args.interval or config.SCAN_INTERVAL_SECONDS
    runner.run_loop(interval_seconds=interval)


# ── Sub-command helpers ────────────────────────────────────────────────────────

def _cmd_status(config: Config, console: Console) -> None:
    """Print agent config and account summary."""
    console.print()
    console.rule("[cyan]Schwab Agent Status[/cyan]")
    console.print()

    # Config summary
    table = Table(show_header=False, box=None, padding=(0, 2))
    table.add_column("Key", style="bold")
    table.add_column("Value")
    table.add_row("DRY_RUN", str(config.DRY_RUN))
    table.add_row("WATCHLIST", ", ".join(config.watchlist))
    table.add_row("STRATEGIES", ", ".join(config.strategies))
    table.add_row("MAX_TOTAL_EXPOSURE", f"${config.MAX_TOTAL_EXPOSURE:,.0f}")
    table.add_row("MAX_POSITION_VALUE", f"${config.MAX_POSITION_VALUE:,.0f}")
    table.add_row("MAX_DRAWDOWN_PCT", f"{config.MAX_DRAWDOWN_PCT}%")
    table.add_row("SCAN_INTERVAL", f"{config.SCAN_INTERVAL_SECONDS}s")
    table.add_row("LLM_ENABLED", str(config.LLM_ENABLED))
    console.print(table)
    console.print()

    # Try to connect and show accounts
    from schwabagent.schwab_client import SchwabClient
    client = SchwabClient(config)
    if client.authenticate():
        console.print("  [green]✓[/green] Schwab API connected")
        accounts = client.get_all_accounts()
        if accounts:
            for a in accounts:
                console.print(
                    f"    [cyan]{a.account_number}[/cyan]  "
                    f"value=${a.total_value:,.2f}  "
                    f"cash=${a.cash_available:,.2f}  "
                    f"positions={len(a.positions)}"
                )
    else:
        console.print("  [red]✗[/red] Schwab API not reachable")
    console.print()


def _cmd_pnl(config: Config, console: Console) -> None:
    """Print P&L summary from persisted state."""
    from schwabagent.persistence import StateStore

    state = StateStore(config.STATE_DIR)
    pnl = state.get_strategy_pnl()
    history = state.get_trade_history(limit=20)

    console.print()
    console.rule("[cyan]Schwab Agent P&L Summary[/cyan]")
    console.print()

    if pnl:
        table = Table(show_lines=False)
        table.add_column("Strategy", style="cyan")
        table.add_column("Trades", justify="right")
        table.add_column("Realized P&L", justify="right")
        table.add_column("Wins", justify="right")
        table.add_column("Losses", justify="right")
        table.add_column("Win %", justify="right")

        total_pnl = 0.0
        for name, data in sorted(pnl.items()):
            trades = data.get("trades", 0)
            wins = data.get("wins", 0)
            realized = data.get("realized_pnl", 0.0)
            losses = data.get("losses", 0)
            wr = wins / trades * 100 if trades > 0 else 0.0
            color = "green" if realized >= 0 else "red"
            table.add_row(
                name,
                str(trades),
                f"[{color}]${realized:+.2f}[/{color}]",
                str(wins),
                str(losses),
                f"{wr:.1f}%",
            )
            total_pnl += realized

        console.print(table)
        total_color = "green" if total_pnl >= 0 else "red"
        console.print(f"\n  Total realized P&L: [{total_color}]${total_pnl:+.2f}[/{total_color}]")
    else:
        console.print("  No P&L data yet.")

    if history:
        console.print(f"\n  Recent trades ({len(history)} shown):")
        htable = Table(show_lines=False)
        htable.add_column("Time")
        htable.add_column("Strategy", style="cyan")
        htable.add_column("Symbol", style="bold")
        htable.add_column("Side")
        htable.add_column("Qty", justify="right")
        htable.add_column("Price", justify="right")
        htable.add_column("Value", justify="right")

        for t in history[-20:]:
            side = t.get("side", "")
            side_color = "green" if side == "BUY" else "red"
            htable.add_row(
                t.get("timestamp", "")[:19].replace("T", " "),
                t.get("strategy", ""),
                t.get("symbol", ""),
                f"[{side_color}]{side}[/{side_color}]",
                str(t.get("quantity", "")),
                f"${t.get('price', 0):.2f}",
                f"${t.get('value', 0):.2f}",
            )
        console.print(htable)
    else:
        console.print("\n  No trade history yet.")

    console.print()


def _print_opportunities(opps: list[dict], console: Console) -> None:
    """Print scan results to console."""
    if not opps:
        console.print("  No opportunities found.")
        return

    table = Table(title=f"Scan Results ({len(opps)} symbols)", show_lines=True)
    table.add_column("Symbol", style="bold")
    table.add_column("Signal")
    table.add_column("Score", justify="right")
    table.add_column("Price", justify="right")
    table.add_column("Strategy", style="cyan")
    table.add_column("Reason")

    for opp in opps:
        signal_str = opp.get("signal", "")
        if hasattr(signal_str, "value"):
            signal_str = signal_str.value
        score = opp.get("score", 0.0)
        color = "green" if score > 0 else ("red" if score < 0 else "dim")
        table.add_row(
            opp.get("symbol", ""),
            f"[{color}]{signal_str}[/{color}]",
            f"[{color}]{score:+.2f}[/{color}]",
            f"${opp.get('price', 0):.2f}",
            opp.get("strategy", ""),
            (opp.get("reason", "") or "")[:60],
        )

    console.print(table)


if __name__ == "__main__":
    main()
