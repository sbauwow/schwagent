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
        prog="schwagent",
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
    parser.add_argument("--web", action="store_true", help="Start the web dashboard")
    parser.add_argument("--port", type=int, default=8898, help="Web dashboard port (default: 8898)")
    parser.add_argument("--autoresearch", action="store_true",
                        help="Run the auto-research pipeline (backtest + validation + LLM critique)")
    parser.add_argument("--autoresearch-force-fetch", action="store_true",
                        help="Force historical data re-fetch even if the CSV is fresh")

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

    # ── Web dashboard ─────────────────────────────────────────────────────

    if args.web:
        from schwabagent.web.app import run_server
        run_server(config, port=args.port)
        return

    # ── Auto-research pipeline ────────────────────────────────────────────

    if args.autoresearch:
        _cmd_autoresearch(config, console, force_fetch=args.autoresearch_force_fetch)
        return

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


def _cmd_autoresearch(config: Config, console: Console, force_fetch: bool = False) -> None:
    """Run the auto-research pipeline across every configured strategy."""
    from schwabagent.autoresearch import AutoResearchPipeline, BACKTESTABLE_STRATEGIES
    from schwabagent.llm import LLMClient
    from schwabagent.schwab_client import SchwabClient

    console.print()
    console.rule("[cyan]Schwab Agent Auto-Research[/cyan]")
    console.print()

    # Schwab client: needed for data fetch. If auth fails the pipeline still
    # runs against any existing cached CSV.
    client: SchwabClient | None = None
    try:
        c = SchwabClient(config)
        if c.authenticate():
            client = c
            console.print("  [green]✓[/green] Schwab API connected")
        else:
            console.print("  [yellow]![/yellow] Schwab API not reachable — using cached CSV if present")
    except Exception as e:
        console.print(f"  [yellow]![/yellow] Schwab init error: {e}")

    # LLM client: optional. Pipeline renders reports without critique if absent.
    llm: LLMClient | None = None
    if config.AUTORESEARCH_LLM_ENABLED:
        try:
            llm = LLMClient(
                provider=config.LLM_PROVIDER,
                model=config.LLM_MODEL or config.OLLAMA_MODEL,
                api_key=config.LLM_API_KEY or config.ANTHROPIC_API_KEY or config.OPENAI_API_KEY,
                base_url=config.LLM_BASE_URL or config.OLLAMA_HOST,
                timeout=config.LLM_TIMEOUT or config.OLLAMA_TIMEOUT,
                temperature=config.LLM_TEMPERATURE,
                max_tokens=config.LLM_MAX_TOKENS,
            )
            if llm.is_available():
                console.print(f"  [green]✓[/green] LLM provider ready ({llm.provider})")
            else:
                console.print(f"  [yellow]![/yellow] LLM provider {llm.provider} not available — reports will skip critique")
                llm = None
        except Exception as e:
            console.print(f"  [yellow]![/yellow] LLM init error: {e}")
            llm = None

    pipeline = AutoResearchPipeline(config=config, client=client, llm=llm)
    console.print(
        f"  Eval window: {pipeline.years}y   "
        f"Data: {pipeline.data_path}   "
        f"Reports: {pipeline.report_dir}"
    )
    console.print()

    try:
        reports = pipeline.run(fetch_force=force_fetch)
    except Exception as e:
        console.print(f"  [red]✗[/red] Pipeline failed: {e}")
        sys.exit(1)

    # Summary table
    table = Table(title="Auto-research results", show_lines=False)
    table.add_column("Strategy", style="cyan")
    table.add_column("Sharpe", justify="right")
    table.add_column("CAGR", justify="right")
    table.add_column("Max DD", justify="right")
    table.add_column("Alpha vs SPY", justify="right")
    table.add_column("Drift", justify="right")
    table.add_column("Status")

    backtestable_sorted = sorted(
        (r for r in reports if r.backtestable and r.sharpe is not None),
        key=lambda r: -r.sharpe,
    )
    others = [r for r in reports if not (r.backtestable and r.sharpe is not None)]

    def fmt_pct(v):
        return f"{v:+.2f}%" if v is not None else "—"

    for r in backtestable_sorted:
        drift = (
            f"{r.sharpe_drift_pct:+.1f}%"
            if r.sharpe_drift_pct is not None
            else "—"
        )
        status = "ok" if not r.errors else f"[red]errors ({len(r.errors)})[/red]"
        if r.drift_flag:
            status = f"[red]drift {drift}[/red]"
        table.add_row(
            r.strategy,
            f"{r.sharpe:.2f}",
            fmt_pct(r.cagr),
            fmt_pct(r.max_drawdown_pct),
            fmt_pct(r.alpha_pct),
            drift,
            status,
        )
    for r in others:
        if r.backtestable:
            status = f"[red]error ({len(r.errors)})[/red]" if r.errors else "[yellow]no data[/yellow]"
        else:
            status = "[dim]unvalidated[/dim]"
        table.add_row(r.strategy, "—", "—", "—", "—", "—", status)

    console.print(table)
    console.print()
    console.print(f"  Reports written to: [cyan]{pipeline.report_dir}[/cyan]")
    console.print(f"  Leaderboard: [cyan]{pipeline.report_dir / 'leaderboard.md'}[/cyan]")
    console.print()

    # Optional Telegram digest — uses a direct HTTPS POST so we don't need to
    # spin up the asyncio-based TelegramBot just for a single message.
    if config.AUTORESEARCH_TELEGRAM_DIGEST and config.TELEGRAM_BOT_TOKEN and config.TELEGRAM_CHAT_ID:
        try:
            _autoresearch_telegram_digest(config, reports)
            console.print("  [green]✓[/green] Telegram digest sent")
        except Exception as e:
            console.print(f"  [yellow]![/yellow] Telegram digest failed: {e}")


def _autoresearch_telegram_digest(config: Config, reports: list) -> None:
    """POST a plain-text autoresearch summary to the configured chat."""
    import requests

    lines = ["Auto-research digest"]
    backtestable = [r for r in reports if r.backtestable and r.sharpe is not None]
    backtestable.sort(key=lambda r: -r.sharpe)
    for r in backtestable:
        flag = " ⚠️" if r.drift_flag else ""
        alpha = f"{r.alpha_pct:+.1f}%" if r.alpha_pct is not None else "n/a"
        lines.append(
            f"{r.strategy}: Sharpe {r.sharpe:.2f} | CAGR {r.cagr:+.1f}% | α {alpha}{flag}"
        )
    unvalidated = [r for r in reports if not r.backtestable]
    if unvalidated:
        lines.append(f"Unvalidated: {', '.join(r.strategy for r in unvalidated)}")

    text = "\n".join(lines)
    chat_id = config.TELEGRAM_CHAT_ID.split(",")[0]
    url = f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/sendMessage"
    resp = requests.post(url, json={"chat_id": int(chat_id), "text": text}, timeout=10)
    resp.raise_for_status()


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
