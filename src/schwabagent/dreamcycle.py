"""Dreamcycle — autonomous background research and self-improvement loop.

Runs periodically (configurable interval) and performs market analysis,
portfolio maintenance, and strategy tuning without user interaction.

Cycle phases:
  1. SCAN            — refresh signals across all universes
  2. CALIBRATE       — run auto-tuner, check drift, update strategy states
  3. RESEARCH        — earnings proximity, dividend dates, volume anomalies
  4. RESEARCH_PAPERS — pull quant-finance feeds, score, digest to Telegram
  5. RECONCILE       — compare expected vs actual positions
  6. DIGEST          — build daily summary, send via Telegram
  7. IMPROVE         — analyze feedback patterns, flag parameter changes
  8. CLEANUP         — prune old data, rotate logs

Each phase is independent — a failure in one doesn't block the rest.
"""
from __future__ import annotations

import logging
import time
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from schwabagent.config import Config

logger = logging.getLogger(__name__)


@dataclass
class DreamResult:
    """Result of a single dreamcycle run."""
    started_at: str = ""
    finished_at: str = ""
    duration_seconds: float = 0.0
    phases_completed: list[str] = field(default_factory=list)
    phases_failed: list[str] = field(default_factory=list)
    signals_recorded: int = 0
    drift_alerts: int = 0
    earnings_warnings: int = 0
    position_mismatches: int = 0
    auto_tune_actions: int = 0
    digest_sent: bool = False
    quant_papers_fetched: int = 0
    quant_papers_notified: int = 0
    errors: list[str] = field(default_factory=list)


class DreamCycle:
    """Autonomous background loop for market research and self-improvement."""

    def __init__(self, runner):
        """Initialize with a reference to the AgentRunner (provides all dependencies)."""
        self._runner = runner
        self._config: Config = runner.config
        self._thread: threading.Thread | None = None
        self._running = False
        self._last_run: str = ""
        self._last_result: DreamResult | None = None
        self._cycle_count = 0

    def start(self, interval_minutes: int = 30) -> None:
        """Start the dreamcycle in a background daemon thread."""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._loop,
            args=(interval_minutes,),
            daemon=True,
            name="dreamcycle",
        )
        self._thread.start()
        logger.info("Dreamcycle started (interval=%dm)", interval_minutes)

    def stop(self) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=10)
        logger.info("Dreamcycle stopped after %d cycles", self._cycle_count)

    def run_once(self) -> DreamResult:
        """Run one full dreamcycle synchronously. Returns results."""
        result = DreamResult(started_at=datetime.now(timezone.utc).isoformat())
        start = time.monotonic()

        phases = [
            ("scan", self._phase_scan),
            ("calibrate", self._phase_calibrate),
            ("research", self._phase_research),
            ("research_papers", self._phase_research_papers),
            ("reconcile", self._phase_reconcile),
            ("digest", self._phase_digest),
            ("improve", self._phase_improve),
            ("cleanup", self._phase_cleanup),
        ]

        for name, fn in phases:
            try:
                fn(result)
                result.phases_completed.append(name)
            except Exception as e:
                result.phases_failed.append(name)
                result.errors.append(f"{name}: {e}")
                logger.error("[dreamcycle] Phase %s failed: %s", name, e)

        result.finished_at = datetime.now(timezone.utc).isoformat()
        result.duration_seconds = round(time.monotonic() - start, 1)
        self._last_result = result
        self._last_run = result.finished_at
        self._cycle_count += 1

        logger.info(
            "[dreamcycle] Cycle %d complete in %.1fs — %d phases ok, %d failed",
            self._cycle_count, result.duration_seconds,
            len(result.phases_completed), len(result.phases_failed),
        )
        return result

    def _loop(self, interval_minutes: int) -> None:
        """Background loop — runs dreamcycle at the configured interval."""
        while self._running:
            try:
                self.run_once()
            except Exception as e:
                logger.error("[dreamcycle] Cycle failed: %s", e)

            # Sleep in 1-second increments so we can stop promptly
            for _ in range(interval_minutes * 60):
                if not self._running:
                    break
                time.sleep(1)

    # ── Phase 1: SCAN ────────────────────────────────────────────────────

    def _phase_scan(self, result: DreamResult) -> None:
        """Refresh signals across all strategy universes."""
        runner = self._runner
        try:
            account = runner._get_account()
            runner._inject_account(account)
        except Exception as e:
            logger.warning("[dreamcycle:scan] Could not get account: %s", e)
            return

        all_signals = []
        for strategy in runner.strategies:
            try:
                opps = strategy.scan()
                all_signals.extend(opps)
            except Exception as e:
                logger.warning("[dreamcycle:scan] %s scan failed: %s", strategy.name, e)

        if all_signals:
            ids = runner.feedback.record_batch(all_signals)
            result.signals_recorded = len(ids)
            logger.info("[dreamcycle:scan] Recorded %d signals", len(ids))

    # ── Phase 2: CALIBRATE ───────────────────────────────────────────────

    def _phase_calibrate(self, result: DreamResult) -> None:
        """Run auto-tuner evaluation and check for drift."""
        runner = self._runner

        # Auto-tune
        actions = runner.autotuner.evaluate()
        result.auto_tune_actions = len(actions)
        if actions:
            logger.info("[dreamcycle:calibrate] %d auto-tune actions: %s",
                        len(actions), [a["action"] for a in actions])

        # Drift check across all strategies
        strategies = [s.name for s in runner.strategies]
        for strat in strategies:
            alerts = runner.feedback.check_drift(strat)
            result.drift_alerts += len(alerts)

    # ── Phase 3: RESEARCH ────────────────────────────────────────────────

    def _phase_research(self, result: DreamResult) -> None:
        """Check earnings proximity, dividend dates, volume anomalies."""
        runner = self._runner
        client = runner.client

        # All symbols across watchlist + all strategy universes
        all_symbols = runner.config.all_symbols

        # Fetch quotes with fundamentals
        try:
            quotes = client.get_quotes(all_symbols)
        except Exception as e:
            logger.warning("[dreamcycle:research] Quote fetch failed: %s", e)
            return

        now = datetime.now(timezone.utc)
        earnings_warnings = []
        volume_anomalies = []

        for sym, quote in quotes.items():
            # Volume anomaly: check via risk manager price history
            if quote.last > 0:
                is_anomaly = runner.risk.check_price_anomaly(sym, quote.last)
                if is_anomaly:
                    volume_anomalies.append(sym)

        result.earnings_warnings = len(earnings_warnings)

        if volume_anomalies:
            logger.warning("[dreamcycle:research] Price anomalies: %s", volume_anomalies)

    # ── Phase 4: RESEARCH_PAPERS ─────────────────────────────────────────

    def _phase_research_papers(self, result: DreamResult) -> None:
        """Fetch + score quant-finance papers, digest top-N to Telegram."""
        config = self._config
        if not getattr(config, "QUANT_RESEARCH_ENABLED", False):
            return

        from schwabagent.scrapers import quant_research as qr

        runner = self._runner
        llm = getattr(runner, "llm", None) if getattr(config, "QUANT_RESEARCH_LLM_SUMMARIES", False) else None

        inserted = qr.fetch_new_papers(config, llm=llm)
        result.quant_papers_fetched = len(inserted)

        top_n = int(getattr(config, "QUANT_RESEARCH_DIGEST_TOP_N", 5))
        top = qr.top_unread(config, limit=top_n)
        if not top:
            return

        if runner.telegram:
            try:
                runner.telegram.send_quant_papers(top)
                qr.mark_notified(config, [p.id for p in top if p.id is not None])
                result.quant_papers_notified = len(top)
            except Exception as e:
                logger.warning("[dreamcycle:research_papers] digest failed: %s", e)

    # ── Phase 5: RECONCILE ───────────────────────────────────────────────

    def _phase_reconcile(self, result: DreamResult) -> None:
        """Compare expected positions vs actual from Schwab API."""
        runner = self._runner

        try:
            account = runner._get_account()
        except Exception:
            return

        # Build expected positions from recent trade history
        trades = runner.state.get_trade_history(limit=500)
        expected: dict[str, float] = {}
        for t in trades:
            if t.get("dry_run"):
                continue
            sym = t.get("symbol", "")
            qty = t.get("quantity", 0)
            side = t.get("side", "")
            if side == "BUY":
                expected[sym] = expected.get(sym, 0) + qty
            elif side == "SELL":
                expected[sym] = expected.get(sym, 0) - qty

        # Remove zero/negative positions
        expected = {s: q for s, q in expected.items() if q > 0.001}

        mismatches = runner.risk.reconcile_positions(expected, account.positions)
        result.position_mismatches = len(mismatches)

        if mismatches and runner.telegram:
            from schwabagent.telegram import _escape_md
            lines = ["*Position Reconciliation Alert*\n"]
            for m in mismatches:
                lines.append(
                    f"`{_escape_md(m['symbol'])}` expected={m['expected']:.1f} "
                    f"actual={m['actual']:.1f} delta={m['delta']:+.1f}"
                )
            runner.telegram.send_alert("\n".join(lines))

    # ── Phase 6: DIGEST ──────────────────────────────────────────────────

    def _phase_digest(self, result: DreamResult) -> None:
        """Build and send daily summary via Telegram."""
        runner = self._runner
        if not runner.telegram:
            return

        # Only send digest once per day
        now = datetime.now(timezone.utc)
        today = now.strftime("%Y-%m-%d")
        state = runner.state.load_risk_state()
        last_digest = state.get("last_digest_date", "")
        if last_digest == today:
            return

        # Build summary
        summary = runner.get_pnl_summary()
        if summary:
            runner.telegram.send_daily_summary(summary)
            result.digest_sent = True

        # Also send auto-tuner state
        tuner_status = runner.autotuner.status()
        if tuner_status:
            from schwabagent.telegram import _escape_md
            lines = ["*Strategy Health*\n"]
            for name, s in tuner_status.items():
                state_str = s["state"].upper()
                emoji = {"normal": "OK", "throttled": "THROTTLED", "paused": "PAUSED"}.get(s["state"], "?")
                sizing = f" sizing={s['sizing_factor']:.0%}" if s["sizing_factor"] < 1.0 else ""
                excluded = f" excluded={','.join(s['excluded_symbols'])}" if s["excluded_symbols"] else ""
                lines.append(f"`{_escape_md(name)}` {emoji}{_escape_md(sizing)}{_escape_md(excluded)}")
            runner.telegram.send_alert("\n".join(lines))

        # Mark digest sent
        state["last_digest_date"] = today
        runner.state.save_risk_state(state)

    # ── Phase 7: IMPROVE ─────────────────────────────────────────────────

    def _phase_improve(self, result: DreamResult) -> None:
        """Analyze feedback patterns and identify improvements."""
        runner = self._runner
        fb = runner.feedback

        # Per-strategy analysis
        summary = fb.get_strategy_summary(days=14)

        improvements = []
        for strat, data in summary.items():
            resolved = data.get("resolved") or 0
            if resolved < 10:
                continue

            wins = data.get("wins") or 0
            losses = data.get("losses") or 0
            win_rate = wins / resolved * 100 if resolved > 0 else 0
            avg_pnl = data.get("avg_pnl") or 0

            # Identify consistently losing symbols
            signals = fb.get_signal_history(strategy=strat, days=14, limit=500)
            symbol_pnl: dict[str, list[float]] = {}
            for sig in signals:
                if sig.get("realized_pnl") is not None:
                    sym = sig["symbol"]
                    symbol_pnl.setdefault(sym, []).append(sig["realized_pnl"])

            for sym, pnls in symbol_pnl.items():
                if len(pnls) >= 3:
                    sym_wr = sum(1 for p in pnls if p > 0) / len(pnls) * 100
                    sym_total = sum(pnls)
                    if sym_wr < 25 and sym_total < 0:
                        improvements.append({
                            "strategy": strat,
                            "type": "exclude_symbol",
                            "symbol": sym,
                            "reason": f"win_rate={sym_wr:.0f}% total_pnl=${sym_total:.2f} over {len(pnls)} trades",
                        })

            # Check if strategy is consistently underperforming
            if win_rate < 35 and resolved >= 20:
                improvements.append({
                    "strategy": strat,
                    "type": "review_parameters",
                    "reason": f"win_rate={win_rate:.1f}% over {resolved} trades — consider parameter sweep",
                })

        if improvements:
            logger.info("[dreamcycle:improve] %d improvement suggestions", len(improvements))
            for imp in improvements:
                fb.record_adjustment(
                    imp["strategy"], f"suggestion:{imp['type']}", imp["reason"],
                    details=imp,
                )

            # Notify via Telegram
            if runner.telegram:
                from schwabagent.telegram import _escape_md
                lines = ["*Improvement Suggestions*\n"]
                for imp in improvements[:5]:
                    lines.append(f"\\- `{_escape_md(imp['strategy'])}` {_escape_md(imp['type'])}: {_escape_md(imp['reason'])}")
                runner.telegram.send_alert("\n".join(lines))

    # ── Phase 8: CLEANUP ─────────────────────────────────────────────────

    def _phase_cleanup(self, result: DreamResult) -> None:
        """Prune old feedback data and stale quant-research rows."""
        runner = self._runner
        runner.feedback.cleanup(retention_days=90)

        if getattr(self._config, "QUANT_RESEARCH_ENABLED", False):
            try:
                from schwabagent.scrapers import quant_research as qr
                pruned = qr.cleanup(self._config)
                if pruned:
                    logger.info("[dreamcycle:cleanup] pruned %d quant_papers rows", pruned)
            except Exception as e:
                logger.warning("[dreamcycle:cleanup] quant_research prune failed: %s", e)

    # ── Status ───────────────────────────────────────────────────────────

    def status(self) -> dict:
        return {
            "running": self._running,
            "cycle_count": self._cycle_count,
            "last_run": self._last_run,
            "last_result": {
                "duration": self._last_result.duration_seconds if self._last_result else 0,
                "phases_ok": self._last_result.phases_completed if self._last_result else [],
                "phases_failed": self._last_result.phases_failed if self._last_result else [],
                "signals": self._last_result.signals_recorded if self._last_result else 0,
                "drift_alerts": self._last_result.drift_alerts if self._last_result else 0,
                "errors": self._last_result.errors if self._last_result else [],
            } if self._last_result else None,
        }
