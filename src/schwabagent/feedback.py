"""ML Feedback Loop — signal recording, outcome tracking, and strategy calibration.

Implements a predict → trade → observe → adjust cycle:

1. RECORD: Every signal from every scan is stored with its features and score.
2. OBSERVE: After trades close, actual P&L is linked back to the original signal.
3. CALIBRATE: Signal accuracy is computed per strategy — how often does a BUY
   signal actually produce a profit? How often does STRONG_BUY beat BUY?
4. DRIFT: Track scoring distributions over time to detect when a strategy's
   signals degrade (e.g., win rate drops below calibrated threshold).
5. SWEEP: Grid search over strategy parameters using historical data.

All data stored in SQLite at ~/.schwab-agent/feedback.db.
"""
from __future__ import annotations

import json
import logging
import sqlite3
import statistics
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from schwabagent.config import Config

logger = logging.getLogger(__name__)

_DB_SCHEMA = """
CREATE TABLE IF NOT EXISTS signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    strategy TEXT NOT NULL,
    symbol TEXT NOT NULL,
    signal TEXT NOT NULL,
    score REAL NOT NULL,
    price REAL,
    reason TEXT,
    features TEXT,
    trade_id TEXT,
    realized_pnl REAL,
    pnl_pct REAL,
    hold_minutes REAL,
    exit_type TEXT,
    resolved_at TEXT
);

CREATE TABLE IF NOT EXISTS calibration (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    strategy TEXT NOT NULL,
    signal_type TEXT NOT NULL,
    total_signals INTEGER NOT NULL,
    traded_count INTEGER NOT NULL,
    win_count INTEGER NOT NULL,
    loss_count INTEGER NOT NULL,
    avg_pnl REAL,
    avg_win REAL,
    avg_loss REAL,
    profit_factor REAL,
    win_rate REAL,
    sample_period_days INTEGER
);

CREATE TABLE IF NOT EXISTS drift (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    strategy TEXT NOT NULL,
    metric TEXT NOT NULL,
    current_value REAL,
    baseline_value REAL,
    deviation_pct REAL,
    alert_level TEXT
);

CREATE TABLE IF NOT EXISTS adjustments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    strategy TEXT NOT NULL,
    action TEXT NOT NULL,
    reason TEXT,
    details TEXT,
    previous_state TEXT,
    new_state TEXT
);

CREATE TABLE IF NOT EXISTS symbol_exclusions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    strategy TEXT NOT NULL,
    symbol TEXT NOT NULL,
    reason TEXT,
    expires_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_signals_strategy ON signals(strategy, ts);
CREATE INDEX IF NOT EXISTS idx_signals_symbol ON signals(symbol, ts);
CREATE INDEX IF NOT EXISTS idx_signals_trade ON signals(trade_id);
CREATE INDEX IF NOT EXISTS idx_calibration_strategy ON calibration(strategy, ts);
CREATE INDEX IF NOT EXISTS idx_drift_strategy ON drift(strategy, ts);
"""


class FeedbackLoop:
    """Records signals, tracks outcomes, and calibrates strategy performance."""

    def __init__(self, config: Config):
        self.config = config
        db_path = Path(config.STATE_DIR).expanduser() / "feedback.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(str(db_path), check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        self._db.executescript(_DB_SCHEMA)
        self._db.execute("PRAGMA journal_mode=WAL")

    # ── 1. RECORD signals ────────────────────────────────────────────────

    def record_signal(
        self,
        strategy: str,
        symbol: str,
        signal: str,
        score: float,
        price: float = 0.0,
        reason: str = "",
        features: dict | None = None,
    ) -> int:
        """Record a signal from a scan. Returns the signal row ID."""
        cur = self._db.execute(
            """INSERT INTO signals (ts, strategy, symbol, signal, score, price, reason, features)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                datetime.now(timezone.utc).isoformat(),
                strategy,
                symbol,
                signal,
                score,
                price,
                reason,
                json.dumps(features) if features else None,
            ),
        )
        self._db.commit()
        return cur.lastrowid

    def record_batch(self, signals: list[dict]) -> list[int]:
        """Record multiple signals from a scan cycle."""
        ids = []
        for s in signals:
            sid = self.record_signal(
                strategy=s.get("strategy", ""),
                symbol=s.get("symbol", ""),
                signal=s.get("signal", "HOLD") if isinstance(s.get("signal"), str) else s.get("signal", {}).value if hasattr(s.get("signal"), "value") else str(s.get("signal", "")),
                score=s.get("score", 0.0),
                price=s.get("price", 0.0),
                reason=s.get("reason", ""),
                features={k: v for k, v in s.items() if k not in ("strategy", "symbol", "signal", "score", "price", "reason") and not k.startswith("_")},
            )
            ids.append(sid)
        return ids

    # ── 2. OBSERVE outcomes ──────────────────────────────────────────────

    def resolve_signal(
        self,
        trade_id: str,
        strategy: str,
        symbol: str,
        realized_pnl: float,
        pnl_pct: float = 0.0,
        hold_minutes: float = 0.0,
        exit_type: str = "",
    ) -> None:
        """Link a trade outcome back to the most recent unresolved signal."""
        # Find the most recent unresolved signal for this strategy+symbol
        row = self._db.execute(
            """SELECT id FROM signals
               WHERE strategy = ? AND symbol = ? AND resolved_at IS NULL
               ORDER BY ts DESC LIMIT 1""",
            (strategy, symbol),
        ).fetchone()

        if row:
            self._db.execute(
                """UPDATE signals SET trade_id = ?, realized_pnl = ?, pnl_pct = ?,
                   hold_minutes = ?, exit_type = ?, resolved_at = ?
                   WHERE id = ?""",
                (
                    trade_id,
                    realized_pnl,
                    pnl_pct,
                    hold_minutes,
                    exit_type,
                    datetime.now(timezone.utc).isoformat(),
                    row["id"],
                ),
            )
            self._db.commit()

    def resolve_from_trade(self, trade: dict) -> None:
        """Convenience: resolve a signal from a trade result dict."""
        if trade.get("side") != "SELL":
            return  # Only resolve on sells (that's when we know P&L)
        self.resolve_signal(
            trade_id=trade.get("order_id", trade.get("trade_id", "")),
            strategy=trade.get("strategy", ""),
            symbol=trade.get("symbol", ""),
            realized_pnl=trade.get("realized_pnl", 0.0),
            pnl_pct=trade.get("pnl_pct", 0.0),
            hold_minutes=trade.get("hold_minutes", 0.0),
            exit_type=trade.get("exit_type", trade.get("reason", "")),
        )

    # ── 3. CALIBRATE strategy performance ────────────────────────────────

    def calibrate(self, strategy: str, days: int = 30) -> dict[str, dict]:
        """Compute signal accuracy for a strategy over the last N days.

        Returns a dict keyed by signal type (BUY, STRONG_BUY, etc.) with
        win rate, avg P&L, profit factor, and sample size.
        """
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()

        rows = self._db.execute(
            """SELECT signal, realized_pnl FROM signals
               WHERE strategy = ? AND ts >= ? AND realized_pnl IS NOT NULL""",
            (strategy, cutoff),
        ).fetchall()

        by_signal: dict[str, list[float]] = defaultdict(list)
        for row in rows:
            by_signal[row["signal"]].append(row["realized_pnl"])

        results = {}
        for signal_type, pnls in by_signal.items():
            wins = [p for p in pnls if p > 0]
            losses = [p for p in pnls if p <= 0]
            gross_win = sum(wins)
            gross_loss = abs(sum(losses))

            cal = {
                "total": len(pnls),
                "wins": len(wins),
                "losses": len(losses),
                "win_rate": len(wins) / len(pnls) * 100 if pnls else 0.0,
                "avg_pnl": statistics.mean(pnls) if pnls else 0.0,
                "avg_win": statistics.mean(wins) if wins else 0.0,
                "avg_loss": statistics.mean(losses) if losses else 0.0,
                "profit_factor": gross_win / gross_loss if gross_loss > 0 else float("inf"),
                "total_pnl": sum(pnls),
            }

            # Store calibration snapshot
            self._db.execute(
                """INSERT INTO calibration
                   (ts, strategy, signal_type, total_signals, traded_count,
                    win_count, loss_count, avg_pnl, avg_win, avg_loss,
                    profit_factor, win_rate, sample_period_days)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    datetime.now(timezone.utc).isoformat(),
                    strategy,
                    signal_type,
                    cal["total"],
                    cal["total"],
                    cal["wins"],
                    cal["losses"],
                    cal["avg_pnl"],
                    cal["avg_win"],
                    cal["avg_loss"],
                    cal["profit_factor"],
                    cal["win_rate"],
                    days,
                ),
            )

            results[signal_type] = cal

        self._db.commit()
        return results

    def calibrate_all(self, days: int = 30) -> dict[str, dict[str, dict]]:
        """Calibrate all strategies. Returns {strategy: {signal: calibration}}."""
        strategies = [
            row["strategy"]
            for row in self._db.execute(
                "SELECT DISTINCT strategy FROM signals"
            ).fetchall()
        ]
        return {s: self.calibrate(s, days) for s in strategies}

    # ── 4. DRIFT detection ───────────────────────────────────────────────

    def check_drift(
        self,
        strategy: str,
        baseline_days: int = 60,
        recent_days: int = 7,
    ) -> list[dict]:
        """Compare recent signal performance to historical baseline.

        Returns a list of drift alerts when metrics deviate significantly.
        """
        baseline_cal = self.calibrate(strategy, days=baseline_days)
        recent_cal = self.calibrate(strategy, days=recent_days)

        alerts = []
        for signal_type in set(baseline_cal) | set(recent_cal):
            base = baseline_cal.get(signal_type, {})
            recent = recent_cal.get(signal_type, {})

            if not base or base.get("total", 0) < 10:
                continue  # Not enough baseline data

            # Check win rate drift
            base_wr = base.get("win_rate", 0)
            recent_wr = recent.get("win_rate", 0)
            if base_wr > 0:
                wr_deviation = (recent_wr - base_wr) / base_wr * 100
                if abs(wr_deviation) > 20:  # >20% relative change
                    alert = {
                        "strategy": strategy,
                        "signal": signal_type,
                        "metric": "win_rate",
                        "baseline": round(base_wr, 1),
                        "current": round(recent_wr, 1),
                        "deviation_pct": round(wr_deviation, 1),
                        "level": "warning" if abs(wr_deviation) < 40 else "critical",
                    }
                    alerts.append(alert)
                    self._record_drift(strategy, "win_rate", recent_wr, base_wr, wr_deviation, alert["level"])

            # Check profit factor drift
            base_pf = base.get("profit_factor", 0)
            recent_pf = recent.get("profit_factor", 0)
            if base_pf > 0 and base_pf != float("inf"):
                pf_deviation = (recent_pf - base_pf) / base_pf * 100
                if recent_pf < 1.0 and base_pf >= 1.0:
                    alert = {
                        "strategy": strategy,
                        "signal": signal_type,
                        "metric": "profit_factor",
                        "baseline": round(base_pf, 2),
                        "current": round(recent_pf, 2),
                        "deviation_pct": round(pf_deviation, 1),
                        "level": "critical",
                    }
                    alerts.append(alert)
                    self._record_drift(strategy, "profit_factor", recent_pf, base_pf, pf_deviation, "critical")

        return alerts

    def _record_drift(
        self, strategy: str, metric: str, current: float, baseline: float,
        deviation: float, level: str,
    ) -> None:
        self._db.execute(
            """INSERT INTO drift (ts, strategy, metric, current_value, baseline_value,
               deviation_pct, alert_level) VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (datetime.now(timezone.utc).isoformat(), strategy, metric, current, baseline, deviation, level),
        )
        self._db.commit()

    # ── 5. QUERY helpers ─────────────────────────────────────────────────

    def get_signal_history(
        self,
        strategy: str | None = None,
        symbol: str | None = None,
        days: int = 30,
        limit: int = 200,
    ) -> list[dict]:
        """Query signal history with optional filters."""
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        conditions = ["ts >= ?"]
        params: list[Any] = [cutoff]

        if strategy:
            conditions.append("strategy = ?")
            params.append(strategy)
        if symbol:
            conditions.append("symbol = ?")
            params.append(symbol)

        where = " AND ".join(conditions)
        rows = self._db.execute(
            f"SELECT * FROM signals WHERE {where} ORDER BY ts DESC LIMIT ?",
            params + [limit],
        ).fetchall()
        return [dict(row) for row in rows]

    def get_strategy_summary(self, days: int = 30) -> dict[str, dict]:
        """High-level summary per strategy: signal counts, resolution rate, P&L."""
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        rows = self._db.execute(
            """SELECT strategy,
                      COUNT(*) as total_signals,
                      SUM(CASE WHEN realized_pnl IS NOT NULL THEN 1 ELSE 0 END) as resolved,
                      SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END) as wins,
                      SUM(CASE WHEN realized_pnl <= 0 AND realized_pnl IS NOT NULL THEN 1 ELSE 0 END) as losses,
                      AVG(realized_pnl) as avg_pnl,
                      SUM(realized_pnl) as total_pnl
               FROM signals WHERE ts >= ?
               GROUP BY strategy""",
            (cutoff,),
        ).fetchall()
        return {
            row["strategy"]: dict(row)
            for row in rows
        }

    def get_drift_alerts(self, days: int = 7) -> list[dict]:
        """Get recent drift alerts."""
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        rows = self._db.execute(
            "SELECT * FROM drift WHERE ts >= ? ORDER BY ts DESC",
            (cutoff,),
        ).fetchall()
        return [dict(row) for row in rows]

    # ── Cleanup ──────────────────────────────────────────────────────────

    def cleanup(self, retention_days: int = 90) -> int:
        """Delete signals older than retention_days. Returns count deleted."""
        cutoff = (datetime.now(timezone.utc) - timedelta(days=retention_days)).isoformat()
        cur = self._db.execute("DELETE FROM signals WHERE ts < ?", (cutoff,))
        self._db.execute("DELETE FROM calibration WHERE ts < ?", (cutoff,))
        self._db.execute("DELETE FROM drift WHERE ts < ?", (cutoff,))
        self._db.commit()
        deleted = cur.rowcount
        if deleted:
            logger.info("Feedback cleanup: deleted %d records older than %d days", deleted, retention_days)
        return deleted

    # ── Adjustment logging ────────────────────────────────────────────────

    def record_adjustment(
        self, strategy: str, action: str, reason: str,
        details: dict | None = None, previous_state: str = "", new_state: str = "",
    ) -> None:
        self._db.execute(
            """INSERT INTO adjustments (ts, strategy, action, reason, details, previous_state, new_state)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (datetime.now(timezone.utc).isoformat(), strategy, action, reason,
             json.dumps(details) if details else None, previous_state, new_state),
        )
        self._db.commit()

    def get_adjustments(self, days: int = 30) -> list[dict]:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        rows = self._db.execute(
            "SELECT * FROM adjustments WHERE ts >= ? ORDER BY ts DESC", (cutoff,),
        ).fetchall()
        return [dict(row) for row in rows]

    def exclude_symbol(self, strategy: str, symbol: str, reason: str, days: int = 7) -> None:
        """Temporarily exclude a symbol from a strategy."""
        expires = (datetime.now(timezone.utc) + timedelta(days=days)).isoformat()
        self._db.execute(
            """INSERT INTO symbol_exclusions (ts, strategy, symbol, reason, expires_at)
               VALUES (?, ?, ?, ?, ?)""",
            (datetime.now(timezone.utc).isoformat(), strategy, symbol, reason, expires),
        )
        self._db.commit()

    def get_excluded_symbols(self, strategy: str) -> set[str]:
        """Return symbols currently excluded for a strategy."""
        now = datetime.now(timezone.utc).isoformat()
        rows = self._db.execute(
            "SELECT DISTINCT symbol FROM symbol_exclusions WHERE strategy = ? AND expires_at > ?",
            (strategy, now),
        ).fetchall()
        return {row["symbol"] for row in rows}

    def get_symbol_streak(self, strategy: str, symbol: str, days: int = 30) -> int:
        """Return the current consecutive loss streak for a symbol in a strategy.
        Returns positive int for losses, negative for wins."""
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        rows = self._db.execute(
            """SELECT realized_pnl FROM signals
               WHERE strategy = ? AND symbol = ? AND ts >= ? AND realized_pnl IS NOT NULL
               ORDER BY ts DESC""",
            (strategy, symbol, cutoff),
        ).fetchall()
        if not rows:
            return 0
        streak = 0
        first_sign = rows[0]["realized_pnl"] <= 0  # True = loss
        for row in rows:
            is_loss = row["realized_pnl"] <= 0
            if is_loss == first_sign:
                streak += 1
            else:
                break
        return streak if first_sign else -streak

    def close(self) -> None:
        self._db.close()


# ══════════════════════════════════════════════════════════════════════════════
# AUTO-TUNER — closes the feedback loop by adjusting strategy behavior
# ══════════════════════════════════════════════════════════════════════════════


class StrategyState:
    """Runtime state for a single strategy managed by the auto-tuner."""
    NORMAL = "normal"
    THROTTLED = "throttled"
    PAUSED = "paused"

    def __init__(self, name: str):
        self.name = name
        self.state: str = self.NORMAL
        self.sizing_factor: float = 1.0  # multiplier on position size
        self.paused_at: str = ""
        self.throttled_at: str = ""
        self.last_eval: str = ""
        self.excluded_symbols: set[str] = set()

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "state": self.state,
            "sizing_factor": self.sizing_factor,
            "paused_at": self.paused_at,
            "throttled_at": self.throttled_at,
            "excluded_symbols": sorted(self.excluded_symbols),
        }


class AutoTuner:
    """Self-improvement loop that adjusts strategy behavior based on performance.

    Runs after each execution cycle and:
    1. Evaluates each strategy's recent performance
    2. Throttles (reduce sizing) strategies that are underperforming
    3. Pauses strategies that are critically underperforming
    4. Restores strategies that recover
    5. Excludes symbols on consecutive loss streaks
    6. Logs all adjustments to the feedback database
    """

    def __init__(self, config: Config, feedback: FeedbackLoop, telegram=None):
        self.config = config
        self.feedback = feedback
        self.telegram = telegram
        self._states: dict[str, StrategyState] = {}
        self._load_state()

    def _load_state(self) -> None:
        """Load persisted auto-tuner state from feedback DB."""
        # Restore from most recent adjustments
        for adj in self.feedback.get_adjustments(days=self.config.AUTOTUNE_EVAL_WINDOW_DAYS):
            strategy = adj["strategy"]
            if strategy not in self._states:
                self._states[strategy] = StrategyState(strategy)
            s = self._states[strategy]
            if adj["action"] == "pause" and s.state == StrategyState.NORMAL:
                s.state = StrategyState.PAUSED
                s.paused_at = adj["ts"]
            elif adj["action"] == "throttle" and s.state == StrategyState.NORMAL:
                s.state = StrategyState.THROTTLED
                s.sizing_factor = self.config.AUTOTUNE_THROTTLE_FACTOR
                s.throttled_at = adj["ts"]
            elif adj["action"] in ("restore", "unpause"):
                s.state = StrategyState.NORMAL
                s.sizing_factor = 1.0

    def get_state(self, strategy: str) -> StrategyState:
        if strategy not in self._states:
            self._states[strategy] = StrategyState(strategy)
        return self._states[strategy]

    def evaluate(self) -> list[dict]:
        """Run the auto-tune evaluation cycle. Returns list of actions taken."""
        if not self.config.AUTOTUNE_ENABLED:
            return []

        actions = []
        window = self.config.AUTOTUNE_EVAL_WINDOW_DAYS

        summary = self.feedback.get_strategy_summary(days=window)

        for strategy, data in summary.items():
            resolved = data.get("resolved") or 0
            if resolved < self.config.AUTOTUNE_MIN_TRADES:
                continue

            state = self.get_state(strategy)
            wins = data.get("wins") or 0
            losses = data.get("losses") or 0
            win_rate = wins / resolved * 100 if resolved > 0 else 0
            total_pnl = data.get("total_pnl") or 0
            avg_pnl = data.get("avg_pnl") or 0

            # Compute profit factor from calibration
            cal = self.feedback.calibrate(strategy, days=window)
            all_wins = sum(c.get("avg_win", 0) * c.get("wins", 0) for c in cal.values())
            all_losses = sum(abs(c.get("avg_loss", 0)) * c.get("losses", 0) for c in cal.values())
            profit_factor = all_wins / all_losses if all_losses > 0 else float("inf")

            action = self._evaluate_strategy(state, win_rate, profit_factor, resolved, strategy)
            if action:
                actions.append(action)

            # Check per-symbol streaks
            symbol_actions = self._check_symbol_streaks(strategy, window)
            actions.extend(symbol_actions)

            state.last_eval = datetime.now(timezone.utc).isoformat()

        return actions

    def _evaluate_strategy(
        self, state: StrategyState, win_rate: float, profit_factor: float,
        resolved: int, strategy: str,
    ) -> dict | None:
        """Evaluate a single strategy and return an action dict, or None."""
        now = datetime.now(timezone.utc)

        # ── Currently PAUSED — check for recovery ────────────────────
        if state.state == StrategyState.PAUSED:
            if win_rate >= self.config.AUTOTUNE_WIN_RATE_WARN and profit_factor >= self.config.AUTOTUNE_PROFIT_FACTOR_WARN:
                # Check if sustained recovery
                if state.paused_at:
                    try:
                        paused_dt = datetime.fromisoformat(state.paused_at.replace("Z", "+00:00"))
                        days_paused = (now - paused_dt).days
                    except ValueError:
                        days_paused = 0
                    if days_paused < self.config.AUTOTUNE_RECOVERY_WINDOW_DAYS:
                        return None  # Not enough time to confirm recovery

                return self._restore(state, strategy,
                    f"Recovered: wr={win_rate:.1f}% pf={profit_factor:.2f} "
                    f"(above thresholds after pause)")
            return None

        # ── Currently THROTTLED — check for recovery or further degradation
        if state.state == StrategyState.THROTTLED:
            if win_rate >= self.config.AUTOTUNE_WIN_RATE_WARN and profit_factor >= self.config.AUTOTUNE_PROFIT_FACTOR_WARN:
                return self._restore(state, strategy,
                    f"Recovered: wr={win_rate:.1f}% pf={profit_factor:.2f}")
            if win_rate < self.config.AUTOTUNE_WIN_RATE_PAUSE or profit_factor < self.config.AUTOTUNE_PROFIT_FACTOR_PAUSE:
                return self._pause(state, strategy,
                    f"Degraded further while throttled: wr={win_rate:.1f}% pf={profit_factor:.2f}")
            return None

        # ── NORMAL — check for degradation ───────────────────────────
        if win_rate < self.config.AUTOTUNE_WIN_RATE_PAUSE or profit_factor < self.config.AUTOTUNE_PROFIT_FACTOR_PAUSE:
            return self._pause(state, strategy,
                f"Critical underperformance: wr={win_rate:.1f}% pf={profit_factor:.2f} "
                f"(n={resolved})")

        if win_rate < self.config.AUTOTUNE_WIN_RATE_WARN or profit_factor < self.config.AUTOTUNE_PROFIT_FACTOR_WARN:
            return self._throttle(state, strategy,
                f"Underperforming: wr={win_rate:.1f}% pf={profit_factor:.2f} "
                f"(n={resolved})")

        return None

    def _throttle(self, state: StrategyState, strategy: str, reason: str) -> dict:
        prev = state.state
        state.state = StrategyState.THROTTLED
        state.sizing_factor = self.config.AUTOTUNE_THROTTLE_FACTOR
        state.throttled_at = datetime.now(timezone.utc).isoformat()

        self.feedback.record_adjustment(
            strategy, "throttle", reason,
            details={"sizing_factor": state.sizing_factor},
            previous_state=prev, new_state=state.state,
        )
        logger.warning("[autotune] THROTTLE %s: %s (sizing=%.0f%%)", strategy, reason, state.sizing_factor * 100)

        if self.telegram:
            from schwabagent.telegram import _escape_md
            self.telegram.send_alert(
                f"*Auto\\-tune: THROTTLE {_escape_md(strategy)}*\n"
                f"{_escape_md(reason)}\n"
                f"Position sizing reduced to {state.sizing_factor:.0%}"
            )

        return {"strategy": strategy, "action": "throttle", "reason": reason, "sizing_factor": state.sizing_factor}

    def _pause(self, state: StrategyState, strategy: str, reason: str) -> dict:
        prev = state.state
        state.state = StrategyState.PAUSED
        state.sizing_factor = 0.0
        state.paused_at = datetime.now(timezone.utc).isoformat()

        self.feedback.record_adjustment(
            strategy, "pause", reason,
            previous_state=prev, new_state=state.state,
        )
        logger.warning("[autotune] PAUSE %s: %s", strategy, reason)

        if self.telegram:
            from schwabagent.telegram import _escape_md
            self.telegram.send_alert(
                f"*Auto\\-tune: PAUSED {_escape_md(strategy)}*\n"
                f"{_escape_md(reason)}\n"
                f"Strategy will not trade until performance recovers\\."
            )

        return {"strategy": strategy, "action": "pause", "reason": reason}

    def _restore(self, state: StrategyState, strategy: str, reason: str) -> dict:
        prev = state.state
        state.state = StrategyState.NORMAL
        state.sizing_factor = 1.0
        state.paused_at = ""
        state.throttled_at = ""

        self.feedback.record_adjustment(
            strategy, "restore", reason,
            previous_state=prev, new_state=state.state,
        )
        logger.info("[autotune] RESTORE %s: %s", strategy, reason)

        if self.telegram:
            from schwabagent.telegram import _escape_md
            self.telegram.send_alert(
                f"*Auto\\-tune: RESTORED {_escape_md(strategy)}*\n"
                f"{_escape_md(reason)}"
            )

        return {"strategy": strategy, "action": "restore", "reason": reason}

    def _check_symbol_streaks(self, strategy: str, window_days: int) -> list[dict]:
        """Check for consecutive loss streaks per symbol and auto-exclude."""
        actions = []
        max_streak = self.config.AUTOTUNE_SYMBOL_MAX_LOSS_STREAK

        # Get all symbols with resolved trades in the window
        cutoff = (datetime.now(timezone.utc) - timedelta(days=window_days)).isoformat()
        rows = self.feedback._db.execute(
            """SELECT DISTINCT symbol FROM signals
               WHERE strategy = ? AND ts >= ? AND realized_pnl IS NOT NULL""",
            (strategy, cutoff),
        ).fetchall()

        already_excluded = self.feedback.get_excluded_symbols(strategy)

        for row in rows:
            symbol = row["symbol"]
            if symbol in already_excluded:
                continue

            streak = self.feedback.get_symbol_streak(strategy, symbol, days=window_days)
            if streak >= max_streak:
                reason = f"{streak} consecutive losses in {strategy}"
                self.feedback.exclude_symbol(strategy, symbol, reason, days=7)
                self.feedback.record_adjustment(
                    strategy, "exclude_symbol", reason,
                    details={"symbol": symbol, "streak": streak, "exclusion_days": 7},
                )
                logger.warning("[autotune] EXCLUDE %s from %s: %s", symbol, strategy, reason)

                if self.telegram:
                    from schwabagent.telegram import _escape_md
                    self.telegram.send_alert(
                        f"*Auto\\-tune: excluded {_escape_md(symbol)} from {_escape_md(strategy)}*\n"
                        f"{_escape_md(reason)}\\. Excluded for 7 days\\."
                    )

                state = self.get_state(strategy)
                state.excluded_symbols.add(symbol)
                actions.append({"strategy": strategy, "action": "exclude_symbol", "symbol": symbol, "reason": reason})

        return actions

    def status(self) -> dict:
        """Return auto-tuner state for display."""
        return {
            name: s.to_dict() for name, s in self._states.items()
        }
