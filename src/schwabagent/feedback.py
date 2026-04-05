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

    def close(self) -> None:
        self._db.close()
