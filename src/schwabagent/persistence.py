"""JSON-based state persistence for the schwagent."""
from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

DEFAULT_STATE_DIR = Path.home() / ".schwagent"


class StateStore:
    """Persists risk state, trade history, and per-strategy P&L to disk."""

    def __init__(self, state_dir: str | Path | None = None):
        self.state_dir = Path(state_dir).expanduser() if state_dir else DEFAULT_STATE_DIR
        self.state_dir.mkdir(parents=True, exist_ok=True)

        self._risk_path = self.state_dir / "risk_state.json"
        self._history_path = self.state_dir / "trade_history.jsonl"
        self._strategy_pnl_path = self.state_dir / "strategy_pnl.json"
        self._audit_path = self.state_dir / "audit.jsonl"

    # ── Risk state ────────────────────────────────────────────────────────────

    def load_risk_state(self) -> dict:
        """Load persisted risk state. Returns empty dict if no state exists."""
        data = self._read_json(self._risk_path)
        return data or {}

    def save_risk_state(self, state: dict) -> None:
        """Overwrite risk state file atomically."""
        self._write_json(self._risk_path, state)

    # ── Trade history ─────────────────────────────────────────────────────────

    def append_trade(self, trade: dict) -> None:
        """Append a completed trade record."""
        if "timestamp" not in trade:
            trade["timestamp"] = datetime.now(timezone.utc).isoformat()
        self._append_jsonl(self._history_path, trade)

    def get_trade_history(self, limit: int = 100) -> list[dict]:
        """Return the most recent *limit* trade records."""
        if not self._history_path.exists():
            return []
        trades = []
        try:
            for line in self._history_path.read_text().strip().split("\n"):
                line = line.strip()
                if line:
                    try:
                        trades.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
        except OSError as e:
            logger.warning("Failed to read trade history: %s", e)
        return trades[-limit:]

    # ── Strategy P&L ──────────────────────────────────────────────────────────

    def update_strategy_pnl(self, strategy: str, pnl: float, win: bool) -> None:
        """Increment per-strategy cumulative P&L counters."""
        data = self._read_json(self._strategy_pnl_path) or {}
        rec = data.setdefault(strategy, {"trades": 0, "wins": 0, "losses": 0, "realized_pnl": 0.0})
        rec["trades"] += 1
        rec["realized_pnl"] = round(rec["realized_pnl"] + pnl, 4)
        if win:
            rec["wins"] += 1
        else:
            rec["losses"] += 1
        self._write_json(self._strategy_pnl_path, data)

    def get_strategy_pnl(self) -> dict:
        """Return per-strategy P&L dict."""
        return self._read_json(self._strategy_pnl_path) or {}

    # ── Audit log ─────────────────────────────────────────────────────────────

    def audit(self, action: str, data: dict) -> None:
        """Append an audit record."""
        record = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "action": action,
            **data,
        }
        self._append_jsonl(self._audit_path, record)

    def get_audit_log(self, limit: int = 100) -> list[dict]:
        """Return recent audit records."""
        if not self._audit_path.exists():
            return []
        entries = []
        try:
            for line in self._audit_path.read_text().strip().split("\n"):
                line = line.strip()
                if line:
                    try:
                        entries.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
        except OSError:
            pass
        return entries[-limit:]

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _write_json(self, path: Path, data: Any) -> None:
        """Write JSON atomically via temp file."""
        tmp = path.with_suffix(".tmp")
        try:
            tmp.write_text(json.dumps(data, indent=2, default=str))
            tmp.rename(path)
        except OSError as e:
            logger.warning("Failed to write %s: %s", path, e)

    def _read_json(self, path: Path) -> dict | None:
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text())
        except (json.JSONDecodeError, OSError) as e:
            logger.warning("Failed to read %s: %s", path, e)
            return None

    def _append_jsonl(self, path: Path, record: dict) -> None:
        try:
            with open(path, "a") as f:
                f.write(json.dumps(record, default=str) + "\n")
        except OSError as e:
            logger.warning("Failed to append to %s: %s", path, e)
