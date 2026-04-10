"""FastAPI web dashboard for the Schwab trading agent."""
from __future__ import annotations

import asyncio
import json
import logging
import threading
from contextlib import asynccontextmanager
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from schwabagent.config import Config
from schwabagent.persistence import StateStore
from schwabagent.schwab_client import SchwabClient

logger = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).parent / "static"


# ── WebSocket manager ────────────────────────────────────────────────────────

class ConnectionManager:
    def __init__(self):
        self._connections: list[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self._connections.append(ws)

    def disconnect(self, ws: WebSocket):
        self._connections.remove(ws)

    async def broadcast(self, data: dict):
        for ws in list(self._connections):
            try:
                await ws.send_json(data)
            except Exception:
                try:
                    self._connections.remove(ws)
                except ValueError:
                    pass


manager = ConnectionManager()


# ── App factory ──────────────────────────────────────────────────────────────

def create_app(config: Config | None = None) -> FastAPI:
    if config is None:
        config = Config()

    state = StateStore(config.STATE_DIR)

    # Lazy client — only authenticated when needed
    _client_lock = threading.Lock()
    _client: dict = {"instance": None}

    def get_client() -> SchwabClient:
        with _client_lock:
            if _client["instance"] is None:
                c = SchwabClient(config)
                if not c.authenticate():
                    raise RuntimeError("Schwab auth failed")
                _client["instance"] = c
            return _client["instance"]

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        yield

    app = FastAPI(title="Schwab Agent Dashboard", lifespan=lifespan)
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    # ── Routes ────────────────────────────────────────────────────────────

    @app.get("/")
    async def index():
        return FileResponse(str(STATIC_DIR / "index.html"))

    @app.get("/api/accounts")
    async def get_accounts():
        """Return all linked Schwab accounts with balances and positions."""
        try:
            client = get_client()
            accounts = client.get_all_accounts()
            result = []
            for a in accounts:
                positions = [asdict(p) for p in a.positions]
                result.append({
                    "account_hash": a.account_hash,
                    "account_number": a.account_number,
                    "account_type": a.account_type,
                    "total_value": round(a.total_value, 2),
                    "cash_available": round(a.cash_available, 2),
                    "unsettled_cash": round(a.unsettled_cash, 2),
                    "invested": round(a.total_value - a.cash_available, 2),
                    "positions": positions,
                    "position_count": len(positions),
                    "round_trips": a.round_trips,
                    "is_day_trader": a.is_day_trader,
                    "is_closing_only": a.is_closing_only,
                })
            # Aggregate totals
            total_value = sum(a["total_value"] for a in result)
            total_cash = sum(a["cash_available"] for a in result)
            total_invested = sum(a["invested"] for a in result)
            total_positions = sum(a["position_count"] for a in result)
            return {
                "accounts": result,
                "totals": {
                    "total_value": round(total_value, 2),
                    "cash_available": round(total_cash, 2),
                    "invested": round(total_invested, 2),
                    "position_count": total_positions,
                    "account_count": len(result),
                },
            }
        except Exception as e:
            logger.error("GET /api/accounts failed: %s", e)
            return {"error": str(e), "accounts": [], "totals": {}}

    @app.get("/api/status")
    async def get_status():
        """Return agent config and risk status."""
        try:
            risk_state = state.load_risk_state()
            return {
                "dry_run": config.DRY_RUN,
                "strategies": config.strategies,
                "watchlist": config.watchlist,
                "risk": {
                    "killed": risk_state.get("killed", False),
                    "kill_reason": risk_state.get("kill_reason", ""),
                    "peak_value": risk_state.get("peak_value", 0),
                    "max_drawdown_pct": config.MAX_DRAWDOWN_PCT,
                    "max_position_pct": config.MAX_POSITION_PCT,
                    "max_position_value": config.MAX_POSITION_VALUE,
                    "max_total_exposure": config.MAX_TOTAL_EXPOSURE,
                },
                "config": {
                    "scan_interval": config.SCAN_INTERVAL_SECONDS,
                    "min_order_value": config.MIN_ORDER_VALUE,
                    "max_order_value": config.MAX_ORDER_VALUE,
                    "regime_enabled": config.REGIME_ENABLED,
                    "telegram_enabled": config.TELEGRAM_ENABLED,
                    "llm_enabled": config.LLM_ENABLED,
                },
            }
        except Exception as e:
            return {"error": str(e)}

    @app.get("/api/trades")
    async def get_trades(limit: int = 50):
        """Return recent trade history."""
        trades = state.get_trade_history(limit=limit)
        return {"trades": trades, "count": len(trades)}

    @app.get("/api/pnl")
    async def get_pnl():
        """Return per-strategy P&L summary."""
        pnl = state.get_strategy_pnl()
        total_pnl = 0.0
        total_trades = 0
        strategies = []
        for name, data in sorted(pnl.items()):
            trades = data.get("trades", 0)
            wins = data.get("wins", 0)
            realized = data.get("realized_pnl", 0.0)
            losses = data.get("losses", 0)
            wr = round(wins / trades * 100, 1) if trades > 0 else 0.0
            strategies.append({
                "strategy": name,
                "trades": trades,
                "wins": wins,
                "losses": losses,
                "realized_pnl": round(realized, 2),
                "win_rate": wr,
            })
            total_pnl += realized
            total_trades += trades
        return {
            "strategies": strategies,
            "total_pnl": round(total_pnl, 2),
            "total_trades": total_trades,
        }

    @app.get("/api/audit")
    async def get_audit(limit: int = 50):
        """Return recent audit log entries."""
        entries = state.get_audit_log(limit=limit)
        return {"entries": entries, "count": len(entries)}

    # ── WebSocket ─────────────────────────────────────────────────────────

    @app.websocket("/ws/live")
    async def websocket_live(ws: WebSocket):
        await manager.connect(ws)
        try:
            while True:
                # Heartbeat every 5 seconds
                await asyncio.sleep(5)
                await ws.send_json({"type": "heartbeat", "ts": datetime.now(timezone.utc).isoformat()})
        except WebSocketDisconnect:
            manager.disconnect(ws)
        except Exception:
            manager.disconnect(ws)

    return app


# ── Server runner ────────────────────────────────────────────────────────────

def run_server(config: Config | None = None, host: str = "0.0.0.0", port: int = 8898):
    """Start the dashboard web server."""
    import uvicorn
    app = create_app(config)
    logger.info("Starting dashboard at http://%s:%d", host, port)
    uvicorn.run(app, host=host, port=port, log_level="info")
