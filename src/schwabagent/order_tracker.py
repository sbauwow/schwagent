"""Order fill tracking — monitors placed orders until filled or cancelled.

After placing an order, the fill price may differ from the quoted price.
This module tracks pending orders, polls for fill status, and updates
position entry prices with actual fill data.

Integrates with:
- StreamManager (account activity for real-time fill notifications)
- SchwabClient (order status polling as fallback)
- Scalp strategy (updates TP/SL based on actual fill price)
"""
from __future__ import annotations

import json
import logging
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from schwabagent.config import Config
from schwabagent.persistence import StateStore

logger = logging.getLogger(__name__)


@dataclass
class PendingOrder:
    """An order that's been placed but not yet confirmed filled."""
    order_id: str
    symbol: str
    side: str               # BUY or SELL
    quantity: int
    expected_price: float   # price at time of order
    account_hash: str
    strategy: str
    placed_at: str          # ISO timestamp
    status: str = "PENDING" # PENDING, FILLED, CANCELLED, EXPIRED, REJECTED
    fill_price: float = 0.0
    fill_quantity: int = 0
    filled_at: str = ""
    attempts: int = 0


FillCallback = Callable[[PendingOrder], None]


class OrderTracker:
    """Track pending orders and confirm fills."""

    def __init__(self, config: Config, state: StateStore):
        self._config = config
        self._state = state
        self._pending: dict[str, PendingOrder] = {}
        self._lock = threading.Lock()
        self._callbacks: list[FillCallback] = []
        self._pending_path = Path(config.STATE_DIR).expanduser() / "pending_orders.json"

        self._load_pending()

    # ── Register callbacks ───────────────────────────────────────────────

    def on_fill(self, callback: FillCallback) -> None:
        """Register a callback for when an order fills."""
        self._callbacks.append(callback)

    # ── Track new orders ─────────────────────────────────────────────────

    def track(
        self,
        order_id: str,
        symbol: str,
        side: str,
        quantity: int,
        expected_price: float,
        account_hash: str,
        strategy: str = "",
    ) -> None:
        """Start tracking a newly placed order."""
        order = PendingOrder(
            order_id=order_id,
            symbol=symbol,
            side=side.upper(),
            quantity=quantity,
            expected_price=expected_price,
            account_hash=account_hash,
            strategy=strategy,
            placed_at=datetime.now(timezone.utc).isoformat(),
        )
        with self._lock:
            self._pending[order_id] = order
        self._save_pending()
        logger.info(
            "Tracking order %s: %s %d %s @ $%.2f",
            order_id, side, quantity, symbol, expected_price,
        )

    # ── Check fills (polling) ────────────────────────────────────────────

    def check_fills(self, schwab_client: Any) -> list[PendingOrder]:
        """Poll Schwab API for order status updates.

        Returns list of orders that were resolved (filled, cancelled, etc.).
        """
        resolved = []
        with self._lock:
            pending = list(self._pending.values())

        for order in pending:
            if order.status != "PENDING":
                continue

            order.attempts += 1
            try:
                orders = schwab_client.get_open_orders(order.account_hash)
                # Also check recent filled orders
                client = schwab_client._require_client()
                now = datetime.now(timezone.utc)
                resp = client.get_orders_for_account(
                    order.account_hash,
                    from_entered_datetime=now - timedelta(hours=1),
                    to_entered_datetime=now,
                )
                resp.raise_for_status()
                all_orders = resp.json() or []

                matched = None
                for api_order in all_orders:
                    oid = str(api_order.get("orderId", ""))
                    if oid == order.order_id:
                        matched = api_order
                        break

                if matched:
                    status = matched.get("status", "").upper()
                    if status == "FILLED":
                        self._resolve_fill(order, matched)
                        resolved.append(order)
                    elif status in ("CANCELED", "CANCELLED", "REJECTED", "EXPIRED"):
                        order.status = status
                        resolved.append(order)
                        logger.warning("Order %s %s: %s", order.order_id, status, order.symbol)
                elif order.attempts > 20:
                    # Give up after ~20 checks
                    order.status = "EXPIRED"
                    resolved.append(order)
                    logger.warning("Order %s expired (not found after %d checks)", order.order_id, order.attempts)

            except Exception as e:
                logger.error("Failed to check order %s: %s", order.order_id, e)

        # Remove resolved orders from pending
        if resolved:
            with self._lock:
                for order in resolved:
                    self._pending.pop(order.order_id, None)
            self._save_pending()

            # Fire callbacks
            for order in resolved:
                if order.status == "FILLED":
                    for cb in self._callbacks:
                        try:
                            cb(order)
                        except Exception as e:
                            logger.error("Fill callback error: %s", e)

        return resolved

    def _resolve_fill(self, order: PendingOrder, api_order: dict) -> None:
        """Extract fill details from Schwab order response."""
        order.status = "FILLED"
        order.filled_at = datetime.now(timezone.utc).isoformat()

        # Extract fill price from order activity
        activities = api_order.get("orderActivityCollection", [])
        if activities:
            fills = []
            for activity in activities:
                for exec_leg in activity.get("executionLegs", []):
                    fills.append({
                        "price": float(exec_leg.get("price", 0)),
                        "quantity": int(exec_leg.get("quantity", 0)),
                    })
            if fills:
                total_qty = sum(f["quantity"] for f in fills)
                total_value = sum(f["price"] * f["quantity"] for f in fills)
                order.fill_price = total_value / total_qty if total_qty > 0 else order.expected_price
                order.fill_quantity = total_qty

        if order.fill_price == 0:
            order.fill_price = float(api_order.get("price", order.expected_price))
        if order.fill_quantity == 0:
            order.fill_quantity = int(api_order.get("filledQuantity", order.quantity))

        slippage = order.fill_price - order.expected_price
        slippage_bps = slippage / order.expected_price * 10000 if order.expected_price > 0 else 0

        logger.info(
            "Order %s FILLED: %s %d %s @ $%.2f (expected $%.2f, slippage=%+.1f bps)",
            order.order_id, order.side, order.fill_quantity, order.symbol,
            order.fill_price, order.expected_price, slippage_bps,
        )

        self._state.audit("order_filled", {
            "order_id": order.order_id,
            "symbol": order.symbol,
            "side": order.side,
            "quantity": order.fill_quantity,
            "expected_price": order.expected_price,
            "fill_price": order.fill_price,
            "slippage_bps": round(slippage_bps, 1),
            "strategy": order.strategy,
        })

    # ── Handle streaming fill events ─────────────────────────────────────

    def handle_stream_fill(self, fill_event: Any) -> None:
        """Called by StreamManager when an account activity event arrives.

        Attempts to match the event to a pending order and resolve it.
        """
        data = fill_event.data if hasattr(fill_event, "data") else {}
        if isinstance(data, str):
            try:
                data = json.loads(data)
            except (json.JSONDecodeError, TypeError):
                return

        # Try to find matching pending order
        order_id = str(data.get("OrderId", data.get("orderId", "")))
        if order_id:
            with self._lock:
                order = self._pending.get(order_id)
            if order and order.status == "PENDING":
                order.status = "FILLED"
                order.fill_price = float(data.get("ExecutionPrice", data.get("price", order.expected_price)))
                order.fill_quantity = int(data.get("Quantity", data.get("quantity", order.quantity)))
                order.filled_at = datetime.now(timezone.utc).isoformat()

                with self._lock:
                    self._pending.pop(order_id, None)
                self._save_pending()

                logger.info(
                    "Order %s filled via stream: %s %d %s @ $%.2f",
                    order_id, order.side, order.fill_quantity, order.symbol, order.fill_price,
                )

                for cb in self._callbacks:
                    try:
                        cb(order)
                    except Exception as e:
                        logger.error("Fill callback error: %s", e)

    # ── Persistence ──────────────────────────────────────────────────────

    def _save_pending(self) -> None:
        with self._lock:
            data = {
                oid: {
                    "order_id": o.order_id, "symbol": o.symbol, "side": o.side,
                    "quantity": o.quantity, "expected_price": o.expected_price,
                    "account_hash": o.account_hash, "strategy": o.strategy,
                    "placed_at": o.placed_at, "status": o.status,
                    "fill_price": o.fill_price, "fill_quantity": o.fill_quantity,
                    "filled_at": o.filled_at, "attempts": o.attempts,
                }
                for oid, o in self._pending.items()
            }
        try:
            self._pending_path.write_text(json.dumps(data, indent=2))
        except OSError as e:
            logger.warning("Failed to save pending orders: %s", e)

    def _load_pending(self) -> None:
        if not self._pending_path.exists():
            return
        try:
            data = json.loads(self._pending_path.read_text())
            for oid, d in data.items():
                self._pending[oid] = PendingOrder(**d)
            if self._pending:
                logger.info("Restored %d pending orders from disk", len(self._pending))
        except Exception as e:
            logger.warning("Failed to load pending orders: %s", e)

    # ── Status ───────────────────────────────────────────────────────────

    @property
    def pending_count(self) -> int:
        return len(self._pending)

    def status(self) -> dict:
        with self._lock:
            return {
                "pending": len(self._pending),
                "orders": [
                    {"id": o.order_id, "symbol": o.symbol, "side": o.side,
                     "status": o.status, "expected": o.expected_price,
                     "attempts": o.attempts}
                    for o in self._pending.values()
                ],
            }
