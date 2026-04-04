"""Risk management — position limits, exposure cap, drawdown kill switch."""
from __future__ import annotations

import logging
from datetime import datetime, timezone

from schwabagent.config import Config
from schwabagent.persistence import StateStore
from schwabagent.schwab_client import AccountSummary

logger = logging.getLogger(__name__)


class RiskManager:
    """Enforces per-position limits, total exposure cap, and drawdown kill switch."""

    def __init__(self, config: Config, state: StateStore):
        self.config = config
        self.state = state
        self._killed = False
        self._kill_reason = ""
        self._peak_value: float = 0.0

        self._load()
        self.state.audit("risk_manager_init", {
            "max_position_pct": config.MAX_POSITION_PCT,
            "max_position_value": config.MAX_POSITION_VALUE,
            "max_total_exposure": config.MAX_TOTAL_EXPOSURE,
            "max_drawdown_pct": config.MAX_DRAWDOWN_PCT,
            "dry_run": config.DRY_RUN,
        })

    # ── Persistence ───────────────────────────────────────────────────────────

    def _load(self) -> None:
        data = self.state.load_risk_state()
        self._killed = data.get("killed", False)
        self._kill_reason = data.get("kill_reason", "")
        self._peak_value = data.get("peak_value", 0.0)
        if self._killed:
            logger.warning("Kill switch is active from prior session: %s", self._kill_reason)

    def _save(self) -> None:
        self.state.save_risk_state({
            "killed": self._killed,
            "kill_reason": self._kill_reason,
            "peak_value": self._peak_value,
            "saved_at": datetime.now(timezone.utc).isoformat(),
        })

    # ── Kill switch ───────────────────────────────────────────────────────────

    def kill(self, reason: str = "") -> None:
        self._killed = True
        self._kill_reason = reason
        logger.critical("KILL SWITCH activated: %s", reason)
        self.state.audit("kill_switch", {"reason": reason})
        self._save()

    def unkill(self) -> None:
        self._killed = False
        self._kill_reason = ""
        logger.info("Kill switch deactivated")
        self.state.audit("unkill", {})
        self._save()

    def is_killed(self) -> bool:
        return self._killed

    # ── Buy checks ────────────────────────────────────────────────────────────

    def can_buy(
        self,
        symbol: str,
        quantity: int,
        price: float,
        account: AccountSummary,
    ) -> tuple[bool, str]:
        """Check whether a buy order is allowed under all risk limits.

        Returns:
            (allowed: bool, reason: str)  — reason is empty when allowed=True.
        """
        if self._killed:
            return False, f"Kill switch active: {self._kill_reason}"

        order_value = quantity * price

        # Minimum order size
        if order_value < self.config.MIN_ORDER_VALUE:
            return False, (
                f"Order value ${order_value:.2f} below minimum ${self.config.MIN_ORDER_VALUE:.2f}"
            )

        # Maximum single order size
        if order_value > self.config.MAX_ORDER_VALUE:
            return False, (
                f"Order value ${order_value:.2f} exceeds max order ${self.config.MAX_ORDER_VALUE:.2f}"
            )

        # Per-position value cap
        if order_value > self.config.MAX_POSITION_VALUE:
            return False, (
                f"Order value ${order_value:.2f} exceeds max position ${self.config.MAX_POSITION_VALUE:.2f}"
            )

        # Per-position portfolio % cap
        if account.total_value > 0:
            pct = order_value / account.total_value
            if pct > self.config.MAX_POSITION_PCT:
                return False, (
                    f"Order is {pct:.1%} of portfolio, exceeds limit {self.config.MAX_POSITION_PCT:.1%}"
                )

        # Total exposure cap: sum of all existing position market values + new order
        current_exposure = sum(p.market_value for p in account.positions if p.quantity > 0)
        if current_exposure + order_value > self.config.MAX_TOTAL_EXPOSURE:
            return False, (
                f"Total exposure ${current_exposure + order_value:,.2f} would exceed "
                f"cap ${self.config.MAX_TOTAL_EXPOSURE:,.2f}"
            )

        # Sufficient cash
        if order_value > account.cash_available:
            return False, (
                f"Insufficient cash: need ${order_value:.2f}, have ${account.cash_available:.2f}"
            )

        return True, ""

    # ── Trade recording ───────────────────────────────────────────────────────

    def record_trade(
        self,
        symbol: str,
        side: str,
        quantity: int,
        price: float,
        strategy: str = "",
    ) -> None:
        """Record an executed trade to the audit log and trade history."""
        trade = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "symbol": symbol,
            "side": side.upper(),
            "quantity": quantity,
            "price": price,
            "value": quantity * price,
            "strategy": strategy,
            "dry_run": self.config.DRY_RUN,
        }
        self.state.append_trade(trade)
        self.state.audit("trade_executed", trade)
        logger.info(
            "Trade recorded: %s %s %d @ $%.2f = $%.2f [%s]",
            side.upper(), symbol, quantity, price, quantity * price,
            "DRY" if self.config.DRY_RUN else "LIVE",
        )

    # ── Drawdown tracking ─────────────────────────────────────────────────────

    def update_peak(self, portfolio_value: float) -> None:
        """Update peak portfolio value. Called after every account refresh."""
        if portfolio_value > self._peak_value:
            self._peak_value = portfolio_value
            self._save()

    def check_drawdown(self, current_value: float) -> tuple[bool, float]:
        """Check whether current drawdown is within limits.

        Returns:
            (is_ok: bool, drawdown_pct: float)
        """
        if self._peak_value <= 0:
            self.update_peak(current_value)
            return True, 0.0

        drawdown_pct = (self._peak_value - current_value) / self._peak_value * 100.0
        max_dd = self.config.MAX_DRAWDOWN_PCT

        if drawdown_pct > max_dd and not self._killed:
            self.kill(
                f"Max drawdown exceeded: {drawdown_pct:.1f}% > {max_dd:.1f}% "
                f"(peak=${self._peak_value:,.2f} → ${current_value:,.2f})"
            )
            return False, drawdown_pct

        return True, drawdown_pct

    # ── Status ────────────────────────────────────────────────────────────────

    def status(self) -> dict:
        return {
            "killed": self._killed,
            "kill_reason": self._kill_reason,
            "peak_value": round(self._peak_value, 2),
            "max_drawdown_pct": self.config.MAX_DRAWDOWN_PCT,
            "max_position_pct": self.config.MAX_POSITION_PCT,
            "max_position_value": self.config.MAX_POSITION_VALUE,
            "max_total_exposure": self.config.MAX_TOTAL_EXPOSURE,
            "dry_run": self.config.DRY_RUN,
        }
