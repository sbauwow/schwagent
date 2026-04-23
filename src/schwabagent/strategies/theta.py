"""Theta / wheel strategy — sell cash-secured puts, roll into covered calls.

"Be right and sit tight." Each symbol walks a small state machine:

    CASH
      └─► sell cash-secured put  (STO_PUT)
            ├─► put expires OTM                   → CASH         (keep credit)
            ├─► 50% profit captured               → BTC_PUT      → CASH
            ├─► ≤ defensive DTE                   → BTC_PUT      → CASH
            └─► assigned at expiry                → LONG_STOCK

    LONG_STOCK
      └─► sell covered call (STO_CALL)
            ├─► call expires OTM                  → LONG_STOCK   (keep credit)
            ├─► 50% profit captured               → BTC_CALL     → LONG_STOCK
            ├─► ≤ defensive DTE                   → BTC_CALL     → LONG_STOCK
            └─► called away at expiry             → CASH

State lives in a sidecar JSON (`theta_positions.json`) — we do not trust
the broker position list to carry enough context (e.g. which credit we
opened at). Assignment / called-away transitions are reconciled on each
scan cycle by comparing broker positions against the sidecar.
"""
from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

from schwabagent.config import Config
from schwabagent.persistence import StateStore
from schwabagent.risk import RiskManager
from schwabagent.schwab_client import (
    AccountSummary,
    OptionContract,
    SchwabClient,
)
from schwabagent.strategies.base import Strategy

logger = logging.getLogger(__name__)


WheelState = Literal["CASH", "SHORT_PUT", "LONG_STOCK", "SHORT_CALL"]


@dataclass
class WheelLeg:
    """An open short option leg we are tracking."""
    osi_symbol: str
    side: str            # "PUT" or "CALL"
    strike: float
    expiration: str      # YYYY-MM-DD
    contracts: int
    credit: float        # per-contract credit received at STO (dollars)
    opened_at: str       # ISO timestamp


@dataclass
class WheelPosition:
    """Per-symbol wheel state."""
    symbol: str
    state: WheelState = "CASH"
    shares: int = 0
    cost_basis: float = 0.0       # effective basis after premium capture
    leg: WheelLeg | None = None
    notes: str = ""


class ThetaStrategy(Strategy):
    """Cash-secured-put → covered-call wheel."""

    name = "theta"

    def __init__(
        self,
        client: SchwabClient,
        config: Config,
        risk: RiskManager,
        state: StateStore,
        account: AccountSummary | None = None,
    ):
        super().__init__(client, config, risk, state)
        self._account = account
        self._positions: dict[str, WheelPosition] = {}
        self._load_positions()

    def set_account(self, account: AccountSummary) -> None:
        self._account = account

    # ── scan ─────────────────────────────────────────────────────────────

    def scan(self) -> list[dict]:
        universe = self.config.theta_symbols
        if not universe:
            return []

        account = self._account
        if account is None:
            logger.warning("[theta] No account set — skipping scan")
            return []

        # Reconcile sidecar state against broker positions before scanning.
        self._reconcile(account)

        opportunities: list[dict] = []
        for symbol in universe:
            pos = self._positions.get(symbol) or WheelPosition(symbol=symbol)
            self._positions.setdefault(symbol, pos)

            if pos.state == "CASH":
                opp = self._scan_cash(symbol, account)
            elif pos.state == "SHORT_PUT":
                opp = self._scan_short_option(pos, "PUT")
            elif pos.state == "LONG_STOCK":
                opp = self._scan_long_stock(pos, account)
            elif pos.state == "SHORT_CALL":
                opp = self._scan_short_option(pos, "CALL")
            else:
                opp = None

            if opp:
                opportunities.append(opp)

        self._save_positions()
        return opportunities

    def _scan_cash(self, symbol: str, account: AccountSummary) -> dict | None:
        """Look for a cash-secured put to sell."""
        cfg = self.config
        chain = self.client.get_option_chain(
            symbol, "PUT", cfg.THETA_DTE_MIN, cfg.THETA_DTE_MAX, strike_count=30,
        )
        contract = self._pick_target(chain)
        if contract is None:
            return None

        # Collateral check: CSP needs strike * 100 per contract.
        contracts = cfg.THETA_MAX_CONTRACTS_PER_SYMBOL
        collateral = contract.strike * 100 * contracts
        if collateral > account.cash_available:
            logger.info(
                "[theta] %s skip CSP — need $%.0f collateral, have $%.0f",
                symbol, collateral, account.cash_available,
            )
            return None

        credit = self._limit_credit(contract)
        if credit <= 0 or credit < cfg.THETA_MIN_CREDIT:
            return None

        return {
            "strategy": self.name,
            "symbol": symbol,
            "signal": "SELL",
            "score": abs(contract.delta) * (credit * 100),
            "price": contract.mark,
            "_action": "STO_PUT",
            "_contract": contract,
            "_contracts": contracts,
            "_limit": credit,
            "reason": (
                f"CSP {symbol} {contract.expiration} P{contract.strike:g} "
                f"Δ={contract.delta:+.2f} credit=${credit:.2f} DTE={contract.dte}"
            ),
        }

    def _scan_long_stock(self, pos: WheelPosition, account: AccountSummary) -> dict | None:
        """We hold shares from an assignment — sell a covered call above basis."""
        cfg = self.config
        held = next(
            (p for p in account.positions if p.symbol == pos.symbol and p.quantity >= 100),
            None,
        )
        if held is None:
            logger.info("[theta] %s LONG_STOCK but no 100+ share block — skipping CC", pos.symbol)
            return None

        contracts = min(
            int(held.quantity) // 100,
            cfg.THETA_MAX_CONTRACTS_PER_SYMBOL,
        )
        if contracts <= 0:
            return None

        chain = self.client.get_option_chain(
            pos.symbol, "CALL", cfg.THETA_DTE_MIN, cfg.THETA_DTE_MAX, strike_count=30,
        )
        # Never sell a call below cost basis — we don't want to lock in a loss on assignment.
        chain = [c for c in chain if c.strike >= pos.cost_basis]
        contract = self._pick_target(chain)
        if contract is None:
            return None

        credit = self._limit_credit(contract)
        if credit <= 0 or credit < cfg.THETA_MIN_CREDIT:
            return None

        return {
            "strategy": self.name,
            "symbol": pos.symbol,
            "signal": "SELL",
            "score": contract.delta * (credit * 100),
            "price": contract.mark,
            "_action": "STO_CALL",
            "_contract": contract,
            "_contracts": contracts,
            "_limit": credit,
            "reason": (
                f"CC {pos.symbol} {contract.expiration} C{contract.strike:g} "
                f"Δ={contract.delta:+.2f} credit=${credit:.2f} DTE={contract.dte} "
                f"basis=${pos.cost_basis:.2f}"
            ),
        }

    def _scan_short_option(self, pos: WheelPosition, side: str) -> dict | None:
        """Check whether to close an open short put/call early."""
        leg = pos.leg
        if leg is None:
            return None

        cfg = self.config
        # Refresh the contract to get current mark + DTE.
        chain = self.client.get_option_chain(
            pos.symbol, side, 0, max(cfg.THETA_DTE_MAX, 60), strike_count=60,
        )
        current = next(
            (c for c in chain if c.symbol == leg.osi_symbol),
            None,
        )
        if current is None:
            logger.debug("[theta] %s %s not found in refreshed chain", pos.symbol, leg.osi_symbol)
            return None

        cost_to_close = max(current.ask, current.mark)
        captured_pct = (
            (leg.credit - cost_to_close) / leg.credit if leg.credit > 0 else 0.0
        )
        reasons: list[str] = []
        if captured_pct >= cfg.THETA_PROFIT_TAKE_PCT:
            reasons.append(f"profit {captured_pct:.0%}")
        if current.dte <= cfg.THETA_DEFENSIVE_DTE:
            reasons.append(f"defensive DTE={current.dte}")
        if not reasons:
            return None

        action = "BTC_PUT" if side == "PUT" else "BTC_CALL"
        return {
            "strategy": self.name,
            "symbol": pos.symbol,
            "signal": "BUY",
            "score": captured_pct,
            "price": current.mark,
            "_action": action,
            "_contract": current,
            "_contracts": leg.contracts,
            "_limit": round(cost_to_close, 2),
            "reason": (
                f"Close {leg.osi_symbol} — " + ", ".join(reasons) +
                f" (credit=${leg.credit:.2f}, cost=${cost_to_close:.2f})"
            ),
        }

    # ── execute ──────────────────────────────────────────────────────────

    def execute(self, opportunity: dict) -> dict | None:
        action = opportunity.get("_action")
        symbol = opportunity["symbol"]
        contract: OptionContract = opportunity["_contract"]
        contracts: int = opportunity["_contracts"]
        limit: float = opportunity["_limit"]

        account = self._account
        if account is None:
            return None

        if action in ("STO_PUT", "STO_CALL"):
            instruction = "SELL_TO_OPEN"
        elif action in ("BTC_PUT", "BTC_CALL"):
            instruction = "BUY_TO_CLOSE"
        else:
            return None

        if self._should_execute(opportunity):
            result = self.client.place_option_order(
                account.account_hash,
                contract.symbol,
                instruction,
                contracts,
                limit_price=limit,
            )
            if result.get("status") != "ok":
                logger.error("[theta] %s failed for %s: %s", action, symbol, result.get("error"))
                return None
        else:
            result = {"status": "dry_run", "order_id": "dry"}

        # Update sidecar state. Entries set the leg; closes clear it.
        pos = self._positions.get(symbol) or WheelPosition(symbol=symbol)
        if action == "STO_PUT":
            pos.state = "SHORT_PUT"
            pos.leg = WheelLeg(
                osi_symbol=contract.symbol,
                side="PUT",
                strike=contract.strike,
                expiration=contract.expiration,
                contracts=contracts,
                credit=limit,
                opened_at=datetime.now(timezone.utc).isoformat(),
            )
        elif action == "STO_CALL":
            pos.state = "SHORT_CALL"
            pos.leg = WheelLeg(
                osi_symbol=contract.symbol,
                side="CALL",
                strike=contract.strike,
                expiration=contract.expiration,
                contracts=contracts,
                credit=limit,
                opened_at=datetime.now(timezone.utc).isoformat(),
            )
        elif action == "BTC_PUT":
            pos.state = "CASH"
            if pos.leg:
                realized = (pos.leg.credit - limit) * 100 * contracts
                self.session_pnl += realized
                self.state.update_strategy_pnl(self.name, realized, win=realized > 0)
            pos.leg = None
        elif action == "BTC_CALL":
            pos.state = "LONG_STOCK"
            if pos.leg:
                realized = (pos.leg.credit - limit) * 100 * contracts
                self.session_pnl += realized
                self.state.update_strategy_pnl(self.name, realized, win=realized > 0)
            pos.leg = None

        self._positions[symbol] = pos
        self._save_positions()

        trade = {
            "strategy": self.name,
            "symbol": symbol,
            "side": "SELL" if instruction == "SELL_TO_OPEN" else "BUY",
            "quantity": contracts,
            "price": limit,
            "value": limit * 100 * contracts,
            "dry_run": not self._should_execute(opportunity),
            "action": action,
            "osi_symbol": contract.symbol,
            "strike": contract.strike,
            "expiration": contract.expiration,
            "dte": contract.dte,
            "delta": contract.delta,
            "reason": opportunity.get("reason", ""),
            **result,
        }
        logger.info(
            "[theta] %s %dx %s @ $%.2f (state=%s)",
            action, contracts, contract.symbol, limit, pos.state,
        )
        return trade

    # ── Helpers ──────────────────────────────────────────────────────────

    def _pick_target(self, chain: list[OptionContract]) -> OptionContract | None:
        """Pick the contract closest to target delta within tolerance."""
        cfg = self.config
        if not chain:
            return None
        target = cfg.THETA_TARGET_DELTA
        tol = cfg.THETA_DELTA_TOLERANCE
        min_oi = cfg.THETA_MIN_OPEN_INTEREST

        candidates = [
            c for c in chain
            if c.open_interest >= min_oi
            and c.bid > 0 and c.ask > 0
            and abs(abs(c.delta) - target) <= tol
        ]
        if not candidates:
            return None
        # Prefer the one whose delta is closest to target; tiebreak on higher OI.
        candidates.sort(key=lambda c: (abs(abs(c.delta) - target), -c.open_interest))
        return candidates[0]

    def _limit_credit(self, contract: OptionContract) -> float:
        """Compute a limit credit for a sell-to-open.

        Start at mid and give up THETA_LIMIT_BUFFER_PCT of the spread width
        for higher fill odds — we want theta on, not perfect pricing.
        """
        if contract.bid <= 0 or contract.ask <= 0:
            return 0.0
        mid = (contract.bid + contract.ask) / 2
        width = contract.ask - contract.bid
        price = mid - width * self.config.THETA_LIMIT_BUFFER_PCT
        return round(max(price, contract.bid), 2)

    def _reconcile(self, account: AccountSummary) -> None:
        """Detect assignment / called-away by comparing sidecar vs broker.

        The broker position list only carries symbol+quantity (no asset type)
        in our AccountSummary, so this reconciliation is conservative:

        - SHORT_PUT + 100+ shares of underlying → assumed assignment.
          Transition to LONG_STOCK with cost basis = strike - credit captured.

        - SHORT_CALL + shares dropped below 100 → assumed called away.
          Transition to CASH.
        """
        for symbol, pos in list(self._positions.items()):
            held = next(
                (p for p in account.positions if p.symbol == symbol and p.quantity > 0),
                None,
            )
            shares = int(held.quantity) if held else 0

            if pos.state == "SHORT_PUT" and shares >= 100 and pos.leg:
                basis = pos.leg.strike - pos.leg.credit
                logger.info(
                    "[theta] %s assigned at %.2f — transition SHORT_PUT → LONG_STOCK "
                    "(basis ≈ $%.2f)",
                    symbol, pos.leg.strike, basis,
                )
                pos.state = "LONG_STOCK"
                pos.shares = shares
                pos.cost_basis = basis
                pos.leg = None
            elif pos.state == "SHORT_CALL" and shares < 100:
                logger.info(
                    "[theta] %s called away — transition SHORT_CALL → CASH",
                    symbol,
                )
                pos.state = "CASH"
                pos.shares = 0
                pos.cost_basis = 0.0
                pos.leg = None
            elif pos.state == "LONG_STOCK" and shares < 100:
                logger.info(
                    "[theta] %s shares gone (%d) — dropping to CASH", symbol, shares,
                )
                pos.state = "CASH"
                pos.shares = 0
                pos.cost_basis = 0.0
                pos.leg = None

    # ── Persistence ──────────────────────────────────────────────────────

    def theta_status(self) -> list[dict]:
        """Return wheel state for all tracked symbols (for /theta command)."""
        out = []
        for sym, pos in sorted(self._positions.items()):
            row: dict = {
                "symbol": sym,
                "state": pos.state,
                "shares": pos.shares,
                "cost_basis": pos.cost_basis,
            }
            if pos.leg:
                row["leg"] = {
                    "osi_symbol": pos.leg.osi_symbol,
                    "side": pos.leg.side,
                    "strike": pos.leg.strike,
                    "expiration": pos.leg.expiration,
                    "contracts": pos.leg.contracts,
                    "credit": pos.leg.credit,
                }
            out.append(row)
        return out

    def _state_file(self) -> Path:
        return Path(self.config.STATE_DIR).expanduser() / "theta_positions.json"

    def _save_positions(self) -> None:
        path = self._state_file()
        path.parent.mkdir(parents=True, exist_ok=True)
        data = {sym: asdict(pos) for sym, pos in self._positions.items()}
        path.write_text(json.dumps(data, indent=2))

    def _load_positions(self) -> None:
        path = self._state_file()
        if not path.exists():
            return
        try:
            raw = json.loads(path.read_text())
            for sym, d in raw.items():
                leg_data = d.pop("leg", None)
                leg = WheelLeg(**leg_data) if leg_data else None
                self._positions[sym] = WheelPosition(leg=leg, **d)
            if self._positions:
                logger.info("[theta] Restored %d wheel positions", len(self._positions))
        except Exception as e:
            logger.warning("[theta] Failed to load positions: %s", e)
