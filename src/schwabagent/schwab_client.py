"""Schwab API client wrapper using schwab-py library."""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from schwabagent.config import Config

logger = logging.getLogger(__name__)


# ── Data models ────────────────────────────────────────────────────────────────

@dataclass
class Position:
    symbol: str
    quantity: float
    market_value: float
    avg_price: float
    unrealized_pnl: float
    weight: float  # pct of portfolio (0-1)


@dataclass
class AccountSummary:
    account_hash: str
    account_number: str  # masked
    total_value: float
    cash_available: float
    positions: list[Position] = field(default_factory=list)


@dataclass
class Quote:
    symbol: str
    bid: float
    ask: float
    last: float
    volume: int
    change_pct: float


# ── Client ─────────────────────────────────────────────────────────────────────

class SchwabClient:
    """Thin wrapper around schwab-py that provides the data shapes the agent needs."""

    def __init__(self, config: Config):
        self.config = config
        self._client: Any = None  # schwab-py client, set after authenticate()

    # ── Auth ──────────────────────────────────────────────────────────────────

    def authenticate(self) -> bool:
        """Load OAuth token from file, or start interactive OAuth flow if missing.

        Returns True if authenticated successfully.
        """
        try:
            import schwab  # type: ignore
        except ImportError:
            logger.error("schwab-py is not installed — run `uv add schwab-py`")
            return False

        token_path = Path(self.config.SCHWAB_TOKEN_PATH).expanduser()
        token_path.parent.mkdir(parents=True, exist_ok=True)

        api_key = self.config.SCHWAB_API_KEY
        app_secret = self.config.SCHWAB_APP_SECRET

        if not api_key or not app_secret:
            logger.error("SCHWAB_API_KEY and SCHWAB_APP_SECRET must be set in .env")
            return False

        try:
            if token_path.exists():
                self._client = schwab.auth.client_from_token_file(
                    api_key, app_secret, str(token_path)
                )
                logger.info("Authenticated from token file: %s", token_path)
            else:
                # Interactive OAuth — opens browser
                logger.info("No token file found at %s — starting OAuth flow", token_path)
                from schwab.auth import client_from_login_flow  # type: ignore
                self._client = client_from_login_flow(
                    api_key,
                    app_secret,
                    "https://127.0.0.1",
                    str(token_path),
                )
                logger.info("OAuth complete — token saved to %s", token_path)
            return True
        except Exception as e:
            logger.error("Authentication failed: %s", e)
            return False

    def _require_client(self) -> Any:
        if self._client is None:
            raise RuntimeError("Not authenticated — call authenticate() first")
        return self._client

    # ── Account ───────────────────────────────────────────────────────────────

    def get_all_accounts(self) -> list[AccountSummary]:
        """Return a summary for every account linked to these credentials."""
        client = self._require_client()
        try:
            resp = client.get_accounts(fields=[client.Account.Fields.POSITIONS])
            resp.raise_for_status()
            raw_accounts = resp.json()
        except Exception as e:
            logger.error("get_all_accounts failed: %s", e)
            return []

        summaries = []
        for item in raw_accounts:
            acct = item.get("securitiesAccount", {})
            summary = self._parse_account(acct)
            if summary:
                summaries.append(summary)
        return summaries

    def get_account_summary(self, account_hash: str) -> AccountSummary | None:
        """Return summary for a specific account hash."""
        client = self._require_client()
        try:
            resp = client.get_account(
                account_hash,
                fields=[client.Account.Fields.POSITIONS],
            )
            resp.raise_for_status()
            acct = resp.json().get("securitiesAccount", {})
            return self._parse_account(acct, account_hash=account_hash)
        except Exception as e:
            logger.error("get_account_summary(%s) failed: %s", account_hash[:8], e)
            return None

    def _parse_account(self, acct: dict, account_hash: str = "") -> AccountSummary | None:
        """Parse raw securitiesAccount dict into AccountSummary."""
        try:
            balance = acct.get("currentBalances", {})
            total_value = float(balance.get("liquidationValue", balance.get("totalValue", 0)))
            cash = float(balance.get("cashBalance", balance.get("availableFunds", 0)))
            acct_number = acct.get("accountNumber", "****")
            # The account hash comes from the parent wrapper in list responses
            # or from the direct get_account call
            if not account_hash:
                account_hash = acct.get("accountId", "")

            positions = []
            for pos in acct.get("positions", []):
                instrument = pos.get("instrument", {})
                symbol = instrument.get("symbol", "")
                quantity = float(pos.get("longQuantity", 0)) - float(pos.get("shortQuantity", 0))
                market_value = float(pos.get("marketValue", 0))
                avg_price = float(pos.get("averagePrice", 0))
                pnl = float(pos.get("currentDayProfitLoss", pos.get("unrealizedProfitLoss", 0)))
                weight = market_value / total_value if total_value > 0 else 0.0
                positions.append(Position(
                    symbol=symbol,
                    quantity=quantity,
                    market_value=market_value,
                    avg_price=avg_price,
                    unrealized_pnl=pnl,
                    weight=weight,
                ))

            return AccountSummary(
                account_hash=account_hash,
                account_number=acct_number,
                total_value=total_value,
                cash_available=cash,
                positions=positions,
            )
        except Exception as e:
            logger.error("Failed to parse account: %s", e)
            return None

    # ── Quotes ────────────────────────────────────────────────────────────────

    def get_quotes(self, symbols: list[str]) -> dict[str, Quote]:
        """Fetch live quotes for a list of symbols."""
        if not symbols:
            return {}
        client = self._require_client()
        try:
            resp = client.get_quotes(symbols)
            resp.raise_for_status()
            raw = resp.json()
        except Exception as e:
            logger.error("get_quotes(%s) failed: %s", symbols[:3], e)
            return {}

        quotes: dict[str, Quote] = {}
        for symbol, data in raw.items():
            try:
                q = data.get("quote", data)  # schema varies by asset type
                quotes[symbol] = Quote(
                    symbol=symbol,
                    bid=float(q.get("bidPrice", q.get("bid", 0))),
                    ask=float(q.get("askPrice", q.get("ask", 0))),
                    last=float(q.get("lastPrice", q.get("last", q.get("mark", 0)))),
                    volume=int(q.get("totalVolume", q.get("volume", 0))),
                    change_pct=float(q.get("netPercentChangeInDouble", q.get("percentChange", 0))),
                )
            except Exception as e:
                logger.warning("Failed to parse quote for %s: %s", symbol, e)
        return quotes

    # ── OHLCV ─────────────────────────────────────────────────────────────────

    def get_ohlcv(self, symbol: str, days: int = 100) -> pd.DataFrame:
        """Fetch daily OHLCV for *symbol* covering the last *days* calendar days.

        Returns a DataFrame with columns: open, high, low, close, volume
        indexed by date (DatetimeIndex, UTC).
        """
        client = self._require_client()
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=days + 30)  # buffer for weekends/holidays

        try:
            resp = client.get_price_history_every_day(
                symbol,
                start_datetime=start,
                end_datetime=end,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.error("get_ohlcv(%s) failed: %s", symbol, e)
            return pd.DataFrame(columns=["open", "high", "low", "close", "volume"])

        candles = data.get("candles", [])
        if not candles:
            logger.warning("No candles returned for %s", symbol)
            return pd.DataFrame(columns=["open", "high", "low", "close", "volume"])

        rows = []
        for c in candles:
            rows.append({
                "date": pd.Timestamp(c["datetime"], unit="ms", tz="UTC"),
                "open": float(c["open"]),
                "high": float(c["high"]),
                "low": float(c["low"]),
                "close": float(c["close"]),
                "volume": int(c["volume"]),
            })

        df = pd.DataFrame(rows).set_index("date").sort_index()
        # Trim to requested days
        cutoff = end - timedelta(days=days)
        df = df[df.index >= pd.Timestamp(cutoff)]
        return df

    # ── Orders ────────────────────────────────────────────────────────────────

    def place_order(
        self,
        account_hash: str,
        symbol: str,
        side: str,
        quantity: int,
        order_type: str = "MARKET",
    ) -> dict:
        """Place an equity order.

        Args:
            account_hash: Schwab account hash (from AccountSummary).
            symbol: Ticker symbol.
            side: "BUY" or "SELL".
            quantity: Number of shares.
            order_type: "MARKET" or "LIMIT".

        Returns:
            Dict with order details and status.
        """
        client = self._require_client()
        try:
            import schwab.orders.equities as eq  # type: ignore
        except ImportError:
            logger.error("schwab.orders not available")
            return {"status": "error", "error": "schwab.orders not available"}

        if quantity <= 0:
            return {"status": "error", "error": f"Invalid quantity: {quantity}"}

        side_upper = side.upper()
        try:
            if side_upper == "BUY":
                order = eq.equity_buy_market(symbol, quantity)
            elif side_upper == "SELL":
                order = eq.equity_sell_market(symbol, quantity)
            else:
                return {"status": "error", "error": f"Unknown side: {side}"}

            resp = client.place_order(account_hash, order)
            resp.raise_for_status()

            # Extract order ID from Location header
            order_id = resp.headers.get("Location", "").split("/")[-1]
            logger.info(
                "Order placed: %s %d %s (id=%s)",
                side_upper, quantity, symbol, order_id,
            )
            return {
                "status": "ok",
                "order_id": order_id,
                "symbol": symbol,
                "side": side_upper,
                "quantity": quantity,
                "order_type": order_type,
            }
        except Exception as e:
            logger.error("place_order(%s %d %s) failed: %s", side_upper, quantity, symbol, e)
            return {"status": "error", "error": str(e)}

    def cancel_order(self, account_hash: str, order_id: str) -> bool:
        """Cancel an open order. Returns True on success."""
        client = self._require_client()
        try:
            resp = client.cancel_order(account_hash, order_id)
            resp.raise_for_status()
            logger.info("Order %s cancelled", order_id)
            return True
        except Exception as e:
            logger.error("cancel_order(%s) failed: %s", order_id, e)
            return False

    def get_open_orders(self, account_hash: str) -> list[dict]:
        """Return all open orders for an account."""
        client = self._require_client()
        try:
            resp = client.get_orders_for_account(
                account_hash,
                from_entered_datetime=datetime.now(timezone.utc) - timedelta(days=1),
                to_entered_datetime=datetime.now(timezone.utc),
                status="WORKING",
            )
            resp.raise_for_status()
            return resp.json() or []
        except Exception as e:
            logger.error("get_open_orders(%s) failed: %s", account_hash[:8], e)
            return []
