"""Schwab API client wrapper using schwab-py library."""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from schwabagent.config import Config
from schwabagent.rate_limiter import RateLimiter

logger = logging.getLogger(__name__)

# Shared rate limiter — one per API (account + market may share the same limit)
_account_limiter = RateLimiter(max_calls=120, window=60.0)
_market_limiter = RateLimiter(max_calls=120, window=60.0)


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
    # Account metadata from Schwab API
    account_type: str = ""          # "CASH" or "MARGIN"
    round_trips: int = 0            # day trades in rolling 5-day window (Schwab-tracked)
    is_day_trader: bool = False     # PDT flag set by Schwab
    is_closing_only: bool = False   # account restricted to closing orders only
    unsettled_cash: float = 0.0     # cash not yet settled (T+1)


@dataclass
class Quote:
    symbol: str
    bid: float
    ask: float
    last: float
    volume: int
    change_pct: float
    # Liquidity & fundamental fields
    spread: float = 0.0           # ask - bid
    spread_pct: float = 0.0       # spread / mid price as percentage
    avg_10d_volume: int = 0       # 10-day average volume
    next_div_ex_date: str = ""    # YYYY-MM-DD or empty
    next_div_amount: float = 0.0  # per-share dividend amount
    pe_ratio: float = 0.0


# ── Client ─────────────────────────────────────────────────────────────────────

class SchwabClient:
    """Thin wrapper around schwab-py that provides the data shapes the agent needs.

    Uses two separate Schwab apps:
    - Account client (SCHWAB_API_KEY) → positions, balances, orders
    - Market client (SCHWAB_MARKET_API_KEY) → quotes, price history, OHLCV
    """

    def __init__(self, config: Config):
        self.config = config
        self._client: Any = None        # account/trading client
        self._market_client: Any = None  # market data client

    # ── Auth ──────────────────────────────────────────────────────────────────

    @staticmethod
    def _load_token(
        api_key: str, app_secret: str, token_path: Path, label: str,
    ) -> Any | None:
        """Try to load a client from an existing token file. Returns None on failure."""
        try:
            import schwab  # type: ignore
        except ImportError:
            logger.error("schwab-py is not installed — run `uv add schwab-py`")
            return None

        if not api_key or not app_secret:
            return None

        if not token_path.exists():
            logger.info("No %s token file at %s", label, token_path)
            return None

        try:
            client = schwab.auth.client_from_token_file(
                token_path=str(token_path),
                api_key=api_key,
                app_secret=app_secret,
            )
            logger.info("%s client loaded from %s", label, token_path)
            return client
        except Exception as e:
            logger.warning("%s token load failed: %s", label, e)
            return None

    @staticmethod
    def _enroll_interactive(
        api_key: str, app_secret: str, callback_url: str, token_path: Path, label: str,
    ) -> Any | None:
        """Run the Schwab OAuth browser flow. Opens browser, handles callback automatically."""
        try:
            import schwab  # type: ignore
        except ImportError:
            logger.error("schwab-py is not installed — run `uv add schwab-py`")
            return None

        if not api_key or not app_secret:
            logger.error("No credentials configured for %s — set them in .env", label)
            return None

        token_path.parent.mkdir(parents=True, exist_ok=True)
        token_path.unlink(missing_ok=True)

        print(f"\n  Enrolling {label} API — browser will open for Schwab login...")
        print(f"  Callback URL: {callback_url}")
        print(f"  Token will be saved to: {token_path}\n")

        # Try automatic flow first (spins up local HTTPS server for callback)
        try:
            client = schwab.auth.client_from_login_flow(
                api_key=api_key,
                app_secret=app_secret,
                callback_url=callback_url,
                token_path=str(token_path),
                callback_timeout=300,
            )
            try:
                token_path.chmod(0o600)
            except OSError:
                pass
            logger.info("%s enrollment complete (auto flow)", label)
            return client
        except Exception as e:
            logger.warning("%s auto flow failed (%s), falling back to manual flow", label, e)

        # Fallback: manual paste flow
        try:
            client = schwab.auth.client_from_manual_flow(
                api_key=api_key,
                app_secret=app_secret,
                callback_url=callback_url,
                token_path=str(token_path),
            )
            try:
                token_path.chmod(0o600)
            except OSError:
                pass
            logger.info("%s enrollment complete (manual flow)", label)
            return client
        except Exception as e:
            logger.error("%s enrollment failed: %s", label, e)
            return None

    def authenticate(self) -> bool:
        """Load both clients from existing token files.

        Returns True if at least one client loaded successfully.
        Use enroll() for the interactive browser-based OAuth flow.
        """
        self._client = self._load_token(
            api_key=self.config.SCHWAB_API_KEY,
            app_secret=self.config.SCHWAB_APP_SECRET,
            token_path=Path(self.config.SCHWAB_TOKEN_PATH).expanduser(),
            label="Account",
        )

        self._market_client = self._load_token(
            api_key=self.config.SCHWAB_MARKET_API_KEY,
            app_secret=self.config.SCHWAB_MARKET_APP_SECRET,
            token_path=Path(self.config.SCHWAB_MARKET_TOKEN_PATH).expanduser(),
            label="Market",
        )

        # Fall back: if only one is configured, use it for both
        if self._client and not self._market_client:
            self._market_client = self._client
            logger.info("No market credentials — using account client for market data")
        elif self._market_client and not self._client:
            self._client = self._market_client
            logger.info("No account credentials — using market client for account data")

        return self._client is not None or self._market_client is not None

    def enroll(self, which: str = "both") -> bool:
        """Interactive OAuth enrollment via browser.

        Args:
            which: "account", "market", or "both"
        """
        if which in ("account", "both"):
            self._client = self._enroll_interactive(
                api_key=self.config.SCHWAB_API_KEY,
                app_secret=self.config.SCHWAB_APP_SECRET,
                callback_url=self.config.SCHWAB_CALLBACK_URL,
                token_path=Path(self.config.SCHWAB_TOKEN_PATH).expanduser(),
                label="Account",
            )

        if which in ("market", "both"):
            self._market_client = self._enroll_interactive(
                api_key=self.config.SCHWAB_MARKET_API_KEY,
                app_secret=self.config.SCHWAB_MARKET_APP_SECRET,
                callback_url=self.config.SCHWAB_MARKET_CALLBACK_URL,
                token_path=Path(self.config.SCHWAB_MARKET_TOKEN_PATH).expanduser(),
                label="Market",
            )

        return self._client is not None or self._market_client is not None

    def _require_client(self) -> Any:
        if self._client is None:
            raise RuntimeError("Not authenticated — call authenticate() first")
        return self._client

    def _require_market_client(self) -> Any:
        if self._market_client is None:
            raise RuntimeError("Market client not authenticated — call authenticate() first")
        return self._market_client

    @staticmethod
    def _throttle(limiter: RateLimiter) -> None:
        """Block until a rate limit slot is available."""
        if not limiter.acquire(block=True, timeout=30.0):
            raise RuntimeError("API rate limit exceeded — could not acquire slot in 30s")

    # ── Account ───────────────────────────────────────────────────────────────

    def get_all_accounts(self) -> list[AccountSummary]:
        """Return a summary for every account linked to these credentials."""
        client = self._require_client()
        self._throttle(_account_limiter)
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
        self._throttle(_account_limiter)
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
            unsettled = float(balance.get("unsettledCash", 0))
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
                account_type=acct.get("type", ""),
                round_trips=int(acct.get("roundTrips", 0)),
                is_day_trader=bool(acct.get("isDayTrader", False)),
                is_closing_only=bool(acct.get("isClosingOnlyRestricted", False)),
                unsettled_cash=unsettled,
            )
        except Exception as e:
            logger.error("Failed to parse account: %s", e)
            return None

    # ── Quotes ────────────────────────────────────────────────────────────────

    def get_quotes(self, symbols: list[str]) -> dict[str, Quote]:
        """Fetch live quotes for a list of symbols."""
        if not symbols:
            return {}
        client = self._require_market_client()
        self._throttle(_market_limiter)
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
                fund = data.get("fundamental", {})

                bid = float(q.get("bidPrice", q.get("bid", 0)))
                ask = float(q.get("askPrice", q.get("ask", 0)))
                spread = ask - bid
                mid = (bid + ask) / 2 if (bid + ask) > 0 else 0.0
                spread_pct = (spread / mid * 100) if mid > 0 else 0.0

                quotes[symbol] = Quote(
                    symbol=symbol,
                    bid=bid,
                    ask=ask,
                    last=float(q.get("lastPrice", q.get("last", q.get("mark", 0)))),
                    volume=int(q.get("totalVolume", q.get("volume", 0))),
                    change_pct=float(q.get("netPercentChangeInDouble", q.get("percentChange", 0))),
                    spread=round(spread, 6),
                    spread_pct=round(spread_pct, 6),
                    avg_10d_volume=int(fund.get("avg10DaysVolume", 0)),
                    next_div_ex_date=str(fund.get("nextDivExDate", "") or ""),
                    next_div_amount=float(fund.get("nextDivAmount", 0.0)),
                    pe_ratio=float(fund.get("peRatio", 0.0)),
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
        client = self._require_market_client()
        self._throttle(_market_limiter)
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

    def get_intraday_ohlcv(
        self,
        symbol: str,
        interval_minutes: int = 3,
        days: int = 1,
    ) -> pd.DataFrame:
        """Fetch intraday OHLCV and resample to the requested interval.

        Pulls 1-minute bars from Schwab and aggregates to *interval_minutes*.
        Filters to regular session hours (9:30–16:00 ET) only.

        Returns DataFrame with columns: open, high, low, close, volume
        indexed by datetime (DatetimeIndex, US/Eastern).
        """
        client = self._require_market_client()
        self._throttle(_market_limiter)
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=days + 1)  # buffer for weekends

        try:
            resp = client.get_price_history_every_minute(
                symbol,
                start_datetime=start,
                end_datetime=end,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.error("get_intraday_ohlcv(%s) failed: %s", symbol, e)
            return pd.DataFrame(columns=["open", "high", "low", "close", "volume"])

        candles = data.get("candles", [])
        if not candles:
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

        # Convert to Eastern and filter regular session (9:30–16:00)
        df = df.tz_convert("US/Eastern")
        df = df.between_time("09:30", "15:59")

        if df.empty:
            return df

        # Resample to requested interval
        if interval_minutes > 1:
            df = df.resample(f"{interval_minutes}min").agg({
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
                "volume": "sum",
            }).dropna()

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
        self._throttle(_account_limiter)
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
        self._throttle(_account_limiter)
        try:
            resp = client.cancel_order(account_hash, order_id)
            resp.raise_for_status()
            logger.info("Order %s cancelled", order_id)
            return True
        except Exception as e:
            logger.error("cancel_order(%s) failed: %s", order_id, e)
            return False

    @staticmethod
    def rate_limit_stats() -> dict:
        """Return rate limiter stats for both API clients."""
        return {
            "account_api": _account_limiter.stats(),
            "market_api": _market_limiter.stats(),
        }

    def get_open_orders(self, account_hash: str) -> list[dict]:
        """Return all open orders for an account."""
        client = self._require_client()
        self._throttle(_account_limiter)
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
