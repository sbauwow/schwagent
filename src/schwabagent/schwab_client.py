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


def _compute_limit_price(side: str, quote: Any, buffer_bps: float) -> float | None:
    """Compute an aggressive limit price from a Quote using a basis-point buffer.

    BUY:  base = ask (or last if ask missing), limit = base * (1 + bps/10000)
    SELL: base = bid (or last if bid missing), limit = base * (1 - bps/10000)

    The "aggressive" direction (above ask / below bid) is deliberate — it's
    the price we're willing to *cross* the spread at, giving a higher fill
    probability than sitting on the near side. A positive buffer on a BUY
    order accepts paying more; on a SELL it accepts receiving less.

    Returns None when no usable price is available (thin symbol with zero
    bid/ask/last), so the caller can surface a clean error instead of
    submitting a $0 limit.

    Prices are rounded to 2 decimals (penny tick for US equities).
    """
    ask = float(getattr(quote, "ask", 0) or 0)
    bid = float(getattr(quote, "bid", 0) or 0)
    last = float(getattr(quote, "last", 0) or 0)
    direction = side.upper()

    if direction == "BUY":
        base = ask if ask > 0 else last
        if base <= 0:
            return None
        return round(base * (1 + buffer_bps / 10000.0), 2)

    if direction == "SELL":
        base = bid if bid > 0 else last
        if base <= 0:
            return None
        return round(base * (1 - buffer_bps / 10000.0), 2)

    return None


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
        """Return a summary for every account linked to these credentials.

        Each summary carries the real Schwab account hash (required by every
        order endpoint). Hashes come from a separate `/accounts/accountNumbers`
        endpoint — the balances endpoint only returns account numbers, not
        hashes, so we pair the two here.
        """
        client = self._require_client()

        hash_map = self._get_account_hash_map()
        if not hash_map:
            return []

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
            number = str(acct.get("accountNumber", ""))
            hv = hash_map.get(number, "")
            if not hv:
                logger.warning("No hash found for account %s — order endpoints will fail", number[-4:] if number else "?")
            summary = self._parse_account(acct, account_hash=hv)
            if summary:
                summaries.append(summary)
        return summaries

    def _get_account_hash_map(self) -> dict[str, str]:
        """Fetch the `{account_number: hash_value}` map from Schwab.

        Cached for the process lifetime — account hashes don't change unless
        the user links/unlinks an account at Schwab. Reset by clearing
        `self._account_hash_map`.
        """
        cached = getattr(self, "_account_hash_map", None)
        if cached:
            return cached

        client = self._require_client()
        self._throttle(_account_limiter)
        try:
            resp = client.get_account_numbers()
            resp.raise_for_status()
            mapping: dict[str, str] = {}
            for m in resp.json() or []:
                num = m.get("accountNumber")
                hv = m.get("hashValue")
                if num and hv:
                    mapping[str(num)] = str(hv)
            self._account_hash_map = mapping
            return mapping
        except Exception as e:
            logger.error("get_account_numbers failed: %s", e)
            return {}

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
        order_type: str | None = None,
        limit_price: float | str | None = None,
        duration: str | None = None,
        session: str | None = None,
    ) -> dict:
        """Place an equity order.

        Resolution order when optional args are not passed:
          1. `order_type` falls back to `config.ORDER_TYPE` (default "LIMIT").
          2. If the resolved type is "LIMIT" and `limit_price` is None, fetch
             a fresh quote and compute a buffered price via
             `_compute_limit_price(side, quote, config.LIMIT_PRICE_BUFFER_BPS)`.
          3. `duration` falls back to `config.ORDER_DURATION` (default "DAY").
          4. `session` falls back to `config.ORDER_SESSION` (default "NORMAL").

        Args:
            account_hash: Schwab account hash (from AccountSummary.account_hash).
            symbol: Ticker symbol.
            side: "BUY" or "SELL".
            quantity: Number of shares.
            order_type: "MARKET", "LIMIT", or None to read config.ORDER_TYPE.
            limit_price: Explicit limit price. If None and order_type resolves
                         to "LIMIT", the price is auto-computed from a quote.
            duration: "DAY", "GOOD_TILL_CANCEL", or None to read config.
            session: "NORMAL", "SEAMLESS", or None to read config. SEAMLESS
                     covers pre-market + regular + post-market (04:00-20:00 ET).

        Returns:
            Dict with order details and status. On success:
              {status: "ok", order_id, symbol, side, quantity, order_type,
               limit_price, duration, session}
            On failure:
              {status: "error", error: "<message>"}
        """
        client = self._require_client()

        if not account_hash:
            return {"status": "error", "error": "empty account_hash (use SchwabClient.get_all_accounts to populate)"}
        if quantity <= 0:
            return {"status": "error", "error": f"Invalid quantity: {quantity}"}

        try:
            import schwab.orders.equities as eq  # type: ignore
            from schwab.orders.common import Duration, Session  # type: ignore
        except ImportError:
            logger.error("schwab.orders not available")
            return {"status": "error", "error": "schwab.orders not available"}

        side_upper = side.upper()
        if side_upper not in ("BUY", "SELL"):
            return {"status": "error", "error": f"Unknown side: {side}"}

        # Resolve order type from config when the caller doesn't pin it.
        resolved_type = (order_type or self.config.ORDER_TYPE or "LIMIT").upper()
        if resolved_type not in ("MARKET", "LIMIT"):
            return {"status": "error", "error": f"Unknown order_type: {resolved_type}"}

        # Resolve duration + session from config. Look up the enum by name so
        # an invalid value raises a clean KeyError we can convert to an error
        # response instead of a cryptic schwab-py failure later.
        resolved_duration = (duration or self.config.ORDER_DURATION or "DAY").upper()
        resolved_session = (session or self.config.ORDER_SESSION or "NORMAL").upper()
        try:
            duration_enum = Duration[resolved_duration]
        except KeyError:
            return {
                "status": "error",
                "error": f"Unknown duration: {resolved_duration} (valid: {[d.name for d in Duration]})",
            }
        try:
            session_enum = Session[resolved_session]
        except KeyError:
            return {
                "status": "error",
                "error": f"Unknown session: {resolved_session} (valid: {[s.name for s in Session]})",
            }

        # Auto-compute limit price from a live quote when LIMIT + no explicit price.
        if resolved_type == "LIMIT" and limit_price is None:
            quote = (self.get_quotes([symbol]) or {}).get(symbol)
            if quote is None:
                return {"status": "error", "error": f"No quote for {symbol} — cannot compute limit price"}
            limit_price = _compute_limit_price(
                side_upper, quote, self.config.LIMIT_PRICE_BUFFER_BPS
            )
            if limit_price is None:
                return {
                    "status": "error",
                    "error": f"No usable bid/ask/last for {symbol} — cannot compute limit price",
                }

        # schwab-py deprecated float prices — pass everything as string.
        price_str = f"{float(limit_price):.2f}" if limit_price is not None else None

        self._throttle(_account_limiter)
        try:
            if resolved_type == "MARKET":
                builder_fn = eq.equity_buy_market if side_upper == "BUY" else eq.equity_sell_market
                builder = builder_fn(symbol, quantity)
            else:  # LIMIT
                builder_fn = eq.equity_buy_limit if side_upper == "BUY" else eq.equity_sell_limit
                builder = builder_fn(symbol, quantity, price_str)

            order = (
                builder
                .set_duration(duration_enum)
                .set_session(session_enum)
                .build()
            )

            resp = client.place_order(account_hash, order)
            resp.raise_for_status()

            # Extract order ID from Location header (may include trailing slash)
            order_id = resp.headers.get("Location", "").rstrip("/").split("/")[-1]
            logger.info(
                "Order placed: %s %d %s %s%s %s/%s (id=%s)",
                side_upper, quantity, symbol, resolved_type,
                f" @ ${price_str}" if price_str else "",
                resolved_duration, resolved_session,
                order_id,
            )
            return {
                "status": "ok",
                "order_id": order_id,
                "symbol": symbol,
                "side": side_upper,
                "quantity": quantity,
                "order_type": resolved_type,
                "limit_price": float(price_str) if price_str else None,
                "duration": resolved_duration,
                "session": resolved_session,
            }
        except Exception as e:
            logger.error(
                "place_order(%s %d %s %s%s %s/%s) failed: %s",
                side_upper, quantity, symbol, resolved_type,
                f" @ ${price_str}" if price_str else "",
                resolved_duration, resolved_session, e,
            )
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

    # Statuses that mean "order is still live and could fill" — anything not
    # in this set is either terminal (FILLED, CANCELED, EXPIRED, REJECTED)
    # or a transient state we don't treat as open.
    _OPEN_ORDER_STATUSES = frozenset({
        "PENDING_ACTIVATION",
        "WORKING",
        "AWAITING_PARENT_ORDER",
        "AWAITING_CONDITION",
        "AWAITING_MANUAL_REVIEW",
        "AWAITING_UR_OUT",
        "AWAITING_RELEASE_TIME",
        "AWAITING_STOP_CONDITION",
        "QUEUED",
        "ACCEPTED",
        "NEW",
    })

    def get_open_orders(self, account_hash: str) -> list[dict]:
        """Return all open (non-terminal) orders for an account.

        Fetches every order entered in the last 24h and filters client-side
        for statuses that mean "still live." We avoid the server-side
        `status=` filter because schwab-py's strict enum mode rejects string
        values and the enum path changes between versions.
        """
        client = self._require_client()
        self._throttle(_account_limiter)
        try:
            resp = client.get_orders_for_account(
                account_hash,
                from_entered_datetime=datetime.now(timezone.utc) - timedelta(days=1),
                to_entered_datetime=datetime.now(timezone.utc),
            )
            resp.raise_for_status()
            orders = resp.json() or []
            return [o for o in orders if o.get("status") in self._OPEN_ORDER_STATUSES]
        except Exception as e:
            logger.error("get_open_orders(%s) failed: %s", account_hash[:8], e)
            return []
