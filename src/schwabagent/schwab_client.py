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
class Straddle:
    """ATM straddle candidate — matched call + put at same strike/expiry."""
    underlying: str
    underlying_price: float
    strike: float
    expiration: str      # YYYY-MM-DD
    dte: int
    call: "OptionContract"
    put: "OptionContract"

    @property
    def cost(self) -> float:
        """Combined ask (what you'd pay to open long straddle)."""
        return self.call.ask + self.put.ask

    @property
    def mid_cost(self) -> float:
        return (self.call.mark or (self.call.bid + self.call.ask) / 2) + \
               (self.put.mark or (self.put.bid + self.put.ask) / 2)

    @property
    def iv(self) -> float:
        """Average of call and put IV (Schwab returns percent)."""
        return (self.call.iv + self.put.iv) / 2

    @property
    def gamma(self) -> float:
        return self.call.gamma + self.put.gamma

    @property
    def gamma_per_dollar(self) -> float:
        """Gamma per $1 of extrinsic paid — higher = more ownership per dollar."""
        c = self.cost
        return self.gamma / c if c > 0 else 0.0


@dataclass
class OptionContract:
    """One option chain row."""
    symbol: str              # OSI symbol, e.g. "SPY   260515P00450000"
    underlying: str
    side: str                # "PUT" or "CALL"
    strike: float
    expiration: str          # YYYY-MM-DD
    dte: int
    bid: float
    ask: float
    mark: float
    delta: float             # signed (puts negative)
    gamma: float             # always ≥ 0 for long options
    iv: float                # implied volatility (percent, as schwab returns)
    open_interest: int
    volume: int


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

    # ── Multi-leg: covered call (buy-write) ──────────────────────────────────

    def place_buy_write(
        self,
        account_hash: str,
        stock_symbol: str,
        option_osi: str,
        contracts: int,
        stock_limit: float | None = None,
        call_limit: float | None = None,
        duration_child: str = "GOOD_TILL_CANCEL",
    ) -> dict:
        """Place a covered-call (buy-write) as a TRIGGER chain.

        Parent is an equity BUY LIMIT for `contracts * 100` shares. On fill,
        Schwab atomically triggers the child: an option SELL_TO_OPEN LIMIT
        for `contracts` contracts at `call_limit`. No leg risk — the option
        short never fires until the stock long is filled.

        Args:
            account_hash: Schwab account hash.
            stock_symbol: Underlying ticker (e.g. "KO").
            option_osi: OSI option symbol as returned in OptionContract.symbol
                        (schwab-py's `option_sell_to_open_limit` accepts it
                        as-is; no need to reformat via OptionSymbol builder).
            contracts: Number of option contracts (>=1). The equity leg is
                       sized at `contracts * 100` shares.
            stock_limit: Equity leg limit price. None → auto-compute from
                         a live quote using LIMIT_PRICE_BUFFER_BPS (same
                         buffered-BUY path as place_order).
            call_limit: Option leg limit price. Required — caller should
                        pass the opportunity's call_bid (conservative fill).
                        None is rejected; the screener already has bid/ask.
            duration_child: Option leg TIF. Defaults to GOOD_TILL_CANCEL so
                            the write outlives the equity fill day.

        Returns:
            On success:
              {status: "ok", parent_order_id, child_order_id: None,
               stock_symbol, option_symbol, contracts, shares, stock_limit,
               call_limit, duration_child}
            On failure:
              {status: "error", error: "<message>"}
        """
        client = self._require_client()

        if not account_hash:
            return {"status": "error", "error": "empty account_hash"}
        if not stock_symbol:
            return {"status": "error", "error": "empty stock_symbol"}
        if not option_osi:
            return {"status": "error", "error": "empty option_osi"}
        if contracts < 1:
            return {"status": "error", "error": f"contracts must be >=1: {contracts}"}
        if call_limit is None:
            return {"status": "error", "error": "call_limit required (pass opportunity call_bid)"}
        if float(call_limit) <= 0:
            return {"status": "error", "error": f"call_limit must be > 0: {call_limit}"}

        try:
            import schwab.orders.equities as eq  # type: ignore
            import schwab.orders.options as op  # type: ignore
            from schwab.orders.common import Duration, Session, first_triggers_second  # type: ignore
        except ImportError as e:
            logger.error("schwab.orders not available: %s", e)
            return {"status": "error", "error": "schwab.orders not available"}

        try:
            child_duration = Duration[duration_child.upper()]
        except KeyError:
            return {
                "status": "error",
                "error": f"Unknown duration_child: {duration_child} (valid: {[d.name for d in Duration]})",
            }

        # Resolve the equity leg limit — reuse the buffered-BUY path that
        # place_order uses so behavior stays consistent.
        if stock_limit is None:
            quote = (self.get_quotes([stock_symbol]) or {}).get(stock_symbol)
            if quote is None:
                return {"status": "error", "error": f"No quote for {stock_symbol} — cannot compute stock_limit"}
            stock_limit = _compute_limit_price(
                "BUY", quote, self.config.LIMIT_PRICE_BUFFER_BPS
            )
            if stock_limit is None:
                return {
                    "status": "error",
                    "error": f"No usable bid/ask/last for {stock_symbol} — cannot compute stock_limit",
                }

        shares = contracts * 100
        stock_price_str = f"{float(stock_limit):.2f}"
        call_price_str = f"{float(call_limit):.2f}"

        self._throttle(_account_limiter)
        try:
            parent = (
                eq.equity_buy_limit(stock_symbol, shares, stock_price_str)
                .set_duration(Duration.DAY)
                .set_session(Session.NORMAL)
            )
            child = (
                op.option_sell_to_open_limit(option_osi, contracts, call_price_str)
                .set_duration(child_duration)
                .set_session(Session.NORMAL)
            )
            order = first_triggers_second(parent, child).build()

            resp = client.place_order(account_hash, order)
            resp.raise_for_status()

            parent_order_id = resp.headers.get("Location", "").rstrip("/").split("/")[-1]
            logger.info(
                "Buy-write placed: BUY %d %s @ $%s → STO %d %s @ $%s (%s) parent_id=%s",
                shares, stock_symbol, stock_price_str,
                contracts, option_osi, call_price_str, duration_child.upper(),
                parent_order_id,
            )
            return {
                "status": "ok",
                "parent_order_id": parent_order_id,
                "child_order_id": None,  # surfaced by order_tracker after parent fills
                "stock_symbol": stock_symbol,
                "option_symbol": option_osi,
                "contracts": contracts,
                "shares": shares,
                "stock_limit": float(stock_price_str),
                "call_limit": float(call_price_str),
                "duration_child": duration_child.upper(),
            }
        except Exception as e:
            logger.error(
                "place_buy_write(%s %d / %s %d @ $%s) failed: %s",
                stock_symbol, shares, option_osi, contracts, call_price_str, e,
            )
            return {"status": "error", "error": str(e)}

    # ── Multi-leg: verticals (all 4 directions × open/close) ────────────────

    def place_vertical(
        self,
        account_hash: str,
        spread_type: str,
        action: str,
        long_osi: str,
        short_osi: str,
        quantity: int,
        net_price: float,
        duration: str = "DAY",
        session: str = "NORMAL",
    ) -> dict:
        """Place a 2-leg vertical spread via schwab-py's native VERTICAL builders.

        Dispatches over the 8 builders (bull_call / bear_call / bull_put /
        bear_put) × (open / close). The caller always passes
        `(long_osi, short_osi)`; this method swaps leg ordering internally
        when the underlying schwab-py builder expects `(short, long)` (bear
        variants). OrderType (NET_DEBIT vs NET_CREDIT) and
        ComplexOrderStrategyType.VERTICAL are set by the builder.

        Args:
            account_hash: Schwab account hash.
            spread_type: "bull_call", "bear_call", "bull_put", or "bear_put".
            action: "open" (BTO/STO) or "close" (STC/BTC).
            long_osi: OSI symbol for the long leg.
            short_osi: OSI symbol for the short leg.
            quantity: Number of spreads (contracts per leg).
            net_price: Net debit/credit per spread. Builder + spread_type
                determines which — caller passes the absolute value either way.
            duration: "DAY" or "GOOD_TILL_CANCEL".
            session: "NORMAL" or "SEAMLESS".

        Returns:
            On success: {status, order_id, spread_type, action, long_osi,
              short_osi, quantity, net_price, net_kind, duration, session}
              where net_kind is "NET_DEBIT" or "NET_CREDIT" per Schwab's
              chosen order type.
            On failure: {status: "error", error}.
        """
        client = self._require_client()

        if not account_hash:
            return {"status": "error", "error": "empty account_hash"}
        if not long_osi:
            return {"status": "error", "error": "empty long_osi"}
        if not short_osi:
            return {"status": "error", "error": "empty short_osi"}
        if quantity <= 0:
            return {"status": "error", "error": f"quantity must be > 0: {quantity}"}
        if net_price is None or float(net_price) <= 0:
            return {"status": "error", "error": f"net_price must be > 0: {net_price}"}

        spread_type = spread_type.lower()
        action = action.lower()
        valid_types = ("bull_call", "bear_call", "bull_put", "bear_put")
        valid_actions = ("open", "close")
        if spread_type not in valid_types:
            return {
                "status": "error",
                "error": f"Unknown spread_type: {spread_type} (valid: {valid_types})",
            }
        if action not in valid_actions:
            return {
                "status": "error",
                "error": f"Unknown action: {action} (valid: {valid_actions})",
            }

        try:
            import schwab.orders.options as op  # type: ignore
            from schwab.orders.common import Duration, Session  # type: ignore
        except ImportError as e:
            logger.error("schwab.orders not available: %s", e)
            return {"status": "error", "error": "schwab.orders not available"}

        try:
            duration_enum = Duration[duration.upper()]
        except KeyError:
            return {
                "status": "error",
                "error": f"Unknown duration: {duration} (valid: {[d.name for d in Duration]})",
            }
        try:
            session_enum = Session[session.upper()]
        except KeyError:
            return {
                "status": "error",
                "error": f"Unknown session: {session} (valid: {[s.name for s in Session]})",
            }

        # bull_* builders expect (long, short, qty, price); bear_* expect
        # (short, long, qty, price). The `swap` flag indicates bear ordering.
        builders = {
            ("bull_call", "open"):  (op.bull_call_vertical_open,  False),
            ("bull_call", "close"): (op.bull_call_vertical_close, False),
            ("bear_call", "open"):  (op.bear_call_vertical_open,  True),
            ("bear_call", "close"): (op.bear_call_vertical_close, True),
            ("bull_put",  "open"):  (op.bull_put_vertical_open,   False),
            ("bull_put",  "close"): (op.bull_put_vertical_close,  False),
            ("bear_put",  "open"):  (op.bear_put_vertical_open,   True),
            ("bear_put",  "close"): (op.bear_put_vertical_close,  True),
        }
        builder_fn, swap = builders[(spread_type, action)]
        first, second = (short_osi, long_osi) if swap else (long_osi, short_osi)
        price_str = f"{float(net_price):.2f}"

        self._throttle(_account_limiter)
        try:
            order = (
                builder_fn(first, second, quantity, price_str)
                .set_duration(duration_enum)
                .set_session(session_enum)
                .build()
            )
            resp = client.place_order(account_hash, order)
            resp.raise_for_status()
            order_id = resp.headers.get("Location", "").rstrip("/").split("/")[-1]
            net_kind = order.get("orderType", "")

            logger.info(
                "Vertical placed: %s %s %d x (long=%s, short=%s) @ %s $%s %s/%s (id=%s)",
                spread_type, action, quantity, long_osi, short_osi,
                net_kind, price_str, duration.upper(), session.upper(), order_id,
            )
            return {
                "status": "ok",
                "order_id": order_id,
                "spread_type": spread_type,
                "action": action,
                "long_osi": long_osi,
                "short_osi": short_osi,
                "quantity": quantity,
                "net_price": float(price_str),
                "net_kind": net_kind,
                "duration": duration.upper(),
                "session": session.upper(),
            }
        except Exception as e:
            logger.error(
                "place_vertical(%s %s %d x %s/%s @ $%s) failed: %s",
                spread_type, action, quantity, long_osi, short_osi, price_str, e,
            )
            return {"status": "error", "error": str(e)}

    # ── Multi-leg: iron condor (4-leg, open/close) ──────────────────────────

    def place_iron_condor(
        self,
        account_hash: str,
        action: str,
        long_put_osi: str,
        short_put_osi: str,
        short_call_osi: str,
        long_call_osi: str,
        quantity: int,
        net_price: float,
        duration: str = "DAY",
        session: str = "NORMAL",
    ) -> dict:
        """Place a 4-leg iron condor via a hand-built OrderBuilder.

        schwab-py ships no iron-condor constructor, so we assemble the
        OrderBuilder directly with ComplexOrderStrategyType.IRON_CONDOR.

        Leg mechanics:
            open (net credit):  BTO long_put, STO short_put,
                                STO short_call, BTO long_call
            close (net debit):  STC long_put, BTC short_put,
                                BTC short_call, STC long_call

        All four legs share `quantity`. Strike ordering must be
        long_put < short_put < short_call < long_call (the caller is
        responsible — not validated here).

        Args:
            account_hash: Schwab account hash.
            action: "open" or "close".
            long_put_osi: Farthest OTM put (wing).
            short_put_osi: Closer OTM put (body).
            short_call_osi: Closer OTM call (body).
            long_call_osi: Farthest OTM call (wing).
            quantity: Number of condors.
            net_price: Net credit (open) or debit (close), positive value.
            duration: "DAY" or "GOOD_TILL_CANCEL".
            session: "NORMAL" or "SEAMLESS".

        Returns:
            On success: {status, order_id, action, long_put_osi,
              short_put_osi, short_call_osi, long_call_osi, quantity,
              net_price, net_kind, duration, session}.
            On failure: {status: "error", error}.
        """
        client = self._require_client()

        if not account_hash:
            return {"status": "error", "error": "empty account_hash"}
        for name, val in (
            ("long_put_osi", long_put_osi),
            ("short_put_osi", short_put_osi),
            ("short_call_osi", short_call_osi),
            ("long_call_osi", long_call_osi),
        ):
            if not val:
                return {"status": "error", "error": f"empty {name}"}
        if quantity <= 0:
            return {"status": "error", "error": f"quantity must be > 0: {quantity}"}
        if net_price is None or float(net_price) <= 0:
            return {"status": "error", "error": f"net_price must be > 0: {net_price}"}

        action = action.lower()
        if action not in ("open", "close"):
            return {
                "status": "error",
                "error": f"Unknown action: {action} (valid: ('open', 'close'))",
            }

        try:
            from schwab.orders.generic import OrderBuilder  # type: ignore
            from schwab.orders.common import (  # type: ignore
                ComplexOrderStrategyType,
                Duration,
                OptionInstruction,
                OrderStrategyType,
                OrderType,
                Session,
            )
        except ImportError as e:
            logger.error("schwab.orders not available: %s", e)
            return {"status": "error", "error": "schwab.orders not available"}

        try:
            duration_enum = Duration[duration.upper()]
        except KeyError:
            return {
                "status": "error",
                "error": f"Unknown duration: {duration} (valid: {[d.name for d in Duration]})",
            }
        try:
            session_enum = Session[session.upper()]
        except KeyError:
            return {
                "status": "error",
                "error": f"Unknown session: {session} (valid: {[s.name for s in Session]})",
            }

        if action == "open":
            order_type = OrderType.NET_CREDIT
            legs = [
                (OptionInstruction.BUY_TO_OPEN,  long_put_osi),
                (OptionInstruction.SELL_TO_OPEN, short_put_osi),
                (OptionInstruction.SELL_TO_OPEN, short_call_osi),
                (OptionInstruction.BUY_TO_OPEN,  long_call_osi),
            ]
        else:
            order_type = OrderType.NET_DEBIT
            legs = [
                (OptionInstruction.SELL_TO_CLOSE, long_put_osi),
                (OptionInstruction.BUY_TO_CLOSE,  short_put_osi),
                (OptionInstruction.BUY_TO_CLOSE,  short_call_osi),
                (OptionInstruction.SELL_TO_CLOSE, long_call_osi),
            ]

        price_str = f"{float(net_price):.2f}"

        self._throttle(_account_limiter)
        try:
            builder = (
                OrderBuilder()
                .set_order_type(order_type)
                .set_complex_order_strategy_type(ComplexOrderStrategyType.IRON_CONDOR)
                .set_order_strategy_type(OrderStrategyType.SINGLE)
                .set_duration(duration_enum)
                .set_session(session_enum)
                .set_price(price_str)
                .set_quantity(quantity)
            )
            for instruction, osi in legs:
                builder.add_option_leg(instruction, osi, quantity)
            order = builder.build()

            resp = client.place_order(account_hash, order)
            resp.raise_for_status()
            order_id = resp.headers.get("Location", "").rstrip("/").split("/")[-1]

            logger.info(
                "Iron condor placed: %s %d x (LP=%s, SP=%s, SC=%s, LC=%s) "
                "@ %s $%s %s/%s (id=%s)",
                action, quantity, long_put_osi, short_put_osi,
                short_call_osi, long_call_osi,
                order_type.value, price_str, duration.upper(), session.upper(),
                order_id,
            )
            return {
                "status": "ok",
                "order_id": order_id,
                "action": action,
                "long_put_osi": long_put_osi,
                "short_put_osi": short_put_osi,
                "short_call_osi": short_call_osi,
                "long_call_osi": long_call_osi,
                "quantity": quantity,
                "net_price": float(price_str),
                "net_kind": order_type.value,
                "duration": duration.upper(),
                "session": session.upper(),
            }
        except Exception as e:
            logger.error(
                "place_iron_condor(%s %d x LP=%s/SP=%s/SC=%s/LC=%s @ $%s) failed: %s",
                action, quantity, long_put_osi, short_put_osi,
                short_call_osi, long_call_osi, price_str, e,
            )
            return {"status": "error", "error": str(e)}

    # ── Mutual funds ──────────────────────────────────────────────────────────

    def place_mutual_fund_order(
        self,
        account_hash: str,
        symbol: str,
        instruction: str,
        dollar_amount: float,
    ) -> dict:
        """Place a mutual fund market order.

        schwab-py has no mutual fund builder — we hand-roll the Schwab REST
        payload. For stable-NAV money funds (SWVXX, SNSXX, etc.) the
        "quantity" field is the dollar amount, which equals the share count
        because NAV is $1.00.

        Args:
            account_hash: Schwab account hash.
            symbol: Mutual fund ticker (e.g. "SWVXX").
            instruction: "BUY" or "SELL".
            dollar_amount: Dollars to buy/sell. Must be > 0.

        Returns:
            {status: "ok"|"error", order_id, symbol, instruction, dollar_amount}.
        """
        client = self._require_client()
        if not account_hash:
            return {"status": "error", "error": "empty account_hash"}
        inst = instruction.upper()
        if inst not in ("BUY", "SELL"):
            return {"status": "error", "error": f"Unknown instruction: {instruction}"}
        if dollar_amount <= 0:
            return {"status": "error", "error": f"dollar_amount must be > 0: {dollar_amount}"}

        # Schwab expects whole-dollar quantity for money market funds.
        quantity = round(float(dollar_amount), 2)
        order_spec = {
            "orderType": "MARKET",
            "session": "NORMAL",
            "duration": "DAY",
            "orderStrategyType": "SINGLE",
            "orderLegCollection": [
                {
                    "instruction": inst,
                    "quantity": quantity,
                    "instrument": {
                        "symbol": symbol.upper(),
                        "assetType": "MUTUAL_FUND",
                    },
                }
            ],
        }

        self._throttle(_account_limiter)
        try:
            resp = client.place_order(account_hash, order_spec)
            resp.raise_for_status()
            order_id = resp.headers.get("Location", "").rstrip("/").split("/")[-1]
            logger.info(
                "Mutual fund order placed: %s $%.2f %s (id=%s)",
                inst, quantity, symbol.upper(), order_id,
            )
            return {
                "status": "ok",
                "order_id": order_id,
                "symbol": symbol.upper(),
                "instruction": inst,
                "dollar_amount": quantity,
                "asset_type": "MUTUAL_FUND",
            }
        except Exception as e:
            logger.error(
                "place_mutual_fund_order(%s $%.2f %s) failed: %s",
                inst, quantity, symbol.upper(), e,
            )
            return {"status": "error", "error": str(e)}

    # ── Options ───────────────────────────────────────────────────────────────

    def get_option_chain(
        self,
        symbol: str,
        side: str,
        dte_min: int,
        dte_max: int,
        strike_count: int = 20,
    ) -> list[OptionContract]:
        """Fetch an option chain slice.

        Args:
            symbol: Underlying ticker (e.g. "SPY").
            side: "PUT" or "CALL".
            dte_min / dte_max: Expiration window in days from today.
            strike_count: Number of strikes around the money to return.

        Returns:
            Flat list of OptionContract rows sorted by (expiration, strike).
            Empty list on any error or if the chain returns no data.
        """
        client = self._require_market_client()
        try:
            from schwab.client import Client as _Cli  # type: ignore
            ct_enum = (
                _Cli.Options.ContractType.PUT
                if side.upper() == "PUT"
                else _Cli.Options.ContractType.CALL
            )
        except ImportError:
            logger.error("schwab-py not available for option chain")
            return []

        from_date = datetime.now(timezone.utc).date() + timedelta(days=dte_min)
        to_date = datetime.now(timezone.utc).date() + timedelta(days=dte_max)

        self._throttle(_market_limiter)
        try:
            resp = client.get_option_chain(
                symbol,
                contract_type=ct_enum,
                strike_count=strike_count,
                from_date=from_date,
                to_date=to_date,
            )
            resp.raise_for_status()
            data = resp.json() or {}
        except Exception as e:
            logger.error("get_option_chain(%s %s) failed: %s", symbol, side, e)
            return []

        map_key = "putExpDateMap" if side.upper() == "PUT" else "callExpDateMap"
        exp_map = data.get(map_key) or {}
        contracts: list[OptionContract] = []
        for exp_key, strikes in exp_map.items():
            # exp_key looks like "2026-05-15:31" (YYYY-MM-DD:DTE)
            exp_date = exp_key.split(":", 1)[0]
            for _strike, rows in strikes.items():
                if not rows:
                    continue
                row = rows[0]
                try:
                    contracts.append(OptionContract(
                        symbol=row.get("symbol", ""),
                        underlying=symbol.upper(),
                        side=side.upper(),
                        strike=float(row.get("strikePrice", 0)),
                        expiration=exp_date,
                        dte=int(row.get("daysToExpiration", 0)),
                        bid=float(row.get("bid", 0) or 0),
                        ask=float(row.get("ask", 0) or 0),
                        mark=float(row.get("mark", 0) or 0),
                        delta=float(row.get("delta", 0) or 0),
                        gamma=float(row.get("gamma", 0) or 0),
                        iv=float(row.get("volatility", 0) or 0),
                        open_interest=int(row.get("openInterest", 0) or 0),
                        volume=int(row.get("totalVolume", 0) or 0),
                    ))
                except (TypeError, ValueError):
                    continue
        contracts.sort(key=lambda c: (c.expiration, c.strike))
        return contracts

    def get_option_chain_all(
        self,
        symbol: str,
        dte_min: int,
        dte_max: int,
        strike_count: int = 20,
    ) -> list[OptionContract]:
        """Fetch calls AND puts in a single API call.

        Same as get_option_chain but omits the contract_type filter so
        Schwab returns both sides. Costs 1 API call instead of 2.
        """
        client = self._require_market_client()
        from_date = datetime.now(timezone.utc).date() + timedelta(days=dte_min)
        to_date = datetime.now(timezone.utc).date() + timedelta(days=dte_max)

        self._throttle(_market_limiter)
        try:
            resp = client.get_option_chain(
                symbol,
                strike_count=strike_count,
                from_date=from_date,
                to_date=to_date,
            )
            resp.raise_for_status()
            data = resp.json() or {}
        except Exception as e:
            logger.error("get_option_chain_all(%s) failed: %s", symbol, e)
            return []

        contracts: list[OptionContract] = []
        for side, map_key in (("CALL", "callExpDateMap"), ("PUT", "putExpDateMap")):
            exp_map = data.get(map_key) or {}
            for exp_key, strikes in exp_map.items():
                exp_date = exp_key.split(":", 1)[0]
                for _strike, rows in strikes.items():
                    if not rows:
                        continue
                    row = rows[0]
                    try:
                        contracts.append(OptionContract(
                            symbol=row.get("symbol", ""),
                            underlying=symbol.upper(),
                            side=side,
                            strike=float(row.get("strikePrice", 0)),
                            expiration=exp_date,
                            dte=int(row.get("daysToExpiration", 0)),
                            bid=float(row.get("bid", 0) or 0),
                            ask=float(row.get("ask", 0) or 0),
                            mark=float(row.get("mark", 0) or 0),
                            delta=float(row.get("delta", 0) or 0),
                            gamma=float(row.get("gamma", 0) or 0),
                            iv=float(row.get("volatility", 0) or 0),
                            open_interest=int(row.get("openInterest", 0) or 0),
                            volume=int(row.get("totalVolume", 0) or 0),
                        ))
                    except (TypeError, ValueError):
                        continue
        contracts.sort(key=lambda c: (c.expiration, c.strike))
        return contracts

    def get_atm_straddles(
        self,
        symbol: str,
        dte_min: int,
        dte_max: int,
    ) -> list[Straddle]:
        """Return one ATM straddle per expiry in the DTE window.

        Fetches a single chain (both puts and calls) for the window and,
        for each expiration, pairs the call and put at the strike nearest
        the underlying price into a Straddle row.
        """
        client = self._require_market_client()
        from_date = datetime.now(timezone.utc).date() + timedelta(days=dte_min)
        to_date = datetime.now(timezone.utc).date() + timedelta(days=dte_max)

        self._throttle(_market_limiter)
        try:
            resp = client.get_option_chain(
                symbol,
                strike_count=10,  # ~5 strikes each side of ATM is plenty
                from_date=from_date,
                to_date=to_date,
            )
            resp.raise_for_status()
            data = resp.json() or {}
        except Exception as e:
            logger.error("get_atm_straddles(%s) failed: %s", symbol, e)
            return []

        underlying_price = float(data.get("underlyingPrice") or 0)
        if underlying_price <= 0:
            return []

        put_map = data.get("putExpDateMap") or {}
        call_map = data.get("callExpDateMap") or {}

        def _build_contract(row: dict, side: str, exp: str) -> OptionContract | None:
            try:
                return OptionContract(
                    symbol=row.get("symbol", ""),
                    underlying=symbol.upper(),
                    side=side,
                    strike=float(row.get("strikePrice", 0)),
                    expiration=exp,
                    dte=int(row.get("daysToExpiration", 0)),
                    bid=float(row.get("bid", 0) or 0),
                    ask=float(row.get("ask", 0) or 0),
                    mark=float(row.get("mark", 0) or 0),
                    delta=float(row.get("delta", 0) or 0),
                    gamma=float(row.get("gamma", 0) or 0),
                    iv=float(row.get("volatility", 0) or 0),
                    open_interest=int(row.get("openInterest", 0) or 0),
                    volume=int(row.get("totalVolume", 0) or 0),
                )
            except (TypeError, ValueError):
                return None

        straddles: list[Straddle] = []
        for exp_key, put_strikes in put_map.items():
            call_strikes = call_map.get(exp_key) or {}
            if not call_strikes:
                continue
            exp_date = exp_key.split(":", 1)[0]
            # Shared strikes between both sides
            common = set(put_strikes.keys()) & set(call_strikes.keys())
            if not common:
                continue
            # Pick the strike closest to the underlying price
            best = min(common, key=lambda s: abs(float(s) - underlying_price))
            put_row = (put_strikes[best] or [None])[0]
            call_row = (call_strikes[best] or [None])[0]
            if put_row is None or call_row is None:
                continue
            put_c = _build_contract(put_row, "PUT", exp_date)
            call_c = _build_contract(call_row, "CALL", exp_date)
            if put_c is None or call_c is None:
                continue
            straddles.append(Straddle(
                underlying=symbol.upper(),
                underlying_price=underlying_price,
                strike=float(best),
                expiration=exp_date,
                dte=int(put_row.get("daysToExpiration", 0)),
                call=call_c,
                put=put_c,
            ))

        straddles.sort(key=lambda s: s.expiration)
        return straddles

    def place_option_order(
        self,
        account_hash: str,
        osi_symbol: str,
        instruction: str,
        quantity: int,
        limit_price: float,
        duration: str | None = None,
    ) -> dict:
        """Place a single-leg option order.

        Args:
            account_hash: Schwab account hash.
            osi_symbol: Full OSI symbol (e.g. "SPY   260515P00450000").
            instruction: One of SELL_TO_OPEN, BUY_TO_CLOSE, BUY_TO_OPEN, SELL_TO_CLOSE.
            quantity: Number of contracts.
            limit_price: Limit credit/debit per contract (required — no market orders for v1).
            duration: "DAY" or "GOOD_TILL_CANCEL"; None → config default.

        Returns:
            Dict shaped like place_order: {status: "ok"|"error", order_id, ...}.
        """
        client = self._require_client()
        if not account_hash:
            return {"status": "error", "error": "empty account_hash"}
        if quantity <= 0:
            return {"status": "error", "error": f"Invalid quantity: {quantity}"}
        if limit_price is None or limit_price <= 0:
            return {"status": "error", "error": f"limit_price required: got {limit_price}"}

        try:
            import schwab.orders.options as opt  # type: ignore
            from schwab.orders.common import Duration  # type: ignore
        except ImportError:
            return {"status": "error", "error": "schwab.orders.options not available"}

        builders = {
            "SELL_TO_OPEN": opt.option_sell_to_open_limit,
            "BUY_TO_CLOSE": opt.option_buy_to_close_limit,
            "BUY_TO_OPEN": opt.option_buy_to_open_limit,
            "SELL_TO_CLOSE": opt.option_sell_to_close_limit,
        }
        inst = instruction.upper()
        builder_fn = builders.get(inst)
        if builder_fn is None:
            return {"status": "error", "error": f"Unknown instruction: {instruction}"}

        resolved_duration = (duration or self.config.ORDER_DURATION or "DAY").upper()
        try:
            duration_enum = Duration[resolved_duration]
        except KeyError:
            return {"status": "error", "error": f"Unknown duration: {resolved_duration}"}

        price_str = f"{float(limit_price):.2f}"

        self._throttle(_account_limiter)
        try:
            order = (
                builder_fn(osi_symbol, quantity, price_str)
                .set_duration(duration_enum)
                .build()
            )
            resp = client.place_order(account_hash, order)
            resp.raise_for_status()
            order_id = resp.headers.get("Location", "").rstrip("/").split("/")[-1]
            logger.info(
                "Option order placed: %s %d %s @ $%s (id=%s)",
                inst, quantity, osi_symbol, price_str, order_id,
            )
            return {
                "status": "ok",
                "order_id": order_id,
                "osi_symbol": osi_symbol,
                "instruction": inst,
                "quantity": quantity,
                "limit_price": float(price_str),
                "duration": resolved_duration,
            }
        except Exception as e:
            logger.error(
                "place_option_order(%s %d %s @ $%s) failed: %s",
                inst, quantity, osi_symbol, price_str, e,
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
