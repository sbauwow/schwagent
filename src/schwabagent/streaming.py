"""Real-time WebSocket streaming from Schwab.

Provides tick-by-tick price updates and account activity (order fills)
via Schwab's streaming API. Replaces REST polling for the scalp strategy.

Architecture:
- Runs in a background asyncio thread
- Callbacks fire on price updates and account activity
- Thread-safe price cache accessible from the main trading loop
- Auto-reconnects on disconnection

Usage:
    stream = StreamManager(config, schwab_client)
    stream.subscribe_quotes(["SPY", "QQQ"], on_quote=my_handler)
    stream.subscribe_account_activity(on_fill=my_fill_handler)
    stream.start()
    ...
    latest = stream.get_price("SPY")  # thread-safe
    stream.stop()
"""
from __future__ import annotations

import asyncio
import logging
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable

from schwabagent.config import Config

logger = logging.getLogger(__name__)


@dataclass
class LiveQuote:
    """Real-time quote from streaming."""
    symbol: str
    bid: float = 0.0
    ask: float = 0.0
    last: float = 0.0
    bid_size: int = 0
    ask_size: int = 0
    volume: int = 0
    high: float = 0.0
    low: float = 0.0
    open: float = 0.0
    close: float = 0.0
    net_change: float = 0.0
    net_change_pct: float = 0.0
    mark: float = 0.0
    timestamp: float = 0.0  # epoch ms

    @property
    def mid(self) -> float:
        if self.bid > 0 and self.ask > 0:
            return (self.bid + self.ask) / 2
        return self.last

    @property
    def spread(self) -> float:
        if self.bid > 0 and self.ask > 0:
            return self.ask - self.bid
        return 0.0


@dataclass
class OrderFill:
    """Account activity event — order fill."""
    account: str
    message_type: str
    data: dict = field(default_factory=dict)
    timestamp: float = 0.0


QuoteCallback = Callable[[LiveQuote], None]
FillCallback = Callable[[OrderFill], None]


class StreamManager:
    """Manages the Schwab WebSocket streaming connection."""

    def __init__(self, config: Config, schwab_client: Any):
        self._config = config
        self._schwab_client = schwab_client
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._stream_client: Any = None
        self._running = False

        # Thread-safe price cache
        self._prices: dict[str, LiveQuote] = {}
        self._price_lock = threading.Lock()

        # Callbacks
        self._quote_callbacks: list[QuoteCallback] = []
        self._fill_callbacks: list[FillCallback] = []

        # Subscriptions
        self._quote_symbols: set[str] = set()
        self._account_activity: bool = False

        # Stats
        self._ticks_received = 0
        self._fills_received = 0
        self._connected_at: float = 0
        self._reconnects = 0

    # ── Subscription setup (call before start) ───────────────────────────

    def subscribe_quotes(self, symbols: list[str], on_quote: QuoteCallback | None = None) -> None:
        """Subscribe to real-time quotes for symbols."""
        self._quote_symbols.update(s.upper() for s in symbols)
        if on_quote:
            self._quote_callbacks.append(on_quote)

    def subscribe_account_activity(self, on_fill: FillCallback | None = None) -> None:
        """Subscribe to account activity (order fills, etc.)."""
        self._account_activity = True
        if on_fill:
            self._fill_callbacks.append(on_fill)

    # ── Thread-safe price access ─────────────────────────────────────────

    def get_price(self, symbol: str) -> LiveQuote | None:
        """Get the latest streamed price for a symbol. Thread-safe."""
        with self._price_lock:
            return self._prices.get(symbol.upper())

    def get_all_prices(self) -> dict[str, LiveQuote]:
        """Get all cached prices. Thread-safe."""
        with self._price_lock:
            return dict(self._prices)

    # ── Lifecycle ────────────────────────────────────────────────────────

    def start(self) -> None:
        """Start the streaming connection in a background thread."""
        if self._running:
            return
        if not self._quote_symbols and not self._account_activity:
            logger.warning("No streaming subscriptions — nothing to stream")
            return

        self._running = True
        self._thread = threading.Thread(
            target=self._run_loop, daemon=True, name="schwab-stream",
        )
        self._thread.start()
        logger.info(
            "Streaming started: %d symbols, account_activity=%s",
            len(self._quote_symbols), self._account_activity,
        )

    def stop(self) -> None:
        """Stop the streaming connection."""
        self._running = False
        if self._loop and not self._loop.is_closed():
            self._loop.call_soon_threadsafe(self._loop.stop)
        if self._thread:
            self._thread.join(timeout=10)
        logger.info(
            "Streaming stopped: %d ticks, %d fills, %d reconnects",
            self._ticks_received, self._fills_received, self._reconnects,
        )

    def _run_loop(self) -> None:
        """Background thread entry — runs asyncio event loop with reconnection."""
        while self._running:
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
            try:
                self._loop.run_until_complete(self._connect_and_stream())
            except Exception as e:
                logger.error("Stream disconnected: %s", e)
            finally:
                self._loop.close()

            if self._running:
                self._reconnects += 1
                wait = min(5 * self._reconnects, 60)
                logger.info("Reconnecting in %ds (attempt %d)...", wait, self._reconnects)
                time.sleep(wait)

    async def _connect_and_stream(self) -> None:
        """Connect to Schwab streaming and process messages."""
        from schwab.streaming import StreamClient

        # Use the account client for streaming
        client = self._schwab_client._client
        if client is None:
            logger.error("No authenticated client for streaming")
            return

        self._stream_client = StreamClient(client)

        # Register handlers
        if self._quote_symbols:
            self._stream_client.add_level_one_equity_handler(self._handle_quote)

        if self._account_activity:
            self._stream_client.add_account_activity_handler(self._handle_activity)

        # Login
        await self._stream_client.login()
        self._connected_at = time.time()
        self._reconnects = 0
        logger.info("Stream connected")

        # Subscribe to quotes
        if self._quote_symbols:
            fields = StreamClient.LevelOneEquityFields
            await self._stream_client.level_one_equity_subs(
                list(self._quote_symbols),
                fields=[
                    fields.SYMBOL, fields.BID_PRICE, fields.ASK_PRICE,
                    fields.LAST_PRICE, fields.BID_SIZE, fields.ASK_SIZE,
                    fields.TOTAL_VOLUME, fields.HIGH_PRICE, fields.LOW_PRICE,
                    fields.OPEN_PRICE, fields.CLOSE_PRICE, fields.NET_CHANGE,
                    fields.NET_CHANGE_PERCENT, fields.MARK,
                    fields.QUOTE_TIME_MILLIS,
                ],
            )
            logger.info("Subscribed to quotes: %s", sorted(self._quote_symbols))

        # Subscribe to account activity
        if self._account_activity:
            await self._stream_client.account_activity_sub()
            logger.info("Subscribed to account activity")

        # Message loop
        while self._running:
            await self._stream_client.handle_message()

    # ── Message handlers ─────────────────────────────────────────────────

    def _handle_quote(self, msg: dict) -> None:
        """Handle a level-1 equity quote update."""
        for content in msg.get("content", []):
            symbol = content.get("key", content.get("SYMBOL", ""))
            if not symbol:
                continue

            quote = LiveQuote(
                symbol=symbol,
                bid=float(content.get("BID_PRICE", content.get("1", 0))),
                ask=float(content.get("ASK_PRICE", content.get("2", 0))),
                last=float(content.get("LAST_PRICE", content.get("3", 0))),
                bid_size=int(content.get("BID_SIZE", content.get("4", 0))),
                ask_size=int(content.get("ASK_SIZE", content.get("5", 0))),
                volume=int(content.get("TOTAL_VOLUME", content.get("8", 0))),
                high=float(content.get("HIGH_PRICE", content.get("12", 0))),
                low=float(content.get("LOW_PRICE", content.get("13", 0))),
                open=float(content.get("OPEN_PRICE", content.get("17", 0))),
                close=float(content.get("CLOSE_PRICE", content.get("15", 0))),
                net_change=float(content.get("NET_CHANGE", content.get("18", 0))),
                net_change_pct=float(content.get("NET_CHANGE_PERCENT", content.get("42", 0))),
                mark=float(content.get("MARK", content.get("37", 0))),
                timestamp=float(content.get("QUOTE_TIME_MILLIS", content.get("38", 0))),
            )

            # Update cache
            with self._price_lock:
                self._prices[symbol] = quote

            self._ticks_received += 1

            # Fire callbacks
            for cb in self._quote_callbacks:
                try:
                    cb(quote)
                except Exception as e:
                    logger.error("Quote callback error for %s: %s", symbol, e)

    def _handle_activity(self, msg: dict) -> None:
        """Handle account activity (order fills, cancels, etc.)."""
        for content in msg.get("content", []):
            fill = OrderFill(
                account=content.get("ACCOUNT", content.get("1", "")),
                message_type=content.get("MESSAGE_TYPE", content.get("2", "")),
                data=content.get("MESSAGE_DATA", content.get("3", {})),
                timestamp=time.time(),
            )

            self._fills_received += 1
            logger.info(
                "Account activity: type=%s account=%s",
                fill.message_type, fill.account[:8] if fill.account else "?",
            )

            for cb in self._fill_callbacks:
                try:
                    cb(fill)
                except Exception as e:
                    logger.error("Fill callback error: %s", e)

    # ── Dynamic subscription management ──────────────────────────────────

    async def _add_symbols(self, symbols: list[str]) -> None:
        """Add symbols to the live subscription (while connected)."""
        if not self._stream_client:
            return
        from schwab.streaming import StreamClient
        fields = StreamClient.LevelOneEquityFields
        await self._stream_client.level_one_equity_add(
            symbols,
            fields=[
                fields.SYMBOL, fields.BID_PRICE, fields.ASK_PRICE,
                fields.LAST_PRICE, fields.TOTAL_VOLUME, fields.MARK,
                fields.QUOTE_TIME_MILLIS,
            ],
        )
        self._quote_symbols.update(s.upper() for s in symbols)

    def add_symbols(self, symbols: list[str]) -> None:
        """Thread-safe: add symbols to streaming subscription."""
        if self._loop and self._running:
            asyncio.run_coroutine_threadsafe(
                self._add_symbols(symbols), self._loop,
            )

    # ── Status ───────────────────────────────────────────────────────────

    def status(self) -> dict:
        uptime = time.time() - self._connected_at if self._connected_at else 0
        return {
            "running": self._running,
            "connected": self._connected_at > 0,
            "uptime_seconds": round(uptime, 0),
            "symbols": sorted(self._quote_symbols),
            "ticks_received": self._ticks_received,
            "fills_received": self._fills_received,
            "reconnects": self._reconnects,
            "cached_prices": len(self._prices),
        }
