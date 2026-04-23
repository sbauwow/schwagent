"""Telegram integration — alerts, trade approval, and commands.

Provides:
- Push alerts for trades, kill switch, daily P&L, errors
- Interactive trade approval with inline buttons (approve/reject)
- Dynamic bot commands registered via register_command()
"""
from __future__ import annotations

import asyncio
import logging
import re
import threading
from datetime import datetime, timezone
from typing import Any, Callable

from schwabagent.config import Config

logger = logging.getLogger(__name__)

# Approval state: trade_id → asyncio.Future
_pending_approvals: dict[str, asyncio.Future] = {}
_bot_loop: asyncio.AbstractEventLoop | None = None


def _render_approval_text(trade: dict, timeout: int) -> str:
    """Render the approval message body for a trade dict.

    Dispatch on `trade["trade_type"]`:
      - "buy_write" → covered-call two-leg proposal (BUY stock + STO call)
      - default      → single-leg equity order (BUY/SELL SYM qty @ price)

    Free function so it can be unit-tested without a running event loop.
    """
    strategy = trade.get("strategy", "?")
    trade_type = (trade.get("trade_type") or "").lower()

    if trade_type == "buy_write":
        symbol = trade.get("symbol", "?")
        contracts = int(trade.get("contracts") or 1)
        shares = contracts * 100
        stock_price = float(trade.get("stock_limit") or trade.get("price") or 0.0)
        strike = trade.get("strike", "?")
        expiration = trade.get("expiration", "?")
        call_limit = float(trade.get("call_limit") or trade.get("call_bid") or 0.0)
        premium_total = call_limit * 100 * contracts
        net_debit = stock_price * shares - premium_total
        max_profit = (float(strike) - stock_price + call_limit) * 100 * contracts if isinstance(strike, (int, float)) else None
        ann_if_called = float(trade.get("if_called_yield_pct") or 0.0)
        div_in_hold = float(trade.get("dividend_capture") or 0.0) * 100 * contracts

        lines = [
            f"*Buy-Write proposal · {_escape_md(symbol)}*",
            f"Strategy: `{_escape_md(strategy)}`",
            f"  BUY {shares} {_escape_md(symbol)} @ LIMIT ${stock_price:,.2f}",
            f"  STO  {contracts} {_escape_md(symbol)} {_escape_md(str(expiration))} ${strike} CALL @ LIMIT ${call_limit:,.2f}",
            f"Net debit: ${net_debit:,.2f}",
        ]
        if max_profit is not None:
            lines.append(f"Max profit \\(if called\\): ${max_profit:,.2f}")
        if ann_if_called:
            lines.append(f"Annualized if\\-called: {ann_if_called:.1f}%")
        if div_in_hold:
            lines.append(f"Div in hold: ${div_in_hold:,.2f}")
        lines.append("")
        lines.append(f"_Expires in {timeout}s_")
        return "\n".join(lines)

    # Single-leg default
    side = trade.get("side", "?")
    symbol = trade.get("symbol", "?")
    qty = int(trade.get("quantity") or 0)
    price = float(trade.get("price") or 0.0)
    value = qty * price
    return (
        f"*Trade Approval Required*\n\n"
        f"*{_escape_md(side)} {_escape_md(symbol)}*\n"
        f"Strategy: `{_escape_md(strategy)}`\n"
        f"Qty: {qty} @ ${price:,.2f}\n"
        f"Value: ${value:,.2f}\n\n"
        f"_Expires in {timeout}s_"
    )


def _escape_md(text: str) -> str:
    """Escape MarkdownV2 special characters, preserving *bold* and `code`."""
    # Protect bold markers and code blocks
    protected: list[tuple[str, str]] = []
    counter = 0

    def _protect(match: re.Match) -> str:
        nonlocal counter
        key = f"\x00PROT{counter}\x00"
        protected.append((key, match.group(0)))
        counter += 1
        return key

    # Protect ```code blocks```, `inline code`, and *bold*
    text = re.sub(r"```[\s\S]*?```", _protect, text)
    text = re.sub(r"`[^`]+`", _protect, text)
    text = re.sub(r"\*[^*]+\*", _protect, text)

    # Escape everything else
    for ch in r"_[]()~>#+-=|{}.!":
        text = text.replace(ch, f"\\{ch}")

    # Restore protected blocks
    for key, original in protected:
        text = text.replace(key, original)

    return text


class TelegramBot:
    """Schwab agent Telegram bot — runs in a background thread."""

    def __init__(self, config: Config):
        self.config = config
        self._app = None
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        # name -> (handler(args) -> markdown_v2_text, menu_description)
        self._command_handlers: dict[str, tuple[Callable[[list[str]], str], str]] = {}

    def start(self) -> None:
        """Start the bot in a background thread."""
        if not self.config.TELEGRAM_ENABLED:
            return
        if not self.config.TELEGRAM_BOT_TOKEN:
            logger.warning("TELEGRAM_ENABLED=true but TELEGRAM_BOT_TOKEN not set")
            return

        self._thread = threading.Thread(target=self._run, daemon=True, name="telegram-bot")
        self._thread.start()
        logger.info("Telegram bot started in background thread")

    def _run(self) -> None:
        """Entry point for the background thread."""
        global _bot_loop
        self._loop = asyncio.new_event_loop()
        _bot_loop = self._loop
        asyncio.set_event_loop(self._loop)
        self._loop.run_until_complete(self._start_polling())

    async def _start_polling(self) -> None:
        from telegram import BotCommand
        from telegram.ext import (
            Application,
            CallbackQueryHandler,
            CommandHandler,
        )

        self._app = (
            Application.builder()
            .token(self.config.TELEGRAM_BOT_TOKEN)
            .build()
        )

        # Static commands (handled inline, no runner dependency)
        self._app.add_handler(CommandHandler("start", self._cmd_start))
        self._app.add_handler(CommandHandler("help", self._cmd_help))

        # Dynamic commands — one dispatcher per name in _command_handlers
        for name in self._command_handlers:
            self._app.add_handler(CommandHandler(name, self._make_dispatcher(name)))

        # Inline button callback handler (for trade approval)
        self._app.add_handler(CallbackQueryHandler(self._handle_callback))

        # Build bot command menu from registered handlers + static entries
        menu = [BotCommand("help", "Show available commands")]
        for name, (_, desc) in self._command_handlers.items():
            menu.append(BotCommand(name, desc[:256]))
        await self._app.bot.set_my_commands(menu)

        await self._app.initialize()
        await self._app.start()
        await self._app.updater.start_polling(drop_pending_updates=True)

        logger.info("Telegram bot polling started")

        # Keep running until thread is stopped
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            pass
        finally:
            await self._app.updater.stop()
            await self._app.stop()
            await self._app.shutdown()

    def _check_authorized(self, chat_id: int) -> bool:
        """Check if the chat ID is authorized."""
        allowed = self.config.TELEGRAM_CHAT_ID
        if not allowed:
            return True  # No restriction set
        return str(chat_id) in allowed.split(",")

    # ── Bot commands ─────────────────────────────────────────────────────

    async def _cmd_start(self, update, context) -> None:
        if not self._check_authorized(update.effective_chat.id):
            return
        await update.message.reply_text(
            "<b>Schwab Agent Bot</b>\n\nUse /help to see available commands.",
            parse_mode="HTML",
        )

    async def _cmd_help(self, update, context) -> None:
        import html
        if not self._check_authorized(update.effective_chat.id):
            return
        lines = ["<b>Available Commands</b>\n"]
        for name, (_, desc) in self._command_handlers.items():
            lines.append(f"/{name} — {html.escape(desc)}")
        lines.append("/help — This message")
        await update.message.reply_text("\n".join(lines), parse_mode="HTML")

    def _make_dispatcher(self, name: str):
        """Build an async CommandHandler callback that invokes the registered handler.

        Dynamic command replies use parse_mode=HTML — far fewer reserved
        characters than MarkdownV2, so numeric output ($215.23, 50%, etc.)
        works without escape gymnastics. Only <, >, and & need escaping.
        """
        import html

        async def _dispatch(update, context) -> None:
            if not self._check_authorized(update.effective_chat.id):
                return
            entry = self._command_handlers.get(name)
            if entry is None:
                await update.message.reply_text(
                    f"Handler for /{name} not registered.",
                    parse_mode="HTML",
                )
                return
            handler, _ = entry
            args = list(context.args) if context.args else []
            try:
                text = handler(args)
            except Exception as e:
                logger.exception("Telegram command /%s failed", name)
                text = f"<b>Error in /{name}:</b> <code>{html.escape(str(e))[:400]}</code>"
            if not text:
                text = "<i>(empty)</i>"
            try:
                await update.message.reply_text(text, parse_mode="HTML")
            except Exception as e:
                # Fall back to plain text if HTML parsing fails for any reason.
                logger.warning("HTML reply failed (%s) — sending plain text", e)
                await update.message.reply_text(text[:4000])

        return _dispatch

    # ── Callback query handler (inline buttons) ─────────────────────────

    async def _handle_callback(self, update, context) -> None:
        """Handle inline button presses (trade approval/rejection)."""
        query = update.callback_query
        if not self._check_authorized(query.message.chat_id):
            await query.answer("Unauthorized")
            return

        data = query.data  # e.g. "approve:trade_123" or "reject:trade_123"
        if ":" not in data:
            await query.answer("Invalid callback")
            return

        action, trade_id = data.split(":", 1)

        future = _pending_approvals.pop(trade_id, None)
        if future is None:
            await query.answer("Trade expired or already handled")
            await query.edit_message_text(
                query.message.text + "\n\n_Expired_",
                parse_mode="MarkdownV2",
            )
            return

        approved = action == "approve"
        future.set_result(approved)

        status = "APPROVED" if approved else "REJECTED"
        await query.answer(f"Trade {status}")
        await query.edit_message_text(
            _escape_md(query.message.text_markdown_v2_urled or query.message.text)
            + f"\n\n*{status}*",
            parse_mode="MarkdownV2",
        )
        logger.info("Trade %s %s via Telegram", trade_id, status.lower())

    # ── Public API: register command handlers ────────────────────────────

    def register_command(
        self,
        command: str,
        handler: Callable[[list[str]], str],
        description: str = "",
    ) -> None:
        """Register a handler that returns MarkdownV2 text for a bot command.

        Args:
            command: Command name without slash (e.g. "status")
            handler: Callable taking a list of space-split args, returning MarkdownV2
            description: One-line menu description shown in Telegram's command picker
        """
        self._command_handlers[command] = (handler, description or command)

    # ── Public API: send alerts ──────────────────────────────────────────

    def send_alert(self, message: str, parse_mode: str = "MarkdownV2") -> None:
        """Send a message to the configured chat. Thread-safe."""
        if not self.config.TELEGRAM_ENABLED or not self._loop or not self._app:
            return
        chat_id = self.config.TELEGRAM_CHAT_ID
        if not chat_id:
            return

        async def _send():
            try:
                await self._app.bot.send_message(
                    chat_id=int(chat_id.split(",")[0]),
                    text=message,
                    parse_mode=parse_mode,
                )
            except Exception as e:
                logger.error("Telegram send_alert failed: %s", e)

        asyncio.run_coroutine_threadsafe(_send(), self._loop)

    def send_trade_alert(self, trade: dict) -> None:
        """Send a formatted trade execution alert."""
        side = trade.get("side", "?")
        symbol = trade.get("symbol", "?")
        qty = trade.get("quantity", 0)
        price = trade.get("price", 0)
        value = trade.get("value", qty * price)
        strategy = trade.get("strategy", "?")
        pnl = trade.get("realized_pnl")
        dry = trade.get("dry_run", True)

        icon = "BUY" if side == "BUY" else "SELL"
        mode = "DRY RUN" if dry else "LIVE"

        lines = [
            f"*{icon} {symbol}* \\({mode}\\)",
            f"Strategy: `{_escape_md(strategy)}`",
            f"Qty: {qty} @ ${price:,.2f}",
            f"Value: ${value:,.2f}",
        ]
        if pnl is not None:
            sign = "\\+" if pnl >= 0 else ""
            lines.append(f"P&L: {sign}${pnl:,.2f}")

        reason = trade.get("reason", "")
        if reason:
            lines.append(f"Reason: `{_escape_md(reason[:100])}`")

        self.send_alert("\n".join(lines))

    def send_kill_switch_alert(self, reason: str) -> None:
        """Alert when kill switch is triggered."""
        self.send_alert(
            f"*KILL SWITCH ACTIVATED*\n\n{_escape_md(reason)}\n\n"
            f"All trading halted\\. Use /resume to clear\\."
        )

    def send_daily_summary(self, summary: dict) -> None:
        """Send daily P&L summary."""
        lines = ["*Daily P&L Summary*\n"]
        total = 0.0
        for strategy, data in sorted(summary.items()):
            pnl = data.get("realized_pnl", 0)
            trades = data.get("trades", 0)
            total += pnl
            sign = "\\+" if pnl >= 0 else ""
            lines.append(f"`{_escape_md(strategy):<20}` {sign}${pnl:,.2f}  \\({trades} trades\\)")

        sign = "\\+" if total >= 0 else ""
        lines.append(f"\n*Total: {sign}${total:,.2f}*")
        self.send_alert("\n".join(lines))

    def send_error(self, error: str) -> None:
        """Alert on agent errors."""
        self.send_alert(f"*Agent Error*\n\n`{_escape_md(error[:500])}`")

    def send_quant_papers(self, papers: list) -> None:
        """Send a digest of top-scoring quant research papers.

        Each paper is a PaperRow (or any object with title, url, source,
        authors, published, relevance_score, relevance_tags, summary).
        """
        if not papers:
            return

        lines = [f"*Quant Research — {len(papers)} new*\n"]
        for p in papers:
            src = _escape_md(getattr(p, "source", "?"))
            title = _escape_md((getattr(p, "title", "") or "")[:140])
            url = getattr(p, "url", "") or ""
            score = float(getattr(p, "relevance_score", 0.0) or 0.0)
            tags = _escape_md((getattr(p, "relevance_tags", "") or "")[:80])
            authors = _escape_md((getattr(p, "authors", "") or "")[:80])
            published = _escape_md((getattr(p, "published", "") or "")[:10])
            summary = (getattr(p, "summary", "") or "").strip()

            lines.append(f"*\\[{src}\\]* [{title}]({_escape_md(url)})")
            meta = f"score={score:.1f}"
            if authors:
                meta += f" · {authors}"
            if published:
                meta += f" · {published}"
            lines.append(f"`{meta}`")
            if tags:
                lines.append(f"_{tags}_")
            if summary:
                lines.append(_escape_md(summary[:400]))
            lines.append("")
        self.send_alert("\n".join(lines).rstrip())

    # ── Public API: trade approval ───────────────────────────────────────

    def request_approval(self, trade_id: str, trade: dict, timeout: int | None = None) -> bool:
        """Request trade approval via Telegram inline buttons.

        Sends a message with Approve/Reject buttons and blocks until
        the user responds or timeout expires.

        Args:
            trade_id: Unique identifier for this trade.
            trade: Trade details dict.
            timeout: Seconds to wait (default from config).

        Returns:
            True if approved, False if rejected or timed out.
        """
        if not self.config.TELEGRAM_ENABLED or not self._loop or not self._app:
            return True  # If telegram not available, auto-approve
        if not self.config.TELEGRAM_REQUIRE_APPROVAL:
            return True

        timeout = timeout or self.config.TELEGRAM_APPROVAL_TIMEOUT
        chat_id = self.config.TELEGRAM_CHAT_ID
        if not chat_id:
            return True

        future = self._loop.create_future()
        _pending_approvals[trade_id] = future

        text = _render_approval_text(trade, timeout)

        async def _send_approval():
            from telegram import InlineKeyboardButton, InlineKeyboardMarkup


            keyboard = InlineKeyboardMarkup([[
                InlineKeyboardButton("Approve", callback_data=f"approve:{trade_id}"),
                InlineKeyboardButton("Reject", callback_data=f"reject:{trade_id}"),
            ]])

            try:
                await self._app.bot.send_message(
                    chat_id=int(chat_id.split(",")[0]),
                    text=text,
                    parse_mode="MarkdownV2",
                    reply_markup=keyboard,
                )
            except Exception as e:
                logger.error("Telegram approval request failed: %s", e)
                _pending_approvals.pop(trade_id, None)
                future.set_result(False)

        asyncio.run_coroutine_threadsafe(_send_approval(), self._loop)

        # Block the calling thread until approved/rejected/timeout
        try:
            result_future = asyncio.run_coroutine_threadsafe(
                asyncio.wait_for(future, timeout=timeout),
                self._loop,
            )
            return result_future.result(timeout=timeout + 5)
        except (asyncio.TimeoutError, TimeoutError, Exception):
            _pending_approvals.pop(trade_id, None)
            logger.warning("Trade %s approval timed out", trade_id)

            # Notify that it timed out
            async def _notify_timeout():
                try:
                    await self._app.bot.send_message(
                        chat_id=int(chat_id.split(",")[0]),
                        text=f"*Trade {_escape_md(trade_id)} timed out* \\- not executed",
                        parse_mode="MarkdownV2",
                    )
                except Exception:
                    pass

            asyncio.run_coroutine_threadsafe(_notify_timeout(), self._loop)
            return False

    # ── Lifecycle ────────────────────────────────────────────────────────

    def stop(self) -> None:
        """Stop the bot."""
        if self._loop and not self._loop.is_closed():
            for future in _pending_approvals.values():
                if not future.done():
                    future.cancel()
            _pending_approvals.clear()
            self._loop.call_soon_threadsafe(self._loop.stop)
        if self._thread:
            self._thread.join(timeout=5)
        logger.info("Telegram bot stopped")
