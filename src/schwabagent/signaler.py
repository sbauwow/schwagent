"""Signal-change notifier — pushes alerts on strategy signal transitions.

Pure signaler: no order path, no account mutation. Watches the scan output
of configured strategies (default: trend_following), diffs each symbol's
signal against the last persisted state, and pushes a Telegram alert when
the signal changes.

Alert rules:
  - A symbol alerts when its signal differs from the last *alerted* signal.
  - Transitions into an actionable signal (BUY/SELL/STRONG_*) require
    |score| >= SIGNALER_MIN_ABS_SCORE.
  - Transitions out of an actionable signal (e.g. BUY → HOLD) always
    qualify — trend exits are as important as entries.
  - A per-symbol cooldown suppresses flapping: while cooling down, the
    stored signal is NOT updated, so a flap back to the original signal
    never re-alerts (hysteresis).

Telegram delivery uses a direct HTTPS POST (plain text) so the signaler
works both inside the runner loop and from one-shot CLI runs without the
asyncio TelegramBot.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Callable

from schwabagent.config import Config
from schwabagent.persistence import StateStore

logger = logging.getLogger(__name__)

ACTIONABLE = {"BUY", "STRONG_BUY", "SELL", "STRONG_SELL"}

_ARROW = {
    "STRONG_BUY": "🚀",
    "BUY": "📈",
    "HOLD": "⏸",
    "SELL": "📉",
    "STRONG_SELL": "🔻",
}


def _signal_str(value) -> str:
    """Normalize a Signal enum or raw string to its plain string value."""
    return str(getattr(value, "value", value))


class SignalNotifier:
    """Diffs scan signals against persisted state and alerts on changes."""

    def __init__(
        self,
        config: Config,
        state: StateStore,
        send: Callable[[str], None] | None = None,
    ):
        self.config = config
        self.state = state
        self._send = send or self._send_telegram

    @property
    def enabled(self) -> bool:
        return self.config.SIGNALER_ENABLED

    def is_watched(self, strategy: str) -> bool:
        """Whether this strategy's scans feed the signaler.

        SIGNALER_STRATEGIES="all" watches every strategy that scans.
        """
        watched = self.config.signaler_strategies
        return "all" in watched or strategy in watched

    # ── Core ──────────────────────────────────────────────────────────────

    def process(self, strategy: str, opportunities: list[dict]) -> list[dict]:
        """Diff one strategy's scan output against persisted signal state.

        Returns the list of alert events emitted (possibly empty). Each
        event dict: strategy, symbol, prev_signal, signal, score, price,
        reason, ts.
        """
        if not self.enabled or not self.is_watched(strategy) or not opportunities:
            return []

        now = datetime.now(timezone.utc)
        cooldown = timedelta(minutes=self.config.SIGNALER_COOLDOWN_MINUTES)
        min_score = self.config.SIGNALER_MIN_ABS_SCORE

        signal_state = self.state.get_signal_state()
        events: list[dict] = []
        dirty = False

        for opp in opportunities:
            symbol = opp.get("symbol")
            if not symbol:
                continue
            cur = _signal_str(opp.get("signal", "HOLD"))
            score = float(opp.get("score", 0.0))

            key = f"{strategy}:{symbol}"
            prev_entry = signal_state.get(key) or {}
            prev = prev_entry.get("signal", "HOLD")

            if cur == prev:
                continue

            # Entry into an actionable signal must clear the score bar;
            # exit from an actionable signal always qualifies.
            entering = cur in ACTIONABLE and abs(score) >= min_score
            exiting = prev in ACTIONABLE
            if not entering and not exiting:
                continue

            # Cooldown: keep the stored signal untouched so a flap back
            # to it never re-alerts.
            alerted_ts = prev_entry.get("alerted_ts")
            if alerted_ts:
                try:
                    last = datetime.fromisoformat(alerted_ts)
                    if now - last < cooldown:
                        logger.debug(
                            "[signaler] %s %s→%s suppressed (cooldown)",
                            key, prev, cur,
                        )
                        continue
                except ValueError:
                    pass

            event = {
                "strategy": strategy,
                "symbol": symbol,
                "prev_signal": prev,
                "signal": cur,
                "score": score,
                "price": opp.get("price"),
                "reason": opp.get("reason", ""),
                "ts": now.isoformat(),
            }
            events.append(event)
            signal_state[key] = {
                "signal": cur,
                "score": score,
                "alerted_ts": now.isoformat(),
            }
            dirty = True

        if dirty:
            self.state.save_signal_state(signal_state)

        for event in events:
            self.state.append_signal_event(event)
            try:
                self._send(self.format_event(event))
            except Exception as e:
                logger.error("[signaler] alert send failed for %s: %s",
                             event["symbol"], e)

        if events:
            logger.info("[signaler] %s: %d signal change(s): %s",
                        strategy, len(events),
                        ", ".join(f"{e['symbol']} {e['prev_signal']}→{e['signal']}"
                                  for e in events))
        return events

    # ── Formatting / delivery ─────────────────────────────────────────────

    @staticmethod
    def format_event(event: dict) -> str:
        icon = _ARROW.get(event["signal"], "•")
        price = event.get("price")
        price_str = f" @ ${price:.2f}" if isinstance(price, (int, float)) and price > 0 else ""
        lines = [
            f"{icon} {event['strategy']}: {event['symbol']} "
            f"{event['prev_signal']} → {event['signal']}{price_str}"
        ]
        if event.get("reason"):
            lines.append(event["reason"])
        return "\n".join(lines)

    def _send_telegram(self, text: str) -> None:
        """Plain-text POST straight to the Telegram API (no bot loop needed)."""
        if not self.config.TELEGRAM_ENABLED:
            return
        token = self.config.TELEGRAM_BOT_TOKEN
        chat_id = self.config.TELEGRAM_CHAT_ID
        if not token or not chat_id:
            return
        import requests

        url = f"https://api.telegram.org/bot{token}/sendMessage"
        resp = requests.post(
            url,
            json={"chat_id": int(chat_id.split(",")[0]), "text": text},
            timeout=10,
        )
        resp.raise_for_status()
