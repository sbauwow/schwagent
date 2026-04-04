"""Local LLM client — Ollama-backed probability and commentary generation."""
from __future__ import annotations

import json
import logging
from typing import Any

logger = logging.getLogger(__name__)


class OllamaClient:
    """Thin wrapper around the Ollama HTTP API.

    Used optionally by strategies to get macro commentary or probability
    shading on top of quantitative signals.
    """

    def __init__(self, host: str = "http://localhost:11434", model: str = "qwen2.5:14b-instruct-q5_K_M", timeout: int = 60):
        self.host = host.rstrip("/")
        self.model = model
        self.timeout = timeout
        self._available: bool | None = None  # cached after first check

    # ── Connectivity ──────────────────────────────────────────────────────────

    def is_available(self) -> bool:
        """Return True if Ollama is reachable and the model is loaded."""
        if self._available is not None:
            return self._available
        try:
            import httpx
            r = httpx.get(f"{self.host}/api/tags", timeout=5)
            models = [m["name"] for m in r.json().get("models", [])]
            # Accept prefix match (e.g. "qwen2.5:14b" matches "qwen2.5:14b-instruct-q5_K_M")
            self._available = any(self.model.split(":")[0] in m for m in models)
        except Exception as e:
            logger.debug("Ollama not reachable: %s", e)
            self._available = False
        return self._available

    # ── Core generate ─────────────────────────────────────────────────────────

    def _generate(self, prompt: str, system: str = "") -> str:
        """Send a completion request and return the response text."""
        import httpx

        payload: dict[str, Any] = {
            "model": self.model,
            "prompt": prompt,
            "stream": False,
            "options": {"temperature": 0.2, "num_predict": 300},
        }
        if system:
            payload["system"] = system

        r = httpx.post(
            f"{self.host}/api/generate",
            json=payload,
            timeout=self.timeout,
        )
        r.raise_for_status()
        return r.json().get("response", "").strip()

    # ── High-level helpers ────────────────────────────────────────────────────

    def etf_commentary(
        self,
        symbol: str,
        description: str,
        momentum_rank: int,
        universe_size: int,
        return_12m: float,
        return_1m: float,
        signal: str,
    ) -> dict:
        """Return macro commentary and a confidence modifier for an ETF signal.

        Returns:
            {
                "commentary": str,
                "confidence": float,   # 0.0–1.0 — multiplied into position size
                "reasoning": str,
            }
        """
        if not self.is_available():
            return {"commentary": "", "confidence": 0.7, "reasoning": "LLM unavailable"}

        system = (
            "You are a macro-aware ETF portfolio analyst. "
            "Be concise. Respond ONLY with valid JSON. No markdown, no preamble."
        )
        prompt = (
            f"ETF: {symbol} ({description})\n"
            f"Quant signal: {signal}\n"
            f"Momentum rank: {momentum_rank} of {universe_size}\n"
            f"12-month return: {return_12m:+.1f}%\n"
            f"1-month return: {return_1m:+.1f}%\n\n"
            "Given current macro conditions, assess this ETF. "
            "Return JSON with keys: commentary (1 sentence), reasoning (1 sentence), "
            "confidence (float 0.0-1.0 representing your conviction the quant signal is correct)."
        )

        try:
            raw = self._generate(prompt, system)
            # Extract JSON from response (may have surrounding text)
            start = raw.find("{")
            end = raw.rfind("}") + 1
            if start >= 0 and end > start:
                data = json.loads(raw[start:end])
                return {
                    "commentary": str(data.get("commentary", "")),
                    "reasoning": str(data.get("reasoning", "")),
                    "confidence": float(max(0.0, min(1.0, data.get("confidence", 0.7)))),
                }
        except Exception as e:
            logger.warning("LLM etf_commentary failed for %s: %s", symbol, e)

        return {"commentary": "", "confidence": 0.7, "reasoning": "parse error"}

    def signal_commentary(
        self,
        symbol: str,
        signal: str,
        indicators: dict,
    ) -> str:
        """One-sentence commentary on a technical signal (used by non-ETF strategies)."""
        if not self.is_available():
            return ""
        prompt = (
            f"Symbol: {symbol}, Signal: {signal}\n"
            f"Indicators: {json.dumps(indicators, default=str)}\n"
            "Give one concise sentence explaining the most important driver of this signal."
        )
        try:
            return self._generate(prompt)
        except Exception as e:
            logger.debug("LLM signal_commentary failed: %s", e)
            return ""
