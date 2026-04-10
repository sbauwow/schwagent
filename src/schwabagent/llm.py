"""Multi-provider LLM client — Ollama, Anthropic Claude, OpenAI-compatible.

Switchable via LLM_PROVIDER in .env:
  - "ollama"    — local Ollama (default, free)
  - "anthropic" — Claude API (cloud, requires ANTHROPIC_API_KEY)
  - "openai"    — OpenAI or any compatible endpoint (requires OPENAI_API_KEY)

All providers expose the same interface: generate(prompt, system) → str.
High-level helpers (etf_commentary, signal_commentary) work regardless
of which backend is active.
"""
from __future__ import annotations

import json
import logging
from typing import Any

logger = logging.getLogger(__name__)


class LLMClient:
    """Unified LLM client that routes to the configured provider."""

    def __init__(
        self,
        provider: str = "ollama",
        model: str = "",
        api_key: str = "",
        base_url: str = "",
        timeout: int = 60,
        temperature: float = 0.2,
        max_tokens: int = 1024,
    ):
        self.provider = provider.lower().strip()
        self.timeout = timeout
        self.temperature = temperature
        self.max_tokens = max_tokens
        self._available: bool | None = None
        self._skills_loader: Any = None  # lazy-loaded SkillsLoader

        # Provider-specific defaults
        if self.provider == "ollama":
            self.model = model or "qwen2.5:14b-instruct-q5_K_M"
            self.base_url = (base_url or "http://localhost:11434").rstrip("/")
            self.api_key = ""
        elif self.provider == "anthropic":
            self.model = model or "claude-sonnet-4-6"
            self.base_url = (base_url or "https://api.anthropic.com").rstrip("/")
            self.api_key = api_key
        elif self.provider == "openai":
            self.model = model or "gpt-4o-mini"
            self.base_url = (base_url or "https://api.openai.com").rstrip("/")
            self.api_key = api_key
        else:
            # Treat unknown providers as OpenAI-compatible
            self.model = model or "gpt-4o-mini"
            self.base_url = (base_url or "http://localhost:8000").rstrip("/")
            self.api_key = api_key
            self.provider = "openai"  # use OpenAI format

    # ── Connectivity ─────────────────────────────────────────────────────

    def is_available(self) -> bool:
        """Check if the LLM backend is reachable."""
        if self._available is not None:
            return self._available

        try:
            if self.provider == "ollama":
                self._available = self._check_ollama()
            elif self.provider == "anthropic":
                self._available = bool(self.api_key)
            elif self.provider == "openai":
                self._available = bool(self.api_key) or "localhost" in self.base_url
            else:
                self._available = False
        except Exception:
            self._available = False

        return self._available

    def _check_ollama(self) -> bool:
        import httpx
        try:
            r = httpx.get(f"{self.base_url}/api/tags", timeout=5)
            models = [m["name"] for m in r.json().get("models", [])]
            return any(self.model.split(":")[0] in m for m in models)
        except Exception:
            return False

    # ── Core generate ────────────────────────────────────────────────────

    def generate(self, prompt: str, system: str = "", max_tokens: int | None = None) -> str:
        """Send a completion request. Routes to the configured provider."""
        if self.provider == "ollama":
            return self._generate_ollama(prompt, system, max_tokens)
        elif self.provider == "anthropic":
            return self._generate_anthropic(prompt, system, max_tokens)
        elif self.provider == "openai":
            return self._generate_openai(prompt, system, max_tokens)
        else:
            raise ValueError(f"Unknown LLM provider: {self.provider}")

    # ── Skills (progressive disclosure) ──────────────────────────────────

    @property
    def skills(self):
        """Lazy-loaded SkillsLoader — loads only when first accessed."""
        if self._skills_loader is None:
            from schwabagent.intelligence import SkillsLoader
            self._skills_loader = SkillsLoader()
        return self._skills_loader

    def with_skills(self, system: str = "") -> str:
        """Augment a system prompt with the skill catalog.

        Emits only one-line summaries grouped by category. The LLM can
        then call load_skill(name) to pull full content on demand.

        Args:
            system: Base system prompt.

        Returns:
            System prompt with a "Skills Available" section appended.
        """
        catalog = self.skills.get_descriptions()
        if not catalog or catalog == "(no skills)":
            return system
        header = (
            "\n\n## Skills Available\n"
            "You have access to the following reference skills. Call the "
            "load_skill(name) helper to fetch the full methodology for any "
            "skill when you need it.\n"
        )
        return (system + header + catalog).strip()

    def load_skill(self, name: str) -> str:
        """Return the full SKILL.md content for a given skill name."""
        return self.skills.get_content(name)

    # ── Core generate (by provider) ──────────────────────────────────────

    def _generate_ollama(self, prompt: str, system: str, max_tokens: int | None) -> str:
        import httpx
        payload: dict[str, Any] = {
            "model": self.model,
            "prompt": prompt,
            "stream": False,
            "options": {
                "temperature": self.temperature,
                "num_predict": max_tokens or self.max_tokens,
            },
        }
        if system:
            payload["system"] = system

        r = httpx.post(f"{self.base_url}/api/generate", json=payload, timeout=self.timeout)
        r.raise_for_status()
        return r.json().get("response", "").strip()

    def _generate_anthropic(self, prompt: str, system: str, max_tokens: int | None) -> str:
        import httpx
        messages = [{"role": "user", "content": prompt}]
        payload: dict[str, Any] = {
            "model": self.model,
            "max_tokens": max_tokens or self.max_tokens,
            "temperature": self.temperature,
            "messages": messages,
        }
        if system:
            payload["system"] = system

        r = httpx.post(
            f"{self.base_url}/v1/messages",
            json=payload,
            headers={
                "x-api-key": self.api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            timeout=self.timeout,
        )
        r.raise_for_status()
        data = r.json()

        # Extract text from content blocks
        content = data.get("content", [])
        texts = [b.get("text", "") for b in content if b.get("type") == "text"]
        return "\n".join(texts).strip()

    def _generate_openai(self, prompt: str, system: str, max_tokens: int | None) -> str:
        import httpx
        messages = []
        if system:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": prompt})

        payload = {
            "model": self.model,
            "messages": messages,
            "max_tokens": max_tokens or self.max_tokens,
            "temperature": self.temperature,
        }

        headers = {"content-type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"

        r = httpx.post(
            f"{self.base_url}/v1/chat/completions",
            json=payload,
            headers=headers,
            timeout=self.timeout,
        )
        r.raise_for_status()
        data = r.json()

        choices = data.get("choices", [])
        if choices:
            return choices[0].get("message", {}).get("content", "").strip()
        return ""

    # ── Provider info ────────────────────────────────────────────────────

    def info(self) -> dict:
        return {
            "provider": self.provider,
            "model": self.model,
            "base_url": self.base_url,
            "available": self.is_available(),
            "has_api_key": bool(self.api_key),
        }

    # ── High-level helpers ───────────────────────────────────────────────

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
        """Return macro commentary and a confidence modifier for an ETF signal."""
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
            'Return JSON with keys: commentary (1 sentence), reasoning (1 sentence), '
            'confidence (float 0.0-1.0 representing your conviction the quant signal is correct).'
        )

        try:
            raw = self.generate(prompt, system, max_tokens=300)
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

    def signal_commentary(self, symbol: str, signal: str, indicators: dict) -> str:
        """One-sentence commentary on a technical signal."""
        if not self.is_available():
            return ""
        prompt = (
            f"Symbol: {symbol}, Signal: {signal}\n"
            f"Indicators: {json.dumps(indicators, default=str)}\n"
            "Give one concise sentence explaining the most important driver of this signal."
        )
        try:
            return self.generate(prompt, max_tokens=150)
        except Exception as e:
            logger.debug("LLM signal_commentary failed: %s", e)
            return ""


# ── Backward compatibility ───────────────────────────────────────────────────

class OllamaClient(LLMClient):
    """Legacy alias — creates an Ollama-backed LLMClient."""
    def __init__(self, host: str = "http://localhost:11434",
                 model: str = "qwen2.5:14b-instruct-q5_K_M", timeout: int = 60):
        super().__init__(provider="ollama", model=model, base_url=host, timeout=timeout)

    # Keep old method name working
    def _generate(self, prompt: str, system: str = "") -> str:
        return self.generate(prompt, system)
