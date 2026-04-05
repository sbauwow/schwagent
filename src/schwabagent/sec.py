"""SEC EDGAR filing retrieval and LLM-powered analysis.

Fetches 10-K, 10-Q, 8-K, and other filings from SEC EDGAR, extracts
key sections, and uses the local Ollama LLM to generate investment-grade
summaries.

Usage:
    from schwabagent.sec import SECAnalyzer

    sec = SECAnalyzer()
    # Get recent filings
    filings = sec.get_filings("AAPL", form="10-K", limit=3)
    # Analyze a filing with LLM
    analysis = sec.analyze_filing("AAPL", form="10-K")
    # Quick risk factor scan
    risks = sec.extract_risk_factors("AAPL")
"""
from __future__ import annotations

import json
import logging
import re
import textwrap
from dataclasses import dataclass, field
from typing import Any

from schwabagent.config import Config

logger = logging.getLogger(__name__)

# Key sections to extract from 10-K/10-Q filings
_10K_SECTIONS = {
    "business": "Item 1. Business",
    "risk_factors": "Item 1A. Risk Factors",
    "financials_md&a": "Item 7. Management's Discussion and Analysis",
    "financial_statements": "Item 8. Financial Statements",
}

_8K_SECTIONS = {
    "entry_info": "Item",  # 8-K items vary (2.02 = results, 5.02 = departures, etc.)
}


@dataclass
class FilingInfo:
    """Metadata for a single SEC filing."""
    symbol: str
    form: str              # 10-K, 10-Q, 8-K, etc.
    filing_date: str       # YYYY-MM-DD
    accession_number: str
    description: str = ""
    url: str = ""


@dataclass
class FilingAnalysis:
    """LLM-generated analysis of a filing."""
    symbol: str
    form: str
    filing_date: str
    summary: str = ""              # executive summary
    key_financials: str = ""       # revenue, earnings, margins, guidance
    risk_assessment: str = ""      # risk factors analysis
    sentiment: str = ""            # bullish / bearish / neutral
    actionable_insights: list[str] = field(default_factory=list)
    raw_text_length: int = 0
    sections_extracted: list[str] = field(default_factory=list)


class SECAnalyzer:
    """Fetch and analyze SEC filings using EDGAR + local LLM."""

    def __init__(self, config: Config | None = None):
        self._config = config or Config()
        self._identity_set = False

    def _ensure_identity(self) -> None:
        """Set SEC EDGAR identity (required for API access)."""
        if self._identity_set:
            return
        try:
            from edgar import set_identity
            set_identity("SchwabAgent admin@schwab-agent.local")
            self._identity_set = True
        except Exception as e:
            logger.error("Failed to set EDGAR identity: %s", e)

    # ── Filing retrieval ─────────────────────────────────────────────────

    def get_filings(
        self,
        symbol: str,
        form: str = "10-K",
        limit: int = 5,
    ) -> list[FilingInfo]:
        """Get recent filings for a symbol."""
        self._ensure_identity()
        try:
            from edgar import Company
            company = Company(symbol)
            filings = company.get_filings(form=form)

            results = []
            for i, filing in enumerate(filings):
                if i >= limit:
                    break
                results.append(FilingInfo(
                    symbol=symbol,
                    form=filing.form,
                    filing_date=str(filing.filing_date),
                    accession_number=str(filing.accession_no),
                    description=str(getattr(filing, "description", "")),
                    url=str(getattr(filing, "filing_href", "")),
                ))
            return results
        except Exception as e:
            logger.error("Failed to get filings for %s: %s", symbol, e)
            return []

    def get_filing_text(
        self,
        symbol: str,
        form: str = "10-K",
        index: int = 0,
        max_chars: int = 100_000,
    ) -> tuple[str, FilingInfo | None]:
        """Get the plain text of a specific filing.

        Args:
            symbol: Ticker symbol.
            form: Filing type (10-K, 10-Q, 8-K).
            index: Which filing (0=most recent).
            max_chars: Truncate text to this length.

        Returns:
            (text, filing_info) or ("", None) on failure.
        """
        self._ensure_identity()
        try:
            from edgar import Company
            company = Company(symbol)
            filings = company.get_filings(form=form)

            filing = filings[index]
            info = FilingInfo(
                symbol=symbol,
                form=filing.form,
                filing_date=str(filing.filing_date),
                accession_number=str(filing.accession_no),
            )

            # Get the HTML and convert to text
            try:
                text = filing.text()
            except Exception:
                html = filing.html()
                text = self._html_to_text(html)

            if len(text) > max_chars:
                text = text[:max_chars] + "\n\n[TRUNCATED]"

            return text, info
        except Exception as e:
            logger.error("Failed to get filing text for %s %s: %s", symbol, form, e)
            return "", None

    def get_financials(self, symbol: str) -> dict[str, Any]:
        """Get structured financial data from XBRL (no filing parsing needed)."""
        self._ensure_identity()
        try:
            from edgar import Company
            company = Company(symbol)
            facts = company.get_facts()

            # Extract key metrics
            result = {"symbol": symbol, "facts": {}}
            key_concepts = [
                "Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax",
                "NetIncomeLoss", "EarningsPerShareBasic", "EarningsPerShareDiluted",
                "Assets", "Liabilities", "StockholdersEquity",
                "OperatingIncomeLoss", "GrossProfit",
                "CashAndCashEquivalentsAtCarryingValue",
                "LongTermDebt", "CommonStockSharesOutstanding",
            ]

            for concept in key_concepts:
                try:
                    data = facts.get(concept)
                    if data is not None:
                        result["facts"][concept] = data
                except Exception:
                    continue

            return result
        except Exception as e:
            logger.error("Failed to get financials for %s: %s", symbol, e)
            return {"symbol": symbol, "error": str(e)}

    # ── LLM analysis ─────────────────────────────────────────────────────

    def analyze_filing(
        self,
        symbol: str,
        form: str = "10-K",
        index: int = 0,
    ) -> FilingAnalysis:
        """Fetch a filing and analyze it with the local LLM.

        Returns a structured analysis with summary, financials, risks, and sentiment.
        """
        text, info = self.get_filing_text(symbol, form, index, max_chars=80_000)
        if not text or not info:
            return FilingAnalysis(symbol=symbol, form=form, filing_date="")

        # Extract key sections for more focused analysis
        sections = self._extract_sections(text, form)
        sections_text = "\n\n".join(
            f"=== {name} ===\n{content[:15000]}"
            for name, content in sections.items()
        )
        if not sections_text:
            sections_text = text[:40000]

        # Build LLM prompt
        prompt = self._build_analysis_prompt(symbol, form, info.filing_date, sections_text)

        # Call Ollama
        llm_response = self._call_llm(prompt)

        analysis = self._parse_analysis(llm_response, symbol, form, info.filing_date)
        analysis.raw_text_length = len(text)
        analysis.sections_extracted = list(sections.keys())
        return analysis

    def extract_risk_factors(self, symbol: str, form: str = "10-K") -> str:
        """Extract and summarize risk factors from the latest filing."""
        text, info = self.get_filing_text(symbol, form, max_chars=100_000)
        if not text:
            return f"Could not retrieve {form} for {symbol}"

        sections = self._extract_sections(text, form)
        risk_text = sections.get("risk_factors", "")
        if not risk_text:
            return f"No risk factors section found in {symbol} {form}"

        prompt = f"""You are a senior securities analyst. Analyze these risk factors from {symbol}'s {form} filing dated {info.filing_date if info else 'unknown'}:

{risk_text[:20000]}

Provide:
1. TOP 5 most material risks (ranked by potential financial impact)
2. Any NEW risks that weren't in prior filings (unusual or emerging)
3. Overall risk assessment: LOW / MODERATE / HIGH / ELEVATED
4. One-sentence investment implication for each top risk

Be specific and quantitative where the filing provides numbers."""

        return self._call_llm(prompt)

    def compare_filings(self, symbol: str, form: str = "10-Q") -> str:
        """Compare the two most recent filings to identify changes."""
        text_new, info_new = self.get_filing_text(symbol, form, index=0, max_chars=40_000)
        text_old, info_old = self.get_filing_text(symbol, form, index=1, max_chars=40_000)

        if not text_new or not text_old:
            return f"Could not retrieve two {form} filings for {symbol}"

        # Extract MD&A sections for comparison
        sections_new = self._extract_sections(text_new, form)
        sections_old = self._extract_sections(text_old, form)

        mda_new = sections_new.get("financials_md&a", text_new[:20000])
        mda_old = sections_old.get("financials_md&a", text_old[:20000])

        prompt = f"""You are a senior equity research analyst. Compare these two {form} filings for {symbol}:

=== NEWER FILING ({info_new.filing_date if info_new else 'recent'}) ===
{mda_new[:15000]}

=== OLDER FILING ({info_old.filing_date if info_old else 'prior'}) ===
{mda_old[:15000]}

Identify:
1. KEY CHANGES in revenue, margins, guidance, or strategy
2. NEW language or disclosures not in the prior filing
3. REMOVED or softened language (could signal improving conditions)
4. TONE SHIFT (more optimistic, cautious, defensive?)
5. Investment implication: has the thesis improved, deteriorated, or unchanged?

Be specific — cite numbers and direct language changes."""

        return self._call_llm(prompt)

    # ── Section extraction ───────────────────────────────────────────────

    def _extract_sections(self, text: str, form: str) -> dict[str, str]:
        """Extract key sections from filing text using regex patterns."""
        sections = {}

        if form in ("10-K", "10-Q"):
            section_map = _10K_SECTIONS
        elif form == "8-K":
            section_map = _8K_SECTIONS
        else:
            return {"full_text": text[:30000]}

        for key, header in section_map.items():
            pattern = re.compile(
                rf"(?:^|\n)\s*{re.escape(header)}.*?\n(.*?)(?=\n\s*Item\s+\d|$)",
                re.DOTALL | re.IGNORECASE,
            )
            match = pattern.search(text)
            if match:
                content = match.group(1).strip()
                if len(content) > 200:  # skip empty sections
                    sections[key] = content

        return sections

    def _html_to_text(self, html: str) -> str:
        """Convert HTML filing to plain text."""
        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html, "html.parser")
            for tag in soup(["script", "style", "sup"]):
                tag.decompose()
            text = soup.get_text(separator="\n")
            # Clean up whitespace
            lines = [line.strip() for line in text.splitlines()]
            text = "\n".join(line for line in lines if line)
            return text
        except Exception:
            # Fallback: strip tags with regex
            text = re.sub(r"<[^>]+>", " ", html)
            return re.sub(r"\s+", " ", text).strip()

    # ── LLM integration ──────────────────────────────────────────────────

    def _build_analysis_prompt(
        self, symbol: str, form: str, date: str, sections_text: str,
    ) -> str:
        return f"""You are a CFA charterholder and senior equity research analyst. Analyze this {form} filing for {symbol} (filed {date}).

{sections_text}

Provide your analysis in this exact JSON format:
{{
  "summary": "2-3 sentence executive summary of the filing's key takeaways",
  "key_financials": "Revenue, earnings, margins, cash flow, and any forward guidance — with specific numbers",
  "risk_assessment": "Top 3 risks and their potential impact",
  "sentiment": "bullish / bearish / neutral — with one sentence justification",
  "actionable_insights": ["insight 1", "insight 2", "insight 3"]
}}

Be specific, quantitative, and actionable. Cite numbers from the filing."""

    def _call_llm(self, prompt: str, max_tokens: int = 4096) -> str:
        """Call the local Ollama LLM."""
        if not self._config.LLM_ENABLED:
            return "[LLM disabled — set LLM_ENABLED=true in .env]"

        try:
            import requests
            resp = requests.post(
                f"{self._config.OLLAMA_HOST}/api/generate",
                json={
                    "model": self._config.OLLAMA_MODEL,
                    "prompt": prompt,
                    "stream": False,
                    "options": {"num_predict": max_tokens, "temperature": 0.3},
                },
                timeout=self._config.OLLAMA_TIMEOUT,
            )
            resp.raise_for_status()
            return resp.json().get("response", "")
        except Exception as e:
            logger.error("LLM call failed: %s", e)
            return f"[LLM error: {e}]"

    def _parse_analysis(
        self, response: str, symbol: str, form: str, date: str,
    ) -> FilingAnalysis:
        """Parse LLM JSON response into FilingAnalysis."""
        analysis = FilingAnalysis(symbol=symbol, form=form, filing_date=date)

        # Try to extract JSON from the response
        try:
            # Find JSON block in response
            json_match = re.search(r"\{[\s\S]*\}", response)
            if json_match:
                data = json.loads(json_match.group())
                analysis.summary = data.get("summary", "")
                analysis.key_financials = data.get("key_financials", "")
                analysis.risk_assessment = data.get("risk_assessment", "")
                analysis.sentiment = data.get("sentiment", "")
                analysis.actionable_insights = data.get("actionable_insights", [])
            else:
                # Fallback: use raw response as summary
                analysis.summary = response[:2000]
        except (json.JSONDecodeError, Exception):
            analysis.summary = response[:2000]

        return analysis

    # ── Convenience ──────────────────────────────────────────────────────

    def quick_scan(self, symbols: list[str], form: str = "8-K", days: int = 7) -> list[dict]:
        """Scan multiple symbols for recent filings.

        Returns list of {symbol, form, date, description} for any filings
        in the last N days. Useful for daily monitoring.
        """
        results = []
        for sym in symbols:
            filings = self.get_filings(sym, form=form, limit=5)
            for f in filings:
                results.append({
                    "symbol": f.symbol,
                    "form": f.form,
                    "date": f.filing_date,
                    "description": f.description,
                    "accession": f.accession_number,
                })
        results.sort(key=lambda x: x["date"], reverse=True)
        return results
