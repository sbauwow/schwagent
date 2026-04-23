"""Web scrapers — fetch structured data from public financial web pages.

Each scraper exposes a `fetch_*()` entry point returning typed dataclass
rows, plus a CLI/Telegram-friendly renderer. Results are cached to
~/.schwagent/scraper_cache/ so repeat calls inside the same ET
trading day don't hammer upstream hosts.
"""
