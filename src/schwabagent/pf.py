"""Point & Figure charting powered by pypf with Schwab market data."""
from __future__ import annotations

import json
import logging
from collections import OrderedDict
from datetime import datetime, timedelta
from decimal import Decimal

from pypf.chart import PFChart
from pypf.instrument import Instrument

from schwabagent.schwab_client import SchwabClient

logger = logging.getLogger(__name__)

TWOPLACES = Decimal("0.01")


class SchwabSecurity(Instrument):
    """pypf Instrument backed by Schwab market data API."""

    def __init__(self, symbol: str, client: SchwabClient, period: float = 2.0,
                 interval: str = "1d", debug: bool = False):
        self._period_years = period  # keep float for data fetching
        super().__init__(
            symbol,
            force_download=False,
            force_cache=True,
            interval=interval,
            period=max(1, int(period)),  # pypf needs int
            debug=debug,
            data_directory="/tmp/pypf_noop",
        )
        self._client = client

    def populate_data(self):
        """Fetch OHLCV from Schwab and populate historical_data."""
        days = int(self._period_years * 365)
        df = self._client.get_ohlcv(self.symbol, days=days)

        self.historical_data = OrderedDict()
        for dt, row in df.iterrows():
            date_str = dt.strftime("%Y-%m-%d")
            self.historical_data[date_str] = {
                "Date": date_str,
                "Open": Decimal(str(row["open"])).quantize(TWOPLACES),
                "High": Decimal(str(row["high"])).quantize(TWOPLACES),
                "Low": Decimal(str(row["low"])).quantize(TWOPLACES),
                "Close": Decimal(str(row["close"])).quantize(TWOPLACES),
                "Volume": int(row["volume"]),
            }

    def _download_data(self):
        pass


def create_pf_chart(
    symbol: str,
    client: SchwabClient,
    box_size: float = 0.01,
    reversal: int = 3,
    duration: float = 1.0,
    period: float = 2.0,
    method: str = "HL",
    style: bool = True,
    trend_lines: bool = True,
) -> PFChart:
    """Create a Point & Figure chart for a symbol.

    Args:
        symbol: Ticker symbol.
        client: Authenticated SchwabClient.
        box_size: Box size as decimal percentage (0.01 = 1%).
        reversal: Reversal box count.
        duration: Chart duration in years.
        period: Years of price history to fetch.
        method: "HL" (high/low) or "C" (close-only).
        style: Enable terminal colors.
        trend_lines: Show support/resistance trend lines.

    Returns:
        PFChart instance with .chart (str) and .chart_meta_data (dict).
    """
    security = SchwabSecurity(
        symbol=symbol,
        client=client,
        period=period,
    )

    chart = PFChart(
        security=security,
        box_size=box_size,
        duration=duration,
        method=method,
        reversal=reversal,
        style=style,
        trend_lines=trend_lines,
    )

    chart.create_chart()
    return chart


def print_pf_chart(
    symbol: str,
    client: SchwabClient,
    box_size: float = 0.01,
    reversal: int = 3,
    duration: float = 1.0,
    period: float = 2.0,
    method: str = "HL",
    style: bool = True,
    trend_lines: bool = True,
    show_meta: bool = False,
) -> None:
    """Create and print a Point & Figure chart to stdout."""
    chart = create_pf_chart(
        symbol=symbol,
        client=client,
        box_size=box_size,
        reversal=reversal,
        duration=duration,
        period=period,
        method=method,
        style=style,
        trend_lines=trend_lines,
    )

    print(chart.chart)

    if show_meta:
        meta = {}
        for date_key, values in chart.chart_meta_data.items():
            row = {}
            for k, v in values.items():
                row[k] = str(v) if not isinstance(v, (str, int, float)) else v
            meta[date_key] = row
        print(json.dumps(meta, indent=2))
