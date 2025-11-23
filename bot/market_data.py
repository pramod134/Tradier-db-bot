# bot/market_data.py

import datetime as dt
from typing import Any, Dict, List

import httpx

# Internal mapping from your generic intervals to provider-specific strings.
# Right now this is Alpaca's format, but the rest of the bot never sees that.
_INTERVAL_MAP = {
    "1m": "1Min",
    "5m": "5Min",
    "15m": "15Min",
    "1h": "1Hour",
    "1d": "1Day",
}


async def fetch_candles(
    client: httpx.AsyncClient,
    symbol: str,
    interval: str = "5m",
    limit: int = 1000,
) -> List[Dict[str, Any]]:
    """
    Generic candle fetcher for the bot.

    Today: implemented using Alpaca's free data API (no auth required).
    Tomorrow: you can swap internals to Polygon / Yahoo / whatever,
    as long as this function keeps the same signature + output format.

    Returns list of candles:
    [
      {"ts": iso_utc, "open": float, "high": float,
       "low": float, "close": float, "volume": float},
      ...
    ]
    Newest candle is last.
    """
    provider_tf = _INTERVAL_MAP.get(interval)
    if not provider_tf:
        raise ValueError(f"Unsupported interval: {interval}")

    url = (
        f"https://data.alpaca.markets/v2/stocks/{symbol}/bars"
        f"?timeframe={provider_tf}&limit={limit}&adjustment=all"
    )

    resp = await client.get(url, timeout=20)
    resp.raise_for_status()
    data = resp.json()

    bars = data.get("bars") or []
    candles: List[Dict[str, Any]] = []

    for bar in bars:
        # Alpaca timestamps are ISO8601 with Z; normalize to explicit UTC offset
        ts = bar["t"].replace("Z", "+00:00")
        candles.append(
            {
                "ts": dt.datetime.fromisoformat(ts).isoformat(),
                "open": float(bar["o"]),
                "high": float(bar["h"]),
                "low": float(bar["l"]),
                "close": float(bar["c"]),
                "volume": float(bar["v"]),
            }
        )

    return candles
