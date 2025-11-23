# bot/market_data.py

import os
import datetime as dt
from typing import Any, Dict, List

import httpx

# Internal mapping from your generic intervals to provider-specific strings.
# Right now this is Alpaca's format, but the rest of the bot never sees that.

_INTERVAL_MAP = {
    "5m": (5, "minute"),
    "15m": (15, "minute"),
    "1h": (1, "hour"),
    "1d": (1, "day"),
}



POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
POLYGON_BASE_URL = os.getenv("POLYGON_BASE_URL", "https://api.polygon.io")




async def fetch_candles(
    client: httpx.AsyncClient,
    symbol: str,
    interval: str = "5m",
    limit: int = 1000,
) -> List[Dict[str, Any]]:

    if not POLYGON_API_KEY:
        raise RuntimeError("POLYGON_API_KEY env var is not set")

    if interval not in _INTERVAL_MAP:
        raise ValueError(f"Unsupported interval: {interval}")

    multiplier, timespan = _INTERVAL_MAP[interval]

    # Pull the last few days to always have enough bars
    now = dt.datetime.now(dt.timezone.utc)

    if interval == "1d":
        # Pull ~90 calendar days so we comfortably have 30+ trading days
        start_dt = now - dt.timedelta(days=90)
    else:
        # For intraday (5m, 15m, 1h), 10 days is fine
        start_dt = now - dt.timedelta(days=10)

    start = start_dt.date().isoformat()
    end = now.date().isoformat()


    url = (
        f"{POLYGON_BASE_URL}/v2/aggs/ticker/"
        f"{symbol.upper()}/range/{multiplier}/{timespan}/{start}/{end}"
    )

    params = {
        "adjusted": "true",
        "sort": "asc",
        "limit": max(limit, 5000),
        "apiKey": POLYGON_API_KEY,
    }

    resp = await client.get(url, params=params, timeout=20)
    resp.raise_for_status()
    data = resp.json()

    results = data.get("results") or []
    candles: List[Dict[str, Any]] = []

    for bar in results[-limit:]:
        ts = bar.get("t")
        if ts is None:
            continue
        ts_iso = dt.datetime.fromtimestamp(ts / 1000, tz=dt.timezone.utc).isoformat()

        candles.append(
            {
                "ts": ts_iso,
                "open": float(bar.get("o", 0)),
                "high": float(bar.get("h", 0)),
                "low": float(bar.get("l", 0)),
                "close": float(bar.get("c", 0)),
                "volume": float(bar.get("v", 0)),
            }
        )

    return candles
