# bot/yahoo_candles.py

import datetime as dt
from typing import Any, Dict, List, Optional

import httpx


# Map internal interval -> Yahoo interval string
_INTERVAL_MAP = {
    "1m": "1m",
    "5m": "5m",
    "15m": "15m",
    "30m": "30m",
    "60m": "60m",
    "1h": "60m",
    "1d": "1d",
}


def _default_range(interval: str, lookback: int) -> str:
    """
    Choose a reasonable Yahoo 'range' parameter for given interval + lookback.
    We only need enough candles to compute swings / FVG etc.
    """
    if interval in ("1m", "5m", "15m", "30m", "60m", "1h"):
        # intraday: up to 60d is usually plenty
        return "60d"
    # daily or higher
    return "2y"


async def fetch_yahoo_candles(
    client: httpx.AsyncClient,
    symbol: str,
    interval: str = "5m",
    lookback: int = 500,
) -> List[Dict[str, Any]]:
    """
    Fetch candles from Yahoo Finance for a symbol and interval.

    Returns a list of dicts:
    [
      {"ts": iso_utc, "open": float, "high": float, "low": float, "close": float, "volume": float},
      ...
    ]
    Newest candle is last in the list.
    """
    yf_interval = _INTERVAL_MAP.get(interval)
    if yf_interval is None:
        raise ValueError(f"Unsupported interval: {interval}")

    yf_range = _default_range(interval, lookback)

    url = (
        f"https://query1.finance.yahoo.com/v8/finance/chart/"
        f"{symbol}?interval={yf_interval}&range={yf_range}&includePrePost=false&events=history"
    )

    resp = await client.get(url, timeout=15)
    resp.raise_for_status()
    data = resp.json()

    chart = (data.get("chart") or {}).get("result")
    if not chart:
        return []

    result = chart[0]
    timestamps = result.get("timestamp") or []
    indicators = result.get("indicators") or {}
    quote_arr = (indicators.get("quote") or [{}])[0]

    opens = quote_arr.get("open") or []
    highs = quote_arr.get("high") or []
    lows = quote_arr.get("low") or []
    closes = quote_arr.get("close") or []
    vols = quote_arr.get("volume") or []

    candles: List[Dict[str, Any]] = []
    for ts, o, h, l, c, v in zip(timestamps, opens, highs, lows, closes, vols):
        if o is None or h is None or l is None or c is None:
            continue
        dt_utc = dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc)
        candles.append(
            {
                "ts": dt_utc.isoformat(),
                "open": float(o),
                "high": float(h),
                "low": float(l),
                "close": float(c),
                "volume": float(v or 0.0),
            }
        )

    # keep only the last `lookback` candles
    if len(candles) > lookback:
        candles = candles[-lookback:]

    return candles
