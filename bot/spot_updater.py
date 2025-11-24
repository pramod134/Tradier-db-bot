# bot/spot_updater.py

import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

import httpx

from .config import settings
from .logger import log
from . import tradier_client
from . import supabase_client


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_float(v: Any) -> Any:
    try:
        if v is None:
            return None
        f = float(v)
        if f != f:  # NaN check
            return None
        return f
    except Exception:
        return None


def _map_instrument_to_tradier_symbol(instrument_id: str, asset_type: str) -> str:
    """
    Map internal instrument_id + asset_type -> Tradier symbol.

    - For equities: instrument_id is the stock symbol, e.g. "SPY", "TSLA".
    - For options: instrument_id is OCC-style, e.g. "O:AMD250919C00160000",
      and Tradier expects "AMD250919C00160000".
    """
    atype = (asset_type or "").lower()

    if atype == "option":
        if instrument_id.startswith("O:"):
            return instrument_id[2:]
        return instrument_id

    # Default: treat as equity
    return instrument_id


def _fetch_spot_rows() -> List[Dict[str, Any]]:
    """
    Load all instruments from public.spot.

    Expected columns:
      - instrument_id (text, PK)
      - asset_type (asset_type_enum: 'equity' | 'option')
    """
    res = supabase_client.sb.table("spot").select("instrument_id, asset_type").execute()
    if res.error:
        raise RuntimeError(f"Error fetching spot rows: {res.error}")
    return res.data or []


def _build_tradier_symbol_map(
    spot_rows: List[Dict[str, Any]]
) -> Tuple[List[str], Dict[str, str]]:
    """
    From spot rows, build:

      - tradier_symbols: list[str] to request from Tradier
      - tradier_to_instrument: map Tradier symbol -> instrument_id
    """
    tradier_symbols: List[str] = []
    tradier_to_instrument: Dict[str, str] = {}

    for row in spot_rows:
        instrument_id = row["instrument_id"]
        asset_type = row.get("asset_type", "equity")
        tsym = _map_instrument_to_tradier_symbol(instrument_id, asset_type)

        # Deduplicate: first instrument wins if multiple map to same Tradier symbol
        tsym_u = tsym.upper()
        if tsym_u not in tradier_to_instrument:
            tradier_to_instrument[tsym_u] = instrument_id
            tradier_symbols.append(tsym_u)

    return tradier_symbols, tradier_to_instrument


def _update_spot_prices(price_map: Dict[str, float], tradier_to_instrument: Dict[str, str]) -> None:
    """
    Update spot.last_price and spot.last_updated for all instruments we have quotes for.

    price_map: {tradier_symbol_upper: last_price}
    tradier_to_instrument: {tradier_symbol_upper: instrument_id}
    """
    if not price_map:
        return

    now_iso = _now_iso()

    for tsym_u, last_price in price_map.items():
        instrument_id = tradier_to_instrument.get(tsym_u)
        if not instrument_id:
            continue

        fields = {
            "last_price": last_price,
            "last_updated": now_iso,
        }

        try:
            supabase_client.sb.table("spot").update(fields).eq(
                "instrument_id", instrument_id
            ).execute()
        except Exception as e:
            log(
                "error",
                "spot_update_error",
                instrument_id=instrument_id,
                symbol=tsym_u,
                error=str(e),
            )


async def run_spot_updater_loop() -> None:
    """
    Periodically refresh public.spot.last_price using Tradier LIVE quotes
    for all instruments in public.spot.

    This is the F0 "price feed" loop that the rest of the system will rely on.
    """
    # Hard-coded 2s as requested; could be made configurable later.
    interval = 2
    log("info", "spot_updater_loop_start", interval=interval, base_url=settings.tradier_live_base)

    async with httpx.AsyncClient() as client:
        while True:
            start = datetime.now(timezone.utc)
            try:
                spot_rows = _fetch_spot_rows()
                if not spot_rows:
                    log("info", "spot_updater_no_rows")
                    await asyncio.sleep(interval)
                    continue

                tradier_symbols, tradier_to_instrument = _build_tradier_symbol_map(spot_rows)

                if not tradier_symbols:
                    log("info", "spot_updater_no_symbols")
                    await asyncio.sleep(interval)
                    continue

                # Ask Tradier for all quotes in batches using existing client helper
                quotes: Dict[str, Dict[str, Any]] = await tradier_client.fetch_quotes(
                    client, tradier_symbols
                )

                price_map: Dict[str, float] = {}
                for tsym_u, q in quotes.items():
                    # Use "last" as primary; fall back to mid of bid/ask if needed.
                    last = _safe_float(q.get("last"))
                    if last is None:
                        bid = _safe_float(q.get("bid"))
                        ask = _safe_float(q.get("ask"))
                        if bid is not None and ask is not None:
                            last = (bid + ask) / 2.0
                    if last is None:
                        # Skip instruments with no usable price
                        continue
                    price_map[tsym_u.upper()] = last

                _update_spot_prices(price_map, tradier_to_instrument)

                log(
                    "info",
                    "spot_updater_cycle_done",
                    count=len(price_map),
                    total=len(tradier_symbols),
                )

            except Exception as e:
                log("error", "spot_updater_loop_error", error=str(e))

            elapsed = (datetime.now(timezone.utc) - start).total_seconds()
            sleep_for = max(0.0, interval - elapsed)
            await asyncio.sleep(sleep_for)


async def main() -> None:
    await run_spot_updater_loop()


if __name__ == "__main__":
    asyncio.run(main())
