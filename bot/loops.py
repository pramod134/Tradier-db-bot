import asyncio
import math
from datetime import datetime, timezone
from typing import Any, Dict, List

import httpx

from .config import settings
from .logger import log
from . import tradier_client
from . import supabase_client
from . import market_data
from . import spot_indicators


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_float(v: Any) -> Any:
    try:
        if v is None:
            return None
        f = float(v)
        if not math.isfinite(f):
            return None
        return f
    except Exception:
        return None


async def run_positions_loop() -> None:
    """
    Periodically sync positions from Tradier SANDBOX into public.positions.

    NEW: This loop now also fetches LIVE quotes before inserting,
    so each row in positions is born with mark / prev_close / underlier_spot
    already filled as best as possible.
    """
    interval = max(3, settings.poll_positions_sec)
    log(
        "info",
        "positions_loop_start",
        interval=interval,
        sandbox_accounts=settings.tradier_sandbox_accounts,
    )

    while True:
        start = datetime.now(timezone.utc)
        try:
            async with httpx.AsyncClient() as client:
                # 1) Fetch all positions from sandbox first
                raw_positions: List[Dict[str, Any]] = []
                for account_id in settings.tradier_sandbox_accounts:
                    positions = await tradier_client.fetch_positions(client, account_id)
                    log(
                        "info",
                        "tradier_sandbox_positions_fetched",
                        account_id=account_id,
                        count=len(positions),
                    )
                    for p in positions:
                        p["_account_id"] = account_id  # keep for building id later
                        raw_positions.append(p)

                # If nothing, skip
                if not raw_positions:
                    await asyncio.sleep(interval)
                    continue

                # 2) Build symbol list for quotes (live)
                symbols_to_quote: List[str] = []
                # We'll also precompute some metadata for each position
                enriched: List[Dict[str, Any]] = []

                for p in raw_positions:
                    account_id = p["_account_id"]
                    sym_raw = str(p.get("symbol", "")).upper()
                    if not sym_raw:
                        continue

                    qty = int(p.get("quantity", 0) or 0)
                    cost_basis_total = float(p.get("cost_basis", 0) or 0.0)
                    avg_cost = cost_basis_total / qty if qty not in (0, 0.0) else None

                    inst = p.get("instrument") or {}
                    inst_type = str(inst.get("asset_type", "")).lower()
                    # Treat as option if Tradier says "option" or if it's a long OCC-like symbol
                    is_option = inst_type == "option" or len(sym_raw) > 15

                    asset_type = "option" if is_option else "equity"
                    contract_multiplier = 100 if is_option else 1

                    # For options, symbol is OCC, occ = OCC
                    symbol = sym_raw
                    occ = sym_raw if is_option else None

                    # Try to find underlier symbol for options from instrument if present
                    underlier_symbol = ""
                    if is_option:
                        # Tradier often has "underlying" or the underlier in "symbol" field of instrument
                        underlier_symbol = (
                            str(inst.get("underlying") or inst.get("symbol") or "")
                            .upper()
                            .strip()
                        )

                    # Collect for quotes:
                    if asset_type == "equity":
                        symbols_to_quote.append(symbol)  # equity symbol itself
                    else:
                        # Option: need both OCC and underlier for spot
                        symbols_to_quote.append(symbol)  # option quote
                        if underlier_symbol:
                            symbols_to_quote.append(underlier_symbol)

                    enriched.append(
                        {
                            "account_id": account_id,
                            "symbol": symbol,
                            "occ": occ,
                            "asset_type": asset_type,
                            "qty": qty,
                            "avg_cost": avg_cost,
                            "contract_multiplier": contract_multiplier,
                            "underlier_symbol": underlier_symbol,
                        }
                    )

                # 3) Fetch LIVE quotes for all collected symbols
                async with httpx.AsyncClient() as live_client:
                    quotes = await tradier_client.fetch_quotes(live_client, symbols_to_quote)

                # 4) Build fully-populated rows and upsert into positions
                current_ids: List[str] = []

                for pos in enriched:
                    account_id = pos["account_id"]
                    symbol = pos["symbol"]
                    occ = pos["occ"]
                    asset_type = pos["asset_type"]
                    qty = pos["qty"]
                    avg_cost = pos["avg_cost"]
                    contract_multiplier = pos["contract_multiplier"]
                    underlier_symbol = pos["underlier_symbol"]

                    mark = None
                    prev_close = None
                    underlier_spot = None

                    if asset_type == "option":
                        # Option mark from OCC symbol
                        oq = quotes.get(symbol)
                        if oq:
                            mark = oq.get("last") or oq.get("close")
                            prev_close = oq.get("prevclose")

                        # Underlier spot from underlying symbol, if we have it
                        if underlier_symbol:
                            uq = quotes.get(underlier_symbol)
                            if uq:
                                underlier_spot = uq.get("last") or uq.get("close")
                    else:
                        # Equity: mark and spot from same symbol
                        sq = quotes.get(symbol)
                        if sq:
                            mark = sq.get("last") or sq.get("close")
                            prev_close = sq.get("prevclose")
                            underlier_spot = mark

                    # Build primary key id
                    pid = supabase_client.build_tradier_id(account_id, symbol)

                    row: Dict[str, Any] = {
                        "id": pid,
                        "symbol": symbol,
                        "asset_type": asset_type,
                        "occ": occ,
                        "qty": qty,
                        "avg_cost": _safe_float(avg_cost),
                        "mark": _safe_float(mark),
                        "prev_close": _safe_float(prev_close),
                        "contract_multiplier": contract_multiplier,
                        "underlier_spot": _safe_float(underlier_spot),
                        "last_updated": _now_iso(),
                    }

                    current_ids.append(pid)
                    status = supabase_client.upsert_position_row(row)
                    log(
                        "info",
                        "position_upsert",
                        id=pid,
                        asset_type=asset_type,
                        qty=qty,
                        status=status,
                        env="sandbox+live",
                    )

                # 5) Delete sandbox-origin positions that no longer exist
                supabase_client.delete_missing_tradier_positions(current_ids)

        except Exception as e:
            log("error", "positions_loop_error", error=str(e))

        elapsed = (datetime.now(timezone.utc) - start).total_seconds()
        sleep_for = max(0, interval - elapsed)
        await asyncio.sleep(sleep_for)


async def run_quotes_loop() -> None:
    """
    Periodically refresh quote fields for active positions using LIVE quotes:
    - mark
    - prev_close
    - underlier_spot

    This now acts as a refresher: positions are initially born with quotes
    in the positions loop, and this loop keeps them up to date.
    """
    interval = max(2, settings.poll_quotes_sec)
    log("info", "quotes_loop_start", interval=interval)

    while True:
        start = datetime.now(timezone.utc)
        try:
            active = supabase_client.fetch_active_tradier_positions()
            if not active:
                await asyncio.sleep(interval)
                continue

            symbols_to_quote: List[str] = []
            for r in active:
                symbol = str(r.get("symbol", "")).upper()
                underlier = str(r.get("underlier") or "").upper()

                if symbol:
                    symbols_to_quote.append(symbol)
                if underlier:
                    symbols_to_quote.append(underlier)

            async with httpx.AsyncClient() as client:
                quotes = await tradier_client.fetch_quotes(client, symbols_to_quote)

            for r in active:
                pid = r["id"]
                symbol = str(r.get("symbol", "")).upper()
                underlier = str(r.get("underlier") or "").upper()
                asset_type = r.get("asset_type")

                mark = None
                prev_close = None
                underlier_spot = None

                if asset_type == "option":
                    oq = quotes.get(symbol)
                    if oq:
                        mark = oq.get("last") or oq.get("close")
                        prev_close = oq.get("prevclose")

                    if underlier:
                        uq = quotes.get(underlier)
                        if uq:
                            underlier_spot = uq.get("last") or uq.get("close")
                else:
                    sq = quotes.get(symbol)
                    if sq:
                        mark = sq.get("last") or sq.get("close")
                        prev_close = sq.get("prevclose")
                        underlier_spot = mark

                fields: Dict[str, Any] = {
                    "mark": _safe_float(mark),
                    "prev_close": _safe_float(prev_close),
                    "underlier_spot": _safe_float(underlier_spot),
                    "last_updated": _now_iso(),
                }
                supabase_client.update_quote_fields(pid, fields)

            log("info", "quotes_updated", count=len(active), env="live")

        except Exception as e:
            log("error", "quotes_loop_error", error=str(e))

        elapsed = (datetime.now(timezone.utc) - start).total_seconds()
        sleep_for = max(0, interval - elapsed)
        await asyncio.sleep(sleep_for)


async def run_spot_indicators_loop() -> None:
    """
    Periodically compute indicator snapshots (swings, FVG, liquidity, volume profile, trend)
    for each *underlier* symbol in the spot table.

    To avoid Yahoo 429:
      - process only a few symbols per cycle
      - only 1 timeframe per cycle (rotating through 5m, 15m, 1h, 1d)
      - small delay between calls
    """
    # Use poll_spot_tf_sec if defined in settings, else default to 900s (15 minutes),
    # and never less than 300s (5 minutes).
    raw_interval = getattr(settings, "poll_spot_tf_sec", 900)
    try:
        raw_interval = int(raw_interval)
    except Exception:
        raw_interval = 900
    interval = max(300, raw_interval)

    log("info", "spot_indicators_loop_start", interval=interval)

    # We will rotate through these timeframes across cycles
    tf_cycle = [
        ("5m", "scalp"),
        ("15m", "day"),
        ("1h", "day"),
        ("1d", "swing"),
    ]
    tf_index = 0  # local cycle pointer

    MAX_SYMBOLS_PER_CYCLE = 3
    PER_REQUEST_DELAY_SEC = 1.0  # 1 second between calls

    while True:
        start = datetime.now(timezone.utc)

        tf, use_case = tf_cycle[tf_index]
        tf_index = (tf_index + 1) % len(tf_cycle)

        try:
            symbols = supabase_client.fetch_spot_symbols_for_indicators(
                max_symbols=MAX_SYMBOLS_PER_CYCLE
            )
            if not symbols:
                log("info", "spot_indicators_no_symbols")
            else:
                log(
                    "info",
                    "spot_indicators_symbols",
                    timeframe=tf,
                    use_case=use_case,
                    count=len(symbols),
                    symbols=symbols,
                )

                async with httpx.AsyncClient() as client:
                    for symbol in symbols:
                        try:
                            candles = await market_data.fetch_candles(
                                client,
                                symbol=symbol,
                                interval=tf,
                                lookback=500,
                            )
                            if len(candles) < 30:
                                log(
                                    "info",
                                    "spot_indicators_not_enough_candles",
                                    symbol=symbol,
                                    timeframe=tf,
                                    count=len(candles),
                                )
                                await asyncio.sleep(PER_REQUEST_DELAY_SEC)
                                continue

                            snapshot = spot_indicators.compute_spot_snapshot(
                                candles=candles,
                                timeframe=tf,
                                use_case=use_case,
                                fractal=2,
                            )

                            supabase_client.upsert_spot_tf_row(symbol, snapshot)
                            log(
                                "info",
                                "spot_indicators_upserted",
                                symbol=symbol,
                                timeframe=tf,
                                use_case=use_case,
                            )

                            await asyncio.sleep(PER_REQUEST_DELAY_SEC)

                        except httpx.HTTPStatusError as exc:
                            status = exc.response.status_code
                            if status == 429:
                                log(
                                    "error",
                                    "spot_indicators_rate_limited",
                                    symbol=symbol,
                                    timeframe=tf,
                                    status=status,
                                    detail=str(exc),
                                )
                                # On 429, break this cycle and try again next interval
                                break
                            else:
                                log(
                                    "error",
                                    "spot_indicators_http_error",
                                    symbol=symbol,
                                    timeframe=tf,
                                    status=status,
                                    detail=str(exc),
                                )
                                await asyncio.sleep(PER_REQUEST_DELAY_SEC)

                        except Exception as inner_e:
                            log(
                                "error",
                                "spot_indicators_symbol_tf_error",
                                symbol=symbol,
                                timeframe=tf,
                                error=str(inner_e),
                            )
                            await asyncio.sleep(PER_REQUEST_DELAY_SEC)

        except Exception as e:
            log("error", "spot_indicators_loop_error", error=str(e))

        elapsed = (datetime.now(timezone.utc) - start).total_seconds()
        sleep_for = max(0, interval - elapsed)
        await asyncio.sleep(sleep_for)
