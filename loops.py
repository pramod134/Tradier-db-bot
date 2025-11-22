import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, List

import httpx

from .config import settings
from .logger import log
from . import tradier_client
from . import supabase_client


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


async def run_positions_loop() -> None:
    """
    Periodically sync positions from Tradier into public.positions.
    """
    interval = max(3, settings.poll_positions_sec)
    log("info", "positions_loop_start", interval=interval, accounts=settings.tradier_accounts)

    while True:
        start = datetime.now(timezone.utc)
        try:
            async with httpx.AsyncClient() as client:
                current_ids: List[str] = []

                for account_id in settings.tradier_accounts:
                    positions = await tradier_client.fetch_positions(client, account_id)
                    log("info", "tradier_positions_fetched", account_id=account_id, count=len(positions))

                    for p in positions:
                        sym_raw = str(p.get("symbol", "")).upper()
                        if not sym_raw:
                            continue

                        qty = int(p.get("quantity", 0) or 0)
                        cost_basis_total = float(p.get("cost_basis", 0) or 0.0)

                        # Protect against div by zero
                        avg_cost = cost_basis_total / qty if qty not in (0, 0.0) else None

                        # Determine option vs equity
                        # Prefer instrument.asset_type if present
                        inst = p.get("instrument") or {}
                        inst_type = str(inst.get("asset_type", "")).lower()
                        is_option = inst_type == "option" or len(sym_raw) > 15

                        asset_type = "option" if is_option else "equity"
                        contract_multiplier = 100 if is_option else 1

                        # For options we keep symbol = OCC, occ = OCC
                        symbol = sym_raw
                        occ = sym_raw if is_option else None

                        # Build primary key id
                        pid = supabase_client.build_tradier_id(account_id, symbol)

                        row: Dict[str, Any] = {
                            "id": pid,
                            "symbol": symbol,
                            "asset_type": asset_type,
                            "occ": occ,
                            "qty": qty,
                            "avg_cost": avg_cost,
                            "mark": None,          # filled in quotes loop
                            "prev_close": None,    # filled in quotes loop
                            "contract_multiplier": contract_multiplier,
                            "underlier_spot": None,
                            "last_updated": _now_iso(),
                        }

                        current_ids.append(pid)
                        status = supabase_client.upsert_position_row(row)
                        log("info", "position_upsert", id=pid, asset_type=asset_type, qty=qty, status=status)

                # Clean up stale Tradier positions
                supabase_client.delete_missing_tradier_positions(current_ids)

        except Exception as e:
            log("error", "positions_loop_error", error=str(e))

        elapsed = (datetime.now(timezone.utc) - start).total_seconds()
        sleep_for = max(0, interval - elapsed)
        await asyncio.sleep(sleep_for)


async def run_quotes_loop() -> None:
    """
    Periodically refresh quote fields for active positions:
    - mark
    - prev_close
    - underlier_spot
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

                # Always quote symbol (stock or OCC)
                if symbol:
                    symbols_to_quote.append(symbol)
                # For options also quote the underlier if present
                if underlier:
                    symbols_to_quote.append(underlier)

            async with httpx.AsyncClient() as client:
                quotes = await tradier_client.fetch_quotes(client, symbols_to_quote)

            # Update each position
            for r in active:
                pid = r["id"]
                symbol = str(r.get("symbol", "")).upper()
                underlier = str(r.get("underlier") or "").upper()
                asset_type = r.get("asset_type")

                mark = None
                prev_close = None
                underlier_spot = None

                if asset_type == "option":
                    # Option price from symbol (OCC)
                    oq = quotes.get(symbol)
                    if oq:
                        mark = oq.get("last") or oq.get("close")
                        prev_close = oq.get("prevclose")

                    # Underlier spot from underlier symbol
                    if underlier:
                        uq = quotes.get(underlier)
                        if uq:
                            underlier_spot = uq.get("last") or uq.get("close")
                else:
                    # Equity: symbol is the stock itself
                    sq = quotes.get(symbol)
                    if sq:
                        mark = sq.get("last") or sq.get("close")
                        prev_close = sq.get("prevclose")
                        underlier_spot = mark

                fields: Dict[str, Any] = {
                    "mark": mark,
                    "prev_close": prev_close,
                    "underlier_spot": underlier_spot,
                    "last_updated": _now_iso(),
                }
                supabase_client.update_quote_fields(pid, fields)

            log("info", "quotes_updated", count=len(active))

        except Exception as e:
            log("error", "quotes_loop_error", error=str(e))

        elapsed = (datetime.now(timezone.utc) - start).total_seconds()
        sleep_for = max(0, interval - elapsed)
        await asyncio.sleep(sleep_for)
