import asyncio
import uuid
from datetime import datetime, timezone, timedelta, date
from typing import Any, Dict, List, Optional, Tuple

import httpx
from supabase import Client, create_client

from .config import settings
from .logger import log
from . import tradier_client


# ---------- Supabase client (local to this module) ----------

_sb: Client | None = None


def get_client() -> Client:
    global _sb
    if _sb is None:
        _sb = create_client(settings.supabase_url, settings.supabase_key)
    return _sb


# ---------- Generic helpers ----------

def _safe_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        f = float(v)
        if f != f:  # NaN
            return None
        return f
    except Exception:
        return None


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_cp(raw: Any) -> Tuple[Optional[str], Optional[str]]:
    """
    Normalize cp field from new_trades.

    Returns:
      (cp_db, cp_dir)

      cp_db  = value suitable for cp_enum in DB: 'call' | 'put' | None
      cp_dir = direction flag for logic: 'C' | 'P' | None
    """
    if raw is None:
        return None, None

    s = str(raw).strip().lower()
    if not s:
        return None, None

    if s in ("call", "c", "buy_call", "long_call"):
        return "call", "C"
    if s in ("put", "p", "buy_put", "long_put"):
        return "put", "P"

    # Unknown / invalid
    return None, None


def _parse_trade_type(raw: Any) -> str:
    s = (raw or "").strip().lower()
    if not s:
        return "swing"
    return s


# ---------- Underlier spot helpers ----------

async def _get_underlier_spot(
    client: httpx.AsyncClient,
    symbol: str,
    max_attempts: int = 3,
    delay_sec: float = 2.0,
) -> Optional[float]:
    """
    Try to get the underlying spot price.

    1) First try DB 'spot' table (read-only).
    2) If missing, retry a few times via Tradier live quotes.
    3) If still missing, return None (caller will skip this trade).
    """
    sb = get_client()
    symbol_u = (symbol or "").upper()
    if not symbol_u:
        return None

    # 1) DB first
    try:
        resp = (
            sb.table("spot")
            .select("last_price")
            .eq("instrument_id", symbol_u)
            .limit(1)
            .execute()
        )
        rows = getattr(resp, "data", None) or []
        if rows:
            lp = _safe_float(rows[0].get("last_price"))
            if lp is not None:
                return lp
    except Exception as e:
        log("error", "nt_import_spot_db_error", symbol=symbol_u, error=str(e))

    # 2) Tradier with retries
    for attempt in range(1, max_attempts + 1):
        try:
            quotes = await tradier_client.fetch_quotes(client, [symbol_u])
            q = quotes.get(symbol_u)
            if q:
                last = _safe_float(q.get("last"))
                if last is None:
                    # fall back to mid of bid/ask
                    bid = _safe_float(q.get("bid"))
                    ask = _safe_float(q.get("ask"))
                    if bid is not None and ask is not None:
                        last = (bid + ask) / 2.0
                if last is not None:
                    log(
                        "info",
                        "nt_import_spot_tradier_ok",
                        symbol=symbol_u,
                        attempt=attempt,
                        price=last,
                    )
                    return last
        except Exception as e:
            log(
                "error",
                "nt_import_spot_tradier_error",
                symbol=symbol_u,
                attempt=attempt,
                error=str(e),
            )

        if attempt < max_attempts and delay_sec > 0:
            await asyncio.sleep(delay_sec)

    log("error", "nt_import_spot_failed", symbol=symbol_u, attempts=max_attempts)
    return None


# ---------- Defaults helpers ----------

def _fetch_trade_defaults(asset_type: str, trade_type: str) -> Optional[Dict[str, Any]]:
    """
    Load a single row from trade_defaults for given asset_type + trade_type.
    We expect you have global defaults (symbol IS NULL).
    """
    sb = get_client()
    try:
        resp = (
            sb.table("trade_defaults")
            .select("*")
            .eq("asset_type", asset_type)
            .eq("trade_type", trade_type)
            .limit(1)
            .execute()
        )
        rows = getattr(resp, "data", None) or []
        if not rows:
            log(
                "error",
                "nt_import_no_defaults",
                asset_type=asset_type,
                trade_type=trade_type,
            )
            return None
        return rows[0]
    except Exception as e:
        log(
            "error",
            "nt_import_defaults_error",
            asset_type=asset_type,
            trade_type=trade_type,
            error=str(e),
        )
        return None


# ---------- Option helpers (OCC, chain snap) ----------

def _build_occ(symbol: str, expiry_date: date, cp_dir: str, strike: float) -> str:
    """
    Build an OCC-style option symbol in Tradier format, e.g.:

      AMD, 2025-09-19, call, 160.0  -> "AMD250919C00160000"
      LLY, 2025-12-16, call, 1160.0 -> "LLY251216C01160000"

    Format (no spaces):
      ROOT (1–6 chars, uppercased ticker) +
      YYMMDD +
      C/P +
      STRIKE (8 digits = round(strike * 1000))
    """
    root = (symbol or "").upper().strip()

    yy = expiry_date.year % 100
    mm = expiry_date.month
    dd = expiry_date.day

    cp_letter = (cp_dir or "C").upper()
    if cp_letter not in ("C", "P"):
        cp_letter = "C"

    strike_int = int(round(float(strike) * 1000))
    strike_code = f"{strike_int:08d}"

    return f"{root}{yy:02d}{mm:02d}{dd:02d}{cp_letter}{strike_code}"


def _snap_strike_to_tradier_chain(
    symbol: str,
    expiry_date: date,
    target_strike: float,
) -> float:
    """
    Ask Tradier for the option chain for (symbol, expiry_date) and
    snap target_strike to the nearest listed strike.

    If anything fails (no chain, HTTP error, etc.), we fall back to
    rounding target_strike to the nearest 5 dollars so we at least
    don't end up with weird decimals like 1165.44.
    """
    base = settings.tradier_live_base.rstrip("/")
    url = f"{base}/markets/options/chains"  # <-- no extra /v1

    params = {
        "symbol": (symbol or "").upper(),
        "expiration": expiry_date.strftime("%Y-%m-%d"),
        "greeks": "false",
    }

    headers = {
        "Authorization": f"Bearer {settings.tradier_live_token}",
        "Accept": "application/json",
    }

    try:
        with httpx.Client(timeout=5.0) as client:
            resp = client.get(url, params=params, headers=headers)
            resp.raise_for_status()
            data = resp.json()

        options = (data.get("options") or {}).get("option") or []
        strikes: List[float] = []

        for opt in options:
            s = _safe_float(opt.get("strike"))
            if s is not None:
                strikes.append(s)

        if strikes:
            nearest = min(strikes, key=lambda s: abs(s - target_strike))
            log(
                "info",
                "nt_import_strike_snap",
                symbol=symbol.upper(),
                expiry=expiry_date.isoformat(),
                target=target_strike,
                snapped=nearest,
            )
            return nearest

        # No strikes at all for this expiry → log and fall back.
        log(
            "error",
            "nt_import_chain_empty",
            symbol=symbol.upper(),
            expiry=expiry_date.isoformat(),
            target=target_strike,
        )

    except Exception as e:
        log(
            "error",
            "nt_import_chain_snap_error",
            symbol=symbol.upper(),
            expiry=expiry_date.isoformat(),
            target=target_strike,
            error=str(e),
        )

    # ---- Fallback: round to nearest $5 ----
    rounded = round(target_strike / 5.0) * 5.0
    log(
        "info",
        "nt_import_strike_round_fallback",
        symbol=symbol.upper(),
        expiry=expiry_date.isoformat(),
        target=target_strike,
        rounded=rounded,
    )
    return rounded


def _snap_expiry_to_tradier(symbol: str, target_days: int) -> date:
    """
    Use Tradier's expirations endpoint to find a *real* expiry
    close to today + target_days.

    If anything fails, we fall back to today + target_days.
    """
    today = datetime.now(timezone.utc).date()
    target_date = today + timedelta(days=target_days)

    base = settings.tradier_live_base.rstrip("/")
    url = f"{base}/markets/options/expirations"

    params = {
        "symbol": (symbol or "").upper(),
        "includeAllRoots": "true",
        "strikes": "false",
    }

    headers = {
        "Authorization": f"Bearer {settings.tradier_live_token}",
        "Accept": "application/json",
    }

    try:
        with httpx.Client(timeout=5.0) as client:
            resp = client.get(url, params=params, headers=headers)
            resp.raise_for_status()
            data = resp.json()

        raw_dates = (data.get("expirations") or {}).get("date") or []
        parsed: List[date] = []
        for d in raw_dates:
            try:
                parsed.append(datetime.strptime(d, "%Y-%m-%d").date())
            except Exception:
                continue

        if not parsed:
            log(
                "error",
                "nt_import_expiry_empty",
                symbol=symbol.upper(),
                target=target_date.isoformat(),
            )
            return target_date

        # Prefer expiries in the future; if none, use all.
        future = [d for d in parsed if d >= today] or parsed

        chosen = min(future, key=lambda d: abs((d - target_date).days))
        log(
            "info",
            "nt_import_expiry_snap",
            symbol=symbol.upper(),
            target=target_date.isoformat(),
            chosen=chosen.isoformat(),
        )
        return chosen

    except Exception as e:
        log(
            "error",
            "nt_import_expiry_snap_error",
            symbol=symbol.upper(),
            target_days=target_days,
            error=str(e),
        )
        return target_date


def _compute_option_strike_and_expiry(
    row: Dict[str, Any],
    defaults: Dict[str, Any],
    spot: float,
    cp_dir: Optional[str],
) -> Dict[str, Any]:
    """
    Decide strike + expiry + occ for a new option trade.

    - If user provided strike/expiry, we respect them (parsing best-effort).
    - Otherwise:
      * expiry: today + weeks_from_defaults, snapped to a real Tradier expiry.
      * strike: +/- % from spot, depending on call/put, then snapped to chain.
    """
    symbol = (row.get("symbol") or "").upper()

    # ----- STRIKE TARGET -----
    user_strike = _safe_float(row.get("strike"))
    strike_pct = _safe_float(defaults.get("option_strike_pct")) or 0.05

    # Direction: C (call) / P (put)
    cp_letter = (cp_dir or "C").upper()
    if cp_letter not in ("C", "P"):
        cp_letter = "C"

    if user_strike is not None:
        target_strike = user_strike
    else:
        if cp_letter == "P":
            target_strike = spot * (1.0 - strike_pct)
        else:  # call (default)
            target_strike = spot * (1.0 + strike_pct)

    # ----- EXPIRY -----
    expiry_text = (row.get("expiry") or "").strip()
    if expiry_text:
        try:
            expiry_date = datetime.strptime(expiry_text, "%Y-%m-%d").date()
        except Exception:
            # If user expiry is malformed, fall back to default logic
            weeks = int(defaults.get("expiry_weeks") or 3)
            target_days = weeks * 7
            expiry_date = _snap_expiry_to_tradier(symbol, target_days)
    else:
        weeks = int(defaults.get("expiry_weeks") or 3)
        target_days = weeks * 7
        expiry_date = _snap_expiry_to_tradier(symbol, target_days)

    # ----- SNAP STRIKE TO REAL CHAIN (OR SAFE FALLBACK) -----
    final_strike = _snap_strike_to_tradier_chain(symbol, expiry_date, target_strike)

    # ----- OCC CODE -----
    occ = _build_occ(symbol, expiry_date, cp_letter, final_strike)

    return {
        "strike": final_strike,
        "expiry": expiry_date.isoformat(),
        "occ": occ,
    }


# ---------- Entry / SL / TP helpers ----------

def _decide_entry_and_sl_conds(
    asset_type: str,
    cp_dir: Optional[str],
    entry_cond: Optional[str],
    entry_level: Optional[float],
    entry_tf: Optional[str],
    sl_cond: Optional[str],
    sl_level: Optional[float],
) -> Dict[str, Optional[str]]:
    """
    Decide entry_cond and sl_cond based on rules:

      - If no entry_cond, entry_level, entry_tf → entry_cond = 'now'.
      - For options:
          Calls: default entry_cond 'ca', sl_cond 'cb'
          Puts:  default entry_cond 'cb', sl_cond 'ca'
      - For equities: we leave user values as-is; if all missing → 'now'.
    """
    atype = (asset_type or "").lower()
    cp_u = (cp_dir or "").upper() if cp_dir else None

    # No entry parameters at all → enter now
    if not entry_cond and entry_level is None and not entry_tf:
        entry_cond = "now"

    # For options, if we have a level but no condition, base on cp_dir
    if atype == "option" and cp_u in ("C", "P"):
        if not entry_cond and entry_level is not None:
            if cp_u == "C":
                entry_cond = "ca"  # close above level
            else:
                entry_cond = "cb"  # close below level

        if not sl_cond and sl_level is not None:
            if cp_u == "C":
                sl_cond = "cb"  # stop if close below SL
            else:
                sl_cond = "ca"  # stop if close above SL

    return {
        "entry_cond": entry_cond,
        "sl_cond": sl_cond,
    }


def _compute_sl_tp_levels(
    asset_type: str,
    cp_dir: Optional[str],
    spot_price: float,
    defaults: Dict[str, Any],
    existing_sl_level: Optional[float],
    existing_tp_level: Optional[float],
) -> Dict[str, Optional[float]]:
    """
    Compute SL and TP levels from spot using defaults if not already provided.

    All SL/TP are based on UNDERLYING spot (equity), even for options.

    Defaults table fields:
      - sl_pct
      - tp_pct
    """
    sl_pct = _safe_float(defaults.get("sl_pct")) or 0.0
    tp_pct = _safe_float(defaults.get("tp_pct")) or 0.0

    atype = (asset_type or "").lower()
    cp_u = (cp_dir or "").upper() if cp_dir else None

    sl_level = existing_sl_level
    tp_level = existing_tp_level

    if sl_level is None or tp_level is None:
        if atype == "option" and cp_u in ("C", "P"):
            # Options: use call/put semantics
            if cp_u == "C":
                # Call: bullish
                if sl_level is None and sl_pct > 0:
                    sl_level = spot_price * (1.0 - sl_pct)
                if tp_level is None and tp_pct > 0:
                    tp_level = spot_price * (1.0 + tp_pct)
            else:
                # Put: bearish
                if sl_level is None and sl_pct > 0:
                    sl_level = spot_price * (1.0 + sl_pct)
                if tp_level is None and tp_pct > 0:
                    tp_level = spot_price * (1.0 - tp_pct)
        else:
            # Equity: assume bullish by default
            if sl_level is None and sl_pct > 0:
                sl_level = spot_price * (1.0 - sl_pct)
            if tp_level is None and tp_pct > 0:
                tp_level = spot_price * (1.0 + tp_pct)

    return {
        "sl_level": sl_level,
        "tp_level": tp_level,
    }


def _decide_qty(row: Dict[str, Any], defaults: Dict[str, Any]) -> int:
    qty_row = row.get("qty")
    if qty_row is not None:
        try:
            return int(qty_row)
        except Exception:
            pass
    try:
        return int(defaults.get("default_qty") or 0)
    except Exception:
        return 0


# ---------- Row builders ----------

def _build_active_trade_row(
    row: Dict[str, Any],
    defaults: Dict[str, Any],
    spot_price: float,
) -> Optional[Dict[str, Any]]:
    """
    Build the full active_trades row dict from a new_trades row + defaults + spot.

    Returns None if we cannot safely build a row.
    """
    symbol = (row.get("symbol") or "").upper()
    if not symbol:
        log("error", "nt_import_missing_symbol", row=row)
        return None

    asset_type = (row.get("asset_type") or "").lower()
    if asset_type not in ("equity", "option"):
        log(
            "error",
            "nt_import_bad_asset_type",
            symbol=symbol,
            asset_type=row.get("asset_type"),
        )
        return None

    trade_type = _parse_trade_type(row.get("trade_type"))

    # cp_db = 'call'/'put' for DB; cp_dir = 'C'/'P' for direction logic
    cp_db, cp_dir = _parse_cp(row.get("cp"))

    # Qty
    qty = _decide_qty(row, defaults)
    if qty <= 0:
        log("error", "nt_import_qty_invalid", symbol=symbol, qty=qty)
        return None

    # Entry / SL fields from row
    entry_type = row.get("entry_type") or asset_type
    entry_cond = row.get("entry_cond")
    entry_level = _safe_float(row.get("entry_level"))
    entry_tf = row.get("entry_tf")

    sl_type = row.get("sl_type") or "equity"
    sl_cond = row.get("sl_cond")
    sl_level = _safe_float(row.get("sl_level"))
    sl_tf = row.get("sl_tf") or entry_tf  # default SL TF to entry TF if none

    tp_type = row.get("tp_type") or "equity"
    tp_level = _safe_float(row.get("tp_level"))

    # Decide entry_cond / sl_cond based on rules
    conds = _decide_entry_and_sl_conds(
        asset_type=asset_type,
        cp_dir=cp_dir,
        entry_cond=entry_cond,
        entry_level=entry_level,
        entry_tf=entry_tf,
        sl_cond=sl_cond,
        sl_level=sl_level,
    )
    entry_cond = conds["entry_cond"]
    sl_cond = conds["sl_cond"]

    # If entry_cond is "now" and no entry_level, we can set entry_level = spot for reference
    if entry_cond == "now" and entry_level is None:
        entry_level = spot_price

    # Compute SL/TP levels if missing
    sltp = _compute_sl_tp_levels(
        asset_type=asset_type,
        cp_dir=cp_dir,
        spot_price=spot_price,
        defaults=defaults,
        existing_sl_level=sl_level,
        existing_tp_level=tp_level,
    )
    sl_level = sltp["sl_level"]
    tp_level = sltp["tp_level"]

    # Ensure sl_cond is set whenever we have an SL level for options
    if asset_type == "option" and sl_level is not None and sl_cond is None:
        if cp_dir == "C":
            sl_cond = "cb"   # call: stop if close below
        else:
            sl_cond = "ca"   # put: stop if close above

    # Ensure sl_tf is never NULL when sl_level exists
    if sl_level is not None and not sl_tf:
        sl_tf = entry_tf or defaults.get("entry_tf") or "5m"

    # For options, compute strike/expiry/occ if needed
    strike = None
    expiry_txt = None
    occ = None

    if asset_type == "option":
        opt_info = _compute_option_strike_and_expiry(
            row=row,
            defaults=defaults,
            spot=spot_price,
            cp_dir=cp_dir,
        )
        strike = opt_info["strike"]
        expiry_txt = opt_info["expiry"]
        occ = opt_info["occ"]

        if cp_dir not in ("C", "P"):
            log("error", "nt_import_option_missing_cp", symbol=symbol, row=row)
            return None
        if strike is None or not expiry_txt or not occ:
            log(
                "error",
                "nt_import_option_incomplete",
                symbol=symbol,
                cp=row.get("cp"),
                strike=strike,
                expiry=expiry_txt,
                occ=occ,
            )
            return None

    now_iso = _now_iso()

    active_row: Dict[str, Any] = {
        "id": str(uuid.uuid4()),
        "symbol": symbol,
        "asset_type": asset_type,
        "status": "nt-waiting",
        "qty": qty,
        "cp": cp_db,
        "strike": strike,
        "expiry": expiry_txt,
        "occ": occ,
        "entry_type": entry_type,
        "entry_cond": entry_cond,
        "entry_level": entry_level,
        "entry_tf": entry_tf,
        "sl_type": sl_type,
        "sl_cond": sl_cond,
        "sl_level": sl_level,
        "sl_tf": sl_tf,
        "tp_type": tp_type,
        "tp_level": tp_level,
        "manage": row.get("manage") or "Y",
        "last_close": None,
        "note": row.get("note"),
        "created_at": now_iso,
        "updated_at": now_iso,
        "trade_type": trade_type,
    }

    return active_row


# ---------- DB IO helpers ----------

def _fetch_pending_new_trades() -> List[Dict[str, Any]]:
    """
    Fetch all rows from new_trades. We assume every row here is pending import.
    """
    sb = get_client()
    try:
        resp = sb.table("new_trades").select("*").execute()
        rows = getattr(resp, "data", None) or []
        return rows
    except Exception as e:
        log("error", "nt_import_fetch_error", error=str(e))
        return []


def _insert_active_trade(row: Dict[str, Any]) -> None:
    """
    Insert a single row into active_trades.
    """
    sb = get_client()
    try:
        sb.table("active_trades").insert(row).execute()
    except Exception as e:
        log("error", "nt_import_insert_error", row=row, error=str(e))
        raise


def _delete_new_trade(row_id: Any) -> None:
    """
    Delete a single row from new_trades by id.
    """
    sb = get_client()
    try:
        sb.table("new_trades").delete().eq("id", row_id).execute()
    except Exception as e:
        log("error", "nt_import_delete_error", id=row_id, error=str(e))
        raise


# ---------- Main async loop ----------

async def run_new_trades_import_loop() -> None:
    """
    Periodically:

      1) Fetch all rows from new_trades.
      2) For each:
           - Load trade_defaults by asset_type + trade_type (default swing).
           - Fetch underlying spot (spot table first, then Tradier with retries).
           - Compute qty, SL/TP, strike/expiry/occ (for options, using chain snap).
           - Insert into active_trades with status = nt-waiting, manage = Y.
           - Delete row from new_trades on success.
      3) Sleep, then repeat.
    """
    # Re-use positions poll interval to avoid adding a new env var.
    interval = max(3, settings.poll_positions_sec)

    log("info", "nt_import_loop_start", interval=interval)

    while True:
        start = datetime.now(timezone.utc)
        try:
            rows = _fetch_pending_new_trades()
            if not rows:
                await asyncio.sleep(interval)
                continue

            log("info", "nt_import_rows_found", count=len(rows))

            async with httpx.AsyncClient() as client:
                for row in rows:
                    row_id = row.get("id")
                    symbol = row.get("symbol")

                    try:
                        asset_type = (row.get("asset_type") or "").lower()
                        trade_type = _parse_trade_type(row.get("trade_type"))

                        # 1) Load defaults
                        defaults = _fetch_trade_defaults(asset_type, trade_type)
                        if not defaults:
                            log(
                                "error",
                                "nt_import_skip_no_defaults",
                                id=row_id,
                                symbol=symbol,
                                asset_type=asset_type,
                                trade_type=trade_type,
                            )
                            continue

                        # 2) Fetch underlying spot
                        spot_price = await _get_underlier_spot(client, symbol)
                        if spot_price is None:
                            # Skip for now; row remains in new_trades to retry later
                            log(
                                "error",
                                "nt_import_skip_no_spot",
                                id=row_id,
                                symbol=symbol,
                            )
                            continue

                        # 3) Build active_trades row
                        active_row = _build_active_trade_row(
                            row, defaults, spot_price
                        )
                        if not active_row:
                            log(
                                "error",
                                "nt_import_build_failed",
                                id=row_id,
                                symbol=symbol,
                            )
                            continue

                        # 4) Insert into active_trades
                        _insert_active_trade(active_row)

                        # 5) Delete from new_trades
                        _delete_new_trade(row_id)

                        log(
                            "info",
                            "nt_import_success",
                            id=row_id,
                            symbol=symbol,
                            asset_type=asset_type,
                            trade_type=trade_type,
                        )

                    except Exception as e:
                        # Do NOT delete the row on failure; just log it.
                        log(
                            "error",
                            "nt_import_row_error",
                            id=row_id,
                            symbol=symbol,
                            error=str(e),
                        )

        except Exception as e:
            log("error", "nt_import_loop_error", error=str(e))

        elapsed = (datetime.now(timezone.utc) - start).total_seconds()
        await asyncio.sleep(max(0, interval - elapsed))
