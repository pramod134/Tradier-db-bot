import math
from datetime import datetime, date
from typing import Any, Dict, List

from supabase import Client, create_client

from .config import settings
from .logger import log


# ---------- JSON sanitization helpers ----------

def _sanitize_value(v: Any) -> Any:
    """
    Make sure a value is safe to send through Supabase's JSON client:
    - datetimes/dates -> ISO strings
    - NaN / +/-inf -> None
    - dicts/lists -> sanitized recursively
    - everything else -> unchanged
    """
    if isinstance(v, (datetime, date)):
        return v.isoformat()

    if isinstance(v, float):
        if not math.isfinite(v):
            return None
        return v

    if isinstance(v, dict):
        return {k: _sanitize_value(x) for k, x in v.items()}

    if isinstance(v, list):
        return [_sanitize_value(x) for x in v]

    return v


def _sanitize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    return {k: _sanitize_value(v) for k, v in row.items()}


# ---------- Supabase client ----------

sb: Client = create_client(settings.supabase_url, settings.supabase_key)


# ---------- Helpers for positions ----------

def build_tradier_id(account_id: str, symbol: str) -> str:
    """
    Build a stable primary key for Tradier positions.
    Example: tradier:12345678:SPY250919C00450000
    """
    return f"tradier:{account_id}:{symbol.upper()}"


def upsert_position_row(row: Dict[str, Any]) -> str:
    """
    Upsert a position row into public.positions.
    Row MUST contain 'id' and any base fields (symbol, asset_type, occ, qty, avg_cost, etc.)

    Uses Supabase 'upsert' on conflict id.
    """
    clean = _sanitize_row(row)
    try:
        sb.table("positions").upsert(clean, on_conflict="id").execute()
    except Exception as e:
        log("error", "supabase_upsert_error", row=clean, error=str(e))
        raise
    return "upserted"


def delete_missing_tradier_positions(current_ids: List[str]) -> None:
    """
    Delete positions whose id starts with 'tradier:' but are not in current_ids.
    This prevents touching other brokers' rows.
    """
    current_set = set(current_ids)

    res = sb.table("positions").select("id").like("id", "tradier:%").execute()
    rows = res.data or []

    for r in rows:
        pid = r["id"]
        if pid not in current_set:
            sb.table("positions").delete().eq("id", pid).execute()
            log("info", "deleted_stale_position", id=pid)


def fetch_active_tradier_positions() -> List[Dict[str, Any]]:
    """
    Get all non-zero qty positions for Tradier (id like 'tradier:%').
    Also select generated 'underlier' for options so quotes loop can fetch underlier spot.
    """
    res = (
        sb.table("positions")
        .select("id,symbol,occ,asset_type,contract_multiplier,qty,avg_cost,underlier")
        .neq("qty", 0)
        .like("id", "tradier:%")
        .execute()
    )
    return res.data or []


def update_quote_fields(pid: str, fields: Dict[str, Any]) -> None:
    """
    Update mark / prev_close / underlier_spot / last_updated for a given position id.
    """
    clean = _sanitize_row(fields)
    try:
        sb.table("positions").update(clean).eq("id", pid).execute()
    except Exception as e:
        # This will dump the exact payload that could not be JSON-encoded
        log("error", "supabase_update_error", id=pid, fields=clean, error=str(e))
        raise
