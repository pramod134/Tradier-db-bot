from typing import Any, Dict, List

from supabase import Client, create_client

from .config import settings
from .logger import log

sb: Client = create_client(settings.supabase_url, settings.supabase_key)


def build_tradier_id(account_id: str, symbol: str) -> str:
    # Single stable primary key per Tradier account + symbol/OCC
    return f"tradier:{account_id}:{symbol.upper()}"


def upsert_position_row(row: Dict[str, Any]) -> str:
    """
    Upsert by primary key 'id' using Supabase's upsert.
    Row MUST contain 'id'.
    """
    sb.table("positions").upsert(row, on_conflict="id").execute()
    return "upserted"


def delete_missing_tradier_positions(current_ids: List[str]) -> None:
    """
    Delete positions whose id starts with 'tradier:' but are not in current_ids.
    This prevents touching other brokers.
    """
    current_set = set(current_ids)
    # Fetch only Tradier-origin rows (id LIKE 'tradier:%')
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
    Also select generated 'underlier' for options.
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
    sb.table("positions").update(fields).eq("id", pid).execute()
