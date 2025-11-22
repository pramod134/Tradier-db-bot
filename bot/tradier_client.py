import httpx
from typing import Any, Dict, List

from .config import settings

# Sandbox auth (positions)
POS_HEADERS = {
    "Authorization": f"Bearer {settings.tradier_sandbox_token}",
    "Accept": "application/json",
}

# Live auth (quotes)
QUOTE_HEADERS = {
    "Authorization": f"Bearer {settings.tradier_live_token}",
    "Accept": "application/json",
}


async def fetch_positions(client: httpx.AsyncClient, account_id: str) -> List[Dict[str, Any]]:
    """
    Sandbox positions: uses sandbox base URL + sandbox token.
    """
    url = f"{settings.tradier_sandbox_base}/accounts/{account_id}/positions"
    r = await client.get(url, headers=POS_HEADERS, timeout=15)
    r.raise_for_status()
    js = r.json().get("positions", {}).get("position")
    if js is None:
        return []
    if isinstance(js, dict):
        return [js]
    return js


async def fetch_quotes(client: httpx.AsyncClient, symbols: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    Live quotes: uses live base URL + live token.
    """
    if not symbols:
        return {}

    unique = sorted(set(s.upper() for s in symbols if s))
    out: Dict[str, Dict[str, Any]] = {}

    for i in range(0, len(unique), 70):
        batch = unique[i : i + 70]
        url = f"{settings.tradier_live_base}/markets/quotes?symbols={','.join(batch)}"
        r = await client.get(url, headers=QUOTE_HEADERS, timeout=15)
        r.raise_for_status()
        qs = r.json().get("quotes", {}).get("quote")
        if not qs:
            continue
        if isinstance(qs, dict):
            qs = [qs]
        for q in qs:
            sym = q.get("symbol", "").upper()
            if sym:
                out[sym] = q

    return out
