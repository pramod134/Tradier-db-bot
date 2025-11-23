# bot/spot_indicators.py

from typing import Any, Dict, List, Tuple, Optional
import math
from statistics import mean


Candle = Dict[str, Any]


# ---------- Swings & structure ----------

def find_swings(candles: List[Candle], fractal: int = 2) -> Dict[str, List[Dict[str, Any]]]:
    """
    Detect swing highs and lows using a simple fractal rule.
    A swing high at i means: high[i] > high[i-k] and high[i] > high[i+k] for k=1..fractal.
    Similar for swing low.
    """
    highs: List[Dict[str, Any]] = []
    lows: List[Dict[str, Any]] = []

    n = len(candles)
    if n < 2 * fractal + 1:
        return {"swing_highs": highs, "swing_lows": lows}

    for i in range(fractal, n - fractal):
        hi = candles[i]["high"]
        lo = candles[i]["low"]

        is_high = all(hi > candles[i - k]["high"] and hi > candles[i + k]["high"] for k in range(1, fractal + 1))
        is_low = all(lo < candles[i - k]["low"] and lo < candles[i + k]["low"] for k in range(1, fractal + 1))

        if is_high:
            highs.append({"price": hi, "ts": candles[i]["ts"]})
        if is_low:
            lows.append({"price": lo, "ts": candles[i]["ts"]})

    return {"swing_highs": highs, "swing_lows": lows}


def _pick_last_two(points: List[Dict[str, Any]]) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    if not points:
        return None, None
    if len(points) == 1:
        return points[-1], None
    return points[-1], points[-2]


def classify_structure(
    last_high: Optional[Dict[str, Any]],
    prev_high: Optional[Dict[str, Any]],
    last_low: Optional[Dict[str, Any]],
    prev_low: Optional[Dict[str, Any]],
) -> str:
    """
    Basic HH/HL/LH/LL classification.
    """
    if not (last_high and prev_high and last_low and prev_low):
        return "unknown"

    lh = last_high["price"]
    ph = prev_high["price"]
    ll = last_low["price"]
    pl = prev_low["price"]

    up_hi = lh > ph
    up_lo = ll > pl
    down_hi = lh < ph
    down_lo = ll < pl

    if up_hi and up_lo:
        return "HH"
    if not up_hi and up_lo:
        return "HL"
    if down_hi and not down_lo:
        return "LH"
    if down_hi and down_lo:
        return "LL"
    return "range"


# ---------- FVG detection (simplified) ----------

def compute_fvgs(candles: List[Candle]) -> List[Dict[str, Any]]:
    """
    Very simple FVG approximation using 3-candle pattern:
    - Bull FVG if previous high < next low
    - Bear FVG if previous low > next high
    """
    fvgs: List[Dict[str, Any]] = []
    n = len(candles)
    if n < 3:
        return fvgs

    for i in range(1, n - 1):
        prev = candles[i - 1]
        curr = candles[i]
        nxt = candles[i + 1]

        # Bullish gap
        if prev["high"] < nxt["low"]:
            fvgs.append(
                {
                    "type": "bull",
                    "top": nxt["low"],
                    "bottom": prev["high"],
                    "age": n - i,
                    "quality": 1.0,  # placeholder scoring
                }
            )

        # Bearish gap
        if prev["low"] > nxt["high"]:
            fvgs.append(
                {
                    "type": "bear",
                    "top": prev["low"],
                    "bottom": nxt["high"],
                    "age": n - i,
                    "quality": 1.0,
                }
            )

    return fvgs


# ---------- Liquidity (equal highs/lows, simple sweeps) ----------

def compute_liquidity(candles: List[Candle], tol: float = 0.0005) -> Dict[str, Any]:
    """
    Detect approximate equal highs/lows and simple sweeps.
    tol is relative (0.0005 ≈ 0.05%).
    """
    equal_highs: List[float] = []
    equal_lows: List[float] = []
    sweeps: List[Dict[str, Any]] = []

    highs = [c["high"] for c in candles]
    lows = [c["low"] for c in candles]

    n = len(candles)
    if n < 3:
        return {"equal_highs": equal_highs, "equal_lows": equal_lows, "sweeps": sweeps}

    # Equal highs / lows
    for i in range(1, n):
        h0, h1 = highs[i - 1], highs[i]
        l0, l1 = lows[i - 1], lows[i]
        if abs(h1 - h0) / max(h0, 1e-6) <= tol:
            equal_highs.append((h0 + h1) / 2.0)
        if abs(l1 - l0) / max(l0, 1e-6) <= tol:
            equal_lows.append((l0 + l1) / 2.0)

    # Simple sweeps: current high > previous high after equal highs, etc.
    for i in range(2, n):
        # sweep of highs
        if highs[i - 2] in equal_highs and highs[i] > highs[i - 1] > highs[i - 2]:
            sweeps.append({"type": "high", "price": highs[i], "ts": candles[i]["ts"]})
        # sweep of lows
        if lows[i - 2] in equal_lows and lows[i] < lows[i - 1] < lows[i - 2]:
            sweeps.append({"type": "low", "price": lows[i], "ts": candles[i]["ts"]})

    # Deduplicate equal levels
    equal_highs = sorted(set(equal_highs))
    equal_lows = sorted(set(equal_lows))

    return {
        "equal_highs": equal_highs,
        "equal_lows": equal_lows,
        "sweeps": sweeps,
    }


# ---------- Volume profile summary (approx from candles) ----------

def compute_volume_profile(candles: List[Candle], bins: int = 20) -> Dict[str, Any]:
    """
    Approximate volume profile by binning closes into bins with volume weights.
    Returns HVN (top bins), LVN (bottom bins), POC (highest volume bin center).
    """
    if not candles:
        return {}

    closes = [c["close"] for c in candles]
    vols = [c["volume"] for c in candles]
    lo = min(closes)
    hi = max(closes)
    if hi <= lo:
        return {}

    width = (hi - lo) / bins
    if width <= 0:
        return {}

    # build volume per bin
    vol_bins = [0.0] * bins
    for c, v in zip(closes, vols):
        idx = int((c - lo) / width)
        if idx >= bins:
            idx = bins - 1
        vol_bins[idx] += v

    # find top HVN bins and LVN bins
    indexed = list(enumerate(vol_bins))
    nonzero = [x for x in indexed if x[1] > 0]
    if not nonzero:
        return {}

    sorted_by_vol = sorted(nonzero, key=lambda x: x[1], reverse=True)
    hvn_bins = sorted_by_vol[:3]  # top 3
    lvn_bins = sorted(sorted_by_vol[-3:], key=lambda x: x[1])  # 3 lowest nonzero

    def bin_range(idx: int) -> Dict[str, float]:
        low = lo + idx * width
        high = low + width
        return {"low": low, "high": high}

    hvn = [bin_range(i) for i, _ in hvn_bins]
    lvn = [bin_range(i) for i, _ in lvn_bins]

    # POC = center of highest volume bin
    poc_bin = hvn_bins[0][0]
    poc_low = lo + poc_bin * width
    poc = poc_low + width / 2.0

    return {"hvn": hvn, "lvn": lvn, "poc": poc}


# ---------- Trend / momentum ----------

def _ema(values: List[float], period: int) -> List[float]:
    if not values or period <= 1:
        return values[:]
    alpha = 2 / (period + 1)
    ema_vals: List[float] = []
    ema_prev = values[0]
    for v in values:
        ema_prev = alpha * v + (1 - alpha) * ema_prev
        ema_vals.append(ema_prev)
    return ema_vals


def compute_trend(candles: List[Candle]) -> Dict[str, Any]:
    if len(candles) < 20:
        return {}

    closes = [c["close"] for c in candles]
    ema_fast = _ema(closes, 9)
    ema_slow = _ema(closes, 21)

    ef = ema_fast[-1]
    es = ema_slow[-1]

    # slope: compare last EMA vs EMA 5 bars ago
    slope_val = ef - ema_fast[-6] if len(ema_fast) > 6 else 0.0
    if slope_val > 0:
        slope = "up"
    elif slope_val < 0:
        slope = "down"
    else:
        slope = "flat"

    momentum = (ef - es) / max(es, 1e-6)

    return {
        "ema_fast": ef,
        "ema_slow": es,
        "slope": slope,
        "momentum": momentum,
    }


# ---------- High-level snapshot for spot_tf ----------

def compute_spot_snapshot(
    candles: List[Candle],
    timeframe: str,
    use_case: str = "generic",
    fractal: int = 2,
) -> Dict[str, Any]:
    """
    Compute the full indicator snapshot for a given symbol/timeframe
    to be stored in spot_tf row.
    """
    swings_info = find_swings(candles, fractal=fractal)
    sh = swings_info["swing_highs"]
    sl = swings_info["swing_lows"]
    last_high, prev_high = _pick_last_two(sh)
    last_low, prev_low = _pick_last_two(sl)

    structure_state = classify_structure(last_high, prev_high, last_low, prev_low)

    swings_payload = {
        "last_high": last_high,
        "prev_high": prev_high,
        "last_low": last_low,
        "prev_low": prev_low,
    }

    fvgs = compute_fvgs(candles)
    liquidity = compute_liquidity(candles)
    volume_profile = compute_volume_profile(candles)
    trend = compute_trend(candles)

    return {
        "timeframe": timeframe,
        "use_case": use_case,
        "structure_state": structure_state,
        "swings": swings_payload,
        "fvgs": fvgs,
        "liquidity": liquidity,
        "volume_profile": volume_profile,
        "trend": trend,
        "extras": {},
    }
