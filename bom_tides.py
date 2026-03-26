"""
Project Horizon — BOM Tidal Prediction Module
Fetches live tide predictions from the Bureau of Meteorology XML feed for the
active port's tidal gauge station.  Falls back to the deterministic cosine model
on any fetch/parse failure so the server never goes down due to BOM issues.

Cache: 60 minutes in memory per station ID.
"""

import hashlib
import logging
import math
import threading
import time
from datetime import datetime, timedelta, timezone

log = logging.getLogger("horizon.bom_tides")

# ── In-memory cache ───────────────────────────────────────────────────────────
_cache: dict = {}       # station_id -> {"series": [...], "fetched_at": float}
_fail_cache: dict = {}  # station_id -> failed_at (monotonic) — suppresses retries
_cache_lock = threading.Lock()
CACHE_TTL_SECS  = 3600   # 60 minutes (successful fetch)
FAIL_TTL_SECS   = 1800   # 30 minutes — don't hammer a dead BOM endpoint


def _cosine_fallback(now: datetime = None, profile: dict = None) -> list:
    """
    Generate 48 hours of tide points at 30-minute intervals using a
    deterministic cosine model with port-specific tidal parameters.
    Returns list of {"datetime": dt, "type": "HW"|"LW"|None, "height_m": float}.
    """
    if now is None:
        now = datetime.now(tz=timezone.utc).replace(microsecond=0)

    PERIOD = 12.42
    # Use port-specific tidal parameters if available, else generic defaults
    MEAN   = (profile or {}).get("tidal_mean_m", 1.4)
    AMP    = (profile or {}).get("tidal_amp_m",  0.9)

    day_h   = hashlib.md5(f"tide-{now.strftime('%Y%m%d')}".encode()).hexdigest()
    phase_h = (int(day_h[0:4], 16) % int(PERIOD * 100)) / 100.0

    points = []
    for step in range(0, 48 * 60 + 1, 30):
        dt     = now + timedelta(minutes=step)
        t      = (dt.hour + dt.minute / 60.0 + phase_h) % PERIOD
        height = round(MEAN + AMP * math.cos(2 * math.pi * t / PERIOD), 2)
        points.append({"datetime": dt, "type": None, "height_m": height})

    # Label HW/LW turning points (local extrema in the series).
    # Threshold is proportional to amplitude so small-range ports (e.g. Melbourne
    # AMP≈0.25m) get turning points labelled correctly.  Old flat 0.05m threshold
    # failed for any port with AMP < ~0.3m.
    _amp     = (profile or {}).get("tidal_amp_m", 0.9)
    _min_gap = max(_amp * 0.04, 0.004)   # e.g. 0.01m for Melbourne, 0.036m for Brisbane
    for i in range(1, len(points) - 1):
        prev_h = points[i - 1]["height_m"]
        curr_h = points[i]["height_m"]
        next_h = points[i + 1]["height_m"]
        if curr_h >= prev_h and curr_h >= next_h and (curr_h - prev_h + curr_h - next_h) > _min_gap:
            points[i]["type"] = "HW"
        elif curr_h <= prev_h and curr_h <= next_h and (prev_h - curr_h + next_h - curr_h) > _min_gap:
            points[i]["type"] = "LW"

    return points


def _interpolate_from_turning_points(events: list, now: datetime, profile: dict = None) -> list:
    """
    Given a list of HW/LW turning point events, interpolate tide height at
    30-minute intervals over 48 hours using sinusoidal interpolation.
    Standard method: h(t) = (h1+h2)/2 + (h1-h2)/2 * cos(π*(t-t1)/(t2-t1))
    """
    if not events:
        return _cosine_fallback(now, profile)

    # Sort by datetime
    evs = sorted(events, key=lambda e: e["datetime"])

    points = []
    for step in range(0, 48 * 60 + 1, 30):
        dt = now + timedelta(minutes=step)

        # Find surrounding turning points
        before = [e for e in evs if e["datetime"] <= dt]
        after  = [e for e in evs if e["datetime"] >  dt]

        if not before or not after:
            # Outside the known prediction window — use nearest value
            nearest = before[-1] if before else after[0]
            points.append({"datetime": dt, "type": None, "height_m": nearest["height_m"]})
            continue

        e1 = before[-1]
        e2 = after[0]

        t1 = e1["datetime"].timestamp()
        t2 = e2["datetime"].timestamp()
        tc = dt.timestamp()
        h1 = e1["height_m"]
        h2 = e2["height_m"]

        if t2 == t1:
            height = h1
        else:
            frac   = (tc - t1) / (t2 - t1)
            height = round((h1 + h2) / 2.0 + (h1 - h2) / 2.0 * math.cos(math.pi * frac), 2)

        points.append({"datetime": dt, "type": None, "height_m": height})

    # Label turning points in the interpolated series.
    # Use same proportional threshold as _cosine_fallback() — port amplitude-aware.
    _amp     = (profile or {}).get("tidal_amp_m", 0.9)
    _min_gap = max(_amp * 0.04, 0.004)
    for i in range(1, len(points) - 1):
        ph = points[i - 1]["height_m"]
        ch = points[i]["height_m"]
        nh = points[i + 1]["height_m"]
        if ch >= ph and ch >= nh and (ch - ph + ch - nh) > _min_gap:
            points[i]["type"] = "HW"
        elif ch <= ph and ch <= nh and (ph - ch + nh - ch) > _min_gap:
            points[i]["type"] = "LW"

    return points


def _parse_bom_xml(xml_text: str) -> list:
    """
    Parse BOM tidal XML feed.
    Expected format (IDO71xxx stations):
        <observations>
          <station ...>
            <period type="Tide" time-local="..." time-utc="...">
              <element type="tide_hgt">1.23</element>
              <element type="tide_type">HW</element>
            </period>
          </station>
        </observations>
    Returns list of {"datetime": dt_utc, "type": "HW"|"LW", "height_m": float}.
    """
    import xml.etree.ElementTree as ET

    root   = ET.fromstring(xml_text)
    events = []

    for period in root.iter("period"):
        if period.get("type") != "Tide":
            continue
        time_utc = period.get("time-utc")
        if not time_utc:
            continue
        try:
            dt = datetime.fromisoformat(time_utc.replace("Z", "+00:00"))
        except ValueError:
            continue

        height_m  = None
        tide_type = None
        for el in period.findall("element"):
            t = el.get("type", "")
            if t == "tide_hgt":
                try:
                    height_m = float(el.text)
                except (TypeError, ValueError):
                    pass
            elif t == "tide_type":
                tide_type = (el.text or "").strip().upper()  # "HW" or "LW"

        if height_m is not None and tide_type in ("HW", "LW"):
            events.append({
                "datetime": dt,
                "type":     tide_type,
                "height_m": round(height_m, 2),
            })

    return sorted(events, key=lambda e: e["datetime"])


def _fetch_live(profile: dict, now: datetime) -> list:
    """
    Fetch BOM XML, parse it, and return interpolated 30-min series.
    Raises on any error — caller catches and falls back to cosine model.
    """
    import urllib.request

    url = profile["bom_tide_url"]
    log.info("Fetching BOM tidal data: %s", url)

    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (compatible; ProjectHorizon/1.0)",
        "Referer":    "http://www.bom.gov.au/oceanography/tides/",
        "Accept":     "application/xml, text/xml, */*",
    })
    with urllib.request.urlopen(req, timeout=10) as resp:
        xml_text = resp.read().decode("utf-8", errors="replace")

    turning_points = _parse_bom_xml(xml_text)
    if not turning_points:
        raise ValueError("BOM XML parsed OK but contained no tide turning points")

    log.info("BOM tides: %d turning points for station %s",
             len(turning_points), profile.get("bom_station_id"))
    return _interpolate_from_turning_points(turning_points, now, profile)


def fetch_bom_tides(profile: dict, now: datetime = None) -> dict:
    """
    Public API — returns a dict with:
        {
          "series":          [...],   # 30-min points: {datetime, type, height_m}
          "source":          "bom" | "cosine",
          "station_id":      str | None,
          "current_height_m": float,
          "state":           "Rising"|"Falling"|"Slack",
          "next_event_type": "HW"|"LW",
          "next_event_time": datetime,
          "next_event_height_m": float,
        }
    Falls back to cosine model if BOM URL is None or fetch fails.
    Results are cached per station ID for CACHE_TTL_SECS.
    """
    if now is None:
        now = datetime.now(tz=timezone.utc).replace(microsecond=0)

    station_id = profile.get("bom_station_id")
    bom_url    = profile.get("bom_tide_url")

    # ── No BOM station configured — port-specific cosine fallback ────────────
    if not bom_url or not station_id:
        log.debug("No BOM station configured — using cosine fallback")
        return _build_result(_cosine_fallback(now, profile), "cosine", None, now)

    # ── Check cache ───────────────────────────────────────────────────────────
    with _cache_lock:
        cached = _cache.get(station_id)
        if cached and (time.monotonic() - cached["fetched_at"]) < CACHE_TTL_SECS:
            log.debug("BOM cache hit for %s", station_id)
            series = cached["series"]
            return _build_result(series, "bom", station_id, now)

        # Failure cooldown — avoid hammering a dead endpoint every request
        failed_at = _fail_cache.get(station_id)
        if failed_at and (time.monotonic() - failed_at) < FAIL_TTL_SECS:
            log.debug("BOM failure cooldown active for %s — skipping fetch", station_id)
            return _build_result(_cosine_fallback(now, profile), "cosine", station_id, now)

    # ── Live fetch ────────────────────────────────────────────────────────────
    try:
        series = _fetch_live(profile, now)
        with _cache_lock:
            _cache[station_id] = {"series": series, "fetched_at": time.monotonic()}
            _fail_cache.pop(station_id, None)   # clear any prior failure
        return _build_result(series, "bom", station_id, now)
    except Exception as exc:
        log.error("BOM fetch failed for %s — falling back to cosine model: %s", station_id, exc)
        with _cache_lock:
            _fail_cache[station_id] = time.monotonic()
        return _build_result(_cosine_fallback(now, profile), "cosine", station_id, now)


def _build_result(series: list, source: str, station_id, now: datetime) -> dict:
    """Derive current height, state, and next event from a tide series."""
    if not series:
        return {
            "series": [], "source": source, "station_id": station_id,
            "current_height_m": 2.1, "state": "Unknown",
            "next_event_type": None, "next_event_time": None, "next_event_height_m": None,
        }

    # Current height = closest point to now
    closest = min(series, key=lambda p: abs((p["datetime"] - now).total_seconds()))
    current_h = closest["height_m"]

    # Derive state from slope
    idx = series.index(closest)
    if idx > 0 and idx < len(series) - 1:
        diff = series[idx + 1]["height_m"] - series[idx - 1]["height_m"]
        if abs(diff) < 0.08:
            state = "Slack"
        elif diff > 0:
            state = "Rising"
        else:
            state = "Falling"
    else:
        state = "Unknown"

    # Next turning point after now
    future_events = [p for p in series if p.get("type") in ("HW", "LW") and p["datetime"] > now]
    if future_events:
        nxt = future_events[0]
        next_type   = nxt["type"]
        next_time   = nxt["datetime"]
        next_height = nxt["height_m"]
    else:
        next_type = next_time = next_height = None

    return {
        "series":               series,
        "source":               source,
        "station_id":           station_id,
        "current_height_m":     round(current_h, 2),
        "state":                state,
        "next_event_type":      next_type,
        "next_event_time":      next_time,
        "next_event_height_m":  next_height,
    }


def predict_height_at(series: list, dt: datetime, profile: dict) -> float:
    """
    Predict tide height at an arbitrary future datetime using the cached series.
    If dt is beyond the series window, falls back to the cosine model.
    """
    if not series:
        # Empty series — fall back to port-specific cosine model
        pts = _cosine_fallback(dt, profile)
        closest = min(pts, key=lambda p: abs((p["datetime"] - dt).total_seconds()))
        return closest["height_m"]

    future = [p for p in series if p["datetime"] >= dt]
    past   = [p for p in series if p["datetime"] <= dt]

    if not past or not future:
        # Out of range — port-specific cosine fallback using profile tidal params
        pts = _cosine_fallback(dt, profile)
        closest = min(pts, key=lambda p: abs((p["datetime"] - dt).total_seconds()))
        return closest["height_m"]

    p1 = past[-1]
    p2 = future[0]
    t1 = p1["datetime"].timestamp()
    t2 = p2["datetime"].timestamp()
    tc = dt.timestamp()
    if t2 == t1:
        return p1["height_m"]
    frac = (tc - t1) / (t2 - t1)
    return round(p1["height_m"] + frac * (p2["height_m"] - p1["height_m"]), 2)
