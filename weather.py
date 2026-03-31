"""
Project Horizon — Live Weather Module
Fetches real weather from Open-Meteo (free, no key, cloud-friendly, per-coordinate).
Falls back to a deterministic port-specific simulation on any fetch failure.

Cache: 30 minutes in memory per port.
"""

import hashlib
import json
import logging
import math
import threading
import time
import urllib.request
from datetime import datetime, timezone

log = logging.getLogger("horizon.weather")

_cache: dict = {}       # port_id -> {"data": dict, "fetched_at": float}
_cache_lock = threading.Lock()
CACHE_TTL_SECS = 1800   # 30 minutes

_COMPASS = ["N","NNE","NE","ENE","E","ESE","SE","SSE","S","SSW","SW","WSW","W","WNW","NW","NNW"]
_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; ProjectHorizon/1.0; +https://projecthorizon.app)"}


def _bft(kts: float) -> int:
    if kts < 4:  return 1
    if kts < 7:  return 2
    if kts < 11: return 3
    if kts < 17: return 4
    if kts < 22: return 5
    return 6


def _conditions(wind_kts: float, swell_m: float,
                precip_mm: float = 0.0, weather_code: int = 0) -> str:
    """
    Operational conditions rating.
    WMO weather_code >= 51 = precipitation (rain/drizzle/showers/thunderstorm).
    WMO weather_code >= 95 = thunderstorm.  precip_mm >= 5 = heavy rain.
    Any significant precipitation prevents 'Excellent'; heavy precip → 'Poor'.
    """
    heavy_precip = precip_mm >= 5.0 or weather_code >= 95   # heavy rain / TS
    any_precip   = precip_mm >= 0.5 or (51 <= weather_code <= 99)

    if heavy_precip:
        return "Poor"
    if wind_kts < 10 and swell_m < 1.0 and not any_precip:
        return "Excellent"
    if wind_kts < 16 and swell_m < 1.5:
        return "Good"
    if wind_kts < 22 and swell_m < 2.0:
        return "Moderate"
    return "Poor"


def _sim_weather(profile: dict, now: datetime) -> dict:
    """
    Deterministic port-specific weather simulation.
    Uses port lat/lon + date in seed so Brisbane and Melbourne diverge.
    """
    lat  = profile.get("lat", -27.38)
    lon  = profile.get("lon", 153.17)
    seed = f"weather-{lat:.2f}-{lon:.2f}-{now.strftime('%Y%m%d')}-{now.hour // 3}"
    h    = hashlib.md5(seed.encode()).hexdigest()

    wind_kts  = 6   + int(h[0:2],  16) % 18        # 6–24 kts
    wind_deg  =       int(h[2:6],  16) % 360
    swell_m   = round(0.4 + (int(h[6:8],   16) % 18) / 10.0, 1)   # 0.4–2.2 m
    swell_per = 6   + int(h[8:10], 16) % 9         # 6–14 s
    vis_nm    = 5   + int(h[10:12],16) % 12        # 5–16 nm
    pressure  = 1007 + int(h[12:14],16) % 18       # 1007–1025 hPa

    wind_lbl  = _COMPASS[round(wind_deg  / 22.5) % 16]
    swell_lbl = _COMPASS[(round(wind_deg / 22.5) + 2) % 16]

    return {
        "wind_speed_kts":        wind_kts,
        "wind_direction_deg":    wind_deg,
        "wind_direction_label":  wind_lbl,
        "wind_beaufort":         _bft(wind_kts),
        "swell_height_m":        swell_m,
        "swell_period_s":        swell_per,
        "swell_direction_label": swell_lbl,
        "visibility_nm":         vis_nm,
        "pressure_hpa":          pressure,
        "conditions":            _conditions(wind_kts, swell_m),
        "source":                "simulation",
    }


def fetch_weather(profile: dict, now: datetime = None, cache_only: bool = False) -> dict:
    """
    Public API — returns weather dict with all fields plus 'source' key.

    Attempts Open-Meteo live fetch (weather + marine APIs).
    Caches result 30 min per port.  Falls back to port-specific simulation.

    Keys returned:
        wind_speed_kts, wind_direction_deg, wind_direction_label,
        wind_beaufort, swell_height_m, swell_period_s, swell_direction_label,
        visibility_nm, pressure_hpa, conditions, source
    """
    if now is None:
        now = datetime.now(tz=timezone.utc)

    port_id = profile.get("id", profile.get("short_name", "UNKNOWN"))
    lat     = profile.get("lat")
    lon     = profile.get("lon")

    # ── Cache check ────────────────────────────────────────────────────────────
    with _cache_lock:
        cached = _cache.get(port_id)
        if cached and (time.time() - cached["fetched_at"]) < CACHE_TTL_SECS:
            return cached["data"]

    if not lat or not lon:
        return _sim_weather(profile, now)

    # ── Cache-only mode: return sim immediately rather than blocking ────────────
    if cache_only:
        log.debug("Weather cache miss for %s (cache_only) — sim fallback", port_id)
        return _sim_weather(profile, now)

    # ── Live fetch ─────────────────────────────────────────────────────────────
    try:
        # 1. Atmospheric conditions
        wx_url = (
            f"https://api.open-meteo.com/v1/forecast"
            f"?latitude={lat}&longitude={lon}"
            f"&current=wind_speed_10m,wind_direction_10m,surface_pressure,visibility"
            f",precipitation,weather_code"
            f"&wind_speed_unit=kn&timezone=UTC"
        )
        req = urllib.request.Request(wx_url, headers=_HEADERS)
        with urllib.request.urlopen(req, timeout=8) as r:
            wx = json.loads(r.read())["current"]

        wind_kts     = round(float(wx.get("wind_speed_10m",    10)))
        wind_deg     = int(  float(wx.get("wind_direction_10m", 0)))
        pressure     = round(float(wx.get("surface_pressure",  1013)))
        vis_raw      = float(wx.get("visibility", 27780))          # metres
        vis_nm       = round(vis_raw / 1852.0, 1)
        precip_mm    = round(float(wx.get("precipitation",     0.0)), 1)
        weather_code = int(  float(wx.get("weather_code",      0)))

        # 2. Marine / swell (separate API — best-effort)
        swell_m, swell_per, swell_dir = 0.5, 8, wind_deg
        try:
            marine_url = (
                f"https://marine-api.open-meteo.com/v1/marine"
                f"?latitude={lat}&longitude={lon}"
                f"&current=wave_height,wave_period,wave_direction&timezone=UTC"
            )
            req2 = urllib.request.Request(marine_url, headers=_HEADERS)
            with urllib.request.urlopen(req2, timeout=6) as r2:
                marine  = json.loads(r2.read())["current"]
                swell_m   = round(float(marine.get("wave_height",    0.5)), 1)
                swell_per = int(  float(marine.get("wave_period",    8)))
                swell_dir = int(  float(marine.get("wave_direction", wind_deg)))
        except Exception as me:
            log.debug("Marine API failed for %s (non-fatal): %s", port_id, me)

        wind_lbl  = _COMPASS[round(wind_deg  / 22.5) % 16]
        swell_lbl = _COMPASS[round(swell_dir / 22.5) % 16]

        result = {
            "wind_speed_kts":        wind_kts,
            "wind_direction_deg":    wind_deg,
            "wind_direction_label":  wind_lbl,
            "wind_beaufort":         _bft(wind_kts),
            "swell_height_m":        swell_m,
            "swell_period_s":        swell_per,
            "swell_direction_label": swell_lbl,
            "visibility_nm":         vis_nm,
            "pressure_hpa":          pressure,
            "conditions":            _conditions(wind_kts, swell_m, precip_mm, weather_code),
            "precipitation_mm":      precip_mm,
            "weather_code":          weather_code,
            "source":                "live",
        }

        with _cache_lock:
            _cache[port_id] = {"data": result, "fetched_at": time.time()}

        log.info("Live weather for %s: %d kts %s, swell %.1fm, precip %.1fmm, WMO %d → %s",
                 port_id, wind_kts, wind_lbl, swell_m, precip_mm, weather_code, result["conditions"])
        return result

    except Exception as e:
        log.warning("Weather fetch failed for %s — using simulation: %s", port_id, e)
        sim = _sim_weather(profile, now)
        with _cache_lock:
            _cache[port_id] = {"data": sim, "fetched_at": time.time()}
        return sim
