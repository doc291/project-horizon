#!/usr/bin/env python3
"""
Project Horizon — Beta 9
Port profile system: multi-port support with live BOM tidal data
and Ports Victoria vessel scraper.  Active port set via HORIZON_PORT env var.

Usage (local):
    python3 server.py
    HORIZON_PORT=MELBOURNE python3 server.py

Then open http://localhost:8000 in your browser.
"""

import json
import uuid
import random
import hashlib
import hmac
import secrets
import math
import os
import threading
import time
import logging
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs

# ── Port profile system ───────────────────────────────────────────────────────
from port_profiles import PORT_PROFILES, get_profile, list_profiles
from bom_tides import fetch_bom_tides, predict_height_at
from vessel_scraper import fetch_vessel_movements
from weather import fetch_weather
import mst_scraper

_ACTIVE_PORT_ID  = os.environ.get("HORIZON_PORT", "BRISBANE").upper()
_PORT_PROFILE    = get_profile(_ACTIVE_PORT_ID)
_profile_lock    = threading.Lock()

log = logging.getLogger("horizon")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [horizon] %(levelname)s %(message)s",
                    datefmt="%Y-%m-%dT%H:%M:%SZ")

PORT = int(os.environ.get("PORT", 8000))
INDEX_HTML    = Path(__file__).parent / "index.html"
LOGO_FILE        = Path(__file__).parent / "logo.svg"
MOBILE_ICON_FILE = Path(__file__).parent / "mobile-icon.png"
AMSG_LOGO_FILE   = Path(__file__).parent / "amsg-logo.png"
QSHIPS_FILE   = Path(__file__).parent / "qships_data.json"

# ── Auth ──────────────────────────────────────────────────────────────────────
_AUTH_USER   = os.environ.get("HORIZON_USER", "horizon")
_AUTH_PASS   = os.environ.get("HORIZON_PASS", "ams2026")
_SESSION_KEY = secrets.token_hex(32)          # regenerated each server restart
_COOKIE_NAME = "hz_sess"
_COOKIE_TTL  = 60 * 60 * 12                   # 12 hours

# Paths that bypass auth entirely (assets needed by the login page itself)
_PUBLIC_PATHS = {"/login", "/logo", "/amsg-logo", "/health"}

# ── Port Brief email config ───────────────────────────────────────────────────
_SMTP_HOST       = os.environ.get("SMTP_HOST", "")
_SMTP_PORT       = int(os.environ.get("SMTP_PORT", "587"))
_SMTP_USER       = os.environ.get("SMTP_USER", "")
_SMTP_PASS       = os.environ.get("SMTP_PASS", "")
_SMTP_FROM       = os.environ.get("SMTP_FROM", "") or _SMTP_USER
_BRIEF_RECIPIENTS = [r.strip() for r in os.environ.get("BRIEF_RECIPIENTS", "").split(",") if r.strip()]

# ── MyShipTracking AIS connector ──────────────────────────────────────────────
_MST_API_KEY = os.environ.get("MST_API_KEY", "")
if _MST_API_KEY:
    mst_scraper.configure(_MST_API_KEY)
    log.info("MyShipTracking AIS connector enabled")
else:
    log.info("MST_API_KEY not set — using simulation for vessel data")

# ── What If overlay state ─────────────────────────────────────────────────────
_WHATIF_OVERLAY  = {}   # {"active": bool, "adjustments": [...], "label": str}
_whatif_lock     = threading.Lock()

def _make_token() -> str:
    """Return an HMAC-signed session token."""
    payload = f"{_AUTH_USER}:{int(time.time())}"
    sig = hmac.new(_SESSION_KEY.encode(), payload.encode(), hashlib.sha256).hexdigest()
    return f"{payload}:{sig}"

def _verify_token(token: str) -> bool:
    """Return True if token is well-formed, unmodified, and not expired."""
    try:
        parts = token.split(":")
        if len(parts) != 3:
            return False
        user, ts, sig = parts
        expected = hmac.new(_SESSION_KEY.encode(), f"{user}:{ts}".encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(sig, expected):
            return False
        if time.time() - int(ts) > _COOKIE_TTL:
            return False
        return True
    except Exception:
        return False

def _get_cookie(handler, name: str) -> str | None:
    """Extract a named cookie value from the request headers."""
    raw = handler.headers.get("Cookie", "")
    for part in raw.split(";"):
        k, _, v = part.strip().partition("=")
        if k.strip() == name:
            return v.strip()
    return None

# ── QShips live data state ────────────────────────────────────────────────────

_qships_data  = None       # Loaded qships_data.json content (dict or None)
_scrape_lock  = threading.Lock()
_scraping     = False      # Flag: scrape in progress

MAX_LIVE_VESSELS = 80   # Safety cap — conflict engine is O(n²); reject bad scrapes

def load_qships_data():
    """Load qships_data.json from disk into _qships_data. Thread-safe."""
    global _qships_data
    if not QSHIPS_FILE.exists():
        log.info("qships_data.json not found — using simulation data")
        _qships_data = None
        return
    try:
        data = json.loads(QSHIPS_FILE.read_text(encoding="utf-8"))
        vessels = data.get("vessels") or []
        if not vessels:
            log.warning("qships_data.json has no vessels — using simulation data")
            _qships_data = None
            return
        if len(vessels) > MAX_LIVE_VESSELS:
            log.warning(
                "qships_data.json has %d vessels (cap=%d) — "
                "scrape filter likely failed; using simulation data",
                len(vessels), MAX_LIVE_VESSELS,
            )
            _qships_data = None
            return
        _qships_data = data
        log.info("Loaded qships_data.json: %d vessels, scraped at %s",
                 len(vessels), data.get("scraped_at", "?"))
    except Exception as e:
        log.error("Failed to load qships_data.json: %s", e)
        _qships_data = None


def get_data_source() -> dict:
    """Return current data source descriptor."""
    if _qships_data:
        return {
            "source":     "qships",
            "label":      f"QShips Live — {_PORT_PROFILE.get('short_name', 'Brisbane')}",
            "scraped_at": _qships_data.get("scraped_at"),
        }
    return {"source": "mock", "label": "Simulation Data", "scraped_at": None}


def _run_scrape_background():
    """Run scraper in background thread. Uses lock to prevent concurrent runs."""
    global _scraping
    with _scrape_lock:
        if _scraping:
            log.info("Scrape already in progress — skipping")
            return
        _scraping = True
    try:
        import qships_scraper
        success = qships_scraper.run_scrape()
        if success:
            load_qships_data()
    except ImportError:
        log.warning("qships_scraper not available — skipping scrape")
    except Exception as e:
        log.error("Background scrape error: %s", e)
    finally:
        _scraping = False


def _schedule_scrapes():
    """
    Schedule four scrapes per day at 06:00, 12:00, 18:00, 00:00 AEST (UTC+10).
    AEST times → UTC: 20:00, 02:00, 08:00, 14:00 UTC.
    Runs in a background daemon thread.
    """
    SCRAPE_UTC_HOURS = {20, 2, 8, 14}

    def _loop():
        last_hour_fired = -1
        while True:
            now_utc = datetime.now(tz=timezone.utc)
            h = now_utc.hour
            if h in SCRAPE_UTC_HOURS and h != last_hour_fired and now_utc.minute < 5:
                last_hour_fired = h
                log.info("Scheduled scrape triggered at %02d:%02d UTC", h, now_utc.minute)
                t = threading.Thread(target=_run_scrape_background, daemon=True)
                t.start()
            elif h not in SCRAPE_UTC_HOURS:
                last_hour_fired = -1   # reset so next window fires
            time.sleep(60)

    t = threading.Thread(target=_loop, daemon=True, name="scrape-scheduler")
    t.start()


def build_vessels_from_qships(data: dict) -> list:
    """Convert qships_data.json vessels to server.py vessel dicts.

    Patches every field that make_pilotage / make_towage / detect_conflicts
    expect but that the QShips scraper does not populate.
    """
    now = utcnow()
    vessels = []
    for v in data.get("vessels", []):
        if v.get("status") == "departed":
            continue
        v_out = dict(v)

        # ── loa: ensure numeric (None breaks "loa > 200" in make_towage) ───────
        v_out["loa"] = float(v_out["loa"]) if v_out.get("loa") else 0.0

        # ── berth_id: simulated berths use B01-B06 keys; live vessels have a
        #    berth text name instead.  Set None so vessel_position falls through
        #    gracefully to the ETA-based position path.
        v_out.setdefault("berth_id", None)

        # ── ETD: estimate if not supplied ────────────────────────────────────
        if not v_out.get("etd"):
            try:
                eta_dt = isoparse(v_out["eta"])
                hrs = 2 if v_out.get("status") == "berthed" else 12
                v_out["etd"] = fmt(eta_dt + timedelta(hours=hrs))
            except Exception:
                v_out["etd"] = v_out.get("eta")

        # ── ATA / ATD defaults ───────────────────────────────────────────────
        if not v_out.get("ata"):
            v_out["ata"] = v_out["eta"] if v_out.get("status") == "berthed" else None
        v_out.setdefault("atd", None)

        # ── towage_required: derive from LOA + vessel type ───────────────────
        if "towage_required" not in v_out:
            loa_val = v_out["loa"]  # already numeric
            vtype   = (v_out.get("type") or "").lower()
            v_out["towage_required"] = bool(
                loa_val > 100
                or any(t in vtype for t in ("tanker", "bulk", "container", "ro-ro"))
            )

        # ── pilotage_required: all large ships need a pilot ──────────────────
        v_out.setdefault("pilotage_required", True)

        # ── Optional text fields ─────────────────────────────────────────────
        v_out.setdefault("notes",     "")
        v_out.setdefault("cargo",     "")
        v_out.setdefault("agent",     "")
        v_out.setdefault("flag",      "")
        v_out.setdefault("call_sign", "")

        # ── Geo position ─────────────────────────────────────────────────────
        try:
            pos = vessel_position(v_out, now)
            v_out["lat"] = pos["lat"]
            v_out["lon"] = pos["lon"]
        except Exception:
            v_out["lat"] = _PORT_PROFILE.get("lat", PORT_GEO["center"]["lat"])
            v_out["lon"] = _PORT_PROFILE.get("lon", PORT_GEO["center"]["lon"])

        vessels.append(v_out)
    return vessels


def build_berths_from_qships(data: dict) -> list:
    """Convert qships_data.json berths to server.py berth dicts."""
    berths = []
    now = utcnow()
    for b in data.get("berths", []):
        b_out = dict(b)
        # Add geo defaults (QShips berths won't have lat/lon)
        b_out.setdefault("lat", None)
        b_out.setdefault("lon", None)
        b_out.setdefault("crane_count", 0)
        b_out.setdefault("lat_depth_m", 13.0)
        berths.append(b_out)
    return berths

# ── Helpers ──────────────────────────────────────────────────────────────────

def utcnow() -> datetime:
    return datetime.now(tz=timezone.utc).replace(microsecond=0)

def fmt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

def isoparse(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))

def stable_jitter(seed_str: str, scale: float = 0.004):
    """Deterministic lat/lon jitter from a string seed (vessel ID)."""
    h = int(hashlib.md5(seed_str.encode()).hexdigest(), 16)
    lat_j = ((h & 0xFFFF) / 0xFFFF - 0.5) * scale
    lon_j = ((h >> 16 & 0xFFFF) / 0xFFFF - 0.5) * scale
    return lat_j, lon_j

# ── Static reference data ─────────────────────────────────────────────────────

AGENTS   = ["Wilhelmsen Ships Service", "Inchcape Shipping", "GAC", "Norton Lilly"]
PILOTS   = ["Capt. Andersen", "Capt. Müller", "Capt. Johansson", "Capt. O'Brien"]
TUGS     = ["TUG Stallion", "TUG Hercules", "TUG Neptune", "TUG Samson", "TUG Trident"]
STATIONS = ["Outer Pilot Station", "North Channel Anchorage"]

# ── Port geography ─────────────────────────────────────────────────────────────
# Fallback geo used only if port profile is missing port_geo (should not occur
# in production — all active profiles define port_geo in port_profiles.py).

PORT_GEO = {
    "center":  {"lat": -27.383, "lon": 153.173},
    "zoom":    13,
    "berths": {
        "B01": {"lat": -27.368, "lon": 153.150, "terminal": "Berth 1", "heading": 350},
        "B02": {"lat": -27.369, "lon": 153.161, "terminal": "Berth 2", "heading": 350},
        "B03": {"lat": -27.370, "lon": 153.172, "terminal": "Berth 3", "heading": 350},
        "B04": {"lat": -27.397, "lon": 153.157, "terminal": "Berth 4", "heading": 170},
        "B05": {"lat": -27.398, "lon": 153.167, "terminal": "Berth 5", "heading": 170},
        "B06": {"lat": -27.399, "lon": 153.177, "terminal": "Berth 6", "heading": 170},
    },
    "anchorage": {
        "lat": -27.352, "lon": 153.253,
        "radius_km": 2.5,
        "label": "Outer Anchorage",
    },
    "pilot_boarding_ground": {"lat": -27.360, "lon": 153.218, "label": "Pilot Boarding Ground"},
    "channel_waypoints": [
        {"lat": -27.352, "lon": 153.246},
        {"lat": -27.357, "lon": 153.228},
        {"lat": -27.362, "lon": 153.212},
        {"lat": -27.367, "lon": 153.198},
        {"lat": -27.372, "lon": 153.186},
        {"lat": -27.374, "lon": 153.175},
        {"lat": -27.370, "lon": 153.161},
    ],
}

def vessel_position(v: dict, now: datetime) -> dict:
    """
    Compute approximate vessel lat/lon from status and ETA.
    Uses the active port profile's geo so vessels appear in the right port.
    """
    geo    = _PORT_PROFILE.get("port_geo", PORT_GEO)
    coords = geo.get("berths", PORT_GEO["berths"])
    center = geo.get("center", PORT_GEO["center"])
    anc    = geo.get("anchorage", PORT_GEO["anchorage"])
    pbg    = geo.get("pilot_boarding_ground", PORT_GEO["pilot_boarding_ground"])
    jlat, jlon = stable_jitter(v["id"])

    if v["status"] in ("berthed", "arrived"):
        c = coords.get(v.get("berth_id"))
        if c:
            return {"lat": c["lat"] + jlat * 0.3, "lon": c["lon"] + jlon * 0.3}

    eta  = isoparse(v["eta"])
    hrs  = (eta - now).total_seconds() / 3600

    if hrs < 0:
        # Should be berthed — fallback to port center
        c = coords.get(v.get("berth_id"))
        if c:
            return {"lat": c["lat"] + jlat * 0.3, "lon": c["lon"] + jlon * 0.3}
        return {"lat": center["lat"] + jlat, "lon": center["lon"] + jlon}
    elif hrs < 2.5:
        # Near pilot boarding ground
        return {"lat": pbg["lat"] + jlat, "lon": pbg["lon"] + jlon}
    elif hrs < 8:
        # Mid-channel — interpolate between pilot ground and anchorage
        mid_lat = (pbg["lat"] + anc["lat"]) / 2
        mid_lon = (pbg["lon"] + anc["lon"]) / 2
        return {"lat": mid_lat + jlat, "lon": mid_lon + jlon * 2}
    elif hrs < 24:
        # Anchorage
        return {"lat": anc["lat"] + jlat * 2, "lon": anc["lon"] + jlon * 2}
    else:
        # Offshore — extrapolate beyond anchorage on the approach bearing
        bear_lat = anc["lat"] - center["lat"]
        bear_lon = anc["lon"] - center["lon"]
        return {"lat": anc["lat"] + bear_lat * 0.5 + jlat * 3,
                "lon": anc["lon"] + bear_lon * 0.5 + jlon * 3}

def _predict_tide_height(dt: datetime) -> float:
    """
    Predict tide height at any future (or past) datetime using the same
    deterministic cosine model as make_tides().  Safe to call for ETA lookahead.
    Uses the active port profile's tidal_mean_m and tidal_amp_m.
    """
    PERIOD  = 12.42
    MEAN    = _PORT_PROFILE.get("tidal_mean_m", 2.1)
    AMP     = _PORT_PROFILE.get("tidal_amp_m",  1.65)
    day_h   = hashlib.md5(f"tide-{dt.strftime('%Y%m%d')}".encode()).hexdigest()
    phase_h = (int(day_h[0:4], 16) % int(PERIOD * 100)) / 100.0
    t       = (dt.hour + dt.minute / 60.0 + phase_h) % PERIOD
    return round(MEAN + AMP * math.cos(2 * math.pi * t / PERIOD), 2)


CHANNEL_DEPTH_M = 12.5   # Generic fallback — always overridden by port profile channel_depth_m

# ── Mock data generation ──────────────────────────────────────────────────────

# Chart datum depths (LAT) per berth in metres
BERTH_LAT_DEPTHS = {
    "B01": 13.5,   # Deep container berth — North Terminal
    "B02": 12.0,   # Container/general — North Terminal
    "B03": 10.2,   # Shallow — tide-restricted — North Terminal
    "B04": 13.0,   # Deep bulk — South Terminal
    "B05": 11.5,   # Bulk/general — South Terminal
    "B06":  8.8,   # Shallow — tide-restricted — South Terminal
}

def make_berths(now: datetime) -> list:
    # Simulation slots: (id, max_loa, max_draught, status, cranes, ready_offset_h)
    # Use port-profile-specific slots if defined, otherwise fall back to Brisbane defaults
    _slot_defs = _PORT_PROFILE.get("sim_berth_slots") or [
        ("B01", 350, 14.5, "occupied",     4,  4),
        ("B02", 300, 13.0, "occupied",     4,  8),
        ("B03", 250, 11.5, "reserved",     2,  2),
        ("B04", 320, 14.0, "available",    3,  None),
        ("B05", 280, 12.5, "maintenance",  0,  20),
        ("B06", 220, 10.0, "occupied",     0,  6),
    ]
    raw = [
        (bid, loa, dr, status, cranes,
         now + timedelta(hours=rh) if rh is not None else None)
        for bid, loa, dr, status, cranes, rh in _slot_defs
    ]
    # Pull berth names and coordinates from the active port profile
    port_geo   = _PORT_PROFILE.get("port_geo", PORT_GEO)
    geo_berths = port_geo.get("berths", PORT_GEO["berths"])
    ch_depth   = _PORT_PROFILE.get("channel_depth_m", 12.5)

    result = []
    for bid, loa, draught, status, cranes, ready in raw:
        geo      = geo_berths.get(bid, {})
        terminal = geo.get("terminal", bid)   # use port profile terminal name
        result.append({
            "id": bid, "name": terminal, "terminal": terminal,
            "max_loa": loa, "max_draught": draught,
            "lat_depth_m": geo.get("depth_m", BERTH_LAT_DEPTHS.get(bid, ch_depth)),
            "status": status, "crane_count": cranes,
            "readiness_time": fmt(ready) if ready else None,
            "lat": geo.get("lat"), "lon": geo.get("lon"),
        })
    return result


def compute_ukc(vessels: list, berths: list, tide_height_m: float) -> dict:
    """
    Compute minimum Under Keel Clearance across all currently berthed vessels.
    UKC = (berth LAT depth + current tide height) - vessel draught
    """
    berth_depth = {b["id"]: b["lat_depth_m"] for b in berths}
    entries = []
    for v in vessels:
        if v["status"] != "berthed" or not v.get("berth_id"):
            continue
        lat_d    = berth_depth.get(v["berth_id"], 12.0)
        avail    = lat_d + tide_height_m
        ukc      = round(avail - v["draught"], 2)
        entries.append({
            "vessel_id":        v["id"],
            "vessel_name":      v["name"],
            "berth_id":         v["berth_id"],
            "ukc_m":            ukc,
            "available_depth_m": round(avail, 2),
            "vessel_draught_m": v["draught"],
        })
    if not entries:
        return {"min_ukc_m": None, "critical_vessel": None,
                "critical_berth": None, "status": "no_vessels", "all": []}
    entries.sort(key=lambda r: r["ukc_m"])
    mn = entries[0]
    ukc_min = _PORT_PROFILE.get("ukc_minimum_m", 0.5)
    status = ("critical" if mn["ukc_m"] < ukc_min else
              "warning"  if mn["ukc_m"] < ukc_min * 2 else "good")
    return {
        "min_ukc_m":      mn["ukc_m"],
        "critical_vessel": mn["vessel_name"],
        "critical_berth":  mn["berth_id"],
        "status":          status,
        "all":             entries,
    }


def _load_vessel_roster(port_id: str) -> list:
    """Load port-specific vessel roster JSON. Returns [] if not found."""
    roster_path = Path(__file__).parent / f"{port_id.lower()}_roster.json"
    try:
        with open(roster_path) as f:
            return json.load(f).get("vessels", [])
    except Exception:
        return []

_FLAGS = ["Marshall Islands", "Panama", "Bahamas", "Liberia", "Norway", "Cyprus", "Singapore"]

def make_vessels(now: datetime) -> list:
    roster = _load_vessel_roster(_ACTIVE_PORT_ID)

    # Structural slots: (id, berth, eta_h, etd_h, status, note)
    # Conflicts are baked into timing — V007 vs V005 at B04, V010 vs V004 at B03
    # Port profiles may set sim_vessel_count to trim this list (smaller ports = fewer vessels)
    _all_slots = [
        ("V001","B01",-18, 4,  "berthed",   None),
        ("V002","B02", -6, 8,  "berthed",   None),
        ("V003","B06",-12, 6,  "berthed",   None),
        ("V004","B03",  3, 19, "confirmed", None),
        ("V005","B04",  5, 17, "confirmed", None),
        ("V006","B01",  7, 27, "scheduled", None),
        ("V007","B04",  2, 10, "at_risk",   "ETA variance +2.5h reported by agent"),
        ("V008","B06", 10, 28, "scheduled", None),
        ("V009","B02", 12, 36, "scheduled", None),
        ("V010","B03", 18, 40, "scheduled", None),
        ("V011","B04", 26, 40, "scheduled", None),
        ("V012","B01", 30, 48, "scheduled", None),
        ("V013","B06", 36, 56, "scheduled", None),
    ]
    n_vessels = _PORT_PROFILE.get("sim_vessel_count", len(_all_slots))
    slots = _all_slots[:n_vessels]

    # Deterministic daily shuffle of roster so vessels rotate each day
    day_seed = f"roster-{_ACTIVE_PORT_ID}-{now.strftime('%Y%m%d')}"
    h = hashlib.md5(day_seed.encode()).hexdigest()
    if roster:
        idx = list(range(len(roster)))
        for i in range(len(idx) - 1, 0, -1):
            j = int(h[(i * 2) % 30 : (i * 2) % 30 + 2], 16) % (i + 1)
            idx[i], idx[j] = idx[j], idx[i]
        shuffled = [roster[k] for k in idx]
    else:
        shuffled = []

    vessels = []
    for slot_i, (vid, berth_id, eta_h, etd_h, status, note) in enumerate(slots):
        eta = now + timedelta(hours=eta_h)
        etd = now + timedelta(hours=etd_h)
        ata = eta if status in ("berthed", "arrived") else None

        if shuffled:
            rv = shuffled[slot_i % len(shuffled)]
            name  = rv["name"]
            vtype = rv["vessel_type"]
            loa   = rv["loa"]
            dr    = rv.get("draught", 9.0)
            cargo = rv["cargo_type"]
            agent = rv.get("agent", AGENTS[slot_i % len(AGENTS)])
        else:
            # Fallback hardcoded vessel if no roster
            _fb = [
                ("Nordic Star","Container",240,12.0,"Containers"),
                ("Atlantic Pioneer","Bulk Carrier",190,10.5,"Grain"),
                ("Baltic Carrier","Tanker",180,9.5,"Crude Oil"),
                ("Oceanic Trader","Container",220,11.5,"Containers"),
                ("Horizon Scout","General Cargo",160,8.5,"Steel Coils"),
                ("Cape Venture","Bulk Carrier",200,11.0,"Coal"),
                ("Northern Light","Car Carrier",185,7.5,"Vehicles"),
                ("Pacific Mariner","Tanker",175,9.0,"Crude Oil"),
                ("Southern Cross","Container",260,13.0,"Containers"),
                ("Eastern Spirit","Bulk Carrier",195,10.5,"Grain"),
                ("Western Passage","General Cargo",145,7.5,"Fertiliser"),
                ("Iron Meridian","Container",230,12.0,"Containers"),
                ("Coral Bay","Bulk Carrier",170,9.0,"Coal"),
            ]
            fn, vtype, loa, dr, cargo = _fb[slot_i]
            name  = f"MV {fn}"
            agent = AGENTS[slot_i % len(AGENTS)]

        flag_idx = int(hashlib.md5(name.encode()).hexdigest(), 16) % len(_FLAGS)
        imo      = str(9000000 + int(hashlib.md5(vid.encode()).hexdigest(), 16) % 999999)

        v = {
            "id": vid, "name": name, "imo": imo,
            "vessel_type": vtype, "flag": _FLAGS[flag_idx],
            "loa": loa, "draught": dr, "cargo_type": cargo,
            "status": status, "berth_id": berth_id,
            "eta": fmt(eta), "etd": fmt(etd),
            "ata": fmt(ata) if ata else None, "atd": None,
            "pilotage_required": True,
            "towage_required": loa > _PORT_PROFILE.get("compulsory_towage_loa_m", 170),
            "agent": agent,
            "notes": note,
        }
        pos = vessel_position(v, now)
        v["lat"] = pos["lat"]
        v["lon"] = pos["lon"]
        vessels.append(v)
    return vessels


def make_pilotage(vessels: list, now: datetime, profile: dict = None) -> list:
    p = profile or _PORT_PROFILE
    pilots   = p.get("pilots",         PILOTS)
    stations = p.get("pilot_stations", STATIONS)
    events = []
    inbound  = [v for v in vessels if v["status"] not in ("berthed", "departed")]
    outbound = [v for v in vessels if v["status"] == "berthed"]
    for v in inbound:
        pilot_idx = int(hashlib.md5(v["id"].encode()).hexdigest(), 16) % len(pilots)
        sched = isoparse(v["eta"]) - timedelta(hours=1, minutes=30)
        events.append({
            "id": f"PIL-{v['id']}-IN",
            "vessel_id": v["id"], "vessel_name": v["name"],
            "pilot_name": pilots[pilot_idx],
            "scheduled_time": fmt(sched),
            "boarding_station": stations[pilot_idx % len(stations)],
            "direction": "inbound",
            "status": "confirmed" if v["status"] == "confirmed" else "scheduled",
        })
    for v in outbound:
        pilot_idx = (int(hashlib.md5(v["id"].encode()).hexdigest(), 16) + 1) % len(pilots)
        sched = isoparse(v["etd"]) - timedelta(hours=1)
        events.append({
            "id": f"PIL-{v['id']}-OUT",
            "vessel_id": v["id"], "vessel_name": v["name"],
            "pilot_name": pilots[pilot_idx],
            "scheduled_time": fmt(sched),
            "boarding_station": stations[pilot_idx % len(stations)],
            "direction": "outbound",
            "status": "scheduled",
        })
    return events


def make_towage(vessels: list, now: datetime, profile: dict = None) -> list:
    p = profile or _PORT_PROFILE
    tug_list = p.get("tugs") or [{"name": t, "bollard_pull_t": 65} for t in TUGS]
    events = []
    eligible = [v for v in vessels if v["towage_required"]]
    for v in eligible:
        n_tugs = 2 if v["loa"] > 200 else 1
        # Deterministic tug assignment from vessel ID hash
        h = int(hashlib.md5(v["id"].encode()).hexdigest(), 16)
        tug_indices = [(h + i) % len(tug_list) for i in range(n_tugs)]
        # Ensure no duplicate indices
        seen_idx = set()
        unique_indices = []
        for idx in tug_indices:
            if idx not in seen_idx:
                seen_idx.add(idx)
                unique_indices.append(idx)
        tugs = [{"tug_id": tug_list[i]["name"].replace(" ", "-").upper(),
                 "tug_name": tug_list[i]["name"]}
                for i in unique_indices]

        if v["status"] == "berthed":
            events.append({
                "id": f"TOW-{v['id']}-DEP",
                "vessel_id": v["id"], "vessel_name": v["name"],
                "tugs": tugs,
                "scheduled_time": fmt(isoparse(v["etd"]) - timedelta(minutes=45)),
                "direction": "departure", "status": "scheduled",
            })
        else:
            events.append({
                "id": f"TOW-{v['id']}-ARR",
                "vessel_id": v["id"], "vessel_name": v["name"],
                "tugs": tugs,
                "scheduled_time": fmt(isoparse(v["eta"]) - timedelta(minutes=30)),
                "direction": "arrival",
                "status": "confirmed" if v["status"] == "confirmed" else "scheduled",
            })
    return events


# ── Sequencing alternatives ────────────────────────────────────────────────────

def _seq_alt(sid, strategy, label, description, vessels, cascade, feasibility,
             saving_h=0, cost_usd=0, cost_label="", delay_mins=0,
             cascade_count=0, risk="medium", recommended=False):
    return {
        "id": sid, "strategy": strategy, "label": label,
        "description": description, "affected_vessels": vessels,
        "cascade_impact": cascade, "feasibility": feasibility,
        "time_saving_hours": saving_h,
        # Decision support impact fields
        "cost_usd": cost_usd,
        "cost_label": cost_label or (f"~${cost_usd:,}" if cost_usd else "Negligible"),
        "delay_mins": delay_mins,
        "cascade_count": cascade_count,
        "risk": risk,
        "recommended": recommended,
    }

def b04_alternatives(a_name, b_name):
    """Alternatives for B04 berth conflict (V007 vs V005)."""
    return [
        _seq_alt("SEQ-B04-1", "delay_arrival",
            f"Hold {a_name} at outer anchorage (+90min)",
            f"Delay {a_name} ETA by 90min. B04 opens after {b_name} departs with full clearance window. Pilot notice is restored.",
            [a_name],
            f"Outbound towage for {a_name} delayed 90min. Pilot rescheduled to +3.5h. Terminal gang start pushed back.",
            "high", 0,
            cost_usd=3800, cost_label="~$3,800 anchorage + delay fees",
            delay_mins=90, cascade_count=1, risk="low", recommended=True),
        _seq_alt("SEQ-B04-2", "advance_departure",
            f"Accelerate {b_name} departure (−3h)",
            f"Advance {b_name} ETD by 3h by accelerating cargo operations. B04 clears before {a_name} arrives.",
            [b_name],
            "Terminal must accelerate crane gang — likely overtime. Shipping line cargo cut-off advanced by 3h.",
            "medium", 3,
            cost_usd=11200, cost_label="~$11,200 overtime + terminal",
            delay_mins=0, cascade_count=2, risk="medium", recommended=False),
        _seq_alt("SEQ-B04-3", "reassign_berth",
            f"Reassign {a_name} to Berth 2",
            f"B02 becomes available +8h. {a_name} (RoRo, LOA 185m) is within B02 dimensional limits (max LOA 300m).",
            [a_name],
            f"Pilot boarding route unchanged. Towage approach changes to North Terminal. Terminal gang reassigned to B02.",
            "high", 0,
            cost_usd=2400, cost_label="~$2,400 repositioning",
            delay_mins=30, cascade_count=0, risk="low", recommended=False),
    ]

def b03_alternatives(a_name, b_name):
    """Alternatives for B03 berth conflict (V004 vs V010)."""
    return [
        _seq_alt("SEQ-B03-1", "delay_arrival",
            f"Delay {b_name} arrival by 2.5h",
            f"Hold {b_name} at anchorage for 2.5h. {a_name} departs +19h, clearance complete +20h. {b_name} ETA becomes +20.5h.",
            [b_name],
            f"Minimal cascade. {b_name} anchorage costs apply. Pilot and tug times adjust accordingly.",
            "high", 0,
            cost_usd=2100, cost_label="~$2,100 anchorage fees",
            delay_mins=150, cascade_count=1, risk="low", recommended=True),
        _seq_alt("SEQ-B03-2", "advance_departure",
            f"Advance {a_name} departure by 2h",
            f"Accelerate cargo operations on {a_name}. {a_name} ETD moves to +17h, giving a 1.5h buffer before {b_name} ETA.",
            [a_name],
            f"Terminal crane gang must accelerate immediately. Shipping line notified of early ETD.",
            "medium", 2,
            cost_usd=7400, cost_label="~$7,400 overtime + ops",
            delay_mins=0, cascade_count=1, risk="medium", recommended=False),
        _seq_alt("SEQ-B03-3", "reassign_berth",
            f"Reassign {b_name} to Berth 4",
            f"B04 opens after V007/V005 window resolves. {b_name} (Bulk, LOA 195m) within B04 limits. Dependent on B04 conflict resolution.",
            [b_name],
            "Requires B04 conflict to be resolved first. Terminal equipment moved to South Terminal.",
            "low", 0,
            cost_usd=3200, cost_label="~$3,200 repositioning",
            delay_mins=60, cascade_count=2, risk="high", recommended=False),
    ]


# ── Conflict detection ────────────────────────────────────────────────────────

CLEARANCE_MINS = 60

def _conflict(cid, ctype, signal_type, severity, vessel_ids, vessel_names,
               berth_id, berth_name, conflict_time, description, resolutions,
               sequencing_alternatives=None, decision_support=None,
               data_source="simulated"):
    return {
        "id": cid,
        "conflict_type": ctype,
        "signal_type": signal_type,        # CONFLICT | WARNING | ADVISORY | WEATHER
        "severity": severity,
        "vessel_ids": vessel_ids,
        "vessel_names": vessel_names,
        "berth_id": berth_id,
        "berth_name": berth_name,
        "conflict_time": conflict_time if isinstance(conflict_time, str) else fmt(conflict_time),
        "description": description,
        "resolution_options": resolutions,
        "sequencing_alternatives": sequencing_alternatives or [],
        "decision_support": decision_support,
        "data_source": data_source,        # "live" | "simulated"
    }


def _build_decision_support(seq_alts, conflict_time_dt, now):
    """Build decision support block from sequencing alternatives."""
    rec   = next((a for a in seq_alts if a.get("recommended")), seq_alts[0] if seq_alts else None)
    # Deadline: 2h before conflict, but at least 20 min from now
    raw_deadline = conflict_time_dt - timedelta(hours=2)
    deadline     = max(raw_deadline, now + timedelta(minutes=20))
    reasoning_map = {
        "delay_arrival":    "Lowest cost option with minimal cascade impact. Anchorage capacity is available and pilot can be rescheduled with adequate notice.",
        "advance_departure":"Restores full clearance window but requires immediate terminal action and incurs overtime.",
        "reassign_berth":   "Eliminates the conflict entirely. Vessel dimensions confirm berth compatibility.",
    }
    return {
        "recommended_option_id":  rec["id"] if rec else None,
        "recommended_reasoning":  reasoning_map.get(rec["strategy"], "Best available option given current port state.") if rec else "",
        "confidence":             "high" if rec and rec.get("risk") == "low" else "medium",
        "decision_deadline":      fmt(deadline),
        "options": seq_alts,
    }


def detect_conflicts(vessels, berths, pilotage, towage, now, is_live=False):
    """
    Detect all operational conflicts.
    When is_live=True (QShips data active), berth/ETA conflicts are tagged data_source="live".
    Pilotage and towage conflicts are always tagged data_source="simulated".
    """
    conflicts = []
    vessel_data_source = "live" if is_live else "simulated"

    # ── 1. Berth overlaps ──────────────────────────────────────────────────────
    by_berth = {}
    for v in vessels:
        if v["status"] != "departed" and v["berth_id"]:
            by_berth.setdefault(v["berth_id"], []).append(v)

    for berth_id, bv in by_berth.items():
        berth_name = next((b["name"] for b in berths if b["id"] == berth_id), berth_id)
        for i in range(len(bv)):
            for j in range(i + 1, len(bv)):
                a, b = bv[i], bv[j]
                a_start = isoparse(a["ata"] or a["eta"])
                a_end   = isoparse(a["atd"] or a["etd"])
                b_start = isoparse(b["ata"] or b["eta"])
                b_end   = isoparse(b["atd"] or b["etd"])
                if a_start > b_start:
                    a, b = b, a
                    a_start, a_end, b_start, b_end = b_start, b_end, a_start, a_end
                a_end_buf = a_end + timedelta(minutes=CLEARANCE_MINS)
                if a_start < b_end and b_start < a_end_buf:
                    gap = int((b_start - a_end).total_seconds() / 60)
                    sev = "critical" if gap < 0 else "high"
                    # Build sequencing alternatives for known conflict pairs
                    seq_alts = []
                    if berth_id == "B04":
                        seq_alts = b04_alternatives(a["name"], b["name"])
                    elif berth_id == "B03":
                        seq_alts = b03_alternatives(a["name"], b["name"])
                    ds = _build_decision_support(seq_alts, b_start, now) if seq_alts else None
                    _cid = hashlib.md5(f"berth_overlap-{berth_id}-{a['id']}-{b['id']}".encode()).hexdigest()[:8]
                    conflicts.append(_conflict(
                        _cid, "berth_overlap", "CONFLICT", sev,
                        [a["id"], b["id"]], [a["name"], b["name"]],
                        berth_id, berth_name, b_start,
                        (f"{b['name']} is scheduled to arrive at {berth_name} "
                         f"only {gap}min after {a['name']} departs. "
                         f"Minimum clearance required: {CLEARANCE_MINS}min."),
                        [f"Delay {b['name']} arrival by {max(CLEARANCE_MINS - gap + 15, 30)}min",
                         f"Bring forward {a['name']} departure",
                         f"Reassign {b['name']} to an alternative berth"],
                        seq_alts, ds,
                        data_source=vessel_data_source,
                    ))

    # ── 2. Berth not ready ────────────────────────────────────────────────────
    berth_map = {b["id"]: b for b in berths}
    for v in vessels:
        if v["status"] in ("scheduled", "confirmed", "at_risk") and v["berth_id"]:
            brt = berth_map.get(v["berth_id"])
            if brt and brt.get("readiness_time"):
                ready = isoparse(brt["readiness_time"])
                eta   = isoparse(v["eta"])
                if ready > eta:
                    gap = int((ready - eta).total_seconds() / 60)
                    sev = "high" if gap > 60 else "medium"
                    _cid = hashlib.md5(f"berth_not_ready-{v['id']}-{brt['id']}".encode()).hexdigest()[:8]
                    conflicts.append(_conflict(
                        _cid, "berth_not_ready", "WARNING", sev,
                        [v["id"]], [v["name"]], brt["id"], brt["name"], v["eta"],
                        (f"{v['name']} ETA is {eta.strftime('%H:%M')} UTC but "
                         f"{brt['name']} will not be ready until "
                         f"{ready.strftime('%H:%M')} UTC (gap: {gap}min)."),
                        [f"Hold {v['name']} at anchorage for {gap}min",
                         "Accelerate departure of current occupant",
                         f"Assign {v['name']} to an alternative berth"],
                        data_source=vessel_data_source,
                    ))

    # ── 3. Short pilotage notice ───────────────────────────────────────────────
    for v in vessels:
        if v["status"] in ("scheduled", "at_risk"):
            pil = next((p for p in pilotage
                        if p["vessel_id"] == v["id"] and p["direction"] == "inbound"), None)
            if pil:
                sched = isoparse(pil["scheduled_time"])
                hrs = (sched - now).total_seconds() / 3600
                if 0 < hrs < 2:
                    _cid = hashlib.md5(f"pilotage_window-{v['id']}-{pil['scheduled_time']}".encode()).hexdigest()[:8]
                    conflicts.append(_conflict(
                        _cid, "pilotage_window", "WARNING", "high",
                        [v["id"]], [v["name"]], None, None, pil["scheduled_time"],
                        (f"Pilotage for {v['name']} is in {hrs:.1f}h — "
                         f"below the 2h minimum notice. Pilot: {pil['pilot_name']}."),
                        [f"Confirm availability with {pil['pilot_name']} immediately",
                         "Request stand-by pilot cover",
                         f"Delay {v['name']} ETA to restore notice period"],
                        data_source="simulated",   # pilotage always simulated
                    ))

    # ── 4. Tug double-booking ─────────────────────────────────────────────────
    tug_ops = {}
    for ev in towage:
        for tug in ev["tugs"]:
            tug_ops.setdefault(tug["tug_id"], []).append(ev)
    seen = set()
    for tug_id, ops in tug_ops.items():
        op_dur = timedelta(hours=2)
        for i in range(len(ops)):
            for j in range(i + 1, len(ops)):
                a, b = ops[i], ops[j]
                key = tuple(sorted([a["id"], b["id"]]))
                if key in seen:
                    continue
                if a["vessel_id"] == b["vessel_id"]:
                    continue
                a_s = isoparse(a["scheduled_time"])
                b_s = isoparse(b["scheduled_time"])
                if a_s < b_s + op_dur and b_s < a_s + op_dur:
                    seen.add(key)
                    tname = next((t["tug_name"] for t in a["tugs"] if t["tug_id"] == tug_id), tug_id)
                    _cid = hashlib.md5(f"towage_resource-{tug_id}-{'-'.join(sorted([a['vessel_id'], b['vessel_id']]))}".encode()).hexdigest()[:8]
                    conflicts.append(_conflict(
                        _cid, "towage_resource", "WARNING", "medium",
                        [a["vessel_id"], b["vessel_id"]], [a["vessel_name"], b["vessel_name"]],
                        None, None, b["scheduled_time"],
                        (f"{tname} is assigned to {a['vessel_name']} ({a['direction']}) "
                         f"at {a_s.strftime('%H:%M')} and {b['vessel_name']} ({b['direction']}) "
                         f"at {b_s.strftime('%H:%M')} — operations overlap."),
                        ["Reassign a spare tug to the later operation",
                         "Adjust one operation time to avoid overlap",
                         "Confirm tug availability with tug operator"],
                        data_source="simulated",   # towage always simulated
                    ))

    # ── 5. ETA variance ───────────────────────────────────────────────────────
    for v in vessels:
        if v["status"] == "at_risk":
            _cid = hashlib.md5(f"eta_variance-{v['id']}".encode()).hexdigest()[:8]
            conflicts.append(_conflict(
                _cid, "eta_variance", "ADVISORY", "medium",
                [v["id"]], [v["name"]], v["berth_id"], v["berth_id"], v["eta"],
                (f"{v['name']} has reported significant ETA variance. "
                 f"Scheduled ETA {isoparse(v['eta']).strftime('%d %b %H:%M')} UTC "
                 f"may not be reliable. {v['notes'] or ''}"),
                ["Request updated ETA from ship's agent",
                 "Place pilotage and towage on standby",
                 "Notify berth terminal of potential schedule shift"],
                data_source=vessel_data_source,
            ))

    # ── 6. Bridge restrictions (Melbourne and other ports with bridge_restrictions) ──
    bridge_rules = _PORT_PROFILE.get("bridge_restrictions", [])
    if bridge_rules:
        # Air draught estimates by vessel type (m)
        AIR_DRAUGHT_EST = {
            "container": 45, "bulk carrier": 35, "bulker": 35,
            "tanker": 30, "general cargo": 28, "car carrier": 38,
            "roro": 38, "ro-ro": 38, "cruise": 55,
        }
        for v in vessels:
            if v.get("status") == "departed":
                continue
            vtype_lc = (v.get("vessel_type") or v.get("type") or "").lower()
            est_air  = next(
                (est for kw, est in AIR_DRAUGHT_EST.items() if kw in vtype_lc),
                32   # conservative default
            )
            for br in bridge_rules:
                bridge_name = br["name"]
                limit_m     = br["max_air_draught_m"]
                if est_air > limit_m:
                    sev = "critical" if br.get("absolute_limit") else "high"
                    _cid = hashlib.md5(f"bridge_restriction-{v['id']}-{bridge_name}".encode()).hexdigest()[:8]
                    conflicts.append(_conflict(
                        _cid, "bridge_restriction", "WARNING", sev,
                        [v["id"]], [v["name"]], None, bridge_name, fmt(now),
                        (f"{v['name']} estimated air draught {est_air}m exceeds "
                         f"{bridge_name} limit of {limit_m}m. "
                         f"{br.get('notes', '')}"),
                        [f"Confirm actual air draught with master before authorising transit",
                         f"Check current tidal state — clearance varies with tide height",
                         f"Contact {br.get('clearance_contact', 'port authority')} if clearance required",
                         f"Consider alternative berth below {bridge_name} if transit cannot be authorised"],
                        data_source="simulated",
                    ))

    sev_order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
    conflicts.sort(key=lambda c: (sev_order.get(c["severity"], 9), c["conflict_time"]))
    return conflicts


# ── Guidance generation ────────────────────────────────────────────────────────

def _short(c):
    t = c["conflict_type"]
    names = c["vessel_names"]
    if t == "berth_overlap":   return f"Berth conflict: {names[0]} / {names[1]}"
    if t == "berth_not_ready": return f"Berth not ready for {names[0]}"
    if t == "pilotage_window": return f"Short pilot notice: {names[0]}"
    if t == "towage_resource": return f"Tug double-booked: {names[0]} & {names[1]}"
    if t == "eta_variance":    return f"ETA uncertainty: {names[0]}"
    return c["description"][:60]

def _gpri(sev):
    return {"critical": "critical", "high": "high", "medium": "medium", "low": "info"}.get(sev, "info")

def build_guidance(conflicts, vessels, berths, pilotage, towage, now):
    items = []

    for c in conflicts:
        deadline = None
        if c["severity"] == "critical":
            ct = isoparse(c["conflict_time"])
            deadline = fmt(ct - timedelta(hours=1))
        items.append({
            "id": hashlib.md5(f"guidance-{c['id']}".encode()).hexdigest()[:8],
            "priority": _gpri(c["severity"]),
            "message": _short(c),
            "detail": c["description"],
            "resolution_options": c.get("resolution_options", []),
            "vessel_id": c["vessel_ids"][0] if c["vessel_ids"] else None,
            "vessel_name": c["vessel_names"][0] if c["vessel_names"] else None,
            "action_required": c["severity"] in ("critical", "high"),
            "deadline": deadline,
        })

    # Proactive: arrivals within 4h
    for v in vessels:
        if v["status"] in ("confirmed", "scheduled", "at_risk"):
            eta = isoparse(v["eta"])
            hrs = (eta - now).total_seconds() / 3600
            if 0 < hrs < 4:
                pri = "high" if hrs < 2 else "medium"
                items.append({
                    "id": hashlib.md5(f"arrival-{v['id']}".encode()).hexdigest()[:8],
                    "priority": pri,
                    "message": f"{v['name']} arriving in {hrs:.1f}h",
                    "detail": (
                        f"{v['name']} ({v['vessel_type']}, LOA {v['loa']}m) expected "
                        f"{eta.strftime('%H:%M')} UTC. Berth: {v['berth_id'] or 'TBA'}. "
                        f"Pilot: {'required' if v['pilotage_required'] else 'N/A'}. "
                        f"Towage: {'required' if v['towage_required'] else 'N/A'}."
                    ),
                    "resolution_options": [],
                    "vessel_id": v["id"], "vessel_name": v["name"],
                    "action_required": hrs < 2,
                    "deadline": fmt(eta - timedelta(hours=1)),
                })

    # Proactive: departures within 4h
    for v in vessels:
        if v["status"] == "berthed":
            etd = isoparse(v["etd"])
            hrs = (etd - now).total_seconds() / 3600
            if 0 < hrs < 4:
                items.append({
                    "id": hashlib.md5(f"departure-{v['id']}".encode()).hexdigest()[:8],
                    "priority": "medium",
                    "message": f"{v['name']} departing in {hrs:.1f}h",
                    "detail": (
                        f"{v['name']} departs {v['berth_id']} at {etd.strftime('%H:%M')} UTC. "
                        f"Ensure outbound pilot and towage confirmed."
                    ),
                    "resolution_options": [],
                    "vessel_id": v["id"], "vessel_name": v["name"],
                    "action_required": hrs < 2,
                    "deadline": fmt(etd - timedelta(minutes=30)),
                })

    # Maintenance berth returning
    for b in berths:
        if b["status"] == "maintenance" and b.get("readiness_time"):
            ready = isoparse(b["readiness_time"])
            hrs = (ready - now).total_seconds() / 3600
            if 0 < hrs < 12:
                items.append({
                    "id": hashlib.md5(f"maintenance-{b['id']}".encode()).hexdigest()[:8],
                    "priority": "info",
                    "message": f"{b['name']} back from maintenance at {ready.strftime('%H:%M')} UTC",
                    "detail": (
                        f"{b['name']} ({b['terminal']}) completing maintenance at "
                        f"{ready.strftime('%H:%M')} UTC — will be available for scheduling."
                    ),
                    "resolution_options": [],
                    "vessel_id": None, "vessel_name": None,
                    "action_required": False, "deadline": None,
                })

    pri_order = {"critical": 0, "high": 1, "medium": 2, "info": 3}
    items.sort(key=lambda g: pri_order.get(g["priority"], 9))
    return items


# ── Weather & Tides ────────────────────────────────────────────────────────────

_COMPASS = ["N","NNE","NE","ENE","E","ESE","SE","SSE","S","SSW","SW","WSW","W","WNW","NW","NNW"]

def make_weather(profile: dict = None):
    """Delegate to weather module — live Open-Meteo with port-specific sim fallback."""
    p = profile or _PORT_PROFILE
    return fetch_weather(p, utcnow())


# ── Beta 4: Port Rules & Weather Alert Detection ───────────────────────────────

HIGH_WINDAGE = {"Container", "RoRo"}  # Vessel types with elevated wind-manoeuvring risk

PORT_RULES_BRISBANE = {
    "wind_advisory": {
        "threshold_kts": 20, "applies_to": "high-windage vessels (Container, RoRo)",
        "rule_ref": "MSQ Port of Brisbane Procedures §5.3",
        "action": "Monitor closely. Advise masters of high-windage vessels to review manoeuvring plan.",
    },
    "wind_no_berthing": {
        "threshold_kts": 25, "applies_to": "all vessels",
        "rule_ref": "MSQ Port of Brisbane Procedures §5.3.1",
        "action": "No new berthing operations to commence. Vessels already alongside may remain.",
    },
    "wind_movements_suspended": {
        "threshold_kts": 30, "applies_to": "all vessels",
        "rule_ref": "MSQ Port of Brisbane Procedures §5.3.2",
        "action": "All vessel movements in port suspended. Masters to maintain engine readiness.",
    },
    "wind_engines_standby": {
        "threshold_kts": 40, "applies_to": "all berthed vessels",
        "rule_ref": "MSQ Port of Brisbane Procedures §5.3.3",
        "action": "All berthed vessels must have main engines on standby. Increased mooring watch.",
    },
    "swell_pilot_caution": {
        "threshold_m": 1.5, "applies_to": "pilot transfer operations at Brisbane Bar",
        "rule_ref": "SOLAS V/23, IMPA Pilot Ladder Guidelines 2022",
        "action": "Enhanced pilot ladder inspection required. Masters to assess transfer conditions at Bar.",
    },
    "swell_transfer_suspended": {
        "threshold_m": 2.0, "applies_to": "pilot ladder transfers at Brisbane Bar",
        "rule_ref": "SOLAS V/23, IMO Res. A.1045(27), IMPA 2022 §4.2",
        "action": "Pilot ladder transfers suspended at Bar. Helicopter transfer only if available.",
    },
    "vis_reduced_procedures": {
        "threshold_nm": 3.0, "applies_to": "all vessels",
        "rule_ref": "COLREGS Rule 19, QMSS Navigation Safety Direction 2013",
        "action": "Reduced visibility procedures in force. Proceed at safe speed, radar watch maintained.",
    },
    "vis_vts_restrictions": {
        "threshold_nm": 1.0, "applies_to": "all movements",
        "rule_ref": "MSQ Port of Brisbane Procedures §3.2, COLREGS Rule 19",
        "action": "VTS movement restrictions apply. No movements without explicit Brisbane VTS approval.",
    },
}

PORT_RULES_MELBOURNE = {
    "wind_advisory": {
        "threshold_kts": 20, "applies_to": "high-windage vessels (Container, RoRo)",
        "rule_ref": "VPC Harbour Master's Directions Ed. 13.1 §3.20",
        "action": "Monitor closely. Advise masters of high-windage vessels to review manoeuvring plan.",
    },
    "wind_no_berthing": {
        "threshold_kts": 25, "applies_to": "all vessels",
        "rule_ref": "VPC Harbour Master's Directions Ed. 13.1 §3.20",
        "action": "No new berthing operations to commence. Vessels already alongside may remain.",
    },
    "wind_movements_suspended": {
        "threshold_kts": 30, "applies_to": "all vessels",
        "rule_ref": "VPC Harbour Master's Directions Ed. 13.1 §3.20, Marine Safety Act 2010 (Vic)",
        "action": "All vessel movements suspended. Masters to maintain engine readiness. Notify Melbourne VTS.",
    },
    "wind_engines_standby": {
        "threshold_kts": 35, "applies_to": "all berthed vessels",
        "rule_ref": "VPC Harbour Master's Directions Ed. 13.1 §3.20",
        "action": "All berthed vessels must have main engines on standby. Increased mooring watch required.",
    },
    "swell_pilot_caution": {
        "threshold_m": 1.5, "applies_to": "pilot transfer operations at Port Phillip Heads",
        "rule_ref": "SOLAS V/23, IMPA Pilot Ladder Guidelines 2022",
        "action": "Enhanced pilot ladder inspection at Heads boarding ground. Masters to assess transfer conditions.",
    },
    "swell_transfer_suspended": {
        "threshold_m": 2.0, "applies_to": "pilot ladder transfers at Port Phillip Heads",
        "rule_ref": "SOLAS V/23, IMO Res. A.1045(27), IMPA 2022 §4.2",
        "action": "Pilot ladder transfers suspended at Heads. Hold inbound vessels at outer anchorage.",
    },
    "vis_reduced_procedures": {
        "threshold_nm": 3.0, "applies_to": "all vessels in Port Phillip Bay",
        "rule_ref": "COLREGS Rule 19, VPC Harbour Master's Directions Ed. 13.1 §3.18",
        "action": "Reduced visibility procedures in force. Proceed at safe speed, enhanced radar watch. Notify Melbourne VTS.",
    },
    "vis_vts_restrictions": {
        "threshold_nm": 1.0, "applies_to": "all movements in inner port",
        "rule_ref": "VPC Harbour Master's Directions Ed. 13.1 §3.18, COLREGS Rule 19",
        "action": "Melbourne VTS movement restrictions apply. No movements without explicit VTS approval.",
    },
    "bridge_air_draft_caution": {
        "threshold_m": 50.0, "applies_to": "vessels transiting West Gate Bridge to Yarra River berths",
        "rule_ref": "VPC Harbour Master's Directions Ed. 13.1 §4.2",
        "action": "Confirm air draft with Harbour Master before West Gate Bridge transit. Clearance 50.0m at MHWS.",
    },
}

PORT_RULES_DARWIN = {
    # Wind thresholds — specific limits in Port Notice PN014; using operational defaults below
    "wind_advisory": {
        "threshold_kts": 20, "applies_to": "high-windage vessels (Container, RoRo, OSV)",
        "rule_ref": "Darwin Port Handbook 2026 §5 / Port Notice PN014",
        "action": "Monitor closely. Advise masters of high-windage vessels to review manoeuvring plan. Confirm tug availability.",
    },
    "wind_no_berthing": {
        "threshold_kts": 30, "applies_to": "all vessels",
        "rule_ref": "Darwin Port Handbook 2026 §5 / Port Notice PN014 — operational default",
        "action": "No new berthing operations to commence. Vessels at berth may remain. Contact Darwin Port VHF 10.",
    },
    "wind_movements_suspended": {
        "threshold_kts": 35, "applies_to": "all vessels",
        "rule_ref": "Darwin Port Handbook 2026 §5 / Port Notice PN014 / Marine Act 2013 (NT)",
        "action": "All vessel movements suspended. Masters to maintain engine readiness. Notify Darwin Port on VHF 10.",
    },
    "wind_engines_standby": {
        "threshold_kts": 40, "applies_to": "all berthed vessels",
        "rule_ref": "Darwin Port Handbook 2026 §5 / Port Notice PN014 / Marine Act 2013 (NT)",
        "action": "All berthed vessels must have main engines on standby. Increased mooring watch. Monitor cyclone advisories if in season (Nov–Apr).",
    },
    "swell_pilot_caution": {
        "threshold_m": 1.5, "applies_to": "pilot transfer operations at Darwin Harbour entrance",
        "rule_ref": "SOLAS V/23, IMPA Pilot Ladder Guidelines 2022",
        "action": "Enhanced pilot ladder inspection required. Masters to assess transfer conditions at outer boarding ground.",
    },
    "swell_transfer_suspended": {
        "threshold_m": 2.5, "applies_to": "pilot ladder transfers at outer boarding ground",
        "rule_ref": "SOLAS V/23, IMO Res. A.1045(27), IMPA 2022 §4.2",
        "action": "Pilot ladder transfers suspended at outer boarding ground. Hold vessels at pilot boarding anchorage pending conditions.",
    },
    "vis_reduced_procedures": {
        "threshold_nm": 3.0, "applies_to": "all vessels in Darwin Harbour",
        "rule_ref": "COLREGS Rule 19, Darwin Port Handbook 2026 §4 / Marine Act 2013 (NT)",
        "action": "Reduced visibility procedures in force. Proceed at safe speed. Enhanced radar watch. Maintain listening watch VHF 10.",
    },
    "vis_vts_restrictions": {
        "threshold_nm": 1.0, "applies_to": "all movements in inner harbour",
        "rule_ref": "COLREGS Rule 19, Darwin Port Handbook 2026 §4 / Marine Act 2013 (NT)",
        "action": "VTS movement restrictions apply. No movements without explicit Darwin Port approval on VHF 10.",
    },
}

# Active rule set resolved at alert-generation time
PORT_RULES_BY_PORT = {
    "BRISBANE":  PORT_RULES_BRISBANE,
    "MELBOURNE": PORT_RULES_MELBOURNE,
    "DARWIN":    PORT_RULES_DARWIN,
}


def _swell_severity(height_m: float, period_s: float) -> float:
    """
    Compute effective swell height applying period correction.
    Long-period swell (≥10s) creates larger vessel motion: multiply height by 1.2.
    Returns effective swell height in metres.
    """
    return round(height_m * 1.2, 2) if period_s >= 10 else height_m


def detect_weather_alerts(weather: dict, vessels: list, now: datetime) -> list:
    """
    Generate port-wide weather alert signals using port-specific thresholds and
    regulatory references, plus SOLAS/IMPA pilot transfer rules.
    At most one alert per category (wind, swell, visibility, bridge).
    Uses the same _conflict() structure.
    """
    # Select rule set for the active port
    PORT_RULES = PORT_RULES_BY_PORT.get(_ACTIVE_PORT_ID, PORT_RULES_BRISBANE)

    alerts     = []
    wind_kts   = weather.get("wind_speed_kts", 0)
    swell_m    = weather.get("swell_height_m", 0.0)
    swell_per  = weather.get("swell_period_s", 7)
    vis_nm     = weather.get("visibility_nm", 10.0)
    wind_dir   = weather.get("wind_direction_label", "")
    eff_swell  = _swell_severity(swell_m, swell_per)

    hw_vessels = [v for v in vessels
                  if v.get("vessel_type") in HIGH_WINDAGE
                  and v["status"] != "departed"]
    all_active = [v for v in vessels if v["status"] != "departed"]
    berthed    = [v for v in vessels if v["status"] == "berthed"]
    inbound_pil = [v for v in vessels
                   if v["status"] not in ("berthed", "departed")
                   and v.get("pilotage_required")]

    # ── 1. Wind alert — thresholds from active port profile ───────────────────
    _w_crit    = _PORT_PROFILE.get("wind_limit_critical_knots", 40)
    _w_berth   = _PORT_PROFILE.get("wind_limit_berthing_knots", 25)
    _w_suspend = max(_w_berth + 5, _w_crit - 10)   # intermediate: movements suspended

    if wind_kts >= _w_crit:
        r = PORT_RULES["wind_engines_standby"]
        alerts.append(_conflict(
            "WX-WIND", "weather_wind", "WEATHER", "critical",
            [v["id"] for v in berthed], [v["name"] for v in berthed],
            None, None, fmt(now),
            (f"Wind {wind_kts}kts {wind_dir} — exceeds {_w_crit}kt threshold ({r['rule_ref']}). "
             f"All berthed vessels must have main engines on standby. "
             f"Increased mooring watch required."),
            [r["action"],
             f"Rule reference: {r['rule_ref']}",
             "Notify all masters and port operations centre immediately",
             "Arrange additional mooring lines as required"],
        ))
    elif wind_kts >= _w_suspend:
        r = PORT_RULES["wind_movements_suspended"]
        alerts.append(_conflict(
            "WX-WIND", "weather_wind", "WEATHER", "critical",
            [v["id"] for v in all_active], [v["name"] for v in all_active],
            None, None, fmt(now),
            (f"Wind {wind_kts}kts {wind_dir} — exceeds {_w_suspend}kt threshold ({r['rule_ref']}). "
             f"All vessel movements in port are suspended."),
            [r["action"],
             f"Rule reference: {r['rule_ref']}",
             "No inbound or outbound movements without Harbour Master approval",
             "Hold inbound vessels at anchorage pending improvement"],
        ))
    elif wind_kts >= _w_berth:
        r = PORT_RULES["wind_no_berthing"]
        alerts.append(_conflict(
            "WX-WIND", "weather_wind", "WEATHER", "high",
            [v["id"] for v in all_active], [v["name"] for v in all_active],
            None, None, fmt(now),
            (f"Wind {wind_kts}kts {wind_dir} — exceeds {_w_berth}kt no-berthing threshold ({r['rule_ref']}). "
             f"No new berthing operations may commence."),
            [r["action"],
             f"Rule reference: {r['rule_ref']}",
             f"Delay inbound vessels at anchorage until wind reduces below {_w_berth}kts",
             "Notify terminal operators and agents of potential delays"],
        ))
    elif wind_kts >= max(_w_berth - 5, 15) and hw_vessels:
        r = PORT_RULES["wind_advisory"]
        hw_names = [v["name"] for v in hw_vessels]
        alerts.append(_conflict(
            "WX-WIND", "weather_wind", "WEATHER", "medium",
            [v["id"] for v in hw_vessels], hw_names,
            None, None, fmt(now),
            (f"Wind {wind_kts}kts {wind_dir} — advisory for high-windage vessels ({r['rule_ref']}). "
             f"Container and RoRo vessels have elevated manoeuvring risk above 20kts."),
            [r["action"],
             f"Rule reference: {r['rule_ref']}",
             f"Advise masters of: {', '.join(hw_names[:4])}",
             "Confirm additional tug support is available if required"],
        ))

    # ── 2. Swell alert (one signal, highest applicable threshold) ────────────
    period_note = (f" (×1.2 long-period correction, period {swell_per}s)"
                   if swell_per >= 10 else f" (period {swell_per}s)")
    if eff_swell >= 2.0:
        r = PORT_RULES["swell_transfer_suspended"]
        aff = inbound_pil or all_active
        alerts.append(_conflict(
            "WX-SWELL", "weather_swell", "WEATHER", "critical",
            [v["id"] for v in aff], [v["name"] for v in aff],
            None, None, fmt(now),
            (f"Swell {swell_m}m{period_note} — effective {eff_swell}m exceeds 2.0m threshold. "
             f"Pilot ladder transfers are suspended under {r['rule_ref']}."),
            [r["action"],
             f"Rule reference: {r['rule_ref']}",
             "Activate helicopter transfer protocol if available",
             "Hold inbound vessels requiring pilotage at outer anchorage until swell subsides"],
        ))
    elif eff_swell >= 1.5:
        r = PORT_RULES["swell_pilot_caution"]
        aff = inbound_pil or all_active
        alerts.append(_conflict(
            "WX-SWELL", "weather_swell", "WEATHER", "high",
            [v["id"] for v in aff], [v["name"] for v in aff],
            None, None, fmt(now),
            (f"Swell {swell_m}m{period_note} — effective {eff_swell}m above 1.5m caution threshold. "
             f"Enhanced pilot transfer protocols apply ({r['rule_ref']})."),
            [r["action"],
             f"Rule reference: {r['rule_ref']}",
             "Inspect pilot ladder condition before each transfer",
             "Masters to confirm safe transfer conditions with pilot station"],
        ))

    # ── 3. Visibility alert (one signal, highest applicable threshold) ────────
    if vis_nm < 1.0:
        r = PORT_RULES["vis_vts_restrictions"]
        alerts.append(_conflict(
            "WX-VIS", "weather_visibility", "WEATHER", "critical",
            [v["id"] for v in all_active], [v["name"] for v in all_active],
            None, None, fmt(now),
            (f"Visibility {vis_nm:.1f}nm — below 1nm VTS restriction threshold ({r['rule_ref']}). "
             f"No vessel movements may commence without explicit VTS approval."),
            [r["action"],
             f"Rule reference: {r['rule_ref']}",
             "Contact VTS for approval before any departure or arrival",
             "All vessels to maintain enhanced radar watch and sound fog signals"],
        ))
    elif vis_nm < 3.0:
        r = PORT_RULES["vis_reduced_procedures"]
        alerts.append(_conflict(
            "WX-VIS", "weather_visibility", "WEATHER", "high",
            [v["id"] for v in all_active], [v["name"] for v in all_active],
            None, None, fmt(now),
            (f"Visibility {vis_nm:.1f}nm — reduced visibility procedures in force ({r['rule_ref']}). "
             f"All vessels to proceed at safe speed with enhanced radar watch."),
            [r["action"],
             f"Rule reference: {r['rule_ref']}",
             "Confirm all vessels have functioning radar and AIS active",
             "Notify inbound vessels to maintain enhanced lookout and sound appropriate signals"],
        ))

    # ── 4. Bridge air draft check (Melbourne only) ────────────────────────────
    bridge_rule = PORT_RULES.get("bridge_air_draft_caution")
    if bridge_rule:
        bridge_limit_m = _PORT_PROFILE.get("west_gate_bridge_air_draft_m", 50.0)
        # Flag any vessel whose air draft is not recorded (None) or exceeds the limit
        bridge_affected = [
            v for v in all_active
            if v.get("air_draft_m") is not None and v["air_draft_m"] >= bridge_limit_m * 0.9
        ]
        if bridge_affected:
            ba_names = [v["name"] for v in bridge_affected]
            alerts.append(_conflict(
                "WX-BRIDGE", "bridge_air_draft", "WEATHER", "high",
                [v["id"] for v in bridge_affected], ba_names,
                None, None, fmt(now),
                (f"{len(bridge_affected)} vessel(s) approaching West Gate Bridge clearance limit "
                 f"({bridge_limit_m}m at MHWS). Air draft confirmation required before Yarra River transit. "
                 f"({bridge_rule['rule_ref']})"),
                [bridge_rule["action"],
                 f"Rule reference: {bridge_rule['rule_ref']}",
                 f"Vessels requiring check: {', '.join(ba_names[:3])}",
                 "Contact Harbour Master's Office +61 3 9644 9777 to confirm clearance"],
            ))

    return alerts


def make_tides(bom_result: dict = None):
    """
    Build the tides dict consumed by the frontend.
    If bom_result is provided (from fetch_bom_tides), use its values.
    Otherwise fall back to the deterministic cosine model.
    """
    now = utcnow()

    # Port-specific tidal parameters (used in cosine model and as fallback)
    PERIOD = 12.42
    MEAN   = _PORT_PROFILE.get("tidal_mean_m", 1.40)
    AMP    = _PORT_PROFILE.get("tidal_amp_m",  0.90)

    def _cosine_next(ref_now):
        """Compute next HW/LW and hours-until from cosine model using port profile params."""
        day_h   = hashlib.md5(f"tide-{ref_now.strftime('%Y%m%d')}".encode()).hexdigest()
        phase_h = (int(day_h[0:4], 16) % int(PERIOD * 100)) / 100.0
        t       = (ref_now.hour + ref_now.minute / 60.0 + phase_h) % PERIOD
        t_to_hw = (PERIOD - t) % PERIOD
        t_to_lw = (PERIOD / 2 - t) % PERIOD
        if t_to_hw <= t_to_lw:
            return "HW", round(MEAN + AMP, 1), t_to_hw
        else:
            return "LW", round(MEAN - AMP, 1), t_to_lw

    if bom_result and bom_result.get("current_height_m") is not None:
        # ── BOM live data ────────────────────────────────────────────────────
        height    = bom_result["current_height_m"]
        state     = bom_result.get("state", "Unknown")
        nxt_type  = bom_result.get("next_event_type")
        nxt_time  = bom_result.get("next_event_time")
        nxt_ht    = bom_result.get("next_event_height_m")
        # Derive mean/amp from series if available
        series    = bom_result.get("series", [])
        if series:
            heights = [p["height_m"] for p in series]
            MEAN    = round(sum(heights) / len(heights), 2)
            AMP     = round((max(heights) - min(heights)) / 2, 2)
        # BOM for Melbourne often omits HW/LW markers — fill from cosine model
        if nxt_type is None or nxt_ht is None:
            nxt_type, nxt_ht, nxt_h = _cosine_next(now)
            nxt_time = now + timedelta(hours=nxt_h)
        next_time_str = fmt(nxt_time) if nxt_time else fmt(now + timedelta(hours=6))
        data_source   = "bom"
    else:
        # ── Cosine fallback ──────────────────────────────────────────────────
        day_h   = hashlib.md5(f"tide-{now.strftime('%Y%m%d')}".encode()).hexdigest()
        phase_h = (int(day_h[0:4], 16) % int(PERIOD * 100)) / 100.0
        t       = (now.hour + now.minute / 60.0 + phase_h) % PERIOD
        height  = round(MEAN + AMP * math.cos(2 * math.pi * t / PERIOD), 2)
        deriv   = -AMP * (2 * math.pi / PERIOD) * math.sin(2 * math.pi * t / PERIOD)

        state = "Slack" if abs(deriv) < 0.06 else ("Rising" if deriv > 0 else "Falling")

        nxt_type, nxt_ht, nxt_h = _cosine_next(now)
        next_time_str = fmt(now + timedelta(hours=nxt_h))
        data_source   = "cosine"

    berth_restrictions = ["B03", "B06"] if height < (MEAN - AMP + 1.1) else []

    return {
        "current_height_m":    height,
        "state":               state,
        "next_event_type":     nxt_type,
        "next_event_time":     next_time_str,
        "next_event_height_m": round(nxt_ht, 1) if nxt_ht is not None else None,
        "mean_height_m":       MEAN,
        "amplitude_m":         AMP,
        "berth_restrictions":  berth_restrictions,
        "data_source":         data_source,
    }


def compute_arrival_ukc(vessels: list, berths: list, now: datetime) -> dict:
    """
    Predicted UKC for each inbound vessel at its assigned berth at ETA.
    Uses the deterministic tide model to forecast height at ETA.
    Only looks ahead 48 h; ignores vessels already past ETA.
    UKC = (berth LAT depth + predicted tide at ETA) − vessel draught
    """
    berth_depth = {b["id"]: b["lat_depth_m"] for b in berths}
    entries = []
    for v in vessels:
        if v["status"] not in ("confirmed", "scheduled", "at_risk"):
            continue
        if not v.get("berth_id"):
            continue
        eta_dt = isoparse(v["eta"])
        hrs_to_eta = (eta_dt - now).total_seconds() / 3600
        if hrs_to_eta < 0 or hrs_to_eta > 48:
            continue
        predicted_tide = _predict_tide_height(eta_dt)
        lat_d  = berth_depth.get(v["berth_id"], 12.0)
        avail  = lat_d + predicted_tide
        ukc    = round(avail - v["draught"], 2)
        entries.append({
            "vessel_id":        v["id"],
            "vessel_name":      v["name"],
            "berth_id":         v["berth_id"],
            "eta":              v["eta"],
            "hrs_to_eta":       round(hrs_to_eta, 1),
            "ukc_m":            ukc,
            "predicted_tide_m": predicted_tide,
            "available_depth_m": round(avail, 2),
            "vessel_draught_m": v["draught"],
        })
    if not entries:
        return {"min_ukc_m": None, "critical_vessel": None, "critical_berth": None,
                "critical_eta": None, "status": "no_vessels", "all": []}
    entries.sort(key=lambda r: r["ukc_m"])
    mn     = entries[0]
    _u_min = _PORT_PROFILE.get("ukc_minimum_m", 0.5)
    status = ("critical" if mn["ukc_m"] < _u_min else
              "warning"  if mn["ukc_m"] < _u_min * 2 else "good")
    return {
        "min_ukc_m":       mn["ukc_m"],
        "critical_vessel": mn["vessel_name"],
        "critical_berth":  mn["berth_id"],
        "critical_eta":    mn["eta"],
        "hrs_to_eta":      mn["hrs_to_eta"],
        "status":          status,
        "all":             entries,
    }


# ── Beta 3: Berth Utilisation Forecast ────────────────────────────────────────

def make_berth_utilisation(vessels, berths, now):
    """48-hour berth occupancy forecast in 2-hour slots (24 slots total)."""
    SLOT_H = 2
    SLOTS  = 24
    result = []
    for b in berths:
        slots = []
        for s in range(SLOTS):
            slot_start = now + timedelta(hours=s * SLOT_H)
            slot_end   = slot_start + timedelta(hours=SLOT_H)
            occupants  = []
            for v in vessels:
                if v["berth_id"] != b["id"] or v["status"] == "departed":
                    continue
                v_start = isoparse(v["ata"] or v["eta"])
                v_end   = isoparse(v["atd"] or v["etd"])
                if v_start < slot_end and v_end > slot_start:
                    occupants.append(v["name"])
            if b["status"] == "maintenance":
                slot_status = "maintenance"
            elif occupants:
                slot_status = "occupied"
            else:
                slot_status = "free"
            slots.append({
                "slot":      s,
                "start":     fmt(slot_start),
                "end":       fmt(slot_end),
                "status":    slot_status,
                "occupants": occupants,
            })
        occ_slots = sum(1 for sl in slots if sl["status"] == "occupied")
        result.append({
            "berth_id":        b["id"],
            "berth_name":      b["name"],
            "terminal":        b["terminal"],
            "utilisation_pct": round(occ_slots / SLOTS * 100),
            "current_status":  b["status"],
            "slots":           slots,
        })
    return result


# ── Beta 3: ETD Risk Scoring ───────────────────────────────────────────────────

def compute_etd_risk(vessels, conflicts, weather, tides):
    """Score each vessel 0–100 for on-time departure risk."""
    conflict_vids  = {vid for c in conflicts for vid in (c.get("vessel_ids") or [])}
    wx_cond        = (weather or {}).get("conditions", "Good")
    tide_restricted = set((tides or {}).get("berth_restrictions", []))
    result = []
    for v in vessels:
        if v["status"] == "departed":
            continue
        score, factors = 0, []

        # 1. Status base risk
        base = {"at_risk": 35, "berthed": 5, "confirmed": 15, "scheduled": 8}.get(v["status"], 8)
        score += base

        # 2. Active conflict involvement
        if v["id"] in conflict_vids:
            score += 25
            factors.append("Active conflict")

        # 3. Weather conditions
        wx_pts = {"Poor": 18, "Moderate": 9, "Good": 2, "Excellent": 0}.get(wx_cond, 5)
        score += wx_pts
        if wx_pts >= 9:
            factors.append(f"{wx_cond} conditions")

        # 4. ETA variance reported
        if v.get("notes") and "variance" in (v.get("notes") or "").lower():
            score += 15
            factors.append("ETA variance reported")

        # 5. Tidal restriction on assigned berth
        if v.get("berth_id") in tide_restricted:
            score += 12
            factors.append("Tidal restriction")

        # 6. Large vessel operational complexity
        if v.get("loa", 0) > 220:
            score += 5
            factors.append("Large vessel")

        score = min(100, score)
        level = ("critical" if score >= 70 else
                 "high"     if score >= 45 else
                 "medium"   if score >= 25 else "low")
        result.append({
            "vessel_id":    v["id"],
            "vessel_name":  v["name"],
            "risk_score":   score,
            "risk_level":   level,
            "risk_factors": factors,
        })
    result.sort(key=lambda r: -r["risk_score"])
    return result


# ── Beta 3: Dashboard KPIs ─────────────────────────────────────────────────────

def make_dashboard(vessels, berths, conflicts, pilotage, towage,
                   weather, tides, etd_risk, berth_util, now):
    """Build executive KPI block for the port operations dashboard."""
    occupied   = sum(1 for b in berths if b["status"] in ("occupied", "reserved"))
    active_b   = sum(1 for b in berths if b["status"] != "maintenance")
    util_pct   = round(occupied / active_b * 100) if active_b else 0

    berthed    = [v for v in vessels if v["status"] == "berthed"]
    risky_ids  = {r["vessel_id"] for r in etd_risk if r["risk_level"] in ("high", "critical")}
    on_time    = sum(1 for v in berthed if v["id"] not in risky_ids)
    otd_pct    = round(on_time / len(berthed) * 100) if berthed else 100

    dwell_hrs  = []
    for v in berthed:
        if v.get("ata"):
            dwell_hrs.append((isoparse(v["etd"]) - isoparse(v["ata"])).total_seconds() / 3600)
    avg_dwell  = round(sum(dwell_hrs) / len(dwell_hrs), 1) if dwell_hrs else 0

    at_risk_n  = sum(1 for r in etd_risk if r["risk_level"] in ("high", "critical"))
    crit_n     = sum(1 for c in conflicts if c["severity"] == "critical")
    pilot_12h  = sum(1 for p in pilotage
                     if 0 <= (isoparse(p["scheduled_time"]) - now).total_seconds() / 3600 <= 12)
    tug_12h    = sum(1 for t in towage
                     if 0 <= (isoparse(t["scheduled_time"]) - now).total_seconds() / 3600 <= 12)
    avg_util_48h = round(
        sum(b["utilisation_pct"] for b in berth_util) / len(berth_util)
    ) if berth_util else 0

    exp_24 = sum(1 for v in vessels
                 if v["status"] not in ("berthed", "departed")
                 and (isoparse(v["eta"]) - now).total_seconds() / 3600 <= 24)

    return {
        "berth_utilisation_pct":    util_pct,
        "forecast_utilisation_48h": avg_util_48h,
        "on_time_departure_pct":    otd_pct,
        "avg_dwell_hours":          avg_dwell,
        "vessels_at_risk":          at_risk_n,
        "active_conflicts":         len(conflicts),
        "critical_conflicts":       crit_n,
        "pilot_ops_12h":            pilot_12h,
        "tug_ops_12h":              tug_12h,
        "vessels_in_port":          len(berthed),
        "vessels_expected_24h":     exp_24,
    }


# ── Beta 7: DUKC / ESG / Safety Score ─────────────────────────────────────────

def _safety_score_for_conflict(c: dict, weather: dict, tides: dict) -> str:
    """
    Roll up conflict severity + weather + tide into Low / Medium / High.
    Used to enrich each conflict card in the decision panel.
    """
    sev   = c.get("severity", "low")
    vis   = weather.get("visibility_nm",   10.0)
    swell = weather.get("swell_height_m",   0.5)
    wind  = weather.get("wind_speed_kts",   8.0)
    tide  = tides.get("current_height_m",   1.5)

    score = {"critical": 4, "high": 3, "medium": 2, "low": 1}.get(sev, 1)

    # Weather contribution
    if vis < 1 or swell > 2.5 or wind > 30:
        score += 3
    elif vis < 3 or swell > 1.5 or wind > 20:
        score += 2
    elif vis < 5 or swell > 1.0 or wind > 15:
        score += 1

    # Low tide adds caution
    if tide < 0.8:
        score += 1

    if score >= 6:
        return "high"
    elif score >= 4:
        return "medium"
    return "low"


def make_dukc_series(vessels: list, berths: list) -> dict:
    """
    Generate Dynamic UKC time series (48 h) for navigation channel and each
    active vessel.  Uses the deterministic tide model already in use.
    """
    now = utcnow()
    berth_depth = {b["id"]: b["lat_depth_m"] for b in berths}

    # Build shared 49-point hourly tide series (0 … 48 h)
    tide_pts = []
    for h in range(49):
        dt = now + timedelta(hours=h)
        tide_pts.append({"h": h, "t": fmt(dt), "tide": _predict_tide_height(dt)})

    # Channel series — deepest draught vessel drives the critical UKC
    ch_depth = _PORT_PROFILE.get("channel_depth_m", CHANNEL_DEPTH_M)
    ukc_min  = _PORT_PROFILE.get("ukc_minimum_m", 0.5)
    active = [v for v in vessels if v.get("status") != "departed"]
    max_dr = max((v.get("draught") or 9.0 for v in active), default=9.0)
    channel_pts = []
    for tp in tide_pts:
        ukc = round(ch_depth + tp["tide"] - max_dr, 2)
        channel_pts.append({**tp, "ukc": ukc, "safe": ukc >= ukc_min})

    # Per-vessel series
    vessel_series = []
    for v in active:
        draught  = v.get("draught") or 9.0
        bid      = v.get("berth_id")
        lat_d    = berth_depth.get(bid, ch_depth) if bid else ch_depth
        pts      = []
        for tp in tide_pts:
            ukc = round(lat_d + tp["tide"] - draught, 2)
            pts.append({**tp, "ukc": ukc, "safe": ukc >= ukc_min})

        min_ukc = min(p["ukc"] for p in pts)

        # Identify contiguous safe windows
        windows, in_w, w_start = [], False, None
        for p in pts:
            if p["safe"] and not in_w:
                in_w, w_start = True, p["t"]
            elif not p["safe"] and in_w:
                in_w = False
                windows.append({"start": w_start, "end": p["t"]})
        if in_w:
            windows.append({"start": w_start, "end": pts[-1]["t"]})

        vessel_series.append({
            "vessel_id":      v["id"],
            "vessel_name":    v["name"],
            "vessel_type":    v.get("vessel_type") or v.get("type") or "–",
            "draught_m":      draught,
            "berth_id":       bid,
            "berth_depth_m":  round(lat_d, 1),
            "min_ukc_m":      min_ukc,
            "tide_restricted": min_ukc < ukc_min,
            "safe_windows":   windows,
            "points":         pts,
        })

    return {
        "channel_depth_m":      ch_depth,
        "max_vessel_draught_m": round(max_dr, 1),
        "tide_series":          tide_pts,
        "channel_points":       channel_pts,
        "vessels":              vessel_series,
    }


_ESG_REASONS = [
    "Favourable SE current — advance arrival captures tide-assisted approach",
    "Tide window alignment — delay optimises flood-tide UKC margin",
    "Berth congestion avoidance — adjusted ETA prevents anchorage wait",
    "Current-optimised track — Brisbane Current adjustment reduces fuel burn",
    "Tidal gate optimisation — realigned ETA achieves maximum depth window",
]


def make_esg_data(vessels: list, now: datetime) -> dict:
    """
    Simulate Ocean Intelligence voyage efficiency optimisation data.
    Conservative, believable figures — clearly labelled as projections.
    """
    items = []
    for v in vessels:
        if v.get("status") == "departed":
            continue
        loa   = float(v.get("loa") or 0)
        h_val = int(hashlib.md5(v["id"].encode()).hexdigest(), 16)

        optimised = (h_val % 4) != 0   # ~75 % of calls get optimised

        adj_mins  = 15 + (h_val % 76)

        # Fuel saving scales with vessel size (tonnes HFO)
        base_fuel = 3.5 if loa > 250 else 2.5 if loa > 180 else 1.5 if loa > 120 else 0.8
        fuel_t    = round(base_fuel * (1 + (h_val % 50) / 100), 1) if optimised else 0.0
        co2_t     = round(fuel_t * 3.14, 1)   # IMO HFO CO₂ factor
        cost_usd  = int(fuel_t * 650)          # ~USD 650 / tonne HFO

        reason = (_ESG_REASONS[h_val % len(_ESG_REASONS)]
                  if optimised else "No optimisation required for this voyage")

        items.append({
            "vessel_id":           v["id"],
            "vessel_name":         v["name"],
            "vessel_type":         v.get("vessel_type") or v.get("type") or "–",
            "loa_m":               loa or None,
            "optimised":           optimised,
            "arrival_adj_mins":    adj_mins if optimised else 0,
            "fuel_saving_t":       fuel_t,
            "co2_saving_t":        co2_t,
            "cost_saving_usd":     cost_usd,
            "optimisation_reason": reason,
        })

    total_co2  = round(sum(i["co2_saving_t"]  for i in items), 1)
    total_fuel = round(sum(i["fuel_saving_t"] for i in items), 1)
    total_cost = sum(i["cost_saving_usd"]     for i in items)
    n_opt      = sum(1 for i in items if i["optimised"])
    proj       = 30 * 4   # rough 30-day projection multiplier (4 calls / day)

    return {
        "vessels": items,
        "summary": {
            "calls_this_period":     len(items),
            "calls_optimised":       n_opt,
            "co2_saved_t":           total_co2,
            "fuel_saved_t":          total_fuel,
            "cost_saved_usd":        total_cost,
            "monthly_co2_proj_t":    int(total_co2  * proj / max(len(items), 1)),
            "monthly_fuel_proj_t":   int(total_fuel * proj / max(len(items), 1)),
            "monthly_cost_proj_usd": int(total_cost * proj / max(len(items), 1)),
        },
    }


# ── What If helpers ────────────────────────────────────────────────────────────

def _apply_whatif_to_vessels(vessels, adjustments):
    """Return a modified copy of vessels with what-if adjustments applied."""
    import copy, datetime as _dt
    vs = copy.deepcopy(vessels)
    offline_resources = set()

    for adj in adjustments:
        atype      = adj.get("type", "")
        vname      = adj.get("vessel", "")
        delta_mins = 0

        if atype == "eta_push":
            delta_mins = adj.get("minutes", 60)
            if adj.get("direction") == "advance":
                delta_mins = -delta_mins
        elif atype == "hold_anchorage":
            delta_mins = adj.get("hours", 2) * 60

        for v in vs:
            name = v.get("name") or v.get("vessel_name", "")
            if name != vname:
                continue
            if atype in ("eta_push", "hold_anchorage"):
                for fld in ("eta", "etd", "scheduled_arrival", "scheduled_departure"):
                    raw = v.get(fld)
                    if not raw:
                        continue
                    try:
                        t  = _dt.datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
                        v[fld] = (t + _dt.timedelta(minutes=delta_mins)).isoformat()
                    except Exception:
                        pass
                if atype == "hold_anchorage":
                    v["status"] = "at_anchorage"
            elif atype == "berth_change":
                new_b = adj.get("new_berth", "")
                v["berth"] = "" if new_b == "__anch__" else new_b
                if new_b == "__anch__":
                    v["status"] = "at_anchorage"
            elif atype == "resource_offline":
                offline_resources.add(adj.get("resource", ""))

    # Mark offline resources in towage/pilotage assignments (best-effort)
    for v in vs:
        tug_assign = v.get("tug_assignment") or []
        v["tug_assignment"] = [t for t in tug_assign
                               if ("tug:" + str(t)) not in offline_resources]
    return vs


def _whatif_shadow(conflict_id, adjustments, base_vessels, base_conflicts):
    """
    Run a shadow simulation: apply adjustments, re-evaluate, return diff.
    Does NOT mutate any globals.
    """
    import copy
    modified_vessels = _apply_whatif_to_vessels(base_vessels, adjustments)
    affected_names   = {a.get("vessel", "") for a in adjustments}

    # Conflict types that can be resolved by timing/berth adjustments
    TIMING_RESOLVABLE = {"berth_conflict", "tug_double_booking", "pilot_conflict",
                         "departure_conflict", "arrival_conflict", "sequence_conflict"}

    resolved = []
    for c in base_conflicts:
        c_vessels = set((c.get("vessel_names") or []) +
                        ([c.get("vessel_name")] if c.get("vessel_name") else []))
        if not c_vessels & affected_names:
            continue
        adj_for_c = [a for a in adjustments if a.get("vessel") in c_vessels]
        ctype     = (c.get("conflict_type") or "").lower()
        signal    = (c.get("signal_type") or "").upper()

        # Weather/UKC conflicts are not resolved by scheduling changes
        if signal == "WEATHER":
            continue

        # ETA/hold shifts resolve timing/berth conflicts if shift >= 30 min
        timing_adjs = [a for a in adj_for_c if a["type"] in ("eta_push", "hold_anchorage")]
        total_shift = sum(
            a.get("minutes", a.get("hours", 0) * 60) for a in timing_adjs
        )
        if total_shift >= 30 and (not ctype or any(k in ctype for k in ("berth", "tug", "pilot", "conflict", "sequence"))):
            resolved.append(c)
            continue

        # Berth change resolves berth conflicts for the moved vessel
        if any(a["type"] == "berth_change" for a in adj_for_c) and "berth" in ctype:
            resolved.append(c)

    new_conflicts = []
    cost_delta    = 0

    # Estimate cost savings from resolved conflicts using cost_usd
    for c in resolved:
        opts = (c.get("decision_support") or {}).get("options") or []
        if opts:
            # Use the recommended option's cost, fall back to first option
            rec_opt = next((o for o in opts if o.get("recommended")), opts[0])
            cost_delta -= int(rec_opt.get("cost_usd") or 0)

    # Add cost of the adjustments being applied — these are real expenses,
    # so they offset the "avoided conflict" savings above.
    for adj in adjustments:
        atype = adj.get("type", "")
        if atype == "hold_anchorage":
            hours = int(adj.get("hours") or 2)
            cost_delta += hours * 900          # ~$900/hr anchorage + delay fees
        elif atype == "eta_push":
            mins = int(adj.get("minutes") or 60)
            if adj.get("direction") == "advance":
                cost_delta += 300              # coordination / overtime
            else:
                cost_delta += 500 + mins * 8  # delay fees: base + per-minute
        elif atype == "berth_change":
            cost_delta += 600                  # operational cost of berth reassignment

    # Detect potential new berth overlap from reassigned vessels
    berth_map: dict = {}
    for v in modified_vessels:
        b = v.get("berth")
        if not b:
            continue
        vn = v.get("name") or v.get("vessel_name", "")
        berth_map.setdefault(b, []).append(vn)
    for berth, occupants in berth_map.items():
        moved_here = [n for n in occupants if n in affected_names]
        others     = [n for n in occupants if n not in affected_names]
        if moved_here and others:
            new_conflicts.append({
                "id":          f"wi-berth-{berth}",
                "description": f"New berth overlap at {berth} — {', '.join(moved_here)} conflicts with {', '.join(others)}",
                "severity":    "critical",
            })
            cost_delta += 2500

    # Resource-offline new conflicts
    for adj in adjustments:
        if adj["type"] == "resource_offline":
            rname = adj.get("resource", "").split(":")[-1]
            new_conflicts.append({
                "id":          f"wi-res-{rname}",
                "description": f"{rname} unavailable — dependent assignments require reassignment",
                "severity":    "high",
            })

    # Generate revised recommendation
    if resolved and not new_conflicts:
        new_rec = "Proceed with adjusted schedule"
        if cost_delta < -500:
            cost_note = f"Net saving: ~${abs(cost_delta):,}."
        elif cost_delta > 500:
            cost_note = f"Net additional cost: ~${cost_delta:,}."
        else:
            cost_note = "Cost-neutral vs current trajectory."
        new_why = (f"This scenario resolves {len(resolved)} conflict(s) with no new issues. "
                   f"{cost_note}")
    elif new_conflicts and not resolved:
        new_rec = "Reconsider — scenario creates new conflicts"
        new_why = (f"{len(new_conflicts)} new conflict(s) introduced with no existing conflicts resolved. "
                   "Net outcome is worse than current plan.")
    elif resolved and new_conflicts:
        new_rec = "Mixed outcome — evaluate trade-offs carefully"
        new_why = (f"Resolves {len(resolved)} conflict(s) but introduces {len(new_conflicts)} new one(s). "
                   "Weigh cost delta against operational disruption.")
    else:
        new_rec = "No material conflict impact"
        new_why = "Adjustments do not significantly change the conflict landscape."

    return {
        "resolved":          [{"id": c["id"], "description": c.get("description", ""), "severity": c.get("severity", "")} for c in resolved],
        "new_conflicts":     new_conflicts,
        "cost_delta":        cost_delta,
        "new_recommendation": new_rec,
        "new_reasoning":     new_why,
    }


# ── Summary builder ────────────────────────────────────────────────────────────

def build_summary():
    now = utcnow()
    with _profile_lock:
        profile         = dict(_PORT_PROFILE)
        active_port_id  = _ACTIVE_PORT_ID

    # ── Beta 8b: Data layer — port profile drives vessel + tidal source ────────
    # Priority order: (1) vessel scraper for configured port, (2) QShips fallback,
    # (3) simulation.  Tides: BOM live > cosine fallback.
    ds          = get_data_source()
    is_live     = False
    using_live_vessel = False
    using_live_tidal  = False

    # Attempt vessel scraper for active port profile
    scrape_result = fetch_vessel_movements(profile, now)
    if scrape_result["using_live_data"] and scrape_result["vessels"]:
        try:
            vessels   = build_vessels_from_qships({"vessels": scrape_result["vessels"], "berths": []})
            berths    = make_berths(now)   # always simulated berths for now
            port_name = profile["display_name"]
            is_live   = True
            using_live_vessel = True
            log.info("Using live vessel data: %d vessels from %s",
                     len(vessels), profile["short_name"])
        except Exception as exc:
            log.error("Live vessel build failed (%s) — falling back", exc)
            vessels = None

    if not using_live_vessel:
        # QShips fallback for Brisbane — only use when active port IS Brisbane
        if ds["source"] == "qships" and _qships_data and active_port_id == "BRISBANE":
            try:
                vessels   = build_vessels_from_qships(_qships_data)
                berths    = build_berths_from_qships(_qships_data)
                port_name = _qships_data.get("port_name", profile["display_name"])
                is_live   = True
            except Exception as exc:
                log.error("QShips vessel build failed (%s) — falling back to simulation", exc)
                vessels = None

    # MST AIS connector — real vessel identities, simulated operational detail
    if not using_live_vessel and not is_live and mst_scraper.is_configured():
        unloco = profile.get("unloco")
        if unloco:
            try:
                berths = make_berths(now)
                mst_vessels = mst_scraper.build_horizon_vessels(unloco, berths, now)
                if mst_vessels:
                    vessels   = mst_vessels
                    port_name = profile["display_name"]
                    is_live   = True
                    using_live_vessel = True
                    log.info("MST AIS: %d vessels loaded for %s", len(vessels), unloco)
            except Exception as exc:
                log.error("MST vessel build failed (%s) — falling back to simulation", exc)
                vessels = None

    if not using_live_vessel and (not is_live or not vessels):
        berths    = make_berths(now)
        vessels   = make_vessels(now)
        port_name = profile["display_name"]
        is_live   = False

    # BOM tidal data
    try:
        bom_result = fetch_bom_tides(profile, now)
        using_live_tidal = bom_result["source"] == "bom"
    except Exception as exc:
        log.error("BOM tides fetch failed: %s — using cosine", exc)
        bom_result = None
        using_live_tidal = False

    try:
        pilotage = make_pilotage(vessels, now, profile)
    except Exception as exc:
        log.error("make_pilotage failed: %s — using empty list", exc)
        pilotage = []

    try:
        towage = make_towage(vessels, now, profile)
    except Exception as exc:
        log.error("make_towage failed: %s — using empty list", exc)
        towage = []

    weather  = make_weather(profile)
    tides    = make_tides(bom_result)

    # ── Apply What If overlay (if a scenario is active) ────────────────────
    with _whatif_lock:
        wi_overlay = dict(_WHATIF_OVERLAY)
    if wi_overlay.get("active"):
        vessels = _apply_whatif_to_vessels(vessels, wi_overlay.get("adjustments", []))

    # Operational conflicts + Beta 4 weather alerts merged and re-sorted
    try:
        op_conflicts = detect_conflicts(vessels, berths, pilotage, towage, now, is_live=is_live)
    except Exception as exc:
        log.error("detect_conflicts failed: %s — using empty list", exc)
        op_conflicts = []
    weather_alerts = detect_weather_alerts(weather, vessels, now)
    sev_order      = {"critical": 0, "high": 1, "medium": 2, "low": 3}
    conflicts      = sorted(
        op_conflicts + weather_alerts,
        key=lambda c: (sev_order.get(c["severity"], 9), c["conflict_time"]),
    )

    guidance   = build_guidance(conflicts, vessels, berths, pilotage, towage, now)

    # Beta 7: enrich every conflict with a consolidated safety score
    for c in conflicts:
        c["safety_score"] = _safety_score_for_conflict(c, weather, tides)

    # Beta 3 additions
    berth_util = make_berth_utilisation(vessels, berths, now)
    etd_risk   = compute_etd_risk(vessels, conflicts, weather, tides)
    dashboard  = make_dashboard(vessels, berths, conflicts, pilotage, towage,
                                weather, tides, etd_risk, berth_util, now)
    ukc        = compute_ukc(vessels, berths, tides["current_height_m"])
    arrival_ukc = compute_arrival_ukc(vessels, berths, now)

    # Beta 7 additions
    dukc = make_dukc_series(vessels, berths)
    esg  = make_esg_data(vessels, now)

    occupied   = sum(1 for b in berths if b["status"] in ("occupied", "reserved"))
    available  = sum(1 for b in berths if b["status"] == "available")
    in_port    = sum(1 for v in vessels if v["status"] == "berthed")
    exp_24     = sum(1 for v in vessels
                     if v["status"] not in ("berthed", "departed")
                     and v.get("eta") and (isoparse(v["eta"]) - now).total_seconds() / 3600 <= 24)
    dep_24     = sum(1 for v in vessels
                     if v["status"] == "berthed"
                     and v.get("etd") and (isoparse(v["etd"]) - now).total_seconds() / 3600 <= 24)
    critical   = sum(1 for c in conflicts if c["severity"] == "critical")

    # Build a port-aware data source label
    if using_live_vessel and any(v.get("source") == "mst" for v in vessels):
        _ds_label = f"AIS Live — {profile['display_name']}"
    elif using_live_vessel:
        _ds_label = f"Live — {profile['display_name']}"
    elif is_live and ds["source"] == "qships":
        _ds_label = ds["label"]
    else:
        _ds_label = f"{profile['short_name']} — Simulation"

    return {
        "port_name":       port_name,
        "generated_at":    fmt(now),
        "lookahead_hours": 48,
        "data_source":     "live" if using_live_vessel else ds["source"],
        "data_source_label": _ds_label,
        "scraped_at":      scrape_result.get("scraped_at") or ds["scraped_at"],
        "port_status": {
            "berths_occupied":    occupied,
            "berths_available":   available,
            "berths_total":       len(berths),
            "vessels_in_port":    in_port,
            "vessels_expected_24h": exp_24,
            "vessels_departing_24h": dep_24,
            "active_conflicts":   len(conflicts),
            "critical_conflicts": critical,
            "pilots_available":   profile.get("pilots_available", 3),
            "tugs_available":     profile.get("tugs_available", 4),
        },
        "vessels":           vessels,
        "berths":            berths,
        "pilotage":          pilotage,
        "towage":            towage,
        "port_tugs":         profile.get("tugs", [{"name": t, "bollard_pull_t": 65} for t in TUGS]),
        "port_gangs":        profile.get("mooring_gangs", []),
        "conflicts":         conflicts,
        "guidance":          guidance,
        "port_geo":          profile.get("port_geo", PORT_GEO),
        "weather":           weather,
        "tides":             tides,
        "berth_utilisation": berth_util,
        "etd_risk":          etd_risk,
        "dashboard":         dashboard,
        "ukc":               ukc,
        "arrival_ukc":       arrival_ukc,
        "dukc":              dukc,
        "esg":               esg,
        "port_profile": {
            "id":                    active_port_id,
            "display_name":          profile["display_name"],
            "short_name":            profile["short_name"],
            "timezone":              profile.get("timezone", "Australia/Brisbane"),
            "vts_callsign":          profile.get("vts_callsign", "VTS"),
            "harbour_master":        profile.get("harbour_master", ""),
            "using_live_vessel_data":  using_live_vessel,
            "using_live_tidal_data":   using_live_tidal,
            "using_live_weather_data": weather.get("source") == "live",
            "bom_station_id":        profile.get("bom_station_id"),
            "data_last_refreshed":   fmt(now),
            "available_ports":       list_profiles(),
        },
    }


# ── HTTP handler ──────────────────────────────────────────────────────────────

class HorizonHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        if args and str(args[1]) not in ("200", "304"):
            super().log_message(format, *args)

    def _is_authenticated(self) -> bool:
        token = _get_cookie(self, _COOKIE_NAME)
        return token is not None and _verify_token(token)

    def _redirect(self, location: str, status: int = 302):
        self.send_response(status)
        self.send_header("Location", location)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_POST(self):
        path = self.path.split("?")[0]
        if path == "/login":
            self._handle_login()
        elif path == "/logout":
            self._handle_logout()
        elif path == "/api/set_port":
            if not self._is_authenticated():
                self.send_error(401)
                return
            self._set_port()
        elif path == "/api/send-brief":
            if not self._is_authenticated():
                self.send_error(401)
                return
            self._send_brief()
        elif path == "/api/whatif":
            self._whatif()
        elif path == "/api/apply-whatif":
            self._apply_whatif()
        elif path == "/api/clear-whatif":
            self._clear_whatif()
        else:
            self.send_error(405)

    def _handle_login(self):
        try:
            length = int(self.headers.get("Content-Length", 0))
            body   = self.rfile.read(length).decode()
            params = parse_qs(body)
            user   = params.get("username", [""])[0].strip()
            pw     = params.get("password", [""])[0].strip()
            next_p = params.get("next", ["/"])[0]
            # Validate next to prevent open redirect
            if not next_p.startswith("/"):
                next_p = "/"
            user_ok = hmac.compare_digest(user, _AUTH_USER)
            pass_ok = hmac.compare_digest(pw,   _AUTH_PASS)
            if user_ok and pass_ok:
                token = _make_token()
                self.send_response(302)
                self.send_header("Location", next_p)
                self.send_header(
                    "Set-Cookie",
                    f"{_COOKIE_NAME}={token}; Path=/; HttpOnly; SameSite=Strict; Max-Age={_COOKIE_TTL}"
                )
                self.send_header("Content-Length", "0")
                self.end_headers()
            else:
                self._serve_login(error=True, next_path=next_p)
        except Exception as exc:
            log.error("Login error: %s", exc)
            self.send_error(500)

    def _handle_logout(self):
        self.send_response(302)
        self.send_header("Location", "/login")
        self.send_header(
            "Set-Cookie",
            f"{_COOKIE_NAME}=; Path=/; HttpOnly; Max-Age=0"
        )
        self.send_header("Content-Length", "0")
        self.end_headers()

    def _serve_login(self, error: bool = False, next_path: str = "/"):
        error_html = (
            '<div class="login-error">Incorrect username or password. Please try again.</div>'
            if error else ""
        )
        # Only allow relative paths to prevent open redirect
        if not next_path.startswith("/"):
            next_path = "/"
        next_field = f'<input type="hidden" name="next" value="{next_path}" />'
        page = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Project Horizon - Sign In</title>
  <style>
    *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{
      min-height: 100vh; display: flex; align-items: center; justify-content: center;
      background: #070f1e;
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    }}
    .login-wrap {{
      width: 100%; max-width: 380px; padding: 0 20px;
    }}
    .login-card {{
      background: #0d1b2e; border: 1px solid rgba(56,189,248,.2);
      border-radius: 12px; padding: 40px 36px; text-align: center;
    }}
    .login-logo {{
      height: 216px; width: auto; display: block; margin: 0 auto -40px;
    }}
    .login-title {{
      font-size: 16px; font-weight: 700; letter-spacing: 1.2px;
      text-transform: uppercase; color: #38bdf8; margin-bottom: 20px;
    }}
    .login-amsg {{
      background: rgba(255,255,255,.95); border-radius: 6px;
      padding: 6px 16px; display: inline-flex; align-items: center;
      margin-bottom: 28px;
    }}
    .login-amsg img {{ height: 44px; width: auto; }}
    label {{
      display: block; text-align: left; font-size: 11px; font-weight: 600;
      letter-spacing: .5px; text-transform: uppercase; color: #64748b;
      margin-bottom: 5px; margin-top: 16px;
    }}
    label:first-of-type {{ margin-top: 0; }}
    input {{
      width: 100%; padding: 10px 12px; border-radius: 6px;
      background: rgba(255,255,255,.05); border: 1px solid rgba(56,189,248,.25);
      color: #e2e8f0; font-size: 14px; outline: none;
      transition: border-color .2s;
    }}
    input:focus {{ border-color: #38bdf8; }}
    .login-btn {{
      width: 100%; margin-top: 24px; padding: 11px;
      background: #38bdf8; color: #07111e; font-size: 14px; font-weight: 700;
      border: none; border-radius: 6px; cursor: pointer;
      transition: background .2s;
    }}
    .login-btn:hover {{ background: #7dd3fc; }}
    .login-error {{
      margin-top: 16px; padding: 9px 12px; border-radius: 6px;
      background: rgba(239,68,68,.15); border: 1px solid rgba(239,68,68,.4);
      color: #f87171; font-size: 12px;
    }}
    .login-footer {{
      margin-top: 28px; font-size: 10px; color: #334155; letter-spacing: .4px;
    }}
  </style>
</head>
<body>
  <div class="login-wrap">
    <div class="login-card">
      <img src="/logo" class="login-logo" alt="Project Horizon" />
      <div class="login-title">Port Operations Intelligence</div>
      <div class="login-amsg">
        <img src="/amsg-logo" alt="AMS Group"
             onerror="this.closest('.login-amsg').style.display='none'" />
      </div>
      <form method="POST" action="/login" autocomplete="on">
        {next_field}
        <label for="username">Username</label>
        <input id="username" name="username" type="text"
               autocomplete="username" required autofocus />
        <label for="password">Password</label>
        <input id="password" name="password" type="password"
               autocomplete="current-password" required />
        <button class="login-btn" type="submit">Sign In →</button>
        {error_html}
      </form>
      <div class="login-footer">AMS GROUP · CONFIDENTIAL</div>
    </div>
  </div>
</body>
</html>"""
        body = page.encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _set_port(self):
        """Switch active port profile for the current session (in-memory only)."""
        global _ACTIVE_PORT_ID, _PORT_PROFILE
        try:
            length = int(self.headers.get("Content-Length", 0))
            body   = json.loads(self.rfile.read(length))
            port_id = body.get("port", "").strip().upper()
            if not port_id:
                self._json({"success": False, "error": "Missing 'port' field"})
                return
            new_profile = get_profile(port_id)
            # Validate against the canonical profile registry — no hardcoded list
            if port_id not in PORT_PROFILES:
                self._json({"success": False, "error": f"Unknown port '{port_id}'"})
                return
            with _profile_lock:
                _ACTIVE_PORT_ID = port_id
                _PORT_PROFILE   = new_profile
            log.info("Port profile switched to %s", port_id)
            self._json({
                "success":      True,
                "port":         port_id,
                "display_name": new_profile["display_name"],
            })
        except Exception as exc:
            log.error("set_port error: %s", exc)
            self._json({"success": False, "error": str(exc)})

    def do_GET(self):
        path = self.path.split("?")[0]

        # ── Auth gate ──────────────────────────────────────────────────────────
        if path == "/login":
            from urllib.parse import urlparse, parse_qs
            qs = parse_qs(urlparse(self.path).query)
            next_path = qs.get("next", ["/"])[0]
            self._serve_login(next_path=next_path)
            return
        if path not in _PUBLIC_PATHS and not self._is_authenticated():
            safe_next = self.path if self.path.startswith("/") else "/"
            self._redirect(f"/login?next={safe_next}")
            return

        if path == "/api/summary":
            try:
                self._json(build_summary())
            except Exception as exc:
                log.error("build_summary crashed: %s — forcing simulation fallback", exc)
                global _qships_data
                _qships_data = None
                try:
                    self._json(build_summary())
                except Exception as exc2:
                    log.error("Simulation fallback also crashed: %s", exc2)
                    self.send_error(500)
        elif path == "/api/scrape":
            self._scrape()
        elif path == "/api/debug":
            self._scrape_debug()
        elif path == "/health":
            ds = get_data_source()
            self._json({"status": "ok", "time": fmt(utcnow()),
                        "data_source": ds["source"], "scraped_at": ds["scraped_at"]})
        elif path == "/api/mst-status":
            with _profile_lock:
                unloco = _PORT_PROFILE.get("unloco", "")
            result = {
                "mst_configured": mst_scraper.is_configured(),
                "unloco": unloco,
                "vessels": [],
                "error": None,
            }
            if mst_scraper.is_configured() and unloco:
                try:
                    result["vessels"] = mst_scraper.get_vessels_in_port(unloco)
                except Exception as exc:
                    result["error"] = str(exc)
            self._json(result)
        elif path in ("/", "/index.html"):
            # Auto-redirect mobile browsers to the PWA companion, unless ?full=1
            from urllib.parse import urlparse, parse_qs as _pqs
            _qs = _pqs(urlparse(self.path).query)
            if _qs.get("full", ["0"])[0] != "1":
                ua = self.headers.get("User-Agent", "")
                if any(k in ua for k in ("iPhone", "Android", "Mobile", "iPod")):
                    self._redirect("/mobile")
                    return
            self._html()
        elif path == "/mobile":
            self._serve_mobile()
        elif path == "/mobile-icon":
            self._mobile_icon()
        elif path == "/logo":
            self._logo()
        elif path == "/amsg-logo":
            self._amsg_logo()
        elif path == "/api/port-brief":
            self._port_brief_pdf()
        elif path == "/api/brief-config":
            self._json({
                "default_recipients": _BRIEF_RECIPIENTS,
                "smtp_configured": bool(_SMTP_HOST),
            })
        else:
            self.send_error(404)

    def _scrape(self):
        """Trigger a manual scrape on demand and wait for it to complete."""
        global _scraping
        if _scraping:
            self._json({"status": "scraping_in_progress",
                        "message": "A scrape is already running. Try again shortly."})
            return
        log.info("Manual scrape triggered via /api/scrape")
        # Run synchronously in this request thread (blocks until done)
        try:
            import qships_scraper
            success = qships_scraper.run_scrape()
            if success:
                load_qships_data()
            ds = get_data_source()
            self._json({
                "status":        "ok" if success else "failed",
                "vessel_count":  _qships_data.get("vessel_count", 0) if _qships_data else 0,
                "scraped_at":    ds["scraped_at"],
                "source":        ds["label"],
            })
        except ImportError:
            self._json({"status": "error",
                        "message": "qships_scraper module not found"})
        except Exception as e:
            self._json({"status": "error", "message": str(e)})

    def _scrape_debug(self):
        """Return diagnostic info: compact scraper summary + state."""
        debug_file = Path(__file__).parent / "qships_debug.json"
        out = {
            "qships_data_exists":   (Path(__file__).parent / "qships_data.json").exists(),
            "debug_file_exists":    debug_file.exists(),
            "data_source":          get_data_source(),
            "scraping_in_progress": _scraping,
        }
        if debug_file.exists():
            try:
                summary = json.loads(debug_file.read_text(encoding="utf-8"))
                # The debug file is a compact summary written by the scraper.
                # Keys: http_status, response_bytes, top_level_keys, result_type,
                #       result_keys, columns, row_count, sample_rows
                out["scraper_summary"] = summary
            except Exception as e:
                out["debug_parse_error"] = str(e)
        else:
            out["note"] = "No debug file — API call likely failed before writing (network/HTTP error)"
        self._json(out)

    def _json(self, data):
        body = json.dumps(data, default=str).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def _html(self):
        if not INDEX_HTML.exists():
            self.send_error(404, "index.html not found")
            return
        body = INDEX_HTML.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _serve_mobile(self):
        """Mobile PWA — decisions-only companion view."""
        html = r"""<!DOCTYPE html>
<html lang="en"><head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
<meta name="apple-mobile-web-app-title" content="Horizon">
<meta name="theme-color" content="#0a1628">
<link rel="apple-touch-icon" href="/mobile-icon?v=3">
<title>Horizon - Port Intelligence</title>
<style>
*{box-sizing:border-box;margin:0;padding:0;-webkit-tap-highlight-color:transparent}
:root{--bg:#0a1628;--card:#0f1f35;--card2:#132238;--acc:#00b4c8;--txt:#c8d8e8;--bright:#e8f4ff;--dim:#6b82a8;--red:#ef4444;--amber:#f59e0b;--green:#22c55e}
html,body{height:100%;background:var(--bg);color:var(--txt);font-family:'Segoe UI',system-ui,-apple-system,sans-serif;overscroll-behavior:none}
.hdr{position:sticky;top:0;z-index:100;background:var(--bg);border-bottom:1px solid rgba(0,180,200,.15);padding:max(12px,env(safe-area-inset-top)) 16px 10px}
.status-line{display:flex;align-items:center;gap:7px;margin-bottom:10px;padding:7px 11px;border-radius:8px;background:rgba(19,34,56,.9);border:1px solid rgba(0,180,200,.12)}
.status-dot{width:7px;height:7px;border-radius:50%;flex-shrink:0}
.status-dot.ok{background:var(--green);box-shadow:0 0 6px rgba(34,197,94,.6)}
.status-dot.warn{background:var(--amber);box-shadow:0 0 6px rgba(245,158,11,.6)}
.status-dot.crit{background:var(--red);box-shadow:0 0 6px rgba(239,68,68,.6);animation:pulse 1.5s infinite}
.status-txt{font-size:11px;font-weight:700;color:var(--bright);letter-spacing:.3px;flex:1}
.status-risk{font-size:11px;font-weight:700;color:var(--amber)}
.hdr-top{display:flex;align-items:center;gap:8px;margin-bottom:8px}
.hdr-logo{height:30px;width:auto}
.hdr-title{font-size:13px;font-weight:700;color:var(--acc);letter-spacing:.5px;flex:1}
.sig-badge{font-size:11px;font-weight:700;background:var(--red);color:#fff;border-radius:10px;padding:3px 8px;min-width:26px;text-align:center}
.sig-badge.zero{background:rgba(34,197,94,.25);color:var(--green)}
.port-row{display:flex;align-items:center;gap:8px;margin-bottom:8px}
.port-sel{background:#132238;border:1px solid rgba(0,180,200,.3);border-radius:8px;color:var(--bright);font-size:13px;font-weight:600;padding:6px 10px;flex:1;-webkit-appearance:none}
.cond{font-size:10px;font-weight:700;letter-spacing:.8px;padding:4px 8px;border-radius:6px;background:rgba(34,197,94,.15);color:var(--green);border:1px solid rgba(34,197,94,.3);white-space:nowrap}
.cond.poor{background:rgba(239,68,68,.15);color:var(--red);border-color:rgba(239,68,68,.3)}
.cond.moderate{background:rgba(245,158,11,.15);color:var(--amber);border-color:rgba(245,158,11,.3)}
.wx-row{display:flex;align-items:center;gap:10px;margin-bottom:8px;flex-wrap:wrap}
.wx-chip{font-size:10px;color:var(--dim);display:flex;align-items:center;gap:3px}
.wx-chip b{color:var(--txt);font-weight:600}
.wx-sep{color:rgba(107,130,168,.3);font-size:10px}
.stats-row{display:grid;grid-template-columns:1fr 1fr 1fr;gap:6px;margin-bottom:2px}
.stat{background:rgba(19,34,56,.8);border:1px solid rgba(0,180,200,.1);border-radius:8px;padding:6px 8px;text-align:center}
.stat-val{font-size:14px;font-weight:700;color:var(--acc)}
.stat-lbl{font-size:9px;color:var(--dim);text-transform:uppercase;letter-spacing:.4px;margin-top:1px}
.content{padding:12px 16px;padding-bottom:calc(60px + env(safe-area-inset-bottom))}
.card{background:var(--card);border:1px solid rgba(0,180,200,.12);border-radius:14px;padding:16px;margin-bottom:12px;cursor:pointer;transition:border-color .15s}
.card.crit{border-color:rgba(239,68,68,.45);background:linear-gradient(135deg,#0f1f35,#180f1a)}
.card.high{border-color:rgba(245,158,11,.4)}
.badges{display:flex;gap:5px;flex-wrap:wrap;margin-bottom:10px}
.b{font-size:9px;font-weight:700;letter-spacing:.8px;text-transform:uppercase;padding:3px 7px;border-radius:4px}
.b-crit{background:rgba(239,68,68,.2);color:var(--red);border:1px solid rgba(239,68,68,.4)}
.b-high{background:rgba(245,158,11,.2);color:var(--amber);border:1px solid rgba(245,158,11,.4)}
.b-med{background:rgba(234,179,8,.15);color:#eab308;border:1px solid rgba(234,179,8,.35)}
.b-conf{background:rgba(239,68,68,.15);color:var(--red);border:1px solid rgba(239,68,68,.3)}
.b-sim{background:rgba(107,130,168,.15);color:var(--dim);border:1px solid rgba(107,130,168,.25)}
.vname{font-size:19px;font-weight:700;color:var(--bright);margin-bottom:4px}
.cdesc{font-size:12px;color:var(--txt);margin-bottom:12px;line-height:1.5}
.rec{background:rgba(0,180,200,.08);border:1px solid rgba(0,180,200,.2);border-radius:8px;padding:10px 12px;margin-bottom:12px}
.rec-lbl{font-size:10px;font-weight:700;color:var(--acc);letter-spacing:.6px;text-transform:uppercase;margin-bottom:4px}
.rec-txt{font-size:12px;color:var(--bright);font-weight:600;line-height:1.4}
.rec-sub{font-size:11px;color:var(--dim);margin-top:3px;line-height:1.4}
.metrics{display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px;margin-bottom:12px}
.met{background:var(--card2);border-radius:8px;padding:8px;text-align:center}
.met-val{font-size:14px;font-weight:700;color:var(--amber)}
.met-val.g{color:var(--green)}
.met-lbl{font-size:9px;color:var(--dim);text-transform:uppercase;letter-spacing:.5px;margin-top:2px}
.timer-row{display:flex;align-items:center;justify-content:space-between;background:rgba(239,68,68,.08);border:1px solid rgba(239,68,68,.2);border-radius:8px;padding:8px 12px;margin-bottom:10px}
.timer-lbl{font-size:10px;color:var(--dim);text-transform:uppercase;letter-spacing:.6px}
.timer{font-size:16px;font-weight:700;color:var(--red);font-variant-numeric:tabular-nums}
.timer.warn{color:var(--amber)}
.expand-btn{width:100%;background:none;border:1px solid rgba(0,180,200,.15);border-radius:8px;padding:7px;color:var(--dim);font-size:11px;cursor:pointer;text-align:center;letter-spacing:.3px}
.expand-btn:active{background:rgba(0,180,200,.08)}
.alts{display:none;margin-top:10px;border-top:1px solid rgba(0,180,200,.1);padding-top:10px}
.alts.open{display:block}
.alt{background:var(--card2);border-radius:8px;padding:10px 12px;margin-bottom:8px;border:1px solid rgba(107,130,168,.15)}
.alt.recommended{border-color:rgba(0,180,200,.3);background:rgba(0,180,200,.06)}
.alt-lbl{font-size:10px;font-weight:700;color:var(--acc);text-transform:uppercase;letter-spacing:.5px;margin-bottom:3px}
.alt-title{font-size:12px;font-weight:600;color:var(--bright);margin-bottom:3px}
.alt-desc{font-size:11px;color:var(--dim);line-height:1.4;margin-bottom:6px}
.alt-meta{display:flex;gap:12px}
.alt-m{font-size:10px;color:var(--dim)}
.alt-m b{color:var(--txt)}
.open-btn{display:block;width:100%;background:rgba(0,180,200,.1);border:1px solid rgba(0,180,200,.25);border-radius:10px;padding:12px;text-align:center;color:var(--acc);font-size:13px;font-weight:600;text-decoration:none;margin-top:4px}
.restrict{background:rgba(239,68,68,.1);border:1px solid rgba(239,68,68,.35);border-radius:8px;padding:8px 12px;margin-top:8px;display:flex;align-items:flex-start;gap:8px}
.restrict.warn{background:rgba(245,158,11,.1);border-color:rgba(245,158,11,.35)}
.restrict-icon{font-size:13px;flex-shrink:0;margin-top:1px}
.restrict-txt{font-size:11px;color:var(--bright);font-weight:600;flex:1;line-height:1.4}
.section-hdr{font-size:9px;font-weight:700;letter-spacing:.8px;text-transform:uppercase;color:var(--dim);margin-bottom:8px}
.mov{background:var(--card);border:1px solid rgba(0,180,200,.1);border-radius:10px;padding:10px 12px;margin-bottom:8px}
.mov-top{display:flex;align-items:center;gap:8px}
.mov-dir{font-size:9px;font-weight:700;letter-spacing:.5px;padding:2px 6px;border-radius:4px;text-transform:uppercase;flex-shrink:0}
.mov-in{background:rgba(59,130,246,.2);color:#60a5fa}
.mov-out{background:rgba(34,197,94,.15);color:var(--green)}
.mov-name{font-size:13px;font-weight:700;color:var(--bright);flex:1;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;min-width:0}
.mov-time{font-size:13px;font-weight:700;color:var(--acc);font-variant-numeric:tabular-nums;flex-shrink:0}
.mov-meta{display:flex;gap:5px;margin-top:6px;flex-wrap:wrap}
.mov-tag{font-size:9px;font-weight:700;padding:2px 6px;border-radius:4px;letter-spacing:.3px}
.mov-tag.ok{background:rgba(34,197,94,.15);color:var(--green)}
.mov-tag.pend{background:rgba(245,158,11,.15);color:var(--amber)}
.mov-tag.berth{background:rgba(0,180,200,.1);color:var(--acc)}
.mov-section{margin-bottom:16px}
.attn-section{margin-bottom:16px}
.attn{background:var(--card);border:1px solid rgba(245,158,11,.25);border-radius:10px;padding:10px 12px;margin-bottom:8px;display:flex;align-items:flex-start;gap:10px}
.attn.crit{border-color:rgba(239,68,68,.35);background:linear-gradient(135deg,#0f1f35,#180f1a)}
.attn-icon{font-size:14px;flex-shrink:0;margin-top:1px}
.attn-body{flex:1;min-width:0}
.attn-msg{font-size:12px;font-weight:700;color:var(--bright);line-height:1.4;margin-bottom:3px}
.attn-detail{font-size:11px;color:var(--dim);line-height:1.4;overflow:hidden;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical}
.attn-detail.expanded{display:block;-webkit-line-clamp:unset}
.attn-deadline{font-size:10px;font-weight:700;color:var(--amber);margin-top:4px;font-variant-numeric:tabular-nums}
.empty{text-align:center;padding:40px 20px 20px;color:var(--dim)}
.empty-icon{font-size:48px;margin-bottom:12px}
.empty-title{font-size:17px;font-weight:600;color:var(--txt);margin-bottom:6px}
.empty-sub{font-size:13px;line-height:1.6;margin-bottom:20px}
.empty-wx{background:var(--card);border:1px solid rgba(0,180,200,.1);border-radius:12px;padding:14px 16px;text-align:left;margin:0 auto;max-width:340px}
.empty-wx-title{font-size:10px;font-weight:700;color:var(--acc);letter-spacing:.6px;text-transform:uppercase;margin-bottom:10px}
.empty-wx-row{display:flex;justify-content:space-between;padding:5px 0;border-bottom:1px solid rgba(0,180,200,.06);font-size:12px}
.empty-wx-row:last-child{border-bottom:none}
.empty-wx-lbl{color:var(--dim)}
.empty-wx-val{color:var(--bright);font-weight:600}
.pill{position:fixed;bottom:calc(16px + env(safe-area-inset-bottom));right:16px;background:var(--card);border:1px solid rgba(0,180,200,.2);border-radius:20px;padding:8px 14px;font-size:11px;color:var(--dim);display:flex;align-items:center;gap:6px;box-shadow:0 4px 20px rgba(0,0,0,.5);cursor:pointer;z-index:200}
.brief-btn{display:inline-flex;align-items:center;gap:4px;font-size:10px;font-weight:700;padding:3px 9px;border-radius:10px;background:rgba(0,180,200,.1);border:1px solid rgba(0,180,200,.25);color:var(--acc);text-decoration:none;letter-spacing:.3px}
/* Pull-to-refresh rubber band */
#ptr-zone{height:0;overflow:hidden;display:flex;align-items:flex-end;justify-content:center;background:var(--bg)}
#ptr-inner{display:flex;align-items:center;gap:8px;font-size:12px;font-weight:600;color:var(--acc);padding-bottom:12px}
#ptr-icon{font-size:15px;line-height:1;display:inline-block;transition:transform .2s ease}
#ptr-icon.ready{transform:rotate(180deg)}
#ptr-icon.spinning{font-size:0;width:16px;height:16px;border:2px solid rgba(0,180,200,.3);border-top-color:var(--acc);border-radius:50%;animation:spin .7s linear infinite;vertical-align:middle}
@keyframes spin{to{transform:rotate(360deg)}}
/* Swipe transition */
.content-wrap{transition:transform .25s ease,opacity .25s ease}
.content-wrap.swipe-left{transform:translateX(-40px);opacity:0}
.content-wrap.swipe-right{transform:translateX(40px);opacity:0}
.dot{width:6px;height:6px;border-radius:50%;background:var(--acc);animation:pulse 2s infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.3}}
</style></head>
<body>
<div class="hdr">
  <div class="hdr-top">
    <img src="/logo" class="hdr-logo" alt="Horizon">
    <span class="hdr-title">PORT BRIEF</span>
    <a href="/api/port-brief" class="brief-btn">📄 Brief</a>
    <span class="sig-badge" id="hs">–</span>
  </div>
  <div class="port-row">
    <select class="port-sel" id="ps" onchange="switchPort(this.value)">
      <option value="BRISBANE">Brisbane</option>
      <option value="DARWIN">Darwin</option>
      <option value="MELBOURNE">Melbourne</option>
    </select>
  </div>
  <div class="status-line" id="status-line">
    <div class="status-dot ok" id="status-dot"></div>
    <span class="status-txt" id="status-txt">Loading…</span>
    <span class="status-risk" id="status-risk"></span>
  </div>
  <div class="wx-row" id="wx-row">
    <span class="wx-chip"><b id="hc">–</b></span>
    <span class="wx-sep">·</span>
    <span class="wx-chip"><b id="wx-wind">–</b></span>
    <span class="wx-sep">·</span>
    <span class="wx-chip"><b id="wx-swell">–</b></span>
    <span class="wx-sep">·</span>
    <span class="wx-chip"><b id="wx-tide">–</b></span>
    <span class="wx-sep">·</span>
    <span class="wx-chip"><b id="wx-vis">–</b></span>
  </div>
  <div class="stats-row">
    <div class="stat"><div class="stat-val" id="st-vessels">–</div><div class="stat-lbl">In Port</div></div>
    <div class="stat"><div class="stat-val" id="st-berths">–</div><div class="stat-lbl">Berths</div></div>
    <div class="stat"><div class="stat-val" id="st-arr">–</div><div class="stat-lbl">Arriving 24h</div></div>
  </div>
  <div id="restrict-row"></div>
</div>
<div id="ptr-zone"><div id="ptr-inner"><span id="ptr-icon">↓</span><span id="ptr-lbl">Pull to refresh</span></div></div>
<div class="content-wrap" id="cwrap"><div class="content" id="ct"><p style="text-align:center;padding:40px;color:var(--dim)">Loading…</p></div></div>
<div class="pill" onclick="doRefresh()"><div class="dot"></div><span id="rl">Live</span></div>
<script>
let _d=null,_cd=null,_dl={};
function esc(s){return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');}
function fmtTimer(secs){if(secs<=0)return'00:00:00';const h=Math.floor(secs/3600),m=Math.floor((secs%3600)/60),s=secs%60;return`${String(h).padStart(2,'0')}:${String(m).padStart(2,'0')}:${String(s).padStart(2,'0')}`;}
function toggleAlts(id){const el=document.getElementById('alts-'+id);const btn=document.getElementById('altbtn-'+id);if(!el)return;const open=el.classList.toggle('open');btn.textContent=open?'▲ Hide options':'▼ View all options';}
function toggleAttn(id){const card=document.getElementById('attn-'+id);if(!card)return;const det=card.querySelector('.attn-detail');const arr=card.querySelector('.attn-arr');if(!det)return;const exp=det.classList.toggle('expanded');if(arr)arr.textContent=exp?'▲':'▼';}
function render(d){
  const cs=(d.conflicts||[]).filter(c=>c.signal_type==='CONFLICT'&&c.decision_support);
  const sb=document.getElementById('hs');sb.textContent=cs.length;sb.className='sig-badge'+(cs.length===0?' zero':'');
  // Weather context
  const wx=d.weather||{};
  const cond=(wx.conditions||'').toUpperCase()||'–';
  const ce=document.getElementById('hc');
  ce.textContent=cond;
  const windKts=wx.wind_speed_kts!=null?Math.round(wx.wind_speed_kts):null;
  const windDir=wx.wind_direction_label||'';
  document.getElementById('wx-wind').textContent=windKts!=null?`WIND ${windKts}kt${windDir?' '+windDir:''}`:' WIND –';
  const swellM=wx.swell_height_m!=null?wx.swell_height_m.toFixed(1):null;
  const swellDir=wx.swell_direction_label||'';
  document.getElementById('wx-swell').textContent=swellM!=null?`SWELL ${swellM}m${swellDir?' '+swellDir:''}`:' SWELL –';
  const tide=d.tides||{};
  const tideArrow=tide.state==='rising'?'↑':tide.state==='falling'?'↓':'→';
  const tideH=tide.current_height_m!=null?tide.current_height_m.toFixed(2):null;
  document.getElementById('wx-tide').textContent=tideH!=null?`TIDE ${tideArrow} ${tideH}m`:'TIDE –';
  const visNm=wx.visibility_nm!=null?Math.round(wx.visibility_nm):null;
  document.getElementById('wx-vis').textContent=visNm!=null?`VIS ${visNm}nm`:'VIS –';
  // Port stats
  const ps=d.port_status||{};
  document.getElementById('st-vessels').textContent=ps.vessels_in_port??'–';
  document.getElementById('st-berths').textContent=ps.berths_occupied!=null?`${ps.berths_occupied}/${ps.berths_total}`:'–';
  document.getElementById('st-arr').textContent=ps.vessels_expected_24h??'–';
  document.getElementById('ps').value=(d.port_profile&&d.port_profile.id)||'BRISBANE';
  // Port status summary line
  const allConflicts=d.conflicts||[];
  const critCount=allConflicts.filter(c=>c.severity==='critical').length;
  const decCount=cs.length;
  const dot=document.getElementById('status-dot');
  const txt=document.getElementById('status-txt');
  const risk=document.getElementById('status-risk');
  if(critCount>0){
    dot.className='status-dot crit';
    txt.textContent=`${decCount} DECISION${decCount!==1?'S':''} PENDING`;
  } else if(decCount>0){
    dot.className='status-dot warn';
    txt.textContent=`${decCount} DECISION${decCount!==1?'S':''} PENDING`;
  } else {
    dot.className='status-dot ok';
    txt.textContent=`PORT OPERATIONS NORMAL · ${(d.port_status||{}).vessels_in_port??'–'} VESSELS`;
  }
  // Cost at risk from recommended options
  const costAtRisk=allConflicts.filter(c=>c.decision_support).reduce((sum,c)=>{
    const rec=((c.decision_support||{}).options||[]).find(o=>o.recommended)||((c.decision_support||{}).options||[])[0];
    if(!rec||!rec.cost_label)return sum;
    const m=rec.cost_label.replace(/[^0-9]/g,'');
    return sum+(m?parseInt(m):0);
  },0);
  risk.textContent=costAtRisk>0?`$${costAtRisk.toLocaleString()} AT RISK`:'';
  // Active port restrictions (WEATHER conflicts)
  const wxCs=(d.conflicts||[]).filter(c=>c.signal_type==='WEATHER');
  const rr=document.getElementById('restrict-row');
  if(wxCs.length){
    const sevO={'critical':0,'high':1,'medium':2};
    const top=[...wxCs].sort((a,b)=>(sevO[a.severity]||9)-(sevO[b.severity]||9))[0];
    const isCrit=top.severity==='critical';
    const shortDesc=(top.description||'').split('.')[0].slice(0,120);
    rr.innerHTML=`<div class="restrict${isCrit?'':' warn'}"><span class="restrict-icon">${isCrit?'🚫':'⚠️'}</span><div class="restrict-txt">${esc(shortDesc)}</div></div>`;
  }else{rr.innerHTML='';}
  // Next movements (arrivals + departures within 8h)
  const _now=new Date();
  const movs=[];
  (d.vessels||[]).forEach(v=>{
    if(['scheduled','confirmed','at_risk'].includes(v.status)&&v.eta){
      const t=new Date(v.eta);const hrs=(t-_now)/3600000;
      if(hrs>-0.5&&hrs<8)movs.push({dir:'ARR',name:v.name,time:t,berth:v.berth_id,pilot:v.pilotage_required,tug:v.towage_required});
    }
    if(v.status==='berthed'&&v.etd){
      const t=new Date(v.etd);const hrs=(t-_now)/3600000;
      if(hrs>-0.5&&hrs<6)movs.push({dir:'DEP',name:v.name,time:t,berth:v.berth_id,pilot:v.pilotage_required,tug:v.towage_required});
    }
  });
  movs.sort((a,b)=>a.time-b.time);
  // For Your Attention — action_required guidance items only
  const attnItems=(d.guidance||[]).filter(g=>g.action_required).slice(0,3);
  const attnHtml=attnItems.length?`<div class="attn-section"><div class="section-hdr">For Your Attention</div>${attnItems.map(g=>{
    const isCrit=g.priority==='critical';
    const dl=g.deadline?new Date(g.deadline):null;
    const dlStr=dl?dl.toLocaleTimeString('en-AU',{hour:'2-digit',minute:'2-digit',hour12:false}):null;
    return`<div class="attn${isCrit?' crit':''}" id="attn-${esc(g.id)}" onclick="toggleAttn('${esc(g.id)}')">
<span class="attn-icon">${isCrit?'🔴':'⚡'}</span>
<div class="attn-body">
<div style="display:flex;align-items:flex-start;justify-content:space-between;gap:6px">
<div class="attn-msg">${esc(g.message)}</div>
<span class="attn-arr" style="font-size:9px;color:var(--dim);flex-shrink:0;margin-top:2px">▼</span>
</div>
<div class="attn-detail">${esc(g.detail||'')}</div>
${dlStr?`<div class="attn-deadline">Act by ${dlStr}</div>`:''}
</div></div>`;
  }).join('')}</div>`:'';
  const movsHtml=movs.slice(0,4).length?`<div class="mov-section"><div class="section-hdr">Next Movements</div>${movs.slice(0,4).map(m=>{
    const tStr=m.time.toLocaleTimeString('en-AU',{hour:'2-digit',minute:'2-digit',hour12:false});
    const tags=(m.berth?`<span class="mov-tag berth">${esc(m.berth)}</span>`:'')+
               (m.pilot?`<span class="mov-tag pend">PILOT</span>`:'')+
               (m.tug?`<span class="mov-tag pend">TUG</span>`:'');
    return`<div class="mov"><div class="mov-top"><span class="mov-dir ${m.dir==='ARR'?'mov-in':'mov-out'}">${m.dir}</span><span class="mov-name">${esc(m.name)}</span><span class="mov-time">${tStr}</span></div>${tags?`<div class="mov-meta">${tags}</div>`:''}</div>`;
  }).join('')}</div>`:'';
  const ct=document.getElementById('ct');
  if(!cs.length){
    ct.innerHTML=movsHtml+attnHtml+`<div class="empty">
<div class="empty-icon">✓</div>
<div class="empty-title">All Clear</div>
<div class="empty-sub">No active decisions required.<br>Port operations running normally.</div>
<div class="empty-wx">
<div class="empty-wx-title">Port Conditions</div>
<div class="empty-wx-row"><span class="empty-wx-lbl">Wind</span><span class="empty-wx-val">${windKts!=null?windKts+'kt '+(windDir||''):'–'}</span></div>
<div class="empty-wx-row"><span class="empty-wx-lbl">Swell</span><span class="empty-wx-val">${swellM!=null?swellM+'m '+(swellDir||''):'–'}</span></div>
<div class="empty-wx-row"><span class="empty-wx-lbl">Tide</span><span class="empty-wx-val">${tideH!=null?tideArrow+' '+tideH+'m ('+(tide.state||'–')+')':'–'}</span></div>
<div class="empty-wx-row"><span class="empty-wx-lbl">Visibility</span><span class="empty-wx-val">${visNm!=null?visNm+' nm':'–'}</span></div>
<div class="empty-wx-row"><span class="empty-wx-lbl">Vessels in port</span><span class="empty-wx-val">${ps.vessels_in_port??'–'}</span></div>
<div class="empty-wx-row"><span class="empty-wx-lbl">Berths occupied</span><span class="empty-wx-val">${ps.berths_occupied!=null?ps.berths_occupied+'/'+ps.berths_total:'–'}</span></div>
</div>
<br><a href="/?full=1" class="open-btn">Open Full Platform →</a>
</div>`;
    return;
  }
  _dl={};
  const ro={'critical':0,'high':1,'medium':2,'low':3};
  const sorted=[...cs].sort((a,b)=>(ro[a.severity]||9)-(ro[b.severity]||9));
  sorted.forEach(c=>{
    const ds=c.decision_support||{};
    _dl[c.id]=ds.decision_deadline?new Date(ds.decision_deadline).getTime():Date.now()+3*3600*1000;
  });
  ct.innerHTML=movsHtml+attnHtml+`<div class="section-hdr" style="margin-top:4px">Decisions</div>`+sorted.map(c=>{
    const sev=c.severity||'medium';
    const ic=sev==='critical',ih=sev==='high';
    const rb=ic?'<span class="b b-crit">CRITICAL</span>':ih?'<span class="b b-high">HIGH RISK</span>':'<span class="b b-med">MED RISK</span>';
    const vname=(c.vessel_names||[])[0]||'Unknown';
    const ds=c.decision_support||{};
    const opts=ds.options||[];
    const rec=opts.find(o=>o.recommended)||opts[0]||{};
    const rh=rec.label?`<div class="rec"><div class="rec-lbl">★ Recommended Action</div><div class="rec-txt">${esc(rec.label)}</div>${rec.description?`<div class="rec-sub">${esc(rec.description.slice(0,140))}${rec.description.length>140?'…':''}</div>`:''}</div>`:'';
    const cost=rec.cost_label||'~$3,800';
    const delay=rec.delay_mins!=null?(rec.delay_mins>0?rec.delay_mins+' min':'0 min'):'90 min';
    const casc=rec.cascade_count!=null?(rec.cascade_count+' vessel'+(rec.cascade_count!==1?'s':'')):'1 vessel';
    // All alternatives (tap to expand)
    const altsHtml=opts.length>1?opts.map(o=>`
<div class="alt${o.recommended?' recommended':''}">
${o.recommended?'<div class="alt-lbl">★ Recommended</div>':''}
<div class="alt-title">${esc(o.label||'')}</div>
<div class="alt-desc">${esc((o.description||'').slice(0,160))}${(o.description||'').length>160?'…':''}</div>
<div class="alt-meta">
<span class="alt-m"><b>${esc(o.cost_label||'–')}</b></span>
<span class="alt-m">Delay: <b>${o.delay_mins!=null?(o.delay_mins>0?o.delay_mins+' min':'None'):'–'}</b></span>
<span class="alt-m">Cascade: <b>${o.cascade_count!=null?o.cascade_count:'–'}</b></span>
</div></div>`).join(''):'';
    const expandBtn=opts.length>1?`<button class="expand-btn" id="altbtn-${esc(c.id)}" onclick="toggleAlts('${esc(c.id)}')">▼ View all options</button><div class="alts" id="alts-${esc(c.id)}">${altsHtml}</div>`:'';
    return`<div class="card${ic?' crit':ih?' high':''}" id="card-${esc(c.id)}">
<div class="badges">${rb}<span class="b b-conf">CONFLICT</span><span class="b b-sim">SIMULATION</span></div>
<div class="vname">${esc(vname)}</div>
<div class="cdesc">${esc(c.description||c.conflict_type||'')}</div>
${rh}
<div class="metrics">
<div class="met"><div class="met-val">${esc(cost)}</div><div class="met-lbl">Cost</div></div>
<div class="met"><div class="met-val">${esc(delay)}</div><div class="met-lbl">Delay</div></div>
<div class="met"><div class="met-val g">${esc(casc)}</div><div class="met-lbl">Cascade</div></div>
</div>
<div class="timer-row"><span class="timer-lbl">⏱ Decide within</span><span class="timer" id="t-${esc(c.id)}">–:––:––</span></div>
${expandBtn}
</div>`;
  }).join('')+`<a href="/?full=1" class="open-btn">Open Full Platform →</a>`;
  startCd();
}
function startCd(){
  if(_cd)clearInterval(_cd);
  _cd=setInterval(()=>{
    const now=Date.now();
    Object.entries(_dl).forEach(([id,dl])=>{
      const el=document.getElementById(`t-${id}`);if(!el)return;
      const s=Math.max(0,Math.round((dl-now)/1000));
      el.textContent=fmtTimer(s);el.className='timer'+(s<3600?' warn':'');
    });
  },1000);
}
async function doRefresh(){
  document.getElementById('rl').textContent='Refreshing…';
  try{const r=await fetch('/api/summary');_d=await r.json();render(_d);document.getElementById('rl').textContent='Live';}
  catch(e){document.getElementById('rl').textContent='Offline';}
  return Promise.resolve();
}
async function switchPort(p){
  try{
    const r=await fetch('/api/set_port',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({port:p})});
    const res=await r.json();
    if(res.success) await doRefresh(); else doRefresh();
  }catch(e){doRefresh();}
}
doRefresh();setInterval(doRefresh,30000);

// ── Pull-to-refresh rubber band ───────────────────────────────────────────────
(function(){
  const THRESHOLD=60, MAX_H=90, HOLD_H=52;
  let startY=0,pulling=false,triggered=false,active=false;
  const zone=document.getElementById('ptr-zone');
  const icon=document.getElementById('ptr-icon');
  const lbl=document.getElementById('ptr-lbl');
  const cwrap=document.getElementById('cwrap');

  // Rubber-band damping: full 1:1 up to 20px, then sqrt taper
  function dampen(dy){return dy<=20?dy:20+Math.sqrt(dy-20)*5.5;}

  function setH(h,animate){
    const easing='cubic-bezier(0.25,0.46,0.45,0.94)';
    zone.style.transition=animate?`height 0.3s ${easing}`:'none';
    cwrap.style.transition=animate?`transform 0.3s ${easing}`:'none';
    zone.style.height=h+'px';
    cwrap.style.transform=h?`translateY(${h}px)`:'';
  }

  function reset(animate){
    setH(0,animate);
    setTimeout(()=>{icon.className='';icon.textContent='↓';lbl.textContent='Pull to refresh';},animate?310:0);
    pulling=false;triggered=false;active=false;
  }

  document.addEventListener('touchstart',e=>{
    if(window.scrollY>2)return;
    startY=e.touches[0].clientY;pulling=true;triggered=false;active=false;
  },{passive:true});

  document.addEventListener('touchmove',e=>{
    if(!pulling)return;
    const dy=e.touches[0].clientY-startY;
    if(dy<=0){if(active)reset(true);else pulling=false;return;}
    active=true;
    const h=Math.min(dampen(dy),MAX_H);
    setH(h,false);
    if(h>=THRESHOLD&&!triggered){
      triggered=true;
      icon.className='ready';
      lbl.textContent='Release to refresh';
    }else if(h<THRESHOLD&&triggered){
      triggered=false;
      icon.className='';
      icon.textContent='↓';
      lbl.textContent='Pull to refresh';
    }
  },{passive:true});

  document.addEventListener('touchend',()=>{
    if(!pulling)return;
    if(triggered){
      icon.className='spinning';
      lbl.textContent='Refreshing…';
      setH(HOLD_H,true);
      doRefresh().finally(()=>setTimeout(()=>reset(true),400));
    }else{
      reset(active);
    }
  });
})();

// ── Swipe between ports ───────────────────────────────────────────────────────
(function(){
  const PORTS=['BRISBANE','MELBOURNE','DARWIN'];
  let sx=0,sy=0;
  const cwrap=document.getElementById('cwrap');
  document.addEventListener('touchstart',e=>{sx=e.touches[0].clientX;sy=e.touches[0].clientY;},{passive:true});
  document.addEventListener('touchend',e=>{
    const dx=e.changedTouches[0].clientX-sx;
    const dy=e.changedTouches[0].clientY-sy;
    if(Math.abs(dx)<50||Math.abs(dx)<Math.abs(dy)*1.5)return; // not a horizontal swipe
    const cur=document.getElementById('ps').value||'BRISBANE';
    const idx=PORTS.indexOf(cur);
    let next;
    if(dx<0&&idx<PORTS.length-1)next=PORTS[idx+1];  // swipe left → next port
    else if(dx>0&&idx>0)next=PORTS[idx-1];           // swipe right → prev port
    if(!next)return;
    const dir=dx<0?'swipe-left':'swipe-right';
    cwrap.classList.add(dir);
    setTimeout(()=>{
      cwrap.classList.remove(dir);
      cwrap.style.transform='';cwrap.style.opacity='';
      switchPort(next);
    },230);
  });
})();
</script>
</body></html>"""
        body = html.encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _build_brief_pdf(self):
        """Generate Port Brief PDF. Returns (pdf_bytes, filename, port_name, date_str)."""
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.units import mm
        from reportlab.lib.colors import HexColor, white, black
        from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable
        from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
        from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT
        import io

        data = build_summary()
        profile  = data.get("port_profile", {})
        ps       = data.get("port_status", {})
        wx       = data.get("weather", {})
        tides    = data.get("tides", {})
        conflicts = data.get("conflicts", [])
        guidance  = data.get("guidance", [])
        vessels   = data.get("vessels", [])
        dash      = data.get("dashboard", {}) or {}

        port_name = profile.get("display_name", "Port")
        now_utc   = utcnow()
        date_str  = now_utc.strftime("%d %B %Y  %H:%M UTC")

        # Colour palette
        C_BG     = HexColor("#0b1120")
        C_HDR    = HexColor("#131e30")
        C_ACC    = HexColor("#38bdf8")
        C_GREEN  = HexColor("#22c55e")
        C_AMBER  = HexColor("#f59e0b")
        C_RED    = HexColor("#ef4444")
        C_DIM    = HexColor("#6b82a8")
        C_TEXT   = HexColor("#cdd9f0")
        C_BRIGHT = HexColor("#e8f0ff")
        C_SURF   = HexColor("#1a2840")

        # Determine status
        crit_count = sum(1 for c in conflicts if c.get("severity") == "critical")
        dec_count  = sum(1 for c in conflicts if c.get("signal_type") == "CONFLICT" and c.get("decision_support"))
        total_sig  = sum(1 for c in conflicts if c.get("signal_type") == "CONFLICT")
        if crit_count > 0:
            status_col = C_RED
            status_txt = f"{crit_count} CRITICAL CONFLICT{'S' if crit_count!=1 else ''} ACTIVE  ·  {dec_count} DECISION{'S' if dec_count!=1 else ''} PENDING"
        elif dec_count > 0:
            status_col = C_AMBER
            status_txt = f"{dec_count} DECISION{'S' if dec_count!=1 else ''} PENDING  ·  {total_sig} ACTIVE SIGNAL{'S' if total_sig!=1 else ''}"
        else:
            vip = ps.get("vessels_in_port", "–")
            util = dash.get("berth_utilisation_pct", "–")
            status_txt = f"OPERATIONS NORMAL  ·  {vip} VESSELS IN PORT  ·  {util}% BERTHS OCCUPIED"
            status_col = C_GREEN

        # Cost at risk
        cost_at_risk = 0
        for c in conflicts:
            if not c.get("decision_support"): continue
            opts = (c["decision_support"] or {}).get("options", [])
            rec = next((o for o in opts if o.get("recommended")), opts[0] if opts else None)
            if rec and rec.get("cost_label"):
                m = "".join(ch for ch in rec["cost_label"] if ch.isdigit())
                if m: cost_at_risk += int(m)

        buf = io.BytesIO()
        doc = SimpleDocTemplate(buf, pagesize=A4,
                                leftMargin=14*mm, rightMargin=14*mm,
                                topMargin=12*mm, bottomMargin=12*mm)

        W = A4[0] - 28*mm  # usable width

        styles = getSampleStyleSheet()
        def sty(name, **kw):
            s = ParagraphStyle(name, **kw)
            return s

        # ── Styles ──────────────────────────────────────────────────────
        s_label = sty("label", fontName="Helvetica-Bold", fontSize=7,
                      textColor=C_DIM, leading=10, spaceAfter=1)
        s_val   = sty("val",   fontName="Helvetica-Bold", fontSize=11,
                      textColor=C_BRIGHT, leading=13)
        s_sub   = sty("sub",   fontName="Helvetica",      fontSize=8,
                      textColor=C_DIM,    leading=11)
        s_sec   = sty("sec",   fontName="Helvetica-Bold", fontSize=8,
                      textColor=C_ACC,    leading=12, spaceBefore=8)
        s_body  = sty("body",  fontName="Helvetica",      fontSize=9,
                      textColor=C_TEXT,   leading=12)
        s_bold  = sty("bold",  fontName="Helvetica-Bold", fontSize=9,
                      textColor=C_BRIGHT, leading=12)
        s_small = sty("small", fontName="Helvetica",      fontSize=7.5,
                      textColor=C_DIM,    leading=11)
        s_hdr   = sty("hdr",   fontName="Helvetica-Bold", fontSize=20,
                      textColor=white,    leading=24)
        s_sub_hdr = sty("subhdr", fontName="Helvetica", fontSize=9,
                         textColor=C_ACC, leading=12)
        s_status  = sty("status", fontName="Helvetica-Bold", fontSize=8.5,
                         textColor=white, leading=12, alignment=TA_CENTER)
        s_footer  = sty("footer", fontName="Helvetica", fontSize=7,
                         textColor=C_DIM, leading=10, alignment=TA_CENTER)

        story = []

        # ── Header block ─────────────────────────────────────────────────
        hdr_data = [[
            Paragraph(f"PORT BRIEF", sty("hdr2", fontName="Helvetica-Bold", fontSize=22, textColor=white, leading=26)),
            Paragraph(f"{port_name.upper()}<br/><font size='9' color='#{C_ACC.hexval()[2:]}' face='Helvetica'>{date_str}</font>",
                      sty("hdrright", fontName="Helvetica-Bold", fontSize=14, textColor=white, leading=18, alignment=TA_RIGHT))
        ]]
        hdr_tbl = Table(hdr_data, colWidths=[W*0.55, W*0.45])
        hdr_tbl.setStyle(TableStyle([
            ("BACKGROUND", (0,0), (-1,-1), C_HDR),
            ("VALIGN",     (0,0), (-1,-1), "MIDDLE"),
            ("TOPPADDING", (0,0), (-1,-1), 10),
            ("BOTTOMPADDING",(0,0),(-1,-1),10),
            ("LEFTPADDING", (0,0),(0,-1), 12),
            ("RIGHTPADDING",(-1,0),(-1,-1),12),
            ("LINEABOVE",  (0,0),(-1,0), 3, C_ACC),
        ]))
        story.append(hdr_tbl)

        # ── Status banner ─────────────────────────────────────────────────
        risk_str = f"  ·  ${cost_at_risk:,} AT RISK" if cost_at_risk > 0 else ""
        stat_data = [[Paragraph(status_txt + risk_str, s_status)]]
        stat_tbl = Table(stat_data, colWidths=[W])
        stat_tbl.setStyle(TableStyle([
            ("BACKGROUND",    (0,0),(-1,-1), status_col),
            ("TOPPADDING",    (0,0),(-1,-1), 7),
            ("BOTTOMPADDING", (0,0),(-1,-1), 7),
        ]))
        story.append(stat_tbl)
        story.append(Spacer(1, 5*mm))

        # ── Snapshot row (Weather + Port Stats + Movements) ───────────────
        cond  = wx.get("conditions", "–").capitalize()
        wind  = f"{wx.get('wind_speed_kts','–')} kts {wx.get('wind_direction_label','')}"
        swell = f"{wx.get('swell_height_m','–')}m {wx.get('swell_direction_label','')}"
        vis   = f"{wx.get('visibility_nm','–')} nm"
        tide_arrow = "↑" if (tides.get("state")=="rising") else "↓" if (tides.get("state")=="falling") else "→"
        tide_h = tides.get("current_height_m")
        tide_str = f"{tide_arrow} {tide_h:.2f}m ({tides.get('state','–')})" if tide_h is not None else "–"

        vip    = ps.get("vessels_in_port", "–")
        berths = f"{ps.get('berths_occupied','–')}/{ps.get('berths_total','–')}"
        arr24  = ps.get("vessels_expected_24h", "–")
        util   = dash.get("berth_utilisation_pct", "–")
        otd    = dash.get("on_time_departure_pct", "–")

        # Next movements
        _now = utcnow()
        movs = []
        for v in vessels:
            if v.get("status") in ("scheduled","confirmed","at_risk") and v.get("eta"):
                try:
                    t = datetime.fromisoformat(v["eta"].replace("Z","+00:00"))
                    hrs = (t - _now).total_seconds() / 3600
                    if -0.5 < hrs < 8:
                        movs.append(("ARR", v.get("name",""), t, v.get("berth_id","")))
                except: pass
            if v.get("status") == "berthed" and v.get("etd"):
                try:
                    t = datetime.fromisoformat(v["etd"].replace("Z","+00:00"))
                    hrs = (t - _now).total_seconds() / 3600
                    if -0.5 < hrs < 6:
                        movs.append(("DEP", v.get("name",""), t, v.get("berth_id","")))
                except: pass
        movs.sort(key=lambda x: x[2])

        def wx_cell():
            rows = [
                [Paragraph("WEATHER", s_label), ""],
                [Paragraph("Conditions", s_small), Paragraph(f"<b>{cond}</b>", s_bold)],
                [Paragraph("Wind",       s_small), Paragraph(wind,  s_body)],
                [Paragraph("Swell",      s_small), Paragraph(swell, s_body)],
                [Paragraph("Visibility", s_small), Paragraph(vis,   s_body)],
                [Paragraph("Tide",       s_small), Paragraph(tide_str, s_body)],
            ]
            t = Table(rows, colWidths=[18*mm, None])
            t.setStyle(TableStyle([("SPAN",(0,0),(1,0)),
                ("BACKGROUND",(0,0),(-1,-1), C_SURF),
                ("TOPPADDING",(0,0),(-1,-1),2),("BOTTOMPADDING",(0,0),(-1,-1),2),
                ("LEFTPADDING",(0,0),(-1,-1),5),("RIGHTPADDING",(0,0),(-1,-1),5),
                ("LINEBELOW",(0,0),(-1,0),0.5,C_ACC),
                ("ROWBACKGROUNDS",(0,0),(-1,-1),[C_SURF, C_HDR]),
            ]))
            return t

        def stats_cell():
            rows = [
                [Paragraph("PORT STATUS", s_label), ""],
                [Paragraph("Vessels in port",  s_small), Paragraph(f"<b>{vip}</b>", s_bold)],
                [Paragraph("Berths occupied",  s_small), Paragraph(berths, s_body)],
                [Paragraph("Arriving 24h",     s_small), Paragraph(str(arr24), s_body)],
                [Paragraph("Utilisation",      s_small), Paragraph(f"{util}%", s_body)],
                [Paragraph("On-time departure",s_small), Paragraph(f"{otd}%", s_body)],
            ]
            t = Table(rows, colWidths=[26*mm, None])
            t.setStyle(TableStyle([("SPAN",(0,0),(1,0)),
                ("BACKGROUND",(0,0),(-1,-1), C_SURF),
                ("TOPPADDING",(0,0),(-1,-1),2),("BOTTOMPADDING",(0,0),(-1,-1),2),
                ("LEFTPADDING",(0,0),(-1,-1),5),("RIGHTPADDING",(0,0),(-1,-1),5),
                ("LINEBELOW",(0,0),(-1,0),0.5,C_ACC),
                ("ROWBACKGROUNDS",(0,0),(-1,-1),[C_SURF, C_HDR]),
            ]))
            return t

        def movs_cell():
            rows = [[Paragraph("NEXT MOVEMENTS", s_label), "", ""]]
            if movs:
                for d, name, t, berth in movs[:5]:
                    tstr = t.strftime("%H:%M")
                    dir_col = C_ACC if d == "ARR" else C_GREEN
                    rows.append([
                        Paragraph(f'<font color="#{dir_col.hexval()[2:]}">{d}</font>', sty("dc",fontName="Helvetica-Bold",fontSize=8,textColor=dir_col,leading=11)),
                        Paragraph(name[:22], s_body),
                        Paragraph(tstr, sty("tc",fontName="Helvetica-Bold",fontSize=9,textColor=C_ACC,leading=11)),
                    ])
            else:
                rows.append([Paragraph("No movements in window", s_small),"",""])
            t2 = Table(rows, colWidths=[8*mm, None, 12*mm])
            t2.setStyle(TableStyle([("SPAN",(0,0),(2,0)),
                ("BACKGROUND",(0,0),(-1,-1), C_SURF),
                ("TOPPADDING",(0,0),(-1,-1),2),("BOTTOMPADDING",(0,0),(-1,-1),2),
                ("LEFTPADDING",(0,0),(-1,-1),5),("RIGHTPADDING",(0,0),(-1,-1),5),
                ("LINEBELOW",(0,0),(-1,0),0.5,C_ACC),
                ("ROWBACKGROUNDS",(0,0),(-1,-1),[C_SURF, C_HDR]),
            ]))
            return t2

        snap = Table([[wx_cell(), stats_cell(), movs_cell()]], colWidths=[W*0.31, W*0.31, W*0.38])
        snap.setStyle(TableStyle([
            ("VALIGN",(0,0),(-1,-1),"TOP"),
            ("LEFTPADDING",(0,0),(-1,-1),0),("RIGHTPADDING",(0,0),(-1,-1),0),
            ("INNERGRID",(0,0),(-1,-1),0,white),
            ("LEFTPADDING",(1,0),(1,-1),3),("LEFTPADDING",(2,0),(2,-1),3),
        ]))
        story.append(snap)
        story.append(Spacer(1, 4*mm))

        # ── Active Conflicts ──────────────────────────────────────────────
        dec_conflicts = [c for c in conflicts if c.get("signal_type")=="CONFLICT" and c.get("decision_support")]
        story.append(Paragraph("ACTIVE CONFLICTS & DECISIONS", s_sec))
        story.append(HRFlowable(width=W, thickness=0.5, color=C_ACC, spaceAfter=3))
        if dec_conflicts:
            tdata = [[
                Paragraph("VESSEL", s_label),
                Paragraph("CONFLICT", s_label),
                Paragraph("SEVERITY", s_label),
                Paragraph("RECOMMENDED ACTION", s_label),
                Paragraph("COST", s_label),
            ]]
            sev_map = {"critical": C_RED, "high": C_AMBER, "medium": HexColor("#eab308")}
            for c in sorted(dec_conflicts, key=lambda x: {"critical":0,"high":1,"medium":2}.get(x.get("severity",""),3)):
                sev  = c.get("severity","medium")
                sc   = sev_map.get(sev, C_DIM)
                vn   = (c.get("vessel_names") or ["–"])[0]
                desc = c.get("description","")[:60]
                opts = (c.get("decision_support") or {}).get("options",[])
                rec  = next((o for o in opts if o.get("recommended")), opts[0] if opts else None)
                rec_lbl  = (rec.get("label","–"))[:50] if rec else "–"
                cost_lbl = rec.get("cost_label","–") if rec else "–"
                tdata.append([
                    Paragraph(vn[:20], s_bold),
                    Paragraph(desc, s_small),
                    Paragraph(f'<font color="#{sc.hexval()[2:]}">{sev.upper()}</font>',
                              sty("sev",fontName="Helvetica-Bold",fontSize=8,textColor=sc,leading=11)),
                    Paragraph(rec_lbl, s_small),
                    Paragraph(cost_lbl, sty("cost",fontName="Helvetica-Bold",fontSize=8,textColor=C_AMBER,leading=11)),
                ])
            cf_tbl = Table(tdata, colWidths=[W*0.15, W*0.26, W*0.1, W*0.36, W*0.13])
            cf_tbl.setStyle(TableStyle([
                ("BACKGROUND",(0,0),(-1,0), C_HDR),
                ("ROWBACKGROUNDS",(0,1),(-1,-1),[C_SURF, C_BG]),
                ("TOPPADDING",(0,0),(-1,-1),4),("BOTTOMPADDING",(0,0),(-1,-1),4),
                ("LEFTPADDING",(0,0),(-1,-1),5),("RIGHTPADDING",(0,0),(-1,-1),5),
                ("VALIGN",(0,0),(-1,-1),"TOP"),
                ("LINEBELOW",(0,-1),(-1,-1),0.5,C_ACC),
            ]))
            story.append(cf_tbl)
        else:
            story.append(Paragraph("No active conflicts requiring decisions.", s_body))
        story.append(Spacer(1, 4*mm))

        # ── For Your Attention ────────────────────────────────────────────
        attn = [g for g in guidance if g.get("action_required")][:5]
        story.append(Paragraph("FOR YOUR ATTENTION", s_sec))
        story.append(HRFlowable(width=W, thickness=0.5, color=C_ACC, spaceAfter=3))
        if attn:
            for g in attn:
                pri  = g.get("priority","medium")
                pc   = {"critical":C_RED,"high":C_AMBER}.get(pri, C_DIM)
                msg  = g.get("message","")
                det  = g.get("detail","")[:120]
                dl   = g.get("deadline")
                dl_str = ""
                if dl:
                    try: dl_str = f"  Act by {datetime.fromisoformat(dl.replace('Z','+00:00')).strftime('%H:%M UTC')}"
                    except: pass
                row_data = [[
                    Paragraph(f'<font color="#{pc.hexval()[2:]}">{pri.upper()}</font>',
                              sty("at_pri",fontName="Helvetica-Bold",fontSize=7,textColor=pc,leading=10)),
                    Paragraph(f"<b>{msg}</b>{' — ' + det if det else ''}{dl_str}",
                              sty("at_msg",fontName="Helvetica",fontSize=8.5,textColor=C_TEXT,leading=12)),
                ]]
                at_tbl = Table(row_data, colWidths=[14*mm, W-14*mm])
                at_tbl.setStyle(TableStyle([
                    ("BACKGROUND",(0,0),(-1,-1),C_SURF),
                    ("TOPPADDING",(0,0),(-1,-1),4),("BOTTOMPADDING",(0,0),(-1,-1),4),
                    ("LEFTPADDING",(0,0),(-1,-1),5),("RIGHTPADDING",(0,0),(-1,-1),5),
                    ("VALIGN",(0,0),(-1,-1),"TOP"),
                    ("LINEBELOW",(0,0),(-1,-1),0.3,C_HDR),
                    ("LINEBEFORE",(0,0),(0,-1),2,pc),
                ]))
                story.append(at_tbl)
        else:
            story.append(Paragraph("No items requiring immediate attention.", s_body))

        # ── Footer ────────────────────────────────────────────────────────
        story.append(Spacer(1, 5*mm))
        story.append(HRFlowable(width=W, thickness=0.5, color=C_HDR))
        story.append(Spacer(1, 2*mm))
        story.append(Paragraph(
            f"Horizon – Port Intelligence  ·  {port_name}  ·  Generated {date_str}  ·  Simulation Data",
            s_footer
        ))

        doc.build(story)
        pdf_bytes = buf.getvalue()
        fname = f"port-brief-{port_name.lower().replace(' ','-')}-{now_utc.strftime('%Y%m%d-%H%M')}.pdf"
        return pdf_bytes, fname, port_name, date_str

    def _port_brief_pdf(self):
        """Stream the Port Brief PDF as a download."""
        try:
            pdf_bytes, fname, _, _ = self._build_brief_pdf()
            self.send_response(200)
            self.send_header("Content-Type", "application/pdf")
            self.send_header("Content-Length", str(len(pdf_bytes)))
            self.send_header("Content-Disposition", f'attachment; filename="{fname}"')
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(pdf_bytes)
        except Exception as exc:
            log.error("port-brief PDF generation failed: %s", exc, exc_info=True)
            self.send_error(500, f"PDF generation failed: {exc}")

    def _whatif(self):
        """POST /api/whatif — run a shadow scenario simulation, returns conflict diff."""
        try:
            length = int(self.headers.get("Content-Length", 0))
            body   = json.loads(self.rfile.read(length)) if length else {}
            adjustments = body.get("adjustments", [])
            conflict_id = body.get("conflict_id", "")
            if not adjustments:
                self._json({"error": "No adjustments provided"})
                return

            # Snapshot current state (no globals mutated)
            with _profile_lock:
                profile        = dict(_PORT_PROFILE)
                active_port_id = _ACTIVE_PORT_ID

            now = utcnow()
            ds  = get_data_source()
            scrape_result = fetch_vessel_movements(profile, now)
            if scrape_result["using_live_data"] and scrape_result["vessels"]:
                base_vessels = build_vessels_from_qships({"vessels": scrape_result["vessels"], "berths": []})
            else:
                base_vessels = make_vessels(now)

            base_conflicts = []
            try:
                berths = make_berths(now)
                try:
                    pilotage = make_pilotage(base_vessels, now, profile)
                except Exception:
                    pilotage = []
                try:
                    towage = make_towage(base_vessels, now, profile)
                except Exception:
                    towage = []
                base_conflicts = detect_conflicts(base_vessels, berths, pilotage, towage, now, is_live=False)
            except Exception as exc:
                log.warning("whatif base conflict detection failed: %s", exc)

            result = _whatif_shadow(conflict_id, adjustments, base_vessels, base_conflicts)
            self._json(result)

        except Exception as exc:
            log.error("whatif failed: %s", exc, exc_info=True)
            self.send_error(500, str(exc))

    def _apply_whatif(self):
        """POST /api/apply-whatif — store scenario as active overlay on live state."""
        try:
            length = int(self.headers.get("Content-Length", 0))
            body   = json.loads(self.rfile.read(length)) if length else {}
            with _whatif_lock:
                _WHATIF_OVERLAY.clear()
                _WHATIF_OVERLAY.update({
                    "active":      True,
                    "adjustments": body.get("adjustments", []),
                    "label":       body.get("label", "Custom scenario"),
                    "conflict_id": body.get("conflict_id", ""),
                })
            self._json({"success": True})
        except Exception as exc:
            log.error("apply-whatif failed: %s", exc, exc_info=True)
            self.send_error(500, str(exc))

    def _clear_whatif(self):
        """POST /api/clear-whatif — remove the active scenario overlay."""
        with _whatif_lock:
            _WHATIF_OVERLAY.clear()
        self._json({"success": True})

    def _send_brief(self):
        """POST /api/send-brief — email the Port Brief PDF to a list of recipients."""
        try:
            length = int(self.headers.get("Content-Length", 0))
            body   = json.loads(self.rfile.read(length)) if length else {}
            recipients = body.get("recipients") or []
            # Fall back to configured defaults if none provided
            if not recipients:
                recipients = list(_BRIEF_RECIPIENTS)
            if not recipients:
                self._json({"success": False, "error": "No recipients specified and BRIEF_RECIPIENTS not configured."})
                return
            if not _SMTP_HOST:
                self._json({"success": False, "error": "SMTP not configured. Set SMTP_HOST, SMTP_USER, SMTP_PASS on the server."})
                return

            pdf_bytes, fname, port_name, date_str = self._build_brief_pdf()

            import smtplib
            from email.message import EmailMessage
            msg = EmailMessage()
            msg["Subject"] = f"Port Brief — {port_name} — {date_str}"
            msg["From"]    = _SMTP_FROM or _SMTP_USER
            msg["To"]      = ", ".join(recipients)
            msg.set_content(
                f"Please find attached the Port Brief for {port_name}, generated {date_str}.\n\n"
                f"— Horizon Port Intelligence"
            )
            msg.add_attachment(pdf_bytes, maintype="application", subtype="pdf", filename=fname)

            with smtplib.SMTP(_SMTP_HOST, _SMTP_PORT) as smtp:
                smtp.ehlo()
                smtp.starttls()
                if _SMTP_USER and _SMTP_PASS:
                    smtp.login(_SMTP_USER, _SMTP_PASS)
                smtp.send_message(msg)

            log.info("Port Brief emailed to: %s", ", ".join(recipients))
            self._json({"success": True, "recipients": recipients, "port": port_name})

        except Exception as exc:
            log.error("send-brief failed: %s", exc, exc_info=True)
            self._json({"success": False, "error": str(exc)})

    def _logo(self):
        # Prefer logo.png/jpg over logo.svg if present
        for candidate, mime in [
            (LOGO_FILE.with_suffix(".png"),          "image/png"),
            (LOGO_FILE.with_suffix(".jpg"),          "image/jpeg"),
            (LOGO_FILE.with_suffix(".jpeg"),         "image/jpeg"),
            (LOGO_FILE,                              "image/svg+xml"),
        ]:
            if candidate.exists():
                body = candidate.read_bytes()
                self.send_response(200)
                self.send_header("Content-Type", mime)
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Cache-Control", "max-age=3600")
                self.end_headers()
                self.wfile.write(body)
                return
        self.send_error(404, "logo not found")

    def _mobile_icon(self):
        """Serve the PWA home-screen icon. Place mobile-icon.png in the project directory.
        Falls back to the main logo if mobile-icon.png is not present."""
        candidates = [
            (MOBILE_ICON_FILE,                          "image/png"),
            (LOGO_FILE.with_suffix(".png"),             "image/png"),
            (LOGO_FILE.with_suffix(".jpg"),             "image/jpeg"),
            (LOGO_FILE,                                 "image/svg+xml"),
        ]
        for path, mime in candidates:
            if path.exists():
                body = path.read_bytes()
                self.send_response(200)
                self.send_header("Content-Type", mime)
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Cache-Control", "no-store, no-cache, must-revalidate")
                self.end_headers()
                self.wfile.write(body)
                return
        self.send_error(404, "mobile icon not found")

    def _amsg_logo(self):
        """Serve AMS Group logo. Place amsg-logo.png in the project directory."""
        for candidate, mime in [
            (AMSG_LOGO_FILE,                              "image/png"),
            (AMSG_LOGO_FILE.with_suffix(".jpg"),          "image/jpeg"),
            (AMSG_LOGO_FILE.with_suffix(".svg"),          "image/svg+xml"),
        ]:
            if candidate.exists():
                body = candidate.read_bytes()
                self.send_response(200)
                self.send_header("Content-Type", mime)
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Cache-Control", "max-age=3600")
                self.end_headers()
                self.wfile.write(body)
                return
        self.send_error(404, "amsg logo not found")


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    log.info("Starting — active port: %s", _ACTIVE_PORT_ID)
    _schedule_scrapes()
    load_qships_data()

    server = ThreadingHTTPServer(("0.0.0.0", PORT), HorizonHandler)
    ds = get_data_source()
    print(f"╔══ HORIZON BETA 9 ═══════════════════════════╗")
    print(f"║  Active Port: {_PORT_PROFILE['display_name']:<28} ║")
    print(f"║  Data Source: {_PORT_PROFILE['vessel_data_source']:<28} ║")
    print(f"║  BOM Station: {str(_PORT_PROFILE.get('bom_station_id','N/A')):<28} ║")
    print(f"╠═════════════════════════════════════════════╣")
    print(f"║  http://localhost:{PORT}                      ║")
    print(f"║  Set port: HORIZON_PORT=MELBOURNE             ║")
    print(f"║  Press Ctrl+C to stop                        ║")
    print(f"╚═════════════════════════════════════════════╝")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nServer stopped.")
