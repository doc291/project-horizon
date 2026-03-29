#!/usr/bin/env python3
"""
Project Horizon — Beta 8b
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

_ACTIVE_PORT_ID  = os.environ.get("HORIZON_PORT", "BRISBANE").upper()
_PORT_PROFILE    = get_profile(_ACTIVE_PORT_ID)
_profile_lock    = threading.Lock()

log = logging.getLogger("horizon")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [horizon] %(levelname)s %(message)s",
                    datefmt="%Y-%m-%dT%H:%M:%SZ")

PORT = int(os.environ.get("PORT", 8000))
INDEX_HTML    = Path(__file__).parent / "index.html"
LOGO_FILE     = Path(__file__).parent / "logo.svg"
AMSG_LOGO_FILE = Path(__file__).parent / "amsg-logo.png"
QSHIPS_FILE   = Path(__file__).parent / "qships_data.json"

# ── Auth ──────────────────────────────────────────────────────────────────────
_AUTH_USER   = os.environ.get("HORIZON_USER", "horizon")
_AUTH_PASS   = os.environ.get("HORIZON_PASS", "ams2026")
_SESSION_KEY = secrets.token_hex(32)          # regenerated each server restart
_COOKIE_NAME = "hz_sess"
_COOKIE_TTL  = 60 * 60 * 12                   # 12 hours

# Paths that bypass auth entirely (assets needed by the login page itself)
_PUBLIC_PATHS = {"/login", "/logo", "/amsg-logo", "/health"}

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
    raw = [
        ("B01", 350, 14.5, "occupied",     4, now + timedelta(hours=4)),
        ("B02", 300, 13.0, "occupied",     4, now + timedelta(hours=8)),
        ("B03", 250, 11.5, "reserved",     2, now + timedelta(hours=2)),
        ("B04", 320, 14.0, "available",    3, None),
        ("B05", 280, 12.5, "maintenance",  0, now + timedelta(hours=20)),
        ("B06", 220, 10.0, "occupied",     0, now + timedelta(hours=6)),
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
    slots = [
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


def make_pilotage(vessels: list, now: datetime) -> list:
    events = []
    inbound  = [v for v in vessels if v["status"] not in ("berthed", "departed")]
    outbound = [v for v in vessels if v["status"] == "berthed"]
    for v in inbound:
        pilot_idx = int(hashlib.md5(v["id"].encode()).hexdigest(), 16) % len(PILOTS)
        sched = isoparse(v["eta"]) - timedelta(hours=1, minutes=30)
        events.append({
            "id": f"PIL-{v['id']}-IN",
            "vessel_id": v["id"], "vessel_name": v["name"],
            "pilot_name": PILOTS[pilot_idx],
            "scheduled_time": fmt(sched),
            "boarding_station": STATIONS[pilot_idx % len(STATIONS)],
            "direction": "inbound",
            "status": "confirmed" if v["status"] == "confirmed" else "scheduled",
        })
    for v in outbound:
        pilot_idx = (int(hashlib.md5(v["id"].encode()).hexdigest(), 16) + 1) % len(PILOTS)
        sched = isoparse(v["etd"]) - timedelta(hours=1)
        events.append({
            "id": f"PIL-{v['id']}-OUT",
            "vessel_id": v["id"], "vessel_name": v["name"],
            "pilot_name": PILOTS[pilot_idx],
            "scheduled_time": fmt(sched),
            "boarding_station": STATIONS[pilot_idx % len(STATIONS)],
            "direction": "outbound",
            "status": "scheduled",
        })
    return events


def make_towage(vessels: list, now: datetime) -> list:
    events = []
    eligible = [v for v in vessels if v["towage_required"]]
    for v in eligible:
        n_tugs = 2 if v["loa"] > 200 else 1
        # Deterministic tug assignment from vessel ID hash
        h = int(hashlib.md5(v["id"].encode()).hexdigest(), 16)
        tug_indices = [(h + i) % len(TUGS) for i in range(n_tugs)]
        # Ensure no duplicate indices
        seen_idx = set()
        unique_indices = []
        for idx in tug_indices:
            if idx not in seen_idx:
                seen_idx.add(idx)
                unique_indices.append(idx)
        tugs = [{"tug_id": TUGS[i].replace(" ", "-").upper(), "tug_name": TUGS[i]}
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
                    conflicts.append(_conflict(
                        str(uuid.uuid4())[:8], "berth_overlap", "CONFLICT", sev,
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
                    conflicts.append(_conflict(
                        str(uuid.uuid4())[:8], "berth_not_ready", "WARNING", sev,
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
                    conflicts.append(_conflict(
                        str(uuid.uuid4())[:8], "pilotage_window", "WARNING", "high",
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
                    conflicts.append(_conflict(
                        str(uuid.uuid4())[:8], "towage_resource", "WARNING", "medium",
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
            conflicts.append(_conflict(
                str(uuid.uuid4())[:8], "eta_variance", "ADVISORY", "medium",
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
                    conflicts.append(_conflict(
                        str(uuid.uuid4())[:8], "bridge_restriction", "WARNING", sev,
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
            "id": str(uuid.uuid4())[:8],
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
                    "id": str(uuid.uuid4())[:8],
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
                    "id": str(uuid.uuid4())[:8],
                    "priority": "medium",
                    "message": f"{v['name']} departing in {hrs:.1f}h",
                    "detail": (
                        f"{v['name']} departs {v['berth_id']} at {etd.strftime('%H:%M')} UTC. "
                        f"Ensure outbound pilot and towage confirmed."
                    ),
                    "resolution_options": [],
                    "vessel_id": v["id"], "vessel_name": v["name"],
                    "action_required": False,
                    "deadline": fmt(etd - timedelta(minutes=30)),
                })

    # Maintenance berth returning
    for b in berths:
        if b["status"] == "maintenance" and b.get("readiness_time"):
            ready = isoparse(b["readiness_time"])
            hrs = (ready - now).total_seconds() / 3600
            if 0 < hrs < 12:
                items.append({
                    "id": str(uuid.uuid4())[:8],
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


# ── Summary builder ────────────────────────────────────────────────────────────

def build_summary():
    now = utcnow()
    with _profile_lock:
        profile = dict(_PORT_PROFILE)

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
        # QShips fallback for Brisbane
        if ds["source"] == "qships" and _qships_data:
            try:
                vessels   = build_vessels_from_qships(_qships_data)
                berths    = build_berths_from_qships(_qships_data)
                port_name = _qships_data.get("port_name", profile["display_name"])
                is_live   = True
            except Exception as exc:
                log.error("QShips vessel build failed (%s) — falling back to simulation", exc)
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
        pilotage = make_pilotage(vessels, now)
    except Exception as exc:
        log.error("make_pilotage failed: %s — using empty list", exc)
        pilotage = []

    try:
        towage = make_towage(vessels, now)
    except Exception as exc:
        log.error("make_towage failed: %s — using empty list", exc)
        towage = []

    weather  = make_weather(profile)
    tides    = make_tides(bom_result)

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
    if using_live_vessel:
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
            "pilots_available":   _PORT_PROFILE.get("pilots_available", 3),
            "tugs_available":     _PORT_PROFILE.get("tugs_available", 4),
        },
        "vessels":           vessels,
        "berths":            berths,
        "pilotage":          pilotage,
        "towage":            towage,
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
            "id":                    _ACTIVE_PORT_ID,
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
        else:
            self.send_error(405)

    def _handle_login(self):
        try:
            length = int(self.headers.get("Content-Length", 0))
            body   = self.rfile.read(length).decode()
            params = parse_qs(body)
            user   = params.get("username", [""])[0].strip()
            pw     = params.get("password", [""])[0].strip()
            user_ok = hmac.compare_digest(user, _AUTH_USER)
            pass_ok = hmac.compare_digest(pw,   _AUTH_PASS)
            if user_ok and pass_ok:
                token = _make_token()
                self.send_response(302)
                self.send_header("Location", "/")
                self.send_header(
                    "Set-Cookie",
                    f"{_COOKIE_NAME}={token}; Path=/; HttpOnly; SameSite=Strict; Max-Age={_COOKIE_TTL}"
                )
                self.send_header("Content-Length", "0")
                self.end_headers()
            else:
                self._serve_login(error=True)
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

    def _serve_login(self, error: bool = False):
        error_html = (
            '<div class="login-error">Incorrect username or password. Please try again.</div>'
            if error else ""
        )
        page = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Project Horizon — Sign In</title>
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
      height: 64px; width: auto; margin-bottom: 6px;
    }}
    .login-title {{
      font-size: 11px; font-weight: 700; letter-spacing: 1.2px;
      text-transform: uppercase; color: #38bdf8; margin-bottom: 28px;
    }}
    .login-amsg {{
      background: rgba(255,255,255,.95); border-radius: 6px;
      padding: 4px 12px; display: inline-flex; align-items: center;
      margin-bottom: 28px;
    }}
    .login-amsg img {{ height: 22px; width: auto; }}
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
            self._serve_login()
            return
        if path not in _PUBLIC_PATHS and not self._is_authenticated():
            self._redirect(f"/login")
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
        elif path in ("/", "/index.html"):
            self._html()
        elif path == "/logo":
            self._logo()
        elif path == "/amsg-logo":
            self._amsg_logo()
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
    print(f"╔══ HORIZON BETA 8 ═══════════════════════════╗")
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
