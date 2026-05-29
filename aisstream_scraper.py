"""
AISStream.io WebSocket connector for Project Horizon.

Connects to wss://stream.aisstream.io/v0/stream and subscribes to live AIS
messages for all four port bounding boxes simultaneously.

What we get that MST doesn't provide:
  - Real vessel draught (voyage-specific, entered by master)
  - Real LOA and beam (from static AIS Type 5 message)
  - Destination port as reported by master
  - Vessel ETA as reported by master
  - Live position and speed

In-port detection:
  Vessels are considered "in port" when their last known position falls
  within the port's tight berth-area bounding box AND their speed over
  ground (SOG) is below IN_PORT_SOG_KTS.  We also track vessels that were
  recently in port (within DEPARTURE_TIMEOUT_MINS) to handle brief AIS gaps.

Architecture:
  A single background thread maintains the WebSocket connection and updates
  shared state dicts.  server.py calls get_vessels_in_port(unloco) which
  reads from those dicts without making any network calls.

Fallback:
  If the WebSocket connection fails or no data is received within
  STALE_TIMEOUT_SECS, get_vessels_in_port() returns None so the caller
  can fall back to MST or simulation.
"""

import json
import logging
import math
import threading
import time
from datetime import datetime, timezone, timedelta

log = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────
_AIS_KEY          = None          # set via configure()
_WS_URL           = "wss://stream.aisstream.io/v0/stream"
IN_PORT_SOG_KTS   = 1.0           # below this = stationary / berthed
DEPARTURE_TIMEOUT = 60 * 30       # seconds — keep vessel in list after leaving
STALE_TIMEOUT     = 60 * 20       # seconds — give up if no message received
RECONNECT_DELAY   = 15            # seconds between reconnect attempts

# ── Port bounding boxes ────────────────────────────────────────────────────────
# Three nested scopes per port (berth_box ⊂ port_box ⊂ approach):
#   approach  — wide AIS subscription area (unchanged)
#   port_box  — operational port area used for Horizon inclusion (Beta 11 Slice 6A)
#   berth_box — tight berth basin, used only for the "berthed" status label
# Format: [min_lat, min_lon, max_lat, max_lon]
# port_box coordinates are APPROXIMATE operating-area rectangles and are tunable
# against live data via the funnel counts exposed in get_status().
_PORT_BOXES = {
    "AUBNE": {
        "name":    "Brisbane",
        "berth_box": [-27.48, 153.08, -27.35, 153.20],   # Port of Brisbane berths
        "port_box":  [-27.52, 153.02, -27.33, 153.22],   # river mouth + basin + inner anchorage
        "approach":  [-27.55, 152.90, -27.25, 153.25],   # wider approach area
    },
    "AUMEL": {
        "name":    "Melbourne",
        "berth_box": [-37.87, 144.88, -37.80, 144.96],   # Swanson / Webb Dock
        "port_box":  [-37.90, 144.85, -37.78, 145.00],   # Port of Melbourne + Williamstown + Yarra mouth
        "approach":  [-38.50, 144.50, -37.70, 145.10],   # Port Phillip Bay
    },
    "AUDRW": {
        "name":    "Darwin",
        "berth_box": [-12.48, 130.82, -12.41, 130.90],   # Darwin Harbour berths
        "port_box":  [-12.52, 130.78, -12.38, 130.95],   # Darwin Harbour operating area
        "approach":  [-12.60, 130.70, -12.30, 131.00],
    },
    "AUGEX": {
        "name":    "Geelong",
        "berth_box": [-38.18, 144.30, -38.09, 144.40],   # Corio Quay / Lascelles
        "port_box":  [-38.22, 144.26, -38.05, 144.45],   # Corio Bay operating area
        "approach":  [-38.30, 144.20, -37.95, 144.55],
    },
}

# AIS vessel type code → human-readable type
_AIS_TYPES = {
    range(70, 80): "Cargo",
    range(80, 90): "Tanker",
    range(60, 70): "Passenger",
    range(30, 33): "Fishing / Towing",
    range(35, 36): "Military",
    range(36, 38): "Sailing / Pleasure",
}

def _ais_type_label(code: int) -> str:
    for r, label in _AIS_TYPES.items():
        if code in r:
            return label
    if code == 71: return "Cargo"
    if code == 72: return "Cargo"
    if code == 73: return "Cargo, Tanker"
    if code == 74: return "Bulk Carrier"
    if code == 79: return "Cargo"
    if code == 89: return "Tanker"
    return "Other"

def _is_commercial(type_code: int, name: str) -> bool:
    """Filter out tugs, small craft, ferries."""
    if type_code in (31, 32, 33, 34, 35, 52):   # towing / tug / port tender
        return False
    name_up = (name or "").upper()
    exclude = {"FERRY", "FLYER", "CAT", "SVITZER", "RIVTOW", "SMIT",
               "TITAN", "SEAHORSE", "PILOT", "PATROL", "RESCUE"}
    return not any(x in name_up for x in exclude)

def _in_box(lat: float, lon: float, box: list) -> bool:
    min_lat, min_lon, max_lat, max_lon = box
    return min_lat <= lat <= max_lat and min_lon <= lon <= max_lon


def _classify(cfg: dict, lat: float, lon: float, sog: float,
              type_code: int, name: str) -> dict:
    """Beta 11 Slice 6A: classify one vessel position against a port's three
    nested scopes plus motion + commercial class. Pure (args only); used by
    both _update_position (inclusion) and get_status (funnel).

    Inclusion rule: accepted_for_horizon = in_port_area AND commercial.
    Stationarity is a STATUS signal, not an inclusion gate.
    """
    port_box     = cfg.get("port_box", cfg["berth_box"])   # fallback keeps pre-6A behaviour
    in_approach  = _in_box(lat, lon, cfg["approach"])
    in_port_area = _in_box(lat, lon, port_box)
    in_berth     = _in_box(lat, lon, cfg["berth_box"])
    stationary   = sog < IN_PORT_SOG_KTS
    commercial   = _is_commercial(type_code, name)
    accepted     = in_port_area and commercial
    if in_berth and stationary:
        status = "berthed"
    elif in_port_area and stationary:
        status = "anchored"
    elif in_port_area:
        status = "underway"
    elif in_approach:
        status = "approach"
    else:
        status = "tracked"
    return {
        "in_approach":  in_approach,
        "in_port_area": in_port_area,
        "in_berth_area": in_berth,
        "stationary":   stationary,
        "commercial":   commercial,
        "accepted":     accepted,
        "status":       status,
    }

# ── Shared state ───────────────────────────────────────────────────────────────
_lock           = threading.Lock()
_positions      = {}   # mmsi → {lat, lon, sog, heading, ts}
_static_data    = {}   # mmsi → {name, imo, callsign, type_code, type_label,
                       #          loa_m, beam_m, draught_m, destination, eta_raw, ts}
_in_port        = {}   # unloco → {mmsi: {arrived_utc, ...merged vessel dict}}
_last_message   = 0.0  # epoch seconds — for stale detection
_connected      = False
_configured     = False


def configure(api_key: str):
    global _AIS_KEY, _configured
    _AIS_KEY    = api_key
    _configured = bool(api_key)


def is_configured() -> bool:
    return _configured


def is_connected() -> bool:
    return _connected


def is_stale() -> bool:
    if not _last_message:
        return True
    return (time.time() - _last_message) > STALE_TIMEOUT


# ── Vessel state helpers ───────────────────────────────────────────────────────

def _update_position(mmsi: str, lat: float, lon: float, sog: float, heading: float):
    """Called for every PositionReport.  Updates position and checks port entry/exit."""
    now = time.time()
    with _lock:
        _positions[mmsi] = {"lat": lat, "lon": lon, "sog": sog,
                            "heading": heading, "ts": now}

    # Check each port (Beta 11 Slice 6A: inclusion = in_port_area AND commercial;
    # stationarity is a status label, not an inclusion gate).
    for unloco, cfg in _PORT_BOXES.items():
        with _lock:
            port_vessels = _in_port.setdefault(unloco, {})
            static = _static_data.get(mmsi, {})
            type_code = static.get("type_code", 0)
            name = static.get("name", f"VESSEL-{mmsi}")
            c = _classify(cfg, lat, lon, sog, type_code, name)

            if c["accepted"]:
                if mmsi not in port_vessels:
                    log.info("AISStream: %s in %s operating area as %s (%.4f,%.4f SOG %.1f)",
                             name or mmsi, unloco, c["status"], lat, lon, sog)
                    port_vessels[mmsi] = {
                        "mmsi":        mmsi,
                        "arrived_utc": datetime.now(timezone.utc).isoformat(),
                        "last_seen":   now,
                        "status":      c["status"],
                    }
                else:
                    port_vessels[mmsi]["last_seen"] = now
                    port_vessels[mmsi]["status"]    = c["status"]
            elif mmsi in port_vessels:
                # Vessel has left the port operating area — start departure timer
                last_seen = port_vessels[mmsi].get("last_seen", now)
                if (now - last_seen) > DEPARTURE_TIMEOUT:
                    log.info("AISStream: %s left %s operating area (timeout)", name or mmsi, unloco)
                    del port_vessels[mmsi]


def _update_static(mmsi: str, data: dict):
    """Called for every ShipStaticData message."""
    # AIS dimensions: A+B = LOA, C+D = beam
    dim = data.get("Dimension", {})
    a = float(dim.get("A", 0) or 0)
    b = float(dim.get("B", 0) or 0)
    c = float(dim.get("C", 0) or 0)
    d = float(dim.get("D", 0) or 0)
    loa_m  = round(a + b, 1) if (a + b) > 10 else None
    beam_m = round(c + d, 1) if (c + d) > 3  else None

    draught_raw = data.get("MaximumStaticDraught") or data.get("Draught") or 0
    try:
        draught_m = round(float(draught_raw) / 10.0, 1) if float(draught_raw) > 0 else None
        # AIS draught is in tenths of metres when it's an int; if it's already
        # a reasonable float (< 30) it's already in metres
        if draught_m and draught_m > 30:
            draught_m = round(float(draught_raw) / 10.0, 1)
    except Exception:
        draught_m = None

    type_code = int(data.get("Type", 0) or 0)

    with _lock:
        _static_data[mmsi] = {
            "name":        (data.get("Name") or "").strip(),
            "imo":         str(data.get("ImoNumber") or ""),
            "callsign":    (data.get("CallSign") or "").strip(),
            "type_code":   type_code,
            "type_label":  _ais_type_label(type_code),
            "loa_m":       loa_m,
            "beam_m":      beam_m,
            "draught_m":   draught_m,
            "destination": (data.get("Destination") or "").strip(),
            "eta_raw":     data.get("Eta"),
            "ts":          time.time(),
        }


# ── Public API ─────────────────────────────────────────────────────────────────

def get_vessels_in_port(unloco: str) -> list | None:
    """
    Return a list of vessel dicts currently in port, or None if no data.
    Each dict is compatible with the MST scraper output schema so that
    mst_scraper.build_horizon_vessels() can be reused unchanged.
    """
    if not _configured or is_stale():
        return None

    with _lock:
        raw = dict(_in_port.get(unloco, {}))

    if not raw:
        return None

    vessels = []
    with _lock:
        for mmsi, entry in raw.items():
            static = _static_data.get(mmsi, {})
            pos    = _positions.get(mmsi, {})
            vessels.append({
                "mmsi":        mmsi,
                "imo":         static.get("imo") or "",
                "name":        static.get("name") or f"VESSEL-{mmsi}",
                "arrived_utc": entry.get("arrived_utc"),
                # Real dimensions — these replace simulated values
                "loa_m":       static.get("loa_m"),
                "beam_m":      static.get("beam_m"),
                "draught_m":   static.get("draught_m"),
                "vessel_type": static.get("type_label", "Cargo"),
                "destination": static.get("destination"),
                "callsign":    static.get("callsign"),
                # Position
                "lat":         pos.get("lat"),
                "lon":         pos.get("lon"),
                "sog":         pos.get("sog"),
                # Beta 11 Slice 6A: AIS-derived status label (additive; does NOT
                # override the operational status assigned downstream).
                "ais_status":  entry.get("status"),
            })

    log.info("AISStream: %d vessels in port at %s", len(vessels), unloco)
    return vessels if vessels else None


def get_status() -> dict:
    """Diagnostic summary for /api/aisstream-status endpoint.

    Beta 11 Slice 6A adds a per-port `funnel` showing where tracked vessels are
    excluded, computed read-time by re-classifying current positions. `in_port`
    now equals the accepted (in_port_area AND commercial) count."""
    with _lock:
        positions = {m: dict(p) for m, p in _positions.items()}
        statics   = {m: dict(s) for m, s in _static_data.items()}
        port_counts = {u: len(v) for u, v in _in_port.items()}
        static_count = len(_static_data)
        pos_count    = len(_positions)

    # Per-port funnel: among vessels in THIS port's approach, how many survive
    # each stage down to accepted. (vessels_tracked is the global count.)
    funnel = {}
    for unloco, cfg in _PORT_BOXES.items():
        cnt = {"in_approach": 0, "in_port_area": 0, "in_berth_area": 0,
               "stationary": 0, "commercial": 0, "accepted": 0}
        status_breakdown = {"berthed": 0, "anchored": 0, "underway": 0, "approach": 0}
        for mmsi, pos in positions.items():
            st = statics.get(mmsi, {})
            c = _classify(cfg, pos.get("lat", 0.0), pos.get("lon", 0.0),
                          pos.get("sog", 0.0), st.get("type_code", 0),
                          st.get("name", f"VESSEL-{mmsi}"))
            if not c["in_approach"]:
                continue
            cnt["in_approach"]  += 1
            cnt["in_port_area"] += 1 if c["in_port_area"] else 0
            cnt["in_berth_area"] += 1 if c["in_berth_area"] else 0
            cnt["stationary"]   += 1 if c["stationary"] else 0
            cnt["commercial"]   += 1 if c["commercial"] else 0
            cnt["accepted"]     += 1 if c["accepted"] else 0
            if c["status"] in status_breakdown:
                status_breakdown[c["status"]] += 1
        funnel[unloco] = {**cnt, "status": status_breakdown}

    age = round(time.time() - _last_message, 1) if _last_message else None
    return {
        "configured":    _configured,
        "connected":     _connected,
        "stale":         is_stale(),
        "last_message_age_s": age,
        "vessels_tracked": pos_count,
        "static_data":   static_count,
        "in_port":       port_counts,   # = accepted (in_port_area AND commercial)
        "funnel":        funnel,
    }


# ── WebSocket thread ───────────────────────────────────────────────────────────

def _build_subscription() -> str:
    """Build the AISStream subscription message covering all port approach boxes."""
    boxes = []
    for cfg in _PORT_BOXES.values():
        lo_lat, lo_lon, hi_lat, hi_lon = cfg["approach"]
        boxes.append([[lo_lat, lo_lon], [hi_lat, hi_lon]])

    return json.dumps({
        "APIKey":             _AIS_KEY,
        "BoundingBoxes":      boxes,
        "FilterMessageTypes": ["PositionReport", "ShipStaticData"],
    })


def _run_websocket():
    """Background thread — connect, subscribe, process messages, reconnect on error."""
    global _connected, _last_message

    try:
        import websocket
    except ImportError:
        log.error("websocket-client not installed — AISStream disabled. "
                  "Add websocket-client to requirements.txt")
        return

    while True:
        try:
            log.info("AISStream: connecting to %s", _WS_URL)
            ws = websocket.create_connection(_WS_URL, timeout=30)
            ws.send(_build_subscription())
            _connected = True
            log.info("AISStream: subscribed to %d port boxes",
                     len(_PORT_BOXES))

            while True:
                raw = ws.recv()
                if not raw:
                    continue
                _last_message = time.time()

                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    continue

                msg_type = msg.get("MessageType", "")
                meta     = msg.get("MetaData", {})
                mmsi     = str(meta.get("MMSI", "") or "")
                if not mmsi:
                    continue

                if msg_type == "PositionReport":
                    pr = msg.get("Message", {}).get("PositionReport", {})
                    lat = float(pr.get("Latitude",  0) or 0)
                    lon = float(pr.get("Longitude", 0) or 0)
                    sog = float(pr.get("Sog",       0) or 0)
                    hdg = float(pr.get("TrueHeading", 0) or 0)
                    if lat != 0 and lon != 0:
                        _update_position(mmsi, lat, lon, sog, hdg)

                elif msg_type == "ShipStaticData":
                    sd = msg.get("Message", {}).get("ShipStaticData", {})
                    _update_static(mmsi, sd)

        except Exception as exc:
            _connected = False
            log.warning("AISStream: connection lost (%s) — reconnect in %ds",
                        exc, RECONNECT_DELAY)
            time.sleep(RECONNECT_DELAY)


def start():
    """Start the background WebSocket thread. Call once at server startup."""
    if not _configured:
        log.info("AISSTREAM_API_KEY not set — AISStream disabled")
        return
    t = threading.Thread(target=_run_websocket, daemon=True, name="aisstream")
    t.start()
    log.info("AISStream: background thread started")
