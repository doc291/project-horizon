from __future__ import annotations
"""
MyShipTracking API connector for Project Horizon.

Uses /api/v2/port/calls to derive which vessels are currently in port,
then maps them into the Horizon vessel schema (with simulated operational
detail filling the fields the AIS data doesn't provide).

Port IDs (pre-resolved, avoid spending credits on /port lookups):
    Brisbane  (AUBNE)  → 108
    Melbourne (AUMEL)  → 293
    Darwin    (AUDRW)  → 3870
"""

import logging
import hashlib
import random
from datetime import datetime, timedelta, timezone

# R4.3 — PORT_PROFILES is imported to resolve the per-port `towage_rule`
# at vessel-build time. Used by build_horizon_vessels for AIS/cached
# vessels only; the pure simulation fallback path (make_vessels) still
# uses the legacy `loa > 200` literal until R4.4.
from port_profiles import PORT_PROFILES


def isoparse(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))

log = logging.getLogger(__name__)

MST_BASE    = "https://api.myshiptracking.com/api/v2"
_MST_KEY    = None   # set via configure()

# Known UNLOCODE → port_id mapping (pre-resolved, saves credits)
_PORT_IDS = {
    "AUBNE": 108,
    "AUMEL": 293,
    "AUDRW": 3870,
    "AUGEX": 180,    # Port of Geelong
}

# Vessel types that are almost certainly not commercial port calls
# (local ferries / small craft that happen to have IMOs)
_EXCLUDE_NAME_FRAGMENTS = {
    "FERRY", "FLYER", "CAT", "REEF", "VEDETTE", "IRONCLAD",
    "MICAT", "OXLEY", "MULGUMPIN",
    "SVITZER", "RIVTOW", "SMIT", "TITAN", "SEAHORSE",  # tug operators
}


def configure(api_key: str):
    """Set the API key. Call once at startup."""
    global _MST_KEY
    _MST_KEY = api_key


def is_configured() -> bool:
    return bool(_MST_KEY)


# ── Raw API calls ─────────────────────────────────────────────────────────────

def _get(path: str, params: dict) -> dict:
    import requests
    r = requests.get(
        f"{MST_BASE}/{path}",
        params=params,
        headers={"authorization": f"Bearer {_MST_KEY}"},
        timeout=15,
    )
    r.raise_for_status()
    return r.json()


def fetch_port_calls(unloco: str, days: int = 2) -> list:
    """Raw port/calls response — list of ARRIVAL/DEPARTURE events."""
    try:
        resp = _get("port/calls", {"unloco": unloco, "days": days})
        if resp.get("status") == "success":
            return resp.get("data", [])
        log.warning("MST port/calls non-success: %s", resp)
        return []
    except Exception as exc:
        log.error("MST port/calls failed for %s: %s", unloco, exc)
        return []


def fetch_port_estimate(unloco: str) -> list:
    """Expected arrivals from port/estimate."""
    try:
        resp = _get("port/estimate", {"unloco": unloco})
        if resp.get("status") == "success":
            return resp.get("data", [])
        return []
    except Exception as exc:
        log.error("MST port/estimate failed for %s: %s", unloco, exc)
        return []


# ── Vessel derivation ─────────────────────────────────────────────────────────

def _is_commercial(event: dict) -> bool:
    """Filter out small craft and local ferries."""
    if not event.get("imo"):
        return False
    name = (event.get("vessel_name") or "").upper()
    for frag in _EXCLUDE_NAME_FRAGMENTS:
        if frag in name:
            return False
    return True


def get_vessels_in_port(unloco: str) -> list:
    """
    Derive the current in-port vessel list from port/calls events.

    Logic: for each vessel (keyed by MMSI), the most recent event
    determines status. ARRIVAL → in port. DEPARTURE → has left.

    Returns list of dicts with: mmsi, imo, name, arrived_utc.
    """
    events = fetch_port_calls(unloco, days=2)
    if not events:
        return []

    # Keep only the most recent event per MMSI
    latest: dict = {}
    for ev in events:
        mmsi = ev.get("mmsi")
        if not mmsi:
            continue
        existing = latest.get(mmsi)
        if not existing or ev["time_utc"] > existing["time_utc"]:
            latest[mmsi] = ev

    in_port = []
    for mmsi, ev in latest.items():
        if ev.get("event") != "ARRIVAL":
            continue
        if not _is_commercial(ev):
            continue
        in_port.append({
            "mmsi":        str(mmsi),
            "imo":         str(ev["imo"]),
            "name":        ev.get("vessel_name", "Unknown"),
            "arrived_utc": ev.get("time_utc"),
        })

    log.info("MST: %d commercial vessels in port at %s", len(in_port), unloco)
    return in_port


# ── Horizon vessel model builder ──────────────────────────────────────────────

# Simulated vessel characteristics seeded from MMSI so they stay stable
# across refreshes. Values are port-context-appropriate.
_LOA_BY_VTYPE  = {"Cargo": (180, 300), "Tanker": (150, 280),
                   "Bulk Carrier": (180, 325), "Container": (200, 400),
                   "Other": (100, 220)}
_TYPES = ["Cargo", "Bulk Carrier", "Tanker", "Container Ship"]


def _seed(mmsi: str) -> random.Random:
    h = int(hashlib.md5(mmsi.encode()).hexdigest(), 16)
    return random.Random(h)


# Reverse-lookup cache: UNLOCODE -> towage_rule dict. Built lazily on first
# access. The cache is invalidated only on module reload, which is
# acceptable because PORT_PROFILES is a module-level constant.
_UNLOCO_TO_TOWAGE_RULE: dict | None = None

def _towage_rule_for_unloco(unloco: str) -> dict:
    """
    Resolve the representative towage-demand rule for an UNLOCODE.

    Returns an empty dict when no profile matches the UNLOCODE. The caller
    treats an empty rule as "no estimate available" and defaults to
    n_tugs=0; this is consistent with the helper's safe-default behaviour.
    """
    global _UNLOCO_TO_TOWAGE_RULE
    if _UNLOCO_TO_TOWAGE_RULE is None:
        _UNLOCO_TO_TOWAGE_RULE = {
            (p.get("unloco") or ""): (p.get("towage_rule") or {})
            for p in PORT_PROFILES.values()
        }
    return _UNLOCO_TO_TOWAGE_RULE.get(unloco or "", {})


# ─────────────────────────────────────────────────────────────────────────────
# R4.2 — Representative towage-demand helper.
#
# Compute an estimated tug demand for a vessel under a port's `towage_rule`
# (see port_profiles.py). This is the Beta 12 representative demand model
# only — NOT an operational towage requirement, NOT a substitute for the
# port's published Harbour Master's Directions or the towage operator's
# matrix. The legacy field `towage_required` (named for historical reasons)
# remains derived elsewhere; this helper produces the primary `n_tugs`
# value only.
#
# Helper is added at R4.2 and not yet wired into build_horizon_vessels or
# make_vessels. Both legacy `loa > 200` sites remain unchanged. Wiring
# happens at R4.3 / R4.4.
# ─────────────────────────────────────────────────────────────────────────────

def _n_tugs_for(rule: dict, vessel: dict) -> int:
    """
    Return estimated tug demand for `vessel` under `rule`.

    `rule` is a port_profile['towage_rule'] block:
      {
        'bands': [{'loa_max': int, 'n_tugs': int}, ...],
        'vessel_type_floor': {'<type>': int, ...},
        'vessel_name_override': {'<UPPER NAME>': int, ...},
        'loa_epsilon_m': float
      }

    `vessel` is a Horizon vessel dict; uses keys `name`, `loa_m`, and
    `vessel_type`. Missing or unknown values fall back safely.

    Resolution order:
      1. Vessel-name override (rare; explicit operator allowlisting).
      2. LOA band lookup with `loa_epsilon_m` tolerance — admits
         hairline-below cases like the 199.9 m car carrier (BYD ZHENGZHOU
         class) into the next band rather than excluding them via AIS
         rounding.
      3. Vessel-type floor — promotes n to the type minimum when the
         vessel's type is listed in `vessel_type_floor` (e.g., car
         carriers get a minimum of 2 regardless of LOA band).

    Returns 0 when no rule, no LOA, no matching band, and no applicable
    floor or override is present. Never returns negative.
    """
    if not isinstance(rule, dict) or not isinstance(vessel, dict):
        return 0

    # 1. Vessel-name override (absolute when present).
    name = (vessel.get("name") or "")
    if isinstance(name, str):
        name_key = name.upper().strip()
    else:
        name_key = ""
    overrides = rule.get("vessel_name_override") or {}
    if name_key and name_key in overrides:
        try:
            return max(0, int(overrides[name_key]))
        except (TypeError, ValueError):
            pass  # malformed override entry — fall through to band lookup

    # 2. LOA band lookup with epsilon tolerance.
    loa = vessel.get("loa_m")
    bands = rule.get("bands") or []
    try:
        eps = float(rule.get("loa_epsilon_m") or 0.0)
    except (TypeError, ValueError):
        eps = 0.0

    n = 0
    if loa is not None:
        try:
            loa_val = float(loa)
            adjusted = loa_val + eps
            matched = False
            for band in bands:
                band_max = band.get("loa_max") if isinstance(band, dict) else None
                if band_max is None:
                    continue
                if adjusted <= float(band_max):
                    n = int(band.get("n_tugs", 0) or 0)
                    matched = True
                    break
            if not matched and bands:
                last = bands[-1]
                if isinstance(last, dict):
                    n = int(last.get("n_tugs", 0) or 0)
        except (TypeError, ValueError):
            n = 0

    # 3. Vessel-type floor.
    vtype = vessel.get("vessel_type") or ""
    if isinstance(vtype, str):
        floors = rule.get("vessel_type_floor") or {}
        if vtype in floors:
            try:
                floor_val = int(floors[vtype])
                if floor_val > n:
                    n = floor_val
            except (TypeError, ValueError):
                pass

    return max(0, n)


def _sim_vessel_props(mmsi: str, now: datetime) -> dict:
    """
    Generate stable simulated properties from MMSI seed.

    R4.4 — no longer carries `towage_required`. Tug-demand estimation
    requires port context (per-port `towage_rule`); the helper
    `_n_tugs_for(rule, vessel)` is the single authoritative source for
    representative tug demand. Callers that need `n_tugs` /
    `towage_required` must compute them from the port's `towage_rule`
    after merging these props with the per-vessel context (notably the
    vessel's UNLOCODE).
    """
    rng = _seed(mmsi)
    vtype = rng.choice(_TYPES)
    loa   = round(rng.uniform(160, 310), 1)
    beam  = round(loa * rng.uniform(0.14, 0.17), 1)
    draught = round(rng.uniform(8.5, 14.0), 2)
    # ETD: 4–72 hours from now
    etd_h = rng.uniform(4, 72)
    etd   = now + timedelta(hours=etd_h)
    return {
        "vessel_type": vtype,
        "loa":         loa,
        "beam":        beam,
        "draught":     draught,
        "etd":         etd.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "flag":        rng.choice(["SG", "HK", "LR", "PA", "MH", "BS"]),
    }


def build_horizon_vessels(unloco: str, berths: list, now: datetime,
                          cached_vessels: list | None = None) -> list | None:
    """
    Build a Horizon-compatible vessel list using real vessel identities from
    MST combined with simulated operational detail.

    cached_vessels: pre-fetched list from the background cache. If provided,
    no API call is made. If None, falls back to a live fetch (use only for
    the /api/mst-status diagnostic endpoint).

    Returns None if no data available (caller falls back to pure simulation).
    """
    # Allow call with pre-fetched vessels (e.g. from AISStream) even when
    # MST itself is not configured — no API call is made in that path.
    if not is_configured() and cached_vessels is None:
        return None

    real_vessels = cached_vessels if cached_vessels is not None else get_vessels_in_port(unloco)
    if not real_vessels:
        return None

    # Assign berths deterministically — only use available/occupied berths.
    # Each berth is claimed by at most one vessel; overflow vessels are left
    # without a berth assignment (at anchorage) which is operationally correct
    # — a port cannot have more vessels berthed than it has berths.
    assignable = [b for b in berths if b.get("status") in ("available", "occupied")]
    claimed_berths: set = set()
    vessels_out = []

    for i, rv in enumerate(real_vessels):
        mmsi = rv["mmsi"]
        props = _sim_vessel_props(mmsi, now)

        # Prefer real dimensions from AISStream (rv) over simulated fallback.
        # AISStream provides loa_m, beam_m, draught_m when available.
        loa     = rv.get("loa_m")     or props["loa"]
        beam    = rv.get("beam_m")    or props["beam"]
        draught = rv.get("draught_m") or props["draught"]
        vtype   = rv.get("vessel_type") or props["vessel_type"]
        dest    = rv.get("destination") or None
        source  = "ais" if rv.get("loa_m") else "mst"

        # Berth assignment — one vessel per berth, MMSI-hash ordered to keep
        # assignments stable across refreshes.  Vessels that cannot be assigned
        # a berth remain in port (at anchorage) with berth_id=None.
        berth = None
        for b in assignable:
            if b["id"] not in claimed_berths:
                berth = b
                claimed_berths.add(b["id"])
                break
        berth_id   = berth["id"]   if berth else None
        berth_name = berth["name"] if berth else None

        # ETA: use arrival time from port/calls as the "berthed since" time
        arrived = rv.get("arrived_utc")
        try:
            eta_dt = isoparse(arrived) if arrived else now - timedelta(hours=2)
        except Exception:
            eta_dt = now - timedelta(hours=2)
        eta_str = eta_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

        # R4.3 — Representative tug-demand estimate for this AIS/cached
        # vessel under the port's towage_rule. NOT an operational towage
        # requirement; NOT a substitute for the port's Harbour Master's
        # Directions. The helper's primary output is `n_tugs`; the
        # historically-named field `towage_required` is derived as
        # `n_tugs >= 1` for downstream callers that still consume the
        # legacy boolean. When the port has no towage_rule configured
        # (unknown unloco, future port), n_tugs falls back to 0.
        _towage_rule = _towage_rule_for_unloco(unloco)
        _n_tugs = _n_tugs_for(_towage_rule, {
            "name":        rv["name"],
            "loa_m":       loa,
            "vessel_type": vtype,
        })
        vessels_out.append({
            "id":              f"MST-{mmsi}",
            "name":            rv["name"],
            "mmsi":            mmsi,
            "imo":             rv["imo"],
            "status":          "berthed",
            "berth_id":        berth_id,
            "berth":           berth_name,
            "eta":             eta_str,
            "ata":             eta_str,   # berthed = already arrived; ata = arrival time
            "atd":             None,
            "etd":             props["etd"],
            "loa":             loa,
            "beam":            beam,
            "draught":         draught,
            "vessel_type":     vtype,
            "flag":            props["flag"],
            "n_tugs":          _n_tugs,
            "towage_required": _n_tugs >= 1,
            "destination":     dest,
            "at_anchorage":    False,
            "source":          source,
        })

    # Add 2–3 simulated inbound vessels so the conflict engine has something
    # to work with on arrivals (MST port/estimate often returns empty).
    # Names are seeded from the UNLOCO so they're stable across refreshes.
    # The pool is shuffled per-port so each port gets distinct names.
    _INBOUND_NAMES = [
        "PACIFIC NAVIGATOR", "SOUTHERN CROSS", "CORAL SEA",
        "BASS STRAIT", "GREAT BARRIER", "IRON MONARCH",
        "CAPE YORK", "ENDEAVOUR BAY", "ALBATROSS SPIRIT",
    ]
    rng = random.Random(int(hashlib.md5(unloco.encode()).hexdigest(), 16))
    n_inbound = rng.randint(2, 3)
    # Shuffle a copy of the pool seeded by UNLOCO, then slice — guarantees
    # no duplicate names within a single port's inbound set.
    name_pool = list(_INBOUND_NAMES)
    rng.shuffle(name_pool)
    for j in range(n_inbound):
        fake_mmsi = f"SIM-{unloco}-{j}"
        props = _sim_vessel_props(fake_mmsi, now)
        eta_dt = now + timedelta(hours=rng.uniform(2, 36))
        eta_str = eta_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        etd_dt  = eta_dt + timedelta(hours=rng.uniform(8, 48))
        # Deliberately assign inbound vessels to an ALREADY OCCUPIED berth so
        # they create realistic berth-handover conflict decisions (arriving
        # vessel vs departing incumbent).  Use the j-th claimed berth so each
        # inbound gets a different berth and conflicts are spread across the port.
        claimed_list = [b for b in assignable if b["id"] in claimed_berths]
        berth = claimed_list[j % len(claimed_list)] if claimed_list else None
        inbound_name = name_pool[j]
        # R4.4 — Representative tug-demand estimate for synthetic-inbound
        # arrivals, computed against the same port `towage_rule` the
        # AIS path uses. NOT an operational towage requirement.
        _inbound_n_tugs = _n_tugs_for(_towage_rule_for_unloco(unloco), {
            "name":        inbound_name,
            "loa_m":       props["loa"],
            "vessel_type": props["vessel_type"],
        })
        vessels_out.append({
            "id":              fake_mmsi,
            "name":            inbound_name,
            "mmsi":            fake_mmsi,
            "imo":             None,
            "status":          "confirmed" if rng.random() > 0.4 else "expected",
            "berth_id":        berth["id"]   if berth else None,
            "berth":           berth["name"] if berth else None,
            "eta":             eta_str,
            "etd":             etd_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "loa":             props["loa"],
            "beam":            props["beam"],
            "draught":         props["draught"],
            "vessel_type":     props["vessel_type"],
            "flag":            props["flag"],
            "n_tugs":          _inbound_n_tugs,
            "towage_required": _inbound_n_tugs >= 1,
            "destination":     None,
            "at_anchorage":    False,
            "source":          "sim",
        })

    return vessels_out
