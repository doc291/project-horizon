#!/usr/bin/env python3
"""
Project Horizon — Beta 3
Self-contained server: zero external dependencies, pure Python 3 stdlib.

Usage (local):
    python3 server.py

Then open http://localhost:8000 in your browser.
"""

import json
import uuid
import random
import hashlib
import math
import os
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

PORT = int(os.environ.get("PORT", 8000))
INDEX_HTML = Path(__file__).parent / "index.html"
LOGO_FILE  = Path(__file__).parent / "logo.svg"

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
# Fictional Port of Northhaven — mapped onto Port of Brisbane / Fisherman Islands

PORT_GEO = {
    "center":  {"lat": -27.383, "lon": 153.173},
    "zoom":    13,
    "berths": {
        # North Terminal — container/ro-ro quay, north face of the island
        "B01": {"lat": -27.368, "lon": 153.150, "terminal": "North Terminal", "heading": 350},
        "B02": {"lat": -27.369, "lon": 153.161, "terminal": "North Terminal", "heading": 350},
        "B03": {"lat": -27.370, "lon": 153.172, "terminal": "North Terminal", "heading": 350},
        # South Terminal — bulk/general cargo, south face of the island
        "B04": {"lat": -27.397, "lon": 153.157, "terminal": "South Terminal", "heading": 170},
        "B05": {"lat": -27.398, "lon": 153.167, "terminal": "South Terminal", "heading": 170},
        "B06": {"lat": -27.399, "lon": 153.177, "terminal": "South Terminal", "heading": 170},
    },
    "anchorage": {
        "lat": -27.352, "lon": 153.253,
        "radius_km": 2.5,
        "label": "Northhaven Anchorage",
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
    Uses a deterministic jitter per vessel so positions are stable across refreshes.
    """
    coords = PORT_GEO["berths"]
    jlat, jlon = stable_jitter(v["id"])

    if v["status"] in ("berthed", "arrived"):
        c = coords.get(v["berth_id"])
        if c:
            return {"lat": c["lat"] + jlat * 0.3, "lon": c["lon"] + jlon * 0.3}

    eta  = isoparse(v["eta"])
    hrs  = (eta - now).total_seconds() / 3600

    if hrs < 0:
        # Should be berthed — fallback to port center
        c = coords.get(v["berth_id"])
        if c:
            return {"lat": c["lat"] + jlat * 0.3, "lon": c["lon"] + jlon * 0.3}
        return {"lat": -27.383 + jlat, "lon": 153.173 + jlon}
    elif hrs < 2.5:
        # Near pilot boarding ground
        pbg = PORT_GEO["pilot_boarding_ground"]
        return {"lat": pbg["lat"] + jlat, "lon": pbg["lon"] + jlon}
    elif hrs < 8:
        # Mid-channel / inbound
        return {"lat": -27.366 + jlat, "lon": 153.200 + jlon * 2}
    elif hrs < 24:
        # Anchorage
        anc = PORT_GEO["anchorage"]
        return {"lat": anc["lat"] + jlat * 2, "lon": anc["lon"] + jlon * 2}
    else:
        # Offshore — further into Moreton Bay
        return {"lat": -27.345 + jlat * 3, "lon": 153.280 + jlon * 3}

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
    raw = [
        ("B01", "Berth 1",  "North Terminal", 350, 14.5, "occupied",     4, now + timedelta(hours=4)),
        ("B02", "Berth 2",  "North Terminal", 300, 13.0, "occupied",     4, now + timedelta(hours=8)),
        ("B03", "Berth 3",  "North Terminal", 250, 11.5, "reserved",     2, now + timedelta(hours=2)),
        ("B04", "Berth 4",  "South Terminal", 320, 14.0, "available",    3, None),
        ("B05", "Berth 5",  "South Terminal", 280, 12.5, "maintenance",  0, now + timedelta(hours=20)),
        ("B06", "Berth 6",  "South Terminal", 220, 10.0, "occupied",     0, now + timedelta(hours=6)),
    ]
    result = []
    for bid, name, terminal, loa, draught, status, cranes, ready in raw:
        geo = PORT_GEO["berths"].get(bid, {})
        result.append({
            "id": bid, "name": name, "terminal": terminal,
            "max_loa": loa, "max_draught": draught,
            "lat_depth_m": BERTH_LAT_DEPTHS.get(bid, 12.0),
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
    status = ("critical" if mn["ukc_m"] < 0.5 else
              "warning"  if mn["ukc_m"] < 1.0 else "good")
    return {
        "min_ukc_m":      mn["ukc_m"],
        "critical_vessel": mn["vessel_name"],
        "critical_berth":  mn["berth_id"],
        "status":          status,
        "all":             entries,
    }


def make_vessels(now: datetime) -> list:
    specs = [
        # id    name                      imo       type             flag               loa    dr   cargo         berth  eta_h  etd_h  status
        ("V001","MV Nordic Star",         "9123456","Container",    "Norway",           240, 12.0,"Containers",  "B01",-18,  -18+22, "berthed"),
        ("V002","MV Atlantic Pioneer",    "9234567","Bulk Carrier", "Panama",           190, 10.5,"Grain",       "B02", -6,   -6+14, "berthed"),
        ("V003","MV Baltic Carrier",      "9345678","Tanker",       "Liberia",          180,  9.5,"Crude Oil",   "B06",-12,  -12+18, "berthed"),
        ("V004","MV Oceanic Trader",      "9456789","Container",   "Bahamas",           220, 11.5,"Containers",  "B03",  3,      19, "confirmed"),
        ("V005","MV Horizon Scout",       "9567890","General Cargo","Cyprus",           160,  8.5,"Steel Coils", "B04",  5,      17, "confirmed"),
        ("V006","MV Cape Venture",        "9678901","Bulk Carrier", "Marshall Islands", 200, 11.0,"Coal",        "B01",  7,      27, "scheduled"),
        # V007 arrives before B04 clear — critical berth conflict with V005
        ("V007","MV Northern Light",      "9789012","RoRo",         "Norway",           185,  7.5,"Vehicles",    "B04",  2,      10, "at_risk"),
        ("V008","MV Pacific Mariner",     "9890123","Tanker",       "Panama",           175,  9.0,"Crude Oil",   "B06", 10,      28, "scheduled"),
        ("V009","MV Southern Cross",      "9901234","Container",    "Liberia",          260, 13.0,"Containers",  "B02", 12,      36, "scheduled"),
        # V010 arrives 1h before V004 departs B03 — high berth conflict
        ("V010","MV Eastern Spirit",      "9012345","Bulk Carrier", "Bahamas",          195, 10.5,"Grain",       "B03", 18,      40, "scheduled"),
        ("V011","MV Western Passage",     "9112233","General Cargo","Cyprus",           145,  7.5,"Fertiliser",  "B04", 26,      40, "scheduled"),
        ("V012","MV Iron Meridian",       "9223344","Container",    "Liberia",          230, 12.0,"Containers",  "B01", 30,      48, "scheduled"),
        ("V013","MV Coral Bay",           "9334455","Bulk Carrier", "Marshall Islands", 170,  9.0,"Coal",        "B06", 36,      56, "scheduled"),
    ]
    vessels = []
    for vid, name, imo, vtype, flag, loa, dr, cargo, berth_id, eta_h, etd_h, status in specs:
        eta = now + timedelta(hours=eta_h)
        etd = now + timedelta(hours=etd_h)
        ata = eta if status in ("berthed", "arrived") else None
        v = {
            "id": vid, "name": name, "imo": imo,
            "vessel_type": vtype, "flag": flag,
            "loa": loa, "draught": dr, "cargo_type": cargo,
            "status": status, "berth_id": berth_id,
            "eta": fmt(eta), "etd": fmt(etd),
            "ata": fmt(ata) if ata else None, "atd": None,
            "pilotage_required": True,
            "towage_required": loa > 170,
            "agent": AGENTS[int(hashlib.md5(vid.encode()).hexdigest(), 16) % len(AGENTS)],
            "notes": "ETA variance +2.5h reported by agent" if status == "at_risk" else None,
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
               sequencing_alternatives=None, decision_support=None):
    return {
        "id": cid,
        "conflict_type": ctype,
        "signal_type": signal_type,        # CONFLICT | WARNING | ADVISORY
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


def detect_conflicts(vessels, berths, pilotage, towage, now):
    conflicts = []

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
            ))

    sev_order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
    conflicts.sort(key=lambda c: (sev_order.get(c["severity"], 9), c["conflict_time"]))

    # Return up to 5 most important conflicts (as requested for Beta 1)
    return conflicts[:5]


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

def make_weather():
    now  = utcnow()
    seed = f"weather-{now.strftime('%Y%m%d')}-{now.hour // 3}"
    h    = hashlib.md5(seed.encode()).hexdigest()

    wind_kts  = 6  + int(h[0:2],  16) % 18        # 6–24 kts
    wind_deg  =       int(h[2:6],  16) % 360
    swell_m   = round(0.4 + (int(h[6:8],   16) % 18) / 10.0, 1)   # 0.4–2.2 m
    swell_per = 6  + int(h[8:10],  16) % 9         # 6–14 s
    vis_nm    = 5  + int(h[10:12], 16) % 12        # 5–16 nm
    pressure  = 1007 + int(h[12:14], 16) % 18      # 1007–1025 hPa

    wind_lbl  = _COMPASS[round(wind_deg  / 22.5) % 16]
    swell_lbl = _COMPASS[(round(wind_deg / 22.5) + 2) % 16]

    bft = (1 if wind_kts < 4 else 2 if wind_kts < 7 else 3 if wind_kts < 11
           else 4 if wind_kts < 17 else 5 if wind_kts < 22 else 6)

    cond = ("Excellent" if wind_kts < 10 and swell_m < 1.0 else
            "Good"      if wind_kts < 16 and swell_m < 1.5 else
            "Moderate"  if wind_kts < 22 and swell_m < 2.0 else "Poor")

    return {
        "wind_speed_kts":       wind_kts,
        "wind_direction_deg":   wind_deg,
        "wind_direction_label": wind_lbl,
        "wind_beaufort":        bft,
        "swell_height_m":       swell_m,
        "swell_period_s":       swell_per,
        "swell_direction_label": swell_lbl,
        "visibility_nm":        vis_nm,
        "pressure_hpa":         pressure,
        "conditions":           cond,
    }


def make_tides():
    now      = utcnow()
    PERIOD   = 12.42   # semidiurnal hours
    MEAN     = 2.1     # m above chart datum
    AMP      = 1.65    # amplitude

    # Deterministic phase offset per calendar day
    day_h    = hashlib.md5(f"tide-{now.strftime('%Y%m%d')}".encode()).hexdigest()
    phase_h  = (int(day_h[0:4], 16) % int(PERIOD * 100)) / 100.0   # 0–12.42 h

    t        = (now.hour + now.minute / 60.0 + phase_h) % PERIOD
    height   = round(MEAN + AMP * math.cos(2 * math.pi * t / PERIOD), 2)
    deriv    = -AMP * (2 * math.pi / PERIOD) * math.sin(2 * math.pi * t / PERIOD)

    if abs(deriv) < 0.06:
        state = "Slack"
    elif deriv > 0:
        state = "Rising"
    else:
        state = "Falling"

    # Time to next HW (cos peak, t % P == 0) and LW (t % P == P/2)
    t_to_hw = (PERIOD      - t) % PERIOD
    t_to_lw = (PERIOD / 2  - t) % PERIOD

    if t_to_hw <= t_to_lw:
        next_type, next_ht, next_h = "HW", round(MEAN + AMP, 1), t_to_hw
    else:
        next_type, next_ht, next_h = "LW", round(MEAN - AMP, 1), t_to_lw

    # Flag shallow berths when tide is near LLW
    berth_restrictions = ["B03", "B06"] if height < (MEAN - AMP + 1.1) else []

    return {
        "current_height_m":      height,
        "state":                 state,
        "next_event_type":       next_type,
        "next_event_time":       fmt(now + timedelta(hours=next_h)),
        "next_event_height_m":   next_ht,
        "mean_height_m":         MEAN,
        "amplitude_m":           AMP,
        "berth_restrictions":    berth_restrictions,
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


# ── Summary builder ────────────────────────────────────────────────────────────

def build_summary():
    now = utcnow()
    berths   = make_berths(now)
    vessels  = make_vessels(now)
    pilotage = make_pilotage(vessels, now)
    towage   = make_towage(vessels, now)
    conflicts = detect_conflicts(vessels, berths, pilotage, towage, now)
    guidance  = build_guidance(conflicts, vessels, berths, pilotage, towage, now)
    weather   = make_weather()
    tides     = make_tides()

    # Beta 3 additions
    berth_util = make_berth_utilisation(vessels, berths, now)
    etd_risk   = compute_etd_risk(vessels, conflicts, weather, tides)
    dashboard  = make_dashboard(vessels, berths, conflicts, pilotage, towage,
                                weather, tides, etd_risk, berth_util, now)
    ukc        = compute_ukc(vessels, berths, tides["current_height_m"])

    occupied   = sum(1 for b in berths if b["status"] in ("occupied", "reserved"))
    available  = sum(1 for b in berths if b["status"] == "available")
    in_port    = sum(1 for v in vessels if v["status"] == "berthed")
    exp_24     = sum(1 for v in vessels
                     if v["status"] not in ("berthed", "departed")
                     and (isoparse(v["eta"]) - now).total_seconds() / 3600 <= 24)
    dep_24     = sum(1 for v in vessels
                     if v["status"] == "berthed"
                     and (isoparse(v["etd"]) - now).total_seconds() / 3600 <= 24)
    critical   = sum(1 for c in conflicts if c["severity"] == "critical")

    return {
        "port_name": "Port of Northhaven",
        "generated_at": fmt(now),
        "lookahead_hours": 48,
        "port_status": {
            "berths_occupied": occupied,
            "berths_available": available,
            "berths_total": len(berths),
            "vessels_in_port": in_port,
            "vessels_expected_24h": exp_24,
            "vessels_departing_24h": dep_24,
            "active_conflicts": len(conflicts),
            "critical_conflicts": critical,
            "pilots_available": 3,
            "tugs_available": 4,
        },
        "vessels":          vessels,
        "berths":           berths,
        "pilotage":         pilotage,
        "towage":           towage,
        "conflicts":        conflicts,
        "guidance":         guidance,
        "port_geo":         PORT_GEO,
        "weather":          weather,
        "tides":            tides,
        "berth_utilisation": berth_util,
        "etd_risk":          etd_risk,
        "dashboard":         dashboard,
        "ukc":               ukc,
    }


# ── HTTP handler ──────────────────────────────────────────────────────────────

class HorizonHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        if args and str(args[1]) not in ("200", "304"):
            super().log_message(format, *args)

    def do_GET(self):
        path = self.path.split("?")[0]
        if path == "/api/summary":
            self._json(build_summary())
        elif path == "/health":
            self._json({"status": "ok", "time": fmt(utcnow())})
        elif path in ("/", "/index.html"):
            self._html()
        elif path == "/logo":
            self._logo()
        else:
            self.send_error(404)

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
        # Serve logo.svg, or any logo.png/jpg if present alongside it
        for candidate, mime in [
            (LOGO_FILE,                              "image/svg+xml"),
            (LOGO_FILE.with_suffix(".png"),          "image/png"),
            (LOGO_FILE.with_suffix(".jpg"),          "image/jpeg"),
            (LOGO_FILE.with_suffix(".jpeg"),         "image/jpeg"),
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


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    server = ThreadingHTTPServer(("0.0.0.0", PORT), HorizonHandler)
    print(f"╔══════════════════════════════════════╗")
    print(f"║   Project Horizon  —  Beta 3         ║")
    print(f"╠══════════════════════════════════════╣")
    print(f"║  http://localhost:{PORT}               ║")
    print(f"║  Press Ctrl+C to stop               ║")
    print(f"╚══════════════════════════════════════╝")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nServer stopped.")
