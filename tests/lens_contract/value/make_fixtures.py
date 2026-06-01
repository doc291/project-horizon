#!/usr/bin/env python3
"""
LV0 — generator for engineered value-acceptance fixtures.

Produces six fixtures under tests/lens_contract/fixtures/value/ by taking the
existing wo_MELBOURNE.json fixture (a complete 27-key /api/summary snapshot)
and surgically mutating ONLY the arrays that create each engineered condition.
Everything else is inherited verbatim, so every fixture is a structurally
complete summary the renderers can consume without crashing.

Timestamps are authored RELATIVE to each fixture's `generated_at` anchor.
The value harness (value_acceptance.test.js) rebases every ISO timestamp to
runtime `now` before building, so the engineered conditions land inside the
live 8h watch / 12h shift / 24h forward windows regardless of when the test
runs. `hrs_to_eta` values are authored to match their intended offsets.

This generator is a test asset. Re-run to regenerate:
    python3 tests/lens_contract/value/make_fixtures.py

It does NOT touch any application/runtime file.
"""
import json, os, copy
from datetime import datetime, timedelta, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
BASE = os.path.join(ROOT, "tests", "lens_contract", "fixtures", "wo_MELBOURNE.json")
OUT  = os.path.join(ROOT, "tests", "lens_contract", "fixtures", "value")

os.makedirs(OUT, exist_ok=True)

with open(BASE) as f:
    base = json.load(f)

ANCHOR = datetime.fromisoformat(base["generated_at"].replace("Z", "+00:00"))

def iso(dt_offset_hours):
    """ISO timestamp at ANCHOR + offset hours."""
    return (ANCHOR + timedelta(hours=dt_offset_hours)).strftime("%Y-%m-%dT%H:%M:%SZ")

def vessel(vid, name, status, berth, loa, vtype, eta_h, etd_h, pil=True, tow=True):
    return {
        "id": vid, "name": name, "imo": "9000000", "vessel_type": vtype,
        "flag": "Singapore", "loa": loa, "draught": 11.0, "cargo_type": "Cargo",
        "status": status, "berth_id": berth,
        "eta": iso(eta_h), "etd": iso(etd_h),
        "ata": iso(eta_h) if status == "berthed" else None, "atd": None,
        "pilotage_required": pil, "towage_required": tow,
        "agent": "Test Agent", "notes": None,
        "lat": -37.84, "lon": 144.92,
    }

def write(name, fixture, meta):
    fixture["_value_meta"] = meta
    path = os.path.join(OUT, name)
    with open(path, "w") as f:
        json.dump(fixture, f, indent=2)
    print(f"  wrote {name}  ({len(json.dumps(fixture))} bytes)  — {meta['engineers']}")

# ── pil_tide_closing.json ──────────────────────────────────────────────────
# One in-watch inbound transit whose UKC is TIGHT and whose tide window
# closes inside the forward range. Exercises VAL-P4 (forward pressure derived).
fx = copy.deepcopy(base)
fx["vessels"] = [
    vessel("VT01", "TIDE RUNNER", "confirmed", "B02", 230.0, "Bulk carrier", 0.5, 14.0),
]
fx["pilotage"] = [
    {"id": "PIL-VT01-IN", "vessel_id": "VT01", "vessel_name": "TIDE RUNNER",
     "pilot_name": "PILOT_MEL_PSP_01", "scheduled_time": iso(0.5),
     "boarding_station": "Point Lonsdale Pilot Station",
     "direction": "inbound", "status": "confirmed"},
]
fx["towage"] = []
fx["arrival_ukc"] = {
    "min_ukc_m": 0.4, "critical_vessel": "TIDE RUNNER", "critical_berth": "B02",
    "critical_eta": iso(0.5), "hrs_to_eta": 0.5, "status": "TIGHT",
    "all": [
        {"vessel_id": "VT01", "vessel_name": "TIDE RUNNER", "berth_id": "B02",
         "eta": iso(0.5), "hrs_to_eta": 0.5, "ukc_m": 0.4,
         "predicted_tide_m": 0.6, "available_depth_m": 10.4, "vessel_draught_m": 10.0,
         "status": "TIGHT"},
    ],
}
# Tide HW (window peak) at +1.5h, then falling — the closing window.
fx["tides"] = dict(base["tides"])
fx["tides"]["state"] = "Rising"
fx["tides"]["next_event_type"] = "HW"
fx["tides"]["next_event_time"] = iso(1.5)
write("pil_tide_closing.json", fx, {
    "engineers": "in-watch TIGHT-UKC transit + closing tide window",
    "targets": ["VAL-P1", "VAL-P2", "VAL-P3", "VAL-P4", "VAL-P5"],
})

# ── pil_cluster.json ───────────────────────────────────────────────────────
# Three boardings stacked inside a ~45-min window at +9h (forward 8-24h range).
# Exercises VAL-P4 cluster-stacking forward pressure.
fx = copy.deepcopy(base)
fx["vessels"] = [
    vessel("VC01", "CLUSTER ONE",   "confirmed", "B01", 200.0, "Container ship", 9.0, 20.0),
    vessel("VC02", "CLUSTER TWO",   "confirmed", "B03", 210.0, "Container ship", 9.25, 21.0),
    vessel("VC03", "CLUSTER THREE", "confirmed", "B05", 195.0, "Bulk carrier",   9.5, 22.0),
    # plus one in-watch transit so Section B is non-empty
    vessel("VC04", "WATCH LEAD",    "confirmed", "B02", 180.0, "Tanker",         1.0, 13.0),
]
fx["pilotage"] = [
    {"id": "PIL-VC04-IN", "vessel_id": "VC04", "vessel_name": "WATCH LEAD",
     "pilot_name": "PILOT_MEL_PSP_02", "scheduled_time": iso(1.0),
     "boarding_station": "Point Lonsdale Pilot Station", "direction": "inbound", "status": "confirmed"},
    {"id": "PIL-VC01-IN", "vessel_id": "VC01", "vessel_name": "CLUSTER ONE",
     "pilot_name": "PILOT_MEL_PSP_01", "scheduled_time": iso(9.0),
     "boarding_station": "Point Lonsdale Pilot Station", "direction": "inbound", "status": "confirmed"},
    {"id": "PIL-VC02-IN", "vessel_id": "VC02", "vessel_name": "CLUSTER TWO",
     "pilot_name": "PILOT_MEL_PSP_03", "scheduled_time": iso(9.25),
     "boarding_station": "Point Lonsdale Pilot Station", "direction": "inbound", "status": "confirmed"},
    {"id": "PIL-VC03-IN", "vessel_id": "VC03", "vessel_name": "CLUSTER THREE",
     "pilot_name": "PILOT_MEL_PSP_04", "scheduled_time": iso(9.5),
     "boarding_station": "Point Lonsdale Pilot Station", "direction": "inbound", "status": "confirmed"},
]
fx["towage"] = []
fx["arrival_ukc"] = {"min_ukc_m": None, "critical_vessel": None, "critical_berth": None,
                     "critical_eta": None, "hrs_to_eta": None, "status": "OK", "all": []}
write("pil_cluster.json", fx, {
    "engineers": "3 boardings stacked in a 45-min forward window (+9h)",
    "targets": ["VAL-P1", "VAL-P4", "VAL-P5"],
})

# ── tow_clash.json ─────────────────────────────────────────────────────────
# Two in-shift jobs SHARING tug 'SVR Apex' at overlapping times (+2h, +2h10m).
# Exercises VAL-T2 (positive) clash detection.
fx = copy.deepcopy(base)
fx["vessels"] = [
    vessel("VK01", "CLASH ALPHA", "berthed",   "B01", 260.0, "Tanker", -2.0, 2.0),
    vessel("VK02", "CLASH BETA",  "confirmed", "B02", 255.0, "Bulk carrier", 2.2, 16.0),
]
apex = {"tug_id": "SVR-APEX", "tug_name": "SVR Apex"}
fx["towage"] = [
    {"id": "TOW-VK01-DEP", "vessel_id": "VK01", "vessel_name": "CLASH ALPHA",
     "tugs": [apex], "scheduled_time": iso(2.0), "direction": "departure", "status": "scheduled"},
    {"id": "TOW-VK02-ARR", "vessel_id": "VK02", "vessel_name": "CLASH BETA",
     "tugs": [apex], "scheduled_time": iso(2.17), "direction": "arrival", "status": "confirmed"},
]
fx["pilotage"] = []
write("tow_clash.json", fx, {
    "engineers": "two in-shift jobs share SVR Apex at overlapping times (+2h, +2h10m)",
    "targets": ["VAL-T1", "VAL-T2(positive)", "VAL-T5"],
})

# ── tow_noclash.json ───────────────────────────────────────────────────────
# Same two jobs at the SAME times but DIFFERENT tugs → no clash.
# Exercises VAL-T2 (negative).
fx = copy.deepcopy(base)
fx["vessels"] = [
    vessel("VN01", "CLEAR ALPHA", "berthed",   "B01", 260.0, "Tanker", -2.0, 2.0),
    vessel("VN02", "CLEAR BETA",  "confirmed", "B02", 255.0, "Bulk carrier", 2.2, 16.0),
]
fx["towage"] = [
    {"id": "TOW-VN01-DEP", "vessel_id": "VN01", "vessel_name": "CLEAR ALPHA",
     "tugs": [{"tug_id": "SVR-APEX", "tug_name": "SVR Apex"}],
     "scheduled_time": iso(2.0), "direction": "departure", "status": "scheduled"},
    {"id": "TOW-VN02-ARR", "vessel_id": "VN02", "vessel_name": "CLEAR BETA",
     "tugs": [{"tug_id": "SVR-MERCURY", "tug_name": "SVR Mercury"}],
     "scheduled_time": iso(2.17), "direction": "arrival", "status": "confirmed"},
]
fx["pilotage"] = []
write("tow_noclash.json", fx, {
    "engineers": "two in-shift jobs at overlapping times but on DIFFERENT tugs (no clash)",
    "targets": ["VAL-T2(negative)"],
})

# ── tow_downtug.json ───────────────────────────────────────────────────────
# Five-tug fleet (so _assignTugStatus marks one MAINTENANCE + one STANDBY) with
# one in-shift job. Exercises VAL-T3 (down-tug impact rendered with return/impact).
fx = copy.deepcopy(base)
fx["vessels"] = [
    vessel("VD01", "DOWN TUG JOB", "berthed", "B01", 240.0, "Bulk carrier", -2.0, 3.0),
]
fx["towage"] = [
    {"id": "TOW-VD01-DEP", "vessel_id": "VD01", "vessel_name": "DOWN TUG JOB",
     "tugs": [{"tug_id": "SVR-APEX", "tug_name": "SVR Apex"}],
     "scheduled_time": iso(3.0), "direction": "departure", "status": "scheduled"},
]
fx["pilotage"] = []
# port_tugs inherited from base (5 tugs) → _assignTugStatus marks a MAINTENANCE tug.
write("tow_downtug.json", fx, {
    "engineers": "5-tug fleet (one assigned MAINTENANCE by _assignTugStatus) + 1 job",
    "targets": ["VAL-T3", "VAL-T5"],
})

# ── tow_demand_peak.json ───────────────────────────────────────────────────
# Six jobs clustered at +14h (forward 12-24h range), each needing tugs; fleet=5.
# Demand (≥6 simultaneous tug-assignments) exceeds capacity. Exercises VAL-T4.
fx = copy.deepcopy(base)
fx["vessels"] = []
fx["towage"] = []
for i in range(6):
    vid = f"VP{i:02d}"
    fx["vessels"].append(
        vessel(vid, f"PEAK {i}", "confirmed", f"B0{(i%6)+1}", 250.0, "Bulk carrier", 14.0 + i*0.1, 26.0))
    fx["towage"].append(
        {"id": f"TOW-{vid}-ARR", "vessel_id": vid, "vessel_name": f"PEAK {i}",
         "tugs": [{"tug_id": "SVR-APEX", "tug_name": "SVR Apex"},
                  {"tug_id": "SVR-MERCURY", "tug_name": "SVR Mercury"}],
         "scheduled_time": iso(14.0 + i*0.1), "direction": "arrival", "status": "confirmed"})
fx["pilotage"] = []
write("tow_demand_peak.json", fx, {
    "engineers": "6 jobs clustered at +14h each needing 2 tugs; fleet=5 (demand>capacity)",
    "targets": ["VAL-T4"],
})

print("\nAll 6 value fixtures generated under tests/lens_contract/fixtures/value/")
