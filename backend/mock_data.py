"""
Generates realistic mock operational data for a mid-sized port.
All times are relative to "now" so the app stays fresh across sessions.
"""
import random
from datetime import datetime, timedelta, timezone
from typing import List

from models import (
    Vessel, VesselStatus, Berth, BerthStatus,
    PilotageEvent, TowageEvent, TugAssignment, EventStatus
)

# Seed for reproducibility within a session
random.seed(42)

# ── Static reference data ────────────────────────────────────────────────────

VESSEL_NAMES = [
    "MV Nordic Star", "MV Atlantic Pioneer", "MV Baltic Carrier",
    "MV Oceanic Trader", "MV Horizon Scout", "MV Cape Venture",
    "MV Northern Light", "MV Pacific Mariner", "MV Southern Cross",
    "MV Eastern Spirit", "MV Western Passage", "MV Iron Meridian",
    "MV Coral Bay", "MV Amber Wave", "MV Steel Current",
]

VESSEL_TYPES = ["Container", "Bulk Carrier", "Tanker", "RoRo", "General Cargo"]
CARGO_TYPES = ["Containers", "Grain", "Crude Oil", "Vehicles", "Steel Coils", "Coal", "Fertiliser"]
FLAGS = ["Norway", "Panama", "Liberia", "Marshall Islands", "Bahamas", "Cyprus"]
AGENTS = ["Wilhelmsen Ships Service", "Inchcape Shipping", "GAC", "Svitzer", "Norton Lilly"]
PILOT_NAMES = ["Capt. Andersen", "Capt. Müller", "Capt. Johansson", "Capt. O'Brien", "Capt. Kowalski"]
BOARDING_STATIONS = ["Outer Pilot Station", "Inner Pilot Station", "North Channel Anchorage"]
TUG_NAMES = ["TUG Stallion", "TUG Hercules", "TUG Neptune", "TUG Samson", "TUG Trident"]


def now_utc() -> datetime:
    return datetime.now(tz=timezone.utc).replace(microsecond=0)


def make_berths() -> List[Berth]:
    """Six berths across two terminals."""
    berths_raw = [
        ("B01", "Berth 1", "North Terminal", 350.0, 14.5, BerthStatus.OCCUPIED, 4),
        ("B02", "Berth 2", "North Terminal", 300.0, 13.0, BerthStatus.OCCUPIED, 4),
        ("B03", "Berth 3", "North Terminal", 250.0, 11.5, BerthStatus.RESERVED, 2),
        ("B04", "Berth 4", "South Terminal", 320.0, 14.0, BerthStatus.AVAILABLE, 3),
        ("B05", "Berth 5", "South Terminal", 280.0, 12.5, BerthStatus.MAINTENANCE, 0),
        ("B06", "Berth 6", "South Terminal", 220.0, 10.0, BerthStatus.OCCUPIED, 0),
    ]
    result = []
    n = now_utc()
    for bid, name, terminal, loa, draught, status, cranes in berths_raw:
        readiness = None
        if status == BerthStatus.OCCUPIED:
            # Departing in 2–10 hours
            readiness = n + timedelta(hours=random.randint(2, 10))
        elif status == BerthStatus.MAINTENANCE:
            readiness = n + timedelta(hours=random.randint(12, 36))
        result.append(Berth(
            id=bid, name=name, terminal=terminal,
            max_loa=loa, max_draught=draught, status=status,
            readiness_time=readiness, crane_count=cranes,
        ))
    return result


def make_vessels(berths: List[Berth]) -> List[Vessel]:
    """Build a realistic 48-hour vessel list."""
    n = now_utc()
    vessels = []

    # Currently berthed vessels (will depart in a few hours)
    berthed_specs = [
        ("V001", "MV Nordic Star",     "9123456", "Container",    "Norway",          240.0, 12.0, "Containers", "B01",
         n - timedelta(hours=18), n + timedelta(hours=4),  VesselStatus.BERTHED),
        ("V002", "MV Atlantic Pioneer","9234567", "Bulk Carrier",  "Panama",          190.0, 10.5, "Grain",      "B02",
         n - timedelta(hours=6),  n + timedelta(hours=8),  VesselStatus.BERTHED),
        ("V003", "MV Baltic Carrier",  "9345678", "Tanker",        "Liberia",         180.0,  9.5, "Crude Oil",  "B06",
         n - timedelta(hours=12), n + timedelta(hours=6),  VesselStatus.BERTHED),
    ]

    for vid, name, imo, vtype, flag, loa, draught, cargo, berth_id, eta, etd, status in berthed_specs:
        vessels.append(Vessel(
            id=vid, name=name, imo=imo, vessel_type=vtype, flag=flag,
            loa=loa, draught=draught, cargo_type=cargo, status=status,
            berth_id=berth_id, eta=eta, etd=etd, ata=eta,
            pilotage_required=True, towage_required=(loa > 180),
            agent=random.choice(AGENTS),
        ))

    # Inbound vessels (arriving in next 48h)
    inbound_specs = [
        # (id, name, imo, type, flag, loa, draught, cargo, berth_id, eta_offset_h, stay_h, status)
        ("V004", "MV Oceanic Trader",   "9456789", "Container",   "Bahamas",         220.0, 11.5, "Containers", "B03",  3,  16, VesselStatus.CONFIRMED),
        ("V005", "MV Horizon Scout",    "9567890", "General Cargo","Cyprus",          160.0,  8.5, "Steel Coils","B04",  5,  12, VesselStatus.CONFIRMED),
        ("V006", "MV Cape Venture",     "9678901", "Bulk Carrier", "Marshall Islands",200.0, 11.0, "Coal",       "B01",  7,  20, VesselStatus.SCHEDULED),
        ("V007", "MV Northern Light",   "9789012", "RoRo",         "Norway",          185.0,  7.5, "Vehicles",   "B04",  2,   8, VesselStatus.AT_RISK),   # arriving before V005 departs
        ("V008", "MV Pacific Mariner",  "9890123", "Tanker",       "Panama",          175.0,  9.0, "Crude Oil",  "B06", 10,  18, VesselStatus.SCHEDULED),
        ("V009", "MV Southern Cross",   "9901234", "Container",    "Liberia",         260.0, 13.0, "Containers", "B02", 12,  24, VesselStatus.SCHEDULED),
        ("V010", "MV Eastern Spirit",   "9012345", "Bulk Carrier", "Bahamas",         195.0, 10.5, "Grain",      "B03", 18,  22, VesselStatus.SCHEDULED),
        ("V011", "MV Western Passage",  "9112233", "General Cargo","Cyprus",          145.0,  7.5, "Fertiliser", "B04", 26,  14, VesselStatus.SCHEDULED),
        ("V012", "MV Iron Meridian",    "9223344", "Container",    "Liberia",         230.0, 12.0, "Containers", "B01", 30,  18, VesselStatus.SCHEDULED),
        ("V013", "MV Coral Bay",        "9334455", "Bulk Carrier", "Marshall Islands",170.0,  9.0, "Coal",       "B06", 36,  20, VesselStatus.SCHEDULED),
    ]

    for vid, name, imo, vtype, flag, loa, draught, cargo, berth_id, eta_h, stay_h, status in inbound_specs:
        eta = n + timedelta(hours=eta_h)
        etd = eta + timedelta(hours=stay_h)
        vessels.append(Vessel(
            id=vid, name=name, imo=imo, vessel_type=vtype, flag=flag,
            loa=loa, draught=draught, cargo_type=cargo, status=status,
            berth_id=berth_id, eta=eta, etd=etd,
            pilotage_required=True, towage_required=(loa > 170),
            agent=random.choice(AGENTS),
            notes="ETA variance +2.5h reported by agent" if status == VesselStatus.AT_RISK else None,
        ))

    return vessels


def make_pilotage(vessels: List[Vessel]) -> List[PilotageEvent]:
    events = []
    pilot_pool = list(PILOT_NAMES)
    used_pilots: dict = {}   # pilot -> datetime when free

    inbound = [v for v in vessels if v.status not in (VesselStatus.BERTHED, VesselStatus.DEPARTED)]
    outbound = [v for v in vessels if v.status == VesselStatus.BERTHED]

    for v in inbound:
        pilot = random.choice(pilot_pool)
        scheduled = v.eta - timedelta(hours=1, minutes=30)
        status = EventStatus.CONFIRMED if v.status == VesselStatus.CONFIRMED else EventStatus.SCHEDULED
        events.append(PilotageEvent(
            id=f"PIL-{v.id}-IN",
            vessel_id=v.id, vessel_name=v.name,
            pilot_name=pilot, scheduled_time=scheduled,
            boarding_station=random.choice(BOARDING_STATIONS),
            direction="inbound", status=status,
        ))

    for v in outbound:
        pilot = random.choice(pilot_pool)
        scheduled = v.etd - timedelta(hours=1)
        events.append(PilotageEvent(
            id=f"PIL-{v.id}-OUT",
            vessel_id=v.id, vessel_name=v.name,
            pilot_name=pilot, scheduled_time=scheduled,
            boarding_station=random.choice(BOARDING_STATIONS),
            direction="outbound", status=EventStatus.SCHEDULED,
        ))

    return events


def make_towage(vessels: List[Vessel]) -> List[TowageEvent]:
    events = []
    tug_pool = list(TUG_NAMES)

    eligible = [v for v in vessels if v.towage_required]

    for v in eligible:
        n_tugs = 2 if v.loa > 200 else 1
        tugs = random.sample(tug_pool, min(n_tugs, len(tug_pool)))

        if v.status == VesselStatus.BERTHED:
            # Departure towage
            events.append(TowageEvent(
                id=f"TOW-{v.id}-DEP",
                vessel_id=v.id, vessel_name=v.name,
                tugs=[TugAssignment(tug_id=f"TUG-{t[:3].upper()}", tug_name=t) for t in tugs],
                scheduled_time=v.etd - timedelta(minutes=45),
                direction="departure",
                status=EventStatus.SCHEDULED,
            ))
        else:
            # Arrival towage
            events.append(TowageEvent(
                id=f"TOW-{v.id}-ARR",
                vessel_id=v.id, vessel_name=v.name,
                tugs=[TugAssignment(tug_id=f"TUG-{t[:3].upper()}", tug_name=t) for t in tugs],
                scheduled_time=v.eta - timedelta(minutes=30),
                direction="arrival",
                status=EventStatus.SCHEDULED if v.status == VesselStatus.SCHEDULED else EventStatus.CONFIRMED,
            ))

    return events


def generate_all():
    berths = make_berths()
    vessels = make_vessels(berths)
    pilotage = make_pilotage(vessels)
    towage = make_towage(vessels)
    return berths, vessels, pilotage, towage
