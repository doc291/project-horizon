"""
Conflict detection engine.
Scans the current operational picture and returns a list of Conflict objects.
"""
from datetime import datetime, timedelta, timezone
from typing import List
import uuid

from models import (
    Vessel, VesselStatus, Berth, BerthStatus,
    PilotageEvent, TowageEvent,
    Conflict, ConflictType, ConflictSeverity,
)

BERTH_CLEARANCE_MINUTES = 60   # Minimum gap needed between departure and next arrival
PILOTAGE_WINDOW_HOURS = 2      # Minimum hours notice needed for pilotage
TOWAGE_RESOURCE_LIMIT = 2      # Max simultaneous towage ops per tug


def _now() -> datetime:
    return datetime.now(tz=timezone.utc)


def _overlap(start_a: datetime, end_a: datetime, start_b: datetime, end_b: datetime) -> bool:
    return start_a < end_b and start_b < end_a


def detect_berth_overlaps(vessels: List[Vessel]) -> List[Conflict]:
    """Two vessels assigned to the same berth at overlapping times."""
    conflicts = []
    n = _now()

    by_berth: dict = {}
    for v in vessels:
        if v.berth_id and v.status not in (VesselStatus.DEPARTED,):
            by_berth.setdefault(v.berth_id, []).append(v)

    for berth_id, bvessels in by_berth.items():
        for i in range(len(bvessels)):
            for j in range(i + 1, len(bvessels)):
                a, b = bvessels[i], bvessels[j]
                # Use actual times where available
                a_start = a.ata or a.eta
                a_end = a.atd or a.etd
                b_start = b.ata or b.eta
                b_end = b.atd or b.etd
                # Add clearance buffer to the earlier departure
                if a_start < b_start:
                    a_end_buffered = a_end + timedelta(minutes=BERTH_CLEARANCE_MINUTES)
                    if _overlap(a_start, a_end_buffered, b_start, b_end):
                        gap_minutes = int((b_start - a_end).total_seconds() / 60)
                        severity = ConflictSeverity.CRITICAL if gap_minutes < 0 else ConflictSeverity.HIGH
                        conflicts.append(Conflict(
                            id=str(uuid.uuid4())[:8],
                            conflict_type=ConflictType.BERTH_OVERLAP,
                            severity=severity,
                            vessel_ids=[a.id, b.id],
                            vessel_names=[a.name, b.name],
                            berth_id=berth_id,
                            berth_name=berth_id,
                            conflict_time=b_start,
                            description=(
                                f"{b.name} is scheduled to arrive at {berth_id} "
                                f"only {gap_minutes}min after {a.name} departs. "
                                f"Minimum clearance required: {BERTH_CLEARANCE_MINUTES}min."
                            ),
                            resolution_options=[
                                f"Delay {b.name} arrival by {BERTH_CLEARANCE_MINUTES - gap_minutes + 15}min",
                                f"Bring forward {a.name} departure by {BERTH_CLEARANCE_MINUTES - gap_minutes + 15}min",
                                f"Reassign {b.name} to an alternative berth",
                            ],
                            created_at=n,
                        ))
    return conflicts


def detect_berth_readiness(vessels: List[Vessel], berths: List[Berth]) -> List[Conflict]:
    """Vessel arriving before its assigned berth will be ready."""
    conflicts = []
    n = _now()
    berth_map = {b.id: b for b in berths}

    for v in vessels:
        if v.status in (VesselStatus.SCHEDULED, VesselStatus.CONFIRMED, VesselStatus.AT_RISK):
            if not v.berth_id:
                continue
            berth = berth_map.get(v.berth_id)
            if not berth or not berth.readiness_time:
                continue
            if berth.readiness_time > v.eta:
                gap_minutes = int((berth.readiness_time - v.eta).total_seconds() / 60)
                severity = ConflictSeverity.HIGH if gap_minutes > 60 else ConflictSeverity.MEDIUM
                conflicts.append(Conflict(
                    id=str(uuid.uuid4())[:8],
                    conflict_type=ConflictType.BERTH_NOT_READY,
                    severity=severity,
                    vessel_ids=[v.id],
                    vessel_names=[v.name],
                    berth_id=berth.id,
                    berth_name=berth.name,
                    conflict_time=v.eta,
                    description=(
                        f"{v.name} ETA is {v.eta.strftime('%H:%M')} UTC but "
                        f"{berth.name} will not be ready until "
                        f"{berth.readiness_time.strftime('%H:%M')} UTC "
                        f"(gap: {gap_minutes}min)."
                    ),
                    resolution_options=[
                        f"Hold {v.name} at anchorage for {gap_minutes}min",
                        f"Accelerate departure of current vessel at {berth.name}",
                        f"Assign {v.name} to an alternative berth",
                    ],
                    created_at=n,
                ))
    return conflicts


def detect_pilotage_window(vessels: List[Vessel], pilotage: List[PilotageEvent]) -> List[Conflict]:
    """Pilotage event scheduled with insufficient notice."""
    conflicts = []
    n = _now()
    pilot_map = {p.vessel_id + p.direction[0]: p for p in pilotage}

    for v in vessels:
        if v.status in (VesselStatus.SCHEDULED, VesselStatus.AT_RISK):
            key = v.id + "i"  # inbound
            p = pilot_map.get(key)
            if p:
                hours_notice = (p.scheduled_time - n).total_seconds() / 3600
                if 0 < hours_notice < PILOTAGE_WINDOW_HOURS:
                    conflicts.append(Conflict(
                        id=str(uuid.uuid4())[:8],
                        conflict_type=ConflictType.PILOTAGE_WINDOW,
                        severity=ConflictSeverity.HIGH,
                        vessel_ids=[v.id],
                        vessel_names=[v.name],
                        conflict_time=p.scheduled_time,
                        description=(
                            f"Pilotage for {v.name} is in "
                            f"{hours_notice:.1f}h — below the {PILOTAGE_WINDOW_HOURS}h "
                            f"minimum notice window. Pilot: {p.pilot_name}."
                        ),
                        resolution_options=[
                            f"Confirm pilot availability with {p.pilot_name} immediately",
                            f"Request stand-by pilot cover",
                            f"Delay {v.name} ETA to allow full notice period",
                        ],
                        created_at=n,
                    ))
    return conflicts


def detect_towage_contention(towage: List[TowageEvent]) -> List[Conflict]:
    """Same tug assigned to overlapping operations."""
    conflicts = []
    n = _now()
    op_duration = timedelta(hours=2)

    tug_ops: dict = {}
    for event in towage:
        for tug in event.tugs:
            tug_ops.setdefault(tug.tug_id, []).append(event)

    for tug_id, events in tug_ops.items():
        for i in range(len(events)):
            for j in range(i + 1, len(events)):
                a, b = events[i], events[j]
                if _overlap(a.scheduled_time, a.scheduled_time + op_duration,
                             b.scheduled_time, b.scheduled_time + op_duration):
                    tug_name = a.tugs[0].tug_name
                    conflicts.append(Conflict(
                        id=str(uuid.uuid4())[:8],
                        conflict_type=ConflictType.TOWAGE_RESOURCE,
                        severity=ConflictSeverity.MEDIUM,
                        vessel_ids=[a.vessel_id, b.vessel_id],
                        vessel_names=[a.vessel_name, b.vessel_name],
                        conflict_time=b.scheduled_time,
                        description=(
                            f"{tug_name} is assigned to {a.vessel_name} "
                            f"({a.direction}) at "
                            f"{a.scheduled_time.strftime('%H:%M')} and also to "
                            f"{b.vessel_name} ({b.direction}) at "
                            f"{b.scheduled_time.strftime('%H:%M')} — operations overlap."
                        ),
                        resolution_options=[
                            f"Reassign a spare tug to {b.vessel_name}",
                            f"Adjust one operation time to avoid overlap",
                            "Request tug availability from tug operator",
                        ],
                        created_at=n,
                    ))
    return conflicts


def detect_eta_variance(vessels: List[Vessel]) -> List[Conflict]:
    """Vessels with AT_RISK status indicating significant ETA uncertainty."""
    conflicts = []
    n = _now()

    for v in vessels:
        if v.status == VesselStatus.AT_RISK:
            conflicts.append(Conflict(
                id=str(uuid.uuid4())[:8],
                conflict_type=ConflictType.ETA_VARIANCE,
                severity=ConflictSeverity.MEDIUM,
                vessel_ids=[v.id],
                vessel_names=[v.name],
                berth_id=v.berth_id,
                conflict_time=v.eta,
                description=(
                    f"{v.name} has reported a significant ETA variance. "
                    f"Scheduled ETA {v.eta.strftime('%d %b %H:%M')} UTC may not be reliable. "
                    f"{v.notes or ''}"
                ),
                resolution_options=[
                    "Request updated ETA from ship's agent",
                    "Place dependent pilotage and towage on standby",
                    "Notify berth terminal of potential schedule shift",
                ],
                created_at=n,
            ))
    return conflicts


def run_all(
    vessels: List[Vessel],
    berths: List[Berth],
    pilotage: List[PilotageEvent],
    towage: List[TowageEvent],
) -> List[Conflict]:
    conflicts = []
    conflicts += detect_berth_overlaps(vessels)
    conflicts += detect_berth_readiness(vessels, berths)
    conflicts += detect_pilotage_window(vessels, pilotage)
    conflicts += detect_towage_contention(towage)
    conflicts += detect_eta_variance(vessels)

    # Sort: critical first, then by time
    severity_order = {
        ConflictSeverity.CRITICAL: 0,
        ConflictSeverity.HIGH: 1,
        ConflictSeverity.MEDIUM: 2,
        ConflictSeverity.LOW: 3,
    }
    conflicts.sort(key=lambda c: (severity_order[c.severity], c.conflict_time))
    return conflicts
