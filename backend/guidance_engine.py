"""
Operational guidance engine.
Converts the conflict list and operational picture into prioritised guidance items.
"""
from datetime import datetime, timedelta, timezone
from typing import List
import uuid

from models import (
    Vessel, VesselStatus, Berth, BerthStatus,
    PilotageEvent, TowageEvent,
    Conflict, ConflictType, ConflictSeverity,
    Guidance, GuidancePriority,
)


def _now() -> datetime:
    return datetime.now(tz=timezone.utc)


def _priority_from_severity(severity: ConflictSeverity) -> GuidancePriority:
    return {
        ConflictSeverity.CRITICAL: GuidancePriority.CRITICAL,
        ConflictSeverity.HIGH: GuidancePriority.HIGH,
        ConflictSeverity.MEDIUM: GuidancePriority.MEDIUM,
        ConflictSeverity.LOW: GuidancePriority.INFO,
    }[severity]


def guidance_from_conflicts(conflicts: List[Conflict]) -> List[Guidance]:
    items = []
    n = _now()
    for c in conflicts:
        items.append(Guidance(
            id=str(uuid.uuid4())[:8],
            priority=_priority_from_severity(c.severity),
            message=_short_message(c),
            detail=c.description,
            vessel_id=c.vessel_ids[0] if c.vessel_ids else None,
            vessel_name=c.vessel_names[0] if c.vessel_names else None,
            action_required=c.severity in (ConflictSeverity.CRITICAL, ConflictSeverity.HIGH),
            deadline=c.conflict_time - timedelta(hours=1) if c.severity == ConflictSeverity.CRITICAL else None,
            created_at=n,
        ))
    return items


def proactive_guidance(
    vessels: List[Vessel],
    berths: List[Berth],
    pilotage: List[PilotageEvent],
    towage: List[TowageEvent],
) -> List[Guidance]:
    """Surfaced guidance not directly tied to a conflict — upcoming key events."""
    items = []
    n = _now()
    window = timedelta(hours=4)

    # Vessels arriving within 4 hours — readiness check
    for v in vessels:
        if v.status in (VesselStatus.CONFIRMED, VesselStatus.SCHEDULED, VesselStatus.AT_RISK):
            if n < v.eta < n + window:
                hours_away = (v.eta - n).total_seconds() / 3600
                items.append(Guidance(
                    id=str(uuid.uuid4())[:8],
                    priority=GuidancePriority.HIGH if hours_away < 2 else GuidancePriority.MEDIUM,
                    message=f"{v.name} arriving in {hours_away:.1f}h",
                    detail=(
                        f"{v.name} ({v.vessel_type}, LOA {v.loa}m) is expected at "
                        f"{v.eta.strftime('%H:%M')} UTC. "
                        f"Berth: {v.berth_id or 'TBA'}. "
                        f"Pilotage: {'required' if v.pilotage_required else 'not required'}. "
                        f"Towage: {'required' if v.towage_required else 'not required'}."
                    ),
                    vessel_id=v.id, vessel_name=v.name,
                    action_required=hours_away < 2,
                    deadline=v.eta - timedelta(hours=1),
                    created_at=n,
                ))

    # Vessels departing within 4 hours
    for v in vessels:
        if v.status == VesselStatus.BERTHED:
            if n < v.etd < n + window:
                hours_away = (v.etd - n).total_seconds() / 3600
                items.append(Guidance(
                    id=str(uuid.uuid4())[:8],
                    priority=GuidancePriority.MEDIUM,
                    message=f"{v.name} departing in {hours_away:.1f}h",
                    detail=(
                        f"{v.name} is scheduled to depart {v.berth_id} at "
                        f"{v.etd.strftime('%H:%M')} UTC. "
                        f"Ensure outbound pilotage and towage are confirmed."
                    ),
                    vessel_id=v.id, vessel_name=v.name,
                    action_required=False,
                    deadline=v.etd - timedelta(minutes=30),
                    created_at=n,
                ))

    # Maintenance berth coming available
    for b in berths:
        if b.status == BerthStatus.MAINTENANCE and b.readiness_time:
            if n < b.readiness_time < n + timedelta(hours=12):
                items.append(Guidance(
                    id=str(uuid.uuid4())[:8],
                    priority=GuidancePriority.INFO,
                    message=f"{b.name} returning from maintenance {b.readiness_time.strftime('%H:%M')} UTC",
                    detail=(
                        f"{b.name} ({b.terminal}) is expected to complete maintenance at "
                        f"{b.readiness_time.strftime('%H:%M')} UTC and will become available "
                        f"for scheduling."
                    ),
                    action_required=False,
                    created_at=n,
                ))

    return items


def _short_message(c: Conflict) -> str:
    if c.conflict_type == ConflictType.BERTH_OVERLAP:
        return f"Berth conflict: {c.vessel_names[0]} / {c.vessel_names[1]} at {c.berth_name}"
    elif c.conflict_type == ConflictType.BERTH_NOT_READY:
        return f"Berth not ready: {c.vessel_names[0]} arrives before {c.berth_name} clears"
    elif c.conflict_type == ConflictType.PILOTAGE_WINDOW:
        return f"Short pilotage notice: {c.vessel_names[0]}"
    elif c.conflict_type == ConflictType.TOWAGE_RESOURCE:
        return f"Tug double-booked: {c.vessel_names[0]} & {c.vessel_names[1]}"
    elif c.conflict_type == ConflictType.ETA_VARIANCE:
        return f"ETA uncertainty: {c.vessel_names[0]}"
    return c.description[:60]


def build_guidance(
    conflicts: List[Conflict],
    vessels: List[Vessel],
    berths: List[Berth],
    pilotage: List[PilotageEvent],
    towage: List[TowageEvent],
) -> List[Guidance]:
    conflict_guidance = guidance_from_conflicts(conflicts)
    proactive = proactive_guidance(vessels, berths, pilotage, towage)
    all_guidance = conflict_guidance + proactive

    priority_order = {
        GuidancePriority.CRITICAL: 0,
        GuidancePriority.HIGH: 1,
        GuidancePriority.MEDIUM: 2,
        GuidancePriority.INFO: 3,
    }
    all_guidance.sort(key=lambda g: (priority_order[g.priority], g.deadline or g.created_at))
    return all_guidance
