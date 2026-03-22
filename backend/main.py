"""
Project Horizon — FastAPI backend
Serves operational data, conflicts, and guidance for the coordination layer.
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime, timezone

from models import OperationalSummary, PortStatus
import mock_data
import conflict_engine
import guidance_engine

app = FastAPI(
    title="Project Horizon API",
    description="Predictive maritime coordination platform — Release 1",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def _now():
    return datetime.now(tz=timezone.utc)


@app.get("/health")
def health():
    return {"status": "ok", "time": _now().isoformat()}


@app.get("/api/summary", response_model=OperationalSummary)
def get_summary():
    """
    Full operational snapshot: vessels, berths, pilotage, towage,
    conflicts, and guidance — everything the UI needs in one call.
    """
    berths, vessels, pilotage, towage = mock_data.generate_all()
    conflicts = conflict_engine.run_all(vessels, berths, pilotage, towage)
    guidance = guidance_engine.build_guidance(conflicts, vessels, berths, pilotage, towage)

    occupied = sum(1 for b in berths if b.status.value in ("occupied", "reserved"))
    available = sum(1 for b in berths if b.status.value == "available")

    from models import VesselStatus
    in_port = sum(1 for v in vessels if v.status == VesselStatus.BERTHED)
    expected_24h = sum(1 for v in vessels
                       if v.status not in (VesselStatus.BERTHED, VesselStatus.DEPARTED)
                       and (v.eta - _now()).total_seconds() / 3600 <= 24)
    departing_24h = sum(1 for v in vessels
                        if v.status == VesselStatus.BERTHED
                        and (v.etd - _now()).total_seconds() / 3600 <= 24)
    critical = sum(1 for c in conflicts if c.severity.value == "critical")

    from models import ConflictSeverity
    port_status = PortStatus(
        berths_occupied=occupied,
        berths_available=available,
        berths_total=len(berths),
        vessels_in_port=in_port,
        vessels_expected_24h=expected_24h,
        vessels_departing_24h=departing_24h,
        active_conflicts=len(conflicts),
        critical_conflicts=critical,
        pilots_available=3,
        tugs_available=4,
    )

    return OperationalSummary(
        port_name="Port of Northhaven",
        generated_at=_now(),
        lookahead_hours=48,
        port_status=port_status,
        vessels=vessels,
        berths=berths,
        pilotage=pilotage,
        towage=towage,
        conflicts=conflicts,
        guidance=guidance,
    )


@app.get("/api/vessels")
def get_vessels():
    _, vessels, _, _ = mock_data.generate_all()
    return vessels


@app.get("/api/berths")
def get_berths():
    berths, _, _, _ = mock_data.generate_all()
    return berths


@app.get("/api/conflicts")
def get_conflicts():
    berths, vessels, pilotage, towage = mock_data.generate_all()
    return conflict_engine.run_all(vessels, berths, pilotage, towage)


@app.get("/api/guidance")
def get_guidance():
    berths, vessels, pilotage, towage = mock_data.generate_all()
    conflicts = conflict_engine.run_all(vessels, berths, pilotage, towage)
    return guidance_engine.build_guidance(conflicts, vessels, berths, pilotage, towage)
