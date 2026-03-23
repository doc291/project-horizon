from pydantic import BaseModel
from typing import Optional, List
from enum import Enum
from datetime import datetime


class VesselStatus(str, Enum):
    SCHEDULED = "scheduled"
    CONFIRMED = "confirmed"
    ARRIVED = "arrived"
    BERTHED = "berthed"
    DEPARTED = "departed"
    AT_RISK = "at_risk"
    DELAYED = "delayed"


class BerthStatus(str, Enum):
    AVAILABLE = "available"
    OCCUPIED = "occupied"
    RESERVED = "reserved"
    MAINTENANCE = "maintenance"


class EventStatus(str, Enum):
    SCHEDULED = "scheduled"
    CONFIRMED = "confirmed"
    EN_ROUTE = "en_route"
    COMPLETED = "completed"
    CANCELLED = "cancelled"


class ConflictSeverity(str, Enum):
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class ConflictType(str, Enum):
    BERTH_OVERLAP = "berth_overlap"
    BERTH_NOT_READY = "berth_not_ready"
    PILOTAGE_WINDOW = "pilotage_window"
    TOWAGE_RESOURCE = "towage_resource"
    ETA_VARIANCE = "eta_variance"
    DEPARTURE_DELAY = "departure_delay"


class GuidancePriority(str, Enum):
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    INFO = "info"


class Vessel(BaseModel):
    id: str
    name: str
    imo: str
    vessel_type: str
    flag: str
    loa: float          # Length overall in metres
    draught: float      # Draught in metres
    cargo_type: str
    status: VesselStatus
    berth_id: Optional[str] = None
    eta: datetime
    etd: datetime
    ata: Optional[datetime] = None   # Actual time of arrival
    atd: Optional[datetime] = None   # Actual time of departure
    pilotage_required: bool = True
    towage_required: bool = True
    agent: str
    notes: Optional[str] = None


class Berth(BaseModel):
    id: str
    name: str
    terminal: str
    max_loa: float
    max_draught: float
    status: BerthStatus
    readiness_time: Optional[datetime] = None   # When berth will be clear/ready
    crane_count: int = 0
    notes: Optional[str] = None


class PilotageEvent(BaseModel):
    id: str
    vessel_id: str
    vessel_name: str
    pilot_name: str
    scheduled_time: datetime
    boarding_station: str
    direction: str    # "inbound" | "outbound"
    status: EventStatus
    notes: Optional[str] = None


class TugAssignment(BaseModel):
    tug_id: str
    tug_name: str


class TowageEvent(BaseModel):
    id: str
    vessel_id: str
    vessel_name: str
    tugs: List[TugAssignment]
    scheduled_time: datetime
    direction: str    # "arrival" | "departure"
    status: EventStatus
    notes: Optional[str] = None


class Conflict(BaseModel):
    id: str
    conflict_type: ConflictType
    severity: ConflictSeverity
    vessel_ids: List[str]
    vessel_names: List[str]
    berth_id: Optional[str] = None
    berth_name: Optional[str] = None
    conflict_time: datetime
    description: str
    resolution_options: List[str]
    created_at: datetime
    data_source: Optional[str] = "simulated"   # "live" | "simulated"


class Guidance(BaseModel):
    id: str
    priority: GuidancePriority
    message: str
    detail: str
    vessel_id: Optional[str] = None
    vessel_name: Optional[str] = None
    action_required: bool
    deadline: Optional[datetime] = None
    created_at: datetime


class PortStatus(BaseModel):
    berths_occupied: int
    berths_available: int
    berths_total: int
    vessels_in_port: int
    vessels_expected_24h: int
    vessels_departing_24h: int
    active_conflicts: int
    critical_conflicts: int
    pilots_available: int
    tugs_available: int


class OperationalSummary(BaseModel):
    port_name: str
    generated_at: datetime
    lookahead_hours: int
    port_status: PortStatus
    vessels: List[Vessel]
    berths: List[Berth]
    pilotage: List[PilotageEvent]
    towage: List[TowageEvent]
    conflicts: List[Conflict]
    guidance: List[Guidance]
    data_source: Optional[str] = None     # "qships" | "mock"
    scraped_at: Optional[str] = None      # ISO UTC timestamp from qships_data.json
