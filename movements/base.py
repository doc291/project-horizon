"""
movements/base.py — Generic movement adapter interface + data models.

`MovementRecord`     — one normalised ship-movement event with provenance.
`MovementFetchResult`— the result of an adapter fetch (records + status).
`PublicShipMovementsAdapter` — the interface every public-feed adapter implements.

All models are immutable. Records always carry a SourceProvenance from the
Slice-1 authority package, so downstream consumers can score authority without
re-deriving where the data came from.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, Protocol, Tuple, runtime_checkable

from authority.categories import AuthorityCategory
from authority.provenance import SourceProvenance


# Movement type constants (closed set)
ARRIVAL = "arrival"
DEPARTURE = "departure"
IN_PORT = "in_port"
MOVEMENT_TYPES = frozenset({ARRIVAL, DEPARTURE, IN_PORT})

# served_from constants
SERVED_LIVE = "live"
SERVED_CACHE = "cache"
SERVED_LAST_GOOD = "last_good"
SERVED_NONE = "none"


@dataclass(frozen=True)
class MovementRecord:
    """One normalised ship-movement event."""

    vessel_name: str
    normalised_name: str
    movement_type: str            # ARRIVAL | DEPARTURE | IN_PORT
    is_actual: bool               # True = actual/confirmed event; False = expected/estimated
    provenance: SourceProvenance
    event_time: Optional[str] = None   # ISO-8601 UTC ("…Z"); None if unparseable
    from_loc: Optional[str] = None
    to_loc: Optional[str] = None
    berth: Optional[str] = None
    agent: Optional[str] = None
    status: Optional[str] = None

    def __post_init__(self):
        if self.movement_type not in MOVEMENT_TYPES:
            raise ValueError(f"invalid movement_type {self.movement_type!r}")


@dataclass(frozen=True)
class MovementFetchResult:
    """Outcome of an adapter fetch. Never raises out of an adapter — failures
    are represented here so the caller is never blocked."""

    records: Tuple[MovementRecord, ...] = field(default_factory=tuple)
    ok: bool = True
    stale: bool = False
    source_updated_at: Optional[float] = None   # epoch seconds (feed's own timestamp)
    served_from: str = SERVED_LIVE
    error: Optional[str] = None

    @property
    def count(self) -> int:
        return len(self.records)


@runtime_checkable
class PublicShipMovementsAdapter(Protocol):
    """Interface for a public ship-movements feed adapter.

    Implementations MUST NOT raise out of `fetch` — they return a
    MovementFetchResult with ok=False on failure (optionally serving a
    last-good snapshot with stale=True). This guarantees the eventual
    /api/summary caller is never blocked by a feed outage.
    """

    def source_name(self) -> str: ...

    def authority_category(self) -> AuthorityCategory: ...

    def fetch(self, profile: dict, now: float) -> MovementFetchResult: ...
