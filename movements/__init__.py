"""
movements/ — Public ship-movement adapters (Beta 11, Slice 2A).

A generic, reusable adapter pattern that turns published ship-movement feeds
into normalised `MovementRecord` objects, each carrying a Slice-1
`authority.SourceProvenance`. First concrete implementations:

  - PortsVictoriaMovementsAdapter  (CONFIRMED_PUBLISHED) — real 5-table page
  - QShipsPublicMovementsAdapter    (CONFIRMED_PUBLISHED) — webx JSON scrape

QShips SOAP (CONFIRMED_SYSTEM, credential-gated) is NOT implemented in this
slice — only a clearly-marked extension point is preserved (see qships_public).

This slice is standalone and unit-tested. It is NOT wired into /api/summary or
the Decision Card UI, and it does not modify AISStream, MST, or any Beta 10
surface.
"""

from movements.base import (
    MovementRecord,
    MovementFetchResult,
    PublicShipMovementsAdapter,
)
from movements.ports_victoria import PortsVictoriaMovementsAdapter
from movements.qships_public import QShipsPublicMovementsAdapter

__all__ = [
    "MovementRecord",
    "MovementFetchResult",
    "PublicShipMovementsAdapter",
    "PortsVictoriaMovementsAdapter",
    "QShipsPublicMovementsAdapter",
]
