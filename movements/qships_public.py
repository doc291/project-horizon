"""
movements/qships_public.py — QShipsPublicMovementsAdapter (CONFIRMED_PUBLISHED).

Wraps the EXISTING Queensland public scrape (qships_scraper.py, which POSTs to
the webx JSON data service /webx/services/wxdata.svc/GetDataX) and adapts its
transformed vessel rows into normalised MovementRecords with
CONFIRMED_PUBLISHED provenance.

This is the public surface. The credentialed SOAP surface
(PublicWebServicesWS.asmx) is a FUTURE extension — see the
`SOAP_EXTENSION_POINT` note below. QShips is NEVER labelled CONFIRMED_SYSTEM in
this slice.

Testability: `fetch` accepts an injectable `rows_provider` so tests can supply
canned vessel rows without any network call. The default provider calls the
live qships_scraper.
"""

from __future__ import annotations

from typing import Callable, List, Optional, Sequence

from authority.categories import AuthorityCategory
from authority.provenance import SourceProvenance
from movements.base import (
    MovementRecord,
    MovementFetchResult,
    SERVED_LIVE,
    SERVED_LAST_GOOD,
    SERVED_NONE,
    ARRIVAL,
    DEPARTURE,
    IN_PORT,
)
from movements.normalise import normalise_name

_SOURCE_NAME = "QShips (public)"

# ── FUTURE EXTENSION POINT ───────────────────────────────────────────────────
# QShips SOAP (PublicWebServicesWS.asmx) would be a SEPARATE adapter
# (QShipsSoapAdapter) emitting CONFIRMED_SYSTEM *only* after a successful
# authenticated Login→SessionKey→GetVoyageExportData call. It would fall back to
# THIS public adapter when credentials are absent or auth fails. Not built in
# Slice 2A. Do not label QShips CONFIRMED_SYSTEM until that adapter exists and a
# real authenticated call succeeds.
SOAP_EXTENSION_POINT = "QShipsSoapAdapter (CONFIRMED_SYSTEM) — future, credential-gated"


def _status_to_movement(status: str) -> tuple:
    """Map a qships status string to (movement_type, is_actual)."""
    s = (status or "").strip().lower()
    if s in ("berthed", "in port", "alongside"):
        return (IN_PORT, True)
    if s in ("sailed", "departed", "departure"):
        return (DEPARTURE, True)
    if s in ("arrived",):
        return (ARRIVAL, True)
    # expected / scheduled / confirmed / at_risk → expected arrival
    return (ARRIVAL, False)


class QShipsPublicMovementsAdapter:
    """Concrete PublicShipMovementsAdapter wrapping the QShips public scrape."""

    def __init__(self, rows_provider: Optional[Callable[[], Sequence[dict]]] = None):
        # rows_provider returns a list of qships vessel dicts (already transformed).
        self._rows_provider = rows_provider
        self._last_good: Optional[MovementFetchResult] = None

    def source_name(self) -> str:
        return _SOURCE_NAME

    def authority_category(self) -> AuthorityCategory:
        return AuthorityCategory.CONFIRMED_PUBLISHED

    def _default_rows(self) -> Sequence[dict]:
        """Live path — pull transformed rows from the existing qships scraper.
        Imported lazily so importing this module never triggers a scrape."""
        import qships_scraper
        if qships_scraper.run_scrape():
            # qships_scraper caches its latest vessels internally; expose them.
            getter = getattr(qships_scraper, "latest_vessels", None)
            if callable(getter):
                return getter() or []
        return []

    def fetch(self, profile: dict, now: float) -> MovementFetchResult:
        provider = self._rows_provider or self._default_rows
        try:
            rows = list(provider())
            records = self._rows_to_records(rows, now)
            result = MovementFetchResult(
                records=tuple(records), ok=True, stale=False,
                source_updated_at=now, served_from=SERVED_LIVE,
            )
            self._last_good = result
            return result
        except Exception as exc:  # never raise out of fetch
            if self._last_good is not None:
                lg = self._last_good
                return MovementFetchResult(
                    records=lg.records, ok=False, stale=True,
                    source_updated_at=lg.source_updated_at,
                    served_from=SERVED_LAST_GOOD, error=str(exc),
                )
            return MovementFetchResult(ok=False, served_from=SERVED_NONE, error=str(exc))

    def _rows_to_records(self, rows: Sequence[dict], now: float) -> List[MovementRecord]:
        out: List[MovementRecord] = []
        for r in rows:
            name = (r.get("name") or r.get("vessel_name") or "").strip()
            if not name:
                continue
            movement_type, is_actual = _status_to_movement(r.get("status", ""))
            prov = SourceProvenance(
                category=AuthorityCategory.CONFIRMED_PUBLISHED,
                source=_SOURCE_NAME,
                observed_at=now,
                detail=("actual" if is_actual else "expected"),
            )
            # Departure and in-port records surface the forward ETD; arrivals the ETA.
            if movement_type in (DEPARTURE, IN_PORT):
                event_time = r.get("etd") or r.get("eta")
            else:
                event_time = r.get("eta")
            out.append(MovementRecord(
                vessel_name=name,
                normalised_name=normalise_name(name),
                movement_type=movement_type,
                is_actual=is_actual,
                provenance=prov,
                event_time=event_time,
                from_loc=r.get("from") or None,
                to_loc=r.get("to") or None,
                berth=r.get("berth_name") or r.get("berth") or None,
                agent=r.get("agent") or None,
                status=r.get("status") or None,
            ))
        return out
