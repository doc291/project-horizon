"""
movements/matching.py — Match MovementRecords to live AIS vessels.

Tiered matching:
  1. exact      — vessel_name equals an AIS vessel name (case-insensitive trim)
  2. normalised — normalise_name() keys match (drops prefix/punctuation/case)
  3. unresolved — no AIS match → a schedule-only record (lower position authority)

A future IMO/MMSI enrichment hook is defined but intentionally NOT implemented
in this slice (raises NotImplementedError so accidental use is loud).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence

from movements.base import MovementRecord
from movements.normalise import normalise_name

# match methods
MATCH_EXACT = "exact"
MATCH_NORMALISED = "normalised"
MATCH_UNRESOLVED = "unresolved"


@dataclass(frozen=True)
class MatchResult:
    method: str                       # MATCH_EXACT | MATCH_NORMALISED | MATCH_UNRESOLVED
    matched: bool
    ais_vessel: Optional[dict] = None  # the matched AIS vessel dict, or None

    @property
    def is_schedule_only(self) -> bool:
        return not self.matched


def _ais_name(v: dict) -> str:
    return (v.get("name") or v.get("vessel_name") or "").strip()


def match_movement_to_ais(
    record: MovementRecord,
    ais_vessels: Sequence[dict],
) -> MatchResult:
    """Match one movement record against a list of AIS vessel dicts."""
    target = (record.vessel_name or "").strip().lower()
    # Tier 1 — exact (case-insensitive)
    for v in ais_vessels:
        if _ais_name(v).lower() == target and target:
            return MatchResult(MATCH_EXACT, True, v)

    # Tier 2 — normalised
    rkey = record.normalised_name or normalise_name(record.vessel_name)
    if rkey:
        for v in ais_vessels:
            if normalise_name(_ais_name(v)) == rkey:
                return MatchResult(MATCH_NORMALISED, True, v)

    # Tier 3 — unresolved (schedule-only)
    return MatchResult(MATCH_UNRESOLVED, False, None)


def enrich_with_identifiers(*args, **kwargs):  # noqa: D401 - intentional stub
    """FUTURE EXTENSION POINT — IMO/MMSI enrichment of schedule-only records.

    Not implemented in Slice 2A. Defined so the integration seam exists and is
    documented, but calling it is a loud error rather than a silent no-op."""
    raise NotImplementedError(
        "IMO/MMSI enrichment is a future slice — not implemented in Slice 2A"
    )
