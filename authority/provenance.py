"""
authority/provenance.py — Source provenance for authority-tagged elements.

A `SourceProvenance` records, for one data element:
  - category    : which AuthorityCategory it belongs to
  - source      : human-readable origin ("AISStream", "BOM", "MST", "Simulation")
  - observed_at : epoch seconds the element was observed/produced (None if the
                  element is not time-stamped — e.g. a static assumption)
  - detail      : optional free-text note

The module also provides the fail-safe path: when category or source is
missing, callers should degrade to ASSUMED rather than silently trusting an
untagged element. `provenance_or_assumed` enforces that.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from authority.categories import AuthorityCategory

# Source label used when an ASSUMED element has no better origin.
ASSUMED_SOURCE = "unspecified"


@dataclass(frozen=True)
class SourceProvenance:
    """Immutable provenance record for one authority-tagged data element."""

    category: AuthorityCategory
    source: str
    observed_at: Optional[float] = None
    detail: Optional[str] = None

    @property
    def is_time_stamped(self) -> bool:
        return self.observed_at is not None

    @property
    def is_assumed(self) -> bool:
        return self.category is AuthorityCategory.ASSUMED


def assumed(source: Optional[str] = None, detail: Optional[str] = None) -> SourceProvenance:
    """Build an ASSUMED provenance (the authority floor)."""
    return SourceProvenance(
        category=AuthorityCategory.ASSUMED,
        source=source or ASSUMED_SOURCE,
        observed_at=None,
        detail=detail,
    )


def provenance_or_assumed(
    *,
    category: Optional[AuthorityCategory] = None,
    source: Optional[str] = None,
    observed_at: Optional[float] = None,
    detail: Optional[str] = None,
) -> SourceProvenance:
    """Construct a provenance, falling back to ASSUMED when category or source
    is missing. This is the safe entry point — an untagged element never gets
    a high-authority category by default."""
    if category is None or not source:
        return assumed(source=source, detail=detail or "missing source/category")
    return SourceProvenance(
        category=category,
        source=source,
        observed_at=observed_at,
        detail=detail,
    )
