"""
authority/ — Operational Authority overlay (Beta 11, Slice 1 foundation).

Pure, additive, side-effect-free building blocks for tagging every data
element that enters Horizon with its operational authority: which category
it belongs to, where it came from (provenance), how fresh it is, and a
basic per-element authority score.

This slice deliberately does NOT wire into /api/summary, the Decision Card
UI, AISStream, or MST. It is the foundation only — importable and unit-
tested in isolation. Nothing here changes Beta 10 behaviour.

Public surface:
  authority.categories  — AuthorityCategory enum + weights
  authority.provenance  — SourceProvenance dataclass + ASSUMED fallback
  authority.freshness   — freshness_factor() decay
  authority.scoring     — element_score(), aggregate_scores()
"""

from authority.categories import AuthorityCategory
from authority.provenance import SourceProvenance, assumed, provenance_or_assumed
from authority.freshness import freshness_factor
from authority.scoring import element_score, aggregate_scores

__all__ = [
    "AuthorityCategory",
    "SourceProvenance",
    "assumed",
    "provenance_or_assumed",
    "freshness_factor",
    "element_score",
    "aggregate_scores",
]
