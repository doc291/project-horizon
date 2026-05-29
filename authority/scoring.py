"""
authority/scoring.py — Basic authority scoring helpers (Beta 11, Slice 1).

Pure functions combining category weight with freshness decay into a single
per-element authority score, plus a simple aggregation mean. This is the
"basic scoring helpers" foundation only — the full four-factor Decision-Card
authority model (data authority × freshness × completeness × dependency
coverage) is a later slice and is NOT implemented here.

All functions take `now` explicitly; no clock reads, no global mutable state.
"""

from __future__ import annotations

from types import MappingProxyType
from typing import Optional, Sequence

from authority.categories import AuthorityCategory
from authority.provenance import SourceProvenance
from authority.freshness import freshness_factor


# Default freshness half-lives (seconds) per time-stamped category.
# Tunable later against live data; conservative defaults for the foundation.
_DEFAULT_HALF_LIFE_S = MappingProxyType({
    AuthorityCategory.LIVE_OBSERVED:       600,    # 10 min — live AIS positions age fast
    AuthorityCategory.LIVE_ENVIRONMENTAL:  3600,   # 1 h   — weather/tides change slower
    AuthorityCategory.CONFIRMED_SYSTEM:    1800,   # 30 min
    AuthorityCategory.CONFIRMED_PUBLISHED: 7200,   # 2 h
})


def default_half_life_s(category: AuthorityCategory) -> float:
    """Default half-life for a category; falls back to the LIVE_OBSERVED window."""
    return _DEFAULT_HALF_LIFE_S.get(category, 600)


def element_score(
    prov: SourceProvenance,
    now: float,
    *,
    half_life_s: Optional[float] = None,
) -> float:
    """Authority score for one element in [0, 1].

    - Non-time-stamped element (observed_at is None): the category weight alone
      (e.g. an ASSUMED placeholder scores its 0.20 floor; a CONFIRMED_PUBLISHED
      element with no timestamp scores 0.70).
    - Time-stamped element: category weight decayed by freshness.
    """
    base = prov.category.weight
    if prov.observed_at is None:
        return base
    hl = half_life_s if half_life_s is not None else default_half_life_s(prov.category)
    return base * freshness_factor(prov.observed_at, now, half_life_s=hl)


def aggregate_scores(scores: Sequence[float]) -> float:
    """Mean of element scores; empty/all-None input -> 0.0."""
    vals = [s for s in scores if s is not None]
    if not vals:
        return 0.0
    return sum(vals) / len(vals)
