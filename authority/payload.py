"""
authority/payload.py — Build the backend authority block for /api/summary (Beta 11, Slice 3).

Pure function: given an already-built summary dict and `now` (epoch seconds),
derive an `authority` block describing how authoritative the underlying data
is. Backend-only — this does NOT change the Decision Card UI.

The block reports:
  - sources_by_category : count of evidence elements per AuthorityCategory
  - feeds               : per-feed provenance + freshness + element score
  - assumed_or_missing  : human-readable list of ASSUMED / missing inputs
  - overall_score       : aggregate authority in [0, 1]
  - overall_band        : HIGH / MEDIUM / LOW
  - degraded            : True if the vessel feed fell back to simulation/stale

It NEVER raises for ordinary inputs; the caller additionally wraps it so a feed
outage degrades authority rather than breaking the request.
"""

from __future__ import annotations

from typing import Optional

from authority.categories import AuthorityCategory
from authority.provenance import SourceProvenance
from authority.scoring import element_score, aggregate_scores, default_half_life_s
from authority.freshness import freshness_factor

BLOCK_VERSION = "beta11-slice3"

# Band thresholds (tunable later against live data).
_BAND_HIGH = 0.80
_BAND_MEDIUM = 0.55


def _band(score: float) -> str:
    if score >= _BAND_HIGH:
        return "HIGH"
    if score >= _BAND_MEDIUM:
        return "MEDIUM"
    return "LOW"


def _category_from_str(name: Optional[str]) -> AuthorityCategory:
    """Map a category string to the enum; unknown/missing → ASSUMED (fail safe)."""
    if not name:
        return AuthorityCategory.ASSUMED
    try:
        return AuthorityCategory[name]
    except KeyError:
        return AuthorityCategory.ASSUMED


def _age_s(observed_at: Optional[float], now: float) -> Optional[float]:
    if observed_at is None:
        return None
    return max(0.0, now - observed_at)


def build_authority_block(summary: dict, now: float) -> dict:
    """Derive the backend authority block from a built summary dict."""
    feeds = []
    elements = []  # (provenance) for scoring

    # ── 1. Vessel feed (the headline) ─────────────────────────────────────────
    vs = summary.get("vessel_source") or {}
    vessel_cat = _category_from_str(vs.get("category"))
    vessel_prov = SourceProvenance(
        category=vessel_cat,
        source=vs.get("source") or "Simulation",
        observed_at=vs.get("observed_at"),
        detail=vs.get("detail"),
    )
    elements.append(vessel_prov)
    feeds.append(_feed_entry("Vessel movements", vessel_prov, now))

    # ── 2. Environmental: tides + weather ─────────────────────────────────────
    # Environmental classification (Slice 4B audit fix):
    #   fresh feed              -> LIVE_ENVIRONMENTAL
    #   fallback / simulated    -> ASSUMED   (NOT PREDICTED)
    # PREDICTED is reserved for Horizon operational FORECASTS (ETA / UKC /
    # berth clearance / conflict probability), per the authority rules. A cosine
    # tide approximation (BOM unavailable) and a simulated weather feed are
    # fallbacks, so they degrade to ASSUMED — matching "simulation/fallback =
    # ASSUMED" and ensuring missing environmental feeds degrade authority.
    tides = summary.get("tides") or {}
    tide_live = (tides.get("data_source") == "bom") or (tides.get("source") == "bom")
    tide_prov = SourceProvenance(
        category=AuthorityCategory.LIVE_ENVIRONMENTAL if tide_live else AuthorityCategory.ASSUMED,
        source="BOM Tides" if tide_live else "Simulation (cosine tide fallback)",
        # Slice 4C: real fetch age stamped by build_summary; never `now`.
        observed_at=tides.get("observed_at") if tide_live else None,
        detail="bom" if tide_live else "cosine-fallback",
    )
    elements.append(tide_prov)
    feeds.append(_feed_entry("Tides", tide_prov, now))

    weather = summary.get("weather") or {}
    weather_live = (weather.get("source") in ("live", "open-meteo", "bom")) or (weather.get("is_live") is True)
    weather_prov = SourceProvenance(
        category=AuthorityCategory.LIVE_ENVIRONMENTAL if weather_live else AuthorityCategory.ASSUMED,
        source="Weather (live)" if weather_live else "Simulation (weather fallback)",
        # Slice 4C: real fetch age stamped by build_summary; never `now`.
        observed_at=weather.get("observed_at") if weather_live else None,
        detail="live" if weather_live else "simulated",
    )
    elements.append(weather_prov)
    feeds.append(_feed_entry("Weather", weather_prov, now))

    # ── 3. Always-assumed operational inputs (current Horizon) ────────────────
    assumed_inputs = [
        ("Towage availability", "Simulation"),
        ("Terminal readiness", "Simulation"),
    ]
    for label, src in assumed_inputs:
        prov = SourceProvenance(category=AuthorityCategory.ASSUMED, source=src, observed_at=None,
                                detail="simulated")
        elements.append(prov)
        feeds.append(_feed_entry(label, prov, now))

    # ── Aggregate ─────────────────────────────────────────────────────────────
    scores = [_score_for(p, now) for p in elements]
    overall = aggregate_scores(scores)

    # sources_by_category counts
    by_cat = {c.name: 0 for c in AuthorityCategory}
    for p in elements:
        by_cat[p.category.name] += 1

    assumed_or_missing = [
        f"{f['name']} ({f['category']})"
        for f in feeds
        if f["category"] == AuthorityCategory.ASSUMED.name
    ]

    degraded = vessel_cat is AuthorityCategory.ASSUMED

    return {
        "version": BLOCK_VERSION,
        "overall_score": round(overall, 3),
        "overall_band": _band(overall),
        "degraded": degraded,
        "sources_by_category": by_cat,
        "feeds": feeds,
        "assumed_or_missing": assumed_or_missing,
        "notes": [
            "Backend-only authority block (Beta 11 Slice 3). No Decision Card UI change.",
            "QShips SOAP not integrated; no CONFIRMED_SYSTEM sources present.",
        ],
    }


def _score_for(prov: SourceProvenance, now: float) -> float:
    """Authority score for one element.

    Environmental LIVE feeds REQUIRE a real observed_at to score fresh: a missing
    timestamp yields freshness 0 (not the full category weight), so unverifiable
    'live' environmental data does not falsely score as fresh, and genuinely fresh
    feeds decay with age. All other categories use the standard element_score()
    (where a None timestamp means "not time-stamped" → category weight)."""
    if prov.category is AuthorityCategory.LIVE_ENVIRONMENTAL:
        hl = default_half_life_s(AuthorityCategory.LIVE_ENVIRONMENTAL)
        return prov.category.weight * freshness_factor(prov.observed_at, now, half_life_s=hl)
    return element_score(prov, now)


def _feed_entry(name: str, prov: SourceProvenance, now: float) -> dict:
    age = _age_s(prov.observed_at, now)
    return {
        "name": name,
        "category": prov.category.name,
        "source": prov.source,
        "observed_at": prov.observed_at,
        "age_s": round(age, 1) if age is not None else None,
        "score": round(_score_for(prov, now), 3),
        "detail": prov.detail,
    }


def degraded_block(error: str) -> dict:
    """Minimal fail-safe authority block when computation itself fails.
    Authority degrades to LOW; the request is never broken."""
    return {
        "version": BLOCK_VERSION,
        "overall_score": 0.0,
        "overall_band": "LOW",
        "degraded": True,
        "sources_by_category": {c.name: 0 for c in AuthorityCategory},
        "feeds": [],
        "assumed_or_missing": [],
        "notes": [f"authority block degraded: {error}"],
    }
