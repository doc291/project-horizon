"""
tests/test_authority.py — Beta 11 Slice 1: authority overlay foundation.

Covers category weights, freshness decay, source provenance objects, and the
ASSUMED fallback. Pure unit tests — no I/O, no wiring, no Beta 10 surface
touched. `now` is always supplied explicitly so tests are deterministic.
"""

import math

import pytest

from authority.categories import AuthorityCategory, all_categories
from authority.provenance import (
    SourceProvenance,
    assumed,
    provenance_or_assumed,
    ASSUMED_SOURCE,
)
from authority.freshness import freshness_factor
from authority.scoring import element_score, aggregate_scores, default_half_life_s


# ── Category weights ─────────────────────────────────────────────────────────

def test_all_six_categories_present():
    names = {c.name for c in AuthorityCategory}
    assert names == {
        "LIVE_OBSERVED", "LIVE_ENVIRONMENTAL", "CONFIRMED_SYSTEM",
        "CONFIRMED_PUBLISHED", "PREDICTED", "ASSUMED",
    }


def test_category_weight_values():
    assert AuthorityCategory.LIVE_OBSERVED.weight == 1.00
    assert AuthorityCategory.LIVE_ENVIRONMENTAL.weight == 1.00
    assert AuthorityCategory.CONFIRMED_SYSTEM.weight == 0.90
    assert AuthorityCategory.CONFIRMED_PUBLISHED.weight == 0.70
    assert AuthorityCategory.PREDICTED.weight == 0.50
    assert AuthorityCategory.ASSUMED.weight == 0.20


def test_weight_ordering_is_monotonic():
    order = [
        AuthorityCategory.LIVE_OBSERVED,
        AuthorityCategory.CONFIRMED_SYSTEM,
        AuthorityCategory.CONFIRMED_PUBLISHED,
        AuthorityCategory.PREDICTED,
        AuthorityCategory.ASSUMED,
    ]
    weights = [c.weight for c in order]
    assert weights == sorted(weights, reverse=True)
    assert all(0.0 <= c.weight <= 1.0 for c in AuthorityCategory)


def test_all_categories_helper_sorted_descending():
    cats = all_categories()
    assert len(cats) == 6
    assert [c.weight for c in cats] == sorted([c.weight for c in cats], reverse=True)
    assert cats[-1] is AuthorityCategory.ASSUMED  # lowest weight last


def test_assumed_is_the_floor():
    assert AuthorityCategory.ASSUMED.weight == min(c.weight for c in AuthorityCategory)


# ── Freshness decay ──────────────────────────────────────────────────────────

def test_freshness_at_zero_age_is_one():
    assert freshness_factor(1000.0, 1000.0, half_life_s=600) == 1.0


def test_freshness_at_one_half_life_is_half():
    assert freshness_factor(0.0, 600.0, half_life_s=600) == pytest.approx(0.5)


def test_freshness_at_two_half_lives_is_quarter():
    assert freshness_factor(0.0, 1200.0, half_life_s=600) == pytest.approx(0.25)


def test_freshness_decays_toward_zero_for_old_data():
    f = freshness_factor(0.0, 60 * 60 * 24, half_life_s=600)  # 1 day old, 10-min half-life
    assert 0.0 <= f < 1e-6


def test_freshness_none_observed_at_is_zero():
    assert freshness_factor(None, 1000.0, half_life_s=600) == 0.0


def test_freshness_future_timestamp_clamps_to_one():
    # clock skew: observed_at slightly in the future
    assert freshness_factor(1100.0, 1000.0, half_life_s=600) == 1.0


def test_freshness_rejects_nonpositive_half_life():
    with pytest.raises(ValueError):
        freshness_factor(0.0, 100.0, half_life_s=0)
    with pytest.raises(ValueError):
        freshness_factor(0.0, 100.0, half_life_s=-5)


def test_freshness_is_monotonic_decreasing_in_age():
    now = 10_000.0
    ages = [0, 100, 300, 600, 1200, 3600]
    vals = [freshness_factor(now - a, now, half_life_s=600) for a in ages]
    assert vals == sorted(vals, reverse=True)


# ── Source provenance objects ────────────────────────────────────────────────

def test_provenance_fields_and_frozen():
    p = SourceProvenance(
        category=AuthorityCategory.LIVE_OBSERVED,
        source="AISStream",
        observed_at=1234.0,
        detail="position",
    )
    assert p.category is AuthorityCategory.LIVE_OBSERVED
    assert p.source == "AISStream"
    assert p.observed_at == 1234.0
    assert p.detail == "position"
    assert p.is_time_stamped is True
    assert p.is_assumed is False
    with pytest.raises(Exception):
        p.source = "x"  # frozen dataclass


def test_provenance_not_time_stamped_when_observed_at_none():
    p = SourceProvenance(category=AuthorityCategory.CONFIRMED_PUBLISHED, source="QShips")
    assert p.observed_at is None
    assert p.is_time_stamped is False


def test_assumed_factory():
    a = assumed()
    assert a.category is AuthorityCategory.ASSUMED
    assert a.source == ASSUMED_SOURCE
    assert a.is_assumed is True
    a2 = assumed(source="Simulation", detail="tug availability")
    assert a2.source == "Simulation"
    assert a2.detail == "tug availability"


# ── Fallback to ASSUMED when source/category missing ─────────────────────────

def test_provenance_or_assumed_happy_path():
    p = provenance_or_assumed(
        category=AuthorityCategory.LIVE_OBSERVED, source="AISStream", observed_at=5.0,
    )
    assert p.category is AuthorityCategory.LIVE_OBSERVED
    assert p.source == "AISStream"
    assert p.observed_at == 5.0


def test_provenance_or_assumed_falls_back_when_source_missing():
    p = provenance_or_assumed(category=AuthorityCategory.LIVE_OBSERVED, source=None)
    assert p.is_assumed is True
    assert p.category is AuthorityCategory.ASSUMED


def test_provenance_or_assumed_falls_back_when_source_empty_string():
    p = provenance_or_assumed(category=AuthorityCategory.LIVE_OBSERVED, source="")
    assert p.is_assumed is True


def test_provenance_or_assumed_falls_back_when_category_missing():
    p = provenance_or_assumed(category=None, source="AISStream")
    assert p.is_assumed is True
    assert p.category is AuthorityCategory.ASSUMED


# ── Element scoring ──────────────────────────────────────────────────────────

def test_element_score_non_timestamped_returns_weight():
    p = SourceProvenance(category=AuthorityCategory.CONFIRMED_PUBLISHED, source="QShips")
    assert element_score(p, now=1000.0) == 0.70


def test_element_score_assumed_floor():
    assert element_score(assumed(), now=1000.0) == 0.20


def test_element_score_timestamped_decays():
    # LIVE_OBSERVED weight 1.0, default half-life 600s, one half-life old
    p = SourceProvenance(category=AuthorityCategory.LIVE_OBSERVED, source="AISStream", observed_at=0.0)
    assert element_score(p, now=600.0) == pytest.approx(0.5)


def test_element_score_fresh_live_is_full_weight():
    p = SourceProvenance(category=AuthorityCategory.LIVE_OBSERVED, source="AISStream", observed_at=1000.0)
    assert element_score(p, now=1000.0) == pytest.approx(1.0)


def test_element_score_custom_half_life_override():
    p = SourceProvenance(category=AuthorityCategory.LIVE_OBSERVED, source="AISStream", observed_at=0.0)
    assert element_score(p, now=300.0, half_life_s=300) == pytest.approx(0.5)


def test_default_half_life_lookup_and_fallback():
    assert default_half_life_s(AuthorityCategory.LIVE_OBSERVED) == 600
    assert default_half_life_s(AuthorityCategory.LIVE_ENVIRONMENTAL) == 3600
    # category without an explicit half-life falls back to 600
    assert default_half_life_s(AuthorityCategory.PREDICTED) == 600


# ── Aggregation ──────────────────────────────────────────────────────────────

def test_aggregate_scores_mean():
    assert aggregate_scores([1.0, 0.5, 0.0]) == pytest.approx(0.5)


def test_aggregate_scores_empty_is_zero():
    assert aggregate_scores([]) == 0.0


def test_aggregate_scores_ignores_none():
    assert aggregate_scores([1.0, None, 0.0]) == pytest.approx(0.5)
