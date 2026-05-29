"""
tests/test_authority_payload.py — Beta 11 Slice 3: backend authority block.

Covers the /api/summary authority block: presence, fallback→ASSUMED,
QShips-public / Ports-Victoria → CONFIRMED_PUBLISHED, never-CONFIRMED_SYSTEM,
and failure-degrades-without-breaking. Backend-only — no UI is exercised.
"""

import pytest

from authority.payload import build_authority_block, degraded_block, BLOCK_VERSION


NOW = 10_000.0


def _summary(vessel_source, *, tides=None, weather=None):
    return {
        "vessel_source": vessel_source,
        "tides": tides or {},
        "weather": weather or {},
    }


def _vs(feed, category, source, observed_at=None, detail=None):
    return {"feed": feed, "category": category, "source": source,
            "observed_at": observed_at, "detail": detail}


# ── Block presence + shape ───────────────────────────────────────────────────

def test_block_has_required_fields():
    block = build_authority_block(_summary(_vs("simulation", "ASSUMED", "Simulation")), NOW)
    for key in ("version", "overall_score", "overall_band", "degraded",
                "sources_by_category", "feeds", "assumed_or_missing", "notes"):
        assert key in block
    assert block["version"] == BLOCK_VERSION
    assert 0.0 <= block["overall_score"] <= 1.0
    assert block["overall_band"] in ("HIGH", "MEDIUM", "LOW")


# ── Fallback simulation → ASSUMED + degraded ─────────────────────────────────

def test_fallback_simulation_marked_assumed_and_degraded():
    block = build_authority_block(_summary(_vs("simulation", "ASSUMED", "Simulation")), NOW)
    assert block["degraded"] is True
    vessel_feed = next(f for f in block["feeds"] if f["name"] == "Vessel movements")
    assert vessel_feed["category"] == "ASSUMED"
    # vessel + towage + terminal all ASSUMED appear in the assumed list
    assert any("Vessel movements" in m for m in block["assumed_or_missing"])
    assert any("Towage" in m for m in block["assumed_or_missing"])
    assert block["overall_band"] == "LOW"


# ── QShips public → CONFIRMED_PUBLISHED (never CONFIRMED_SYSTEM) ──────────────

def test_qships_public_confirmed_published():
    vs = _vs("qships_public", "CONFIRMED_PUBLISHED", "QShips (public)", observed_at=NOW)
    block = build_authority_block(_summary(vs), NOW)
    vessel_feed = next(f for f in block["feeds"] if f["name"] == "Vessel movements")
    assert vessel_feed["category"] == "CONFIRMED_PUBLISHED"
    assert vessel_feed["source"] == "QShips (public)"
    assert block["sources_by_category"]["CONFIRMED_PUBLISHED"] >= 1
    assert block["sources_by_category"]["CONFIRMED_SYSTEM"] == 0


# ── Ports Victoria public → CONFIRMED_PUBLISHED ──────────────────────────────

def test_ports_victoria_public_confirmed_published():
    vs = _vs("public_scrape", "CONFIRMED_PUBLISHED", "Public movements (Melbourne)", observed_at=NOW)
    block = build_authority_block(_summary(vs), NOW)
    vessel_feed = next(f for f in block["feeds"] if f["name"] == "Vessel movements")
    assert vessel_feed["category"] == "CONFIRMED_PUBLISHED"
    assert block["degraded"] is False


# ── Never CONFIRMED_SYSTEM anywhere ──────────────────────────────────────────

def test_no_confirmed_system_anywhere():
    for vs in (
        _vs("simulation", "ASSUMED", "Simulation"),
        _vs("qships_public", "CONFIRMED_PUBLISHED", "QShips (public)", observed_at=NOW),
        _vs("aisstream", "LIVE_OBSERVED", "AISStream", observed_at=NOW),
    ):
        block = build_authority_block(_summary(vs), NOW)
        assert block["sources_by_category"]["CONFIRMED_SYSTEM"] == 0


# ── Band sensitivity (live vs simulated) ─────────────────────────────────────

def test_live_feeds_score_higher_than_simulation():
    live = build_authority_block(
        _summary(_vs("aisstream", "LIVE_OBSERVED", "AISStream", observed_at=NOW),
                 tides={"data_source": "bom"}, weather={"source": "live"}),
        NOW,
    )
    sim = build_authority_block(_summary(_vs("simulation", "ASSUMED", "Simulation")), NOW)
    assert live["overall_score"] > sim["overall_score"]
    assert live["overall_band"] in ("MEDIUM", "HIGH")
    assert sim["overall_band"] == "LOW"


def test_stale_live_feed_degrades_score():
    fresh = build_authority_block(
        _summary(_vs("aisstream", "LIVE_OBSERVED", "AISStream", observed_at=NOW)), NOW)
    stale = build_authority_block(
        _summary(_vs("aisstream", "LIVE_OBSERVED", "AISStream", observed_at=NOW - 36000)), NOW)
    assert stale["overall_score"] < fresh["overall_score"]


# ── Failure degrades without breaking ────────────────────────────────────────

def test_degraded_block_shape():
    b = degraded_block("boom")
    assert b["overall_band"] == "LOW"
    assert b["degraded"] is True
    assert b["overall_score"] == 0.0
    assert "boom" in b["notes"][0]


def test_build_block_on_empty_summary_does_not_raise():
    # Missing vessel_source / tides / weather → treated as ASSUMED, no exception
    block = build_authority_block({}, NOW)
    assert block["degraded"] is True
    assert block["overall_band"] == "LOW"


# ── Integration: build_summary + attach (simulation env) ─────────────────────

def test_summary_attach_authority_simulation():
    import server
    summary = server.build_summary()
    server._attach_authority_block(summary)
    assert "vessel_source" in summary
    assert "authority" in summary
    auth = summary["authority"]
    assert auth["version"] == BLOCK_VERSION
    # test env has no AIS/MST/network → vessel feed is ASSUMED (simulation)
    assert summary["vessel_source"]["category"] == "ASSUMED"
    assert auth["degraded"] is True
    assert auth["sources_by_category"]["CONFIRMED_SYSTEM"] == 0


def test_attach_authority_never_raises_on_bad_input():
    import server
    # non-dict input must not raise
    server._attach_authority_block(None)
    bad = {"unexpected": True}
    server._attach_authority_block(bad)
    assert "authority" in bad
