"""
tests/test_authority_classification.py — Beta 11 Slice 4B.

Explicit, case-by-case validation of authority classification across the live /
published / predicted / assumed states, focused on the environmental fix (cosine
tide + simulated weather → ASSUMED, not PREDICTED) and the vessel-source
passthrough (the feed→category decision made in build_summary._VSRC_MAP).
"""

from authority.categories import AuthorityCategory
from authority.payload import build_authority_block

NOW = 10_000.0


def _summary(vessel_source, *, tides=None, weather=None):
    return {"vessel_source": vessel_source, "tides": tides or {}, "weather": weather or {}}


def _vs(feed, category, source, observed_at=None):
    return {"feed": feed, "category": category, "source": source, "observed_at": observed_at}


def _feed(block, name):
    return next(f for f in block["feeds"] if f["name"] == name)


_SIM_VS = _vs("simulation", "ASSUMED", "Simulation")


# ── Environmental: tides ─────────────────────────────────────────────────────

def test_tides_fresh_bom_is_live_environmental():
    b = build_authority_block(_summary(_SIM_VS, tides={"data_source": "bom"}), NOW)
    assert _feed(b, "Tides")["category"] == "LIVE_ENVIRONMENTAL"


def test_tides_cosine_fallback_is_assumed():
    b = build_authority_block(_summary(_SIM_VS, tides={"data_source": "cosine"}), NOW)
    f = _feed(b, "Tides")
    assert f["category"] == "ASSUMED"           # Slice 4B fix: was PREDICTED
    assert f["category"] != "PREDICTED"


def test_tides_missing_is_assumed():
    b = build_authority_block(_summary(_SIM_VS, tides={}), NOW)
    assert _feed(b, "Tides")["category"] == "ASSUMED"


# ── Environmental: weather ───────────────────────────────────────────────────

def test_weather_live_is_live_environmental():
    b = build_authority_block(_summary(_SIM_VS, weather={"source": "live"}), NOW)
    assert _feed(b, "Weather")["category"] == "LIVE_ENVIRONMENTAL"


def test_weather_simulation_is_assumed():
    b = build_authority_block(_summary(_SIM_VS, weather={"source": "simulation"}), NOW)
    f = _feed(b, "Weather")
    assert f["category"] == "ASSUMED"           # Slice 4B fix: was PREDICTED
    assert f["category"] != "PREDICTED"


def test_weather_missing_is_assumed():
    b = build_authority_block(_summary(_SIM_VS, weather={}), NOW)
    assert _feed(b, "Weather")["category"] == "ASSUMED"


# ── Vessel source passthrough (build_summary._VSRC_MAP → block) ───────────────

def test_vessel_aisstream_is_live_observed():
    vs = _vs("aisstream", "LIVE_OBSERVED", "AISStream", observed_at=NOW)
    b = build_authority_block(_summary(vs), NOW)
    assert _feed(b, "Vessel movements")["category"] == "LIVE_OBSERVED"


def test_vessel_mst_is_live_observed():
    vs = _vs("mst", "LIVE_OBSERVED", "MST (AIS cache)", observed_at=NOW)
    b = build_authority_block(_summary(vs), NOW)
    assert _feed(b, "Vessel movements")["category"] == "LIVE_OBSERVED"


def test_vessel_public_scrape_is_confirmed_published():
    vs = _vs("public_scrape", "CONFIRMED_PUBLISHED", "Public movements (Melbourne)", observed_at=NOW)
    b = build_authority_block(_summary(vs), NOW)
    assert _feed(b, "Vessel movements")["category"] == "CONFIRMED_PUBLISHED"


def test_vessel_qships_public_is_confirmed_published():
    vs = _vs("qships_public", "CONFIRMED_PUBLISHED", "QShips (public)", observed_at=NOW)
    b = build_authority_block(_summary(vs), NOW)
    assert _feed(b, "Vessel movements")["category"] == "CONFIRMED_PUBLISHED"


def test_vessel_simulation_is_assumed():
    b = build_authority_block(_summary(_SIM_VS), NOW)
    assert _feed(b, "Vessel movements")["category"] == "ASSUMED"


# ── Operational inputs always assumed ────────────────────────────────────────

def test_towage_and_terminal_always_assumed():
    b = build_authority_block(_summary(_vs("aisstream", "LIVE_OBSERVED", "AISStream", observed_at=NOW),
                                       tides={"data_source": "bom"}, weather={"source": "live"}), NOW)
    assert _feed(b, "Towage availability")["category"] == "ASSUMED"
    assert _feed(b, "Terminal readiness")["category"] == "ASSUMED"


# ── Stale live degrades; never CONFIRMED_SYSTEM ──────────────────────────────

def test_stale_live_vessel_degrades_score():
    fresh = build_authority_block(_summary(_vs("aisstream", "LIVE_OBSERVED", "AISStream", observed_at=NOW)), NOW)
    stale = build_authority_block(_summary(_vs("aisstream", "LIVE_OBSERVED", "AISStream", observed_at=NOW - 36000)), NOW)
    assert stale["feeds"][0]["score"] < fresh["feeds"][0]["score"]
    assert stale["overall_score"] < fresh["overall_score"]


def test_never_confirmed_system():
    for vs in (_SIM_VS,
               _vs("qships_public", "CONFIRMED_PUBLISHED", "QShips (public)", observed_at=NOW),
               _vs("aisstream", "LIVE_OBSERVED", "AISStream", observed_at=NOW)):
        b = build_authority_block(_summary(vs, tides={"data_source": "bom"}, weather={"source": "live"}), NOW)
        assert b["sources_by_category"]["CONFIRMED_SYSTEM"] == 0


# ── Whole-state bands ────────────────────────────────────────────────────────

def test_all_live_band_at_least_medium():
    b = build_authority_block(
        _summary(_vs("aisstream", "LIVE_OBSERVED", "AISStream", observed_at=NOW),
                 tides={"data_source": "bom"}, weather={"source": "live"}), NOW)
    assert b["overall_band"] in ("MEDIUM", "HIGH")


def test_all_simulation_band_low():
    b = build_authority_block(_summary(_SIM_VS, tides={"data_source": "cosine"},
                                       weather={"source": "simulation"}), NOW)
    assert b["overall_band"] == "LOW"
    # every contributing element is ASSUMED in pure simulation
    assert b["sources_by_category"]["ASSUMED"] == 5
    assert b["sources_by_category"]["PREDICTED"] == 0
