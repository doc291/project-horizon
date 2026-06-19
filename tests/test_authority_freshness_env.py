"""
tests/test_authority_freshness_env.py — Beta 11 Slice 4C.

Real environmental-feed freshness: fresh feeds score fresh, stale feeds degrade
on age, missing timestamps do NOT falsely score as fresh, and fallback feeds
(cosine tide / simulated weather) remain ASSUMED. Also covers the additive
cache-age accessors and that build_summary stamps observed_at onto tides/weather.
"""

import time

import weather as weather_mod
import bom_tides as bom_mod
from authority.payload import build_authority_block

NOW = 1_000_000.0


def _summary(tides=None, weather=None):
    # vessel feed fixed as ASSUMED so we isolate environmental behaviour
    return {
        "vessel_source": {"feed": "simulation", "category": "ASSUMED",
                          "source": "Simulation", "observed_at": None},
        "tides": tides or {},
        "weather": weather or {},
    }


def _feed(block, name):
    return next(f for f in block["feeds"] if f["name"] == name)


# ── Fresh environmental feeds score as fresh ─────────────────────────────────

def test_fresh_tides_score_near_full():
    b = build_authority_block(_summary(tides={"data_source": "bom", "observed_at": NOW}), NOW)
    f = _feed(b, "Tides")
    assert f["category"] == "LIVE_ENVIRONMENTAL"
    assert f["score"] >= 0.99            # observed_at == now → freshness ~1.0


def test_fresh_weather_score_near_full():
    b = build_authority_block(_summary(weather={"source": "live", "observed_at": NOW}), NOW)
    f = _feed(b, "Weather")
    assert f["category"] == "LIVE_ENVIRONMENTAL"
    assert f["score"] >= 0.99


# ── Stale environmental feeds degrade with age ───────────────────────────────

def test_stale_tides_degrade_with_age():
    # tide half-life = 3600s; 2 half-lives old → freshness ~0.25
    fresh = _feed(build_authority_block(_summary(tides={"data_source": "bom", "observed_at": NOW}), NOW), "Tides")
    stale = _feed(build_authority_block(_summary(tides={"data_source": "bom", "observed_at": NOW - 7200}), NOW), "Tides")
    assert stale["score"] < fresh["score"]
    assert stale["score"] == round(1.0 * (0.5 ** (7200 / 3600)), 3)   # ~0.25
    assert stale["category"] == "LIVE_ENVIRONMENTAL"                  # still classified live, just degraded


def test_stale_weather_degrades_with_age():
    fresh = _feed(build_authority_block(_summary(weather={"source": "live", "observed_at": NOW}), NOW), "Weather")
    stale = _feed(build_authority_block(_summary(weather={"source": "live", "observed_at": NOW - 10800}), NOW), "Weather")
    assert stale["score"] < fresh["score"]


# ── Missing timestamp does NOT falsely score as fresh ────────────────────────

def test_live_tide_without_timestamp_is_not_fresh():
    # classified live (data_source bom) but no observed_at → must NOT score full
    f = _feed(build_authority_block(_summary(tides={"data_source": "bom"}), NOW), "Tides")
    assert f["category"] == "LIVE_ENVIRONMENTAL"
    assert f["score"] == 0.0             # freshness 0 when timestamp absent (not 1.0)


def test_live_weather_without_timestamp_is_not_fresh():
    f = _feed(build_authority_block(_summary(weather={"source": "live"}), NOW), "Weather")
    assert f["category"] == "LIVE_ENVIRONMENTAL"
    assert f["score"] == 0.0


# ── Fallbacks remain ASSUMED regardless of timestamp ─────────────────────────

def test_cosine_tide_fallback_remains_assumed():
    f = _feed(build_authority_block(_summary(tides={"data_source": "cosine"}), NOW), "Tides")
    assert f["category"] == "ASSUMED"
    assert f["score"] == 0.2            # ASSUMED floor, timestamp ignored


def test_simulated_weather_remains_assumed():
    f = _feed(build_authority_block(_summary(weather={"source": "simulation"}), NOW), "Weather")
    assert f["category"] == "ASSUMED"
    assert f["score"] == 0.2


# ── Additive cache-age accessors ─────────────────────────────────────────────

def test_weather_cache_age_none_when_absent():
    assert weather_mod.cache_age_s({"id": "NONEXISTENT_PORT_XYZ"}) is None
    assert weather_mod.cache_age_s({}) is None


def test_weather_cache_age_returns_age_when_present():
    pid = "TEST_PORT_4C"
    with weather_mod._cache_lock:
        weather_mod._cache[pid] = {"data": {"source": "live"}, "fetched_at": time.time() - 120}
    try:
        age = weather_mod.cache_age_s({"id": pid})
        assert age is not None and 110 <= age <= 200
    finally:
        with weather_mod._cache_lock:
            weather_mod._cache.pop(pid, None)


def test_bom_cache_age_none_when_absent():
    assert bom_mod.cache_age_s({"bom_station_id": "NONEXISTENT_STN"}) is None
    assert bom_mod.cache_age_s({}) is None            # no station configured


def test_bom_cache_age_returns_age_when_present():
    sid = "TEST_STN_4C"
    with bom_mod._cache_lock:
        bom_mod._cache[sid] = {"events": [], "fetched_at": time.monotonic() - 90}
    try:
        age = bom_mod.cache_age_s({"bom_station_id": sid})
        assert age is not None and 80 <= age <= 160
    finally:
        with bom_mod._cache_lock:
            bom_mod._cache.pop(sid, None)


# ── build_summary stamps observed_at onto tides/weather ──────────────────────

def test_build_summary_stamps_env_observed_at_keys():
    import server
    s = server.build_summary()
    assert "observed_at" in s["tides"]     # key present (value may be None in sim/test env)
    assert "observed_at" in s["weather"]
