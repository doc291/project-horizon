"""
tests/test_ais_identity.py — Beta 11 Slice 7A: honest AIS identity + provenance.

Verifies that build_horizon_vessels preserves real AIS identity (MMSI/IMO/name/
lat/lon/heading), uses the Slice-6A ais_status rather than forcing "berthed",
labels missing-name targets honestly (MMSI-based, name_confirmed False), and
tags per-vessel match_state / provenance so AIS and simulated vessels are not
blanket-claimed as LIVE_OBSERVED.
"""

from datetime import datetime, timezone

import mst_scraper

NOW = datetime(2026, 5, 29, 12, 0, tzinfo=timezone.utc)

BERTHS = [
    {"id": "B01", "name": "Berth 1", "status": "available"},
    {"id": "B02", "name": "Berth 2", "status": "occupied"},
    {"id": "B03", "name": "Berth 3", "status": "available"},
]


def _ais(mmsi, name, **kw):
    base = {
        "mmsi": mmsi, "imo": kw.get("imo", ""), "name": name,
        "arrived_utc": None, "loa_m": kw.get("loa_m", 210.0), "beam_m": 32.0,
        "draught_m": 11.0, "vessel_type": "Cargo", "destination": "AUMEL",
        "callsign": "ABCD", "lat": kw.get("lat", -37.83), "lon": kw.get("lon", 144.92),
        "sog": kw.get("sog", 0.2), "heading": kw.get("heading", 270),
        "ais_status": kw.get("ais_status", "berthed"),
    }
    return base


def _build(cached):
    return mst_scraper.build_horizon_vessels("AUMEL", BERTHS, NOW, cached_vessels=cached)


def _ais_vessels(out):
    return [v for v in out if v.get("match_state") == "ais_only"]


def _sim_vessels(out):
    return [v for v in out if v.get("match_state") == "simulated"]


# ── Identity preservation ────────────────────────────────────────────────────

def test_ais_vessel_preserves_mmsi():
    out = _build([_ais("503001234", "MV CARGO STAR")])
    v = _ais_vessels(out)[0]
    assert v["mmsi"] == "503001234"


def test_ais_vessel_preserves_imo_when_present():
    out = _build([_ais("503001234", "MV CARGO STAR", imo="9512345")])
    v = _ais_vessels(out)[0]
    assert v["imo"] == "9512345"


def test_ais_vessel_preserves_lat_lon():
    out = _build([_ais("503001234", "MV CARGO STAR", lat=-37.81, lon=144.93)])
    v = _ais_vessels(out)[0]
    assert v["lat"] == -37.81 and v["lon"] == 144.93


def test_ais_vessel_preserves_heading_and_sog():
    out = _build([_ais("503001234", "MV CARGO STAR", heading=123, sog=4.2)])
    v = _ais_vessels(out)[0]
    assert v["heading"] == 123 and v["sog"] == 4.2


def test_ais_vessel_preserves_confirmed_name():
    out = _build([_ais("503001234", "MV CARGO STAR")])
    v = _ais_vessels(out)[0]
    assert v["name"] == "MV CARGO STAR"
    assert v["name_confirmed"] is True


def test_ais_vessel_without_name_is_mmsi_based_not_fake():
    # get_vessels_in_port yields "VESSEL-<mmsi>" when static is missing
    out = _build([_ais("503009999", "VESSEL-503009999")])
    v = _ais_vessels(out)[0]
    assert v["name"] == "AIS 503009999"
    assert v["name_confirmed"] is False
    assert not v["name"].startswith("VESSEL-")


# ── Status from ais_status (not forced berthed) ──────────────────────────────

def test_ais_status_used_not_forced_berthed():
    out = _build([
        _ais("1", "MV ALPHA", ais_status="underway", sog=6.0),
        _ais("2", "MV BRAVO", ais_status="anchored", sog=0.1),
        _ais("3", "MV CHARLIE", ais_status="berthed", sog=0.0),
    ])
    by = {v["mmsi"]: v for v in _ais_vessels(out)}
    assert by["1"]["status"] == "underway"
    assert by["2"]["status"] == "anchored" and by["2"]["at_anchorage"] is True
    assert by["3"]["status"] == "berthed"
    # not ALL forced to berthed
    assert {v["status"] for v in _ais_vessels(out)} != {"berthed"}


# ── Provenance / traceability ────────────────────────────────────────────────

def test_ais_vessel_is_ais_only_live_observed():
    out = _build([_ais("503001234", "MV CARGO STAR")])
    v = _ais_vessels(out)[0]
    assert v["match_state"] == "ais_only"
    assert v["provenance"] == "LIVE_OBSERVED"
    assert v["source"] == "ais"
    assert v["berth_provenance"] == "assumed"   # berth not AIS-authoritative


def test_ais_vessel_id_is_ais_prefixed():
    out = _build([_ais("503001234", "MV CARGO STAR")])
    v = _ais_vessels(out)[0]
    assert v["id"] == "AIS-503001234"


# ── Simulated injected vessels marked honestly ───────────────────────────────

def test_simulated_vessels_are_marked():
    out = _build([_ais("503001234", "MV CARGO STAR")])
    sims = _sim_vessels(out)
    assert len(sims) >= 2                       # 2–3 injected
    for s in sims:
        assert s["match_state"] == "simulated"
        assert s["provenance"] == "ASSUMED"
        assert s["source"] == "sim"
        assert s["name_confirmed"] is False


def test_simulated_vessels_not_live_observed():
    out = _build([_ais("503001234", "MV CARGO STAR")])
    for s in _sim_vessels(out):
        assert s["provenance"] != "LIVE_OBSERVED"


# ── Mixed population is not a blanket LIVE_OBSERVED claim ─────────────────────

def test_mixed_population_has_both_provenances():
    out = _build([_ais("503001234", "MV CARGO STAR")])
    provs = {v.get("provenance") for v in out}
    assert "LIVE_OBSERVED" in provs and "ASSUMED" in provs
    # not every vessel is LIVE_OBSERVED
    assert any(v.get("provenance") == "ASSUMED" for v in out)


# ── Map can plot AIS vessels (lat/lon present) ───────────────────────────────

def test_ais_vessels_have_plottable_position():
    out = _build([_ais("503001234", "MV CARGO STAR", lat=-37.82, lon=144.95)])
    v = _ais_vessels(out)[0]
    assert v.get("lat") is not None and v.get("lon") is not None
