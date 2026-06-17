"""
Project Horizon — Beta 12 Slice 0: SIM-as-Live provenance gate.

Locks in the first build gate: no simulated vessel may surface as live evidence.
Backend is additive — the legacy `data_source` field is preserved unchanged for
Beta 10 parity; a new `provenance` field carries the SIM-aware truth, and every
summary vessel carries an explicit `source`.

Mirrors Beta 10 production posture: DATABASE_URL unset, no network.
"""

from __future__ import annotations

import os
from datetime import timedelta

os.environ.pop("DATABASE_URL", None)

import server  # noqa: E402


# ── Helpers ──────────────────────────────────────────────────────────────────

def _v(vid, name, source, *, status, berth="B01", loa=200,
       start_h=None, end_h=None):
    """Minimal commercial vessel (LOA >= 100) for berth-overlap detection."""
    now = server.utcnow()
    d = {"id": vid, "name": name, "source": source, "status": status,
         "berth_id": berth, "loa": loa, "vessel_type": "container", "notes": ""}
    if status == "berthed":
        d["ata"] = server.fmt(now + timedelta(hours=start_h if start_h is not None else -1))
        d["atd"] = server.fmt(now + timedelta(hours=end_h if end_h is not None else 1))
        d["eta"] = d["ata"]; d["etd"] = d["atd"]
    else:
        d["eta"] = server.fmt(now + timedelta(hours=start_h if start_h is not None else 0.5))
        d["etd"] = server.fmt(now + timedelta(hours=end_h if end_h is not None else 3))
    return d


_BERTHS = [{"id": "B01", "name": "Berth 1"}]


def _overlap_conflict(vessels):
    now = server.utcnow()
    cs = server.detect_conflicts(vessels, _BERTHS, [], [], now, is_live=True)
    return next((c for c in cs if c["conflict_type"] == "berth_overlap"), None)


# ── _vessel_is_sim ───────────────────────────────────────────────────────────

def test_vessel_is_sim_detects_both_markers():
    assert server._vessel_is_sim({"id": "SIM-9", "source": "ais"})   # SIM- id
    assert server._vessel_is_sim({"id": "X-1", "source": "sim"})     # sim source
    assert not server._vessel_is_sim({"id": "MST-1", "source": "mst"})
    assert server._vessel_is_sim({})  # unknown -> conservative non-live


# ── Conflict provenance (the gate) ───────────────────────────────────────────

def test_mixed_real_and_sim_conflict_is_not_live():
    real = _v("MST-1", "REAL ONE", "mst", status="berthed")
    sim  = _v("SIM-2", "SIM TWO", "sim", status="scheduled")
    c = _overlap_conflict([real, sim])
    assert c is not None, "expected a berth_overlap conflict"
    assert c["provenance"] == "mixed", c["provenance"]
    assert c["provenance"] != "live"          # THE GATE
    # Beta 10 parity: legacy field still present and untouched
    assert "data_source" in c and c["data_source"] == "live"


def test_all_real_conflict_is_live():
    a = _v("MST-1", "REAL ONE", "mst", status="berthed")
    b = _v("AIS-2", "REAL TWO", "aisstream", status="scheduled")
    c = _overlap_conflict([a, b])
    assert c is not None
    assert c["provenance"] == "live"


def test_all_sim_conflict_is_simulated():
    a = _v("SIM-1", "SIM ONE", "sim", status="berthed")
    b = _v("SIM-2", "SIM TWO", "sim", status="scheduled")
    c = _overlap_conflict([a, b])
    assert c is not None
    assert c["provenance"] == "simulated"


def test_pilotage_towage_default_to_simulated():
    # Conflicts not built from the vessel-source path keep provenance 'simulated'
    # via the _conflict default (inferred domains).
    c = server._conflict("x", "pilotage_window", "WARNING", "high",
                         ["MST-1"], ["REAL"], None, None, server.fmt(server.utcnow()),
                         "desc", [], data_source="simulated")
    assert c["provenance"] == "simulated"


# ── Per-vessel source surfaced in /api/summary ───────────────────────────────

def test_summary_vessels_carry_source():
    s = server.build_summary()
    vessels = s.get("vessels") or []
    assert vessels, "expected vessels in summary"
    allowed = {"ais", "aisstream", "mst", "scraper", "qships", "sim"}
    for v in vessels:
        assert v.get("source") in allowed, f"{v.get('id')} source={v.get('source')!r}"


def test_summary_conflicts_carry_provenance_and_preserve_data_source():
    s = server.build_summary()
    for c in s.get("conflicts") or []:
        assert "provenance" in c, f"conflict {c.get('id')} missing provenance"
        assert c["provenance"] in {"live", "mixed", "simulated"}
        assert "data_source" in c          # Beta 10 field preserved
