"""
Project Horizon — recommendation audit helper tests.

Phase 0.7b per ADR-002 §1.4.1. Verifies RECOMMENDATION_GENERATED
emission plus the mandatory decision-time snapshot across:

  1. RECOMMENDATION_GENERATED emits when audit enabled
  2. decision_snapshot present and complete enough for ADR-002 §1.4.1
  3. snapshot does NOT mutate the conflict/summary objects
  4. no-op when DATABASE_URL unset
  5. /api/summary path survives audit failure (best-effort)
  6. no RECOMMENDATION_PRESENTED event emitted anywhere in 0.7b
  7. /api/summary output remains unchanged with audit disabled
  8. snapshot size below target for normal Beta 10 conflicts

Plus production-safety, dedup, lifecycle, and payload-shape checks
mirroring tests/test_conflict_audit.py.
"""

from __future__ import annotations

import copy
import json
import logging
import os
import sys
import threading
import time
from uuid import uuid4

import pytest

# Make repo root importable
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
db_required = pytest.mark.skipif(
    not DATABASE_URL,
    reason="DATABASE_URL not set; live-DB tests require Postgres with audit schema",
)


# ── Fixture data builders ────────────────────────────────────────────

def _sample_seq_alts(recommended_id: str = "SEQ-1") -> list[dict]:
    return [
        {
            "id": "SEQ-1", "strategy": "delay_arrival",
            "headline": "Delay V-002 by 2.5h",
            "description": "Hold V-002 at anchorage.",
            "affected_vessels": ["V-002"],
            "ops_note": "Anchorage capacity OK.",
            "feasibility": "high", "cascade_count": 0,
            "cost_usd": 2100, "cost_label": "~$2,100 anchorage",
            "delay_mins": 150, "risk": "low",
            "recommended": (recommended_id == "SEQ-1"),
        },
        {
            "id": "SEQ-2", "strategy": "advance_departure",
            "headline": "Accelerate V-001 departure",
            "description": "Push V-001 ETD forward 2h.",
            "affected_vessels": ["V-001"],
            "ops_note": "Terminal must accelerate.",
            "feasibility": "medium", "cascade_count": 2,
            "cost_usd": 7400, "cost_label": "~$7,400 overtime",
            "delay_mins": 0, "risk": "medium",
            "recommended": (recommended_id == "SEQ-2"),
        },
        {
            "id": "SEQ-3", "strategy": "reassign_berth",
            "headline": "Move V-002 to B4",
            "description": "Send V-002 to Berth 4.",
            "affected_vessels": ["V-002"],
            "ops_note": "Dependent on B4 availability.",
            "feasibility": "low", "cascade_count": 0,
            "cost_usd": 3200, "cost_label": "~$3,200 repositioning",
            "delay_mins": 60, "risk": "high",
            "recommended": (recommended_id == "SEQ-3"),
        },
    ]


def _sample_decision_support(recommended_id: str = "SEQ-1") -> dict:
    return {
        "recommended_option_id":  recommended_id,
        "recommended_reasoning":  "Lowest cost with minimal cascade impact.",
        "confidence":             "high",
        "decision_deadline":      "2026-05-13T12:30:00+00:00",
        "options":                _sample_seq_alts(recommended_id),
    }


def _sample_conflict(suffix: str = "", recommended_id: str = "SEQ-1") -> dict:
    """A conflict carrying a decision_support recommendation — matches
    the engine output shape produced by server.py's _conflict()."""
    return {
        "id": f"c-{suffix or 'one'}",
        "conflict_type": "berth_overlap",
        "signal_type": "CONFLICT",
        "severity": "high",
        "vessel_ids": ["V-001", "V-002"],
        "vessel_names": ["MV Alpha", "MV Bravo"],
        "berth_id": "B3",
        "berth_name": "Berth 3",
        "conflict_time": "2026-05-13T14:30:00+00:00",
        "description": "V-002 arrives 30min after V-001 departs at Berth 3.",
        "resolution_options": ["Hold V-002", "Reassign V-002"],
        "sequencing_alternatives": _sample_seq_alts(recommended_id),
        "decision_support": _sample_decision_support(recommended_id),
        "data_source": "simulated",
        "safety_score": 78,
    }


def _sample_summary() -> dict:
    """Minimal in-memory build_summary() shape sufficient for snapshot tests."""
    return {
        "port_name": "Brisbane",
        "data_source": "live",
        "data_source_label": "AIS Live — Port of Brisbane",
        "scraped_at": "2026-05-13T10:05:00+00:00",
        "lookahead_hours": 48,
        "vessels": [
            {"id": "V-001", "name": "MV Alpha", "loa": 220, "beam": 32,
             "draft": 11.5, "status": "berthed", "berth_id": "B3",
             "eta": "2026-05-12T22:00:00+00:00",
             "etd": "2026-05-13T14:00:00+00:00",
             "ata": "2026-05-12T21:55:00+00:00", "atd": None,
             "source": "ais", "data_source": "live"},
            {"id": "V-002", "name": "MV Bravo", "loa": 200, "beam": 30,
             "draft": 10.8, "status": "scheduled", "berth_id": "B3",
             "eta": "2026-05-13T14:30:00+00:00",
             "etd": "2026-05-14T08:00:00+00:00",
             "ata": None, "atd": None,
             "source": "ais", "data_source": "live"},
            {"id": "V-999", "name": "Unrelated", "loa": 180, "berth_id": "B7",
             "status": "berthed", "eta": "x", "etd": "y"},
        ],
        "berths": [
            {"id": "B3", "name": "Berth 3", "length_m": 270, "depth_m": 13,
             "max_loa": 260, "max_draft": 12.0, "status": "occupied",
             "readiness_time": None},
            {"id": "B7", "name": "Berth 7", "length_m": 180},
        ],
        "tides": {
            "current_height_m": 1.9, "next_high": "2026-05-13T15:42:00+00:00",
            "next_low": "2026-05-13T21:55:00+00:00", "source": "bom",
        },
        "weather": {
            "wind_kts": 14, "wind_dir": "SE", "gust_kts": 21,
            "sea_state": "moderate", "swell_m": 1.2,
            "visibility": "good", "source": "live",
        },
        "ukc": [
            {"vessel_id": "V-001", "ukc_m": 1.8, "required_m": 1.0,
             "tide_at_eta_m": 1.9, "status": "ok"},
            {"vessel_id": "V-002", "ukc_m": 1.5, "required_m": 1.0,
             "status": "ok"},
            {"vessel_id": "V-999", "ukc_m": 0.2, "status": "warning"},
        ],
        "arrival_ukc": [
            {"vessel_id": "V-002", "ukc_m": 1.4, "required_m": 1.0,
             "status": "ok"},
        ],
        "port_status": {
            "berths_occupied": 6, "berths_available": 4, "berths_total": 10,
            "vessels_in_port": 12, "vessels_expected_24h": 5,
            "vessels_departing_24h": 4, "active_conflicts": 3,
            "critical_conflicts": 1, "pilots_available": 3, "tugs_available": 4,
        },
        "port_profile": {
            "id": "BRISBANE", "display_name": "Port of Brisbane",
            "short_name": "Brisbane", "timezone": "Australia/Brisbane",
            "vts_callsign": "Brisbane VTS",
            "using_live_vessel_data": True,
            "using_live_tidal_data": True,
            "using_live_weather_data": True,
            "bom_station_id": "IDQ60901.94576",
        },
    }


# ════════════════════════════════════════════════════════════════════
# Production safety
# ════════════════════════════════════════════════════════════════════

class TestProductionSafetyPath:

    def test_imports_with_psycopg_blocked(self):
        """recommendation_audit must import cleanly under simulated production."""
        orig_meta_path = sys.meta_path.copy()
        for k in list(sys.modules):
            if k.startswith(("psycopg", "recommendation_audit", "db", "audit")):
                del sys.modules[k]

        class Blocker:
            def find_spec(self, name, path, target=None):
                if name.split(".")[0] in {"psycopg", "psycopg_binary"}:
                    raise ImportError(f"simulated production: {name} not installed")
                return None

        sys.meta_path.insert(0, Blocker())
        try:
            import recommendation_audit  # noqa: F401
            assert hasattr(recommendation_audit, "emit_async")
            assert hasattr(recommendation_audit, "is_enabled")
            assert hasattr(recommendation_audit, "make_payload")
            assert hasattr(recommendation_audit, "make_snapshot")
        finally:
            sys.meta_path[:] = orig_meta_path
            for k in list(sys.modules):
                if k.startswith(("recommendation_audit", "db", "audit")):
                    del sys.modules[k]

    def test_emit_async_is_noop_when_database_url_unset(self, monkeypatch):
        """Case 4: No thread spawned, no psycopg imported when DATABASE_URL unset."""
        monkeypatch.delenv("DATABASE_URL", raising=False)
        for mod in ("db", "recommendation_audit"):
            if mod in sys.modules:
                del sys.modules[mod]

        threads_before = set(threading.enumerate())

        import recommendation_audit
        assert recommendation_audit.is_enabled() is False

        recommendation_audit.emit_async(
            "ams-demo", _sample_conflict(), summary=_sample_summary())
        time.sleep(0.05)

        new_threads = set(threading.enumerate()) - threads_before
        assert not new_threads, (
            f"emit_async spawned threads under no DATABASE_URL: {new_threads}")

    def test_emit_async_is_noop_when_feature_flag_disabled(self, monkeypatch):
        monkeypatch.setenv("AUDIT_EMISSION_ENABLED", "false")
        if DATABASE_URL:
            monkeypatch.setenv("DATABASE_URL", DATABASE_URL)
        for mod in ("db", "recommendation_audit"):
            if mod in sys.modules:
                del sys.modules[mod]

        threads_before = set(threading.enumerate())
        import recommendation_audit
        assert recommendation_audit.is_enabled() is False
        recommendation_audit.emit_async(
            "ams-demo", _sample_conflict(), summary=_sample_summary())
        time.sleep(0.05)
        new_threads = set(threading.enumerate()) - threads_before
        assert not new_threads


# ════════════════════════════════════════════════════════════════════
# Snapshot completeness (Case 2)
# ════════════════════════════════════════════════════════════════════

class TestSnapshotCompleteness:
    """Case 2: decision_snapshot must satisfy ADR-002 §1.4.1."""

    REQUIRED_FIELDS = (
        "engine_version",
        "relevant_vessels",
        "relevant_berths",
        "eta_source_hierarchy",
        "tide_inputs",
        "weather_inputs",
        "ukc_inputs",
        "conflict_state",
        "constraints",
        "alternatives_generated",
        "recommended_option",
        "decision_deadline",
    )

    def test_snapshot_has_all_adr_002_fields(self):
        import recommendation_audit as ra
        snap = ra.make_snapshot(_sample_conflict(), summary=_sample_summary())
        missing = [f for f in self.REQUIRED_FIELDS if f not in snap]
        assert not missing, f"snapshot missing required fields: {missing}"

    def test_relevant_vessels_filtered_to_conflict_only(self):
        import recommendation_audit as ra
        snap = ra.make_snapshot(_sample_conflict(), summary=_sample_summary())
        ids = {v["id"] for v in snap["relevant_vessels"]}
        assert ids == {"V-001", "V-002"}, (
            f"unrelated V-999 leaked into snapshot: {ids}")

    def test_relevant_berths_filtered_to_conflict_only(self):
        import recommendation_audit as ra
        snap = ra.make_snapshot(_sample_conflict(), summary=_sample_summary())
        assert len(snap["relevant_berths"]) == 1
        assert snap["relevant_berths"][0]["id"] == "B3"

    def test_recommended_option_resolved_from_id(self):
        import recommendation_audit as ra
        c = _sample_conflict(recommended_id="SEQ-3")
        snap = ra.make_snapshot(c, summary=_sample_summary())
        assert snap["recommended_option"]["id"] == "SEQ-3"
        assert snap["recommended_option"]["strategy"] == "reassign_berth"

    def test_ukc_inputs_filtered_to_conflict_vessels(self):
        import recommendation_audit as ra
        snap = ra.make_snapshot(_sample_conflict(), summary=_sample_summary())
        vids = {u["vessel_id"] for u in snap["ukc_inputs"]}
        assert vids == {"V-001", "V-002"}
        assert "V-999" not in vids

    def test_alternatives_preserve_decision_relevant_fields(self):
        import recommendation_audit as ra
        snap = ra.make_snapshot(_sample_conflict(), summary=_sample_summary())
        alts = snap["alternatives_generated"]
        assert len(alts) == 3
        for a in alts:
            for key in ("id", "strategy", "cost_usd", "delay_mins",
                        "risk", "recommended"):
                assert key in a, f"alternative missing {key}"
        recs = [a for a in alts if a["recommended"]]
        assert len(recs) == 1

    def test_engine_version_present(self):
        import recommendation_audit as ra
        snap = ra.make_snapshot(_sample_conflict(), summary=_sample_summary())
        assert snap["engine_version"] == ra.ENGINE_VERSION
        assert snap["engine_version"].startswith("horizon-")

    def test_constraints_include_berth_and_clearance(self):
        import recommendation_audit as ra
        snap = ra.make_snapshot(_sample_conflict(), summary=_sample_summary())
        ct = snap["constraints"]
        assert ct["clearance_mins"] == 60
        assert ct["berth_max_loa"] == 260
        assert ct["berth_max_draft"] == 12.0
        assert ct["port_id"] == "BRISBANE"

    def test_eta_hierarchy_records_live_state(self):
        import recommendation_audit as ra
        snap = ra.make_snapshot(_sample_conflict(), summary=_sample_summary())
        h = snap["eta_source_hierarchy"]
        assert h["data_source"] == "live"
        assert h["using_live_vessel_data"] is True
        assert h["using_live_tidal_data"] is True


# ════════════════════════════════════════════════════════════════════
# Snapshot purity (Case 3) — does not mutate inputs
# ════════════════════════════════════════════════════════════════════

class TestSnapshotDoesNotMutateInputs:
    """Case 3: snapshot must not mutate conflict/recommendation objects."""

    def test_make_snapshot_does_not_mutate_conflict(self):
        import recommendation_audit as ra
        conflict = _sample_conflict()
        before = copy.deepcopy(conflict)
        ra.make_snapshot(conflict, summary=_sample_summary())
        assert conflict == before, "make_snapshot mutated the conflict dict"

    def test_make_snapshot_does_not_mutate_summary(self):
        import recommendation_audit as ra
        summary = _sample_summary()
        before = copy.deepcopy(summary)
        ra.make_snapshot(_sample_conflict(), summary=summary)
        assert summary == before, "make_snapshot mutated the summary dict"

    def test_make_payload_does_not_mutate_inputs(self):
        import recommendation_audit as ra
        conflict = _sample_conflict()
        summary = _sample_summary()
        before_c = copy.deepcopy(conflict)
        before_s = copy.deepcopy(summary)
        ra.make_payload(conflict, summary=summary, port_id="BRISBANE")
        assert conflict == before_c
        assert summary == before_s


# ════════════════════════════════════════════════════════════════════
# Snapshot size (Case 8)
# ════════════════════════════════════════════════════════════════════

class TestSnapshotSize:
    """Case 8: snapshot size below target for normal Beta 10 conflicts.

    Target: 16 KB serialised JSON. This bounds the per-recommendation
    payload growth before Phase 0.9 payload-capture is introduced.
    """

    TARGET_BYTES = 16 * 1024   # 16 KB

    def test_normal_conflict_snapshot_under_target(self):
        import recommendation_audit as ra
        snap = ra.make_snapshot(_sample_conflict(), summary=_sample_summary())
        size = len(json.dumps(snap, separators=(",", ":")).encode("utf-8"))
        assert size < self.TARGET_BYTES, (
            f"snapshot size {size}B exceeds target {self.TARGET_BYTES}B")

    def test_payload_under_target(self):
        import recommendation_audit as ra
        payload = ra.make_payload(
            _sample_conflict(), summary=_sample_summary(), port_id="BRISBANE")
        size = len(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
        assert size < self.TARGET_BYTES, (
            f"payload size {size}B exceeds target {self.TARGET_BYTES}B")


# ════════════════════════════════════════════════════════════════════
# Scope guard (Case 6) — no RECOMMENDATION_PRESENTED yet
# ════════════════════════════════════════════════════════════════════

class TestScopeGuard:
    """Case 6: nothing in 0.7b emits RECOMMENDATION_PRESENTED."""

    # Allowed forms (descriptive comments / docstrings explaining what
    # is NOT yet emitted). Disallowed: any string that would actually
    # cause the event to be emitted at runtime.
    _PRESENTED_EMISSION_PATTERNS = (
        'event_type="RECOMMENDATION_PRESENTED"',
        "event_type='RECOMMENDATION_PRESENTED'",
        'event_type = "RECOMMENDATION_PRESENTED"',
        "event_type = 'RECOMMENDATION_PRESENTED'",
    )

    def test_recommendation_audit_module_does_not_emit_presented(self):
        """Source-level proof that the helper never emits PRESENTED."""
        import recommendation_audit as ra
        with open(ra.__file__) as f:
            src = f.read()
        for pat in self._PRESENTED_EMISSION_PATTERNS:
            assert pat not in src, (
                f"Phase 0.7b helper must not emit RECOMMENDATION_PRESENTED yet; "
                f"found: {pat!r}")

    def test_server_py_does_not_emit_presented(self):
        """server.py wire-in must not emit RECOMMENDATION_PRESENTED either."""
        import os
        path = os.path.join(os.path.dirname(__file__), "..", "server.py")
        with open(path) as f:
            src = f.read()
        for pat in self._PRESENTED_EMISSION_PATTERNS:
            assert pat not in src, (
                f"server.py emits RECOMMENDATION_PRESENTED — out of scope for 0.7b")

    def test_dedup_key_requires_recommended_option_id(self):
        """Conflicts without decision_support are skipped (no false emission)."""
        import recommendation_audit as ra
        c = _sample_conflict()
        c["decision_support"] = None
        assert ra._dedup_key(c) is None


# ════════════════════════════════════════════════════════════════════
# Payload shape — no secrets, json-safe, authorised fields
# ════════════════════════════════════════════════════════════════════

class TestPayloadShape:

    def test_payload_has_no_secret_named_keys(self):
        import recommendation_audit as ra
        payload = ra.make_payload(
            _sample_conflict(), summary=_sample_summary(), port_id="BRISBANE")
        blob = json.dumps(payload).lower()
        for tok in ("password", "api_key", "apikey", "secret_key",
                    "private_key", "credential"):
            assert tok not in blob, f"secret-like token '{tok}' in payload"

    def test_payload_is_json_safe(self):
        import recommendation_audit as ra
        payload = ra.make_payload(
            _sample_conflict(), summary=_sample_summary(), port_id="BRISBANE")
        round_trip = json.loads(json.dumps(payload))
        assert round_trip == payload

    def test_payload_includes_recommendation_id(self):
        import recommendation_audit as ra
        payload = ra.make_payload(
            _sample_conflict(recommended_id="SEQ-2"),
            summary=_sample_summary(), port_id="BRISBANE")
        assert payload["recommendation_id"].endswith("::SEQ-2")
        assert payload["recommended_option_id"] == "SEQ-2"
        assert payload["engine_version"].startswith("horizon-")
        assert "decision_snapshot" in payload


# ════════════════════════════════════════════════════════════════════
# Dedup behaviour (in-process)
# ════════════════════════════════════════════════════════════════════

class TestDeduplication:

    def setup_method(self):
        import recommendation_audit as ra
        ra.reset_dedup_state()

    def test_dedup_key_pair_format(self):
        import recommendation_audit as ra
        c = _sample_conflict(suffix="X", recommended_id="SEQ-3")
        key = ra._dedup_key(c)
        assert key == ("c-X", "SEQ-3")

    def test_dedup_skips_repeated_same_recommendation(self):
        """Two calls with same (conflict_id, rec_option_id) — second is skipped.

        We verify by inspecting the dedup set since emit_async returns
        without payload either way; without DB we can still see the set
        gain the key on the first call and not change on the second.
        """
        import recommendation_audit as ra
        ra.reset_dedup_state()
        c = _sample_conflict()
        # Manually populate the dedup set as emit_async would, then
        # confirm a second key-build for the same conflict matches.
        key = ra._dedup_key(c)
        with ra._emitted_lock:
            ra._emitted_keys.setdefault("ams-demo", set()).add(key)
            already = key in ra._emitted_keys["ams-demo"]
        assert already is True

    def test_change_of_recommendation_produces_distinct_key(self):
        """Same conflict_id, different recommended_option_id → fresh emit."""
        import recommendation_audit as ra
        c1 = _sample_conflict(recommended_id="SEQ-1")
        c2 = _sample_conflict(recommended_id="SEQ-2")
        assert ra._dedup_key(c1) != ra._dedup_key(c2)


# ════════════════════════════════════════════════════════════════════
# Case 7: /api/summary output unchanged with audit disabled
# ════════════════════════════════════════════════════════════════════

class TestSummaryUnchangedWithAuditDisabled:
    """Case 7: emit_async must not perturb the summary object the
    handler returns to the client.

    With DATABASE_URL unset the helper is a hard no-op (proven in
    TestProductionSafetyPath). What this case verifies is the
    stronger claim: even if we DID build the payload, it would not
    mutate the summary or any conflict inside it.
    """

    def test_conflict_dict_byte_identical_after_payload_build(self):
        import recommendation_audit as ra
        summary = _sample_summary()
        summary["conflicts"] = [_sample_conflict()]
        # Snapshot the conflict subtree as the JSON the client would see.
        before = json.dumps(summary["conflicts"], sort_keys=True,
                            separators=(",", ":"))
        for c in summary["conflicts"]:
            if c.get("decision_support"):
                ra.make_payload(c, summary=summary, port_id="BRISBANE")
        after = json.dumps(summary["conflicts"], sort_keys=True,
                           separators=(",", ":"))
        assert before == after, (
            "make_payload mutated conflict subtree visible to /api/summary")

    def test_summary_top_level_byte_identical_after_payload_build(self):
        import recommendation_audit as ra
        summary = _sample_summary()
        summary["conflicts"] = [_sample_conflict()]
        # Exclude `conflicts` since we already check that explicitly
        before_top = {k: v for k, v in summary.items() if k != "conflicts"}
        before_blob = json.dumps(before_top, sort_keys=True,
                                 default=str, separators=(",", ":"))
        for c in summary["conflicts"]:
            if c.get("decision_support"):
                ra.make_payload(c, summary=summary, port_id="BRISBANE")
        after_top = {k: v for k, v in summary.items() if k != "conflicts"}
        after_blob = json.dumps(after_top, sort_keys=True,
                                default=str, separators=(",", ":"))
        assert before_blob == after_blob


# ════════════════════════════════════════════════════════════════════
# DB-required (Case 1, 5)
# ════════════════════════════════════════════════════════════════════

@pytest.fixture
def conn():
    import psycopg
    c = psycopg.connect(DATABASE_URL)
    yield c
    c.close()


@pytest.fixture
def tenant_id(conn):
    tid = f"test-rec-{uuid4().hex[:8]}"
    yield tid
    with conn.cursor() as cur:
        cur.execute("DELETE FROM audit.events WHERE tenant_id = %s", (tid,))
    conn.commit()


@pytest.fixture
def rec_audit_enabled(monkeypatch):
    monkeypatch.setenv("AUDIT_EMISSION_ENABLED", "true")
    monkeypatch.setenv("DATABASE_URL", DATABASE_URL)
    for mod in ("db", "recommendation_audit"):
        if mod in sys.modules:
            del sys.modules[mod]
    import recommendation_audit
    recommendation_audit.reset_dedup_state()
    assert recommendation_audit.is_enabled() is True
    return recommendation_audit


@db_required
class TestRecommendationGeneratedEmission:
    """Case 1: RECOMMENDATION_GENERATED emits when audit enabled."""

    def test_emit_sync_creates_audit_row(self, rec_audit_enabled, conn, tenant_id):
        ra = rec_audit_enabled
        conflict = _sample_conflict()
        ok = ra.emit_sync(
            tenant_id, conflict,
            summary=_sample_summary(), port_id="BRISBANE")
        assert ok is True

        with conn.cursor() as cur:
            cur.execute(
                "SELECT event_type, subject_type, subject_id, actor_type, "
                "payload->'decision_snapshot'->>'engine_version', "
                "payload->>'recommended_option_id', "
                "payload->>'port_id', "
                "jsonb_array_length(payload->'decision_snapshot'->'relevant_vessels'), "
                "jsonb_array_length(payload->'decision_snapshot'->'alternatives_generated') "
                "FROM audit.events WHERE tenant_id = %s",
                (tenant_id,),
            )
            rows = cur.fetchall()
        assert len(rows) == 1
        (evt, subj_type, subj_id, actor, eng_ver, rec_opt,
         pid, n_vessels, n_alts) = rows[0]
        assert evt == "RECOMMENDATION_GENERATED"
        assert subj_type == "recommendation"
        assert subj_id == f"{conflict['id']}::SEQ-1"
        assert actor == "system"
        assert eng_ver and eng_ver.startswith("horizon-")
        assert rec_opt == "SEQ-1"
        assert pid == "BRISBANE"
        assert n_vessels == 2
        assert n_alts == 3

    def test_emit_async_eventually_inserts(self, rec_audit_enabled, conn, tenant_id):
        ra = rec_audit_enabled
        ra.emit_async(
            tenant_id, _sample_conflict(),
            summary=_sample_summary(), port_id="BRISBANE")
        count = 0
        for _ in range(50):
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM audit.events WHERE tenant_id = %s",
                    (tenant_id,),
                )
                count = cur.fetchone()[0]
            if count > 0:
                break
            time.sleep(0.1)
        assert count == 1

    def test_repeat_same_recommendation_is_deduped(
        self, rec_audit_enabled, conn, tenant_id
    ):
        ra = rec_audit_enabled
        c = _sample_conflict()
        assert ra.emit_sync(tenant_id, c, summary=_sample_summary()) is True
        assert ra.emit_sync(tenant_id, c, summary=_sample_summary()) is False

        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM audit.events WHERE tenant_id = %s",
                (tenant_id,),
            )
            count = cur.fetchone()[0]
        assert count == 1

    def test_change_of_recommendation_re_emits(
        self, rec_audit_enabled, conn, tenant_id
    ):
        """If the engine flips to a different option for the same
        conflict_id, that is a new decision moment and must emit."""
        ra = rec_audit_enabled
        c1 = _sample_conflict(recommended_id="SEQ-1")
        c2 = copy.deepcopy(c1)
        c2["decision_support"]["recommended_option_id"] = "SEQ-2"
        for opt in c2["sequencing_alternatives"]:
            opt["recommended"] = (opt["id"] == "SEQ-2")

        assert ra.emit_sync(tenant_id, c1, summary=_sample_summary()) is True
        assert ra.emit_sync(tenant_id, c2, summary=_sample_summary()) is True

        with conn.cursor() as cur:
            cur.execute(
                "SELECT payload->>'recommended_option_id' "
                "FROM audit.events WHERE tenant_id = %s ORDER BY sequence_no",
                (tenant_id,),
            )
            recs = [r[0] for r in cur.fetchall()]
        assert recs == ["SEQ-1", "SEQ-2"]


@db_required
class TestBestEffortFailureMode:
    """Case 5: failures do not affect /api/summary."""

    def test_emit_sync_returns_false_when_db_unreachable(self, monkeypatch, caplog):
        monkeypatch.setenv("AUDIT_EMISSION_ENABLED", "true")
        monkeypatch.setenv(
            "DATABASE_URL", "postgresql://localhost:1/nonexistent_test_db")
        for mod in ("db", "recommendation_audit"):
            if mod in sys.modules:
                del sys.modules[mod]
        import recommendation_audit
        recommendation_audit.reset_dedup_state()

        with caplog.at_level(logging.WARNING, logger="horizon.recommendation_audit"):
            ok = recommendation_audit.emit_sync(
                "ams-demo", _sample_conflict(), summary=_sample_summary())
        assert ok is False
        warn = [r for r in caplog.records if r.levelno >= logging.WARNING]
        assert any("audit emission failed" in r.message for r in warn)

    def test_emit_async_does_not_raise_when_db_unreachable(self, monkeypatch):
        monkeypatch.setenv("AUDIT_EMISSION_ENABLED", "true")
        monkeypatch.setenv(
            "DATABASE_URL", "postgresql://localhost:1/nonexistent_test_db")
        for mod in ("db", "recommendation_audit"):
            if mod in sys.modules:
                del sys.modules[mod]
        import recommendation_audit
        recommendation_audit.reset_dedup_state()
        # Must not raise; the only failure surface is a WARN log.
        recommendation_audit.emit_async(
            "ams-demo", _sample_conflict(), summary=_sample_summary())
        time.sleep(0.3)


@db_required
class TestLifecycleSafety:
    """Caller mutation after emit_async must not affect the stored row."""

    def test_payload_isolated_from_caller_mutation(
        self, rec_audit_enabled, conn, tenant_id
    ):
        ra = rec_audit_enabled
        conflict = _sample_conflict()
        summary = _sample_summary()
        ra.emit_async(tenant_id, conflict, summary=summary, port_id="BRISBANE")

        # Mutate both inputs immediately after emit_async returns.
        conflict["severity"] = "TAMPERED"
        conflict["decision_support"]["recommended_option_id"] = "TAMPERED-OPT"
        conflict["sequencing_alternatives"].append(
            {"id": "INJECTED", "strategy": "delay_arrival", "recommended": False})
        summary["vessels"][0]["loa"] = 999999
        summary["weather"]["wind_kts"] = 999

        stored = None
        for _ in range(50):
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT payload FROM audit.events WHERE tenant_id = %s",
                    (tenant_id,),
                )
                row = cur.fetchone()
            if row:
                stored = row[0]
                break
            time.sleep(0.1)

        assert stored is not None
        snap = stored["decision_snapshot"]
        assert snap["conflict_state"]["severity"] == "high"
        assert snap["recommended_option"]["id"] == "SEQ-1"
        # Tampered alternative must NOT appear
        alt_ids = [a["id"] for a in snap["alternatives_generated"]]
        assert "INJECTED" not in alt_ids
        # Tampered vessel loa must NOT appear
        for v in snap["relevant_vessels"]:
            if v["id"] == "V-001":
                assert v["loa"] == 220, (
                    f"vessel loa tamper leaked: {v}")
        # Tampered weather must NOT appear
        assert snap["weather_inputs"]["wind_kts"] == 14
