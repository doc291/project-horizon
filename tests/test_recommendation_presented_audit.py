"""
Project Horizon — recommendation-presented audit helper tests.

Phase 0.7c per ADR-002. Verifies RECOMMENDATION_PRESENTED emission
across the same safety surfaces as 0.7a / 0.7b:

  1. RECOMMENDATION_PRESENTED emits when audit enabled
  2. no-op when DATABASE_URL unset
  3. request path survives audit failure (best-effort)
  4. no mutation of summary / conflict / decision_support
  5. no secrets in payload
  6. /api/summary output remains unchanged with audit disabled
  7. payload contains ONLY the authorised presentation metadata
     fields — no decision_snapshot, no resolution_options, no
     alternatives
  8. dedup: same operator viewing the same recommendation emits once;
     a distinct operator viewing the same recommendation emits a
     fresh event
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


def _sample_conflict(suffix: str = "", recommended_id: str = "SEQ-1") -> dict:
    """A conflict carrying a decision_support recommendation."""
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
        "sequencing_alternatives": [
            {"id": "SEQ-1", "strategy": "delay_arrival",
             "recommended": recommended_id == "SEQ-1"},
            {"id": "SEQ-2", "strategy": "advance_departure",
             "recommended": recommended_id == "SEQ-2"},
        ],
        "decision_support": {
            "recommended_option_id":  recommended_id,
            "recommended_reasoning":  "lowest cost option",
            "confidence":             "high",
            "decision_deadline":      "2026-05-13T12:30:00+00:00",
        },
        "data_source": "simulated",
        "safety_score": 78,
    }


# ════════════════════════════════════════════════════════════════════
# Production safety (Case 2 — no DB required)
# ════════════════════════════════════════════════════════════════════

class TestProductionSafetyPath:

    def test_imports_with_psycopg_blocked(self):
        orig_meta_path = sys.meta_path.copy()
        for k in list(sys.modules):
            if k.startswith((
                "psycopg", "recommendation_presented_audit", "db", "audit",
            )):
                del sys.modules[k]

        class Blocker:
            def find_spec(self, name, path, target=None):
                if name.split(".")[0] in {"psycopg", "psycopg_binary"}:
                    raise ImportError(f"simulated production: {name}")
                return None

        sys.meta_path.insert(0, Blocker())
        try:
            import recommendation_presented_audit as rpa
            assert hasattr(rpa, "emit_async")
            assert hasattr(rpa, "is_enabled")
            assert hasattr(rpa, "make_payload")
            assert rpa.SURFACE_API_SUMMARY == "api_summary"
        finally:
            sys.meta_path[:] = orig_meta_path
            for k in list(sys.modules):
                if k.startswith((
                    "recommendation_presented_audit", "db", "audit",
                )):
                    del sys.modules[k]

    def test_emit_async_is_noop_when_database_url_unset(self, monkeypatch):
        """No thread spawned, no psycopg imported when DATABASE_URL unset."""
        monkeypatch.delenv("DATABASE_URL", raising=False)
        for mod in ("db", "recommendation_presented_audit"):
            if mod in sys.modules:
                del sys.modules[mod]

        threads_before = set(threading.enumerate())

        import recommendation_presented_audit as rpa
        assert rpa.is_enabled() is False

        rpa.emit_async(
            "ams-demo", _sample_conflict(),
            port_id="BRISBANE", actor_handle="O-1",
        )
        time.sleep(0.05)

        new_threads = set(threading.enumerate()) - threads_before
        assert not new_threads, f"emit_async spawned threads: {new_threads}"

    def test_emit_async_is_noop_when_feature_flag_disabled(self, monkeypatch):
        monkeypatch.setenv("AUDIT_EMISSION_ENABLED", "false")
        if DATABASE_URL:
            monkeypatch.setenv("DATABASE_URL", DATABASE_URL)
        for mod in ("db", "recommendation_presented_audit"):
            if mod in sys.modules:
                del sys.modules[mod]

        threads_before = set(threading.enumerate())
        import recommendation_presented_audit as rpa
        assert rpa.is_enabled() is False
        rpa.emit_async(
            "ams-demo", _sample_conflict(),
            port_id="BRISBANE", actor_handle="O-1",
        )
        time.sleep(0.05)
        new_threads = set(threading.enumerate()) - threads_before
        assert not new_threads


# ════════════════════════════════════════════════════════════════════
# Payload shape — only authorised fields (Case 7)
# ════════════════════════════════════════════════════════════════════

class TestPayloadShape:
    """The 0.7c payload is intentionally tiny — presentation metadata
    only. No decision_snapshot. No alternatives. No engine internals."""

    AUTHORISED_FIELDS = {
        "recommendation_id", "conflict_id", "conflict_type", "severity",
        "displayed_at", "surface", "port_id", "actor_handle",
    }

    def test_authorised_fields_only(self):
        import recommendation_presented_audit as rpa
        payload = rpa.make_payload(
            _sample_conflict(),
            port_id="BRISBANE", actor_handle="O-1",
        )
        extra = set(payload.keys()) - self.AUTHORISED_FIELDS
        assert not extra, f"unexpected payload keys: {extra}"

    def test_required_fields_present(self):
        import recommendation_presented_audit as rpa
        payload = rpa.make_payload(
            _sample_conflict(),
            port_id="BRISBANE", actor_handle="O-1",
        )
        for k in ("recommendation_id", "conflict_id", "conflict_type",
                  "severity", "displayed_at", "surface"):
            assert k in payload, f"missing required field: {k}"

    def test_no_decision_snapshot_or_alternatives_in_payload(self):
        import recommendation_presented_audit as rpa
        payload = rpa.make_payload(
            _sample_conflict(),
            port_id="BRISBANE", actor_handle="O-1",
        )
        for forbidden in (
            "decision_snapshot",
            "alternatives_generated",
            "recommended_option",
            "sequencing_alternatives",
            "decision_support",
            "resolution_options",
            "ukc_inputs",
            "tide_inputs",
            "weather_inputs",
            "constraints",
            "engine_version",
        ):
            assert forbidden not in payload, (
                f"0.7c payload must not include {forbidden!r} — that is "
                f"0.7b territory"
            )

    def test_recommendation_id_matches_generated_pattern(self):
        """recommendation_id must use the same `<conflict_id>::<rec_opt_id>`
        format as RECOMMENDATION_GENERATED so events can be joined."""
        import recommendation_presented_audit as rpa
        payload = rpa.make_payload(
            _sample_conflict(suffix="X", recommended_id="SEQ-2"),
            port_id="BRISBANE", actor_handle="O-1",
        )
        assert payload["recommendation_id"] == "c-X::SEQ-2"

    def test_surface_defaults_to_api_summary(self):
        import recommendation_presented_audit as rpa
        payload = rpa.make_payload(_sample_conflict())
        assert payload["surface"] == "api_summary"

    def test_payload_has_no_secret_named_keys(self):
        import recommendation_presented_audit as rpa
        payload = rpa.make_payload(
            _sample_conflict(),
            port_id="BRISBANE", actor_handle="O-1",
        )
        blob = json.dumps(payload).lower()
        for tok in ("password", "api_key", "apikey", "secret_key",
                    "private_key", "credential", "token_secret", "cookie",
                    "hmac"):
            assert tok not in blob, f"secret-like token '{tok}' in payload"

    def test_payload_is_json_safe(self):
        import recommendation_presented_audit as rpa
        payload = rpa.make_payload(
            _sample_conflict(),
            port_id="BRISBANE", actor_handle="O-1",
        )
        assert json.loads(json.dumps(payload)) == payload

    def test_actor_handle_omitted_when_none(self):
        """Anonymous-actor case — no actor_handle key in payload at all."""
        import recommendation_presented_audit as rpa
        payload = rpa.make_payload(_sample_conflict())
        assert "actor_handle" not in payload

    def test_port_id_omitted_when_none(self):
        import recommendation_presented_audit as rpa
        payload = rpa.make_payload(_sample_conflict())
        assert "port_id" not in payload


# ════════════════════════════════════════════════════════════════════
# Mutation safety (Case 4)
# ════════════════════════════════════════════════════════════════════

class TestNoMutation:

    def test_make_payload_does_not_mutate_conflict(self):
        import recommendation_presented_audit as rpa
        conflict = _sample_conflict()
        before = copy.deepcopy(conflict)
        rpa.make_payload(conflict, port_id="BRISBANE", actor_handle="O-1")
        assert conflict == before

    def test_make_payload_does_not_mutate_decision_support(self):
        import recommendation_presented_audit as rpa
        conflict = _sample_conflict()
        ds_before = copy.deepcopy(conflict["decision_support"])
        rpa.make_payload(conflict, port_id="BRISBANE", actor_handle="O-1")
        assert conflict["decision_support"] == ds_before


# ════════════════════════════════════════════════════════════════════
# /api/summary output unchanged with audit disabled (Case 6)
# ════════════════════════════════════════════════════════════════════

class TestSummaryUnchangedWithAuditDisabled:
    """Even if make_payload were invoked, the summary subtree must be
    byte-identical before and after — the helper is purely read-only."""

    def test_conflict_subtree_byte_identical(self):
        import recommendation_presented_audit as rpa
        summary = {"conflicts": [_sample_conflict()]}
        before = json.dumps(summary["conflicts"], sort_keys=True,
                            separators=(",", ":"))
        for c in summary["conflicts"]:
            if c.get("decision_support"):
                rpa.make_payload(
                    c, port_id="BRISBANE", actor_handle="O-1")
        after = json.dumps(summary["conflicts"], sort_keys=True,
                           separators=(",", ":"))
        assert before == after


# ════════════════════════════════════════════════════════════════════
# Dedup (Case 8)
# ════════════════════════════════════════════════════════════════════

class TestDeduplication:

    def setup_method(self):
        import recommendation_presented_audit as rpa
        rpa.reset_dedup_state()

    def test_dedup_key_includes_actor(self):
        import recommendation_presented_audit as rpa
        c = _sample_conflict()
        k_alice = rpa._dedup_key(c, "O-1")
        k_bob   = rpa._dedup_key(c, "O-2")
        assert k_alice != k_bob, "different operators must produce distinct keys"

    def test_dedup_key_pair_format(self):
        import recommendation_presented_audit as rpa
        c = _sample_conflict(suffix="X", recommended_id="SEQ-2")
        assert rpa._dedup_key(c, "O-1") == ("c-X", "SEQ-2", "O-1")

    def test_no_dedup_key_when_no_recommended_option(self):
        import recommendation_presented_audit as rpa
        c = _sample_conflict()
        c["decision_support"] = None
        assert rpa._dedup_key(c, "O-1") is None


# ════════════════════════════════════════════════════════════════════
# Scope guard — no overlap with 0.7a or 0.7b helpers
# ════════════════════════════════════════════════════════════════════

class TestScopeGuard:
    """Phase 0.7c must not touch the 0.7a (CONFLICT_DETECTED) or 0.7b
    (RECOMMENDATION_GENERATED) helpers and must not re-implement the
    decision snapshot."""

    def test_module_does_not_call_recommendation_audit(self):
        import recommendation_presented_audit as rpa
        with open(rpa.__file__) as f:
            src = f.read()
        for bad in (
            "import recommendation_audit",
            "import conflict_audit",
            "from recommendation_audit",
            "from conflict_audit",
            "make_snapshot",
        ):
            assert bad not in src, (
                f"0.7c helper must not depend on / re-implement 0.7a/0.7b: "
                f"found {bad!r}"
            )

    def test_module_only_emits_recommendation_presented(self):
        """Source-level proof of the only event_type emitted."""
        import recommendation_presented_audit as rpa
        with open(rpa.__file__) as f:
            src = f.read()
        import re
        emissions = re.findall(r'event_type *= *["\']([A-Z_]+)["\']', src)
        assert emissions == ["RECOMMENDATION_PRESENTED"], (
            f"unexpected emissions in 0.7c module: {emissions}"
        )


# ════════════════════════════════════════════════════════════════════
# DB-required tests
# ════════════════════════════════════════════════════════════════════

@pytest.fixture
def conn():
    import psycopg
    c = psycopg.connect(DATABASE_URL)
    yield c
    c.close()


@pytest.fixture
def tenant_id(conn):
    tid = f"test-pres-{uuid4().hex[:8]}"
    yield tid
    with conn.cursor() as cur:
        cur.execute("DELETE FROM audit.events WHERE tenant_id = %s", (tid,))
    conn.commit()


@pytest.fixture
def rpa_enabled(monkeypatch):
    monkeypatch.setenv("AUDIT_EMISSION_ENABLED", "true")
    monkeypatch.setenv("DATABASE_URL", DATABASE_URL)
    for mod in ("db", "recommendation_presented_audit"):
        if mod in sys.modules:
            del sys.modules[mod]
    import recommendation_presented_audit
    recommendation_presented_audit.reset_dedup_state()
    assert recommendation_presented_audit.is_enabled() is True
    return recommendation_presented_audit


@db_required
class TestRecommendationPresentedEmission:
    """Case 1: RECOMMENDATION_PRESENTED emits when audit enabled."""

    def test_emit_sync_creates_audit_row(self, rpa_enabled, conn, tenant_id):
        rpa = rpa_enabled
        ok = rpa.emit_sync(
            tenant_id, _sample_conflict(),
            port_id="BRISBANE", actor_handle="O-1",
        )
        assert ok is True

        with conn.cursor() as cur:
            cur.execute(
                "SELECT event_type, subject_type, subject_id, actor_type, "
                "actor_handle, payload->>'conflict_type', "
                "payload->>'severity', payload->>'surface', "
                "payload->>'port_id', payload->>'actor_handle' "
                "FROM audit.events WHERE tenant_id = %s",
                (tenant_id,),
            )
            rows = cur.fetchall()
        assert len(rows) == 1
        (evt, subj_type, subj_id, actor_type, actor_handle,
         ctype, sev, surface, pid, payload_actor) = rows[0]
        assert evt == "RECOMMENDATION_PRESENTED"
        assert subj_type == "recommendation"
        assert subj_id == "c-one::SEQ-1"
        assert actor_type == "operator"
        assert actor_handle == "O-1"
        assert ctype == "berth_overlap"
        assert sev == "high"
        assert surface == "api_summary"
        assert pid == "BRISBANE"
        assert payload_actor == "O-1"

    def test_emit_async_eventually_inserts(self, rpa_enabled, conn, tenant_id):
        rpa = rpa_enabled
        rpa.emit_async(
            tenant_id, _sample_conflict(),
            port_id="BRISBANE", actor_handle="O-1",
        )
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

    def test_repeat_same_operator_is_deduped(
        self, rpa_enabled, conn, tenant_id
    ):
        rpa = rpa_enabled
        c = _sample_conflict()
        assert rpa.emit_sync(tenant_id, c, actor_handle="O-1") is True
        assert rpa.emit_sync(tenant_id, c, actor_handle="O-1") is False
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM audit.events WHERE tenant_id = %s",
                (tenant_id,),
            )
            assert cur.fetchone()[0] == 1

    def test_different_operator_emits_fresh(self, rpa_enabled, conn, tenant_id):
        """Same recommendation, distinct actor → distinct PRESENTED event."""
        rpa = rpa_enabled
        c = _sample_conflict()
        assert rpa.emit_sync(tenant_id, c, actor_handle="O-1") is True
        assert rpa.emit_sync(tenant_id, c, actor_handle="O-2") is True
        with conn.cursor() as cur:
            cur.execute(
                "SELECT actor_handle FROM audit.events WHERE tenant_id = %s "
                "ORDER BY sequence_no",
                (tenant_id,),
            )
            actors = [r[0] for r in cur.fetchall()]
        assert actors == ["O-1", "O-2"]

    def test_anonymous_actor_emits_as_system(
        self, rpa_enabled, conn, tenant_id
    ):
        """If actor_handle is None, actor_type falls back to system."""
        rpa = rpa_enabled
        ok = rpa.emit_sync(
            tenant_id, _sample_conflict(),
            port_id="BRISBANE", actor_handle=None,
        )
        assert ok is True
        with conn.cursor() as cur:
            cur.execute(
                "SELECT actor_type, actor_handle, "
                "payload ? 'actor_handle' "
                "FROM audit.events WHERE tenant_id = %s",
                (tenant_id,),
            )
            actor_type, actor_handle, has_key = cur.fetchone()
        assert actor_type == "system"
        assert actor_handle is None
        # The payload should NOT contain an actor_handle key at all when None
        assert has_key is False


@db_required
class TestBestEffortFailureMode:
    """Case 3: failures do not affect /api/summary."""

    def test_emit_sync_returns_false_when_db_unreachable(
        self, monkeypatch, caplog
    ):
        monkeypatch.setenv("AUDIT_EMISSION_ENABLED", "true")
        monkeypatch.setenv(
            "DATABASE_URL", "postgresql://localhost:1/nonexistent_test_db")
        for mod in ("db", "recommendation_presented_audit"):
            if mod in sys.modules:
                del sys.modules[mod]
        import recommendation_presented_audit as rpa
        rpa.reset_dedup_state()

        with caplog.at_level(
            logging.WARNING, logger="horizon.recommendation_presented_audit"
        ):
            ok = rpa.emit_sync(
                "ams-demo", _sample_conflict(),
                port_id="BRISBANE", actor_handle="O-1",
            )
        assert ok is False
        warn = [r for r in caplog.records if r.levelno >= logging.WARNING]
        assert any("audit emission failed" in r.message for r in warn)

    def test_emit_async_does_not_raise_when_db_unreachable(self, monkeypatch):
        monkeypatch.setenv("AUDIT_EMISSION_ENABLED", "true")
        monkeypatch.setenv(
            "DATABASE_URL", "postgresql://localhost:1/nonexistent_test_db")
        for mod in ("db", "recommendation_presented_audit"):
            if mod in sys.modules:
                del sys.modules[mod]
        import recommendation_presented_audit as rpa
        rpa.reset_dedup_state()
        rpa.emit_async(
            "ams-demo", _sample_conflict(),
            port_id="BRISBANE", actor_handle="O-1",
        )
        time.sleep(0.3)


@db_required
class TestLifecycleSafety:
    """Caller mutation after emit_async must not affect the stored row."""

    def test_payload_isolated_from_caller_mutation(
        self, rpa_enabled, conn, tenant_id
    ):
        rpa = rpa_enabled
        conflict = _sample_conflict()
        rpa.emit_async(
            tenant_id, conflict,
            port_id="BRISBANE", actor_handle="O-1",
        )

        # Mutate the conflict immediately after emit_async returns.
        conflict["severity"] = "TAMPERED"
        conflict["conflict_type"] = "REWRITTEN"
        conflict["decision_support"]["recommended_option_id"] = "TAMPER"

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
        assert stored["severity"] == "high"
        assert stored["conflict_type"] == "berth_overlap"
        # subject_id was already pinned before tamper:
        assert stored["recommendation_id"] == "c-one::SEQ-1"
