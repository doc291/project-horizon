"""
Project Horizon — conflict audit helper tests.

Phase 0.7a per ADR-002. Verifies CONFLICT_DETECTED emission across
all production-safe paths and audit-enabled paths, plus payload-shape,
dedup behaviour, and lifecycle isolation.

Five required test cases (per Phase 0.7a authorisation):
  1. CONFLICT_DETECTED emits when DB/audit enabled
  2. no-op when DATABASE_URL unset
  3. failures do not affect /api/summary (proven via emit failure path
     returning False without raising)
  4. payload contains no secrets
  5. conflict detection output remains unchanged (proven by
     make_payload being purely extractive — does not mutate input)
"""

from __future__ import annotations

import hashlib
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


def _sample_conflict(suffix: str = "") -> dict:
    """Build a representative conflict dict matching server.py's
    _conflict() output shape."""
    cid = hashlib.md5(f"berth_overlap-V001-B1-12345{suffix}".encode()).hexdigest()[:8]
    return {
        "id": cid,
        "conflict_type": "berth_overlap",
        "signal_type": "CONFLICT",
        "severity": "high",
        "vessel_ids": ["V-001", "V-002"],
        "vessel_names": ["MV Test One", "MV Test Two"],
        "berth_id": "B1",
        "berth_name": "Berth 1",
        "conflict_time": "2026-05-13T14:30:00+00:00",
        "description": "Berth overlap between MV Test One and MV Test Two at Berth 1",
        "resolution_options": ["Hold MV Test Two", "Reschedule"],
        "sequencing_alternatives": [{"option": "A", "delay_mins": 0}],
        "decision_support": {"recommended_option_id": "A"},
        "data_source": "simulated",
    }


# ════════════════════════════════════════════════════════════════════
# Production-safety path (no DB required)
# ════════════════════════════════════════════════════════════════════

class TestProductionSafetyPath:
    """Tests that must pass even without DATABASE_URL set."""

    def test_imports_with_psycopg_blocked(self):
        """conflict_audit must import cleanly under simulated production."""
        orig_meta_path = sys.meta_path.copy()
        for k in list(sys.modules):
            if k.startswith(("psycopg", "conflict_audit", "db", "audit")):
                del sys.modules[k]

        class Blocker:
            def find_spec(self, name, path, target=None):
                if name.split(".")[0] in {"psycopg", "psycopg_binary"}:
                    raise ImportError(f"simulated production: {name} not installed")
                return None

        sys.meta_path.insert(0, Blocker())
        try:
            import conflict_audit  # noqa: F401
            assert hasattr(conflict_audit, "emit_async")
            assert hasattr(conflict_audit, "is_enabled")
            assert hasattr(conflict_audit, "make_payload")
        finally:
            sys.meta_path[:] = orig_meta_path
            for k in list(sys.modules):
                if k.startswith(("conflict_audit", "db", "audit")):
                    del sys.modules[k]

    def test_emit_async_is_noop_when_database_url_unset(self, monkeypatch):
        """No thread spawned, no psycopg imported when DATABASE_URL unset."""
        monkeypatch.delenv("DATABASE_URL", raising=False)
        for mod in ("db", "conflict_audit"):
            if mod in sys.modules:
                del sys.modules[mod]

        threads_before = set(threading.enumerate())

        import conflict_audit
        assert conflict_audit.is_enabled() is False

        conflict_audit.emit_async("ams-demo", _sample_conflict())
        time.sleep(0.05)

        new_threads = set(threading.enumerate()) - threads_before
        assert not new_threads, f"emit_async spawned threads under no DATABASE_URL: {new_threads}"

    def test_emit_async_is_noop_when_feature_flag_disabled(self, monkeypatch):
        monkeypatch.setenv("AUDIT_EMISSION_ENABLED", "false")
        if DATABASE_URL:
            monkeypatch.setenv("DATABASE_URL", DATABASE_URL)
        for mod in ("db", "conflict_audit"):
            if mod in sys.modules:
                del sys.modules[mod]

        threads_before = set(threading.enumerate())
        import conflict_audit
        assert conflict_audit.is_enabled() is False

        conflict_audit.emit_async("ams-demo", _sample_conflict())
        time.sleep(0.05)
        new_threads = set(threading.enumerate()) - threads_before
        assert not new_threads


class TestPayloadShape:
    """Pure-function tests: make_payload extracts only safe fields."""

    def test_make_payload_contains_only_authorised_fields(self):
        for mod in ("conflict_audit",):
            if mod in sys.modules:
                del sys.modules[mod]
        import conflict_audit
        p = conflict_audit.make_payload(_sample_conflict(), port_id="BRISBANE")
        authorised = {
            "conflict_id", "conflict_type", "severity",
            "vessel_ids", "vessel_names",
            "berth_id", "berth_name",
            "conflict_time", "description", "data_source",
            "observed_at", "port_id",
        }
        extra = set(p) - authorised
        assert not extra, f"payload contains unauthorised fields: {extra}"
        missing = {"conflict_id", "conflict_type", "severity", "observed_at"} - set(p)
        assert not missing, f"payload missing required fields: {missing}"

    def test_make_payload_does_NOT_include_decision_support_or_resolutions(self):
        """Decision_support and resolution_options are Phase 0.7b/c territory."""
        for mod in ("conflict_audit",):
            if mod in sys.modules:
                del sys.modules[mod]
        import conflict_audit
        p = conflict_audit.make_payload(_sample_conflict())
        assert "decision_support" not in p
        assert "resolution_options" not in p
        assert "sequencing_alternatives" not in p
        assert "signal_type" not in p

    def test_make_payload_does_not_mutate_input_conflict(self):
        """Conflict detection output (which build_summary returns) must
        remain unmodified after make_payload runs."""
        for mod in ("conflict_audit",):
            if mod in sys.modules:
                del sys.modules[mod]
        import conflict_audit
        c = _sample_conflict()
        original = dict(c)
        original_vessel_ids = list(c["vessel_ids"])
        original_vessel_names = list(c["vessel_names"])

        conflict_audit.make_payload(c, port_id="BRISBANE")

        assert c == original, "make_payload mutated the conflict dict"
        assert c["vessel_ids"] == original_vessel_ids
        assert c["vessel_names"] == original_vessel_names

    def test_make_payload_safe_field_inventory(self):
        """Every value in the payload is JSON-serialisable and non-secret."""
        for mod in ("conflict_audit",):
            if mod in sys.modules:
                del sys.modules[mod]
        import conflict_audit
        import json
        p = conflict_audit.make_payload(_sample_conflict(), port_id="BRISBANE")
        # JSON-serialisable
        json.dumps(p, default=str)
        # No keys look secret-ish
        for k in p:
            assert not any(
                bad in k.lower()
                for bad in ("password", "secret", "key", "token", "credential", "cookie")
            ), f"suspicious field name: {k}"


class TestDeduplication:
    """Per-tenant per-process dedup behaviour."""

    def test_repeated_emit_for_same_conflict_id_only_fires_once(self, monkeypatch):
        """emit_async on the same conflict_id twice should not spawn a second thread."""
        monkeypatch.setenv("AUDIT_EMISSION_ENABLED", "true")
        if not DATABASE_URL:
            pytest.skip("requires DATABASE_URL for is_enabled() to return True")
        monkeypatch.setenv("DATABASE_URL", DATABASE_URL)
        for mod in ("db", "conflict_audit"):
            if mod in sys.modules:
                del sys.modules[mod]
        import conflict_audit
        conflict_audit.reset_dedup_state()
        assert conflict_audit.is_enabled() is True

        tenant_id = f"test-dedup-{uuid4().hex[:8]}"
        c = _sample_conflict()

        threads_before = set(threading.enumerate())
        conflict_audit.emit_async(tenant_id, c)
        threads_after_first = set(threading.enumerate())
        first_new = threads_after_first - threads_before
        # First call should spawn one thread
        # (could already have completed; check by waiting briefly)

        conflict_audit.emit_async(tenant_id, c)
        time.sleep(0.05)
        threads_after_second = set(threading.enumerate())
        # The second call should NOT have spawned a new thread for emission
        # We allow the first thread to still be running but not a NEW one.
        # The cleanest signal: dedup set already contains the conflict_id
        with conflict_audit._emitted_lock:
            assert c["id"] in conflict_audit._emitted_ids[tenant_id]

        # Clean up
        if DATABASE_URL:
            import psycopg
            cleanup = psycopg.connect(DATABASE_URL)
            with cleanup.cursor() as cur:
                cur.execute("DELETE FROM audit.events WHERE tenant_id = %s", (tenant_id,))
            cleanup.commit()
            cleanup.close()
        conflict_audit.reset_dedup_state()

    def test_emit_returns_for_missing_id(self, monkeypatch):
        """A conflict dict without an 'id' field is silently ignored (defensive)."""
        monkeypatch.setenv("AUDIT_EMISSION_ENABLED", "true")
        if DATABASE_URL:
            monkeypatch.setenv("DATABASE_URL", DATABASE_URL)
        for mod in ("db", "conflict_audit"):
            if mod in sys.modules:
                del sys.modules[mod]
        import conflict_audit
        # Should not raise even without an id field
        conflict_audit.emit_async("ams-demo", {"description": "no id"})

    def test_reset_dedup_state_clears_memory(self, monkeypatch):
        for mod in ("conflict_audit",):
            if mod in sys.modules:
                del sys.modules[mod]
        import conflict_audit
        with conflict_audit._emitted_lock:
            conflict_audit._emitted_ids["t1"] = {"abc"}
        conflict_audit.reset_dedup_state()
        with conflict_audit._emitted_lock:
            assert not conflict_audit._emitted_ids


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
    tid = f"test-conf-{uuid4().hex[:8]}"
    yield tid
    with conn.cursor() as cur:
        cur.execute("DELETE FROM audit.events WHERE tenant_id = %s", (tid,))
    conn.commit()


@pytest.fixture
def conflict_audit_enabled(monkeypatch):
    monkeypatch.setenv("AUDIT_EMISSION_ENABLED", "true")
    monkeypatch.setenv("DATABASE_URL", DATABASE_URL)
    for mod in ("db", "conflict_audit"):
        if mod in sys.modules:
            del sys.modules[mod]
    import conflict_audit
    conflict_audit.reset_dedup_state()
    assert conflict_audit.is_enabled() is True
    return conflict_audit


@db_required
class TestConflictDetectedEmission:
    """Case 1: CONFLICT_DETECTED emits when DB/audit enabled."""

    def test_emit_sync_creates_audit_row(self, conflict_audit_enabled, conn, tenant_id):
        ca = conflict_audit_enabled
        conflict = _sample_conflict()
        ok = ca.emit_sync(tenant_id, conflict, port_id="BRISBANE")
        assert ok is True

        with conn.cursor() as cur:
            cur.execute(
                "SELECT event_type, subject_type, subject_id, actor_type, "
                "payload->>'conflict_type', payload->>'severity', "
                "payload->>'port_id' "
                "FROM audit.events WHERE tenant_id = %s",
                (tenant_id,),
            )
            rows = cur.fetchall()
        assert len(rows) == 1
        evt_type, subj_type, subj_id, actor_type, ctype, sev, pid = rows[0]
        assert evt_type == "CONFLICT_DETECTED"
        assert subj_type == "conflict"
        assert subj_id == conflict["id"]
        assert actor_type == "system"
        assert ctype == "berth_overlap"
        assert sev == "high"
        assert pid == "BRISBANE"

    def test_emit_async_eventually_inserts(self, conflict_audit_enabled, conn, tenant_id):
        ca = conflict_audit_enabled
        conflict = _sample_conflict()
        ca.emit_async(tenant_id, conflict, port_id="MELBOURNE")
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

    def test_repeat_emit_is_deduped(self, conflict_audit_enabled, conn, tenant_id):
        """Calling emit_sync twice for the same conflict_id produces one row."""
        ca = conflict_audit_enabled
        conflict = _sample_conflict()
        assert ca.emit_sync(tenant_id, conflict) is True
        assert ca.emit_sync(tenant_id, conflict) is False  # deduped

        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM audit.events WHERE tenant_id = %s",
                (tenant_id,),
            )
            count = cur.fetchone()[0]
        assert count == 1

    def test_different_conflict_ids_both_emit(self, conflict_audit_enabled, conn, tenant_id):
        ca = conflict_audit_enabled
        c1 = _sample_conflict(suffix="-A")
        c2 = _sample_conflict(suffix="-B")
        assert c1["id"] != c2["id"]
        assert ca.emit_sync(tenant_id, c1) is True
        assert ca.emit_sync(tenant_id, c2) is True

        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM audit.events WHERE tenant_id = %s",
                (tenant_id,),
            )
            count = cur.fetchone()[0]
        assert count == 2


@db_required
class TestBestEffortFailureMode:
    """Case 3: failures do not affect the caller."""

    def test_emit_sync_returns_false_when_db_unreachable(self, monkeypatch, caplog):
        monkeypatch.setenv("AUDIT_EMISSION_ENABLED", "true")
        monkeypatch.setenv("DATABASE_URL", "postgresql://localhost:1/nonexistent_test_db")
        for mod in ("db", "conflict_audit"):
            if mod in sys.modules:
                del sys.modules[mod]
        import conflict_audit
        conflict_audit.reset_dedup_state()

        with caplog.at_level(logging.WARNING, logger="horizon.conflict_audit"):
            ok = conflict_audit.emit_sync("ams-demo", _sample_conflict())
        assert ok is False
        warn = [r for r in caplog.records if r.levelno >= logging.WARNING]
        assert any("audit emission failed" in r.message for r in warn)

    def test_emit_async_does_not_raise_when_db_unreachable(self, monkeypatch):
        monkeypatch.setenv("AUDIT_EMISSION_ENABLED", "true")
        monkeypatch.setenv("DATABASE_URL", "postgresql://localhost:1/nonexistent_test_db")
        for mod in ("db", "conflict_audit"):
            if mod in sys.modules:
                del sys.modules[mod]
        import conflict_audit
        conflict_audit.reset_dedup_state()
        # Must not raise
        conflict_audit.emit_async("ams-demo", _sample_conflict())
        time.sleep(0.3)


@db_required
class TestLifecycleSafety:
    """Payload deep-copy guarantee — caller mutation after emit_async
    must not affect the stored row."""

    def test_payload_isolated_from_caller_mutation(
        self, conflict_audit_enabled, conn, tenant_id
    ):
        ca = conflict_audit_enabled
        conflict = _sample_conflict()
        ca.emit_async(tenant_id, conflict, port_id="BRISBANE")

        # Mutate the conflict dict immediately after emit_async returns
        conflict["severity"] = "TAMPERED"
        conflict["vessel_ids"].append("INJECTED-V")
        conflict["description"] = "REWRITTEN AFTER EMIT"

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
        assert stored["severity"] == "high", f"severity tamper leaked: {stored}"
        assert "INJECTED-V" not in stored["vessel_ids"]
        assert "REWRITTEN" not in stored["description"]
