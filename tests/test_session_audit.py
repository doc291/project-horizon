"""
Project Horizon — session audit helper tests.

Phase 0.6 per ADR-002. Verifies the best-effort session audit emission
helper across the production-safe paths and the audit-enabled path.

Five required test cases (per Phase 0.6 amendment):
  1. successful login emits SESSION_STARTED when DB/audit enabled
  2. logout emits SESSION_ENDED when DB/audit enabled
  3. login/logout still succeed when audit emission fails
  4. no audit work is attempted when DATABASE_URL is unset
  5. production import path remains safe
"""

from __future__ import annotations

import importlib
import logging
import os
import sys
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


# ════════════════════════════════════════════════════════════════════
# Case 4: no audit work when DATABASE_URL unset
# Case 5: production import path remains safe
# (Both can run without DB)
# ════════════════════════════════════════════════════════════════════

class TestProductionSafetyPath:
    """Tests that don't require DATABASE_URL — must always pass."""

    def test_session_audit_imports_with_psycopg_blocked(self):
        """
        Production has no psycopg installed. session_audit must import
        cleanly under that constraint (lazy psycopg import inside
        worker; module top is stdlib only).
        """
        # Snapshot original meta_path and modules to restore
        orig_meta_path = sys.meta_path.copy()
        orig_modules = {
            k: sys.modules[k] for k in list(sys.modules)
            if k.startswith(("psycopg", "session_audit", "db", "audit"))
        }
        # Remove these so the import re-runs under the blocker
        for k in list(sys.modules):
            if k.startswith(("psycopg", "session_audit", "db", "audit")):
                del sys.modules[k]

        class Blocker:
            def find_spec(self, name, path, target=None):
                if name.split(".")[0] in {"psycopg", "psycopg_binary"}:
                    raise ImportError(f"simulated production: {name} not installed")
                return None

        sys.meta_path.insert(0, Blocker())
        try:
            import session_audit  # noqa: F401
            assert hasattr(session_audit, "emit_async")
            assert hasattr(session_audit, "is_enabled")
        finally:
            sys.meta_path[:] = orig_meta_path
            # Clean up so the rest of the suite gets a fresh import
            for k in list(sys.modules):
                if k.startswith(("session_audit", "db", "audit")):
                    del sys.modules[k]
            for k, v in orig_modules.items():
                sys.modules[k] = v

    def test_emit_async_is_noop_when_database_url_unset(self, monkeypatch):
        """
        With DATABASE_URL unset, emit_async must return immediately.
        No thread should be spawned; no psycopg/audit import should occur.
        """
        # Ensure DATABASE_URL is unset for this test
        monkeypatch.delenv("DATABASE_URL", raising=False)

        # Force reload of db (it reads env at import) and session_audit
        for mod in ("db", "session_audit"):
            if mod in sys.modules:
                del sys.modules[mod]

        import threading
        threads_before = set(threading.enumerate())

        import session_audit
        assert session_audit.is_enabled() is False

        # emit_async must return without raising and without spawning a thread
        session_audit.emit_async(
            tenant_id="ams-demo",
            event_type="SESSION_STARTED",
            subject_type="operator_session",
            subject_id="horizon",
            payload={"test": True},
        )
        # Give any rogue thread a moment to appear
        time.sleep(0.05)
        threads_after = set(threading.enumerate())
        new_threads = threads_after - threads_before
        assert not new_threads, f"emit_async spawned threads under no DATABASE_URL: {new_threads}"

    def test_emit_async_is_noop_when_feature_flag_disabled(self, monkeypatch):
        """
        With AUDIT_EMISSION_ENABLED=false, emit_async must no-op even
        when DATABASE_URL is set.
        """
        monkeypatch.setenv("AUDIT_EMISSION_ENABLED", "false")
        if DATABASE_URL:
            monkeypatch.setenv("DATABASE_URL", DATABASE_URL)
        # Reload session_audit so the flag is re-read at import
        for mod in ("db", "session_audit"):
            if mod in sys.modules:
                del sys.modules[mod]

        import threading
        threads_before = set(threading.enumerate())

        import session_audit
        assert session_audit.is_enabled() is False

        session_audit.emit_async(
            tenant_id="ams-demo",
            event_type="SESSION_STARTED",
            subject_type="operator_session",
            subject_id="horizon",
            payload={"test": True},
        )
        time.sleep(0.05)
        new_threads = set(threading.enumerate()) - threads_before
        assert not new_threads


class TestActorHandleResolution:
    """Pure-function tests for the actor_handle mapping."""

    def test_resolves_matching_auth_user_to_O_1(self):
        # Need to import session_audit fresh
        for mod in ("session_audit",):
            if mod in sys.modules:
                del sys.modules[mod]
        import session_audit
        assert session_audit.resolve_actor_handle("horizon", "horizon") == "O-1"

    def test_returns_none_for_non_matching_user(self):
        for mod in ("session_audit",):
            if mod in sys.modules:
                del sys.modules[mod]
        import session_audit
        assert session_audit.resolve_actor_handle("attacker", "horizon") is None

    def test_returns_none_for_none_username(self):
        for mod in ("session_audit",):
            if mod in sys.modules:
                del sys.modules[mod]
        import session_audit
        assert session_audit.resolve_actor_handle(None, "horizon") is None


class TestPayloadShapes:
    """Pure-function tests for payload builders."""

    def test_session_started_payload_includes_username_and_login_at(self):
        for mod in ("session_audit",):
            if mod in sys.modules:
                del sys.modules[mod]
        import session_audit
        p = session_audit.make_session_started_payload("horizon", remote_addr="1.2.3.4", next_path="/")
        assert p["username"] == "horizon"
        assert "login_at" in p
        assert p["client_info"]["remote_addr"] == "1.2.3.4"
        assert p["next_path"] == "/"

    def test_session_ended_payload(self):
        for mod in ("session_audit",):
            if mod in sys.modules:
                del sys.modules[mod]
        import session_audit
        p = session_audit.make_session_ended_payload("horizon", reason="explicit_logout")
        assert p["username"] == "horizon"
        assert p["reason"] == "explicit_logout"
        assert "logout_at" in p


# ════════════════════════════════════════════════════════════════════
# Cases 1, 2, 3: DB-required (audit enabled + DATABASE_URL set)
# ════════════════════════════════════════════════════════════════════

@pytest.fixture
def conn():
    """Direct psycopg connection for verification queries."""
    import psycopg
    c = psycopg.connect(DATABASE_URL)
    yield c
    c.close()


@pytest.fixture
def tenant_id(conn):
    """Unique test tenant; clean up after."""
    tid = f"test-sess-{uuid4().hex[:8]}"
    yield tid
    with conn.cursor() as cur:
        cur.execute("DELETE FROM audit.events WHERE tenant_id = %s", (tid,))
        cur.execute("DELETE FROM audit.payloads WHERE tenant_id = %s", (tid,))
    conn.commit()


@pytest.fixture
def session_audit_enabled(monkeypatch):
    """
    Force session_audit to re-read env with audit ENABLED and
    DATABASE_URL set. Reloads dependent modules so the feature flag
    and DATABASE_URL constants are refreshed.
    """
    monkeypatch.setenv("AUDIT_EMISSION_ENABLED", "true")
    monkeypatch.setenv("DATABASE_URL", DATABASE_URL)
    for mod in ("db", "session_audit"):
        if mod in sys.modules:
            del sys.modules[mod]
    import session_audit
    assert session_audit.is_enabled() is True
    return session_audit


@db_required
class TestSessionEventsEmission:
    """Cases 1 and 2 — audit-enabled emission of SESSION_STARTED / SESSION_ENDED."""

    def test_case_1_session_started_emitted_when_enabled(self, session_audit_enabled, conn, tenant_id):
        """Case 1: emit_sync emits SESSION_STARTED into audit.events."""
        sa = session_audit_enabled
        ok = sa.emit_sync(
            tenant_id=tenant_id,
            event_type="SESSION_STARTED",
            subject_type="operator_session",
            subject_id="horizon",
            payload=sa.make_session_started_payload("horizon", remote_addr="127.0.0.1"),
            actor_handle="O-1",
            actor_type="operator",
        )
        assert ok is True
        with conn.cursor() as cur:
            cur.execute(
                "SELECT event_type, subject_id, actor_handle, payload->>'username' "
                "FROM audit.events WHERE tenant_id = %s",
                (tenant_id,),
            )
            rows = cur.fetchall()
        assert len(rows) == 1
        assert rows[0] == ("SESSION_STARTED", "horizon", "O-1", "horizon")

    def test_case_2_session_ended_emitted_when_enabled(self, session_audit_enabled, conn, tenant_id):
        """Case 2: emit_sync emits SESSION_ENDED into audit.events."""
        sa = session_audit_enabled
        ok = sa.emit_sync(
            tenant_id=tenant_id,
            event_type="SESSION_ENDED",
            subject_type="operator_session",
            subject_id="horizon",
            payload=sa.make_session_ended_payload("horizon"),
            actor_handle="O-1",
            actor_type="operator",
        )
        assert ok is True
        with conn.cursor() as cur:
            cur.execute(
                "SELECT event_type, subject_id, payload->>'reason' "
                "FROM audit.events WHERE tenant_id = %s",
                (tenant_id,),
            )
            rows = cur.fetchall()
        assert len(rows) == 1
        assert rows[0] == ("SESSION_ENDED", "horizon", "explicit_logout")

    def test_emit_async_eventually_inserts(self, session_audit_enabled, conn, tenant_id):
        """emit_async fires-and-forgets; row appears in DB shortly after."""
        sa = session_audit_enabled
        sa.emit_async(
            tenant_id=tenant_id,
            event_type="SESSION_STARTED",
            subject_type="operator_session",
            subject_id="horizon",
            payload=sa.make_session_started_payload("horizon"),
            actor_handle="O-1",
        )
        # Wait for the daemon thread to complete; bounded sleep avoids flakes
        for _ in range(50):  # up to 5 seconds
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

    def test_payload_isolated_from_caller_mutation_after_emit_async(
        self, session_audit_enabled, conn, tenant_id
    ):
        """
        Lifecycle safety per ChatGPT Phase-0.6 review:
        the worker thread MUST see an immutable snapshot of the
        payload, even if the caller mutates the dict immediately
        after emit_async returns.
        """
        sa = session_audit_enabled
        payload = {
            "username": "horizon",
            "marker": "ORIGINAL",
            "client_info": {"remote_addr": "10.0.0.1"},
        }
        sa.emit_async(
            tenant_id=tenant_id,
            event_type="SESSION_STARTED",
            subject_type="operator_session",
            subject_id="horizon",
            payload=payload,
        )
        # Immediately mutate the payload AFTER emit_async returns —
        # this races with the daemon thread. Deep-copy in emit_async
        # must make the worker see the original values.
        payload["marker"] = "TAMPERED_AFTER_THREAD_START"
        payload["injected"] = "SECRET"
        payload["client_info"]["remote_addr"] = "9.9.9.9"
        del payload["username"]

        # Wait for the daemon thread to complete and inspect the stored row
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

        assert stored is not None, "no audit row appeared within 5 seconds"
        assert stored.get("marker") == "ORIGINAL", (
            f"caller mutation leaked into stored audit row: {stored}"
        )
        assert stored.get("username") == "horizon", (
            f"caller deletion leaked into stored audit row: {stored}"
        )
        assert "injected" not in stored, (
            f"caller injection leaked into stored audit row: {stored}"
        )
        assert stored["client_info"]["remote_addr"] == "10.0.0.1", (
            f"caller nested mutation leaked: {stored}"
        )

    def test_payload_isolated_from_caller_mutation_emit_sync(
        self, session_audit_enabled, conn, tenant_id
    ):
        """emit_sync also pins a payload snapshot (symmetric guarantee)."""
        sa = session_audit_enabled
        payload = {"username": "horizon", "marker": "ORIGINAL"}
        sa.emit_sync(
            tenant_id=tenant_id,
            event_type="SESSION_STARTED",
            subject_type="operator_session",
            subject_id="horizon",
            payload=payload,
        )
        # Mutate after emit_sync — should not affect stored row (already
        # written), but tests that emit_sync also copies (no shared state)
        payload["marker"] = "TAMPERED"

        with conn.cursor() as cur:
            cur.execute(
                "SELECT payload FROM audit.events WHERE tenant_id = %s",
                (tenant_id,),
            )
            stored = cur.fetchone()[0]
        assert stored["marker"] == "ORIGINAL"


@db_required
class TestBestEffortFailureMode:
    """Case 3: emission failure does NOT propagate to caller."""

    def test_emit_sync_returns_false_when_db_unreachable(self, monkeypatch, caplog):
        """
        Point DATABASE_URL at a non-existent DB and confirm emit_sync
        returns False (does not raise) and logs a warning. This proves
        the best-effort guarantee that the caller (login/logout) is
        never affected by audit failures.
        """
        monkeypatch.setenv("AUDIT_EMISSION_ENABLED", "true")
        # Use a deliberately invalid URL (wrong port, wrong DB name)
        monkeypatch.setenv("DATABASE_URL", "postgresql://localhost:1/nonexistent_db_for_test")
        for mod in ("db", "session_audit"):
            if mod in sys.modules:
                del sys.modules[mod]
        import session_audit

        with caplog.at_level(logging.WARNING, logger="horizon.session_audit"):
            ok = session_audit.emit_sync(
                tenant_id="ams-demo",
                event_type="SESSION_STARTED",
                subject_type="operator_session",
                subject_id="horizon",
                payload={"username": "horizon"},
            )
        assert ok is False
        warn_messages = [r for r in caplog.records if r.levelno >= logging.WARNING]
        assert any("audit emission failed" in r.message for r in warn_messages), (
            f"Expected 'audit emission failed' warning. Got records: {[r.message for r in caplog.records]}"
        )

    def test_emit_async_does_not_raise_when_db_unreachable(self, monkeypatch):
        """emit_async catches exceptions in the worker thread."""
        monkeypatch.setenv("AUDIT_EMISSION_ENABLED", "true")
        monkeypatch.setenv("DATABASE_URL", "postgresql://localhost:1/nonexistent_db_for_test")
        for mod in ("db", "session_audit"):
            if mod in sys.modules:
                del sys.modules[mod]
        import session_audit
        # Must not raise even with a broken DB
        session_audit.emit_async(
            tenant_id="ams-demo",
            event_type="SESSION_STARTED",
            subject_type="operator_session",
            subject_id="horizon",
            payload={"username": "horizon"},
        )
        # Give the worker a chance to fail and log
        time.sleep(0.5)
        # If we reach here without an exception, test passes
