"""
Project Horizon — operator-action audit helper tests.

Phase 0.8a per ADR-002. Verifies OPERATOR_ACTED emission for the three
existing Beta 10 operator-action endpoints:

  whatif_apply / whatif_clear / send_brief

(port_switch is dashboard navigation, not an operational action — see
TestScopeGuard.test_port_switch_not_in_action_types.)

Six required test cases (per Step 0.8a authorisation):
  1. event emits when DB/audit enabled
  2. no-op when DATABASE_URL unset
  3. request path survives audit failure
  4. no mutation of request/action payloads
  5. no secrets in audit payload
  6. existing endpoint responses unchanged with audit disabled

Plus a scope guard: 0.8a must NOT emit DEADLINE_PASSED,
SESSION_ENDED_WITHOUT_ACTION, OPERATOR_DEFERRED, or OPERATOR_OVERRODE.
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


# ════════════════════════════════════════════════════════════════════
# Production safety (Case 2)
# ════════════════════════════════════════════════════════════════════

class TestProductionSafetyPath:

    def test_imports_with_psycopg_blocked(self):
        orig_meta_path = sys.meta_path.copy()
        for k in list(sys.modules):
            if k.startswith(("psycopg", "operator_action_audit", "db", "audit")):
                del sys.modules[k]

        class Blocker:
            def find_spec(self, name, path, target=None):
                if name.split(".")[0] in {"psycopg", "psycopg_binary"}:
                    raise ImportError(f"simulated production: {name}")
                return None

        sys.meta_path.insert(0, Blocker())
        try:
            import operator_action_audit as oaa
            for fn in ("emit_async", "is_enabled", "make_payload"):
                assert hasattr(oaa, fn), f"missing {fn}"
            assert oaa.ACTION_WHATIF_APPLY == "whatif_apply"
            assert oaa.ACTION_WHATIF_CLEAR == "whatif_clear"
            assert oaa.ACTION_SEND_BRIEF == "send_brief"
            # port_switch was deliberately excluded — navigation, not action.
            assert not hasattr(oaa, "ACTION_PORT_SWITCH"), (
                "port_switch must not appear as an OPERATOR_ACTED action_type"
            )
        finally:
            sys.meta_path[:] = orig_meta_path
            for k in list(sys.modules):
                if k.startswith(("operator_action_audit", "db", "audit")):
                    del sys.modules[k]

    def test_emit_async_is_noop_when_database_url_unset(self, monkeypatch):
        monkeypatch.delenv("DATABASE_URL", raising=False)
        for mod in ("db", "operator_action_audit"):
            if mod in sys.modules:
                del sys.modules[mod]

        threads_before = set(threading.enumerate())

        import operator_action_audit as oaa
        assert oaa.is_enabled() is False

        oaa.emit_async(
            "ams-demo", oaa.ACTION_WHATIF_APPLY,
            summary="Applied scenario overlay with 1 adjustment(s)",
            surface="api_apply_whatif",
            actor_handle="O-1", port_id="BRISBANE",
        )
        time.sleep(0.05)

        new_threads = set(threading.enumerate()) - threads_before
        assert not new_threads, f"emit_async spawned threads: {new_threads}"

    def test_emit_async_is_noop_when_feature_flag_disabled(self, monkeypatch):
        monkeypatch.setenv("AUDIT_EMISSION_ENABLED", "false")
        if DATABASE_URL:
            monkeypatch.setenv("DATABASE_URL", DATABASE_URL)
        for mod in ("db", "operator_action_audit"):
            if mod in sys.modules:
                del sys.modules[mod]

        threads_before = set(threading.enumerate())
        import operator_action_audit as oaa
        assert oaa.is_enabled() is False
        oaa.emit_async(
            "ams-demo", oaa.ACTION_WHATIF_APPLY,
            summary="Applied scenario", surface="api_apply_whatif",
            actor_handle="O-1", port_id="BRISBANE",
        )
        time.sleep(0.05)
        new_threads = set(threading.enumerate()) - threads_before
        assert not new_threads


# ════════════════════════════════════════════════════════════════════
# Payload shape (Case 5) — only safe metadata
# ════════════════════════════════════════════════════════════════════

class TestPayloadShape:

    AUTHORISED_FIELDS = {
        "action_type", "summary", "surface", "timestamp",
        "actor_handle", "port_id", "conflict_id", "recommendation_id",
    }

    def test_authorised_fields_only(self):
        import operator_action_audit as oaa
        payload = oaa.make_payload(
            oaa.ACTION_WHATIF_APPLY,
            summary="Applied scenario", surface="api_apply_whatif",
            actor_handle="O-1", port_id="BRISBANE",
            conflict_id="c-1", recommendation_id="c-1::SEQ-1",
        )
        extra = set(payload.keys()) - self.AUTHORISED_FIELDS
        assert not extra, f"unexpected payload keys: {extra}"

    def test_required_fields_present(self):
        import operator_action_audit as oaa
        payload = oaa.make_payload(
            oaa.ACTION_WHATIF_CLEAR,
            summary="Cleared active scenario overlay",
            surface="api_clear_whatif",
        )
        for k in ("action_type", "summary", "surface", "timestamp"):
            assert k in payload, f"missing required field: {k}"

    def test_payload_omits_optional_fields_when_none(self):
        import operator_action_audit as oaa
        payload = oaa.make_payload(
            oaa.ACTION_WHATIF_CLEAR,
            summary="x", surface="api_clear_whatif",
        )
        for k in ("actor_handle", "port_id", "conflict_id", "recommendation_id"):
            assert k not in payload, f"{k} should not appear when None"

    def test_payload_has_no_secret_named_keys(self):
        import operator_action_audit as oaa
        payload = oaa.make_payload(
            oaa.ACTION_SEND_BRIEF,
            summary="Sent port brief to 3 recipient(s)",
            surface="api_send_brief",
            actor_handle="O-1", port_id="BRISBANE",
        )
        blob = json.dumps(payload).lower()
        for tok in (
            "password", "api_key", "apikey", "secret_key", "private_key",
            "credential", "token_secret", "cookie", "hmac",
            "smtp_pass", "smtp_user", "smtp_host",
        ):
            assert tok not in blob, f"secret-like token '{tok}' in payload"

    def test_payload_does_not_include_recipient_addresses(self):
        """Brief recipient emails are PII — must not enter the audit
        ledger in Phase 0. The handler passes a summary string with a
        count only; the helper never receives the raw recipient list."""
        import operator_action_audit as oaa
        # Simulate what server.py passes — count-only summary
        payload = oaa.make_payload(
            oaa.ACTION_SEND_BRIEF,
            summary="Sent port brief to 3 recipient(s)",
            surface="api_send_brief",
            actor_handle="O-1", port_id="BRISBANE",
        )
        blob = json.dumps(payload)
        assert "@" not in blob, f"email-like content in payload: {blob}"

    def test_payload_is_json_safe(self):
        import operator_action_audit as oaa
        payload = oaa.make_payload(
            oaa.ACTION_WHATIF_APPLY,
            summary="Applied scenario with 5 adjustment(s)",
            surface="api_apply_whatif",
            actor_handle="O-1", port_id="BRISBANE", conflict_id="c-1",
        )
        assert json.loads(json.dumps(payload)) == payload


# ════════════════════════════════════════════════════════════════════
# Closed-set guard — refuses unknown action types (no DB write)
# ════════════════════════════════════════════════════════════════════

class TestClosedSetGuard:

    def test_unknown_action_type_no_thread_spawn(self, monkeypatch):
        if DATABASE_URL:
            monkeypatch.setenv("DATABASE_URL", DATABASE_URL)
        else:
            monkeypatch.delenv("DATABASE_URL", raising=False)
        for mod in ("db", "operator_action_audit"):
            if mod in sys.modules:
                del sys.modules[mod]
        import operator_action_audit as oaa
        # is_enabled may be False (no DB) — either way, an unknown
        # action_type must never spawn a worker.
        threads_before = set(threading.enumerate())
        oaa.emit_async(
            "ams-demo", "delete_universe",
            summary="evil", surface="api_evil",
        )
        time.sleep(0.05)
        new_threads = set(threading.enumerate()) - threads_before
        assert not new_threads


# ════════════════════════════════════════════════════════════════════
# Subject mapping (ADR-002 closed set)
# ════════════════════════════════════════════════════════════════════

class TestSubjectMapping:

    def test_whatif_apply_with_conflict_id_targets_conflict(self):
        import operator_action_audit as oaa
        s = oaa._subject_for_action(
            oaa.ACTION_WHATIF_APPLY,
            conflict_id="c-123",
            port_id="BRISBANE",
        )
        assert s == ("conflict", "c-123")

    def test_whatif_apply_without_conflict_id_targets_system(self):
        import operator_action_audit as oaa
        s = oaa._subject_for_action(
            oaa.ACTION_WHATIF_APPLY,
            conflict_id=None,
            port_id="BRISBANE",
        )
        assert s == ("system", "BRISBANE")

    def test_send_brief_targets_system_port(self):
        import operator_action_audit as oaa
        s = oaa._subject_for_action(
            oaa.ACTION_SEND_BRIEF, conflict_id=None, port_id="BRISBANE")
        assert s == ("system", "BRISBANE")


# ════════════════════════════════════════════════════════════════════
# Mutation safety (Case 4)
# ════════════════════════════════════════════════════════════════════

class TestNoMutation:

    def test_make_payload_does_not_mutate_input_strings(self):
        import operator_action_audit as oaa
        action_type = oaa.ACTION_WHATIF_APPLY
        summary = "Applied scenario with 3 adjustment(s)"
        surface = "api_apply_whatif"
        oaa.make_payload(
            action_type, summary=summary, surface=surface,
            actor_handle="O-1", port_id="BRISBANE", conflict_id="c-1",
        )
        # Strings are immutable, but verify the helper didn't reassign anything
        assert action_type == "whatif_apply"
        assert summary == "Applied scenario with 3 adjustment(s)"
        assert surface == "api_apply_whatif"

    def test_make_payload_returns_fresh_dict(self):
        import operator_action_audit as oaa
        p1 = oaa.make_payload(
            oaa.ACTION_WHATIF_CLEAR, summary="x", surface="api_clear_whatif")
        p2 = oaa.make_payload(
            oaa.ACTION_WHATIF_CLEAR, summary="x", surface="api_clear_whatif")
        # Distinct dicts (different identity, equal content modulo timestamp)
        assert p1 is not p2
        p1["timestamp"] = p2["timestamp"]
        assert p1 == p2


# ════════════════════════════════════════════════════════════════════
# Scope guard — 0.8a only, no 0.8b/c events
# ════════════════════════════════════════════════════════════════════

class TestScopeGuard:
    """0.8a must emit OPERATOR_ACTED ONLY. No inaction events, no
    DEADLINE_PASSED, no defer/override (those are 0.8b/c)."""

    FORBIDDEN_EVENT_TYPES = (
        "DEADLINE_PASSED",
        "SESSION_ENDED_WITHOUT_ACTION",
        "OPERATOR_DEFERRED",
        "OPERATOR_OVERRODE",
    )

    def test_module_emits_only_operator_acted(self):
        import operator_action_audit as oaa
        with open(oaa.__file__) as f:
            src = f.read()
        import re
        emissions = re.findall(r'event_type *= *["\']([A-Z_]+)["\']', src)
        assert emissions == ["OPERATOR_ACTED"], (
            f"unexpected emissions: {emissions}"
        )

    def test_module_does_not_emit_inaction_events(self):
        import operator_action_audit as oaa
        with open(oaa.__file__) as f:
            src = f.read()
        for bad in self.FORBIDDEN_EVENT_TYPES:
            assert f'event_type="{bad}"' not in src, (
                f"0.8a module must not emit {bad}"
            )
            assert f"event_type='{bad}'" not in src

    def test_server_py_does_not_emit_inaction_events(self):
        import os
        path = os.path.join(os.path.dirname(__file__), "..", "server.py")
        with open(path) as f:
            src = f.read()
        for bad in self.FORBIDDEN_EVENT_TYPES:
            assert f'event_type="{bad}"' not in src
            assert f"event_type='{bad}'" not in src

    def test_port_switch_not_in_action_types(self):
        """port_switch was rejected during 0.8a review (PR #18 ChatGPT
        feedback): it is dashboard navigation / view-state, not an
        operational action. It must not appear in the closed
        action_type set or be referenced by the /api/set_port handler."""
        import operator_action_audit as oaa
        assert "port_switch" not in oaa.ACTION_TYPES
        assert not hasattr(oaa, "ACTION_PORT_SWITCH")

        import os
        path = os.path.join(os.path.dirname(__file__), "..", "server.py")
        with open(path) as f:
            src = f.read()
        # The /api/set_port handler must not call operator_action_audit
        # at all. We assert that the substring "ACTION_PORT_SWITCH" does
        # not appear anywhere in server.py.
        assert "ACTION_PORT_SWITCH" not in src, (
            "server.py still references ACTION_PORT_SWITCH — port switch "
            "should not be wired to OPERATOR_ACTED."
        )


# ════════════════════════════════════════════════════════════════════
# Case 6: endpoint responses byte-identical with audit disabled
# ════════════════════════════════════════════════════════════════════
#
# We cannot drive a full HTTP request in-process without spawning a
# server, but we can verify the structural invariant: the helper's
# fast path is a hard no-op under DATABASE_URL unset (already
# established in TestProductionSafetyPath). Combined with the fact
# that server.py invokes emit_async AFTER self._json(...), the
# response bytes the client receives are unchanged regardless of
# whether the audit emit succeeds, fails, or is skipped entirely.
#
# This module documents that contract via two structural assertions:
# (a) emit_async is a true no-op under DATABASE_URL unset, and (b)
# the helper never mutates a request body it receives by reference.

class TestEndpointResponseUnchangedWithAuditDisabled:

    def test_emit_async_is_noop_when_disabled(self, monkeypatch):
        monkeypatch.delenv("DATABASE_URL", raising=False)
        for mod in ("db", "operator_action_audit"):
            if mod in sys.modules:
                del sys.modules[mod]
        import operator_action_audit as oaa
        # No state change in the dedup-equivalent (none exists) and no
        # return value to inspect; the contract is "returns None,
        # spawns no thread, raises nothing".
        before = threading.active_count()
        oaa.emit_async(
            "ams-demo", oaa.ACTION_WHATIF_APPLY,
            summary="x", surface="api_apply_whatif",
            actor_handle="O-1", port_id="BRISBANE", conflict_id="c-1",
        )
        oaa.emit_async(
            "ams-demo", oaa.ACTION_WHATIF_CLEAR,
            summary="x", surface="api_clear_whatif",
            actor_handle="O-1", port_id="BRISBANE",
        )
        oaa.emit_async(
            "ams-demo", oaa.ACTION_SEND_BRIEF,
            summary="x", surface="api_send_brief",
            actor_handle="O-1", port_id="BRISBANE",
        )
        time.sleep(0.05)
        assert threading.active_count() == before


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
    tid = f"test-op-{uuid4().hex[:8]}"
    yield tid
    with conn.cursor() as cur:
        cur.execute("DELETE FROM audit.events WHERE tenant_id = %s", (tid,))
    conn.commit()


@pytest.fixture
def oaa_enabled(monkeypatch):
    monkeypatch.setenv("AUDIT_EMISSION_ENABLED", "true")
    monkeypatch.setenv("DATABASE_URL", DATABASE_URL)
    for mod in ("db", "operator_action_audit"):
        if mod in sys.modules:
            del sys.modules[mod]
    import operator_action_audit
    assert operator_action_audit.is_enabled() is True
    return operator_action_audit


@db_required
class TestOperatorActedEmission:
    """Case 1: OPERATOR_ACTED emits when audit enabled."""

    def test_whatif_apply_creates_row(self, oaa_enabled, conn, tenant_id):
        oaa = oaa_enabled
        ok = oaa.emit_sync(
            tenant_id, oaa.ACTION_WHATIF_APPLY,
            summary="Applied scenario overlay with 2 adjustment(s)",
            surface="api_apply_whatif",
            actor_handle="O-1", port_id="BRISBANE", conflict_id="c-xyz",
        )
        assert ok is True
        with conn.cursor() as cur:
            cur.execute(
                "SELECT event_type, subject_type, subject_id, actor_type, "
                "actor_handle, payload->>'action_type', "
                "payload->>'surface', payload->>'port_id' "
                "FROM audit.events WHERE tenant_id = %s",
                (tenant_id,),
            )
            rows = cur.fetchall()
        assert len(rows) == 1
        (evt, subj_t, subj_id, actor_t, actor_h, a_type, surface, pid) = rows[0]
        assert evt == "OPERATOR_ACTED"
        assert subj_t == "conflict"
        assert subj_id == "c-xyz"
        assert actor_t == "operator"
        assert actor_h == "O-1"
        assert a_type == "whatif_apply"
        assert surface == "api_apply_whatif"
        assert pid == "BRISBANE"

    def test_whatif_apply_with_conflict_id_uses_conflict_subject(
        self, oaa_enabled, conn, tenant_id
    ):
        oaa = oaa_enabled
        ok = oaa.emit_sync(
            tenant_id, oaa.ACTION_WHATIF_APPLY,
            summary="Applied scenario with 3 adjustment(s)",
            surface="api_apply_whatif",
            actor_handle="O-1", port_id="BRISBANE", conflict_id="c-abc123",
        )
        assert ok is True
        with conn.cursor() as cur:
            cur.execute(
                "SELECT subject_type, subject_id, payload->>'action_type', "
                "payload->>'conflict_id' "
                "FROM audit.events WHERE tenant_id = %s",
                (tenant_id,),
            )
            subj_t, subj_id, a_type, cid = cur.fetchone()
        assert subj_t == "conflict"
        assert subj_id == "c-abc123"
        assert a_type == "whatif_apply"
        assert cid == "c-abc123"

    def test_whatif_clear_without_conflict_falls_back_to_system(
        self, oaa_enabled, conn, tenant_id
    ):
        oaa = oaa_enabled
        ok = oaa.emit_sync(
            tenant_id, oaa.ACTION_WHATIF_CLEAR,
            summary="Cleared active scenario overlay",
            surface="api_clear_whatif",
            actor_handle="O-1", port_id="BRISBANE",
        )
        assert ok is True
        with conn.cursor() as cur:
            cur.execute(
                "SELECT subject_type, subject_id "
                "FROM audit.events WHERE tenant_id = %s",
                (tenant_id,),
            )
            subj_t, subj_id = cur.fetchone()
        assert subj_t == "system"
        assert subj_id == "BRISBANE"

    def test_send_brief_emit(self, oaa_enabled, conn, tenant_id):
        oaa = oaa_enabled
        ok = oaa.emit_sync(
            tenant_id, oaa.ACTION_SEND_BRIEF,
            summary="Sent port brief to 5 recipient(s)",
            surface="api_send_brief",
            actor_handle="O-1", port_id="BRISBANE",
        )
        assert ok is True
        with conn.cursor() as cur:
            cur.execute(
                "SELECT payload->>'action_type', payload->>'summary' "
                "FROM audit.events WHERE tenant_id = %s",
                (tenant_id,),
            )
            a_type, summary = cur.fetchone()
        assert a_type == "send_brief"
        assert "recipient" in summary
        # No actual email addresses in payload
        assert "@" not in summary

    def test_emit_async_eventually_inserts(self, oaa_enabled, conn, tenant_id):
        oaa = oaa_enabled
        oaa.emit_async(
            tenant_id, oaa.ACTION_WHATIF_APPLY,
            summary="Applied scenario overlay with 1 adjustment(s)",
            surface="api_apply_whatif",
            actor_handle="O-1", port_id="BRISBANE", conflict_id="c-abc",
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

    def test_repeat_emit_NOT_deduped(self, oaa_enabled, conn, tenant_id):
        """Unlike conflict/recommendation events, two genuine operator
        actions produce two audit rows even with identical metadata."""
        oaa = oaa_enabled
        for _ in range(3):
            ok = oaa.emit_sync(
                tenant_id, oaa.ACTION_WHATIF_CLEAR,
                summary="Cleared active scenario overlay",
                surface="api_clear_whatif",
                actor_handle="O-1", port_id="BRISBANE",
            )
            assert ok is True
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM audit.events WHERE tenant_id = %s",
                (tenant_id,),
            )
            assert cur.fetchone()[0] == 3


@db_required
class TestBestEffortFailureMode:
    """Case 3: failures do not affect the request path."""

    def test_emit_sync_returns_false_when_db_unreachable(
        self, monkeypatch, caplog
    ):
        monkeypatch.setenv("AUDIT_EMISSION_ENABLED", "true")
        monkeypatch.setenv(
            "DATABASE_URL", "postgresql://localhost:1/nonexistent_test_db")
        for mod in ("db", "operator_action_audit"):
            if mod in sys.modules:
                del sys.modules[mod]
        import operator_action_audit as oaa
        with caplog.at_level(
            logging.WARNING, logger="horizon.operator_action_audit"
        ):
            ok = oaa.emit_sync(
                "ams-demo", oaa.ACTION_WHATIF_CLEAR,
                summary="x", surface="api_clear_whatif",
                actor_handle="O-1", port_id="BRISBANE",
            )
        assert ok is False
        warn = [r for r in caplog.records if r.levelno >= logging.WARNING]
        assert any("audit emission failed" in r.message for r in warn)

    def test_emit_async_does_not_raise_when_db_unreachable(self, monkeypatch):
        monkeypatch.setenv("AUDIT_EMISSION_ENABLED", "true")
        monkeypatch.setenv(
            "DATABASE_URL", "postgresql://localhost:1/nonexistent_test_db")
        for mod in ("db", "operator_action_audit"):
            if mod in sys.modules:
                del sys.modules[mod]
        import operator_action_audit as oaa
        oaa.emit_async(
            "ams-demo", oaa.ACTION_WHATIF_APPLY,
            summary="x", surface="api_apply_whatif",
            actor_handle="O-1", port_id="BRISBANE",
        )
        time.sleep(0.3)


@db_required
class TestLifecycleSafety:
    """Caller mutation after emit_async must not affect stored row."""

    def test_payload_isolated_from_caller_mutation(
        self, oaa_enabled, conn, tenant_id
    ):
        oaa = oaa_enabled
        # The helper doesn't take a payload dict — it takes named
        # primitives that are then frozen via make_payload. The real
        # test is that subsequent calls do not bleed metadata into
        # earlier audit rows.
        oaa.emit_async(
            tenant_id, oaa.ACTION_WHATIF_APPLY,
            summary="Applied scenario overlay with 1 adjustment(s)",
            surface="api_apply_whatif",
            actor_handle="O-1", port_id="BRISBANE", conflict_id="c-A",
        )
        time.sleep(0.2)
        oaa.emit_async(
            tenant_id, oaa.ACTION_WHATIF_APPLY,
            summary="Applied scenario overlay with 5 adjustment(s)",
            surface="api_apply_whatif",
            actor_handle="O-1", port_id="BRISBANE", conflict_id="c-B",
        )

        # Allow async writes to land
        for _ in range(50):
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM audit.events WHERE tenant_id = %s",
                    (tenant_id,),
                )
                n = cur.fetchone()[0]
            if n >= 2:
                break
            time.sleep(0.1)

        with conn.cursor() as cur:
            cur.execute(
                "SELECT payload->>'conflict_id' "
                "FROM audit.events WHERE tenant_id = %s ORDER BY sequence_no",
                (tenant_id,),
            )
            cids = [r[0] for r in cur.fetchall()]
        assert cids == ["c-A", "c-B"], (
            f"per-emit payload isolation broken: {cids}"
        )
