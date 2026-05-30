"""
Beta 11 Phase 1 backend tests: decision state model, state machine,
concurrency, flag gating, and migration validation.

These tests exercise the in memory backend (no DATABASE_URL needed). The
database backend is validated separately by the migration offline SQL check
and is import guarded so it never runs here.
"""

from __future__ import annotations

import os
import sys
import threading
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

os.environ.setdefault("HORIZON_USER", "test")
os.environ.setdefault("HORIZON_PASS", "test")
os.environ.setdefault("TOKEN_SECRET", "x" * 32)

import beta11_decision as b11


def _stk():
    return [
        {"role": "TOWAGE",   "display_name": "Tug Alpha", "action_label": "Reassign tug"},
        {"role": "PILOTAGE", "display_name": "Pilot One", "action_label": "Amend window"},
        {"role": "TERMINAL", "display_name": "Berth 4",   "action_label": "Confirm ready"},
    ]


@pytest.fixture(autouse=True)
def _clean():
    b11.reset_for_tests()
    yield
    b11.reset_for_tests()


# ── State model ─────────────────────────────────────────────────────────────
class TestStateModel:
    def test_issue_creates_propagated_decision(self):
        d = b11.issue_decision("C1", "O-1", _stk())
        assert d["decision_state"] == b11.STATE_PROPAGATED
        assert d["conflict_id"] == "C1"
        assert len(d["required_stakeholders"]) == 3
        assert all(s["status"] == b11.ST_AWAITING for s in d["required_stakeholders"])
        assert all(s["simulated"] is True for s in d["required_stakeholders"])

    def test_issue_records_propagation_log(self):
        d = b11.issue_decision("C1", "O-1", _stk())
        events = [e["event"] for e in d["propagation_log"]]
        assert "issued" in events and "propagated" in events

    def test_decision_id_is_deterministic_shape(self):
        d = b11.issue_decision("C1", "O-1", _stk())
        assert d["decision_id"].startswith("D-")

    def test_predicted_impact_carried_and_labelled(self):
        d = b11.issue_decision("C1", "O-1", _stk(),
                               predicted_impact={"value": "x", "label": "Simulated"})
        assert d["predicted_impact"]["label"] == "Simulated"


# ── State machine transitions ───────────────────────────────────────────────
class TestStateMachine:
    def test_acknowledge_moves_line_item(self):
        d = b11.issue_decision("C1", "O-1", _stk())
        d2 = b11.act_on_decision(d["decision_id"], "TOWAGE", "acknowledge", actor="t")
        tow = next(s for s in d2["required_stakeholders"] if s["role"] == "TOWAGE")
        assert tow["status"] == b11.ST_ACKNOWLEDGED
        assert tow["acted_by"] == "t"

    def test_full_acknowledge_confirms_decision(self):
        d = b11.issue_decision("C1", "O-1", _stk())
        for r in ("TOWAGE", "PILOTAGE", "TERMINAL"):
            res = b11.act_on_decision(d["decision_id"], r, "acknowledge", actor="x")
        assert res["decision_state"] == b11.STATE_CONFIRMED
        assert res["confirmed_at"] is not None

    def test_flag_does_not_confirm_and_annotates(self):
        d = b11.issue_decision("C1", "O-1", _stk())
        b11.act_on_decision(d["decision_id"], "TOWAGE", "acknowledge", actor="x")
        b11.act_on_decision(d["decision_id"], "PILOTAGE", "acknowledge", actor="x")
        res = b11.act_on_decision(d["decision_id"], "TERMINAL", "flag",
                                  actor="x", flag_reason="crane down")
        assert res["decision_state"] == b11.STATE_PROPAGATED  # NOT confirmed
        term = next(s for s in res["required_stakeholders"] if s["role"] == "TERMINAL")
        assert term["status"] == b11.ST_FLAGGED
        assert term["flag_reason"] == "crane down"

    def test_flag_then_resolve_confirms(self):
        d = b11.issue_decision("C1", "O-1", _stk())
        b11.act_on_decision(d["decision_id"], "TOWAGE", "acknowledge", actor="x")
        b11.act_on_decision(d["decision_id"], "PILOTAGE", "acknowledge", actor="x")
        b11.act_on_decision(d["decision_id"], "TERMINAL", "flag",
                            actor="x", flag_reason="crane down")
        res = b11.act_on_decision(d["decision_id"], "TERMINAL", "acknowledge", actor="x")
        assert res["decision_state"] == b11.STATE_CONFIRMED

    def test_supersede_is_terminal(self):
        d = b11.issue_decision("C1", "O-1", _stk())
        res = b11.supersede_decision(d["decision_id"], "O-1")
        assert res["decision_state"] == b11.STATE_SUPERSEDED
        with pytest.raises(b11.InvalidTransitionError):
            b11.act_on_decision(d["decision_id"], "TOWAGE", "acknowledge", actor="x")

    def test_expire_overdue(self):
        past = (datetime.now(timezone.utc) - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
        d = b11.issue_decision("C1", "O-1", _stk(), decision_deadline=past)
        expired = b11.expire_overdue()
        assert any(e["decision_id"] == d["decision_id"] for e in expired)
        assert b11.get_decision(d["decision_id"])["decision_state"] == b11.STATE_EXPIRED


# ── Authority model ─────────────────────────────────────────────────────────
class TestAuthority:
    def test_only_vtso_may_issue(self):
        with pytest.raises(b11.AuthorityError):
            b11.issue_decision("C1", "O-1", _stk(), actor_role="TOWAGE")

    def test_only_vtso_may_supersede(self):
        d = b11.issue_decision("C1", "O-1", _stk())
        with pytest.raises(b11.AuthorityError):
            b11.supersede_decision(d["decision_id"], "O-1", actor_role="PILOTAGE")

    def test_stakeholder_cannot_act_on_foreign_line_item(self):
        # Issue with only TOWAGE; PILOTAGE has no line item -> authority error
        d = b11.issue_decision("C1", "O-1",
                               [{"role": "TOWAGE", "action_label": "x"}])
        with pytest.raises(b11.AuthorityError):
            b11.act_on_decision(d["decision_id"], "PILOTAGE", "acknowledge", actor="x")

    def test_vtso_role_cannot_acknowledge_line_item(self):
        d = b11.issue_decision("C1", "O-1", _stk())
        with pytest.raises(b11.AuthorityError):
            b11.act_on_decision(d["decision_id"], "VTSO", "acknowledge", actor="x")

    def test_flag_requires_reason(self):
        d = b11.issue_decision("C1", "O-1", _stk())
        with pytest.raises(b11.ValidationError):
            b11.act_on_decision(d["decision_id"], "TOWAGE", "flag", actor="x")


# ── Validation ──────────────────────────────────────────────────────────────
class TestValidation:
    def test_empty_stakeholders_rejected(self):
        with pytest.raises(b11.ValidationError):
            b11.issue_decision("C1", "O-1", [])

    def test_unknown_role_rejected(self):
        with pytest.raises(b11.ValidationError):
            b11.issue_decision("C1", "O-1", [{"role": "WIZARD", "action_label": "x"}])

    def test_duplicate_role_rejected(self):
        with pytest.raises(b11.ValidationError):
            b11.issue_decision("C1", "O-1", [
                {"role": "TOWAGE", "action_label": "a"},
                {"role": "TOWAGE", "action_label": "b"},
            ])

    def test_act_on_missing_decision_raises_notfound(self):
        with pytest.raises(b11.NotFoundError):
            b11.act_on_decision("D-9999-deadbeef", "TOWAGE", "acknowledge", actor="x")


# ── Concurrency ─────────────────────────────────────────────────────────────
class TestConcurrency:
    def test_concurrent_distinct_acks_all_apply_and_confirm(self):
        d = b11.issue_decision("C1", "O-1", _stk())
        did = d["decision_id"]
        roles = ["TOWAGE", "PILOTAGE", "TERMINAL"]
        barrier = threading.Barrier(len(roles))
        errors = []

        def worker(role):
            try:
                barrier.wait()
                b11.act_on_decision(did, role, "acknowledge", actor=role)
            except Exception as exc:  # pragma: no cover
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(r,)) for r in roles]
        for t in threads: t.start()
        for t in threads: t.join()

        assert not errors, f"concurrent acks raised: {errors}"
        final = b11.get_decision(did)
        assert final["decision_state"] == b11.STATE_CONFIRMED
        assert all(s["status"] == b11.ST_ACKNOWLEDGED for s in final["required_stakeholders"])

    def test_concurrent_same_line_item_no_corruption(self):
        d = b11.issue_decision("C1", "O-1", _stk())
        did = d["decision_id"]
        barrier = threading.Barrier(2)
        results = []

        def ack():
            barrier.wait()
            try:
                results.append(b11.act_on_decision(did, "TOWAGE", "acknowledge", actor="x"))
            except Exception as exc:
                results.append(exc)

        threads = [threading.Thread(target=ack) for _ in range(2)]
        for t in threads: t.start()
        for t in threads: t.join()

        final = b11.get_decision(did)
        tow = next(s for s in final["required_stakeholders"] if s["role"] == "TOWAGE")
        # Whatever the interleaving, the line item is consistently ACKNOWLEDGED.
        assert tow["status"] == b11.ST_ACKNOWLEDGED

    def test_many_decisions_unique_ids(self):
        ids = set()
        threads = []
        lock = threading.Lock()

        def issue(i):
            r = b11.issue_decision(f"C{i}", "O-1", _stk())
            with lock:
                ids.add(r["decision_id"])

        for i in range(20):
            threads.append(threading.Thread(target=issue, args=(i,)))
        for t in threads: t.start()
        for t in threads: t.join()
        assert len(ids) == 20  # no id collisions under concurrent issue


# ── active_decisions view (used by build_summary) ───────────────────────────
class TestActiveView:
    def test_active_keyed_by_conflict(self):
        b11.issue_decision("CONF-A", "O-1", _stk())
        active = b11.active_decisions()
        assert "CONF-A" in active
        assert active["CONF-A"]["decision_state"] == b11.STATE_PROPAGATED

    def test_superseded_drops_from_active(self):
        d = b11.issue_decision("CONF-A", "O-1", _stk())
        b11.supersede_decision(d["decision_id"], "O-1")
        assert "CONF-A" not in b11.active_decisions()


# ── Migration validation ────────────────────────────────────────────────────
# The migration module imports `from alembic import op`, which is a dev-only
# dependency (requirements-dev.txt) not present in the base test environment.
# These tests validate the migration's revision metadata and closed-set
# constants without needing alembic by executing the module with the alembic
# import lines stripped. Online application against a real database is
# validated separately by `alembic upgrade` in a DATABASE_URL environment.
def _load_migration_ns():
    path = REPO_ROOT / "migrations" / "versions" / "0005_beta11_decision_schema.py"
    src_lines = path.read_text().splitlines()
    stripped = "\n".join(
        l for l in src_lines
        if not l.strip().startswith("from alembic")
        and not l.strip().startswith("import sqlalchemy")
    )
    ns = {"op": None, "sa": None}
    exec(compile(stripped, str(path), "exec"), ns)
    return ns


class TestMigration:
    def test_migration_parses_and_chains(self):
        ns = _load_migration_ns()
        assert ns["revision"] == "0005_beta11_decision_schema"
        assert ns["down_revision"] == "0004_audit_schema"
        assert callable(ns["upgrade"]) and callable(ns["downgrade"])

    def test_migration_vocab_matches_module(self):
        ns = _load_migration_ns()
        assert set(ns["_DECISION_STATES"]) == set(b11.DECISION_STATES)
        assert set(ns["_CONFIRMER_ROLES"]) == set(b11.CONFIRMER_ROLES)
        assert set(ns["_STAKEHOLDER_STATUSES"]) == set(b11.STAKEHOLDER_STATUSES)
