#!/usr/bin/env python3
"""
Project Horizon — audit DB activation smoke test.

Phase 1.2 Stage D-preview helper. Write-capable end-to-end probe of
the audit ledger: emits one controlled row for each of the six event
types currently in scope, then verifies the rows landed and the hash
chain is intact.

Designed to be invoked manually after `alembic upgrade head` has
applied the audit schema to a target DATABASE_URL — typically the
Railway preview environment, then production once preview passes.

What this script does (when run)
--------------------------------
  1. Verifies DATABASE_URL is set
  2. Verifies psycopg is importable
  3. Reuses scripts/activate_audit.py for schema-presence checks
     (Alembic head, schemas, parent tables, partitions, constraints,
     indexes, row counts). If schema validation fails, smoke emits
     are NOT attempted.
  4. Captures starting state: current MAX(sequence_no) for the
     target tenant
  5. Emits one row via audit.emit() per event type, in order:
       - SESSION_STARTED
       - CONFLICT_DETECTED
       - RECOMMENDATION_GENERATED
       - RECOMMENDATION_PRESENTED
       - OPERATOR_ACTED
       - SESSION_ENDED
     Each row's payload carries `origin: "activation_smoke"` so it
     can be identified post-run as test evidence rather than real
     operational data.
  6. Verifies every emitted row is queryable via the returned
     event_id and that the row's payload contains the smoke marker
  7. Verifies sequence_no advances by exactly 1 between consecutive
     smoke emits
  8. Verifies the per-tenant chain is intact via
     audit.verify_chain(conn, tenant_id) — no gaps, no
     hash mismatches, no tamper detection
  9. Prints a PASS/FAIL summary, exits 0 on full success

What this script does NOT do
----------------------------
  * Does NOT clean up the smoke rows it inserts. The hash chain is
    append-only by design — deleting a row would break prev_hash
    linkage for every subsequent row. The smoke rows remain as
    activation evidence, tagged via payload.origin and easily
    filtered out of operational queries. Run the script knowingly
    and only against environments where adding ~6 rows is
    acceptable.
  * Does NOT call any of the 5 audit helpers (session_audit,
    conflict_audit, recommendation_audit, recommendation_presented_audit,
    operator_action_audit). The smoke test exercises audit.emit()
    directly because the helpers' jobs include request-handler-side
    payload construction that this script does not need to repeat
    or mock.
  * Does NOT mutate environment variables
  * Does NOT run Alembic
  * Does NOT enable AUDIT_EMISSION_ENABLED for any process beyond
    its own (and even within the process, the helpers are not
    invoked — audit.emit is called directly)
  * Does NOT touch production state unless DATABASE_URL points at it

Exit codes
----------
  0 — every emit and verification passed
  1 — one or more emits or verifications failed
  2 — DATABASE_URL not set, or psycopg unavailable, or schema
      validation failed (no emits attempted)

Configuration
-------------
  DATABASE_URL                       required
  AUDIT_VALIDATE_TENANT_ID           optional, default 'ams-demo'
  AUDIT_VALIDATE_PARTITIONS_MIN      passed through to activate_audit.py

Usage
-----
  $ export DATABASE_URL=postgresql://...
  $ python scripts/audit_smoke_test.py
"""

from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

# Make repo root importable so audit.py and scripts/activate_audit.py
# can both be imported without further setup.
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_SCRIPTS_DIR = Path(__file__).resolve().parent
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))


# ── Smoke-row marker ──────────────────────────────────────────────────

# Every payload this script writes carries this marker. Operational
# queries that want to exclude smoke rows can filter on
# `payload->>'origin' <> 'activation_smoke'`.
SMOKE_ORIGIN = "activation_smoke"

DEFAULT_TENANT_ID = "ams-demo"


# ── Event-type smoke specs ────────────────────────────────────────────

@dataclass
class SmokeEvent:
    """One audit emit instruction for the smoke test."""
    event_type: str
    subject_type: str
    subject_id: str
    actor_handle: str | None
    actor_type: str
    payload: dict = field(default_factory=dict)

    def stamped_payload(self) -> dict:
        """Return the payload with the smoke marker and observed_at
        added (deep-copy not required — we own this dict)."""
        out = dict(self.payload)
        out["origin"] = SMOKE_ORIGIN
        out["observed_at"] = datetime.now(timezone.utc).isoformat()
        return out


def _smoke_events(tenant_id: str) -> list[SmokeEvent]:
    """Build the six smoke-event specs.

    Subject ids are stable strings so re-runs against the same DB
    produce predictable evidence (with new sequence_nos appended
    each time). The events are ordered to roughly mirror an
    operator session: log in, see a conflict, get a recommendation,
    view it, act, log out.
    """
    smoke_run = f"smoke-{uuid4().hex[:8]}"
    return [
        SmokeEvent(
            event_type="SESSION_STARTED",
            subject_type="operator_session",
            subject_id="horizon",
            actor_handle="O-1",
            actor_type="operator",
            payload={
                "username": "horizon",
                "login_at": datetime.now(timezone.utc).isoformat(),
                "smoke_run": smoke_run,
            },
        ),
        SmokeEvent(
            event_type="CONFLICT_DETECTED",
            subject_type="conflict",
            subject_id=f"{smoke_run}-conflict",
            actor_handle=None,
            actor_type="system",
            payload={
                "conflict_id":   f"{smoke_run}-conflict",
                "conflict_type": "berth_overlap",
                "severity":      "high",
                "vessel_ids":    ["V-001", "V-002"],
                "vessel_names":  ["MV Smoke One", "MV Smoke Two"],
                "berth_id":      "B-SMOKE",
                "berth_name":    "Smoke Berth",
                "data_source":   "simulated",
                "smoke_run":     smoke_run,
            },
        ),
        SmokeEvent(
            event_type="RECOMMENDATION_GENERATED",
            subject_type="recommendation",
            subject_id=f"{smoke_run}-conflict::SMOKE-OPT-1",
            actor_handle=None,
            actor_type="system",
            payload={
                "recommendation_id":     f"{smoke_run}-conflict::SMOKE-OPT-1",
                "conflict_id":           f"{smoke_run}-conflict",
                "recommended_option_id": "SMOKE-OPT-1",
                "engine_version":        "horizon-beta-10",
                "decision_snapshot": {
                    "engine_version":         "horizon-beta-10",
                    "smoke_marker":           True,
                    "recommended_option":     {"id": "SMOKE-OPT-1"},
                    "alternatives_generated": [{"id": "SMOKE-OPT-1"}],
                    "conflict_state":         {"conflict_id": f"{smoke_run}-conflict"},
                    "constraints":            {"clearance_mins": 60},
                    "tide_inputs":            {},
                    "weather_inputs":         {},
                    "ukc_inputs":             [],
                    "relevant_vessels":       [],
                    "relevant_berths":        [],
                    "eta_source_hierarchy":   {},
                },
                "smoke_run": smoke_run,
            },
        ),
        SmokeEvent(
            event_type="RECOMMENDATION_PRESENTED",
            subject_type="recommendation",
            subject_id=f"{smoke_run}-conflict::SMOKE-OPT-1",
            actor_handle="O-1",
            actor_type="operator",
            payload={
                "recommendation_id":     f"{smoke_run}-conflict::SMOKE-OPT-1",
                "conflict_id":           f"{smoke_run}-conflict",
                "recommended_option_id": "SMOKE-OPT-1",
                "surface":               "api_summary",
                "displayed_at":          datetime.now(timezone.utc).isoformat(),
                "smoke_run":             smoke_run,
            },
        ),
        SmokeEvent(
            event_type="OPERATOR_ACTED",
            subject_type="conflict",
            subject_id=f"{smoke_run}-conflict",
            actor_handle="O-1",
            actor_type="operator",
            payload={
                "action_type":   "whatif_apply",
                "summary":       "Smoke-test scenario apply",
                "surface":       "api_apply_whatif",
                "conflict_id":   f"{smoke_run}-conflict",
                "smoke_run":     smoke_run,
            },
        ),
        SmokeEvent(
            event_type="SESSION_ENDED",
            subject_type="operator_session",
            subject_id="horizon",
            actor_handle="O-1",
            actor_type="operator",
            payload={
                "username":   "horizon",
                "logout_at":  datetime.now(timezone.utc).isoformat(),
                "reason":     "smoke_test_complete",
                "smoke_run":  smoke_run,
            },
        ),
    ]


# ── Result type ───────────────────────────────────────────────────────

@dataclass
class StepResult:
    name: str
    passed: bool
    detail: str = ""

    def render(self) -> str:
        mark = "PASS" if self.passed else "FAIL"
        line = f"  [{mark}] {self.name}"
        if self.detail:
            line += f"  — {self.detail}"
        return line


# ── Driver ────────────────────────────────────────────────────────────


def _max_sequence_no(conn, tenant_id: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COALESCE(MAX(sequence_no), 0) "
            "FROM audit.events WHERE tenant_id = %s",
            (tenant_id,),
        )
        return int(cur.fetchone()[0])


def _row_by_event_id(conn, event_id) -> dict | None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT event_type, sequence_no, payload->>'origin', "
            "       payload->>'smoke_run' "
            "FROM audit.events WHERE event_id = %s",
            (event_id,),
        )
        row = cur.fetchone()
    if row is None:
        return None
    return {
        "event_type":  row[0],
        "sequence_no": int(row[1]),
        "origin":      row[2],
        "smoke_run":   row[3],
    }


def _run_schema_validation(url: str, tenant_id: str) -> list:
    """Delegate to activate_audit.py for schema-presence checks.

    We import the module and call its check primitives directly with
    our own connection. This keeps a single source of truth for
    schema validation."""
    import psycopg

    # Lazy import of the validator module — it's in the same scripts/
    # directory which we already added to sys.path at module load.
    import activate_audit  # type: ignore[import-not-found]

    conn = psycopg.connect(url, connect_timeout=10, autocommit=False)
    try:
        results = activate_audit._all_checks(
            conn,
            tenant_id=tenant_id,
            partitions_min=int(os.environ.get(
                "AUDIT_VALIDATE_PARTITIONS_MIN", "12")),
        )
    finally:
        try:
            conn.rollback()
        except Exception:
            pass
        conn.close()
    return results


def _emit_smoke_event(conn, tenant_id: str, evt) -> dict:
    """Emit one audit row using audit.emit() and commit.

    Returns the dict audit.emit returns (event_id, sequence_no,
    ts_recorded, row_hash). Raises if emit raises."""
    import audit  # type: ignore[import-not-found]

    result = audit.emit(
        conn,
        tenant_id,
        event_type=evt.event_type,
        subject_type=evt.subject_type,
        subject_id=evt.subject_id,
        payload=evt.stamped_payload(),
        actor_handle=evt.actor_handle,
        actor_type=evt.actor_type,
    )
    conn.commit()
    return result


def _verify_chain(conn, tenant_id: str) -> dict:
    import audit  # type: ignore[import-not-found]
    return audit.verify_chain(conn, tenant_id)


def main() -> int:
    url = os.environ.get("DATABASE_URL", "").strip()
    if not url:
        print(
            "DATABASE_URL is not set — cannot run smoke test. Set it to "
            "the target preview audit DB and re-run.",
            file=sys.stderr,
        )
        return 2

    tenant_id = os.environ.get(
        "AUDIT_VALIDATE_TENANT_ID", DEFAULT_TENANT_ID
    ).strip() or DEFAULT_TENANT_ID

    # Lazy psycopg / audit imports
    try:
        import psycopg  # noqa: F401
    except ImportError as exc:
        print(
            f"psycopg not installed: {exc}. "
            "Install via `pip install -r requirements.txt` and retry.",
            file=sys.stderr,
        )
        return 2

    print(
        f"Audit smoke test against host="
        f"{_host_from_url(url)!r}  tenant_id={tenant_id!r}\n"
    )

    # ── Phase 1: schema validation prelude ──────────────────────────
    print("Phase 1 — schema validation (delegated to activate_audit.py)")
    schema_results = _run_schema_validation(url, tenant_id)
    for r in schema_results:
        print(r.render())
    schema_failed = [r for r in schema_results if not r.passed]
    if schema_failed:
        print(
            f"\nSchema validation FAILED ({len(schema_failed)} check(s) "
            f"failed). Aborting before any smoke emit.",
            file=sys.stderr,
        )
        return 2

    # ── Phase 2: emit one row per event type ────────────────────────
    print("\nPhase 2 — smoke emits (one row per event type)")
    import psycopg as _psycopg
    emit_results: list[StepResult] = []
    emitted: list[dict] = []  # event_id + sequence_no per emit

    conn = _psycopg.connect(url, connect_timeout=10, autocommit=False)
    try:
        start_seq = _max_sequence_no(conn, tenant_id)
        print(f"  starting MAX(sequence_no) for tenant={tenant_id!r}: {start_seq}")

        events = _smoke_events(tenant_id)
        previous_seq = start_seq
        for evt in events:
            try:
                result = _emit_smoke_event(conn, tenant_id, evt)
            except Exception as exc:
                emit_results.append(StepResult(
                    f"emit {evt.event_type}",
                    False,
                    f"audit.emit raised: {exc!r}",
                ))
                continue
            seq = int(result["sequence_no"])
            emitted.append({
                "event_id": result["event_id"],
                "event_type": evt.event_type,
                "sequence_no": seq,
            })
            # Monotonic check inline
            expected = previous_seq + 1
            mono_ok = seq == expected
            previous_seq = seq
            emit_results.append(StepResult(
                f"emit {evt.event_type}",
                mono_ok,
                f"seq={seq} (expected {expected})"
                if not mono_ok
                else f"seq={seq}",
            ))

        # ── Phase 3: verify rows by event_id ────────────────────────
        print()
        print("Phase 3 — read-back verification of each emitted row")
        readback_results: list[StepResult] = []
        for em in emitted:
            row = _row_by_event_id(conn, em["event_id"])
            if row is None:
                readback_results.append(StepResult(
                    f"read-back {em['event_type']}",
                    False,
                    f"event_id={em['event_id']} not found",
                ))
                continue
            ok = (
                row["event_type"] == em["event_type"]
                and row["sequence_no"] == em["sequence_no"]
                and row["origin"] == SMOKE_ORIGIN
            )
            readback_results.append(StepResult(
                f"read-back {em['event_type']}",
                ok,
                f"seq={row['sequence_no']} origin={row['origin']!r}",
            ))

        # ── Phase 4: monotonicity over all emitted rows ─────────────
        seqs = [em["sequence_no"] for em in emitted]
        monotonic_ok = (
            len(seqs) == 6
            and seqs == list(range(start_seq + 1, start_seq + 7))
        )
        mono_result = StepResult(
            "sequence_no advances monotonically by 1",
            monotonic_ok,
            f"start={start_seq} emitted={seqs}",
        )

        # ── Phase 5: verify_chain over the whole tenant chain ───────
        chain = _verify_chain(conn, tenant_id)
        chain_result = StepResult(
            "audit.verify_chain(tenant_id)",
            bool(chain.get("ok")),
            f"checked={chain.get('checked')}  "
            f"break_at={chain.get('break_at')}  "
            f"error_kind={chain.get('error_kind')}",
        )

    finally:
        try:
            conn.rollback()  # in case anything is left uncommitted
        except Exception:
            pass
        conn.close()

    # ── Render Phase 2, 3, 4, 5 results ─────────────────────────────
    for r in emit_results:
        print(r.render())
    print()
    for r in readback_results:
        print(r.render())
    print()
    print(mono_result.render())
    print(chain_result.render())

    # ── Summary ─────────────────────────────────────────────────────
    all_step_results = (
        emit_results + readback_results + [mono_result, chain_result]
    )
    failed = [r for r in all_step_results if not r.passed]

    print()
    if failed:
        print(
            f"RESULT: FAIL — {len(failed)} of {len(all_step_results)} "
            "step(s) failed.\n"
            "Smoke rows that were inserted REMAIN in audit.events tagged "
            f"with payload.origin='{SMOKE_ORIGIN}' — they are not "
            "cleaned up because doing so would break hash-chain semantics."
        )
        return 1

    print(
        f"RESULT: PASS — all {len(all_step_results)} steps passed.\n"
        f"Smoke rows are now part of the audit ledger for tenant "
        f"{tenant_id!r}, tagged with payload.origin='{SMOKE_ORIGIN}'.\n"
        f"They are intentionally preserved as activation evidence; the "
        f"hash chain is append-only by design and the rows must not be "
        f"deleted post-hoc."
    )
    return 0


def _host_from_url(url: str) -> str:
    """Best-effort host extraction; avoids printing credentials."""
    try:
        from urllib.parse import urlparse
        parsed = urlparse(url)
        if parsed.hostname:
            port = f":{parsed.port}" if parsed.port else ""
            return f"{parsed.hostname}{port}"
    except Exception:
        pass
    return "<unparseable>"


if __name__ == "__main__":
    sys.exit(main())
