#!/usr/bin/env python3
"""
Project Horizon — preview rollback drill (subprocess-per-phase).

Phase 1.2 Drill-preview helper. Validates the AUDIT_EMISSION_ENABLED
mechanism end-to-end through subprocess-per-phase isolation: each
phase spawns a child Python with a deliberately-set
AUDIT_EMISSION_ENABLED env var. The child imports the 5 helper
modules fresh, drives emit_async on each (six emits per phase), and
waits for the daemon worker threads to commit to Postgres before
exiting.

Subprocess-per-phase mirrors Railway's redeploy semantics: each
phase is a fresh process that reads AUDIT_EMISSION_ENABLED from the
environment at module load, exactly as production helpers do. This
proves the operational enable/disable mechanism without
monkey-patching private module globals.

Designed to be invoked from inside the Railway preview environment
via railway.toml [deploy].startCommand on the
chore/preview-rollback-drill DO-NOT-MERGE branch.

Phases
------
  0. Parent: verify DATABASE_URL is set; refuse to run otherwise
  1. Parent: capture baseline (count, max_sequence_no) for ams-demo
  2. Subprocess 'disabled-pre' (env: AUDIT_EMISSION_ENABLED=false):
     - imports helpers fresh; confirms all 5 is_enabled() == False
     - drives emit_async on each helper (6 emits total)
     - waits for any audit-emit-* daemon threads
     - exits 0
     Parent: confirms row count unchanged from baseline
  3. Subprocess 'enabled' (env: AUDIT_EMISSION_ENABLED=true):
     - imports helpers fresh; confirms all 5 is_enabled() == True
     - drives emit_async on each helper (6 emits total)
     - joins audit-emit-* daemon threads until none remain
     - exits 0
     Parent: confirms exactly 6 new rows landed, all tagged with
     subject_id LIKE 'drill-{run_id}-enabled-%'
  4. Subprocess 'disabled-post' (env: AUDIT_EMISSION_ENABLED=false):
     - same as 'disabled-pre' with phase='disabled-post' subject_ids
     Parent: confirms row count unchanged from end of Phase 3
  5. Parent: audit.verify_chain over whole tenant chain — must be ok
  6. Parent: prints PASS/FAIL summary; exits 0 only if all phases pass

Drill row preservation
----------------------
The 6 helper-path rows from Phase 3 are NOT cleaned up. The audit
chain is append-only by design; a DELETE would break prev_hash
linkage. Drill rows remain as activation evidence, filterable via:

  SELECT … FROM audit.events
  WHERE tenant_id='ams-demo'
    AND subject_id LIKE 'drill-%';

Exit codes
----------
  0 — every phase passed
  1 — one or more phases failed (drill rows still remain — chain
      is append-only)
  2 — DATABASE_URL not set, psycopg unavailable, or other prerequisite
      failure (no subprocesses invoked, no rows added)

Usage
-----
  $ export DATABASE_URL=postgresql://...
  $ python3 scripts/audit_rollback_drill.py
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

# Make repo root importable for both orchestrator and subprocess invocations.
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


# ── Configuration ────────────────────────────────────────────────────

DEFAULT_TENANT_ID = "ams-demo"
DRILL_ORIGIN_MARKER = "rollback_drill"

# Per-subprocess hard timeout (seconds). Enabled phase needs to wait for
# 6 daemon-thread DB writes; 30s is generous.
SUBPROCESS_TIMEOUT_S = 30


# ── Result type ──────────────────────────────────────────────────────

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


# ── Orchestrator side (parent process) ───────────────────────────────


def _capture_state(conn, tenant_id: str) -> tuple[int, int]:
    """Return (row_count, max_sequence_no) for the tenant. max_seq is 0
    if the tenant has no rows yet."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*), COALESCE(MAX(sequence_no), 0) "
            "FROM audit.events WHERE tenant_id = %s",
            (tenant_id,),
        )
        count, max_seq = cur.fetchone()
        return int(count), int(max_seq)


def _count_drill_rows(conn, tenant_id: str, run_id: str, phase: str) -> int:
    """Count rows whose subject_id is tagged with this drill run + phase."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM audit.events "
            "WHERE tenant_id = %s AND subject_id LIKE %s",
            (tenant_id, f"drill-{run_id}-{phase}-%"),
        )
        return int(cur.fetchone()[0])


def _run_subprocess(phase: str, run_id: str) -> int:
    """Spawn ourselves with --subprocess-phase=<phase>, controlled env.

    Each subprocess is a fresh Python that imports the helper modules
    from scratch. AUDIT_EMISSION_ENABLED is set in the child's env
    before module load, so the helpers' module-level
    _AUDIT_EMISSION_ENABLED reads the intended value at import time —
    no monkey-patching, no module reload tricks. This is the
    closest in-process analogue to a Railway redeploy with a different
    env var.

    Returns the child's exit code. stdout/stderr are echoed line-by-line
    with a clear prefix so failures are visible in the deploy logs.
    """
    env = dict(os.environ)
    env["AUDIT_EMISSION_ENABLED"] = "true" if phase == "enabled" else "false"

    cmd = [
        sys.executable,
        str(Path(__file__).resolve()),
        f"--subprocess-phase={phase}",
        f"--run-id={run_id}",
    ]
    try:
        result = subprocess.run(
            cmd,
            env=env,
            capture_output=True,
            timeout=SUBPROCESS_TIMEOUT_S,
            text=True,
        )
    except subprocess.TimeoutExpired:
        print(
            f"  [subprocess phase={phase}] TIMEOUT after "
            f"{SUBPROCESS_TIMEOUT_S}s",
            file=sys.stderr,
        )
        return 124

    for line in (result.stdout or "").splitlines():
        print(f"      [subprocess.out] {line}")
    for line in (result.stderr or "").splitlines():
        print(f"      [subprocess.err] {line}")
    return result.returncode


def _orchestrator_main() -> int:
    url = os.environ.get("DATABASE_URL", "").strip()
    if not url:
        print(
            "DATABASE_URL is not set — cannot run rollback drill. "
            "Set it to the target preview audit DB and re-run.",
            file=sys.stderr,
        )
        return 2

    try:
        import psycopg  # noqa: F401
    except ImportError as exc:
        print(
            f"psycopg not installed: {exc}. Install via "
            "`pip install -r requirements.txt` and retry.",
            file=sys.stderr,
        )
        return 2

    import psycopg
    import audit  # noqa: F401 — used for verify_chain below

    run_id = uuid4().hex[:8]
    tenant_id = DEFAULT_TENANT_ID

    print(
        f"Rollback drill against host={_host_from_url(url)!r}  "
        f"tenant_id={tenant_id!r}  run_id={run_id!r}\n"
    )

    results: list[StepResult] = []

    conn = psycopg.connect(url, connect_timeout=10, autocommit=False)
    try:
        # ── Phase 1 — baseline capture ────────────────────────────
        baseline_count, baseline_max_seq = _capture_state(conn, tenant_id)
        results.append(StepResult(
            "Phase 1 — baseline capture",
            True,
            f"count={baseline_count}  max_seq={baseline_max_seq}",
        ))
        print(f"Phase 1 — baseline: count={baseline_count}  "
              f"max_seq={baseline_max_seq}\n")

        # ── Phase 2 — subprocess 'disabled-pre' ───────────────────
        print("Phase 2 — subprocess 'disabled-pre' "
              "(AUDIT_EMISSION_ENABLED=false)")
        rc = _run_subprocess("disabled-pre", run_id)
        results.append(StepResult(
            "Phase 2 — disabled-pre subprocess exited cleanly",
            rc == 0,
            f"exit={rc}",
        ))
        # Re-query (start a new transaction; otherwise we'd see the
        # snapshot from before the subprocess wrote anything).
        conn.commit()
        post_pre_count, post_pre_max_seq = _capture_state(conn, tenant_id)
        results.append(StepResult(
            "Phase 2a — row count unchanged from baseline",
            post_pre_count == baseline_count
            and post_pre_max_seq == baseline_max_seq,
            f"count={post_pre_count}  max_seq={post_pre_max_seq}  "
            f"expected count={baseline_count}",
        ))

        # ── Phase 3 — subprocess 'enabled' ────────────────────────
        print("\nPhase 3 — subprocess 'enabled' "
              "(AUDIT_EMISSION_ENABLED=true)")
        rc = _run_subprocess("enabled", run_id)
        results.append(StepResult(
            "Phase 3 — enabled subprocess exited cleanly",
            rc == 0,
            f"exit={rc}",
        ))
        conn.commit()
        post_enabled_count, post_enabled_max_seq = _capture_state(
            conn, tenant_id)
        results.append(StepResult(
            "Phase 3a — exactly 6 helper-path rows landed",
            post_enabled_count == baseline_count + 6,
            f"count={post_enabled_count}  "
            f"expected={baseline_count + 6}",
        ))
        # Confirm the 6 rows are tagged with this drill run + phase
        drill_rows = _count_drill_rows(conn, tenant_id, run_id, "enabled")
        results.append(StepResult(
            "Phase 3b — drill rows tagged correctly via subject_id",
            drill_rows == 6,
            f"subject_id LIKE 'drill-{run_id}-enabled-%' "
            f"matched {drill_rows}",
        ))

        # ── Phase 4 — subprocess 'disabled-post' ──────────────────
        print("\nPhase 4 — subprocess 'disabled-post' "
              "(AUDIT_EMISSION_ENABLED=false)")
        rc = _run_subprocess("disabled-post", run_id)
        results.append(StepResult(
            "Phase 4 — disabled-post subprocess exited cleanly",
            rc == 0,
            f"exit={rc}",
        ))
        conn.commit()
        post_post_count, post_post_max_seq = _capture_state(conn, tenant_id)
        results.append(StepResult(
            "Phase 4a — row count unchanged from end of Phase 3",
            post_post_count == post_enabled_count
            and post_post_max_seq == post_enabled_max_seq,
            f"count={post_post_count}  max_seq={post_post_max_seq}  "
            f"expected count={post_enabled_count}",
        ))

        # ── Phase 5 — chain integrity over whole tenant chain ─────
        print("\nPhase 5 — audit.verify_chain over whole tenant chain")
        chain = audit.verify_chain(conn, tenant_id)
        results.append(StepResult(
            "Phase 5 — chain integrity",
            bool(chain.get("ok")),
            f"checked={chain.get('checked')}  "
            f"break_at={chain.get('break_at')}  "
            f"error_kind={chain.get('error_kind')}",
        ))

    finally:
        try:
            conn.rollback()
        except Exception:
            pass
        conn.close()

    # ── Render summary ───────────────────────────────────────────
    print()
    for r in results:
        print(r.render())

    failed = [r for r in results if not r.passed]
    print()
    if failed:
        print(
            f"RESULT: FAIL — {len(failed)} of {len(results)} step(s) "
            "failed.\n"
            "Drill rows that were inserted REMAIN in audit.events "
            "(append-only chain). Filter via subject_id LIKE "
            f"'drill-{run_id}-%'."
        )
        return 1

    print(
        f"RESULT: PASS — all {len(results)} steps passed.\n"
        f"Drill rows are preserved as activation evidence, tagged via "
        f"subject_id LIKE 'drill-{run_id}-%'. The hash chain is "
        "append-only by design and these rows must not be deleted "
        "post-hoc."
    )
    return 0


# ── Subprocess side (child process) ──────────────────────────────────


def _subprocess_main(phase: str, run_id: str) -> int:
    """Runs inside a child process. AUDIT_EMISSION_ENABLED was set by
    the orchestrator before this process started, so module-load reads
    the intended value at the helpers' import time.

    Drives one emit per helper (six emits total). Waits for any
    audit-emit-* daemon threads to commit before exiting.
    """
    flag_value = os.environ.get("AUDIT_EMISSION_ENABLED", "<unset>")
    expected_enabled = (phase == "enabled")

    print(
        f"subprocess: phase={phase!r}  "
        f"AUDIT_EMISSION_ENABLED={flag_value!r}  "
        f"expecting is_enabled()={expected_enabled}"
    )

    try:
        import session_audit
        import conflict_audit
        import recommendation_audit
        import recommendation_presented_audit
        import operator_action_audit
    except ImportError as exc:
        print(
            f"subprocess: helper import failed: {exc!r}",
            file=sys.stderr,
        )
        return 1

    helpers = (
        ("session_audit", session_audit),
        ("conflict_audit", conflict_audit),
        ("recommendation_audit", recommendation_audit),
        ("recommendation_presented_audit", recommendation_presented_audit),
        ("operator_action_audit", operator_action_audit),
    )

    # Verify every helper agrees with the expected state. Mismatch =
    # subprocess failure; the orchestrator will surface it as Phase
    # exit != 0.
    for name, mod in helpers:
        e = mod.is_enabled()
        print(
            f"subprocess:   {name:32s} is_enabled() = {e}  "
            f"(expected {expected_enabled})"
        )
        if e != expected_enabled:
            print(
                f"subprocess: FAIL — {name}.is_enabled() == {e}, "
                f"expected {expected_enabled}",
                file=sys.stderr,
            )
            return 1

    tenant_id = DEFAULT_TENANT_ID
    now_iso = datetime.now(timezone.utc).isoformat()

    # Drill subject_ids embed (drill, run_id, phase) so the
    # orchestrator can filter rows precisely post-hoc. The 6 emits
    # collectively cover all 6 currently-emitted event types.
    session_id = f"drill-{run_id}-{phase}-session"
    conflict_id = f"drill-{run_id}-{phase}-conflict"

    # Shared conflict-shaped dict for the four conflict/recommendation
    # related emits. recommendation_audit consumes a summary stub too.
    conflict_dict = {
        "id": conflict_id,
        "conflict_type": "berth_overlap",
        "signal_type": "CONFLICT",
        "severity": "high",
        "vessel_ids": ["V-DRILL-A", "V-DRILL-B"],
        "vessel_names": [f"DRILL ALPHA ({run_id})",
                         f"DRILL BRAVO ({run_id})"],
        "berth_id": "B-DRILL",
        "berth_name": "Drill Berth",
        "conflict_time": now_iso,
        "description": (
            f"Rollback drill conflict run={run_id} phase={phase}"
        ),
        "data_source": "simulated",
        "safety_score": 90,
        "sequencing_alternatives": [{
            "id":               "DRILL-OPT",
            "strategy":         "delay_arrival",
            "headline":         f"Drill option ({phase})",
            "description":      "rollback drill option",
            "affected_vessels": ["V-DRILL-A"],
            "cost_usd":         0,
            "cost_label":       "n/a",
            "delay_mins":       0,
            "cascade_count":    0,
            "risk":             "low",
            "feasibility":      "high",
            "recommended":      True,
        }],
        "decision_support": {
            "recommended_option_id":  "DRILL-OPT",
            "recommended_reasoning":  "rollback drill",
            "confidence":             "high",
            "decision_deadline":      now_iso,
            "options":                [],
        },
    }
    summary_stub = {
        "vessels": [],
        "berths": [{"id": "B-DRILL", "name": "Drill Berth"}],
        "tides": {},
        "weather": {},
        "ukc": {},
        "arrival_ukc": {},
        "port_profile": {
            "id": "DRILL",
            "bom_station_id": None,
            "using_live_vessel_data": False,
            "using_live_tidal_data": False,
            "using_live_weather_data": False,
        },
        "data_source": "simulated",
        "data_source_label": "drill",
        "scraped_at": now_iso,
        "lookahead_hours": 48,
        "port_status": {"pilots_available": 0, "tugs_available": 0},
    }

    # 1. SESSION_STARTED — session_audit passes payload verbatim, so we
    #    can include an explicit origin marker in the payload itself.
    session_audit.emit_async(
        tenant_id, "SESSION_STARTED", "operator_session", session_id,
        {
            "username":  session_id,
            "login_at":  now_iso,
            "origin":    DRILL_ORIGIN_MARKER,
            "run_id":    run_id,
            "phase":     phase,
        },
        actor_handle="O-1", actor_type="operator",
    )

    # 2. CONFLICT_DETECTED — conflict_audit builds its own whitelisted
    #    payload from the conflict dict; the drill marker lives in the
    #    subject_id (= conflict_id).
    conflict_audit.emit_async(tenant_id, conflict_dict, port_id="DRILL")

    # 3. RECOMMENDATION_GENERATED — uses conflict + summary stub.
    recommendation_audit.emit_async(
        tenant_id, conflict_dict,
        summary=summary_stub, port_id="DRILL",
    )

    # 4. RECOMMENDATION_PRESENTED — same conflict_id + rec_opt_id, with
    #    actor handle.
    recommendation_presented_audit.emit_async(
        tenant_id, conflict_dict,
        port_id="DRILL", actor_handle="O-1",
    )

    # 5. OPERATOR_ACTED — whatif_apply against this drill conflict.
    operator_action_audit.emit_async(
        tenant_id, operator_action_audit.ACTION_WHATIF_APPLY,
        summary=f"rollback drill action run={run_id} phase={phase}",
        surface="api_apply_whatif",
        actor_handle="O-1", port_id="DRILL",
        conflict_id=conflict_id,
    )

    # 6. SESSION_ENDED.
    session_audit.emit_async(
        tenant_id, "SESSION_ENDED", "operator_session", session_id,
        {
            "username":   session_id,
            "logout_at":  datetime.now(timezone.utc).isoformat(),
            "reason":     "drill_complete",
            "origin":     DRILL_ORIGIN_MARKER,
            "run_id":     run_id,
            "phase":      phase,
        },
        actor_handle="O-1", actor_type="operator",
    )

    # Wait for any audit-emit-* daemon threads to commit. When the
    # phase is disabled, emit_async returns early without spawning
    # threads, so this loop exits immediately. When enabled, we wait
    # for all six writes to land.
    deadline = time.time() + 20.0
    while time.time() < deadline:
        audit_threads = [
            t for t in threading.enumerate()
            if t.is_alive() and t.name.startswith("audit-emit-")
        ]
        if not audit_threads:
            break
        for t in audit_threads:
            t.join(timeout=0.5)

    remaining = [
        t for t in threading.enumerate()
        if t.is_alive() and t.name.startswith("audit-emit-")
    ]
    if remaining:
        print(
            f"subprocess: WARN — {len(remaining)} audit-emit threads "
            "still running at subprocess exit; row writes may not have "
            "completed.",
            file=sys.stderr,
        )
        return 1

    print(
        f"subprocess: phase={phase} complete  emits_issued=6  "
        f"threads_remaining=0"
    )
    return 0


# ── CLI dispatcher ───────────────────────────────────────────────────


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Phase 1.2 rollback drill — subprocess-per-phase",
    )
    parser.add_argument("--subprocess-phase", default=None,
                        choices=("disabled-pre", "enabled", "disabled-post"))
    parser.add_argument("--run-id", default=None)
    args = parser.parse_args()

    if args.subprocess_phase is not None:
        if not args.run_id:
            print(
                "--run-id is required with --subprocess-phase",
                file=sys.stderr,
            )
            return 2
        return _subprocess_main(args.subprocess_phase, args.run_id)

    return _orchestrator_main()


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
