"""
Project Horizon — operator-action audit helper.

Phase 0.8a per ADR-002. Best-effort, feature-flagged emission of
OPERATOR_ACTED events for *existing* Beta 10 operator actions only.

Scope of Phase 0.8a (no new action semantics, no new UI):

  /api/set_port      → action_type = "port_switch"
  /api/apply-whatif  → action_type = "whatif_apply"
  /api/clear-whatif  → action_type = "whatif_clear"
  /api/send-brief    → action_type = "send_brief"

Endpoints deliberately NOT instrumented in 0.8a:
  - /api/whatif (shadow simulation; read-only, no state change)
  - /login, /logout (already covered by SESSION_STARTED / SESSION_ENDED
    in Phase 0.6)

Inaction / DEADLINE_PASSED is Phase 0.8b and is NOT emitted here.
OPERATOR_DEFERRED / OPERATOR_OVERRODE are NOT emitted in 0.8a because
neither defer nor override is a real action surface in Beta 10 — they
have no UI control and no endpoint.

Authorised payload fields (per Step 0.8a brief):
  action_type, conflict_id (if available), recommendation_id (if
  available), actor_handle, port_id, surface, timestamp, summary.

Design properties (mirrors session/conflict/recommendation/presented
audit):

- **No-ops when DATABASE_URL is unset.** db.is_configured() returns
  False in production → emit_async returns immediately, no thread
  spawned, no psycopg touched.
- **No-ops when AUDIT_EMISSION_ENABLED is false.**
- **psycopg / audit lazy-imported in the worker thread.**
- **Daemon-thread emission.** emit_async returns within microseconds.
- **Best-effort.** All exceptions caught and logged at WARN. Action
  handlers (set_port / apply / clear / send-brief) are never affected.
- **Payload deep-copied before thread spawn.**
- **No deduplication.** Operator actions are by definition discrete
  events; calling set_port twice IS two distinct audit events. Unlike
  CONFLICT_DETECTED or RECOMMENDATION_GENERATED, we do not collapse
  repeats.
"""

from __future__ import annotations

import copy
import logging
import os
import threading
from datetime import datetime, timezone
from typing import Any

log = logging.getLogger("horizon.operator_action_audit")


# ── Feature flag ──────────────────────────────────────────────────────
_AUDIT_EMISSION_ENABLED: bool = os.environ.get(
    "AUDIT_EMISSION_ENABLED", "true"
).lower() in ("true", "1", "yes", "on")


# ── Canonical action types (closed set within 0.8a) ───────────────────
ACTION_PORT_SWITCH = "port_switch"
ACTION_WHATIF_APPLY = "whatif_apply"
ACTION_WHATIF_CLEAR = "whatif_clear"
ACTION_SEND_BRIEF   = "send_brief"

ACTION_TYPES: frozenset[str] = frozenset({
    ACTION_PORT_SWITCH,
    ACTION_WHATIF_APPLY,
    ACTION_WHATIF_CLEAR,
    ACTION_SEND_BRIEF,
})


def is_enabled() -> bool:
    """
    Return True iff OPERATOR_ACTED emission should be attempted.

    Same gating as every other Phase 0 audit helper:
      1. AUDIT_EMISSION_ENABLED env var truthy (default true)
      2. DATABASE_URL configured (db.is_configured())
    """
    if not _AUDIT_EMISSION_ENABLED:
        return False
    import db
    return db.is_configured()


def make_payload(
    action_type: str,
    *,
    summary: str,
    surface: str,
    actor_handle: str | None = None,
    port_id: str | None = None,
    conflict_id: str | None = None,
    recommendation_id: str | None = None,
    timestamp: str | None = None,
) -> dict:
    """
    Build the OPERATOR_ACTED payload.

    Only the authorised metadata fields are emitted. Request bodies
    are NOT included — they may contain free-form labels, recipient
    addresses, or other operator input that we deliberately keep out
    of the audit ledger in Phase 0. A short, sanitised `summary`
    string takes their place.
    """
    payload: dict[str, Any] = {
        "action_type": action_type,
        "surface":     surface,
        "summary":     summary,
        "timestamp":   timestamp or datetime.now(timezone.utc).isoformat(),
    }
    if actor_handle:
        payload["actor_handle"] = actor_handle
    if port_id:
        payload["port_id"] = port_id
    if conflict_id:
        payload["conflict_id"] = conflict_id
    if recommendation_id:
        payload["recommendation_id"] = recommendation_id
    return payload


def _subject_for_action(
    action_type: str,
    conflict_id: str | None,
    port_id: str | None,
) -> tuple[str, str]:
    """
    Map an action to an ADR-002 closed-set subject (subject_type,
    subject_id). The audit ledger's CHECK constraint on subject_type
    accepts: vessel | berth | conflict | recommendation |
    operator_session | integration | system | tenant.

    Rules:
      - whatif_apply / whatif_clear: if a conflict_id is present
        (operator was acting on a specific decision card), subject is
        that conflict. Otherwise subject is system / port_id.
      - port_switch / send_brief: subject is system / port_id.
    """
    if action_type in (ACTION_WHATIF_APPLY, ACTION_WHATIF_CLEAR) and conflict_id:
        return ("conflict", conflict_id)
    return ("system", port_id or "system")


def emit_async(
    tenant_id: str,
    action_type: str,
    *,
    summary: str,
    surface: str,
    actor_handle: str | None = None,
    port_id: str | None = None,
    conflict_id: str | None = None,
    recommendation_id: str | None = None,
) -> None:
    """
    Best-effort OPERATOR_ACTED emission. Returns immediately.

    Spawns a daemon thread only when audit is enabled. The action
    handler itself must have already returned its response — callers
    invoke this after `self._json(...)`.

    Lifecycle safety: payload is deep-copied here, before the worker
    thread starts.
    """
    if not is_enabled():
        return
    if action_type not in ACTION_TYPES:
        # Out-of-set action type — refuse rather than emit a row that
        # would fail the ledger's CHECK constraint.
        log.warning(
            "operator_action_audit: unknown action_type=%r — skipping emit",
            action_type,
        )
        return

    payload_snapshot = copy.deepcopy(make_payload(
        action_type,
        summary=summary, surface=surface, actor_handle=actor_handle,
        port_id=port_id, conflict_id=conflict_id,
        recommendation_id=recommendation_id,
    ))
    subj_type, subj_id = _subject_for_action(action_type, conflict_id, port_id)

    threading.Thread(
        target=_emit_worker,
        args=(tenant_id, payload_snapshot, subj_type, subj_id, actor_handle),
        daemon=True,
        name=f"audit-emit-OPERATOR_ACTED-{action_type}",
    ).start()


def emit_sync(
    tenant_id: str,
    action_type: str,
    *,
    summary: str,
    surface: str,
    actor_handle: str | None = None,
    port_id: str | None = None,
    conflict_id: str | None = None,
    recommendation_id: str | None = None,
) -> bool:
    """
    Synchronous variant. Returns True on success, False if disabled,
    rejected (unknown action_type), or failed. Primary use: tests.
    """
    if not is_enabled():
        return False
    if action_type not in ACTION_TYPES:
        log.warning(
            "operator_action_audit: unknown action_type=%r — skipping emit",
            action_type,
        )
        return False

    payload_snapshot = copy.deepcopy(make_payload(
        action_type,
        summary=summary, surface=surface, actor_handle=actor_handle,
        port_id=port_id, conflict_id=conflict_id,
        recommendation_id=recommendation_id,
    ))
    subj_type, subj_id = _subject_for_action(action_type, conflict_id, port_id)
    return _emit_worker(
        tenant_id, payload_snapshot, subj_type, subj_id, actor_handle,
    )


# ── Internal worker ───────────────────────────────────────────────────

def _emit_worker(
    tenant_id: str,
    payload: dict,
    subject_type: str,
    subject_id: str,
    actor_handle: str | None,
) -> bool:
    """Worker entrypoint. Returns True on success, False on failure."""
    try:
        # Lazy imports — only invoked when emission is actually attempted.
        import psycopg
        import audit
        import db

        url = db.get_url()
        with psycopg.connect(url, connect_timeout=5) as conn:
            audit.emit(
                conn, tenant_id,
                event_type="OPERATOR_ACTED",
                subject_type=subject_type,
                subject_id=subject_id,
                payload=payload,
                actor_handle=actor_handle,
                actor_type="operator" if actor_handle else "system",
            )
        return True
    except Exception as exc:
        log.warning(
            "audit emission failed (best-effort): "
            "event_type=OPERATOR_ACTED tenant=%s subject=%s/%s action=%s error=%s",
            tenant_id, subject_type, subject_id,
            payload.get("action_type"), exc,
        )
        return False
