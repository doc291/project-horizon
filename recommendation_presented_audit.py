"""
Project Horizon — recommendation-presented audit helper.

Phase 0.7c per ADR-002.

Best-effort, feature-flagged emission of RECOMMENDATION_PRESENTED
events. Where RECOMMENDATION_GENERATED (Phase 0.7b) records that the
engine produced a recommendation, RECOMMENDATION_PRESENTED records
that the recommendation was delivered to an authenticated operator
surface — in Phase 0 that surface is `/api/summary`.

Scope is deliberately small. The payload contains presentation
metadata only, NOT the decision-time snapshot. The snapshot is owned
by RECOMMENDATION_GENERATED and would be redundant on every poll.

Authorised payload fields (per Step 0.7c brief):
  recommendation_id, conflict_id, conflict_type, severity,
  displayed_at, surface ("api_summary"), port_id, actor_handle

Design properties (mirrors session_audit / conflict_audit /
recommendation_audit):

- **No-ops when DATABASE_URL is unset.** db.is_configured() returns
  False in production → emit_async returns immediately, no thread
  spawned, no psycopg touched.
- **No-ops when AUDIT_EMISSION_ENABLED is false.** Emergency disable
  without code change via env var.
- **psycopg / audit lazy-imported in the worker thread.**
- **Daemon-thread emission.** emit_async returns within microseconds.
- **Best-effort.** All exceptions caught and logged at WARN. The
  /api/summary handler is never affected.
- **Per-tenant, per-process dedup.** Key is
  (conflict_id, recommended_option_id, actor_handle). The same
  recommendation rendered to the same operator on every 30s poll
  emits exactly once. A different operator viewing the same
  recommendation produces a fresh emission (their presentation event
  is a distinct evidence artefact).
- **Payload deep-copied before thread spawn.**
- **Authenticated surfaces only.** Caller must gate the call on
  `_is_authenticated()`; the helper itself does NOT inspect auth
  state. This keeps the auth surface in one place (server.py) and the
  audit helper purely a payload-and-emit module.

Phase 0.7c scope is RECOMMENDATION_PRESENTED only. No changes to:
- RECOMMENDATION_GENERATED snapshot logic (Phase 0.7b)
- CONFLICT_DETECTED emission (Phase 0.7a)
- decision-support output, conflict detection, or any engine logic
- auth / cookie / routing / header / icon / deploy assets
"""

from __future__ import annotations

import copy
import logging
import os
import threading
from datetime import datetime, timezone
from typing import Any

log = logging.getLogger("horizon.recommendation_presented_audit")


# ── Feature flag ──────────────────────────────────────────────────────
_AUDIT_EMISSION_ENABLED: bool = os.environ.get(
    "AUDIT_EMISSION_ENABLED", "true"
).lower() in ("true", "1", "yes", "on")


# ── Per-tenant per-process dedup state ────────────────────────────────
# Key is (conflict_id, recommended_option_id, actor_handle_or_empty).
# Different operators viewing the same recommendation are distinct
# presentation events; repeated polls by the same operator are not.
_emitted_lock = threading.Lock()
_emitted_keys: dict[str, set[tuple[str, str, str]]] = {}


SURFACE_API_SUMMARY = "api_summary"


def is_enabled() -> bool:
    """
    Return True iff RECOMMENDATION_PRESENTED emission should be attempted.

    Same gating as the rest of the audit helpers:
      1. AUDIT_EMISSION_ENABLED env var truthy (default true)
      2. DATABASE_URL configured (db.is_configured())
    """
    if not _AUDIT_EMISSION_ENABLED:
        return False
    import db
    return db.is_configured()


def make_payload(
    conflict: dict,
    *,
    surface: str = SURFACE_API_SUMMARY,
    port_id: str | None = None,
    actor_handle: str | None = None,
    displayed_at: str | None = None,
) -> dict:
    """
    Build the RECOMMENDATION_PRESENTED payload.

    Pure function over the in-memory `conflict` dict. The decision
    snapshot is NOT included — that lives on RECOMMENDATION_GENERATED.
    Here we record only the presentation metadata: which conflict, who
    saw it, when, on what surface.

    Authorised fields per Step 0.7c:
      recommendation_id, conflict_id, conflict_type, severity,
      displayed_at, surface, port_id, actor_handle.
    """
    ds = conflict.get("decision_support") or {}
    rec_opt = ds.get("recommended_option_id")
    cid = conflict.get("id")
    payload: dict[str, Any] = {
        "recommendation_id": f"{cid}::{rec_opt}",
        "conflict_id":       cid,
        "conflict_type":     conflict.get("conflict_type"),
        "severity":          conflict.get("severity"),
        "displayed_at":      displayed_at or datetime.now(timezone.utc).isoformat(),
        "surface":           surface,
    }
    if port_id:
        payload["port_id"] = port_id
    if actor_handle:
        payload["actor_handle"] = actor_handle
    return payload


# ── Emission ──────────────────────────────────────────────────────────

def _dedup_key(conflict: dict, actor_handle: str | None) -> tuple[str, str, str] | None:
    cid = conflict.get("id")
    rec_opt = (conflict.get("decision_support") or {}).get("recommended_option_id")
    if not cid or not rec_opt:
        return None
    return (str(cid), str(rec_opt), actor_handle or "")


def emit_async(
    tenant_id: str,
    conflict: dict,
    *,
    surface: str = SURFACE_API_SUMMARY,
    port_id: str | None = None,
    actor_handle: str | None = None,
) -> None:
    """
    Best-effort emission of RECOMMENDATION_PRESENTED for one conflict.

    Returns immediately. Spawns a daemon thread only if:
      (a) audit is enabled, AND
      (b) the conflict carries a decision_support recommendation, AND
      (c) this (conflict_id, recommended_option_id, actor_handle) has
          not been emitted before for this tenant in this process.

    Caller responsibility: only call this when the surface is
    authenticated. The helper does not inspect auth state itself.

    Lifecycle safety: payload is deep-copied here, before the worker
    thread starts.
    """
    if not is_enabled():
        return

    key = _dedup_key(conflict, actor_handle)
    if not key:
        return

    with _emitted_lock:
        seen = _emitted_keys.setdefault(tenant_id, set())
        if key in seen:
            return
        seen.add(key)

    payload_snapshot = copy.deepcopy(
        make_payload(
            conflict, surface=surface, port_id=port_id,
            actor_handle=actor_handle,
        )
    )
    subject_id = payload_snapshot["recommendation_id"]

    threading.Thread(
        target=_emit_worker,
        args=(tenant_id, payload_snapshot, subject_id, actor_handle),
        daemon=True,
        name=f"audit-emit-RECOMMENDATION_PRESENTED-{subject_id[:16]}",
    ).start()


def emit_sync(
    tenant_id: str,
    conflict: dict,
    *,
    surface: str = SURFACE_API_SUMMARY,
    port_id: str | None = None,
    actor_handle: str | None = None,
) -> bool:
    """
    Synchronous variant of emit_async. Returns True if emission
    succeeded, False if disabled, deduplicated, or failed.

    Primary use: tests.
    """
    if not is_enabled():
        return False

    key = _dedup_key(conflict, actor_handle)
    if not key:
        return False

    with _emitted_lock:
        seen = _emitted_keys.setdefault(tenant_id, set())
        if key in seen:
            return False
        seen.add(key)

    payload_snapshot = copy.deepcopy(
        make_payload(
            conflict, surface=surface, port_id=port_id,
            actor_handle=actor_handle,
        )
    )
    subject_id = payload_snapshot["recommendation_id"]
    return _emit_worker(tenant_id, payload_snapshot, subject_id, actor_handle)


def reset_dedup_state() -> None:
    """Clear the per-process emitted-presentation memory. Tests only."""
    with _emitted_lock:
        _emitted_keys.clear()


# ── Internal worker ───────────────────────────────────────────────────

def _emit_worker(
    tenant_id: str,
    payload: dict,
    subject_id: str,
    actor_handle: str | None,
) -> bool:
    """Worker entrypoint. Returns True on success, False on failure."""
    try:
        # Lazy imports — only invoked when emission is actually attempted.
        # In production (no psycopg, no DATABASE_URL), never reached.
        import psycopg
        import audit
        import db

        url = db.get_url()
        with psycopg.connect(url, connect_timeout=5) as conn:
            audit.emit(
                conn, tenant_id,
                event_type="RECOMMENDATION_PRESENTED",
                subject_type="recommendation",
                subject_id=subject_id,
                payload=payload,
                actor_handle=actor_handle,
                actor_type="operator" if actor_handle else "system",
            )
        return True
    except Exception as exc:
        log.warning(
            "audit emission failed (best-effort): "
            "event_type=RECOMMENDATION_PRESENTED tenant=%s subject=%s error=%s",
            tenant_id, subject_id, exc,
        )
        return False
