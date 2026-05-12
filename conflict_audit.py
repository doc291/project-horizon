"""
Project Horizon — conflict audit emission helper.

Phase 0.7a per ADR-002. Best-effort, feature-flagged emission of
CONFLICT_DETECTED events when the conflict-detection engine surfaces
new conflicts in `build_summary()`.

Design properties (identical to session_audit.py for consistency):

- **No-ops when DATABASE_URL is unset.** db.is_configured() returns
  False in production → emit_async returns immediately, no thread
  spawned, no psycopg touched.
- **No-ops when AUDIT_EMISSION_ENABLED is false.** Emergency disable
  without code change via env var.
- **psycopg / audit lazy-imported in the worker thread.** Production
  (no psycopg in requirements.txt, DATABASE_URL unset) never reaches
  the import.
- **Daemon-thread emission.** emit_async returns within microseconds;
  the actual DB work happens off the request thread.
- **Best-effort.** All exceptions caught and logged at WARN. The
  caller (the /api/summary handler) is never affected.
- **Per-tenant, per-process deduplication.** Conflict detection runs
  on every /api/summary request (typically every 30s). Without dedup,
  the same conflict_id would be emitted on every request. We track
  emitted conflict_ids per tenant in-process and skip duplicates.
- **Payload lifecycle safety.** Payload dict is deep-copied BEFORE
  the worker thread starts (same pattern as session_audit).
- **Safe payload only.** The make_payload helper extracts only the
  authorised metadata (Phase 0.7a scope): conflict_id, conflict_type,
  severity, vessel_ids, vessel_names, berth_id, berth_name,
  conflict_time, description, data_source, port_id. No
  resolution_options, no sequencing_alternatives, no decision_support
  (these belong to Phase 0.7b/c).

Deduplication caveat: the dedup set is in-process. Multi-process
deployments (e.g. Railway with multiple instances) would each
maintain their own set, potentially producing one emission per
instance per conflict_id. For Phase 0 (single-process Railway), this
is acceptable. A later phase will move dedup state to Postgres (e.g.
UNIQUE constraint on tenant_id + conflict_id) when multi-instance
serving is introduced.
"""

from __future__ import annotations

import copy
import logging
import os
import threading
from datetime import datetime, timezone
from typing import Any

log = logging.getLogger("horizon.conflict_audit")


# ── Feature flag ──────────────────────────────────────────────────────
_AUDIT_EMISSION_ENABLED: bool = os.environ.get(
    "AUDIT_EMISSION_ENABLED", "true"
).lower() in ("true", "1", "yes", "on")


# ── Per-tenant per-process dedup state ────────────────────────────────
_emitted_lock = threading.Lock()
_emitted_ids: dict[str, set[str]] = {}


def is_enabled() -> bool:
    """
    Return True iff conflict audit emission should be attempted.

    Same gating as session_audit:
      1. AUDIT_EMISSION_ENABLED env var truthy (default true)
      2. DATABASE_URL configured (db.is_configured())

    In production (DATABASE_URL unset), returns False without
    importing psycopg.
    """
    if not _AUDIT_EMISSION_ENABLED:
        return False
    import db
    return db.is_configured()


def make_payload(conflict: dict, *, port_id: str | None = None) -> dict:
    """
    Build a SAFE payload from a conflict dict.

    Extracts only the metadata authorised in Phase 0.7a. Does NOT
    include resolution_options, sequencing_alternatives, or
    decision_support — those are Phase 0.7b/c territory.
    """
    payload: dict[str, Any] = {
        "conflict_id":   conflict.get("id"),
        "conflict_type": conflict.get("conflict_type"),
        "severity":      conflict.get("severity"),
        "vessel_ids":    list(conflict.get("vessel_ids") or []),
        "vessel_names":  list(conflict.get("vessel_names") or []),
        "berth_id":      conflict.get("berth_id"),
        "berth_name":    conflict.get("berth_name"),
        "conflict_time": conflict.get("conflict_time"),
        "description":   conflict.get("description"),
        "data_source":   conflict.get("data_source"),
        "observed_at":   datetime.now(timezone.utc).isoformat(),
    }
    if port_id:
        payload["port_id"] = port_id
    return payload


def emit_async(
    tenant_id: str,
    conflict: dict,
    *,
    port_id: str | None = None,
) -> None:
    """
    Best-effort emission of CONFLICT_DETECTED for one conflict.

    Returns immediately. Spawns a daemon thread only if (a) audit is
    enabled AND (b) this conflict_id has not been emitted before for
    this tenant in this process.

    When DATABASE_URL is unset OR the dedup set already contains the
    conflict_id, returns without side effect.

    Lifecycle safety: the payload dict is deep-copied here, before
    the worker thread starts. The worker sees an immutable snapshot
    regardless of what the caller does next.
    """
    if not is_enabled():
        return

    conflict_id = conflict.get("id")
    if not conflict_id:
        return

    # Per-tenant per-process dedup — only the first observation of a
    # conflict_id triggers emission.
    with _emitted_lock:
        seen = _emitted_ids.setdefault(tenant_id, set())
        if conflict_id in seen:
            return
        seen.add(conflict_id)

    payload_snapshot = copy.deepcopy(make_payload(conflict, port_id=port_id))

    threading.Thread(
        target=_emit_worker,
        args=(tenant_id, payload_snapshot, conflict_id),
        daemon=True,
        name=f"audit-emit-CONFLICT_DETECTED-{str(conflict_id)[:8]}",
    ).start()


def emit_sync(
    tenant_id: str,
    conflict: dict,
    *,
    port_id: str | None = None,
) -> bool:
    """
    Synchronous variant of emit_async. Returns True if emission
    succeeded, False if disabled, deduplicated, or failed.

    Primary use: tests. Not invoked from request handlers.
    """
    if not is_enabled():
        return False

    conflict_id = conflict.get("id")
    if not conflict_id:
        return False

    with _emitted_lock:
        seen = _emitted_ids.setdefault(tenant_id, set())
        if conflict_id in seen:
            return False
        seen.add(conflict_id)

    payload_snapshot = copy.deepcopy(make_payload(conflict, port_id=port_id))
    return _emit_worker(tenant_id, payload_snapshot, conflict_id)


def reset_dedup_state() -> None:
    """
    Clear the per-process emitted-conflict-id memory.

    Intended for tests so each test starts with a fresh dedup state.
    Not for production use — duplicate emissions would result if
    called while the application is running.
    """
    with _emitted_lock:
        _emitted_ids.clear()


# ── Internal worker ───────────────────────────────────────────────────

def _emit_worker(tenant_id: str, payload: dict, conflict_id: str) -> bool:
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
                event_type="CONFLICT_DETECTED",
                subject_type="conflict",
                subject_id=str(conflict_id),
                payload=payload,
                actor_handle=None,        # system-detected, no operator actor
                actor_type="system",
            )
        return True
    except Exception as exc:
        log.warning(
            "audit emission failed (best-effort): "
            "event_type=CONFLICT_DETECTED tenant=%s conflict_id=%s error=%s",
            tenant_id, conflict_id, exc,
        )
        return False
