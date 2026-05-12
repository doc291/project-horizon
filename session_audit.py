"""
Project Horizon — session audit emission helper.

Phase 0.6 per ADR-002. Provides a best-effort, feature-flagged helper
for emitting SESSION_STARTED / SESSION_ENDED events at login/logout.

Design properties (all of these are mandatory for production safety):

- **No-ops when DATABASE_URL is unset.** db.is_configured() returns
  False in production → emit_async returns immediately, no thread
  spawned, no psycopg touched.
- **No-ops when AUDIT_EMISSION_ENABLED is false.** Emergency disable
  without code change via env var.
- **psycopg / audit lazy-imported in the worker thread.** Production
  (no psycopg in requirements.txt, DATABASE_URL unset) never reaches
  the import.
- **Daemon thread emission.** emit_async returns within microseconds;
  the actual DB work happens off the request thread.
- **Best-effort.** All exceptions caught and logged at WARN level. The
  caller (login / logout) is never affected by audit failures.

This best-effort posture is a Phase 0 concession to demo stability per
ADR-001 / ADR-002. Phase 1 customer deployments will tighten this — see
ADR-002 §6 (commits / forecloses) for the explicit transition point.
"""

from __future__ import annotations

import copy
import logging
import os
import threading
from datetime import datetime, timezone
from typing import Any

log = logging.getLogger("horizon.session_audit")


# ── Feature flag ──────────────────────────────────────────────────────
# Read once at module load. Permits emergency disable in Railway by
# setting AUDIT_EMISSION_ENABLED=false without a code deploy.
_AUDIT_EMISSION_ENABLED: bool = os.environ.get(
    "AUDIT_EMISSION_ENABLED", "true"
).lower() in ("true", "1", "yes", "on")


# ── Public API ────────────────────────────────────────────────────────

def is_enabled() -> bool:
    """
    Return True iff audit emission should be attempted.

    Requires both:
      1. AUDIT_EMISSION_ENABLED env var truthy (default true)
      2. DATABASE_URL configured (db.is_configured())

    In production where DATABASE_URL is unset, this returns False
    without ever importing psycopg.
    """
    if not _AUDIT_EMISSION_ENABLED:
        return False
    # Lazy import so this module is safe to import in environments
    # where db.py exists but psycopg is not installed.
    import db
    return db.is_configured()


def emit_async(
    tenant_id: str,
    event_type: str,
    subject_type: str,
    subject_id: str,
    payload: dict[str, Any],
    *,
    actor_handle: str | None = None,
    actor_type: str = "operator",
) -> None:
    """
    Best-effort audit emission. Returns immediately. Spawns a daemon
    thread for the actual emission if (and only if) audit is enabled.

    Never raises. All exceptions in the worker thread are logged at
    WARN level.

    When DATABASE_URL is unset OR AUDIT_EMISSION_ENABLED is false:
    no thread is spawned, no DB connection attempted, no psycopg
    imported.

    Lifecycle safety: the payload dict is deep-copied here, BEFORE the
    worker thread starts. The worker thread sees an immutable snapshot
    of the payload regardless of what the caller does next (including
    mutating, garbage-collecting, or reusing the original dict).
    """
    if not is_enabled():
        return

    # Lifecycle: pin a snapshot of the payload. The caller may construct
    # the payload using shared references that are mutated, freed, or
    # reused after this call returns. The worker thread runs
    # concurrently, so we must own our own copy before crossing the
    # thread boundary. tenant_id, event_type, subject_type, subject_id,
    # actor_handle, and actor_type are all immutable strings (or None),
    # so they are inherently safe to share by reference.
    payload_snapshot = copy.deepcopy(payload)

    threading.Thread(
        target=_emit_worker,
        args=(tenant_id, event_type, subject_type, subject_id, payload_snapshot, actor_handle, actor_type),
        daemon=True,
        name=f"audit-emit-{event_type}",
    ).start()


def emit_sync(
    tenant_id: str,
    event_type: str,
    subject_type: str,
    subject_id: str,
    payload: dict[str, Any],
    *,
    actor_handle: str | None = None,
    actor_type: str = "operator",
) -> bool:
    """
    Synchronous variant of emit_async. Returns True if emission
    succeeded; False if disabled or failed. Catches all exceptions.

    Primary use: tests. Not invoked from server.py request handlers.
    """
    if not is_enabled():
        return False
    # Symmetric lifecycle safety with emit_async — snapshot the payload
    # so test code that mutates the payload after emit_sync still sees
    # the expected stored value.
    payload_snapshot = copy.deepcopy(payload)
    return _emit_worker(
        tenant_id, event_type, subject_type, subject_id, payload_snapshot,
        actor_handle, actor_type,
    )


def resolve_actor_handle(username: str | None, auth_user: str) -> str | None:
    """
    Phase 0.6 placeholder: map auth username → operator_handle.

    The current production HORIZON_USER value maps to operator_handle
    'O-1' per the AMS demo tenant seed in migration 0003. Other
    usernames return None; the audit event records the username in
    the payload regardless.

    A later phase will replace this with a config.users table read.
    """
    if username and username == auth_user:
        return "O-1"
    return None


def make_session_started_payload(
    username: str,
    *,
    remote_addr: str | None = None,
    next_path: str | None = None,
) -> dict:
    """Build the SESSION_STARTED payload."""
    payload: dict[str, Any] = {
        "username": username,
        "login_at": datetime.now(timezone.utc).isoformat(),
    }
    if remote_addr:
        payload["client_info"] = {"remote_addr": remote_addr}
    if next_path:
        payload["next_path"] = next_path
    return payload


def make_session_ended_payload(
    username: str,
    *,
    reason: str = "explicit_logout",
) -> dict:
    """Build the SESSION_ENDED payload."""
    return {
        "username": username,
        "logout_at": datetime.now(timezone.utc).isoformat(),
        "reason": reason,
    }


# ── Internal worker ───────────────────────────────────────────────────

def _emit_worker(
    tenant_id: str,
    event_type: str,
    subject_type: str,
    subject_id: str,
    payload: dict,
    actor_handle: str | None,
    actor_type: str,
) -> bool:
    """
    Worker entrypoint — either invoked by emit_async (in a thread) or
    by emit_sync (in-line). Returns True on success, False on failure.
    """
    try:
        # Lazy imports — only invoked when emission is actually attempted.
        # In production (no psycopg, no DATABASE_URL), this function is
        # never reached because is_enabled() returns False upstream.
        import psycopg
        import audit
        import db

        url = db.get_url()
        with psycopg.connect(url, connect_timeout=5) as conn:
            audit.emit(
                conn, tenant_id,
                event_type=event_type,
                subject_type=subject_type,
                subject_id=subject_id,
                payload=payload,
                actor_handle=actor_handle,
                actor_type=actor_type,
            )
        return True
    except Exception as exc:
        log.warning(
            "audit emission failed (best-effort): "
            "event_type=%s tenant=%s subject=%s/%s error=%s",
            event_type, tenant_id, subject_type, subject_id, exc,
        )
        return False
