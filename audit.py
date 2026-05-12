"""
Project Horizon — audit ledger writer and verifier.

Phase 0.5b per ADR-002. This module is the canonical entry point for
writing rows to the audit ledger (audit.events, audit.payloads) and for
verifying the per-tenant hash chain.

Design summary
--------------
- Pure-function layer (canonical_json, payload_hash, compute_row_hash,
  genesis_prev_hash) is deterministic and DB-free.
- Database-touching functions (emit, verify_chain, store_payload) accept
  a psycopg Connection as a parameter; they do not create connections
  themselves and they do not import psycopg at module top.
- Per-tenant serialised inserts use Postgres advisory locks
  (pg_advisory_xact_lock) keyed by a stable 64-bit hash of tenant_id.
- Hash chain links each row to its predecessor via prev_hash; the first
  row of a tenant uses a tenant-specific GENESIS prev_hash.

No application code calls into this module yet (Phase 0.5b ships writer
+ tests; Phase 0.6 onwards wires emission from server.py).
"""

from __future__ import annotations

import hashlib
import json
import logging
import struct
from datetime import datetime, timezone
from typing import Any, Iterable
from uuid import UUID, uuid4

log = logging.getLogger("horizon.audit")


# ── Constants ──────────────────────────────────────────────────────────

# SHA-256 produces 32 bytes; matches the audit.* schema CHECK constraints.
HASH_BYTES: int = 32

# Closed v1 sets — must match audit/* DB CHECK constraints in migration 0004.
EVENT_TYPES: frozenset[str] = frozenset({
    "TENANT_INITIALISED",
    "SESSION_STARTED", "SESSION_REFRESHED", "SESSION_ENDED",
    "VESSEL_STATE_OBSERVED", "BERTH_STATE_OBSERVED",
    "TIDAL_FORECAST_OBSERVED", "WEATHER_FORECAST_OBSERVED",
    "SCHEDULE_RECEIVED",
    "INTEGRATION_CONNECTED", "INTEGRATION_DISCONNECTED",
    "CONFLICT_DETECTED", "CONFLICT_RESOLVED",
    "RECOMMENDATION_GENERATED", "RECOMMENDATION_PRESENTED", "RECOMMENDATION_OBSOLETED",
    "OPERATOR_ACKNOWLEDGED", "OPERATOR_ACTED", "OPERATOR_OVERRODE", "OPERATOR_DEFERRED",
    "DEADLINE_PASSED", "SESSION_ENDED_WITHOUT_ACTION",
    "OUTCOME_RECORDED",
    "INTEGRITY_VERIFIED", "INTEGRITY_BREACH_DETECTED",
})

SUBJECT_TYPES: frozenset[str] = frozenset({
    "vessel", "berth", "conflict", "recommendation",
    "operator_session", "integration", "system", "tenant",
})

ACTOR_TYPES: frozenset[str] = frozenset({
    "operator", "system", "integration", "scheduled_job",
})

PAYLOAD_SOURCES: frozenset[str] = frozenset({
    "AIS", "MST", "AISSTREAM",
    "BOM_TIDES", "BOM_WEATHER",
    "TOS", "QSHIPS",
    "MANUAL_OPERATOR", "SCHEDULE_FEED",
    "PILOTAGE_SYSTEM", "TOWAGE_SYSTEM",
})

RETENTION_CLASSES: frozenset[str] = frozenset({
    "verbatim_full",
    "verbatim_recent_then_hash",
    "hash_only",
})


# ── Pure functions (no DB) ─────────────────────────────────────────────

def canonical_json(obj: Any) -> bytes:
    """
    Deterministic JSON serialisation suitable for hashing.

    Sorted keys, compact separators, UTF-8 output. UUIDs, datetimes and
    other non-JSON-native types are coerced to strings via `default=str`.
    """
    return json.dumps(
        obj,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
        ensure_ascii=False,
    ).encode("utf-8")


def payload_hash(payload: dict) -> bytes:
    """SHA-256 of the canonical JSON serialisation of `payload`. 32 raw bytes."""
    return hashlib.sha256(canonical_json(payload)).digest()


def hash_bytes(data: bytes) -> bytes:
    """SHA-256 of raw bytes. 32 raw bytes."""
    return hashlib.sha256(data).digest()


def genesis_prev_hash(tenant_id: str) -> bytes:
    """
    Tenant-specific genesis hash for the first row of a chain.

    Embeds the tenant_id so each tenant's chain begins from a distinct
    deterministic value, preventing cross-tenant chain replay.
    """
    return hashlib.sha256(b"AUDIT_GENESIS_v1:" + tenant_id.encode("utf-8")).digest()


def compute_row_hash(
    event_id: UUID | str,
    tenant_id: str,
    sequence_no: int,
    ts_event: str,
    ts_recorded: str,
    event_type: str,
    subject_type: str,
    subject_id: str,
    actor_handle: str | None,
    actor_type: str,
    payload_hash_value: bytes,
    source_payload_refs: Iterable[UUID | str],
    prev_hash: bytes,
) -> bytes:
    """
    Compute the canonical row_hash linking this row into the chain.

    Hash inputs:
      event_id, tenant_id, sequence_no, ts_event, ts_recorded,
      event_type, subject_type, subject_id, actor_handle, actor_type,
      payload_hash (hex), source_payload_refs (sorted strings),
      prev_hash (hex)

    Timestamps must be ISO-8601 strings for determinism. The caller is
    responsible for formatting datetimes consistently.
    """
    _require_hash_bytes("payload_hash", payload_hash_value)
    _require_hash_bytes("prev_hash", prev_hash)
    canonical = canonical_json({
        "event_id": str(event_id),
        "tenant_id": tenant_id,
        "sequence_no": sequence_no,
        "ts_event": ts_event,
        "ts_recorded": ts_recorded,
        "event_type": event_type,
        "subject_type": subject_type,
        "subject_id": subject_id,
        "actor_handle": actor_handle,
        "actor_type": actor_type,
        "payload_hash": payload_hash_value.hex(),
        "source_payload_refs": sorted(str(r) for r in source_payload_refs),
        "prev_hash": prev_hash.hex(),
    })
    return hashlib.sha256(canonical).digest()


def _require_hash_bytes(name: str, value: bytes) -> None:
    """Validate that `value` is exactly 32 bytes. Raises ValueError otherwise."""
    if not isinstance(value, (bytes, bytearray)):
        raise ValueError(f"{name} must be bytes, got {type(value).__name__}")
    if len(value) != HASH_BYTES:
        raise ValueError(f"{name} must be {HASH_BYTES} bytes, got {len(value)}")


def _tenant_lock_key(tenant_id: str) -> int:
    """
    Stable 64-bit signed integer derived from tenant_id, used as the
    advisory-lock key. Postgres `pg_advisory_xact_lock(bigint)` takes a
    bigint, so we hash and pack into a signed 64-bit integer.
    """
    digest = hashlib.sha256(tenant_id.encode("utf-8")).digest()
    # Take the first 8 bytes; interpret as signed big-endian int64.
    return struct.unpack(">q", digest[:8])[0]


def _iso8601(ts: datetime) -> str:
    """
    Format a timezone-aware datetime as ISO-8601 in UTC for hash determinism.

    Datetimes round-tripped through Postgres TIMESTAMPTZ are returned by
    psycopg in the connection's session timezone (which is typically the
    server's local zone, not UTC). Hash computation must be invariant to
    that representation: we always project to UTC before formatting.
    """
    if ts.tzinfo is None:
        raise ValueError("timestamps must be timezone-aware")
    return ts.astimezone(timezone.utc).isoformat()


# ── Database functions ────────────────────────────────────────────────
#
# These functions accept a psycopg Connection as the first argument.
# They do not import psycopg at module top — the connection's methods
# are used directly, keeping audit.py importable in environments that
# do not have psycopg installed (matches the gating discipline of db.py).

def emit(
    conn,
    tenant_id: str,
    event_type: str,
    subject_type: str,
    subject_id: str,
    payload: dict,
    *,
    actor_handle: str | None = None,
    actor_type: str = "system",
    source_payload_refs: list[UUID] | None = None,
    ts_event: datetime | None = None,
) -> dict:
    """
    Emit one audit event with serialised per-tenant chain insertion.

    Acquires a Postgres advisory lock keyed by tenant_id, assigns the
    next sequence_no, computes prev_hash and row_hash, and inserts.

    Returns a dict with `event_id`, `sequence_no`, `ts_recorded`, and
    `row_hash` for the inserted row.

    The caller is expected to either run within autocommit or open a
    transaction; this function opens an inner transaction that holds
    the advisory lock and the INSERT atomically.
    """
    # Validate inputs against closed sets — fail fast before touching DB.
    if event_type not in EVENT_TYPES:
        raise ValueError(f"unknown event_type: {event_type!r}")
    if subject_type not in SUBJECT_TYPES:
        raise ValueError(f"unknown subject_type: {subject_type!r}")
    if actor_type not in ACTOR_TYPES:
        raise ValueError(f"unknown actor_type: {actor_type!r}")
    if not isinstance(payload, dict):
        raise ValueError("payload must be a dict")

    refs: list[UUID] = list(source_payload_refs or [])
    p_hash = payload_hash(payload)
    lock_key = _tenant_lock_key(tenant_id)

    with conn.transaction():
        with conn.cursor() as cur:
            # Per-tenant serialisation: lock released on transaction commit/rollback.
            cur.execute("SELECT pg_advisory_xact_lock(%s)", (lock_key,))

            # Determine next sequence_no for this tenant.
            cur.execute(
                "SELECT COALESCE(MAX(sequence_no), 0) + 1 "
                "FROM audit.events WHERE tenant_id = %s",
                (tenant_id,),
            )
            sequence_no = cur.fetchone()[0]

            # Determine prev_hash: genesis for first row, prior row_hash otherwise.
            if sequence_no == 1:
                prev_hash = genesis_prev_hash(tenant_id)
            else:
                cur.execute(
                    "SELECT row_hash FROM audit.events "
                    "WHERE tenant_id = %s AND sequence_no = %s",
                    (tenant_id, sequence_no - 1),
                )
                row = cur.fetchone()
                if row is None:
                    raise RuntimeError(
                        f"chain gap: tenant={tenant_id} missing sequence_no={sequence_no - 1}"
                    )
                prev_hash = bytes(row[0])

            # Assign identifiers and timestamps.
            event_id = uuid4()
            ts_recorded = datetime.now(timezone.utc)
            ts_event_resolved = ts_event or ts_recorded

            # Compute row hash deterministically.
            r_hash = compute_row_hash(
                event_id=event_id,
                tenant_id=tenant_id,
                sequence_no=sequence_no,
                ts_event=_iso8601(ts_event_resolved),
                ts_recorded=_iso8601(ts_recorded),
                event_type=event_type,
                subject_type=subject_type,
                subject_id=subject_id,
                actor_handle=actor_handle,
                actor_type=actor_type,
                payload_hash_value=p_hash,
                source_payload_refs=refs,
                prev_hash=prev_hash,
            )

            # Insert. Postgres CHECK constraints enforce hash lengths and enums
            # again at the DB layer as defence-in-depth.
            cur.execute(
                """
                INSERT INTO audit.events (
                    event_id, tenant_id, sequence_no, ts_event, ts_recorded,
                    event_type, subject_type, subject_id, actor_handle, actor_type,
                    payload, payload_hash, source_payload_refs, prev_hash, row_hash,
                    schema_version
                ) VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s::jsonb, %s, %s, %s, %s,
                    1
                )
                """,
                (
                    event_id, tenant_id, sequence_no,
                    ts_event_resolved, ts_recorded,
                    event_type, subject_type, subject_id, actor_handle, actor_type,
                    canonical_json(payload).decode("utf-8"),
                    p_hash, refs, prev_hash, r_hash,
                ),
            )

    return {
        "event_id": event_id,
        "sequence_no": sequence_no,
        "ts_recorded": ts_recorded,
        "row_hash": r_hash,
    }


def verify_chain(
    conn,
    tenant_id: str,
    from_sequence_no: int = 1,
    to_sequence_no: int | None = None,
) -> dict:
    """
    Walk the chain for `tenant_id` from `from_sequence_no` (inclusive)
    to `to_sequence_no` (inclusive, or latest if None). For each row:

    1. Verify there is no sequence gap.
    2. Verify prev_hash matches the prior row's row_hash (or genesis).
    3. Verify row_hash equals the recomputed hash of this row's content.

    Returns a dict:
      {
        "tenant_id":         str,
        "from_sequence_no":  int,
        "to_sequence_no":    int | None,
        "checked":           int,             # number of rows verified
        "ok":                bool,
        "break_at":          int | None,      # sequence_no where chain broke
        "error_kind":        str | None,      # 'gap' | 'prev_hash_mismatch' | 'row_hash_mismatch'
      }
    """
    base = {
        "tenant_id": tenant_id,
        "from_sequence_no": from_sequence_no,
        "to_sequence_no": to_sequence_no,
        "checked": 0,
        "ok": True,
        "break_at": None,
        "error_kind": None,
    }

    with conn.cursor() as cur:
        query = (
            "SELECT event_id, sequence_no, ts_event, ts_recorded, "
            "event_type, subject_type, subject_id, actor_handle, actor_type, "
            "payload_hash, source_payload_refs, prev_hash, row_hash "
            "FROM audit.events "
            "WHERE tenant_id = %s AND sequence_no >= %s"
        )
        params: list = [tenant_id, from_sequence_no]
        if to_sequence_no is not None:
            query += " AND sequence_no <= %s"
            params.append(to_sequence_no)
        query += " ORDER BY sequence_no ASC"
        cur.execute(query, params)
        rows = cur.fetchall()

    if not rows:
        return base

    # Establish the expected prev_hash for the first row in the window.
    if from_sequence_no == 1:
        expected_prev_hash = genesis_prev_hash(tenant_id)
    else:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT row_hash FROM audit.events "
                "WHERE tenant_id = %s AND sequence_no = %s",
                (tenant_id, from_sequence_no - 1),
            )
            r = cur.fetchone()
            if r is None:
                return {**base, "ok": False, "break_at": from_sequence_no - 1, "error_kind": "gap"}
            expected_prev_hash = bytes(r[0])

    expected_seq = from_sequence_no
    checked = 0

    for row in rows:
        (event_id, seq, ts_event, ts_recorded,
         event_type, subject_type, subject_id,
         actor_handle, actor_type,
         p_hash_db, src_refs, prev_h_db, row_h_db) = row

        # Gap detection
        if seq != expected_seq:
            return {**base, "checked": checked, "ok": False,
                    "break_at": expected_seq, "error_kind": "gap"}

        prev_h_db_bytes = bytes(prev_h_db)
        row_h_db_bytes = bytes(row_h_db)
        p_hash_db_bytes = bytes(p_hash_db)

        # prev_hash linkage
        if prev_h_db_bytes != expected_prev_hash:
            return {**base, "checked": checked, "ok": False,
                    "break_at": seq, "error_kind": "prev_hash_mismatch"}

        # Recompute row_hash from row content
        recomputed = compute_row_hash(
            event_id=event_id,
            tenant_id=tenant_id,
            sequence_no=seq,
            ts_event=_iso8601(ts_event),
            ts_recorded=_iso8601(ts_recorded),
            event_type=event_type,
            subject_type=subject_type,
            subject_id=subject_id,
            actor_handle=actor_handle,
            actor_type=actor_type,
            payload_hash_value=p_hash_db_bytes,
            source_payload_refs=src_refs or [],
            prev_hash=prev_h_db_bytes,
        )
        if recomputed != row_h_db_bytes:
            return {**base, "checked": checked, "ok": False,
                    "break_at": seq, "error_kind": "row_hash_mismatch"}

        expected_prev_hash = row_h_db_bytes
        expected_seq = seq + 1
        checked += 1

    return {**base, "checked": checked, "ok": True}


def store_payload(
    conn,
    tenant_id: str,
    source: str,
    payload_kind: str,
    payload_bytes: bytes,
    *,
    content_type: str = "application/octet-stream",
    retention_class: str = "verbatim_full",
    source_url: str | None = None,
    encoding_notes: str | None = None,
) -> dict:
    """
    Store an upstream payload in audit.payloads.

    Retention class drives whether `payload_bytes` is stored verbatim
    or only the hash is kept:
      - 'verbatim_full'              full bytes retained
      - 'verbatim_recent_then_hash'  full bytes retained now (later
                                     prune job converts to hash-only)
      - 'hash_only'                  bytes discarded; only hash stored

    Returns a dict with `payload_id`, `payload_hash`, `ts_captured`.
    """
    if source not in PAYLOAD_SOURCES:
        raise ValueError(f"unknown source: {source!r}")
    if retention_class not in RETENTION_CLASSES:
        raise ValueError(f"unknown retention_class: {retention_class!r}")
    if not isinstance(payload_bytes, (bytes, bytearray)):
        raise ValueError("payload_bytes must be bytes")

    p_hash = hash_bytes(payload_bytes)
    size = len(payload_bytes)
    bytes_to_store: bytes | None = (
        None if retention_class == "hash_only" else bytes(payload_bytes)
    )
    payload_id = uuid4()
    ts_captured = datetime.now(timezone.utc)

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO audit.payloads (
                payload_id, tenant_id, ts_captured, source, source_url,
                payload_kind, payload_bytes, payload_size_bytes, payload_hash,
                content_type, encoding_notes, retention_class
            ) VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s
            )
            """,
            (
                payload_id, tenant_id, ts_captured, source, source_url,
                payload_kind, bytes_to_store, size, p_hash,
                content_type, encoding_notes, retention_class,
            ),
        )

    return {
        "payload_id": payload_id,
        "payload_hash": p_hash,
        "ts_captured": ts_captured,
    }
