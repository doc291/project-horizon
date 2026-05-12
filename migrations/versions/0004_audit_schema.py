"""create audit schema with monthly-partitioned events and payloads tables

Revision ID: 0004_audit_schema
Revises: 0003_seed_ams_demo_tenant
Create Date: 2026-05-12

Phase 0.5a per ADR-002. Creates the audit ledger data model:
  - audit.events     append-only hash-chained ledger
  - audit.payloads   immutable upstream-data store

Both tables are RANGE-partitioned by month on their primary insert-time
column (ts_recorded for events, ts_captured for payloads). Twelve forward
monthly partitions (current month + 11 ahead) are pre-created so the
writer can begin inserting on day one without requiring a partition-
maintenance job in Phase 0.5b. A later phase will add an automated
partition-creation routine before this 12-month window runs out.

This migration creates schema and tables ONLY. It does NOT:
  - create the audit writer module (Phase 0.5b)
  - wire any emission into server.py
  - insert any rows
  - read or modify any operational tables

Reversibility:
  - downgrade() drops the entire audit schema CASCADE
  - the audit schema has no data at this point so no rows are lost
"""

from datetime import datetime, timezone
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0004_audit_schema"
down_revision: Union[str, None] = "0003_seed_ams_demo_tenant"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


# ── Closed v1 event_type set per ADR-002 §1.4 ──────────────────────────
# STATE_SNAPSHOT and EXTERNAL_ANCHOR_RECORDED are deferred to v1.1.
# Decision-time snapshots live inside RECOMMENDATION_GENERATED.payload
# per ADR-002 §1.4.1 (AM-001), not as a separate event type.
_EVENT_TYPES: list[str] = [
    "TENANT_INITIALISED",
    # Session lifecycle
    "SESSION_STARTED", "SESSION_REFRESHED", "SESSION_ENDED",
    # Observations (what Horizon knew)
    "VESSEL_STATE_OBSERVED", "BERTH_STATE_OBSERVED",
    "TIDAL_FORECAST_OBSERVED", "WEATHER_FORECAST_OBSERVED",
    "SCHEDULE_RECEIVED",
    "INTEGRATION_CONNECTED", "INTEGRATION_DISCONNECTED",
    # Reasoning (what Horizon concluded)
    "CONFLICT_DETECTED", "CONFLICT_RESOLVED",
    "RECOMMENDATION_GENERATED", "RECOMMENDATION_PRESENTED", "RECOMMENDATION_OBSOLETED",
    # Operator action
    "OPERATOR_ACKNOWLEDGED", "OPERATOR_ACTED", "OPERATOR_OVERRODE", "OPERATOR_DEFERRED",
    # Inaction
    "DEADLINE_PASSED", "SESSION_ENDED_WITHOUT_ACTION",
    # Outcome
    "OUTCOME_RECORDED",
    # Integrity
    "INTEGRITY_VERIFIED", "INTEGRITY_BREACH_DETECTED",
]

# ── Closed source set for audit.payloads per ADR-002 §1.7 ──────────────
_PAYLOAD_SOURCES: list[str] = [
    "AIS", "MST", "AISSTREAM",
    "BOM_TIDES", "BOM_WEATHER",
    "TOS", "QSHIPS",
    "MANUAL_OPERATOR", "SCHEDULE_FEED",
    "PILOTAGE_SYSTEM", "TOWAGE_SYSTEM",
]

# ── Closed retention_class set ─────────────────────────────────────────
_RETENTION_CLASSES: list[str] = [
    "verbatim_full",
    "verbatim_recent_then_hash",
    "hash_only",
]

# Number of monthly partitions to pre-create (current month + N ahead).
# A later phase will add automated partition rotation before this runs out.
_PARTITION_MONTHS_AHEAD = 11   # current + 11 ahead = 12 months total


def _partition_ranges(months_ahead: int) -> list[tuple[str, str, str]]:
    """
    Generate (suffix, lower_bound, upper_bound) tuples for monthly
    partitions starting at the current month and extending N months
    forward.

    Returns suffix in the form 'YYYY_MM' and inclusive lower / exclusive
    upper ISO date strings ('YYYY-MM-01').
    """
    now = datetime.now(timezone.utc)
    year, month = now.year, now.month
    ranges: list[tuple[str, str, str]] = []
    for _ in range(months_ahead + 1):
        next_year = year + (1 if month == 12 else 0)
        next_month = 1 if month == 12 else month + 1
        suffix = f"{year:04d}_{month:02d}"
        lower = f"{year:04d}-{month:02d}-01"
        upper = f"{next_year:04d}-{next_month:02d}-01"
        ranges.append((suffix, lower, upper))
        year, month = next_year, next_month
    return ranges


def upgrade() -> None:
    """Create audit schema, partitioned parent tables, and 12 monthly partitions."""
    op.execute("CREATE SCHEMA IF NOT EXISTS audit")

    # ── audit.events parent (partitioned by RANGE on ts_recorded) ──────
    #
    # PK includes ts_recorded because Postgres requires partition-key
    # columns in any unique constraint. (tenant_id, sequence_no) is the
    # logically-unique pair, enforced application-side by the serialised
    # writer; ts_recorded is added to satisfy the partitioning rule.
    event_type_check = ", ".join(f"'{t}'" for t in _EVENT_TYPES)
    op.execute(
        f"""
        CREATE TABLE audit.events (
            event_id            UUID         NOT NULL DEFAULT gen_random_uuid(),
            tenant_id           TEXT         NOT NULL,
            sequence_no         BIGINT       NOT NULL,
            ts_event            TIMESTAMPTZ  NOT NULL,
            ts_recorded         TIMESTAMPTZ  NOT NULL DEFAULT now(),
            event_type          TEXT         NOT NULL,
            subject_type        TEXT         NOT NULL,
            subject_id          TEXT         NOT NULL,
            actor_handle        TEXT,
            actor_type          TEXT         NOT NULL,
            payload             JSONB        NOT NULL,
            payload_hash        BYTEA        NOT NULL,
            source_payload_refs UUID[]       NOT NULL DEFAULT ARRAY[]::UUID[],
            prev_hash           BYTEA        NOT NULL,
            row_hash            BYTEA        NOT NULL,
            schema_version      SMALLINT     NOT NULL DEFAULT 1,
            CONSTRAINT events_pkey            PRIMARY KEY (tenant_id, sequence_no, ts_recorded),
            CONSTRAINT events_event_id_unique UNIQUE (event_id, ts_recorded),
            CONSTRAINT events_event_type_valid CHECK (event_type IN ({event_type_check})),
            CONSTRAINT events_actor_type_valid CHECK (actor_type IN ('operator', 'system', 'integration', 'scheduled_job')),
            CONSTRAINT events_subject_type_valid CHECK (subject_type IN ('vessel', 'berth', 'conflict', 'recommendation', 'operator_session', 'integration', 'system', 'tenant')),
            CONSTRAINT events_payload_hash_len CHECK (octet_length(payload_hash) = 32),
            CONSTRAINT events_prev_hash_len    CHECK (octet_length(prev_hash) = 32),
            CONSTRAINT events_row_hash_len     CHECK (octet_length(row_hash) = 32),
            CONSTRAINT events_sequence_nonneg  CHECK (sequence_no >= 0),
            CONSTRAINT events_schema_version_valid CHECK (schema_version >= 1)
        ) PARTITION BY RANGE (ts_recorded);
        """
    )

    # Indexes on the partitioned parent. Postgres 11+ automatically
    # creates matching child indexes on each existing and future
    # partition (subject to type compatibility).
    op.execute("CREATE INDEX events_ts_event_idx ON audit.events (tenant_id, ts_event);")
    op.execute("CREATE INDEX events_event_type_idx ON audit.events (tenant_id, event_type, ts_event);")
    op.execute("CREATE INDEX events_subject_idx ON audit.events (tenant_id, subject_type, subject_id, ts_event);")

    # ── audit.payloads parent (partitioned by RANGE on ts_captured) ────
    source_check = ", ".join(f"'{s}'" for s in _PAYLOAD_SOURCES)
    retention_check = ", ".join(f"'{r}'" for r in _RETENTION_CLASSES)
    op.execute(
        f"""
        CREATE TABLE audit.payloads (
            payload_id          UUID         NOT NULL DEFAULT gen_random_uuid(),
            tenant_id           TEXT         NOT NULL,
            ts_captured         TIMESTAMPTZ  NOT NULL DEFAULT now(),
            source              TEXT         NOT NULL,
            source_url          TEXT,
            payload_kind        TEXT         NOT NULL,
            payload_bytes       BYTEA,
            payload_size_bytes  BIGINT       NOT NULL,
            payload_hash        BYTEA        NOT NULL,
            content_type        TEXT         NOT NULL,
            encoding_notes      TEXT,
            retention_class     TEXT         NOT NULL,
            CONSTRAINT payloads_pkey             PRIMARY KEY (payload_id, ts_captured),
            CONSTRAINT payloads_source_valid     CHECK (source IN ({source_check})),
            CONSTRAINT payloads_retention_valid  CHECK (retention_class IN ({retention_check})),
            CONSTRAINT payloads_hash_len         CHECK (octet_length(payload_hash) = 32),
            CONSTRAINT payloads_size_nonneg      CHECK (payload_size_bytes >= 0)
        ) PARTITION BY RANGE (ts_captured);
        """
    )

    op.execute("CREATE INDEX payloads_source_idx ON audit.payloads (tenant_id, source, ts_captured);")
    op.execute("CREATE INDEX payloads_hash_idx   ON audit.payloads (payload_hash);")
    op.execute("CREATE INDEX payloads_kind_idx   ON audit.payloads (tenant_id, payload_kind, ts_captured);")

    # ── Pre-create monthly partitions ──────────────────────────────────
    # Calculated at migration apply time. Twelve months gives the writer
    # ample runway. A later phase adds automated partition rotation.
    for suffix, lower, upper in _partition_ranges(_PARTITION_MONTHS_AHEAD):
        op.execute(
            f"CREATE TABLE audit.events_{suffix} "
            f"PARTITION OF audit.events "
            f"FOR VALUES FROM ('{lower}') TO ('{upper}');"
        )
        op.execute(
            f"CREATE TABLE audit.payloads_{suffix} "
            f"PARTITION OF audit.payloads "
            f"FOR VALUES FROM ('{lower}') TO ('{upper}');"
        )


def downgrade() -> None:
    """Drop the entire audit schema. Safe because no data exists yet."""
    op.execute("DROP SCHEMA IF EXISTS audit CASCADE")
