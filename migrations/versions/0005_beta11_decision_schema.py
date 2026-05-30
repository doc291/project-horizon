"""create beta11 schema with decisions and stakeholder_actions tables

Revision ID: 0005_beta11_decision_schema
Revises: 0004_audit_schema
Create Date: 2026-05-31

Beta 11 Phase 1 per the approved implementation plan. Creates the
operational decision loop data model:
  - beta11.decisions             one row per issued decision
  - beta11.stakeholder_actions   one row per required stakeholder per decision

This migration is ADDITIVE ONLY. It creates a new schema and two new tables
in it. It does NOT touch any existing schema, table, audit object, or
operational data, and it does not read or modify the public schema. The
Beta 10 baseline runs with no database at all, so applying or not applying
this migration has zero effect on Beta 10 behaviour.

These tables are the durable projection of the operational decision state
held by beta11_decision.py. They are written only when DATABASE_URL is set
and only on the Beta 11 code path (BETA11_ENABLED true).

Reversibility:
  - downgrade() drops the entire beta11 schema CASCADE
  - the beta11 schema is new in this migration so no pre existing data is lost
"""

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0005_beta11_decision_schema"
down_revision: Union[str, None] = "0004_audit_schema"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


# Closed sets mirrored from beta11_decision.py. Enforced at the database
# level as CHECK constraints so a projection bug cannot write an invalid
# state. Keep these in sync with the module if the vocabulary ever changes.
_DECISION_STATES = [
    "DRAFT", "ISSUED", "PROPAGATED", "CONFIRMED", "SUPERSEDED", "EXPIRED",
]
_CONFIRMER_ROLES = ["TOWAGE", "PILOTAGE", "TERMINAL"]
_STAKEHOLDER_STATUSES = ["AWAITING", "ACKNOWLEDGED", "FLAGGED"]


def upgrade() -> None:
    """Create the beta11 schema and its two operational tables."""
    op.execute("CREATE SCHEMA IF NOT EXISTS beta11")

    state_check = ", ".join(f"'{s}'" for s in _DECISION_STATES)
    op.execute(
        f"""
        CREATE TABLE beta11.decisions (
            decision_id        TEXT         NOT NULL,
            conflict_id        TEXT         NOT NULL,
            decision_state     TEXT         NOT NULL,
            issued_by          TEXT,
            issued_at          TIMESTAMPTZ  NOT NULL,
            confirmed_at       TIMESTAMPTZ,
            decision_deadline  TIMESTAMPTZ,
            predicted_impact   JSONB,
            superseded_by      TEXT,
            propagation_log    JSONB        NOT NULL DEFAULT '[]'::jsonb,
            created_at         TIMESTAMPTZ  NOT NULL,
            updated_at         TIMESTAMPTZ  NOT NULL,
            CONSTRAINT decisions_pkey PRIMARY KEY (decision_id),
            CONSTRAINT decisions_state_valid CHECK (decision_state IN ({state_check}))
        );
        """
    )
    op.execute(
        "CREATE INDEX decisions_conflict_idx "
        "ON beta11.decisions (conflict_id, updated_at);"
    )
    op.execute(
        "CREATE INDEX decisions_state_idx "
        "ON beta11.decisions (decision_state, updated_at);"
    )

    role_check = ", ".join(f"'{r}'" for r in _CONFIRMER_ROLES)
    status_check = ", ".join(f"'{s}'" for s in _STAKEHOLDER_STATUSES)
    op.execute(
        f"""
        CREATE TABLE beta11.stakeholder_actions (
            decision_id   TEXT         NOT NULL,
            role          TEXT         NOT NULL,
            display_name  TEXT         NOT NULL,
            action_label  TEXT         NOT NULL,
            status        TEXT         NOT NULL,
            acted_by      TEXT,
            acted_at      TIMESTAMPTZ,
            flag_reason   TEXT,
            simulated     BOOLEAN      NOT NULL DEFAULT TRUE,
            CONSTRAINT stakeholder_actions_pkey PRIMARY KEY (decision_id, role),
            CONSTRAINT stakeholder_actions_role_valid CHECK (role IN ({role_check})),
            CONSTRAINT stakeholder_actions_status_valid CHECK (status IN ({status_check})),
            CONSTRAINT stakeholder_actions_decision_fk
                FOREIGN KEY (decision_id) REFERENCES beta11.decisions (decision_id)
                ON DELETE CASCADE
        );
        """
    )
    op.execute(
        "CREATE INDEX stakeholder_actions_decision_idx "
        "ON beta11.stakeholder_actions (decision_id);"
    )


def downgrade() -> None:
    """Drop the entire beta11 schema. Safe: the schema is new in this migration."""
    op.execute("DROP SCHEMA IF EXISTS beta11 CASCADE")
