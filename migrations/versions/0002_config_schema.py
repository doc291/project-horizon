"""create config schema and 8 tenant configuration tables

Revision ID: 0002_config_schema
Revises: 0001_initial_marker
Create Date: 2026-05-12

Phase 0 / Step 0.3 per ADR-002. Creates the per-tenant configuration
data model as a separate schema (`config`). Application code does NOT
read from these tables yet — Step 0.4 wires the tenant_id resolution
layer; later Phase 0 steps gradually migrate code paths to read from
config.

Tables created:
  - config.tenant            single-row tenant metadata
  - config.ports             port profiles available to this tenant
  - config.feature_flags     per-tenant capability switches
  - config.integrations      per-tenant API endpoints and secret refs
  - config.permissions       global closed set of permission identifiers
  - config.roles             per-tenant role catalogue with permission arrays
  - config.users             per-tenant user accounts with operator handles
  - config.role_assignments  many-to-many users <-> roles

Schema design notes:
  - Per-tenant deployment means each tenant has its own database; the
    `tenant` table holds one row identifying the tenant that owns this
    database. The row's `id` matches the tenant_id used throughout the
    codebase (e.g. 'ams-demo').
  - `permissions` is a global closed set rather than per-tenant because
    the permission catalogue is part of the codebase contract, not
    customer-configurable.
  - Roles carry their permission list as a TEXT[] rather than via a
    many-to-many join table. For v1 with ~12 permissions and a small
    number of roles per tenant, the denormalisation is simpler to query.
  - `secret_ref` in `integrations` holds a *reference* to a secret in an
    external secret manager (Railway env var name today; AWS Secrets
    Manager later). The secret value is never stored in Postgres.
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "0002_config_schema"
down_revision: Union[str, None] = "0001_initial_marker"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create config schema and 8 tables."""
    op.execute("CREATE SCHEMA IF NOT EXISTS config")

    # ── config.tenant ────────────────────────────────────────────────────
    op.create_table(
        "tenant",
        sa.Column("id", sa.Text(), nullable=False),
        sa.Column("display_name", sa.Text(), nullable=False),
        sa.Column("tenant_type", sa.Text(), nullable=False),
        sa.Column(
            "allowed_hostnames",
            postgresql.ARRAY(sa.Text()),
            nullable=False,
            server_default=sa.text("ARRAY[]::TEXT[]"),
        ),
        sa.Column("contract_reference", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint("id", name="tenant_pkey"),
        sa.CheckConstraint(
            "tenant_type IN ('demo', 'qa', 'customer', 'fleet')",
            name="tenant_type_valid",
        ),
        schema="config",
    )

    # ── config.permissions (global closed set, not tenant-scoped) ────────
    op.create_table(
        "permissions",
        sa.Column("id", sa.Text(), nullable=False),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("category", sa.Text(), nullable=False),
        sa.PrimaryKeyConstraint("id", name="permissions_pkey"),
        schema="config",
    )

    # ── config.ports ─────────────────────────────────────────────────────
    op.create_table(
        "ports",
        sa.Column("id", sa.Text(), nullable=False),
        sa.Column("tenant_id", sa.Text(), nullable=False),
        sa.Column("display_name", sa.Text(), nullable=False),
        sa.Column("short_name", sa.Text(), nullable=False),
        sa.Column("unloco", sa.Text(), nullable=True),
        sa.Column("lat", sa.Numeric(10, 6), nullable=True),
        sa.Column("lon", sa.Numeric(10, 6), nullable=True),
        sa.Column("bom_station_id", sa.Text(), nullable=True),
        sa.Column("profile_jsonb", postgresql.JSONB, nullable=False),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint("tenant_id", "id", name="ports_pkey"),
        sa.ForeignKeyConstraint(
            ["tenant_id"], ["config.tenant.id"], name="ports_tenant_fk"
        ),
        schema="config",
    )

    # ── config.feature_flags ─────────────────────────────────────────────
    op.create_table(
        "feature_flags",
        sa.Column("tenant_id", sa.Text(), nullable=False),
        sa.Column("flag_name", sa.Text(), nullable=False),
        sa.Column("flag_value", postgresql.JSONB, nullable=False),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint("tenant_id", "flag_name", name="feature_flags_pkey"),
        sa.ForeignKeyConstraint(
            ["tenant_id"], ["config.tenant.id"], name="feature_flags_tenant_fk"
        ),
        schema="config",
    )

    # ── config.integrations ──────────────────────────────────────────────
    op.create_table(
        "integrations",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("tenant_id", sa.Text(), nullable=False),
        sa.Column("integration_kind", sa.Text(), nullable=False),
        sa.Column("endpoint_url", sa.Text(), nullable=True),
        sa.Column("secret_ref", sa.Text(), nullable=True),
        sa.Column(
            "enabled", sa.Boolean(), nullable=False, server_default=sa.text("false")
        ),
        sa.Column(
            "config_jsonb",
            postgresql.JSONB,
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint("id", name="integrations_pkey"),
        sa.ForeignKeyConstraint(
            ["tenant_id"], ["config.tenant.id"], name="integrations_tenant_fk"
        ),
        sa.UniqueConstraint(
            "tenant_id", "integration_kind", name="integrations_kind_unique"
        ),
        schema="config",
    )

    # ── config.roles ─────────────────────────────────────────────────────
    op.create_table(
        "roles",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("tenant_id", sa.Text(), nullable=False),
        sa.Column("role_name", sa.Text(), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column(
            "permissions",
            postgresql.ARRAY(sa.Text()),
            nullable=False,
            server_default=sa.text("ARRAY[]::TEXT[]"),
        ),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint("id", name="roles_pkey"),
        sa.ForeignKeyConstraint(
            ["tenant_id"], ["config.tenant.id"], name="roles_tenant_fk"
        ),
        sa.UniqueConstraint("tenant_id", "role_name", name="roles_name_unique"),
        schema="config",
    )

    # ── config.users ─────────────────────────────────────────────────────
    op.create_table(
        "users",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("tenant_id", sa.Text(), nullable=False),
        sa.Column("operator_handle", sa.Text(), nullable=False),
        sa.Column("username", sa.Text(), nullable=False),
        sa.Column("password_hash", sa.Text(), nullable=False),
        sa.Column(
            "mfa_enabled", sa.Boolean(), nullable=False, server_default=sa.text("false")
        ),
        sa.Column(
            "active", sa.Boolean(), nullable=False, server_default=sa.text("true")
        ),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint("id", name="users_pkey"),
        sa.ForeignKeyConstraint(
            ["tenant_id"], ["config.tenant.id"], name="users_tenant_fk"
        ),
        sa.UniqueConstraint("tenant_id", "username", name="users_username_unique"),
        sa.UniqueConstraint("tenant_id", "operator_handle", name="users_handle_unique"),
        schema="config",
    )

    # ── config.role_assignments ──────────────────────────────────────────
    op.create_table(
        "role_assignments",
        sa.Column("user_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("role_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "granted_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("granted_by", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("user_id", "role_id", name="role_assignments_pkey"),
        sa.ForeignKeyConstraint(
            ["user_id"], ["config.users.id"], name="role_assignments_user_fk"
        ),
        sa.ForeignKeyConstraint(
            ["role_id"], ["config.roles.id"], name="role_assignments_role_fk"
        ),
        schema="config",
    )


def downgrade() -> None:
    """Drop all config.* tables and the schema itself."""
    op.drop_table("role_assignments", schema="config")
    op.drop_table("users", schema="config")
    op.drop_table("roles", schema="config")
    op.drop_table("integrations", schema="config")
    op.drop_table("feature_flags", schema="config")
    op.drop_table("ports", schema="config")
    op.drop_table("permissions", schema="config")
    op.drop_table("tenant", schema="config")
    op.execute("DROP SCHEMA IF EXISTS config")
