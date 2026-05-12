"""seed ams-demo tenant — ports, demo user, feature flags, permissions, role

Revision ID: 0003_seed_ams_demo_tenant
Revises: 0002_config_schema
Create Date: 2026-05-12

Phase 0 / Step 0.3 seed per ADR-002. Populates the AMS demo tenant
configuration so that, once Step 0.4 wires the tenant resolution layer
and later phases gradually migrate code paths to read from `config.*`,
the demo continues to behave identically.

What this seeds:
  - One `config.tenant` row: id='ams-demo', type='demo'
  - The closed v1 permission catalogue (12 permissions)
  - Default feature flags for the demo tenant per ADR-002 §3.3
  - Four port profiles (Brisbane, Melbourne, Geelong, Darwin) imported
    from port_profiles.py at migration apply time
  - One `demo_operator` role granted every permission
  - One demo user (username='horizon', operator_handle='O-1') with a
    PLACEHOLDER password hash — real authentication still goes through
    the env vars HORIZON_USER/HORIZON_PASS until a later Phase wires
    auth to config.users
  - The demo user is assigned the demo_operator role

Application behaviour is unchanged: server.py and all operational paths
continue reading HORIZON_PORT, port_profiles.py, and env vars. This
seed simply ensures the tables contain the right shape of data when
those reads start moving to config.* in later steps.
"""

import json
import os
import sys
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0003_seed_ams_demo_tenant"
down_revision: Union[str, None] = "0002_config_schema"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


# ── Closed v1 permission catalogue ──────────────────────────────────────
# Adding a permission requires a migration; removing one requires careful
# review (existing roles may reference it).
_PERMISSIONS: list[tuple[str, str, str]] = [
    ("audit.read",                 "View audit ledger records",            "audit"),
    ("audit.export",               "Export audit ledger for inquiry",      "audit"),
    ("recommendation.acknowledge", "Acknowledge a recommendation",         "recommendation"),
    ("recommendation.act",         "Take action on a recommendation",      "recommendation"),
    ("recommendation.override",    "Override with a different option",     "recommendation"),
    ("recommendation.defer",       "Defer a recommendation to later",      "recommendation"),
    ("whatif.run",                 "Run a scenario in the engine",         "scenario"),
    ("whatif.apply",               "Apply a scenario overlay to live",     "scenario"),
    ("portbrief.generate",         "Generate a Port Brief PDF",            "reporting"),
    ("portbrief.email",            "Email a Port Brief to recipients",     "reporting"),
    ("port.switch",                "Switch the active port",               "navigation"),
    ("config.read",                "View tenant configuration",            "admin"),
]


# ── AMS demo tenant feature flag defaults (per ADR-002 §3.3) ────────────
_AMS_DEMO_FLAGS: dict[str, object] = {
    "enable_multi_port":            True,
    "enable_what_if":               True,
    "enable_port_brief":            True,
    "enable_aisstream":             False,   # conditional on key — config in integrations later
    "enable_mst":                   False,   # conditional on key
    "enable_simulated_inbound":     True,
    "enable_audit_export":          True,
    "enable_external_anchoring":   False,    # deferred to v1.1
    "enable_experimental_features": True,
    "read_only_mode":              False,
}


def upgrade() -> None:
    """Populate AMS demo tenant configuration."""
    # Make repo root importable so we can load port_profiles.py.
    # Migration files live at migrations/versions/, so repo root is two levels up.
    sys.path.insert(
        0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    )
    from port_profiles import PORT_PROFILES  # noqa: E402

    conn = op.get_bind()

    # 1. Tenant row
    conn.execute(
        sa.text(
            """
            INSERT INTO config.tenant
                (id, display_name, tenant_type, allowed_hostnames, contract_reference)
            VALUES
                (:id, :display_name, :tenant_type, :hostnames, NULL)
            """
        ),
        {
            "id": "ams-demo",
            "display_name": "AMS Group Demo",
            "tenant_type": "demo",
            "hostnames": ["app.horizon.ams.group", "horizon.ams.group"],
        },
    )

    # 2. Permissions (global closed set)
    for pid, descr, category in _PERMISSIONS:
        conn.execute(
            sa.text(
                """
                INSERT INTO config.permissions (id, description, category)
                VALUES (:id, :descr, :category)
                """
            ),
            {"id": pid, "descr": descr, "category": category},
        )

    # 3. Feature flags for the demo tenant
    for flag_name, flag_value in _AMS_DEMO_FLAGS.items():
        conn.execute(
            sa.text(
                """
                INSERT INTO config.feature_flags (tenant_id, flag_name, flag_value)
                VALUES (:tenant_id, :flag_name, CAST(:flag_value AS JSONB))
                """
            ),
            {
                "tenant_id": "ams-demo",
                "flag_name": flag_name,
                "flag_value": json.dumps(flag_value),
            },
        )

    # 4. Four port profiles — full profile dict stored as JSONB
    for port_id in ("BRISBANE", "MELBOURNE", "GEELONG", "DARWIN"):
        profile = PORT_PROFILES[port_id]
        conn.execute(
            sa.text(
                """
                INSERT INTO config.ports
                    (id, tenant_id, display_name, short_name, unloco,
                     lat, lon, bom_station_id, profile_jsonb)
                VALUES
                    (:id, :tenant_id, :display_name, :short_name, :unloco,
                     :lat, :lon, :bom_station_id, CAST(:profile AS JSONB))
                """
            ),
            {
                "id":             port_id,
                "tenant_id":      "ams-demo",
                "display_name":   profile["display_name"],
                "short_name":     profile["short_name"],
                "unloco":         profile.get("unloco"),
                "lat":            profile.get("lat"),
                "lon":            profile.get("lon"),
                "bom_station_id": profile.get("bom_station_id"),
                "profile":        json.dumps(profile, default=str),
            },
        )

    # 5. Demo role — all permissions
    role_row = conn.execute(
        sa.text(
            """
            INSERT INTO config.roles
                (tenant_id, role_name, description, permissions)
            VALUES
                (:tenant_id, :role_name, :description, :permissions)
            RETURNING id
            """
        ),
        {
            "tenant_id":   "ams-demo",
            "role_name":   "demo_operator",
            "description": "All permissions — used for AMS Group sales demos and internal validation",
            "permissions": [p[0] for p in _PERMISSIONS],
        },
    ).fetchone()
    role_id = role_row[0]

    # 6. Demo user — placeholder password hash (real auth still uses env vars)
    user_row = conn.execute(
        sa.text(
            """
            INSERT INTO config.users
                (tenant_id, operator_handle, username, password_hash, mfa_enabled, active)
            VALUES
                (:tenant_id, :operator_handle, :username, :password_hash, FALSE, TRUE)
            RETURNING id
            """
        ),
        {
            "tenant_id":       "ams-demo",
            "operator_handle": "O-1",
            "username":        "horizon",
            "password_hash":   "PHASE_0_PLACEHOLDER_NOT_USED_FOR_AUTH",
        },
    ).fetchone()
    user_id = user_row[0]

    # 7. Assign demo role to demo user
    conn.execute(
        sa.text(
            """
            INSERT INTO config.role_assignments
                (user_id, role_id, granted_by)
            VALUES
                (:user_id, :role_id, :granted_by)
            """
        ),
        {
            "user_id":    user_id,
            "role_id":    role_id,
            "granted_by": "PHASE_0_SEED",
        },
    )


def downgrade() -> None:
    """Remove all AMS demo tenant seed data."""
    conn = op.get_bind()
    # Delete in reverse FK order
    conn.execute(sa.text("DELETE FROM config.role_assignments"))
    conn.execute(sa.text("DELETE FROM config.users WHERE tenant_id = 'ams-demo'"))
    conn.execute(sa.text("DELETE FROM config.roles WHERE tenant_id = 'ams-demo'"))
    conn.execute(sa.text("DELETE FROM config.feature_flags WHERE tenant_id = 'ams-demo'"))
    conn.execute(sa.text("DELETE FROM config.ports WHERE tenant_id = 'ams-demo'"))
    conn.execute(sa.text("DELETE FROM config.permissions"))
    conn.execute(sa.text("DELETE FROM config.tenant WHERE id = 'ams-demo'"))
