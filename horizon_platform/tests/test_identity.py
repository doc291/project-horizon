"""
horizon_platform/tests/test_identity.py — PF-M1 identity-type tests.

Asserts that the dataclass + enum domain types are constructible,
behave as expected (active / disabled / effective-interval), and
preserve the closed-set invariants required by the Scope Proposal
§3.2 / §3.3.

Pure unit tests. No I/O, no persistence, no HTTP, no UI.
"""

from datetime import datetime, timedelta

import pytest

from horizon_platform.identity.types import (
    ActionClass,
    Organisation,
    OrganisationType,
    Permission,
    PortScopeAssignment,
    Role,
    ScopeClass,
    User,
    make_permission,
    make_role,
)


# ── Closed-enum invariants ───────────────────────────────────────────────────


def test_organisation_type_is_closed_set_of_six():
    """OrganisationType must be exactly the six stakeholder classes
    documented in the PF-M1 Implementation Plan §6.1."""
    expected = {
        "port_authority",
        "terminal_operator",
        "pilot_organisation",
        "tug_operator",
        "regulator",
        "partner",
    }
    actual = {member.value for member in OrganisationType}
    assert actual == expected


def test_action_class_pf_m1_set():
    """ActionClass must contain the PF-M1 read-only classes and
    admin classes; must NOT contain write action classes (PF-M4)."""
    members = {m.value for m in ActionClass}
    # PF-M1 read-only classes — present
    assert "read.operational" in members
    assert "read.audit" in members
    assert "read.aggregated" in members
    assert "read.audit_full" in members
    assert "read.terminal" in members
    # PF-M1 admin classes — present
    assert "admin.user" in members
    assert "admin.role" in members
    assert "admin.port_scope" in members
    # PF-M4 write classes — must NOT be present in PF-M1
    for forbidden in (
        "ack", "acknowledge",
        "defer",
        "apply",
        "reject",
        "escalate",
        "override",
        "commit",
        "write.operational",
    ):
        assert forbidden not in members, (
            f"PF-M1 must not introduce write action class {forbidden!r}; "
            f"write actions are PF-M4 scope."
        )


def test_scope_class_closed_set():
    """ScopeClass must be the four documented scope classes."""
    actual = {m.value for m in ScopeClass}
    assert actual == {"port", "terminal", "organisation", "system"}


# ── Permission / Role constructibility ───────────────────────────────────────


def test_permission_is_frozen_dataclass():
    p = make_permission(ActionClass.READ_OPERATIONAL, ScopeClass.PORT)
    assert p.action_class is ActionClass.READ_OPERATIONAL
    assert p.scope_class  is ScopeClass.PORT
    # frozen
    with pytest.raises(Exception):
        p.action_class = ActionClass.READ_AUDIT  # type: ignore[misc]


def test_role_freezes_permission_set():
    perms = {make_permission(ActionClass.READ_OPERATIONAL, ScopeClass.PORT)}
    role = make_role(key="vtso", description="VTSO", permissions=perms)
    assert role.key == "vtso"
    assert isinstance(role.permissions, frozenset)
    assert len(role.permissions) == 1


def test_role_make_role_freezes_iterable_input():
    """make_role must accept any iterable of permissions and freeze it."""
    perms_list = [
        make_permission(ActionClass.READ_OPERATIONAL, ScopeClass.PORT),
        make_permission(ActionClass.READ_AUDIT, ScopeClass.PORT),
    ]
    role = make_role(key="hm", description="Harbour Master", permissions=perms_list)  # type: ignore[arg-type]
    assert isinstance(role.permissions, frozenset)
    assert len(role.permissions) == 2


# ── Organisation ─────────────────────────────────────────────────────────────


def test_organisation_active_by_default():
    now = datetime(2026, 5, 21, 10, 0, 0)
    org = Organisation(
        id="org-1",
        name="Ports Victoria",
        type=OrganisationType.PORT_AUTHORITY,
        created_at=now,
    )
    assert org.is_active() is True
    assert org.disabled_at is None


def test_organisation_inactive_when_disabled():
    now = datetime(2026, 5, 21, 10, 0, 0)
    org = Organisation(
        id="org-1",
        name="Old Org",
        type=OrganisationType.PORT_AUTHORITY,
        created_at=now,
        disabled_at=now + timedelta(days=1),
    )
    assert org.is_active() is False


def test_organisation_is_frozen():
    now = datetime(2026, 5, 21, 10, 0, 0)
    org = Organisation(
        id="org-1", name="X", type=OrganisationType.PORT_AUTHORITY, created_at=now,
    )
    with pytest.raises(Exception):
        org.name = "Y"  # type: ignore[misc]


# ── User ─────────────────────────────────────────────────────────────────────


def test_user_active_by_default():
    now = datetime(2026, 5, 21, 10, 0, 0)
    u = User(
        id="u1",
        organisation_id="org-1",
        display_name="VTSO Alice",
        email="alice@example.com",
        created_at=now,
    )
    assert u.is_active() is True
    assert u.disabled_at is None


def test_user_inactive_when_disabled():
    now = datetime(2026, 5, 21, 10, 0, 0)
    u = User(
        id="u1",
        organisation_id="org-1",
        display_name="X",
        email=None,
        created_at=now,
        disabled_at=now + timedelta(days=1),
    )
    assert u.is_active() is False


def test_user_email_optional():
    """Resource-safe-code users (per PR #57 §5.9) may have no email."""
    now = datetime(2026, 5, 21, 10, 0, 0)
    u = User(
        id="u-pilot",
        organisation_id="org-pilots",
        display_name="PILOT_BNE_PSP_03",
        email=None,
        created_at=now,
    )
    assert u.email is None
    assert u.display_name == "PILOT_BNE_PSP_03"


def test_user_belongs_to_exactly_one_organisation():
    """PF-M1 forbids multi-organisation membership (Scope Proposal §4.15)."""
    now = datetime(2026, 5, 21, 10, 0, 0)
    u = User(
        id="u1",
        organisation_id="org-1",
        display_name="X",
        email=None,
        created_at=now,
    )
    # Single string, not a list. No way to express multiple memberships.
    assert isinstance(u.organisation_id, str)


# ── PortScopeAssignment ──────────────────────────────────────────────────────


def test_port_scope_active_within_interval():
    now    = datetime(2026, 5, 21, 10, 0, 0)
    past   = now - timedelta(days=1)
    future = now + timedelta(days=1)
    a = PortScopeAssignment(
        user_id="u1", port_id="MELBOURNE", role_key="vtso",
        organisation_id="org-1",
        created_at=past, created_by="admin-1",
        effective_from=past, effective_to=future,
    )
    assert a.is_active(as_of=now) is True


def test_port_scope_inactive_before_effective_from():
    now  = datetime(2026, 5, 21, 10, 0, 0)
    later = now + timedelta(days=1)
    a = PortScopeAssignment(
        user_id="u1", port_id="MELBOURNE", role_key="vtso",
        organisation_id="org-1",
        created_at=now, created_by="admin-1",
        effective_from=later,
    )
    assert a.is_active(as_of=now) is False


def test_port_scope_inactive_after_effective_to():
    earlier = datetime(2026, 5, 21, 10, 0, 0)
    expired = earlier + timedelta(days=1)
    now     = expired + timedelta(seconds=1)
    a = PortScopeAssignment(
        user_id="u1", port_id="MELBOURNE", role_key="vtso",
        organisation_id="org-1",
        created_at=earlier, created_by="admin-1",
        effective_from=earlier, effective_to=expired,
    )
    assert a.is_active(as_of=now) is False


def test_port_scope_indefinite_when_no_end():
    now    = datetime(2026, 5, 21, 10, 0, 0)
    future = now + timedelta(days=365)
    a = PortScopeAssignment(
        user_id="u1", port_id="MELBOURNE", role_key="vtso",
        organisation_id="org-1",
        created_at=now, created_by="admin-1",
        effective_from=now, effective_to=None,
    )
    assert a.is_active(as_of=now) is True
    assert a.is_active(as_of=future) is True


def test_port_scope_assignment_is_frozen():
    now = datetime(2026, 5, 21, 10, 0, 0)
    a = PortScopeAssignment(
        user_id="u1", port_id="MELBOURNE", role_key="vtso",
        organisation_id="org-1",
        created_at=now, created_by="admin-1",
        effective_from=now,
    )
    with pytest.raises(Exception):
        a.port_id = "DARWIN"  # type: ignore[misc]
