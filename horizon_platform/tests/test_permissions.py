"""
horizon_platform/tests/test_permissions.py — PF-M1 permission-resolution tests.

Asserts the pure permission-resolution functions correctly compute:
  - active port-scope assignments for a user
  - effective port-scope set
  - resolved permission set
  - has_permission boolean
  - filter_by_port_scope (the server-authoritative read filter)
  - can_read_organisation (cross-organisation guard)

Includes Sev-1 cross-port-leakage negative tests per Scope Proposal
§5.5 / §8.4 / §19.4.
"""

from datetime import datetime, timedelta

import pytest

from horizon_platform.access.permissions import (
    active_assignments_for_user,
    can_read_organisation,
    effective_port_scope,
    filter_by_port_scope,
    has_permission,
    resolve_permissions,
)
from horizon_platform.identity.catalogue import (
    ROLE_ADMIN,
    ROLE_HARBOUR_MASTER,
    ROLE_PORT_EXECUTIVE,
    ROLE_VTSO,
    get_default_role_catalogue,
)
from horizon_platform.identity.types import (
    ActionClass,
    Organisation,
    OrganisationType,
    PortScopeAssignment,
    User,
)


# ── Fixtures (pure constructors) ─────────────────────────────────────────────


NOW   = datetime(2026, 5, 21, 10, 0, 0)
PAST  = NOW - timedelta(days=7)
LATER = NOW + timedelta(days=7)


def make_org(id="org-pv", typ=OrganisationType.PORT_AUTHORITY) -> Organisation:
    return Organisation(
        id=id, name=f"Org-{id}", type=typ, created_at=PAST,
    )


def make_user(
    id="u1",
    organisation_id="org-pv",
    *,
    disabled=False,
) -> User:
    return User(
        id=id,
        organisation_id=organisation_id,
        display_name=f"User-{id}",
        email=f"{id}@example.com",
        created_at=PAST,
        disabled_at=NOW if disabled else None,
    )


def make_assignment(
    *,
    user_id="u1",
    port_id="MELBOURNE",
    role_key=ROLE_VTSO,
    organisation_id="org-pv",
    expired=False,
) -> PortScopeAssignment:
    return PortScopeAssignment(
        user_id=user_id,
        port_id=port_id,
        role_key=role_key,
        organisation_id=organisation_id,
        created_at=PAST,
        created_by="admin-1",
        effective_from=PAST,
        effective_to=PAST + timedelta(hours=1) if expired else None,
    )


# ── active_assignments_for_user ──────────────────────────────────────────────


def test_active_assignments_filters_to_user():
    user = make_user(id="u1")
    a1 = make_assignment(user_id="u1", port_id="MELBOURNE")
    a2 = make_assignment(user_id="u2", port_id="MELBOURNE")
    result = active_assignments_for_user(
        user=user, assignments=[a1, a2], as_of=NOW,
    )
    assert result == [a1]


def test_active_assignments_excludes_expired():
    user = make_user(id="u1")
    a_active  = make_assignment(user_id="u1", port_id="MELBOURNE")
    a_expired = make_assignment(user_id="u1", port_id="GEELONG", expired=True)
    result = active_assignments_for_user(
        user=user, assignments=[a_active, a_expired], as_of=NOW,
    )
    assert result == [a_active]


def test_disabled_user_has_no_active_assignments():
    """A disabled user must have no active assignments — even if
    their PortScopeAssignment rows look effective. This is Sev-1
    enforcement at the user level."""
    user = make_user(id="u1", disabled=True)
    a = make_assignment(user_id="u1", port_id="MELBOURNE")
    result = active_assignments_for_user(
        user=user, assignments=[a], as_of=NOW,
    )
    assert result == []


# ── effective_port_scope ─────────────────────────────────────────────────────


def test_effective_port_scope_multi_port_user():
    """A user with assignments at two ports has both in their scope."""
    user = make_user()
    a_mel = make_assignment(user_id="u1", port_id="MELBOURNE")
    a_gee = make_assignment(user_id="u1", port_id="GEELONG")
    scope = effective_port_scope(
        user=user, assignments=[a_mel, a_gee], as_of=NOW,
    )
    assert scope == {"MELBOURNE", "GEELONG"}


def test_effective_port_scope_no_assignments_returns_empty():
    """Per Scope Proposal §5.5, a user with no active assignments
    must produce an empty port-scope set."""
    user = make_user()
    scope = effective_port_scope(
        user=user, assignments=[], as_of=NOW,
    )
    assert scope == set()


def test_effective_port_scope_disabled_user_returns_empty():
    """A disabled user has no port-scope, regardless of past assignments."""
    user = make_user(disabled=True)
    a = make_assignment(user_id="u1", port_id="MELBOURNE")
    scope = effective_port_scope(
        user=user, assignments=[a], as_of=NOW,
    )
    assert scope == set()


# ── resolve_permissions ──────────────────────────────────────────────────────


def test_resolve_permissions_for_vtso():
    user = make_user()
    a = make_assignment(role_key=ROLE_VTSO)
    cat = get_default_role_catalogue()
    perms = resolve_permissions(
        user=user, assignments=[a], role_catalogue=cat, as_of=NOW,
    )
    # VTSO has READ_OPERATIONAL on PORT
    assert any(
        p.action_class is ActionClass.READ_OPERATIONAL
        for p in perms
    )


def test_resolve_permissions_unions_across_assignments():
    """A user with two roles across two ports gets the union of
    permissions."""
    user = make_user()
    a_vtso = make_assignment(role_key=ROLE_VTSO, port_id="MELBOURNE")
    a_hm   = make_assignment(role_key=ROLE_HARBOUR_MASTER, port_id="GEELONG")
    cat = get_default_role_catalogue()
    perms = resolve_permissions(
        user=user, assignments=[a_vtso, a_hm], role_catalogue=cat, as_of=NOW,
    )
    actions = {p.action_class for p in perms}
    assert ActionClass.READ_OPERATIONAL in actions
    # Harbour Master adds these:
    assert ActionClass.READ_AUDIT in actions
    assert ActionClass.READ_AUDIT_FULL in actions


def test_resolve_permissions_drops_unknown_role_defensively():
    """An assignment referencing an unknown role_key must NOT crash
    and must NOT grant permissions. This is a defensive contract for
    configuration drift — and the caller's audit-relevant logging
    should record it (PF-M3 ingests later)."""
    user = make_user()
    a = make_assignment(role_key="not_a_real_role")
    cat = get_default_role_catalogue()
    perms = resolve_permissions(
        user=user, assignments=[a], role_catalogue=cat, as_of=NOW,
    )
    assert perms == frozenset()


def test_resolve_permissions_returns_frozenset():
    """Returned permission set is immutable."""
    user = make_user()
    a = make_assignment()
    cat = get_default_role_catalogue()
    perms = resolve_permissions(
        user=user, assignments=[a], role_catalogue=cat, as_of=NOW,
    )
    assert isinstance(perms, frozenset)


def test_disabled_user_has_no_permissions():
    user = make_user(disabled=True)
    a = make_assignment()
    cat = get_default_role_catalogue()
    perms = resolve_permissions(
        user=user, assignments=[a], role_catalogue=cat, as_of=NOW,
    )
    assert perms == frozenset()


# ── has_permission ───────────────────────────────────────────────────────────


def test_has_permission_true_for_assigned_action():
    user = make_user()
    a = make_assignment(role_key=ROLE_VTSO)
    cat = get_default_role_catalogue()
    assert has_permission(
        user=user, action_class=ActionClass.READ_OPERATIONAL,
        assignments=[a], role_catalogue=cat, as_of=NOW,
    ) is True


def test_has_permission_false_for_unassigned_action():
    """VTSO does not have audit_full; Harbour Master does."""
    user = make_user()
    a = make_assignment(role_key=ROLE_VTSO)
    cat = get_default_role_catalogue()
    assert has_permission(
        user=user, action_class=ActionClass.READ_AUDIT_FULL,
        assignments=[a], role_catalogue=cat, as_of=NOW,
    ) is False


def test_has_permission_disabled_user_false():
    user = make_user(disabled=True)
    a = make_assignment(role_key=ROLE_VTSO)
    cat = get_default_role_catalogue()
    assert has_permission(
        user=user, action_class=ActionClass.READ_OPERATIONAL,
        assignments=[a], role_catalogue=cat, as_of=NOW,
    ) is False


# ── filter_by_port_scope (Sev-1 server-authoritative read filter) ────────────


class _Vessel:
    """Minimal test entity carrying a port_id."""
    def __init__(self, name: str, port_id: str):
        self.name = name
        self.port_id = port_id


def _vessel_port(v: _Vessel):
    return v.port_id


def test_filter_by_port_scope_keeps_in_scope_items():
    user = make_user()
    a = make_assignment(port_id="MELBOURNE")
    vessels = [
        _Vessel("In-Scope", "MELBOURNE"),
        _Vessel("Out-Of-Scope", "DARWIN"),
    ]
    result = filter_by_port_scope(
        items=vessels, port_id_of=_vessel_port,
        user=user, assignments=[a], as_of=NOW,
    )
    assert len(result) == 1
    assert result[0].name == "In-Scope"


def test_filter_by_port_scope_no_scope_returns_empty_SEV1():
    """SEV-1: a user with no port-scope must see zero items.
    Returning even one item from a port not in scope is a security
    defect (Scope Proposal §5.5 / §8.4)."""
    user = make_user()
    vessels = [
        _Vessel("Any", "MELBOURNE"),
        _Vessel("Other", "DARWIN"),
    ]
    result = filter_by_port_scope(
        items=vessels, port_id_of=_vessel_port,
        user=user, assignments=[], as_of=NOW,
    )
    assert result == []


def test_filter_by_port_scope_cross_port_leakage_SEV1():
    """SEV-1: a user scoped to MELBOURNE must NEVER see a DARWIN
    item, no matter how the caller constructs the input list.
    This is the cross-port leakage guard (Scope Proposal §8.4)."""
    user = make_user()
    melbourne_only = make_assignment(port_id="MELBOURNE")
    mixed_items = [
        _Vessel("Mel-1",   "MELBOURNE"),
        _Vessel("Darwin-1", "DARWIN"),
        _Vessel("Mel-2",   "MELBOURNE"),
        _Vessel("Geelong-1", "GEELONG"),
        _Vessel("Darwin-2", "DARWIN"),
    ]
    result = filter_by_port_scope(
        items=mixed_items, port_id_of=_vessel_port,
        user=user, assignments=[melbourne_only], as_of=NOW,
    )
    port_ids = {v.port_id for v in result}
    assert port_ids == {"MELBOURNE"}, (
        f"SEV-1 cross-port leakage: user scoped to MELBOURNE saw {port_ids}"
    )


def test_filter_by_port_scope_drops_items_with_none_port():
    """Items whose port_id_of returns None are dropped (defensive —
    a missing port is not in any scope)."""
    user = make_user()
    a = make_assignment(port_id="MELBOURNE")
    items = [
        _Vessel("OK",     "MELBOURNE"),
        _Vessel("NoPort", None),  # type: ignore[arg-type]
    ]
    result = filter_by_port_scope(
        items=items, port_id_of=_vessel_port,
        user=user, assignments=[a], as_of=NOW,
    )
    assert len(result) == 1
    assert result[0].name == "OK"


def test_filter_by_port_scope_disabled_user_returns_empty():
    user = make_user(disabled=True)
    a = make_assignment(port_id="MELBOURNE")
    items = [_Vessel("MEL", "MELBOURNE")]
    result = filter_by_port_scope(
        items=items, port_id_of=_vessel_port,
        user=user, assignments=[a], as_of=NOW,
    )
    assert result == []


# ── can_read_organisation (cross-organisation guard) ─────────────────────────


def test_can_read_own_organisation():
    user = make_user(id="u1", organisation_id="org-pv")
    assert can_read_organisation(actor=user, target_organisation_id="org-pv") is True


def test_cannot_read_other_organisation_PF_M1():
    """PF-M1 sandbox has no cross-organisation read permissions
    configured (Scope Proposal §3.3, §4.15)."""
    user = make_user(id="u1", organisation_id="org-terminal")
    assert can_read_organisation(actor=user, target_organisation_id="org-pilots") is False


def test_disabled_user_cannot_read_any_organisation():
    user = make_user(id="u1", organisation_id="org-pv", disabled=True)
    assert can_read_organisation(actor=user, target_organisation_id="org-pv") is False
