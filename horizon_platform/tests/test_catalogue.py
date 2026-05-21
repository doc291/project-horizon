"""
horizon_platform/tests/test_catalogue.py — PF-M1 role-catalogue tests.

Asserts the default role catalogue (PF-M1 sandbox bootstrap)
satisfies the invariants from the Scope Proposal §3.5 and the
Implementation Plan §7.
"""

from horizon_platform.identity.catalogue import (
    ADMIN_ACTION_CLASSES,
    EXPECTED_ROLE_KEYS,
    READ_ONLY_ACTION_CLASSES,
    ROLE_ADMIN,
    ROLE_HARBOUR_MASTER,
    ROLE_PILOTAGE_COORDINATOR,
    ROLE_PORT_EXECUTIVE,
    ROLE_REPLAY_AUDIT_USER,
    ROLE_TERMINAL_OPERATOR,
    ROLE_TOWAGE_COORDINATOR,
    ROLE_VTSO,
    get_default_role_catalogue,
)
from horizon_platform.identity.types import ActionClass, Role, ScopeClass


def test_catalogue_contains_all_eight_workflow_profile_roles():
    """The catalogue must include exactly the 8 roles mapped to the
    Operational Platform Workflows §3 user profiles."""
    cat = get_default_role_catalogue()
    assert set(cat.keys()) == EXPECTED_ROLE_KEYS


def test_catalogue_role_keys_match_constants():
    """Role-key constants must match the keys in the catalogue."""
    cat = get_default_role_catalogue()
    for k in (
        ROLE_VTSO,
        ROLE_HARBOUR_MASTER,
        ROLE_PORT_EXECUTIVE,
        ROLE_PILOTAGE_COORDINATOR,
        ROLE_TOWAGE_COORDINATOR,
        ROLE_TERMINAL_OPERATOR,
        ROLE_ADMIN,
        ROLE_REPLAY_AUDIT_USER,
    ):
        assert k in cat
        assert isinstance(cat[k], Role)
        assert cat[k].key == k


def test_every_role_has_a_description():
    """Every role must have a non-empty description (configuration hygiene)."""
    cat = get_default_role_catalogue()
    for role in cat.values():
        assert isinstance(role.description, str)
        assert len(role.description) > 0


def test_every_role_has_at_least_one_permission():
    """A role with zero permissions is degenerate; the catalogue must
    not ship one."""
    cat = get_default_role_catalogue()
    for role in cat.values():
        assert len(role.permissions) >= 1, (
            f"Role {role.key!r} has no permissions"
        )


def test_no_role_introduces_write_action_classes():
    """PF-M1 is read-only. No role must reference a write action
    class. (Defensive: ActionClass currently has no write members,
    but this test guards against accidental future additions in
    PF-M1.)"""
    cat = get_default_role_catalogue()
    write_words = {"ack", "acknowledge", "defer", "apply", "reject",
                   "escalate", "override", "commit", "write"}
    for role in cat.values():
        for perm in role.permissions:
            for w in write_words:
                assert w not in perm.action_class.value, (
                    f"Role {role.key!r} contains action class "
                    f"{perm.action_class.value!r} matching forbidden "
                    f"write keyword {w!r}"
                )


def test_admin_actions_are_organisation_scoped_only():
    """Admin actions must be scoped to ORGANISATION, never SYSTEM
    in PF-M1. Cross-organisation admin is PF-M8+ scope."""
    cat = get_default_role_catalogue()
    admin = cat[ROLE_ADMIN]
    for perm in admin.permissions:
        if perm.action_class in ADMIN_ACTION_CLASSES:
            assert perm.scope_class is ScopeClass.ORGANISATION, (
                f"Admin permission {perm.action_class.value!r} must be "
                f"scoped to ORGANISATION, got {perm.scope_class.value!r}"
            )


def test_admin_role_does_NOT_have_silent_operational_read():
    """Admin must be a distinct identity class (Scope Proposal §3.2 /
    §3.6). Admin alone must not silently grant operational read."""
    cat = get_default_role_catalogue()
    admin = cat[ROLE_ADMIN]
    operational_reads = {
        ActionClass.READ_OPERATIONAL,
        ActionClass.READ_AUDIT,
        ActionClass.READ_AUDIT_FULL,
        ActionClass.READ_TERMINAL,
        ActionClass.READ_AGGREGATED,
    }
    granted = {p.action_class for p in admin.permissions}
    assert granted.isdisjoint(operational_reads), (
        "Admin role must not silently grant operational read; "
        "admins requiring operational read must hold a separate "
        "operational role (Scope Proposal §3.6)."
    )


def test_terminal_operator_uses_terminal_scope():
    """Terminal operator's read is finer than port — TERMINAL scope."""
    cat = get_default_role_catalogue()
    terminal = cat[ROLE_TERMINAL_OPERATOR]
    has_terminal_read = any(
        p.action_class is ActionClass.READ_TERMINAL
        and p.scope_class is ScopeClass.TERMINAL
        for p in terminal.permissions
    )
    assert has_terminal_read


def test_port_executive_only_reads_aggregated():
    """Port Authority Executive sees aggregated KPIs only, not
    fine-grained operator actions (Operational Platform Workflows
    §3.3)."""
    cat = get_default_role_catalogue()
    exec_role = cat[ROLE_PORT_EXECUTIVE]
    actions = {p.action_class for p in exec_role.permissions}
    assert ActionClass.READ_AGGREGATED in actions
    # Must NOT have audit-full (would expose operator identity)
    assert ActionClass.READ_AUDIT_FULL not in actions
    # Must NOT have operational read (raw data; not the executive's surface)
    assert ActionClass.READ_OPERATIONAL not in actions


def test_harbour_master_has_full_audit():
    """Harbour Master is accountable; reads full audit detail including
    operator identity (Operational Platform Workflows §3.2)."""
    cat = get_default_role_catalogue()
    hm = cat[ROLE_HARBOUR_MASTER]
    actions = {p.action_class for p in hm.permissions}
    assert ActionClass.READ_AUDIT_FULL in actions
    assert ActionClass.READ_OPERATIONAL in actions


def test_replay_audit_user_is_read_only_audit():
    """Replay / Audit User is observational only — no operational
    write, no admin (Operational Platform Workflows §3.8)."""
    cat = get_default_role_catalogue()
    ru = cat[ROLE_REPLAY_AUDIT_USER]
    actions = {p.action_class for p in ru.permissions}
    # Must read audit
    assert ActionClass.READ_AUDIT in actions
    assert ActionClass.READ_AUDIT_FULL in actions
    # Must NOT have admin
    assert actions.isdisjoint(ADMIN_ACTION_CLASSES)


def test_catalogue_returns_fresh_dict():
    """get_default_role_catalogue() must return a fresh dict each
    call so callers cannot mutate the canonical catalogue."""
    a = get_default_role_catalogue()
    b = get_default_role_catalogue()
    assert a is not b
    a["custom_role"] = a[ROLE_VTSO]
    c = get_default_role_catalogue()
    assert "custom_role" not in c


def test_role_permissions_are_frozen_sets():
    """Mutability of a role's permission set is forbidden."""
    cat = get_default_role_catalogue()
    for role in cat.values():
        assert isinstance(role.permissions, frozenset)


def test_read_only_action_classes_constant_well_formed():
    """READ_ONLY_ACTION_CLASSES must contain only read-prefixed members."""
    for ac in READ_ONLY_ACTION_CLASSES:
        assert ac.value.startswith("read."), (
            f"{ac.value!r} is in READ_ONLY_ACTION_CLASSES but is not a read-class"
        )


def test_admin_action_classes_constant_well_formed():
    """ADMIN_ACTION_CLASSES must contain only admin-prefixed members."""
    for ac in ADMIN_ACTION_CLASSES:
        assert ac.value.startswith("admin."), (
            f"{ac.value!r} is in ADMIN_ACTION_CLASSES but is not an admin-class"
        )
