"""
horizon_platform/identity/catalogue.py — PF-M1 role catalogue.

The PF-M1 default role catalogue maps the eight Operational Platform
Workflows (PR #59) §3 user profiles to permission bundles. Per the
PF-M1 Implementation Plan §7.1, the catalogue is configuration —
not code — but PF-M1 ships an initial in-code catalogue for the
sandbox bootstrap. Live configurations override at runtime in later
slices.

PF-M1 introduces read-only action classes only. Write action classes
(ACK / DEFER / APPLY / REJECT / ESCALATE) are PF-M4 scope and are
NOT present here.

No cross-organisation read permissions are configured in PF-M1
sandbox by default (Scope Proposal §3.3, §4.15).
"""

from __future__ import annotations

from typing import Dict, FrozenSet

from .types import (
    ActionClass,
    Permission,
    Role,
    ScopeClass,
    make_permission,
    make_role,
)


# ── Role keys (constants) ────────────────────────────────────────────────────

ROLE_VTSO                = "vtso"
ROLE_HARBOUR_MASTER      = "harbour_master"
ROLE_PORT_EXECUTIVE      = "port_executive"
ROLE_PILOTAGE_COORDINATOR = "pilotage_coordinator"
ROLE_TOWAGE_COORDINATOR  = "towage_coordinator"
ROLE_TERMINAL_OPERATOR   = "terminal_operator"
ROLE_ADMIN               = "admin"
ROLE_REPLAY_AUDIT_USER   = "replay_audit_user"


# Frozen permission helpers — keeps the catalogue declarative and prevents
# accidental in-place mutation.

def _p(action: ActionClass, scope: ScopeClass) -> Permission:
    return make_permission(action, scope)


# ── Default role catalogue (PF-M1 sandbox bootstrap) ─────────────────────────


def get_default_role_catalogue() -> Dict[str, Role]:
    """Return the PF-M1 default role catalogue as a fresh dict.

    Returns a *fresh* dict on each call so callers cannot mutate the
    canonical catalogue. Permission sets within each role are frozen.

    PF-M1 sandbox bootstraps with this catalogue. Live deployments
    override via configuration; the runtime loader (later slices)
    must produce the same Role / Permission shapes.
    """
    return {
        ROLE_VTSO: make_role(
            key=ROLE_VTSO,
            description="Vessel Traffic Services Operator — coordination operator (Operational Platform Workflows §3.1)",
            permissions=frozenset({
                _p(ActionClass.READ_OPERATIONAL, ScopeClass.PORT),
            }),
        ),
        ROLE_HARBOUR_MASTER: make_role(
            key=ROLE_HARBOUR_MASTER,
            description="Harbour Master — operational accountability (Operational Platform Workflows §3.2)",
            permissions=frozenset({
                _p(ActionClass.READ_OPERATIONAL, ScopeClass.PORT),
                _p(ActionClass.READ_AUDIT,       ScopeClass.PORT),
                _p(ActionClass.READ_AUDIT_FULL,  ScopeClass.PORT),
            }),
        ),
        ROLE_PORT_EXECUTIVE: make_role(
            key=ROLE_PORT_EXECUTIVE,
            description="Port Authority Executive / Viewer — strategic, read-only (Operational Platform Workflows §3.3)",
            permissions=frozenset({
                _p(ActionClass.READ_AGGREGATED, ScopeClass.PORT),
            }),
        ),
        ROLE_PILOTAGE_COORDINATOR: make_role(
            key=ROLE_PILOTAGE_COORDINATOR,
            description="Pilotage Coordinator — dispatch / schedule context (Operational Platform Workflows §3.4)",
            permissions=frozenset({
                _p(ActionClass.READ_OPERATIONAL, ScopeClass.PORT),
            }),
        ),
        ROLE_TOWAGE_COORDINATOR: make_role(
            key=ROLE_TOWAGE_COORDINATOR,
            description="Towage Coordinator — tug dispatch context (Operational Platform Workflows §3.5)",
            permissions=frozenset({
                _p(ActionClass.READ_OPERATIONAL, ScopeClass.PORT),
            }),
        ),
        ROLE_TERMINAL_OPERATOR: make_role(
            key=ROLE_TERMINAL_OPERATOR,
            description="Terminal Operator — own terminal's berths only (Operational Platform Workflows §3.6)",
            permissions=frozenset({
                _p(ActionClass.READ_TERMINAL, ScopeClass.TERMINAL),
            }),
        ),
        ROLE_ADMIN: make_role(
            key=ROLE_ADMIN,
            description="Admin / System Owner — provisioning within own organisation (Operational Platform Workflows §3.7)",
            permissions=frozenset({
                _p(ActionClass.ADMIN_USER,       ScopeClass.ORGANISATION),
                _p(ActionClass.ADMIN_ROLE,       ScopeClass.ORGANISATION),
                _p(ActionClass.ADMIN_PORT_SCOPE, ScopeClass.ORGANISATION),
            }),
        ),
        ROLE_REPLAY_AUDIT_USER: make_role(
            key=ROLE_REPLAY_AUDIT_USER,
            description="Replay / Audit User — historical audit visibility (Operational Platform Workflows §3.8)",
            permissions=frozenset({
                _p(ActionClass.READ_AUDIT,      ScopeClass.PORT),
                _p(ActionClass.READ_AUDIT_FULL, ScopeClass.PORT),
            }),
        ),
    }


# ── Catalogue invariants (referenced by tests) ───────────────────────────────


# The eight role keys expected from Operational Platform Workflows §3.
EXPECTED_ROLE_KEYS: FrozenSet[str] = frozenset({
    ROLE_VTSO,
    ROLE_HARBOUR_MASTER,
    ROLE_PORT_EXECUTIVE,
    ROLE_PILOTAGE_COORDINATOR,
    ROLE_TOWAGE_COORDINATOR,
    ROLE_TERMINAL_OPERATOR,
    ROLE_ADMIN,
    ROLE_REPLAY_AUDIT_USER,
})


# Read-only action classes (PF-M1 scope). Write actions arrive in PF-M4.
READ_ONLY_ACTION_CLASSES: FrozenSet[ActionClass] = frozenset({
    ActionClass.READ_OPERATIONAL,
    ActionClass.READ_AUDIT,
    ActionClass.READ_AGGREGATED,
    ActionClass.READ_AUDIT_FULL,
    ActionClass.READ_TERMINAL,
})

# Admin action classes (PF-M1 scope; organisation-scoped only).
ADMIN_ACTION_CLASSES: FrozenSet[ActionClass] = frozenset({
    ActionClass.ADMIN_USER,
    ActionClass.ADMIN_ROLE,
    ActionClass.ADMIN_PORT_SCOPE,
})
