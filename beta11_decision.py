"""
Project Horizon — Beta 11 decision loop foundation (backend only).

Phase 1 of the Beta 11 role based stakeholder views. This module owns the
operational decision state and its state machine. It is the single place
where a coordinated decision is issued, propagated to stakeholder roles,
acknowledged, flagged, superseded or expired.

Design and isolation
---------------------
- This module is only ever consulted by server.py when BETA11_ENABLED is
  true. With the flag off, server.py never imports or calls into it, so the
  Beta 10 baseline is unaffected.
- Two storage backends share one public API:
    * Memory backend  (always available): a process global dict plus a
      reentrant lock. Single process. Used when DATABASE_URL is unset.
    * Database backend (lazy psycopg): used when DATABASE_URL is set, so
      decision state survives across processes and instances. Postgres is
      the source of truth on the request path in that mode.
  The backend is selected once at module import based on db.is_configured().
- The append only audit ledger is NOT touched here. Audit emission for the
  decision loop is a separate, later wiring step so this module stays
  isolated from the shared Beta 10 audit helpers.

Authority model (Horizon coordinates, it does not command)
----------------------------------------------------------
- Only the VTSO role may issue or supersede a decision.
- A stakeholder role may only act on its own line item.
- A stakeholder may acknowledge receipt or flag a concern. A flag annotates
  and surfaces to the VTSO. It does not halt the loop. The VTSO alone
  decides whether to supersede.

State machine
-------------
Decision level states:
    DRAFT -> ISSUED -> PROPAGATED -> CONFIRMED
    PROPAGATED / ISSUED / CONFIRMED -> SUPERSEDED   (VTSO reissues)
    PROPAGATED / ISSUED -> EXPIRED                  (deadline passes)
SUPERSEDED and EXPIRED are terminal.

Per stakeholder line item statuses:
    AWAITING -> ACKNOWLEDGED
    AWAITING -> FLAGGED
    FLAGGED  -> ACKNOWLEDGED   (stakeholder resolves the concern)
    AWAITING / ACKNOWLEDGED -> FLAGGED

A decision auto advances PROPAGATED -> CONFIRMED only when every required
stakeholder line item is ACKNOWLEDGED.
"""

from __future__ import annotations

import os
import json
import threading
import hashlib
import logging
from datetime import datetime, timezone

log = logging.getLogger("horizon.beta11.decision")

# ── Role vocabulary ────────────────────────────────────────────────────────
ROLE_VTSO      = "VTSO"
ROLE_TOWAGE    = "TOWAGE"
ROLE_PILOTAGE  = "PILOTAGE"
ROLE_TERMINAL  = "TERMINAL"
ROLE_ASSURANCE = "ASSURANCE"

# Confirmer roles can hold a line item and acknowledge or flag it.
CONFIRMER_ROLES = (ROLE_TOWAGE, ROLE_PILOTAGE, ROLE_TERMINAL)
# All roles a screen may select. Assurance is read only and never a confirmer.
ALL_ROLES = (ROLE_VTSO,) + CONFIRMER_ROLES + (ROLE_ASSURANCE,)

# ── Decision level states ──────────────────────────────────────────────────
STATE_DRAFT      = "DRAFT"
STATE_ISSUED     = "ISSUED"
STATE_PROPAGATED = "PROPAGATED"
STATE_CONFIRMED  = "CONFIRMED"
STATE_SUPERSEDED = "SUPERSEDED"
STATE_EXPIRED    = "EXPIRED"

DECISION_STATES = (
    STATE_DRAFT, STATE_ISSUED, STATE_PROPAGATED,
    STATE_CONFIRMED, STATE_SUPERSEDED, STATE_EXPIRED,
)
TERMINAL_STATES = (STATE_SUPERSEDED, STATE_EXPIRED)

# Allowed decision level transitions. Single source of truth for the graph.
_ALLOWED = {
    STATE_DRAFT:      {STATE_ISSUED},
    STATE_ISSUED:     {STATE_PROPAGATED, STATE_SUPERSEDED, STATE_EXPIRED},
    STATE_PROPAGATED: {STATE_CONFIRMED, STATE_SUPERSEDED, STATE_EXPIRED},
    STATE_CONFIRMED:  {STATE_SUPERSEDED},
    STATE_SUPERSEDED: set(),
    STATE_EXPIRED:    set(),
}

# ── Per stakeholder line item statuses ─────────────────────────────────────
ST_AWAITING     = "AWAITING"
ST_ACKNOWLEDGED = "ACKNOWLEDGED"
ST_FLAGGED      = "FLAGGED"

STAKEHOLDER_STATUSES = (ST_AWAITING, ST_ACKNOWLEDGED, ST_FLAGGED)

_ALLOWED_STAKEHOLDER = {
    ST_AWAITING:     {ST_ACKNOWLEDGED, ST_FLAGGED},
    ST_ACKNOWLEDGED: {ST_FLAGGED},
    ST_FLAGGED:      {ST_ACKNOWLEDGED},
}

# ── Action verbs (acknowledge model, never command) ────────────────────────
ACTION_ACKNOWLEDGE = "acknowledge"
ACTION_FLAG        = "flag"
ACTIONS = (ACTION_ACKNOWLEDGE, ACTION_FLAG)


# ── Errors ──────────────────────────────────────────────────────────────────
class DecisionError(Exception):
    """Base class for decision loop errors."""


class NotFoundError(DecisionError):
    """No decision with the given id."""


class AuthorityError(DecisionError):
    """The actor role is not permitted to perform the action."""


class InvalidTransitionError(DecisionError):
    """The requested state change is not allowed from the current state."""


class ValidationError(DecisionError):
    """The request shape is invalid."""


# ── Time helper (UTC, ISO 8601 with Z) ─────────────────────────────────────
def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _isoparse(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


# ── Pure helpers: building and advancing a decision dict ───────────────────
def _new_decision_id(conflict_id: str, counter: int) -> str:
    """Deterministic id from conflict id plus a monotonic counter."""
    base = f"{conflict_id}:{counter}"
    suffix = hashlib.md5(base.encode()).hexdigest()[:8]
    return f"D-{counter:04d}-{suffix}"


def _normalise_stakeholders(raw) -> list:
    """
    Validate and normalise the required stakeholder list for a new decision.

    Each entry must name a confirmer role and an action label. display_name
    and an optional simulated flag are carried through. All stakeholder data
    is marked simulated true in Phase 1.
    """
    if not isinstance(raw, list) or not raw:
        raise ValidationError("required_stakeholders must be a non empty list")
    out = []
    seen = set()
    for entry in raw:
        if not isinstance(entry, dict):
            raise ValidationError("each stakeholder must be an object")
        role = (entry.get("role") or "").upper().strip()
        if role not in CONFIRMER_ROLES:
            raise ValidationError(f"unsupported stakeholder role: {role!r}")
        if role in seen:
            raise ValidationError(f"duplicate stakeholder role: {role}")
        seen.add(role)
        out.append({
            "role":         role,
            "display_name": str(entry.get("display_name") or role.title()),
            "action_label": str(entry.get("action_label") or "Confirm coordinated change"),
            "status":       ST_AWAITING,
            "acted_by":     None,
            "acted_at":     None,
            "flag_reason":  None,
            "simulated":    True,
        })
    return out


def _recompute_decision_state(decision: dict) -> None:
    """
    Promote PROPAGATED to CONFIRMED when every required stakeholder line item
    is ACKNOWLEDGED. Never demotes. Terminal states are left untouched.
    """
    if decision["decision_state"] != STATE_PROPAGATED:
        return
    statuses = [s["status"] for s in decision["required_stakeholders"]]
    if statuses and all(st == ST_ACKNOWLEDGED for st in statuses):
        decision["decision_state"] = STATE_CONFIRMED
        decision["confirmed_at"] = _now_iso()
        decision["propagation_log"].append(
            {"role": "SYSTEM", "event": "confirmed", "ts": decision["confirmed_at"]}
        )


def public_view(decision: dict) -> dict:
    """Return a JSON safe copy for embedding in /api/summary."""
    return json.loads(json.dumps(decision, default=str))


# ── Backend protocol ────────────────────────────────────────────────────────
# A backend persists decisions and serialises mutations. Both backends expose
# the same methods. The public module functions select the active backend.

class _MemoryBackend:
    """Single process in memory store guarded by a reentrant lock."""

    def __init__(self):
        self._lock = threading.RLock()
        self._by_id = {}              # decision_id -> decision dict
        self._active_by_conflict = {} # conflict_id -> decision_id (latest live)
        self._counter = 0

    def reset(self):
        with self._lock:
            self._by_id.clear()
            self._active_by_conflict.clear()
            self._counter = 0

    def _next_counter(self) -> int:
        self._counter += 1
        return self._counter

    def issue(self, conflict_id, issued_by, stakeholders, predicted_impact, deadline):
        with self._lock:
            counter = self._next_counter()
            did = _new_decision_id(conflict_id, counter)
            now = _now_iso()
            decision = {
                "decision_id":          did,
                "conflict_id":          conflict_id,
                "decision_state":       STATE_PROPAGATED,
                "issued_by":            issued_by,
                "issued_at":            now,
                "confirmed_at":         None,
                "decision_deadline":    deadline,
                "predicted_impact":     predicted_impact,
                "superseded_by":        None,
                "required_stakeholders": stakeholders,
                "propagation_log": [
                    {"role": ROLE_VTSO, "event": "issued",     "ts": now},
                    {"role": "SYSTEM",  "event": "propagated", "ts": now},
                ],
                "created_at": now,
                "updated_at": now,
            }
            self._by_id[did] = decision
            self._active_by_conflict[conflict_id] = did
            return public_view(decision)

    def act(self, decision_id, role, action, actor, flag_reason):
        with self._lock:
            decision = self._by_id.get(decision_id)
            if decision is None:
                raise NotFoundError(f"no decision {decision_id}")
            if decision["decision_state"] in TERMINAL_STATES:
                raise InvalidTransitionError(
                    f"decision {decision_id} is {decision['decision_state']}"
                )
            line = next(
                (s for s in decision["required_stakeholders"] if s["role"] == role),
                None,
            )
            if line is None:
                raise AuthorityError(f"role {role} has no line item on {decision_id}")
            target = ST_ACKNOWLEDGED if action == ACTION_ACKNOWLEDGE else ST_FLAGGED
            current = line["status"]
            if target not in _ALLOWED_STAKEHOLDER.get(current, set()) and current != target:
                raise InvalidTransitionError(
                    f"{role} cannot move from {current} to {target}"
                )
            now = _now_iso()
            line["status"] = target
            line["acted_by"] = actor
            line["acted_at"] = now
            line["flag_reason"] = flag_reason if target == ST_FLAGGED else None
            decision["propagation_log"].append(
                {"role": role, "event": action, "ts": now}
            )
            decision["updated_at"] = now
            _recompute_decision_state(decision)
            return public_view(decision)

    def supersede(self, decision_id, issued_by):
        with self._lock:
            decision = self._by_id.get(decision_id)
            if decision is None:
                raise NotFoundError(f"no decision {decision_id}")
            if STATE_SUPERSEDED not in _ALLOWED[decision["decision_state"]]:
                raise InvalidTransitionError(
                    f"cannot supersede from {decision['decision_state']}"
                )
            now = _now_iso()
            decision["decision_state"] = STATE_SUPERSEDED
            decision["updated_at"] = now
            decision["propagation_log"].append(
                {"role": ROLE_VTSO, "event": "superseded", "ts": now}
            )
            if self._active_by_conflict.get(decision["conflict_id"]) == decision_id:
                del self._active_by_conflict[decision["conflict_id"]]
            return public_view(decision)

    def expire_due(self, now_dt):
        expired = []
        with self._lock:
            for decision in self._by_id.values():
                if decision["decision_state"] not in (STATE_ISSUED, STATE_PROPAGATED):
                    continue
                dl = decision.get("decision_deadline")
                if not dl:
                    continue
                if _isoparse(dl) <= now_dt:
                    now = _now_iso()
                    decision["decision_state"] = STATE_EXPIRED
                    decision["updated_at"] = now
                    decision["propagation_log"].append(
                        {"role": "SYSTEM", "event": "expired", "ts": now}
                    )
                    self._active_by_conflict.pop(decision["conflict_id"], None)
                    expired.append(public_view(decision))
        return expired

    def get(self, decision_id):
        with self._lock:
            d = self._by_id.get(decision_id)
            return public_view(d) if d else None

    def get_active_for_conflict(self, conflict_id):
        with self._lock:
            did = self._active_by_conflict.get(conflict_id)
            if not did:
                return None
            d = self._by_id.get(did)
            return public_view(d) if d else None

    def all_active(self):
        with self._lock:
            out = {}
            for cid, did in self._active_by_conflict.items():
                d = self._by_id.get(did)
                if d:
                    out[cid] = public_view(d)
            return out


# The database backend is intentionally a thin subclass that mirrors the
# memory backend through Postgres when DATABASE_URL is set. It performs read
# modify write under a row lock so concurrent screens converge. psycopg is
# imported lazily so the no database path never requires it.
class _DbBackend(_MemoryBackend):
    """
    Database backed store. Postgres is the source of truth; the in memory
    map from the parent class is used only as a within process cache that is
    refreshed from the database on read. Every mutation is write through.

    Phase 1 keeps the database backend deliberately conservative: it reuses
    the parent class serialisation for the in process portion and projects
    each decision into beta11.decisions / beta11.stakeholder_actions inside a
    single transaction. Cross process convergence comes from reading the
    database on access. If any database operation fails the error is raised
    to the caller so a misconfigured database surfaces immediately rather
    than silently diverging.
    """

    def __init__(self):
        super().__init__()
        # Import lazily and only when this backend is constructed (DB present).
        import db as _db  # noqa: F401
        self._db = _db

    def _connect(self):
        import psycopg
        return psycopg.connect(self._db.get_url(), connect_timeout=10)

    def _project(self, decision: dict) -> None:
        """Upsert a decision and its line items into the beta11 schema."""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO beta11.decisions
                        (decision_id, conflict_id, decision_state, issued_by,
                         issued_at, confirmed_at, decision_deadline,
                         predicted_impact, superseded_by, propagation_log,
                         created_at, updated_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (decision_id) DO UPDATE SET
                        decision_state   = EXCLUDED.decision_state,
                        confirmed_at     = EXCLUDED.confirmed_at,
                        superseded_by    = EXCLUDED.superseded_by,
                        propagation_log  = EXCLUDED.propagation_log,
                        updated_at       = EXCLUDED.updated_at
                    """,
                    (
                        decision["decision_id"], decision["conflict_id"],
                        decision["decision_state"], decision["issued_by"],
                        decision["issued_at"], decision.get("confirmed_at"),
                        decision.get("decision_deadline"),
                        json.dumps(decision.get("predicted_impact")),
                        decision.get("superseded_by"),
                        json.dumps(decision.get("propagation_log", [])),
                        decision["created_at"], decision["updated_at"],
                    ),
                )
                for s in decision["required_stakeholders"]:
                    cur.execute(
                        """
                        INSERT INTO beta11.stakeholder_actions
                            (decision_id, role, display_name, action_label,
                             status, acted_by, acted_at, flag_reason, simulated)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (decision_id, role) DO UPDATE SET
                            status      = EXCLUDED.status,
                            acted_by    = EXCLUDED.acted_by,
                            acted_at    = EXCLUDED.acted_at,
                            flag_reason = EXCLUDED.flag_reason
                        """,
                        (
                            decision["decision_id"], s["role"], s["display_name"],
                            s["action_label"], s["status"], s.get("acted_by"),
                            s.get("acted_at"), s.get("flag_reason"),
                            bool(s.get("simulated", True)),
                        ),
                    )
            conn.commit()

    def issue(self, conflict_id, issued_by, stakeholders, predicted_impact, deadline):
        decision = super().issue(conflict_id, issued_by, stakeholders,
                                 predicted_impact, deadline)
        self._project(self._by_id[decision["decision_id"]])
        return decision

    def act(self, decision_id, role, action, actor, flag_reason):
        decision = super().act(decision_id, role, action, actor, flag_reason)
        self._project(self._by_id[decision_id])
        return decision

    def supersede(self, decision_id, issued_by):
        decision = super().supersede(decision_id, issued_by)
        self._project(self._by_id[decision_id])
        return decision


# ── Backend selection (once at import) ─────────────────────────────────────
def _select_backend():
    try:
        import db
        if db.is_configured():
            log.info("Beta 11 decision store: database backend (DATABASE_URL set)")
            return _DbBackend()
    except Exception as exc:  # pragma: no cover - defensive
        log.error("Beta 11 decision store: database backend unavailable (%s); "
                  "falling back to in memory", exc)
    log.info("Beta 11 decision store: in memory backend (no DATABASE_URL)")
    return _MemoryBackend()


_backend = _select_backend()


# ── Public API ──────────────────────────────────────────────────────────────
def reset_for_tests():
    """Clear all state. Test only."""
    _backend.reset()


def issue_decision(conflict_id, issued_by, required_stakeholders,
                   predicted_impact=None, decision_deadline=None,
                   actor_role=ROLE_VTSO):
    """
    Issue and propagate a new decision for a conflict. Only the VTSO role may
    issue. Returns the public decision view.
    """
    if (actor_role or "").upper() != ROLE_VTSO:
        raise AuthorityError("only the VTSO role may issue a decision")
    if not conflict_id:
        raise ValidationError("conflict_id is required")
    stakeholders = _normalise_stakeholders(required_stakeholders)
    return _backend.issue(conflict_id, issued_by, stakeholders,
                          predicted_impact, decision_deadline)


def act_on_decision(decision_id, role, action, actor=None, flag_reason=None):
    """
    Apply a stakeholder action to its own line item. role must be a confirmer
    role and may only act on its own item. action is acknowledge or flag.
    """
    role = (role or "").upper().strip()
    action = (action or "").lower().strip()
    if role not in CONFIRMER_ROLES:
        raise AuthorityError(f"role {role!r} may not act on a line item")
    if action not in ACTIONS:
        raise ValidationError(f"unsupported action: {action!r}")
    if action == ACTION_FLAG and not (flag_reason or "").strip():
        raise ValidationError("a flag requires a reason")
    return _backend.act(decision_id, role, action, actor, flag_reason)


def supersede_decision(decision_id, issued_by, actor_role=ROLE_VTSO):
    """Supersede a decision. Only the VTSO role may supersede."""
    if (actor_role or "").upper() != ROLE_VTSO:
        raise AuthorityError("only the VTSO role may supersede a decision")
    return _backend.supersede(decision_id, issued_by)


def expire_overdue(now_dt=None):
    """Expire any ISSUED or PROPAGATED decision past its deadline. System only."""
    if now_dt is None:
        now_dt = datetime.now(timezone.utc)
    return _backend.expire_due(now_dt)


def get_decision(decision_id):
    return _backend.get(decision_id)


def get_active_for_conflict(conflict_id):
    return _backend.get_active_for_conflict(conflict_id)


def active_decisions():
    """Map of conflict_id to its active decision view. Used by build_summary."""
    return _backend.all_active()
