"""
Project Horizon — Beta 11 decision loop foundation (backend only).

Phase 1 / 1.5 of the Beta 11 role based stakeholder views. This module owns
the operational decision state and its state machine. It is the single place
where a coordinated decision is issued, propagated to stakeholder roles,
acknowledged, flagged, superseded or expired.

Design and isolation
---------------------
- This module is only ever consulted by server.py when BETA11_ENABLED is
  true. With the flag off, server.py never imports or calls into it, so the
  Beta 10 baseline is unaffected.
- Two storage backends share one public API and ONE set of pure transition
  helpers (so the state machine is identical in both):
    * Memory backend  (always available): a process global dict plus a lock.
      Single process. Used when DATABASE_URL is unset.
    * Database backend: used when DATABASE_URL is set. Postgres is the source
      of truth on BOTH the read and write paths, so decision state is
      genuinely consistent across processes and instances. Every mutation is
      a read modify write inside one transaction with the decision row locked
      (SELECT ... FOR UPDATE), and every read queries Postgres. psycopg is
      imported lazily so the no database path never requires it.
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

CONFIRMER_ROLES = (ROLE_TOWAGE, ROLE_PILOTAGE, ROLE_TERMINAL)
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


# ── Time helpers (UTC, ISO 8601 with Z) ────────────────────────────────────
def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _isoparse(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def _fmt_dt(value):
    """Normalise a timestamp (datetime or str or None) to ISO 8601 Z, or None."""
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ── Pure helpers: construct and mutate a decision dict ─────────────────────
# These contain the ENTIRE state machine. Both backends call them so the
# memory and database paths can never diverge in behaviour.

def _new_decision_id(conflict_id: str, counter: int) -> str:
    base = f"{conflict_id}:{counter}"
    suffix = hashlib.md5(base.encode()).hexdigest()[:8]
    return f"D-{counter:04d}-{suffix}"


def _normalise_stakeholders(raw) -> list:
    if not isinstance(raw, list) or not raw:
        raise ValidationError("required_stakeholders must be a non empty list")
    out, seen = [], set()
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


def _construct_decision(decision_id, conflict_id, issued_by, stakeholders,
                        predicted_impact, deadline) -> dict:
    now = _now_iso()
    return {
        "decision_id":          decision_id,
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


def _recompute_decision_state(decision: dict) -> None:
    """Promote PROPAGATED to CONFIRMED when all line items are ACKNOWLEDGED."""
    if decision["decision_state"] != STATE_PROPAGATED:
        return
    statuses = [s["status"] for s in decision["required_stakeholders"]]
    if statuses and all(st == ST_ACKNOWLEDGED for st in statuses):
        decision["decision_state"] = STATE_CONFIRMED
        decision["confirmed_at"] = _now_iso()
        decision["propagation_log"].append(
            {"role": "SYSTEM", "event": "confirmed", "ts": decision["confirmed_at"]}
        )


def _apply_action(decision: dict, role: str, action: str, actor, flag_reason) -> None:
    """
    Apply a stakeholder action to its own line item, in place. Raises on a
    terminal decision, a missing line item, or an illegal status change.
    """
    if decision["decision_state"] in TERMINAL_STATES:
        raise InvalidTransitionError(
            f"decision {decision['decision_id']} is {decision['decision_state']}"
        )
    line = next((s for s in decision["required_stakeholders"] if s["role"] == role), None)
    if line is None:
        raise AuthorityError(f"role {role} has no line item on {decision['decision_id']}")
    target = ST_ACKNOWLEDGED if action == ACTION_ACKNOWLEDGE else ST_FLAGGED
    current = line["status"]
    if target not in _ALLOWED_STAKEHOLDER.get(current, set()) and current != target:
        raise InvalidTransitionError(f"{role} cannot move from {current} to {target}")
    now = _now_iso()
    line["status"] = target
    line["acted_by"] = actor
    line["acted_at"] = now
    line["flag_reason"] = flag_reason if target == ST_FLAGGED else None
    decision["propagation_log"].append({"role": role, "event": action, "ts": now})
    decision["updated_at"] = now
    _recompute_decision_state(decision)


def _apply_supersede(decision: dict, issued_by) -> None:
    """Mark a decision SUPERSEDED in place. Raises if not allowed."""
    if STATE_SUPERSEDED not in _ALLOWED[decision["decision_state"]]:
        raise InvalidTransitionError(f"cannot supersede from {decision['decision_state']}")
    now = _now_iso()
    decision["decision_state"] = STATE_SUPERSEDED
    decision["updated_at"] = now
    decision["propagation_log"].append({"role": ROLE_VTSO, "event": "superseded", "ts": now})


def _apply_expire(decision: dict) -> None:
    now = _now_iso()
    decision["decision_state"] = STATE_EXPIRED
    decision["updated_at"] = now
    decision["propagation_log"].append({"role": "SYSTEM", "event": "expired", "ts": now})


def public_view(decision: dict) -> dict:
    """Return a JSON safe copy for embedding in /api/summary."""
    return json.loads(json.dumps(decision, default=str))


# ── Memory backend ──────────────────────────────────────────────────────────
class _MemoryBackend:
    """Single process in memory store guarded by a reentrant lock."""

    def __init__(self):
        self._lock = threading.RLock()
        self._by_id = {}
        self._active_by_conflict = {}
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
            did = _new_decision_id(conflict_id, self._next_counter())
            decision = _construct_decision(did, conflict_id, issued_by, stakeholders,
                                           predicted_impact, deadline)
            self._by_id[did] = decision
            self._active_by_conflict[conflict_id] = did
            return public_view(decision)

    def act(self, decision_id, role, action, actor, flag_reason):
        with self._lock:
            decision = self._by_id.get(decision_id)
            if decision is None:
                raise NotFoundError(f"no decision {decision_id}")
            _apply_action(decision, role, action, actor, flag_reason)
            return public_view(decision)

    def supersede(self, decision_id, issued_by):
        with self._lock:
            decision = self._by_id.get(decision_id)
            if decision is None:
                raise NotFoundError(f"no decision {decision_id}")
            _apply_supersede(decision, issued_by)
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
                if dl and _isoparse(dl) <= now_dt:
                    _apply_expire(decision)
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
            d = self._by_id.get(did) if did else None
            return public_view(d) if d else None

    def all_active(self):
        with self._lock:
            out = {}
            for cid, did in self._active_by_conflict.items():
                d = self._by_id.get(did)
                if d:
                    out[cid] = public_view(d)
            return out


# ── Database backend (Postgres is the source of truth) ─────────────────────
class _DbBackend:
    """
    Postgres backed store. Every read queries the database and every mutation
    is a read modify write inside one transaction with the decision row locked
    via SELECT ... FOR UPDATE, so concurrent processes and instances converge
    on one consistent state. The same pure transition helpers as the memory
    backend are used, so behaviour is identical.
    """

    # Advisory lock key for serialising the global issue counter across
    # processes. Any stable 32 bit int works.
    _ISSUE_LOCK_KEY = 0x42455431  # "BET1"

    _DEC_COLS = (
        "decision_id, conflict_id, decision_state, issued_by, issued_at, "
        "confirmed_at, decision_deadline, predicted_impact, superseded_by, "
        "propagation_log, created_at, updated_at"
    )
    _STK_COLS = (
        "role, display_name, action_label, status, acted_by, acted_at, "
        "flag_reason, simulated"
    )

    def __init__(self):
        import db as _db
        self._db = _db

    def _connect(self):
        import psycopg
        return psycopg.connect(self._db.get_url(), connect_timeout=10)

    # ── row -> dict reconstruction ──────────────────────────────────────────
    def _row_to_decision(self, drow, srows) -> dict:
        (decision_id, conflict_id, decision_state, issued_by, issued_at,
         confirmed_at, decision_deadline, predicted_impact, superseded_by,
         propagation_log, created_at, updated_at) = drow
        stakeholders = []
        for (role, display_name, action_label, status, acted_by, acted_at,
             flag_reason, simulated) in srows:
            stakeholders.append({
                "role": role, "display_name": display_name,
                "action_label": action_label, "status": status,
                "acted_by": acted_by, "acted_at": _fmt_dt(acted_at),
                "flag_reason": flag_reason, "simulated": bool(simulated),
            })
        return {
            "decision_id": decision_id,
            "conflict_id": conflict_id,
            "decision_state": decision_state,
            "issued_by": issued_by,
            "issued_at": _fmt_dt(issued_at),
            "confirmed_at": _fmt_dt(confirmed_at),
            "decision_deadline": _fmt_dt(decision_deadline),
            "predicted_impact": predicted_impact,
            "superseded_by": superseded_by,
            "required_stakeholders": stakeholders,
            "propagation_log": propagation_log or [],
            "created_at": _fmt_dt(created_at),
            "updated_at": _fmt_dt(updated_at),
        }

    def _load_locked(self, cur, decision_id):
        """SELECT the decision FOR UPDATE plus its line items. None if absent."""
        cur.execute(
            f"SELECT {self._DEC_COLS} FROM beta11.decisions "
            f"WHERE decision_id = %s FOR UPDATE",
            (decision_id,),
        )
        drow = cur.fetchone()
        if drow is None:
            return None
        cur.execute(
            f"SELECT {self._STK_COLS} FROM beta11.stakeholder_actions "
            f"WHERE decision_id = %s ORDER BY role",
            (decision_id,),
        )
        return self._row_to_decision(drow, cur.fetchall())

    def _load(self, cur, decision_id):
        cur.execute(
            f"SELECT {self._DEC_COLS} FROM beta11.decisions WHERE decision_id = %s",
            (decision_id,),
        )
        drow = cur.fetchone()
        if drow is None:
            return None
        cur.execute(
            f"SELECT {self._STK_COLS} FROM beta11.stakeholder_actions "
            f"WHERE decision_id = %s ORDER BY role",
            (decision_id,),
        )
        return self._row_to_decision(drow, cur.fetchall())

    def _insert(self, cur, decision):
        cur.execute(
            """
            INSERT INTO beta11.decisions
                (decision_id, conflict_id, decision_state, issued_by, issued_at,
                 confirmed_at, decision_deadline, predicted_impact, superseded_by,
                 propagation_log, created_at, updated_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
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
                    (decision_id, role, display_name, action_label, status,
                     acted_by, acted_at, flag_reason, simulated)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    decision["decision_id"], s["role"], s["display_name"],
                    s["action_label"], s["status"], s.get("acted_by"),
                    s.get("acted_at"), s.get("flag_reason"),
                    bool(s.get("simulated", True)),
                ),
            )

    def _update(self, cur, decision):
        cur.execute(
            """
            UPDATE beta11.decisions SET
                decision_state  = %s,
                confirmed_at    = %s,
                superseded_by   = %s,
                propagation_log = %s,
                updated_at      = %s
            WHERE decision_id = %s
            """,
            (
                decision["decision_state"], decision.get("confirmed_at"),
                decision.get("superseded_by"),
                json.dumps(decision.get("propagation_log", [])),
                decision["updated_at"], decision["decision_id"],
            ),
        )
        for s in decision["required_stakeholders"]:
            cur.execute(
                """
                UPDATE beta11.stakeholder_actions SET
                    status = %s, acted_by = %s, acted_at = %s, flag_reason = %s
                WHERE decision_id = %s AND role = %s
                """,
                (
                    s["status"], s.get("acted_by"), s.get("acted_at"),
                    s.get("flag_reason"), decision["decision_id"], s["role"],
                ),
            )

    # ── public backend API ──────────────────────────────────────────────────
    def reset(self):
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM beta11.stakeholder_actions")
                cur.execute("DELETE FROM beta11.decisions")
            conn.commit()

    def issue(self, conflict_id, issued_by, stakeholders, predicted_impact, deadline):
        with self._connect() as conn:
            with conn.cursor() as cur:
                # Serialise the global counter across processes for the
                # duration of this transaction.
                cur.execute("SELECT pg_advisory_xact_lock(%s)", (self._ISSUE_LOCK_KEY,))
                cur.execute("SELECT COUNT(*) FROM beta11.decisions")
                counter = cur.fetchone()[0] + 1
                did = _new_decision_id(conflict_id, counter)
                decision = _construct_decision(did, conflict_id, issued_by,
                                               stakeholders, predicted_impact, deadline)
                self._insert(cur, decision)
            conn.commit()
            return public_view(decision)

    def act(self, decision_id, role, action, actor, flag_reason):
        with self._connect() as conn:
            with conn.cursor() as cur:
                decision = self._load_locked(cur, decision_id)
                if decision is None:
                    raise NotFoundError(f"no decision {decision_id}")
                _apply_action(decision, role, action, actor, flag_reason)
                self._update(cur, decision)
            conn.commit()
            return public_view(decision)

    def supersede(self, decision_id, issued_by):
        with self._connect() as conn:
            with conn.cursor() as cur:
                decision = self._load_locked(cur, decision_id)
                if decision is None:
                    raise NotFoundError(f"no decision {decision_id}")
                _apply_supersede(decision, issued_by)
                self._update(cur, decision)
            conn.commit()
            return public_view(decision)

    def expire_due(self, now_dt):
        expired = []
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT decision_id FROM beta11.decisions "
                    "WHERE decision_state IN (%s, %s) AND decision_deadline IS NOT NULL "
                    "AND decision_deadline <= %s FOR UPDATE",
                    (STATE_ISSUED, STATE_PROPAGATED, now_dt),
                )
                ids = [r[0] for r in cur.fetchall()]
                for did in ids:
                    decision = self._load_locked(cur, did)
                    if decision is None:
                        continue
                    _apply_expire(decision)
                    self._update(cur, decision)
                    expired.append(public_view(decision))
            conn.commit()
        return expired

    def get(self, decision_id):
        with self._connect() as conn:
            with conn.cursor() as cur:
                d = self._load(cur, decision_id)
            return public_view(d) if d else None

    def get_active_for_conflict(self, conflict_id):
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT decision_id FROM beta11.decisions "
                    "WHERE conflict_id = %s AND decision_state NOT IN (%s, %s) "
                    "ORDER BY updated_at DESC LIMIT 1",
                    (conflict_id, STATE_SUPERSEDED, STATE_EXPIRED),
                )
                row = cur.fetchone()
                if row is None:
                    return None
                d = self._load(cur, row[0])
            return public_view(d) if d else None

    def all_active(self):
        with self._connect() as conn:
            with conn.cursor() as cur:
                # Latest non terminal decision per conflict.
                cur.execute(
                    "SELECT DISTINCT ON (conflict_id) conflict_id, decision_id "
                    "FROM beta11.decisions "
                    "WHERE decision_state NOT IN (%s, %s) "
                    "ORDER BY conflict_id, updated_at DESC",
                    (STATE_SUPERSEDED, STATE_EXPIRED),
                )
                pairs = cur.fetchall()
                out = {}
                for conflict_id, decision_id in pairs:
                    d = self._load(cur, decision_id)
                    if d:
                        out[conflict_id] = public_view(d)
            return out


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
    """Issue and propagate a new decision. Only the VTSO role may issue."""
    if (actor_role or "").upper() != ROLE_VTSO:
        raise AuthorityError("only the VTSO role may issue a decision")
    if not conflict_id:
        raise ValidationError("conflict_id is required")
    stakeholders = _normalise_stakeholders(required_stakeholders)
    return _backend.issue(conflict_id, issued_by, stakeholders,
                          predicted_impact, decision_deadline)


def act_on_decision(decision_id, role, action, actor=None, flag_reason=None):
    """Apply a stakeholder action to its own line item (acknowledge or flag)."""
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


# ── Beta 11 Phase 2: Melbourne pinned scenario (single source of truth) ─────
# The pinned demo scenario lives here so the server, not the client, owns it.
# All stakeholder data is simulated and labelled. The cost figure is an
# explicitly labelled illustrative value, never presented as evidence.
SCENARIO_PORT = "MELBOURNE"

# Role display metadata used to theme each lens. Pure presentation hints; the
# authority rules live in the state machine, not here.
ROLE_META = {
    ROLE_VTSO:      {"label": "VTSO Coordinator", "tone": "vtso",
                     "blurb": "Detects conflicts and issues coordinated decisions. Sole decision authority."},
    ROLE_TOWAGE:    {"label": "Towage Provider", "tone": "towage",
                     "blurb": "Receives tug reassignment requests and confirms availability."},
    ROLE_PILOTAGE:  {"label": "Pilotage", "tone": "pilotage",
                     "blurb": "Receives amended boarding windows and confirms the pilot can meet them."},
    ROLE_TERMINAL:  {"label": "Terminal", "tone": "terminal",
                     "blurb": "Receives revised arrival windows and confirms berth readiness."},
    ROLE_ASSURANCE: {"label": "Assurance", "tone": "assurance",
                     "blurb": "Read only. Records the full decision loop for audit and verification."},
}


def pinned_stakeholders():
    """
    The three simulated stakeholder line items for the pinned Melbourne
    scenario. Hand authored so the loop always cascades to towage, pilotage
    and terminal even if the live conflict does not naturally imply all three.
    Every entry is clearly simulated.
    """
    return [
        {"role": ROLE_TOWAGE,
         "display_name": "Tug Wando (simulated)",
         "action_label": "Reassign tug to the revised berthing window"},
        {"role": ROLE_PILOTAGE,
         "display_name": "Pilot roster slot 2 (simulated)",
         "action_label": "Amend pilot boarding time to the new tidal window"},
        {"role": ROLE_TERMINAL,
         "display_name": "Appleton Dock berth (simulated)",
         "action_label": "Confirm berth readiness for the revised arrival"},
    ]


def pinned_predicted_impact():
    """
    Labelled, illustrative predicted impact for the pinned scenario. This is a
    simulated figure for demonstration only and must never be shown as evidence.
    """
    return {
        "label": "Simulated illustrative value",
        "headline": "Reduced anchorage waiting and avoided standby",
        "cost_text": "Illustrative only. Not a verified figure.",
        "cost_per_hour_aud": 3500,   # illustrative simulated rate, labelled at the UI
        "simulated": True,
    }


def pick_pinned_conflict_id(conflicts):
    """
    Choose the conflict the pinned scenario attaches to. Prefer a berth_overlap
    (the cascade that implies tug, pilot and terminal), else the first conflict.
    Returns a conflict id or None. Pure read; never mutates conflicts.
    """
    if not conflicts:
        return None
    for c in conflicts:
        if c.get("conflict_type") == "berth_overlap":
            return c.get("id")
    return conflicts[0].get("id")


def summary_block(active_port_id, conflicts):
    """
    Build the top level `beta11` block for /api/summary. Returns the scenario
    descriptor plus the current active decision (if any) for the pinned
    conflict. Caller only invokes this when BETA11_ENABLED is true.
    """
    is_scenario_port = (active_port_id == SCENARIO_PORT)
    pinned_id = pick_pinned_conflict_id(conflicts) if is_scenario_port else None
    active = None
    if pinned_id:
        try:
            active = get_active_for_conflict(pinned_id)
        except Exception:
            active = None
    return {
        "enabled": True,
        "simulated": True,
        "scenario_port": SCENARIO_PORT,
        "is_scenario_port": is_scenario_port,
        "pinned_conflict_id": pinned_id,
        "active_decision": active,
        "roles": [
            {"id": r, "label": ROLE_META[r]["label"],
             "tone": ROLE_META[r]["tone"], "blurb": ROLE_META[r]["blurb"]}
            for r in ALL_ROLES
        ],
        "confirmer_roles": list(CONFIRMER_ROLES),
        "predicted_impact": pinned_predicted_impact(),
    }


def issue_pinned(conflict_id, issued_by):
    """Issue the pinned Melbourne scenario decision for a conflict (VTSO only)."""
    return issue_decision(
        conflict_id=conflict_id,
        issued_by=issued_by,
        required_stakeholders=pinned_stakeholders(),
        predicted_impact=pinned_predicted_impact(),
        actor_role=ROLE_VTSO,
    )
