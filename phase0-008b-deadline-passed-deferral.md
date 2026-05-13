# Phase 0 Decision Note — DEADLINE_PASSED / Inaction Events Deferred to Phase 1.x

**Status:** Deferred — Phase 1.x
**Date logged:** 2026-05-13
**Scope:** Phase 0.8b (inaction events) and the closely-related event types `OPERATOR_DEFERRED`, `OPERATOR_OVERRODE`, and `SESSION_ENDED_WITHOUT_ACTION`
**Decision authority:** Product / Engineering Management
**ADR linkage:** ADR-002 §1.4 (operator decision events)

---

## Decision

The following event types are **not** to be emitted in Phase 0:

- `DEADLINE_PASSED`
- `SESSION_ENDED_WITHOUT_ACTION`
- `OPERATOR_DEFERRED`
- `OPERATOR_OVERRODE`

They are deferred to Phase 1.x.

The 25-entry `EVENT_TYPES` closed set in `audit.py` remains unchanged — the
event types are reserved at the schema level, ledger-writable when prerequisites
are met, but no emission path is implemented anywhere in Phase 0.

## Rationale

Beta 10 does not have the operational substrate that makes these events
meaningful. Concretely:

1. **No recommendation action surface.** Beta 10 has no UI control or endpoint
   that lets an operator accept, decline, defer, or override a specific
   recommendation. The four `OPERATOR_ACTED` endpoints instrumented in
   Phase 0.8a (`whatif_apply`, `whatif_clear`, `send_brief`) are scenario
   exploration and external email — none of them mark a recommendation as
   actioned. Consequence: every recommendation in Beta 10 is structurally
   "inactioned" by default. Emitting `DEADLINE_PASSED` against that
   background would record that operators failed to take an action they
   cannot take.

2. **No fixed, persisted decision_deadline.** The `decision_deadline` value
   on a conflict is recomputed on every `/api/summary` poll. For ongoing
   conflicts it is always `now + 4h` — a moving target that never elapses.
   For future conflicts it has a `max(..., now + 20min)` floor that
   prevents the deadline from sitting in the past at scan time. A reliable
   inaction signal requires the deadline pinned at recommendation-generation
   time and read back from persistent storage.

3. **No DB-backed idempotency in production.** Phase 0's production
   posture is `DATABASE_URL` unset. In-process dedup (the pattern used
   for `CONFLICT_DETECTED` and `RECOMMENDATION_GENERATED`) does not
   survive a Railway restart. For `DEADLINE_PASSED` specifically, a
   restart would re-emit indefinitely as the moving deadline crosses
   `now` again on each restart cycle.

4. **No tenant policy that separates demo from real operations.** The
   AMS demo tenant has hardcoded scenarios that recur indefinitely,
   no operator on shift, and a demo browser that may or may not be
   open. Emitting inaction events from the demo tenant would dilute
   the audit signal once Phase 1 customer tenants arrive.

5. **No operator-on-shift context.** Distinguishing "operator was on
   duty but did not act" from "demo browser was open in a meeting
   room" requires shift / roster awareness that Beta 10 does not have.

## Prerequisites for Phase 1.x reinstatement

All five must be in place before `DEADLINE_PASSED` (and its companions)
are emitted:

1. **Real recommendation action surface.** A first-class UI control
   (accept / decline / defer / override) wired to an endpoint that
   emits the corresponding `OPERATOR_ACTED` / `OPERATOR_DEFERRED` /
   `OPERATOR_OVERRODE` event with the recommendation's stable
   `recommendation_id` as subject.

2. **Pinned `decision_deadline` read from the persisted
   `RECOMMENDATION_GENERATED` event.** Phase 0.7b already captures the
   deadline at generation time inside the decision snapshot
   (`decision_snapshot.decision_deadline`). Phase 1.x scanners must
   read that value from the audit ledger rather than recomputing.

3. **DB-backed idempotency.** A UNIQUE constraint such as
   `(tenant_id, recommendation_id, event_type="DEADLINE_PASSED")` or
   a read-before-emit check against `audit.events`. Process-level
   dedup is insufficient given Railway restart semantics.

4. **Tenant-aware emission policy.** A per-tenant flag (e.g.
   `tenants.config.emit_inaction_events`) that excludes the AMS demo
   tenant by default. Inaction events should be enabled per-tenant
   only when there is a contracted operations surface to evaluate
   inaction against.

5. **Operator-on-shift context where applicable.** For
   `SESSION_ENDED_WITHOUT_ACTION` specifically, the event needs roster
   context — a session that ended outside a shift window is not the
   same evidentiary signal as one that ended during one. For
   `DEADLINE_PASSED`, a deadline that passed with no operator on duty
   should be tagged differently from one that passed with an operator
   logged in.

## Scope guard

The existing test `tests/test_operator_action_audit.py::TestScopeGuard`
already asserts at module-import time that `DEADLINE_PASSED`,
`OPERATOR_DEFERRED`, `OPERATOR_OVERRODE`, and
`SESSION_ENDED_WITHOUT_ACTION` are not emitted anywhere in server.py
or in any audit helper. This deferral does not require additional
test scaffolding — the negative assertion is already in CI.

## Not affected by this deferral

The audit data already captured by Phase 0.7b is sufficient
foundation for retrospective Phase 1.x inaction analysis:

- `RECOMMENDATION_GENERATED.decision_snapshot.decision_deadline` —
  the pinned deadline value the engine produced
- `RECOMMENDATION_GENERATED.decision_snapshot.recommended_option` —
  what the operator was being asked to consider
- `RECOMMENDATION_PRESENTED` — confirmation that the recommendation
  reached an operator surface
- `OPERATOR_ACTED` (0.8a) — explicit positive operator actions where
  they exist

A Phase 1.x scanner can replay these rows, identify recommendations
whose pinned deadline has passed with no `OPERATOR_ACTED` event keyed
to the same `recommendation_id`, and emit `DEADLINE_PASSED`
idempotently keyed off the audit ledger itself.

## Out of scope for this note

- Whether `RECOMMENDATION_OBSOLETED` (engine flipped to a different
  option for the same conflict) should fire in Phase 0. That is a
  Phase 0.7d question, not 0.8b — and is also better answered after
  multi-tenant policy exists.
- Whether `OUTCOME_RECORDED` (the actual operational outcome was
  observed after a recommendation was or was not actioned) should
  appear in Phase 0. That requires a feedback loop from real port
  systems and is firmly Phase 1+.
