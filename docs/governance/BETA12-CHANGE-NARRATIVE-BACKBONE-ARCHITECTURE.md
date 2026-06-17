# Beta 12 — Durable Change-Narrative Backbone (Slice 2 Architecture)

> **Status:** Architecture plan — design only. No code, no deploy, no Beta 10
> changes, no Slice 2 implementation. Prerequisite design for propagating the
> Rationale / `change_narrative` to decision cards, visitor cards, Towage/VTS
> and notifications.
>
> **Scope of this document:** the durable server-side change-narrative backbone
> ONLY. It does not authorise broader stakeholder rollout.
>
> **Predecessors:** Slice 1 (canonical Rationale object on the readiness signal)
> and Slice 1.5 (client-side, in-memory `change_narrative` transition for the
> Pilotage/Readiness signal). This plan makes that transition durable and
> server-authoritative.

---

## Two anchoring realities

1. **The audit backbone already exists and is ideal for this.** `audit.py` is a
   hash-chained, append-only, per-tenant ledger (`audit.events` +
   `audit.payloads`) with a closed `EVENT_TYPES` set, `emit_async` (best-effort,
   `DATABASE_URL`-gated), `source_payload_refs` for provenance linkage, and
   `verify_chain` for tamper-evident replay. Critically, it **already separates
   `ts_event` (causal) from `ts_recorded` (detection)**. The five domain
   emitters (`conflict_audit.py`, `recommendation_audit.py`, …) are the exact
   pattern a readiness emitter follows.
2. **The canonical worst-wins composite is computed client-side** (the PRS engine
   in `index.html`); the server has no composite today. This single fact is the
   crux of the "do not build a second engine" constraint.

## Framing decision (load-bearing)

A durable *server-side* transition log requires the server to know each vessel's
canonical composite at each poll. Two honest reconciliations with "no second
engine":

- **Option A — consolidate the engine onto the server** (port PRS-PURE to Python
  as the single source of truth; the client becomes a pure renderer).
- **Option B — client stays the engine, server is only a durable log** (client
  reports its computed composite each poll; server persists + detects).

**Recommendation: Option A.** The downstream consumers this backbone must later
feed — visitor cards, notifications, audit replay — are all **server-generated**
and will need the composite server-side regardless. Option B would force a server
composite later anyway (eventually two engines). Option A makes the server the
**single** engine now and the client a renderer, which is the only configuration
that truly satisfies the constraint. The cost (porting + a cross-language parity
guard) is sequenced and de-risked in §6.

---

## 1. Proposed server-side data model

Built **on the existing `audit` backbone** — no new persistence layer.

- **Append-only history (`audit.events`, unchanged schema):** extend the closed
  sets — `EVENT_TYPES += {READINESS_OBSERVED, READINESS_TRANSITION,
  READINESS_RESOLVED}`; `SUBJECT_TYPES += {readiness_signal}` with
  `subject_id = "<port_id>:<vessel_id>"`. These ride the existing hash chain
  (`prev_hash`/`row_hash`), the per-tenant advisory-lock serialisation, and
  `verify_chain`. (The DB `CHECK` constraints in migration 0004 must be amended
  to match the extended sets — defence-in-depth.)
- **`READINESS_TRANSITION` payload (the durable change_narrative, hash-chained):**
  ```json
  {
    "vessel_id": "...", "port_id": "...",
    "from": { "state": "...", "trust_gate": "...", "driver_component": "...", "driver_provenance": "..." },
    "to":   { "state": "...", "trust_gate": "...", "driver_component": "...", "driver_provenance": "..." },
    "what_changed": "Trust moved from Strong to Watch",
    "why": "the berth clearance driver changed from live to schedule-based",
    "evidence_delta": [
      { "label": "expected berth clearance", "from": "14:10", "to": "15:02",
        "provenance_from": "live", "provenance_to": "schedule-based" }
    ],
    "if_no_action": "If conditions persist, the berth may not clear before this arrival.",
    "causal_time": "...", "causal_time_known": false,
    "detection_time": "..."
  }
  ```
  `source_payload_refs` links the event to the `audit.payloads` row(s) for the
  inputs that moved (e.g. the AIS/schedule payload whose ETD changed) — the
  provenance chain for "which evidence changed."
- **Mutable projection table `readiness_state`** (current composite per
  `(tenant, port, vessel)`): the "previous snapshot" the detector diffs against.
  It is a **derived projection, rebuildable from the event log** — not a second
  source of truth. Living in Postgres (not browser memory) is what makes history
  survive reloads, sessions and watch handovers.
- **`DATABASE_URL` unset →** no tables; the system **degrades to the Slice 1.5
  in-memory client diff** (non-durable but functional). Preserves the
  "works without a DB / no Beta 10 dependency" property.

## 2. Event / transition lifecycle

Runs at the **existing emit point** in the `/api/summary` handler (where
`conflict_audit.emit_async` already fires), via a new `readiness_audit.py`
emitter mirroring `conflict_audit.py`:

1. Server engine computes the canonical worst-wins composite per vessel (Option A).
2. Load `readiness_state` (prior composite) for each vessel.
3. **Detect** (one server-side implementation — replaces the client Slice 1.5
   diff): compare `{state, trust_gate, driver, driver_provenance,
   evidence-time @ minute granularity}`.
4. On a material change → `emit_async(READINESS_TRANSITION)` (hash-chained, with
   `from/to`, the time model, `source_payload_refs`); update the
   `readiness_state` projection. Return to READY → `READINESS_RESOLVED`.
5. **Idempotency** carries over from Slice 1.5, now keyed on `generated_at` plus
   the projection: a re-poll with no input change writes nothing.
6. `/api/summary` returns the **current** `change_narrative` per vessel (latest
   transition, or `present:false` on first observation) for the client to render.
7. Best-effort throughout: a DB failure never blocks `/api/summary` (matches the
   existing audit discipline).

## 3. API changes required (all additive)

- `GET /api/summary` — each readiness signal gains a server-computed
  `change_narrative` (replacing the client-derived one when DB-on); under
  Option A it also gains the server-computed `readiness` composite per vessel.
- `GET /api/readiness-history?port=&vessel=` — the immutable transition timeline
  for resident drill-down and regulator/assurance replay (reads `audit.events`,
  runs `verify_chain`).
- No new auth surface; reuse session/tenant. No new write endpoint (the server
  detects from data it already assembles — the client does NOT post state,
  avoiding a trust/round-trip problem).

## 4. How to avoid a second engine

- **One engine, one language:** the worst-wins composite is computed **only** on
  the server (Python). The client renders the server's composite; the JS PRS
  assessment logic is retired (kept only as the DB-off fallback path during
  migration, then removed behind the flag).
- **A cross-language parity harness is the guardrail:** mirror the existing
  `tests/lens_contract/` fixtures in Python so the server engine is verified
  against the *same* spec and fixtures the JS engine was. The JS→server composite
  cutover happens only when parity is byte-equivalent. This is the explicit
  mechanism that prevents drift.
- **The transition detector is not an engine** — it observes composite changes
  and never recomputes or alters the composite. Worst-wins remains canonical and
  authoritative; the log is downstream of it.

### Detection time vs causal time (honest model)

Reuse the backbone's existing `ts_event` / `ts_recorded` split:

- `detection_time = ts_recorded` — when Horizon observed the change.
- `causal_time = ts_event` — set to the **source payload's own timestamp** when
  the moving input carries one (e.g. the schedule feed's ETD revision time) with
  `causal_time_known: true`; otherwise `causal_time = generated_at` with
  `causal_time_known: false`, and the rendered narrative says *"detected at
  HH:MM"* — never *"changed at HH:MM"*. No invented causal precision.

## 5. Resident vs visitor rendering (one object, four projections)

The same `change_narrative` object feeds all consumers; only the projection differs:

- **Resident lenses (VTSO / Pilotage / Towage):** full narrative
  (what / when / why / which-evidence / if-conditions-persist) **plus** a
  transition-history drill-down (per-vessel timeline from
  `/api/readiness-history`). In-world, interactive.
- **Visitor cards (terminal / agent / line / shipping line):** the **latest
  transition only**, one scoped line ("Readiness moved to AT RISK at 13:42 —
  berth clearance slipped to schedule-based"), read-only, payload-ceiling
  enforced, link back to Horizon for detail (Platform Gravity). No history
  drill-down.
- **Notifications:** a **transition is the natural trigger** — fire only on
  *material* transitions (into AT RISK / NOT READY, or a trust-gate degrade),
  never per poll; payload = the one-line headline + link. The event log is the
  notification source of truth (no separate notify pipeline).
- **Audit replay (regulator / assurance):** the hash-chained
  `READINESS_TRANSITION` sequence *is* the immutable "what was knowable and when"
  record, with the time model and `source_payload_refs`; `verify_chain` proves
  integrity.

All projections preserve advisory-only language, the conditional consequence,
provenance/trust gates, and worst-wins canonicality.

## 6. Migration path from client-side Slice 1.5

Phased so there is **never a live divergence** and Beta 10 is untouched:

- **2a — Engine consolidation:** port PRS-PURE to Python behind the flag; emit the
  server composite in `/api/summary` *alongside* the client's; the parity harness
  must be green. Client still renders its own composite (no behaviour change yet).
- **2b — Transition backbone:** add the event types + `readiness_state` projection
  + `readiness_audit.py`; begin emitting `READINESS_TRANSITION` server-side
  (DB-on). No UI change.
- **2c — Narrative cutover:** `/api/summary` returns the server `change_narrative`;
  the client renders it when present and **falls back to the Slice 1.5 in-memory
  diff when `DATABASE_URL` is unset**. Slice 1.5's `_b12ChangeNarrative` thus
  becomes the documented DB-off fallback, not dead code.
- **2d — Composite cutover + retire JS engine:** once parity holds, the client
  renders the server composite and the JS assessment logic is removed behind the
  flag. THEN propagate to visitor cards / notifications / audit replay.

Beta 10 (`main`, flag-off) is unaffected at every phase: all of this is
`BETA11_ENABLED` / `d.beta11`-gated and `DATABASE_URL`-gated.

## 7. Test / acceptance plan

- **Transition detector (Python pure-function tests):** first observation → no
  transition; each of state/trust/driver/provenance/evidence-time change →
  correct `from/to`; unchanged → no event; idempotent within a poll.
- **Hash-chain integrity:** `READINESS_TRANSITION` rows chain correctly;
  `verify_chain` green; tamper of payload/hash detected (mirror
  `test_conflict_audit.py` / `test_recommendation_audit.py`).
- **Durability:** simulate reload / new session / watch handover → narrative read
  from DB survives.
- **Time model:** causal-known vs causal-unknown render correctly ("changed at"
  vs "detected at").
- **Parity (the anti-second-engine gate):** server composite == JS composite
  across the full `lens_contract` fixture set — release-blocking.
- **DB-off fallback:** `DATABASE_URL` unset → Slice 1.5 client diff path; flag-off
  → Beta 10 byte-identical.
- **Advisory lint:** emitted narratives contain no command verbs; consequence is
  conditional; SIM-driven transitions carry SIM provenance (Slice 0 gate holds
  into the log).

## 8. Risks & open questions

- **Engine port is the dominant risk.** The parity harness is mandatory and
  release-blocking; without it, two engines drift. *Open Q:* port now
  (recommended — visitors/notifications need it) vs. defer with Option B
  (lighter, but defers the inevitable).
- **Is the audit schema provisioned where Beta 12 runs?** The backbone needs
  Postgres with migration 0004's `audit.*` schema. The Beta 12 preview env had
  `DATABASE_URL` set; production Beta 10 did not. Confirm before 2b.
- **Causal time is usually unknown** (feeds rarely stamp the true event time) →
  keep the "detected at" framing; do not oversell causal precision to the
  regulator surface (the highest-liability one).
- **Log volume / cardinality:** log transitions only (not `READINESS_OBSERVED`
  every poll) + retention class on transition payloads; the projection table
  caps diff cost.
- **Subject identity across ports/tenants:** key by `tenant:port:vessel` to avoid
  collision; ensure `ts_event` is a stable ISO string for chain determinism.
- **"What changed since *you* last looked" (per-operator handover)** is a
  different, harder feature than per-vessel durable history — explicitly out of
  scope; flagged for later.
- **SIM provenance:** transitions driven by simulated inbounds must record SIM
  provenance so the immutable log never asserts a sim-driven transition as live
  (carries the Slice 0 gate into the audit record).

---

*Authored as the Slice 2 prerequisite design. Implementation is not authorised by
this document.*
