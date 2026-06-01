---
version: 1.1
effective_date: 2026-06-01
owner: Tony Trajceski
status: ratified
companion: LENS-CONTRACT.md §5 (Threshold-Defensibility Doctrine)
companion: LENS-CONTRACT.md §6.4 (LC-12 / LC-13)
amends: 1.0 — B1 promoted from Pending to Allowlist; see LENS-CONTRACT-CHANGELOG.md v1.1.
---

# Approved Numeric Thresholds

Per [`LENS-CONTRACT.md`](LENS-CONTRACT.md) §5, no hard-coded numeric
threshold may gate stakeholder visibility unless it is:

1. **Sourced from a port profile** (`port_profile.<key>`) and operator-reviewed,
   OR
2. **Explicitly listed in this allowlist** with file:line, value, source,
   justification, operator-review date, and boundary-test fixture
   references.

Anything else is a **governance failure** under LC-12. The
`threshold_scan.test.js` harness enforces this list as the canonical
allowlist.

This file is intentionally narrow at version 1.0. Adding a threshold to
this allowlist is a governance action, not a routine edit; it requires
the contract owner's approval and a changelog entry in
[`LENS-CONTRACT-CHANGELOG.md`](LENS-CONTRACT-CHANGELOG.md).

---

## Allowlist

### A1 — `IN_PORT_SOG_KTS = 1.0`

| Field | Value |
|---|---|
| File | `aisstream_scraper.py` |
| Line | 44 (constant) — referenced at line 153 (`sog < IN_PORT_SOG_KTS`) |
| Value | `1.0` knots |
| Source | Named module constant, AIS-domain physics |
| Purpose | Classify a vessel as stationary (at berth / anchored) vs in motion. Used as a precondition for entering `_in_port[<unloco>]`. |
| Justification | This threshold is not port-specific. It encodes the AIS-domain definition of "stationary" (Class A AIS transmits SOG with 0.1-knot resolution; a vessel reading < 1.0 kt with a stable position is unambiguously stationary). The constant is named, scoped to `aisstream_scraper`, and not consumed by any stakeholder lens directly. It gates classification, not visibility. |
| Operator-review date | 2026-06-01 (initial ratification) |
| Boundary-test fixture | NOT YET CREATED — see LC-13 status below |
| Status | ✅ APPROVED — pending boundary-test fixtures (LC-13) |

### B1 — Representative towage-demand model (formerly `loa > 200`) — **MIGRATED**

| Field | Value |
|---|---|
| Previous location | `mst_scraper.py:179` (sim) and `:263` (AIS) — `"towage_required": loa > 200` |
| Current location | NO hard-coded threshold remains. Towage demand is computed by `mst_scraper._n_tugs_for(rule, vessel)` from `port_profile.towage_rule` data (`port_profiles.py`). Producers: `mst_scraper.build_horizon_vessels` (AIS + synthetic-inbound paths), `server.py:make_vessels` (simulation), `server.py:build_vessels_from_qships` (live QShips patch). Consumer: `server.py:make_towage`. |
| Pattern | (none — migrated to data) |
| Gates | Vessel inclusion in `towage[]` server-side via `towage_required = n_tugs >= 1`, which is derived from the port-configurable, multi-dimensional, tiered `towage_rule`. |
| Doctrine compliance | LC-12 §5.3 properties: (1) per-port — ✓ each port profile carries its own `towage_rule`; (2) multi-dimensional — ✓ LOA bands + vessel-type floors + `loa_epsilon_m` source-precision tolerance + optional name overrides; (3) tiered — ✓ `n_tugs` output is integer 0…N, not a boolean. |
| Operator-review status | Per-port v1 band onsets and floors are **representative-demand placeholders** marked `status: "PLACEHOLDER — operator-review-required"` in `port_profiles.py`. The structural model is approved; the numeric values await per-port operator ratification. UI wording (`ui_label: "estimated tug demand"`) explicitly disclaims authority. |
| Boundary-test fixtures | `tests/lens_contract/thresholds/B1_pre_threshold.json` (199.9 m), `B1_at_threshold.json` (200.0 m), `B1_post_threshold.json` (200.1 m). All three exercise the Melbourne `towage_rule` with a Car-carrier vessel type and assert `n_tugs == 2`. Verified by `threshold_scan.test.js` boundary-fixture inventory block. |
| Canary closure | BYD ZHENGZHOU (LOA ≈ 199.9 m, Car carrier) was the canary class. Under R4.3 wiring she lands in the 170–250 m band → `n_tugs=2`; the Car-carrier vessel-type floor of 2 also yields 2; `towage_required=true`. She is now addressable via `mst_scraper.build_horizon_vessels` and surfaces in Towage Section A (vessels at berth requiring outbound towage), Section B (jobs in 12 h shift if etd is in window), and Section C (jobs 12–24 h if etd is in that range). |
| Operator-review date | 2026-06-01 (structural ratification; per-port values pending) |
| V1 target | Replace placeholder values in `port_profiles.py:<port>.towage_rule` with HMD-derived matrices: Melbourne HMD §3.21 berth-specific table + beam tiers (Post Panamax / Bosphorus Max) + Bolte Bridge air-draught modifier; Brisbane MSQ §8 per-berth matrix; Geelong HMD §4.6 + wind escalation; Darwin NT RHM directions + tidal/cyclone modifiers + INPEX Bladin Point terminal rules. See `LENS-CONTRACT.md` §5.3 and the Australian Port Towage Requirements research. |
| Status | ✅ **APPROVED** — structural model only. Per-port placeholder values are tracked separately in `port_profiles.py` and remain operator-review-required for V1. |

---

## Pending review (NOT approved)

The following hard-coded numeric thresholds have been detected by the
`threshold_scan.test.js` harness and are **not** on the allowlist. Each
is treated as a governance failure under LC-12. None may be silently
allowlisted; each must be either:

(a) Moved to `port_profile.<key>` and removed from source, OR
(b) Justified, operator-reviewed, and explicitly added to the
allowlist above with a changelog entry.

### B1 — (resolved by migration at v1.1 — see Allowlist entry above)

### B2 — `hrs_to_eta < 0 or hrs_to_eta > 48` (arrival_ukc inclusion gate)

| Field | Value |
|---|---|
| File | `server.py:1901` |
| Pattern | `if hrs_to_eta < 0 or hrs_to_eta > 48:` |
| Gates | UKC entry inclusion in `arrival_ukc.all[]` → both lenses' UKC posture |
| Failure mode | LC-12: hard-coded 48 h window. Universal, not port-configurable. LC-13: no boundary fixtures at 47.9 / 48.0 / 48.1. |
| Status | ❌ UNAPPROVED — requires port-profile review |

### B3 — Weather classification thresholds

| Field | Value |
|---|---|
| File | `server.py:2105-2109` |
| Pattern | `if vis < 1 or swell > 2.5 or wind > 30:` (Poor) / `vis < 3 or swell > 1.5 or wind > 20` (Moderate) / `vis < 5 or swell > 1.0 or wind > 15` (Good) |
| Gates | `conditions` field (Excellent/Good/Moderate/Poor) which downstream gates weather threshold-breach exceptions in both lenses (Section D) |
| Failure mode | LC-12: hard-coded universal thresholds; do not vary by port despite real operational practice doing so (Geelong / Darwin have different wind / swell tolerances). LC-13: no boundary fixtures. |
| Status | ❌ UNAPPROVED — requires per-port operator review. CLAUDE.md already notes these are "universal across all ports (not port-specific)". |

### B4 — Swell categorisation thresholds

| Field | Value |
|---|---|
| File | `server.py:1728` (`eff_swell >= 2.0`) and `server.py:1742` (`eff_swell >= 1.5`) |
| Gates | Swell-severity classification used in conflict severity calculations and decision support |
| Failure mode | LC-12: same shape as B3 |
| Status | ❌ UNAPPROVED |

### B5 — Pilotage watch window (8 hours)

| Field | Value |
|---|---|
| File | `index.html:4267` |
| Pattern | `const winEnd = now + 7*3600*1000;` (Section B end at now + 7h, with winStart at now − 1h → 8h watch) |
| Gates | Section B inclusion in Pilotage Watch Overview |
| Failure mode | LC-12: hard-coded operational window. While `LENS-CONTRACT.md` §3.2 ratifies an 8-hour Section B window, the magic number is embedded in renderer code rather than referenced from a named operational constant. LC-13: no boundary fixtures at 7.9 h / 8.0 h / 8.1 h. |
| Status | ❌ UNAPPROVED — needs to be promoted to a named constant (e.g., `LENS_PILOTAGE_WATCH_HOURS`) and boundary-tested. |

### B6 — Towage shift window (12 hours)

| Field | Value |
|---|---|
| File | `index.html:3923` |
| Pattern | `const winEnd = now + 11*3600*1000;` (Section B end at now + 11h, 12h shift) |
| Gates | Section B inclusion in Towage My Shift |
| Failure mode | Same as B5, scaled to 12 h |
| Status | ❌ UNAPPROVED |

### B7 — UKC danger banding

| Field | Value |
|---|---|
| Files | `index.html:2275, 3087, 4329` |
| Pattern | `min_ukc_m < 0.5` (critical), `< 1.0` (warning), `ukc.ukc_m < 0` (unsafe) |
| Gates | UKC status colour banding and unsafe flag inclusion in lens Section B confidence pills |
| Failure mode | LC-12: hard-coded universal UKC bands; real port DUKC matrices vary. LC-13: no boundary fixtures. |
| Status | ❌ UNAPPROVED |

### B8 — LOA-tiered fuel estimate (informational, not visibility-gating)

| Field | Value |
|---|---|
| File | `server.py:2219` |
| Pattern | `base_fuel = 3.5 if loa > 250 else 2.5 if loa > 180 else 1.5 if loa > 120 else 0.8` |
| Gates | Fuel-cost estimate display (Beta 10 dashboard). Does NOT gate stakeholder lens visibility. |
| Status | ℹ️ INFORMATIONAL — flagged for review but outside LC-12 scope (does not gate stakeholder visibility). |

---

## LC-13 boundary-fixture status

LC-13 requires that every approved threshold has fixtures at
`value − ε`, `value`, and `value + ε` with documented expected outcomes,
stored under `tests/lens_contract/thresholds/`.

| Threshold | Boundary fixtures created? |
|---|---|
| A1 — `IN_PORT_SOG_KTS = 1.0` | ❌ Not yet |
| **B1 — Representative towage-demand model** | **✅ `B1_pre_threshold.json`, `B1_at_threshold.json`, `B1_post_threshold.json`** under `tests/lens_contract/thresholds/`. All three validate (Melbourne `towage_rule`, Car-carrier @ 199.9 / 200.0 / 200.1 m → `n_tugs=2`). |
| All B2–B7 | ❌ Not yet (R5 / future remediation) |

LC-13 status at v1.1: 1 of 2 approved thresholds (B1) has full
boundary-fixture coverage; A1 fixtures remain pending. B2–B7 are
unrelated thresholds outside the R4 migration scope (see R5).

---

## Adding a threshold to this allowlist

1. Write the proposed addition (file:line, value, source, purpose,
   justification, operator-review date).
2. Create boundary-test fixtures under
   `tests/lens_contract/thresholds/<name>.json` covering ε-below,
   exact, ε-above.
3. Add a changelog entry to
   [`LENS-CONTRACT-CHANGELOG.md`](LENS-CONTRACT-CHANGELOG.md) bumping
   the contract version (additive — minor bump).
4. Submit to the contract owner for approval.
5. Move the entry from "Pending review" to "Allowlist" only after
   approval.

Silent edits to this file are prohibited. Every diff requires an
accompanying changelog entry.
