# Lens Contract — Changelog

## Purpose

This file is the append-only record of every amendment to
[`LENS-CONTRACT.md`](LENS-CONTRACT.md), Horizon's binding stakeholder lens
governance document.

The changelog exists to prevent silent doctrine drift. Every diff to the
contract — including typographical edits — gets a dated entry here. The
contract's `version:` and `effective_date:` header fields are bumped in the
same change as the changelog entry that records the bump.

## Amendment process

1. **Propose.** Open the proposed amendment as a written change to
   `LENS-CONTRACT.md`. State what is being changed and why.
2. **Classify.**
   - Editorial / typographical / clarifying: `x.y → x.(y+1)` (patch bump).
   - Additive (new criterion, new section, new lens template field that
     does not invalidate existing lenses): `x.y → x.(y+1)` (minor bump).
   - Breaking (criterion removed, criterion meaning narrowed, lens previously
     compliant now non-compliant): `x.y → (x+1).0` (major bump).
3. **Approve.** Amendments must be authored or approved by the contract owner
   (currently Tony Trajceski).
4. **Record.** Append a new entry to this file using the template below.
   Update the `version:` and `effective_date:` fields in `LENS-CONTRACT.md` in
   the same commit.

### Entry template

```markdown
## Version X.Y — YYYY-MM-DD

**Type:** editorial | additive | breaking

**Summary:** one-line description.

**Changes:**
- Section §N: <what changed and why>
- Criterion LC-N: <what changed and why>

**Author:** <name>
**Approved by:** <contract owner>
**Notes:** <optional — references to discussions, signoffs, or prior versions>
```

---

## Version 1.1 — 2026-06-01

**Type:** additive (migration; no breaking change to contract criteria).

**Summary:** B1 (`loa > 200` towage_required gate) migrated from a
hard-coded numeric threshold to a port-profile-sourced representative
towage-demand model. Production code no longer carries the `loa > 200`
literal in any towage-visibility path. B1 is promoted to the
Approved Allowlist as a structurally-compliant threshold-defensibility
case; per-port placeholder values remain operator-review-required.

**Changes:**

- **Application code (`port_profiles.py`, `mst_scraper.py`, `server.py`):**
  - `port_profiles.py` — added `towage_rule` block to each of the four
    port profiles (BRISBANE, MELBOURNE, DARWIN, GEELONG) carrying
    representative LOA bands + vessel-type floors + `loa_epsilon_m`
    source-precision tolerance + `ui_label: "estimated tug demand"` +
    `ui_disclaimer` text. All values marked
    `status: "PLACEHOLDER — operator-review-required"`.
  - `mst_scraper.py` — added `_n_tugs_for(rule, vessel)` representative
    towage-demand helper (R4.2). Wired into `build_horizon_vessels` AIS
    path (R4.3) and synthetic-inbound block (R4.4). Removed
    `towage_required` field from `_sim_vessel_props` output (no longer
    authoritative — port context required).
  - `server.py:make_vessels` — wired `_n_tugs_for` (R4.6). Replaces
    `loa > _PORT_PROFILE.get("compulsory_towage_loa_m", 170)`.
  - `server.py:build_vessels_from_qships` — wired `_n_tugs_for` (R4.6).
    Replaces the `loa_val > 100 or vtype in (...)` setdefault block.
    Preserves any pre-existing `n_tugs` on the input vessel.
  - `server.py:make_towage` — consumer logic now reads `v["n_tugs"]`
    directly (R4.5 wiring; R4.6 legacy fallback removed). Booking-shape
    clamp to 1 retained as defensive guard.

- **Governance (`APPROVED-THRESHOLDS.md`):**
  - B1 promoted from "Pending review" to "Allowlist" with status
    "APPROVED — structural model only". Per-port placeholder values
    remain operator-review-required.
  - LC-13 boundary fixtures recognised for B1:
    `tests/lens_contract/thresholds/B1_pre_threshold.json`,
    `B1_at_threshold.json`, `B1_post_threshold.json`.
  - V1 target documented: replace placeholders with HMD-derived
    matrices (Melbourne §3.21 / Brisbane MSQ §8 / Geelong §4.6 /
    Darwin NT RHM directions + INPEX Bladin Point), per the
    Australian Port Towage Requirements research.

- **Test harness (`tests/lens_contract/threshold_scan.test.js`):**
  - B1 added to the ALLOWLIST (no remaining hard-coded site — flagged
    as MIGRATED). Stale hardcoded-line exception for `base_fuel` rewritten
    as a content-pattern match so future line shifts do not flip the
    classification (R4.5 harness fix).

**Doctrine consequences:**

- LC-12: the `loa > 200` canary case (BYD ZHENGZHOU class) is closed.
  No production code path uses the literal. References in comments
  remain INFORMATIONAL.
- LC-13: 1 of 2 approved thresholds (B1) has full boundary-fixture
  coverage. A1 (`IN_PORT_SOG_KTS`) fixtures remain pending and are
  unaffected by this version.
- Beta 12 lens-contract criteria LC-1 through LC-11 unaffected — all
  remain at 10/10 pass.

**Author:** Tony Trajceski (with implementation by the Horizon Beta
12 remediation session, R4.1 through R4.6, 2026-06-01).

**Approved by:** Tony Trajceski.

**Notes:** The structural towage-rule model is approved; the per-port
numeric values are placeholders and must be operator-ratified before
V1. UI wording in Beta 12 must continue to use "estimated tug demand"
language. Any future change that promotes these values to operational
authority requires a separate changelog entry and a new operator-review
date.

---

## Version 1.0 — 2026-06-01

**Type:** initial ratification

**Summary:** Stakeholder Lens Contract ratified from the Pilotage / Towage
governance review.

**Changes:**

- §1 Preamble — purpose, scope, mandatory reading, owner authority.
- §2 Common A / B / C / D structure — section definitions, derivation rules,
  empty-state behaviour.
- §3 Pilotage Lens Contract — Sections A, B, C, D defined; 8-hour Section B
  window ratified.
- §4 Towage Lens Contract — Sections A, B, C, D defined; 12-hour Section B
  window ratified.
- §5 Threshold-Defensibility Doctrine — `loa > 200` documented as the canary
  non-compliant case; per-port, multi-dimensional, tiered rule pattern
  established; `docs/governance/APPROVED-THRESHOLDS.md` introduced as the
  allowlist artefact.
- §6 Acceptance Criteria — LC-1 through LC-14 defined and made binding. LC-14
  marked blocking-for-demo, non-blocking-for-preview.
- §7 New stakeholder lens template — Lens Proposal Template and lifecycle
  gates established.
- §8 Amendment process — points to this changelog.

**Author:** Tony Trajceski (with implementation drafting by the Horizon Beta
12 governance session, 2026-06-01).
**Approved by:** Tony Trajceski.
**Notes:** Ratified following the BYD ZHENGZHOU regression review, in which
a vessel demonstrably present in `vessels[]` for Melbourne did not appear in
the Beta 12 Pilotage or Towage stakeholder lenses. The root cause was the
silent collapse of Section A onto Section B and the hidden
`loa > 200` visibility gate in `mst_scraper.build_horizon_vessels`. The
contract exists to prevent this class of regression from recurring.
