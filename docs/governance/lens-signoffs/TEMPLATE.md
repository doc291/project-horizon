# Stakeholder Lens Read-Aloud Sign-Off (VAL-X1)

> Copy this file to `YYYY-MM-DD-<role>-<PORT>.md` (e.g.
> `2026-06-15-pilot-MELBOURNE.md`) and complete one per lens, per port,
> per stakeholder. This is the **ultimate** value gate — the automated
> VAL-P/VAL-T checks are proxies for it. A lens that passes every automated
> check but fails this read-aloud has not earned its place.

VAL-X1 implements `LENS-CONTRACT.md` §6.5 (LC-14) and the value-harness
manual gate documented in `tests/lens_contract/value/README.md`.

---

## Session details

| Field | Value |
|---|---|
| Stakeholder name | |
| Role | Pilot / Tug Master |
| Port | BRISBANE / MELBOURNE / GEELONG / DARWIN |
| Date | YYYY-MM-DD |
| Lens tested | Pilotage / Towage |
| Beta 12 version / commit | `<git short SHA>` |
| Reviewer (facilitator) | |
| Environment | preview URL / local |

## Method

The stakeholder is shown the lens **cold** (no preliminary explanation) and
given **30 seconds** to read it. The facilitator then asks the four questions
below verbatim. Each answer must be **sourceable from the lens alone** — the
stakeholder points to the element that gave them the answer. Record the element
they used, or "could not answer".

Do not coach. Do not explain markers. If the stakeholder cannot find an answer,
that is a FAIL for that question and a defect to record.

---

## The four questions

### 1. What is happening right now?
- Stakeholder's answer:
- Element(s) they used (e.g. shift-shape headline, watch overview, lead block):
- Answerable from lens alone?  YES / NO

### 2. What do I need to do?
- Stakeholder's answer:
- Element(s) they used (e.g. lead job/transit, verdict line, tug assignment):
- Answerable from lens alone?  YES / NO

### 3. What is coming next?
- Stakeholder's answer:
- Element(s) they used (e.g. Section C forward pressure / demand curve / forming list):
- Answerable from lens alone?  YES / NO

### 4. What just changed?
- Stakeholder's answer:
- Element(s) they used (e.g. Section D exceptions, capacity-clash warning):
- Answerable from lens alone?  YES / NO

---

## Verdict

| Item | Result |
|---|---|
| Q1 answerable | PASS / FAIL |
| Q2 answerable | PASS / FAIL |
| Q3 answerable | PASS / FAIL |
| Q4 answerable | PASS / FAIL |
| **VAL-X1 overall** | **PASS / FAIL** |

VAL-X1 PASSES only if all four questions are answerable from the lens alone.

## Comments

(Free text — what worked, what confused, what the stakeholder reached for the
VTSO dashboard to find instead.)

## Recommended changes

(Specific, actionable. Each becomes a candidate for a future LV phase. Do not
implement from this sign-off directly — route through the brainstorming →
plan → phased-implementation governance flow.)

## Honesty note

Record any place the lens stated something the stakeholder knew to be
simulated/assumed (tug status, pilot count, towage demand estimate, terminal
readiness). The provenance tags must have made that clear. If the stakeholder
mistook a simulated value for live/authoritative, record it as a FAIL-adjacent
trust risk even if the four questions passed.
