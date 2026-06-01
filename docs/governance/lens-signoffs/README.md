# Lens Read-Aloud Sign-Offs (VAL-X1)

This directory holds completed stakeholder read-aloud sign-offs — the manual
value gate VAL-X1 from `tests/lens_contract/value/` and LC-14 in
[`../LENS-CONTRACT.md`](../LENS-CONTRACT.md) §6.5.

## How to use

1. Copy [`TEMPLATE.md`](TEMPLATE.md) to `YYYY-MM-DD-<role>-<PORT>.md`
   (e.g. `2026-06-15-pilot-MELBOURNE.md`,
   `2026-06-15-tugmaster-MELBOURNE.md`).
2. Run the 30-second cold read-aloud with a real pilot / tug master.
3. Record verdict, comments, and recommended changes.
4. Commit the completed sign-off (a sign-off is a governance record).

## Status

| Lens | Port | Stakeholder | Sign-off file | Status |
|---|---|---|---|---|
| Pilotage | Melbourne | Pilot | _pending_ | NOT YET CONDUCTED |
| Towage | Melbourne | Tug Master | _pending_ | NOT YET CONDUCTED |

Sign-offs are **blocking for production / demo-ready status** and
**non-blocking for preview**. The lenses may run on the Beta 12 preview with
VAL-X1 marked pending; they may not be described as demo-ready or
production-ready until the relevant sign-offs PASS.

## Relationship to automated gates

The automated VAL-P1..VAL-T5 checks (in
`tests/lens_contract/value/value_acceptance.test.js`) verify that the lens
*structurally carries* the operational markers (lead, verdict, shape,
pressure, clash, demand curve). VAL-X1 verifies that a domain practitioner can
*actually use* them to answer the four operational questions. Passing the
automated checks is necessary but not sufficient; VAL-X1 is the real test.
