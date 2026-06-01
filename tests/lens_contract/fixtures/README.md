# Lens-Contract Fixtures

Fixtures consumed by the lens-contract acceptance harness
(`tests/lens_contract/*.test.js`).

## Inventory

| Prefix | Purpose | Ports captured |
|---|---|---|
| `wo_<PORT>.json` | Pilotage Watch Overview acceptance (used by `pilotage.test.js`) | BRISBANE, MELBOURNE, GEELONG, DARWIN |
| `wo_MELBOURNE_dec.json` | Pilotage Watch Overview with an active beta11 decision present (exercises Section D path) | MELBOURNE |
| `p567_<PORT>.json` | Earlier Pilotage P5-P7 acceptance (used by `legacy/pilotage_p567.test.js`) | BRISBANE, MELBOURNE, GEELONG, DARWIN |
| `p567_MELBOURNE_dec.json` | P5-P7 with active beta11 decision | MELBOURNE |
| `dc_<PORT>.json` | Data Confidence overlay AO1/AO2 acceptance (used by `data_confidence.test.js`) | BRISBANE, MELBOURNE, GEELONG, DARWIN |

Total: 14 fixture files at G3 migration.

## Source

Each fixture is a captured `/api/summary` response from the Horizon backend,
saved at a known point in time when the relevant test scenario was exercised:

- **wo_* and p567_*** captured from the development environment with
  `BETA11_ENABLED=1` set, against each port (`HORIZON_PORT=<port>`) plus a
  Melbourne variant in which a beta11 coordinated decision was active.
- **dc_*** captured similarly to exercise the four-port Data Confidence
  overlay paths (live vs simulated AIS, live vs sim weather, etc.).

Capture date: 2026-04 to 2026-05 (pre-Beta-12-preview era). The fixtures are
not time-correlated to each other; each was captured for its specific harness.

## Refresh process

Fixtures may be refreshed when:

1. The `/api/summary` schema changes in a way that invalidates the captured
   shape (a field is renamed, removed, or its type changes).
2. A new lens needs a fixture not currently captured (e.g., a Terminal lens
   proposal at G-future).
3. An operator review surfaces that a captured fixture no longer represents
   plausible operational state (e.g., the captured AIS_LIVE block reflects a
   stale upstream service).

To refresh:

```bash
# 1. Start the server with the relevant configuration.
BETA11_ENABLED=1 HORIZON_PORT=<PORT> python3 server.py

# 2. Authenticate and capture.
curl -c /tmp/cookies.txt -d "username=$USER&password=$PASS" -X POST http://localhost:8000/login
curl -b /tmp/cookies.txt http://localhost:8000/api/summary > tests/lens_contract/fixtures/<prefix>_<PORT>.json

# 3. Inspect the diff against the previous version.
git diff tests/lens_contract/fixtures/<prefix>_<PORT>.json
```

## Governance review requirement

**Replacing or refreshing a fixture is a governance action, not a routine
edit.** Before a fixture is replaced:

1. The reason for replacement MUST be documented in the commit message.
2. The diff MUST be inspected by the contract owner (currently
   Tony Trajceski) to confirm the new fixture represents a realistic
   operational state — not a degenerate, hand-crafted, or AIS-cold-start
   snapshot that would mask the regression a harness exists to catch.
3. The matching harness criteria MUST be re-run against the new fixture and
   the resulting changes in pass/fail counts MUST be reported in the commit.
4. If the refresh is in service of removing an operationally-realistic case
   that a current harness fails, that is a **doctrine drift** and requires
   an explicit approval recorded in
   [`docs/governance/LENS-CONTRACT-CHANGELOG.md`](../../../docs/governance/LENS-CONTRACT-CHANGELOG.md)
   rather than a quiet fixture swap.

The general principle: **fixtures are evidence, not convenience**. They
exist to anchor the harness to observable port operational reality. A test
that fails because the fixture is realistic is a test that has done its job.
A test that passes because the fixture has been quietly softened has been
defeated.

## File format

Each fixture is a JSON object matching the `/api/summary` response shape:

```jsonc
{
  "port_status":   { ... },
  "vessels":       [ ... ],
  "berths":        [ ... ],
  "conflicts":     [ ... ],
  "pilotage":      [ ... ],
  "towage":        [ ... ],
  "weather":       { ... },
  "tides":         { ... },
  "arrival_ukc":   { "all": [ ... ] },
  "dashboard":     { ... },
  "beta11":        { ... }
  // ...
}
```

The harness files document which top-level keys each one reads.

## Cross-references

- [`tests/lens_contract/README.md`](../README.md) — harness runner and stage gates.
- [`docs/governance/LENS-CONTRACT.md`](../../../docs/governance/LENS-CONTRACT.md) — binding contract; §6 defines the criteria these fixtures support.
- [`docs/governance/LENS-CONTRACT-CHANGELOG.md`](../../../docs/governance/LENS-CONTRACT-CHANGELOG.md) — amendment record; fixture-realism amendments are logged here.
