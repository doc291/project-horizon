# horizon_platform/

Server-side platform module introduced by PF-M1 (Horizon V1
Platform Foundation Milestone 1: authentication, RBAC, identity,
port-scoped access).

This package is **server-side only**. It has no I/O, no
persistence, no HTTP, no UI awareness in the scaffold slice (this
slice). Later PF-M1 slices add login / session handlers, request
middleware, and the admin surface — each in its own slice under
explicit Tony authorisation.

## Path convention

Named `horizon_platform/` (not `platform/`) because `platform` is
a Python stdlib module and a top-level package named `platform/`
would shadow it. This matches the PF-M1 Implementation Plan §4.1
fallback path.

## Slice 1a (this slice): scaffold + types + pure permission resolution + tests

Per the PF-M1 Implementation Plan §20.2, slice 1 covers "Identity
model + persistence + framework choice (scope amendment captured)."
This slice is sub-slice 1a — it lands the structure and tests
**without** persistence or HTTP. Persistence and login handlers
arrive in subsequent slices under separate authorisation.

### Framework / language choice (Implementation Plan §19.19 scope amendment)

| Choice | Value | Rationale |
|---|---|---|
| Language | Python 3.10 | Continuity with the Beta 10 codebase; existing pytest infrastructure; no new dependencies for the scaffold |
| Dependencies | stdlib only | This slice introduces no new pip / npm dependencies |
| Test framework | pytest | Reuses the existing project test infrastructure |
| Persistence | **Not chosen yet** | Deferred to the slice that introduces login / session handlers |
| Web framework | **Not chosen yet** | Same deferral |
| API style | **Not chosen yet** | Same deferral |
| Session-token format | **Not chosen yet** | Same deferral |
| IdP | **Not chosen yet** | Per Scope Proposal §5.2 — IdP-agnostic |

### Layout

```
horizon_platform/
├── __init__.py
├── README.md                    (this file)
├── identity/
│   ├── __init__.py
│   ├── types.py                 # Dataclass + enum domain types
│   └── catalogue.py             # PF-M1 default role catalogue
├── access/
│   ├── __init__.py
│   └── permissions.py           # Pure permission resolution + port-scope filter
└── tests/
    ├── __init__.py
    ├── test_identity.py         # Domain-type invariants
    ├── test_catalogue.py        # Role-catalogue invariants
    ├── test_permissions.py      # Permission-resolution behaviour
    └── test_server_authoritative.py  # Structural server-authoritative shape
```

### What this slice does NOT do

Per the PF-M1 authorisation for slice 1a:

- **No persistence.** No database, no migrations, no schema files
- **No HTTP handlers.** No login route, no logout route, no /me endpoint
- **No session machinery.** No cookie signing, no token generation
- **No frontend changes.** No login UI; no AuthProvider; no
  authenticated boot wiring
- **No `server.py` modification.** The Beta 10 `server.py` is
  untouched
- **No `port_profiles.py` modification.** Untouched
- **No audit-helper modification.** All six Beta 10 audit helpers
  untouched
- **No Railway / config / env changes.** Untouched
- **No production auth activation.** None of this code is wired
  into the running V1 frontend

Later slices land each of these incrementally under separate
explicit authorisation.

### Running the tests

```
python3.10 -m pytest horizon_platform/tests/ -q
```

Expect all tests to pass green. The tests are pure unit tests
and exercise:

- Dataclass + enum constructibility and freezing
- Role catalogue invariants (8 workflow-profile roles; read-only
  PF-M1 scope; admin scoped to organisation; no silent operational
  read for admins)
- Permission resolution (active assignments, effective port-scope,
  resolved permissions, has_permission)
- Server-authoritative read filter, including Sev-1 cross-port
  leakage negative tests
- Cross-organisation read guard (PF-M1 defaults to same-org only)
- Server-authoritative *shape* — no global mutable state, no
  positional args, no UI imports, no skip / bypass / unsafe
  affordances

### Governance

- **Beta 10 Immutability Rule** in force — this package adds
  *new* code in a *new* path; it touches no Beta 10 surface
- **Independence Reset** framing preserved — no Smart Ocean X
  references anywhere in this package
- **Kyber boundary** in force — no cryptographic primitives
  selected in this slice (session-secret bootstrap, password
  hashing, etc. arrive in later slices under explicit governance)
- **Centre Panel Navigation** is view navigation only — this
  package contains no port-selector affordance and asserts the
  absence of port-switching shapes in its tests
- **Ocean Intelligence** is integration-shaped (PF-M7), not
  foundational — no OI reference appears anywhere in this
  package
