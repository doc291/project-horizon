# Phase 1 Hardening Note — What-If POST Application-Layer Auth

**Status:** Deferred — Phase 1.x
**Date logged:** 2026-05-13
**Surfaced by:** Step 0.11 PR #20 regression-gate review
**Scope:** Application-layer authentication on three `/api/whatif*` POST
endpoints
**Phase 0 disposition:** Documented as-is. Regression test reflects the
current shipped behaviour, not the future hardened behaviour.

---

## Observation

The following POST endpoints in Beta 10 return `HTTP 200` without an
authenticated session cookie at the application layer:

```
POST /api/whatif         → 200
POST /api/apply-whatif   → 200
POST /api/clear-whatif   → 200
```

The two adjacent operator-action POSTs **do** auth-gate at the
application layer:

```
POST /api/set_port       → 401
POST /api/send-brief     → 401
```

The difference is in `server.py:do_POST` (around line 2549) where only
`/api/set_port` and `/api/send-brief` invoke `_is_authenticated()` and
call `send_error(401)`. The three what-if POSTs route directly to
their handlers with no auth check.

### Surfaced by

This was surfaced by ChatGPT's review of PR #20 (Step 0.11 regression
gate). The regression test's `TestAuthGatedRoutes` deliberately
parametrises only the two paths that genuinely return 401, with a
class docstring explaining the scoping. The test reflects current
Beta 10 behaviour, not desired Phase 1 behaviour.

Live in-process probe on `origin/main` (post-PR #20 tip):

```
POST /api/set_port       → 401   ← test asserts
POST /api/send-brief     → 401   ← test asserts
POST /api/whatif         → 200   ← test does NOT assert (excluded)
POST /api/apply-whatif   → 200   ← test does NOT assert (excluded)
POST /api/clear-whatif   → 200   ← test does NOT assert (excluded)
```

## Why this is acceptable for Phase 0 / Beta 10 demo

1. **HTTPS perimeter.** Railway terminates TLS and only exposes the
   service on `https://horizon.amsgroup.com.au`. Network-layer access
   is constrained to TLS clients.

2. **No credentials in request bodies.** The endpoints accept JSON
   bodies describing scenario adjustments — no operator credentials,
   no PII, no payment-grade material.

3. **Effects are scoped to view state.** `/api/whatif` is read-only
   shadow simulation. `/api/apply-whatif` mutates `_WHATIF_OVERLAY`,
   which only affects what `/api/summary` subsequently returns —
   and `/api/summary` *is* auth-gated. An unauthenticated caller
   cannot observe the overlay's effect.

4. **Beta 10 is a single-tenant demo.** No multi-tenant isolation
   concerns apply yet. The audit ledger gains the OPERATOR_ACTED
   event for `whatif_apply` / `whatif_clear` when audit is enabled,
   but Phase 0 production has `DATABASE_URL` unset.

5. **No CSRF surface.** The handler is not browser-forms-driven for
   what-if; the SPA's fetch calls carry the cookie when authenticated.
   An unauthenticated attacker could POST a what-if overlay, but it
   would only affect their own view of subsequent summary requests,
   which they cannot read without authenticating.

## Why it must be tightened in Phase 1

1. **Multi-tenant deployment.** When Phase 1 introduces customer
   tenants, an unauthenticated POST to `/api/apply-whatif` could
   land an overlay tagged against a tenant the caller has no right
   to mutate state for.

2. **Audit ledger integrity.** With persistent audit enabled, the
   OPERATOR_ACTED row recorded against `whatif_apply` would carry
   an `actor_handle` derived from `_AUTH_USER` (the demo user)
   even when the request was made by an unauthenticated client.
   That is a misattribution risk: the ledger would say operator O-1
   acted, when actually an unauthenticated network caller did.

3. **CSRF in browser context.** A logged-in operator visiting a
   malicious third-party page could have their browser silently
   POST a what-if scenario to the what-if endpoints. The current
   `SameSite=Strict` cookie attribute mitigates this in modern
   browsers, but defence in depth is appropriate at the
   application layer.

4. **Consistency with the audit-evidence story.** Phase 0.7c /
   Phase 0.8a established that audit emission for
   `RECOMMENDATION_PRESENTED` and `OPERATOR_ACTED` is gated on the
   surface being authenticated. The what-if POSTs are the only
   operator-action surfaces that do not enforce this gate at the
   application layer.

## Prescribed change (Phase 1.x)

The fix is small and isolated to `server.py:do_POST`. Add the same
two-line guard already used for `set_port` / `send-brief`:

```python
elif path == "/api/whatif":
    if not self._is_authenticated():
        self.send_error(401)
        return
    self._whatif()
elif path == "/api/apply-whatif":
    if not self._is_authenticated():
        self.send_error(401)
        return
    self._apply_whatif()
elif path == "/api/clear-whatif":
    if not self._is_authenticated():
        self.send_error(401)
        return
    self._clear_whatif()
```

Or, equivalently, lift the auth check into a single guard at the
top of `do_POST` mirroring `do_GET`'s gate at server.py:2910 —
that is the cleaner refactor.

### Regression-test update at that time

`tests/test_beta10_regression.py::TestAuthGatedRoutes` parametrises:

```python
@pytest.mark.parametrize("path", ("/api/set_port", "/api/send-brief"))
def test_protected_post_returns_401_without_cookie(self, path):
```

When the change ships, expand the parametrise tuple to:

```python
@pytest.mark.parametrize("path", (
    "/api/set_port",
    "/api/send-brief",
    "/api/whatif",
    "/api/apply-whatif",
    "/api/clear-whatif",
))
```

The class docstring will need a one-line update to remove the
"network edge in a real deployment" caveat. No other test changes
required.

## Not affected by this deferral

- The Phase 0.11 regression test continues to pass — it correctly
  reflects current behaviour and does not over-assert.
- The Phase 0 exit gate is unaffected; this is a Phase 1 hardening
  item, not a Phase 0 blocker.
- Audit-data lineage is unchanged: `OPERATOR_ACTED` for
  `whatif_apply` / `whatif_clear` still fires correctly under the
  current code, with `actor_handle="O-1"` (the demo user). Phase 1
  hardening prevents misattribution in production multi-tenant
  deployments — Phase 0 single-tenant demo is unaffected.

## Out of scope for this note

- A blanket `do_POST` auth gate vs per-endpoint checks (style
  decision; either works).
- CSRF token implementation. `SameSite=Strict` covers the relevant
  cases for Phase 0 demo browsing; richer CSRF defences are a
  separate Phase 1 question.
- Rate limiting on what-if endpoints. Out of scope for Phase 1
  initial hardening; revisit if abuse signals appear.
