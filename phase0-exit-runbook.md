# Phase 0 Exit Runbook — Manual Verification Companion

**Status:** Phase 0 exit gate manual companion
**Automated companion:** `tests/test_beta10_regression.py`
**Run conditions:** Once, after the Phase 0 exit PR has merged and Railway has redeployed.
**Operator:** Tony Trajceski (or delegated)

---

## Purpose

`tests/test_beta10_regression.py` locks the production-safety invariants
that machines can check: response shape, header presence, asset hashes,
auth gating, audit no-op posture, source-level event-type scope. It
runs as part of `pytest tests/` and is suitable as a CI gate.

This runbook covers the four things machines cannot do well: a live
browser, a live Railway environment, and a human's pair of eyes. All
four checks must pass before the Phase 0 exit can be declared.

After all four checks pass, sign off at the bottom of this file with
the date and current `main` commit SHA.

---

## Check 1 — Live `/api/summary` on Railway production

**Why:** The automated test drives `build_summary()` in-process. This
check confirms the live deployment is serving the same shape, has the
six security headers in transit (not just declared in source), and is
not silently writing to a database.

**Steps:**

1. Log in to production via browser. Confirm cookie established.
2. From the same browser session, open DevTools → Network. Refresh
   the page so `/api/summary` fires.
3. Copy the request as cURL. Run it from a terminal:
   ```sh
   curl -sS -i 'https://horizon.amsgroup.com.au/api/summary' \
        -b "$COOKIE_FROM_DEVTOOLS"  | head -40
   ```

**Pass criteria:**

- HTTP status: `200`
- Headers present: `X-Content-Type-Options`, `X-Frame-Options`,
  `Referrer-Policy`, `Permissions-Policy`,
  `Cross-Origin-Opener-Policy`, `Strict-Transport-Security`
- Response body parses as JSON
- Top-level key `data_source` present and is one of
  `"live" | "qships" | "simulated"`
- Top-level key `conflicts` is a list

**Fail action:** Stop. Do not declare Phase 0 exit. Inspect the
deviation against the locked baseline.

---

## Check 2 — UI smoke: Decision Card renders, Brisbane map loads

**Why:** The automated tests confirm that the *data* for Decision
Cards is present in `/api/summary`. They cannot confirm that the
*rendering* succeeds — that the React/Vue/whatever-it-is in
`index.html` consumes the data without console errors.

**Steps:**

1. Log in to production in a clean browser tab.
2. Confirm the live ops view loads (no white screen, no error banner).
3. Confirm at least one Decision Card is visible (the demo's B03 or
   B04 berth_overlap scenario should produce one, given the simulated
   data fallback).
4. Click the Decision Card and confirm sequencing alternatives expand.
5. Confirm the Brisbane port map renders with vessel markers.
6. Open DevTools → Console. Confirm no red-level errors.

**Pass criteria:** All five steps pass. Console has at most warnings
(yellow), no errors (red).

**Fail action:** Stop. Capture the console error and inspect what the
UI is reading from `/api/summary` that the automated test missed.

---

## Check 3 — Marketing site (`https://horizon.ams.group`)

**Why:** Host-based routing splits operational app (default) from
marketing site (`_SITE_HOST = "horizon.ams.group"`). The automated
test confirms `_SITE_HOST` is unchanged in source; this check
confirms the live DNS + Railway routing still delivers the marketing
site at the marketing hostname.

**Steps:**

1. Open `https://horizon.ams.group` in a clean browser tab (or
   incognito to bypass cached auth cookies).
2. Confirm the marketing landing page renders.
3. Confirm the page favicon shows the Project Horizon port-scene
   logo (post-Beta-10 favicon refresh; commit `129639b` / `e043837`).
4. View page source. Confirm `<link rel="icon">` references match the
   `deploy/` directory assets.

**Pass criteria:** Marketing site loads. Favicon is the port-scene
logo, not the AMS Group "AM" logo, not the browser default.

**Fail action:** Stop. Confirm DNS, Railway domains, and
`deploy/index.html` content.

---

## Check 4 — Login flow end-to-end

**Why:** The automated test confirms unauthenticated requests redirect
to `/login?next=...` and that protected POSTs return 401 without a
cookie. This check confirms the full login round-trip — credentials
accepted, cookie set, redirect lands on the requested page.

**Steps:**

1. Open production in a clean incognito tab.
2. Visit `https://horizon.amsgroup.com.au/api/summary` directly.
   Expect redirect to `/login?next=/api/summary`.
3. Enter credentials (`HORIZON_USER` / `HORIZON_PASS` from Railway env).
4. Submit form.
5. Expect redirect back to `/api/summary` and the JSON response.
6. Confirm the cookie is set with `HttpOnly; Secure; SameSite=Strict`
   attributes (DevTools → Application → Cookies).

**Pass criteria:** All six steps pass. Cookie has the three security
attributes.

**Fail action:** Stop. Check that `TOKEN_SECRET` is still set in
Railway and that the auth surface has not been disturbed.

---

## Sign-off

Phase 0 is declared complete when all four checks above pass.

When that happens, fill in the line below (do not delete previous
sign-offs — they are the audit trail of Phase 0 exits across deploys):

```
Phase 0 exit confirmed on 2026-05-13 by Tony Trajceski
Verified against main commit 4ad4aae
All 4 manual checks: PASS
Automated regression test: PASS (140 passed)
```

After sign-off, tag the commit locally and push:

```sh
git tag -a phase-0-complete <SHA> -m "Phase 0 exit gate verified"
git push origin phase-0-complete
```

Phase 1 work may then begin against this gate. Any PR that breaks
`tests/test_beta10_regression.py` fails CI; any PR that requires
deliberately updating a locked baseline must do so explicitly with
reviewer authorisation.
