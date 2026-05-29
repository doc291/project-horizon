# Beta 10 — Rollback & Redeploy Runbook

**Status:** ACTIVE — Beta 10 is a protected commercial demonstration asset.
**Owner:** Tony Trajceski (AMSG)
**Last reviewed:** 2026-05-29

---

## 1. Purpose

Beta 10 is the live commercial demonstration platform shown to Ports Victoria,
Darwin Ports, Shipping Australia, and senior maritime stakeholders. It must be
re-deployable to a known-good state at any time, with zero dependence on
in-flight Beta 11 / V1 work.

This runbook defines the **authoritative Beta 10 commit**, the **redeploy
procedure**, and the **verification gate** that confirms a rollback succeeded.

---

## 2. ⚠ CRITICAL — Which commit is Beta 10

**The authoritative Beta 10 commit is `d04db8b`.**

| Ref | Points to | Use for rollback? |
|---|---|---|
| **`beta-10-demo`** (annotated tag) | **`d04db8b`** (2026-05-29) | ✅ **YES — this is the authoritative pin** |
| **`demo/beta-10`** (protected branch) | **`d04db8b`** (2026-05-29) | ✅ **YES — redeployable source branch** |
| `beta-10` (old lightweight tag) | `8a26fc3` (2026-05-07) | ❌ **NO — STALE. DO NOT USE.** |

### Why the old `beta-10` tag must NOT be used

The pre-existing `beta-10` tag points to `8a26fc3` from **2026-05-07** — roughly
three weeks and ~25 commits behind the current demo. It **predates**:

- the Darwin demo-card fixes (PRs #55 / #56),
- the M2 visual remediation,
- **all five cost-credibility fixes (PRs #77–#81)** that the market responded
  positively to (decision-card and Scenario Builder costs scaled to the
  A$50,000–A$120,000 AUD credibility band).

Verification: `git show beta-10:server.py | grep -c _scale_cost_by_loa` → **0**
(the cost-band logic does not exist at the stale tag).

**Redeploying the stale `beta-10` tag would visibly regress the demo** to a
pre-cost-credibility state. Always roll back to `beta-10-demo` / `demo/beta-10`
(`d04db8b`), never to `beta-10`.

> The stale `beta-10` tag has been left in place (not deleted or moved) pending
> an explicit decision. Until it is retired/relabelled, treat it as a trap and
> rely only on `beta-10-demo`.

---

## 3. Deployment topology

| Item | Value |
|---|---|
| Railway project | `Project-Horizon` |
| Environment | production |
| Public URL | `https://project-horizon-production-a03c.up.railway.app` |
| Custom domain | `https://horizon.amsgroup.com.au` |
| Start command | `python3 server.py` |
| Build command | `pip install -r requirements.txt && playwright install chromium --with-deps || true` |
| Beta 10 backend | `server.py` + Python modules |
| Beta 10 frontend | `index.html` (served by `server.py`) |

> **KNOWN RISK (not yet remediated):** the `Project-Horizon` production service
> currently auto-deploys from **`main`**. `main` also carries Beta 11 / V1 work.
> Until production is repointed to `demo/beta-10`, a merge to `main` can
> auto-deploy over the demo. See §6.

---

## 4. Rollback / redeploy procedure

### 4.1 Confirm the target
```
git fetch origin --tags
git rev-parse beta-10-demo^{commit}     # MUST print d04db8b09691f69964a94d9b9c0575500daf9930
git rev-parse origin/demo/beta-10       # MUST print the same
```
If either does not match `d04db8b…`, **stop** and escalate — do not redeploy.

### 4.2 Redeploy via Railway (dashboard — preferred, lowest risk)
1. Railway → `Project-Horizon` → production → **Deployments**.
2. Locate the last known-good deployment built from `d04db8b` (or the most
   recent green deployment that predates the incident).
3. Use **Redeploy** / **Rollback to this deployment**.
4. Wait for the build to go healthy.

### 4.3 Redeploy via source (if the service tracks a branch)
If/when production tracks `demo/beta-10`:
```
# demo/beta-10 is protected and already at d04db8b — a redeploy of its HEAD
# restores Beta 10. No code changes required.
```
If production still tracks `main` and `main` has drifted, **do not force `main`
backwards.** Instead repoint the service to `demo/beta-10` (see §6) and redeploy.

### 4.4 Verify (all must pass before declaring rollback complete)
```
curl -sS -o /dev/null -w "%{http_code}\n" https://project-horizon-production-a03c.up.railway.app/health
#   expect: 200

curl -sS https://project-horizon-production-a03c.up.railway.app/api/health-data | python3 -m json.tool | grep '"overall"'
#   expect: "overall": "ready"   (transient "warning" is acceptable if caused
#   only by upstream weather/tide/AIS advisories — check the issues list)
```
Then, in an authenticated operator browser session:
- A Decision Card cost shows a value in **A$50,000–A$120,000** (proves the
  cost-credibility work is live — the single fastest "is this the right
  Beta 10?" check).
- Scenario Builder for a berth-change action shows a banded A$ value, **not**
  ~A$600 (proves PR #81 is present).

### 4.5 Regression gate
```
git switch demo/beta-10        # or check out d04db8b
.venv/bin/python -m pytest tests/test_beta10_regression.py -q
#   expect: 46 passed
```

### 4.6 Record
Append to the change log (§7): date/time, operator, from-SHA, to-SHA, reason.

---

## 5. Redeploy-anytime guarantee — current status

| Protection | Status |
|---|---|
| Authoritative commit pinned (`beta-10-demo` tag @ `d04db8b`) | ✅ done |
| Protected source branch (`demo/beta-10` @ `d04db8b`) | ✅ done (push protection rules = manual, §6) |
| Regression gate green at the pinned commit | ✅ 46/46 |
| Deployment isolated from `main` | ❌ **NOT YET** — see §6 |
| Rollback procedure documented | ✅ this file |

**Beta 10 is pinned and redeployable, BUT not yet isolated from `main`.** The
isolation step (§6) is required to fully satisfy "no Beta 11 deployment may
overwrite Beta 10."

---

## 6. Outstanding manual actions (require explicit owner approval)

These were intentionally **not** performed automatically.

### 6.1 Apply GitHub branch-protection rules to `demo/beta-10`
Branch protection is a GitHub setting, not a git operation. In GitHub →
Settings → Branches → add a rule for `demo/beta-10`:
- Require a pull request before merging.
- Block force-pushes.
- Block deletion.

### 6.2 Repoint Railway production off `main` (load-bearing isolation step)
In Railway → `Project-Horizon` → production → Settings → Source:
- Change the deployment source branch from `main` to **`demo/beta-10`** (or pin
  to commit `d04db8b`).
- This guarantees future `main` activity (Beta 11 / V1) cannot auto-deploy over
  the demo.

> Until 6.2 is done, the demo environment remains exposed to `main` merges.

### 6.3 Retire / relabel the stale `beta-10` tag (deferred by owner)
Left untouched per instruction. When approved, relabel to e.g.
`beta-10-historic-2026-05-07` to remove the rollback trap.

---

## 7. Change log

| Date | Actor | Action | From | To |
|---|---|---|---|---|
| 2026-05-29 | AMSG | Created `beta-10-demo` tag + `demo/beta-10` branch; authored this runbook | — | `d04db8b` |
