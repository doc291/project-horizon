"""
beta12_commitment.py — Beta 12 commitment-feasibility + consequence projection.

Phase 1. Stateless, additive, flag-gated on BETA11_ENABLED. This module is
imported ONLY when the flag is on (see server._beta12_summary_block); with the
flag off it is never imported, so Beta 10 behaviour is byte-identical.

PRODUCT FRAME
    "Horizon shows what is becoming impossible, what options still exist, and
     when those options expire."
    The machine detects and projects. The Harbour Master proposes and decides.

This module NEVER recommends, ranks, optimises, or selects a "best" option. It
reshapes the existing Beta 10 conflict + decision-support data into:

  - commitments  — a vessel's committed movement plus a feasibility state
                   (healthy / degrading / critical / broken), and
  - decision support cards — for each at-risk commitment, the four operator
                   questions:
                     1. Why is this commitment becoming infeasible?
                     2. What happens if nothing changes?
                     3. What happens if I choose this option?  (consequence)
                     4. When does this option cease to exist?  (expiry)

It consumes the standard operational levers (hold / advance / reassign)
NEUTRALLY — the "recommended" flag on those options is deliberately ignored.
No detection, decision-support, or What-If logic is modified; this is a
read-only reshaping layer over data those engines already produce.
"""

from datetime import datetime, timedelta, timezone


# ── Configurable Phase 1 planning thresholds (NOT operational rules) ───────────
# Conservative placeholder assumptions for demo capability only. Each value is
# the lead time a low-impact intervention needs BEFORE the conflict; once the
# clock passes (conflict_time - lead_time) the option is treated as expired.
#
# These are planning thresholds, not maritime truth. Any expiry derived from
# them must be framed in the UI as:
#   "Estimated option expiry based on configured planning threshold."
# Maritime validation is required before they are treated as operational rules.
OPTION_LEAD_TIME_MINS = {
    "delay_arrival":     90,    # hold at anchorage — pilot re-notice lead time
    "advance_departure": 180,   # accelerate cargo ops — terminal lead time
    "reassign_berth":    120,   # berth reassignment — port-planner lead time
}
_DEFAULT_LEAD_MINS = 120

ASSUMPTION_NOTE = "Estimated option expiry based on configured planning threshold."

# A vessel is a COMMITMENT only where Horizon has a berth/time/window
# relationship to test (owner direction, Phase 1). Not every visible vessel.
_INBOUND_COMMIT_STATUSES = {"scheduled", "confirmed", "at_risk"}
_BERTHED_STATUS = "berthed"

# Feasibility-state precedence (worst wins).
_STATE_ORDER = {"healthy": 0, "degrading": 1, "critical": 2, "broken": 3}

# How near an option's expiry must be (minutes) before an otherwise-high
# commitment is escalated in urgency. Tunable Phase 1 threshold.
_CRITICAL_EXPIRY_WINDOW_MINS = 60


# ── datetime helpers (mirror server.fmt / server.isoparse) ─────────────────────
def _parse_iso(s):
    if not s:
        return None
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except Exception:
        return None


def _fmt(dt):
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _worse(a, b):
    return a if _STATE_ORDER.get(a, 0) >= _STATE_ORDER.get(b, 0) else b


# ── Option expiry (Q4) ─────────────────────────────────────────────────────────
def compute_option_expiry(strategy, conflict_time_dt, now, lead_times=None):
    """When does this low-impact option cease to exist?

    expiry_at = conflict_time - configured lead time for the option's strategy.
    Returns a factual, clearly-flagged estimate — not an operational rule.
    """
    lead = (lead_times or OPTION_LEAD_TIME_MINS).get(strategy, _DEFAULT_LEAD_MINS)
    result = {
        "lead_time_mins": lead,
        "basis": ASSUMPTION_NOTE,
        "assumption": True,
    }
    if conflict_time_dt is None:
        result.update({"expiry_at": None, "expires_in_mins": None, "expired": False})
        return result
    expiry = conflict_time_dt - timedelta(minutes=lead)
    delta_mins = int((expiry - now).total_seconds() // 60)
    result.update({
        "expiry_at": _fmt(expiry),
        "expires_in_mins": max(0, delta_mins),
        "expired": delta_mins <= 0,
    })
    return result


# ── Consequence projection (Q3) — reuse the existing What-If shadow engine ──────
def _option_to_adjustment(option, fallback_vessel):
    """Translate a standard lever into a What-If adjustment. Never mutates."""
    strat = option.get("strategy", "")
    affected = option.get("affected_vessels") or []
    vessel = affected[0] if affected else fallback_vessel
    if not vessel:
        return None
    if strat == "delay_arrival":
        mins = int(option.get("delay_mins") or 0) or 90
        return {"type": "eta_push", "vessel": vessel, "minutes": mins, "direction": "delay"}
    if strat == "advance_departure":
        hrs = int(option.get("time_saving_hours") or 0) or 3
        return {"type": "eta_push", "vessel": vessel, "minutes": hrs * 60, "direction": "advance"}
    if strat == "reassign_berth":
        return {"type": "berth_change", "vessel": vessel, "new_berth": "ALT-" + str(vessel)[:6]}
    return None


def project_consequence(conflict, option, base_vessels, base_conflicts, shadow_fn):
    """What happens if the operator chooses this option? Neutral projection.

    Delegates to the existing What-If shadow engine (passed in as shadow_fn to
    avoid a circular import) and reshapes its factual output into consequence
    lines. Deliberately ignores the shadow engine's recommendation wording.
    """
    fallback = (conflict.get("vessel_names") or [None])[0]
    adj = _option_to_adjustment(option, fallback)
    empty = {
        "summary_lines": ["Consequence cannot be projected for this option."],
        "resolved_ids": [], "new_conflict_ids": [], "cost_delta": 0, "projectable": False,
    }
    if adj is None or shadow_fn is None:
        return empty
    try:
        shadow = shadow_fn(conflict.get("id"), [adj], base_vessels, base_conflicts) or {}
    except Exception:
        return empty

    resolved = shadow.get("resolved") or []
    new_conf = shadow.get("new_conflicts") or []
    cost_delta = int(shadow.get("cost_delta") or 0)

    lines = []
    if resolved:
        names = ", ".join(c.get("description", "") for c in resolved if c.get("description"))
        lines.append(f"Relieves {len(resolved)} conflict(s)" + (f": {names}" if names else "."))
    if new_conf:
        for c in new_conf:
            lines.append(f"Introduces: {c.get('description', 'a new conflict')}")
    if option.get("delay_mins"):
        lines.append(f"Applied delay: {int(option['delay_mins'])} min")
    if option.get("cascade_impact"):
        lines.append(f"Downstream: {option['cascade_impact']}")
    if cost_delta < 0:
        lines.append(f"Projected net saving vs current plan: ~A${abs(cost_delta):,}")
    elif cost_delta > 0:
        lines.append(f"Projected net additional cost vs current plan: ~A${cost_delta:,}")
    else:
        lines.append("Cost-neutral vs current plan.")
    if not resolved and not new_conf:
        lines.insert(0, "No material change to the conflict landscape projected.")

    return {
        "summary_lines": lines,
        "resolved_ids": [c.get("id") for c in resolved],
        "new_conflict_ids": [c.get("id") for c in new_conf],
        "cost_delta": cost_delta,
        "projectable": True,
    }


# ── Commitments (feasibility state) ────────────────────────────────────────────
def _is_commitment(v):
    """A vessel is a commitment only where a berth/time/window exists to test."""
    status = v.get("status")
    if status == _BERTHED_STATUS and v.get("etd"):
        return ("departure", v.get("etd"))          # expected berth release
    if status in _INBOUND_COMMIT_STATUSES and v.get("berth_id") and v.get("eta"):
        return ("arrival", v.get("eta"))            # committed inbound movement
    return None


def _conflict_contribution(conflict, now):
    """Classify one conflict's impact on a commitment + gather option runway."""
    sig = (conflict.get("signal_type") or "").upper()
    sev = (conflict.get("severity") or "").lower()
    ctype = (conflict.get("conflict_type") or "").lower()
    ds = conflict.get("decision_support") or {}
    options = ds.get("options") or []
    ct = _parse_iso(conflict.get("conflict_time"))

    available = 0
    nearest_expiry = None
    for o in options:
        ex = compute_option_expiry(o.get("strategy", ""), ct, now)
        if (o.get("feasibility") != "low") and not ex["expired"]:
            available += 1
            em = ex["expires_in_mins"]
            if em is not None and (nearest_expiry is None or em < nearest_expiry):
                nearest_expiry = em

    if sig == "CONFLICT":
        if ctype == "bridge_restriction" and sev == "critical":
            contrib = "broken"                       # absolute air-draft limit
        elif options and available == 0:
            contrib = "broken"                       # levers exist but all gone
        elif sev in ("critical", "high"):
            contrib = "critical"
        else:
            contrib = "degrading"
    elif sig in ("WARNING", "ADVISORY"):
        contrib = "degrading"
    else:
        contrib = "healthy"                          # WEATHER / other: not per-commitment

    evidence = {
        "conflict_id": conflict.get("id"),
        "conflict_type": conflict.get("conflict_type"),
        "signal_type": conflict.get("signal_type"),
        "severity": conflict.get("severity"),
        "why": conflict.get("description", ""),
        "provenance": conflict.get("provenance", "simulated"),
        "options_available": available,
        "nearest_option_expiry_mins": nearest_expiry,
    }
    return contrib, evidence


def build_commitments(vessels, berths, conflicts, now):
    """Reshape vessels into committed movements with a feasibility state."""
    berth_name = {b.get("id"): b.get("name") for b in (berths or [])}
    # Only conflicts that can attach to a specific commitment (have vessel ids).
    per_vessel = {}
    for c in conflicts or []:
        if (c.get("signal_type") or "").upper() == "WEATHER":
            continue
        for vid in (c.get("vessel_ids") or []):
            per_vessel.setdefault(vid, []).append(c)

    commitments = []
    for v in vessels or []:
        commit = _is_commitment(v)
        if not commit:
            continue
        movement, committed_time = commit
        attached = per_vessel.get(v.get("id"), [])
        state = "healthy"
        evidence = []
        for c in attached:
            contrib, ev = _conflict_contribution(c, now)
            evidence.append(ev)
            state = _worse(state, contrib)
        ct = _parse_iso(committed_time)
        commitments.append({
            "commitment_id": v.get("id"),
            "vessel_name": v.get("name") or v.get("vessel_name", ""),
            "berth_id": v.get("berth_id"),
            "berth_name": berth_name.get(v.get("berth_id"), v.get("berth_id")),
            "movement": movement,
            "committed_time": committed_time,
            "committed_in_mins": (int((ct - now).total_seconds() // 60) if ct else None),
            "feasibility_state": state,
            "degradation_evidence": evidence,
            "conflict_ids": [c.get("id") for c in attached],
            "provenance": (evidence[0]["provenance"] if evidence else _vessel_provenance(v)),
        })
    return commitments


def _vessel_provenance(v):
    _REAL = {"ais", "aisstream", "mst", "qships", "scraper", "live"}
    if str(v.get("id", "")).startswith("SIM-"):
        return "simulated"
    return "live" if v.get("source") in _REAL else "simulated"


# ── Decision Support Cards ─────────────────────────────────────────────────────
_DO_NOTHING = {
    "berth_overlap":     "Berth stays double-committed; the committed movement cannot berth on schedule and following arrivals, pilot and tug bookings are affected.",
    "berth_not_ready":   "Berth is not clear in time; the committed arrival has nowhere to go and must wait, compressing later movements.",
    "pilotage_window":   "Pilot notice stays inside the required window; the movement risks proceeding without confirmed pilotage.",
    "towage_resource":   "The tug stays double-booked; one of the movements loses its towage and cannot proceed safely.",
    "eta_variance":      "The reported ETA drift stands; the committed window is uncertain and downstream planning cannot firm up.",
    "bridge_restriction":"Estimated air-draft stays over the bridge limit; the transit cannot proceed under the bridge as planned.",
}


def build_decision_support_cards(commitments, conflicts, vessels, now, shadow_fn):
    """One card per at-risk commitment, anchored to its worst conflict.

    Answers the four operator questions. Never recommends or ranks.
    """
    by_id = {c.get("id"): c for c in (conflicts or [])}
    cards = []
    for cm in commitments:
        if cm["feasibility_state"] == "healthy":
            continue
        attached = [by_id[cid] for cid in cm["conflict_ids"] if cid in by_id]
        if not attached:
            continue
        # Anchor to the worst conflict (severity, then earliest conflict_time).
        sev_rank = {"critical": 0, "high": 1, "medium": 2, "low": 3}
        primary = sorted(
            attached,
            key=lambda c: (sev_rank.get((c.get("severity") or "").lower(), 9),
                           c.get("conflict_time") or ""),
        )[0]
        ctype = (primary.get("conflict_type") or "").lower()
        ds = primary.get("decision_support") or {}
        options_in = ds.get("options") or []
        ct = _parse_iso(primary.get("conflict_time"))

        options_out = []
        for o in options_in:
            options_out.append({
                "id": o.get("id"),
                "strategy": o.get("strategy"),
                "label": o.get("label"),
                "description": o.get("description"),
                "feasibility": o.get("feasibility"),
                "direct_cost_label": o.get("cost_label"),
                # Q4 — when does this option cease to exist?
                "expiry": compute_option_expiry(o.get("strategy", ""), ct, now),
                # Q3 — what happens if I choose this option?
                "consequence": project_consequence(primary, o, vessels, conflicts, shadow_fn),
            })

        cards.append({
            "commitment_id": cm["commitment_id"],
            "vessel_name": cm["vessel_name"],
            "berth_name": cm["berth_name"],
            "movement": cm["movement"],
            "committed_time": cm["committed_time"],
            "feasibility_state": cm["feasibility_state"],
            "provenance": cm["provenance"],
            "primary_conflict_id": primary.get("id"),
            # Q1 — why is this commitment becoming infeasible?
            "why_infeasible": [ev["why"] for ev in cm["degradation_evidence"] if ev.get("why")],
            # Q2 — what happens if nothing changes?
            "do_nothing": _DO_NOTHING.get(
                ctype,
                f"If unchanged, {primary.get('description', 'the conflict')} persists and the committed "
                f"{cm['movement']} cannot proceed as planned.",
            ),
            # Q3 + Q4 carried on each option
            "options": options_out,
            "advisory_note": ("Horizon projects consequences of options you test. "
                              "The Harbour Master selects and decides. No option is recommended."),
        })
    return cards


# ── Top-level block builder ────────────────────────────────────────────────────
def build_beta12_block(vessels, berths, conflicts, now, shadow_fn=None):
    """Assemble the flag-gated Beta 12 block for /api/summary. Never raises."""
    commitments = build_commitments(vessels, berths, conflicts, now)
    cards = build_decision_support_cards(commitments, conflicts, vessels, now, shadow_fn)
    counts = {"healthy": 0, "degrading": 0, "critical": 0, "broken": 0}
    for cm in commitments:
        counts[cm["feasibility_state"]] = counts.get(cm["feasibility_state"], 0) + 1
    return {
        "generated_at": _fmt(now),
        "commitments": commitments,
        "state_counts": counts,
        "decision_support_cards": cards,
        "planning_thresholds_mins": OPTION_LEAD_TIME_MINS,
        "assumption_note": ASSUMPTION_NOTE,
        "frame": "The machine detects and projects. The Harbour Master proposes and decides.",
    }
