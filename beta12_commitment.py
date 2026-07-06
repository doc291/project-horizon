"""
beta12_commitment.py — Beta 12 commitment-feasibility + consequence projection.

Phase 1 (Preview-Readiness pass). Stateless, additive, flag-gated on
BETA11_ENABLED. Imported ONLY when the flag is on (see server._beta12_summary_
block); with the flag off it is never imported, so Beta 10 is byte-identical.

PRODUCT FRAME
    "Horizon shows what is becoming impossible, what options still exist, and
     when those options expire."
    The machine detects and projects. The Harbour Master proposes and decides.

This module NEVER recommends, ranks, optimises, or names a "best" option. It
reshapes existing Beta 10 conflict + decision-support data into:

  - commitments  — a vessel movement Horizon can test (arrival with a berth, or
                   a berthed vessel's expected departure) with an operator-facing
                   state: On Track / Watch / Act Now, and evidence freshness, and
  - decision support cards — for operator-relevant threats only (advisory-grade
                   signals contribute to state + evidence but do NOT create a
                   card), answering the four operator questions:
                     1. Evidence — why this commitment needs attention
                     2. If unchanged — what continues
                     3. Consequence — what a tested option changes
                     4. Option expires — when a low-impact option ceases to exist

Operator plan correction (apply_operator_plan) lets the Harbour Master retime a
commitment and have feasibility + consequences re-projected against that
"Operator test plan" — a shadow projection only, no external write-back, no
notifications.

Costs are deliberately NOT surfaced (demo cost figures are not trusted yet).
Per-option "feasibility" grades are NOT surfaced (they read as disguised
recommendations). Both are used internally where needed, never shown.
"""

import copy
from datetime import datetime, timedelta, timezone


# ── Configurable Phase 1 planning thresholds (NOT operational rules) ───────────
# Conservative placeholder assumptions for demo capability only. Each value is
# the lead time a low-impact option needs BEFORE the conflict; once the clock
# passes (conflict_time - lead_time) the option is treated as expired. Any expiry
# shown must be framed as "Estimated option expiry based on configured planning
# threshold." Maritime validation required before treated as operational.
OPTION_LEAD_TIME_MINS = {
    "delay_arrival":     90,
    "advance_departure": 180,
    "reassign_berth":    120,
}
_DEFAULT_LEAD_MINS = 120
ASSUMPTION_NOTE = "Estimated option expiry based on configured planning threshold."

# A vessel is a COMMITMENT only where a berth/time/window relationship exists.
_INBOUND_COMMIT_STATUSES = {"scheduled", "confirmed", "at_risk"}
_BERTHED_STATUS = "berthed"

# Internal 4-level feasibility (worst wins) → collapsed to 3 operator states.
_STATE_ORDER = {"healthy": 0, "degrading": 1, "critical": 2, "broken": 3}
_OP_STATE = {"healthy": "on_track", "degrading": "watch",
             "critical": "act_now", "broken": "act_now"}
OP_LABEL = {"on_track": "On Track", "watch": "Watch", "act_now": "Act Now"}

# Cards are for operator-relevant threats only. Advisory-grade signals
# (ADVISORY/WARNING/WEATHER) inform state + evidence but never create a card.
_CARD_WORTHY_SIGNALS = {"CONFLICT"}

_CRITICAL_EXPIRY_WINDOW_MINS = 60

DEPARTURE_CONFIDENCE_NOTE = "Lower confidence: scheduled berth release only"
SOURCE_UNAVAILABLE = "Source time unavailable"

# Schedule/AIS evidence older than this is flagged as materially stale.
STALE_AFTER_MINS = 360   # 6 hours

# The operator test plan re-checks berth/pilotage/towage/ETA/bridge feasibility
# only. It does NOT recompute tide, UKC, weather or PBG-to-berth transit, so it
# must be presented as PARTIAL, never as full feasibility/consequence.
PARTIAL_PLAN_LABEL = "Partial operator test plan"
PARTIAL_PLAN_NOTE = ("Projection excludes tide, UKC, weather and transit "
                     "recalculation in this preview.")


# ── datetime helpers ───────────────────────────────────────────────────────────
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


def _default_freshness():
    return {"assembled_at": None, "ais_as_of": None, "schedule_as_of": None,
            "weather_source": None, "weather_as_of": None,
            "tide_source": None, "tide_as_of": None}


def _freshness_stale(freshness, now):
    """Return (stale, [sources]) — schedule/AIS evidence older than the
    threshold. Unavailable (no timestamp) is 'unknown', not 'stale'."""
    stale_sources = []
    for key in ("schedule_as_of", "ais_as_of"):
        ts = _parse_iso((freshness or {}).get(key))
        if ts is not None and (now - ts).total_seconds() > STALE_AFTER_MINS * 60:
            stale_sources.append(key)
    return (bool(stale_sources), stale_sources)


# ── Option expiry (Q4) ─────────────────────────────────────────────────────────
def compute_option_expiry(strategy, conflict_time_dt, now, lead_times=None):
    lead = (lead_times or OPTION_LEAD_TIME_MINS).get(strategy, _DEFAULT_LEAD_MINS)
    result = {"lead_time_mins": lead, "basis": ASSUMPTION_NOTE, "assumption": True}
    if conflict_time_dt is None:
        result.update({"expiry_at": None, "expires_in_mins": None, "expired": False})
        return result
    expiry = conflict_time_dt - timedelta(minutes=lead)
    delta_mins = int((expiry - now).total_seconds() // 60)
    result.update({"expiry_at": _fmt(expiry),
                   "expires_in_mins": max(0, delta_mins),
                   "expired": delta_mins <= 0})
    return result


# ── Consequence projection (Q3) — reuse the What-If shadow engine, NO costs ─────
def _option_to_adjustment(option, fallback_vessel):
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
    """Neutral, cost-free projection of an option's effect.

    Delegates to the What-If shadow engine (passed in) and reshapes only its
    factual conflict deltas — costs and the engine's recommendation wording are
    deliberately dropped.
    """
    fallback = (conflict.get("vessel_names") or [None])[0]
    adj = _option_to_adjustment(option, fallback)
    empty = {"summary_lines": ["Consequence cannot be projected for this option."],
             "resolved_ids": [], "new_conflict_ids": [], "projectable": False}
    if adj is None or shadow_fn is None:
        return empty
    try:
        shadow = shadow_fn(conflict.get("id"), [adj], base_vessels, base_conflicts) or {}
    except Exception:
        return empty

    resolved = shadow.get("resolved") or []
    new_conf = shadow.get("new_conflicts") or []
    lines = []
    if resolved:
        names = ", ".join(c.get("description", "") for c in resolved if c.get("description"))
        lines.append(f"Relieves {len(resolved)} conflict(s)" + (f": {names}" if names else "."))
    if new_conf:
        for c in new_conf:
            lines.append(f"Introduces: {c.get('description', 'a new conflict')}")
    if option.get("delay_mins"):
        lines.append(f"Applied delay: {int(option['delay_mins'])} min")
    # NOTE: the Beta 10 option 'cascade_impact' narrative is deliberately NOT
    # surfaced — it carries Beta 10 imperative phrasing ("must accelerate…")
    # that the Beta 12 language rules exclude. Downstream impact is represented
    # by the factual resolved/new-conflict deltas above instead.
    if not resolved and not new_conf:
        lines.insert(0, "Evidence indicates no material change to the conflict landscape.")
    return {"summary_lines": lines,
            "resolved_ids": [c.get("id") for c in resolved],
            "new_conflict_ids": [c.get("id") for c in new_conf],
            "projectable": True}


# ── Commitments (state + evidence) ─────────────────────────────────────────────
def _is_commitment(v):
    status = v.get("status")
    if status == _BERTHED_STATUS and v.get("etd"):
        return ("departure", v.get("etd"))
    if status in _INBOUND_COMMIT_STATUSES and v.get("berth_id") and v.get("eta"):
        return ("arrival", v.get("eta"))
    return None


def _conflict_contribution(conflict, now):
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
        if (o.get("feasibility") != "low") and not ex["expired"]:   # internal only
            available += 1
            em = ex["expires_in_mins"]
            if em is not None and (nearest_expiry is None or em < nearest_expiry):
                nearest_expiry = em

    if sig == "CONFLICT":
        if ctype == "bridge_restriction" and sev == "critical":
            contrib = "broken"
        elif options and available == 0:
            contrib = "broken"
        elif sev in ("critical", "high"):
            contrib = "critical"
        else:
            contrib = "degrading"
    elif sig in ("WARNING", "ADVISORY"):
        contrib = "degrading"
    else:
        contrib = "healthy"

    evidence = {
        "conflict_id": conflict.get("id"),
        "conflict_type": conflict.get("conflict_type"),
        "signal_type": conflict.get("signal_type"),
        "severity": conflict.get("severity"),
        "why": conflict.get("description", ""),
        "provenance": conflict.get("provenance", "simulated"),
        "advisory_grade": sig not in _CARD_WORTHY_SIGNALS,
    }
    return contrib, evidence


def build_commitments(vessels, berths, conflicts, now):
    berth_name = {b.get("id"): b.get("name") for b in (berths or [])}
    per_vessel = {}
    for c in conflicts or []:
        if (c.get("signal_type") or "").upper() == "WEATHER":
            continue
        for vid in (c.get("vessel_ids") or []):
            per_vessel.setdefault(vid, []).append(c)

    out = []
    for v in vessels or []:
        commit = _is_commitment(v)
        if not commit:
            continue
        movement, committed_time = commit
        attached = per_vessel.get(v.get("id"), [])
        internal = "healthy"
        evidence = []
        card_worthy = False
        for c in attached:
            contrib, ev = _conflict_contribution(c, now)
            evidence.append(ev)
            internal = _worse(internal, contrib)
            if (c.get("signal_type") or "").upper() in _CARD_WORTHY_SIGNALS:
                card_worthy = True
        op_state = _OP_STATE.get(internal, "on_track")
        ct = _parse_iso(committed_time)
        out.append({
            "commitment_id": v.get("id"),
            "vessel_name": v.get("name") or v.get("vessel_name", ""),
            "berth_id": v.get("berth_id"),
            "berth_name": berth_name.get(v.get("berth_id"), v.get("berth_id")),
            "movement": movement,
            "committed_time": committed_time,
            "committed_in_mins": (int((ct - now).total_seconds() // 60) if ct else None),
            "feasibility_state": op_state,          # on_track | watch | act_now
            "state_label": OP_LABEL.get(op_state, op_state),
            "confidence_note": (DEPARTURE_CONFIDENCE_NOTE if movement == "departure" else None),
            "degradation_evidence": evidence,
            "conflict_ids": [c.get("id") for c in attached],
            "card_worthy": card_worthy,             # has an operator-relevant CONFLICT
            "provenance": (evidence[0]["provenance"] if evidence else _vessel_provenance(v)),
        })
    return out


def _vessel_provenance(v):
    _REAL = {"ais", "aisstream", "mst", "qships", "scraper", "live"}
    if str(v.get("id", "")).startswith("SIM-"):
        return "simulated"
    return "live" if v.get("source") in _REAL else "simulated"


# ── Decision Support Cards ─────────────────────────────────────────────────────
_DO_NOTHING = {
    "berth_overlap":      "Evidence indicates the berth stays committed to another movement; this arrival has no clear berth window and following movements tighten.",
    "berth_not_ready":    "Evidence indicates the berth is not released in time; the arrival waits offshore and later movements compress.",
    "pilotage_window":    "Evidence indicates pilot notice stays inside the required window for this movement.",
    "towage_resource":    "Evidence indicates the tug stays committed to another movement across the same window.",
    "eta_variance":       "Evidence indicates the reported arrival time is drifting; the committed window is uncertain.",
    "bridge_restriction": "Evidence indicates estimated air-draft stays above the bridge limit for this transit.",
}
ADVISORY_NOTE = ("Horizon projects the consequence of options you test. "
                 "The Harbour Master decides which plan to test and what to do.")


def build_decision_support_cards(commitments, conflicts, vessels, now, shadow_fn):
    by_id = {c.get("id"): c for c in (conflicts or [])}
    sev_rank = {"critical": 0, "high": 1, "medium": 2, "low": 3}
    cards = []
    for cm in commitments:
        # Only operator-relevant threats get a card; advisory-only do not.
        if cm["feasibility_state"] == "on_track" or not cm.get("card_worthy"):
            continue
        card_conflicts = [by_id[cid] for cid in cm["conflict_ids"]
                          if cid in by_id
                          and (by_id[cid].get("signal_type") or "").upper() in _CARD_WORTHY_SIGNALS]
        if not card_conflicts:
            continue
        primary = sorted(card_conflicts,
                         key=lambda c: (sev_rank.get((c.get("severity") or "").lower(), 9),
                                        c.get("conflict_time") or ""))[0]
        ctype = (primary.get("conflict_type") or "").lower()
        ds = primary.get("decision_support") or {}
        ct = _parse_iso(primary.get("conflict_time"))

        options_out = []
        for o in (ds.get("options") or []):
            options_out.append({
                "id": o.get("id"),
                "strategy": o.get("strategy"),
                "label": o.get("label"),
                "description": o.get("description"),
                # NO feasibility grade, NO cost — deliberately omitted.
                "expiry": compute_option_expiry(o.get("strategy", ""), ct, now),
                "consequence": project_consequence(primary, o, vessels, conflicts, shadow_fn),
            })

        cards.append({
            "commitment_id": cm["commitment_id"],
            "vessel_name": cm["vessel_name"],
            "berth_name": cm["berth_name"],
            "movement": cm["movement"],
            "committed_time": cm["committed_time"],
            "feasibility_state": cm["feasibility_state"],
            "state_label": cm["state_label"],
            "confidence_note": cm.get("confidence_note"),
            "provenance": cm["provenance"],
            "primary_conflict_id": primary.get("id"),
            "why_infeasible": [ev["why"] for ev in cm["degradation_evidence"] if ev.get("why")],
            "do_nothing": _DO_NOTHING.get(
                ctype,
                f"Evidence indicates {primary.get('description', 'the conflict')} continues; the committed "
                f"{cm['movement']} is under pressure if the plan is unchanged."),
            "options": options_out,
            "advisory_note": ADVISORY_NOTE,
        })
    return cards


# ── Ordering: arrival story leads ──────────────────────────────────────────────
def _sort_key(item):
    move_rank = {"arrival": 0, "departure": 1}
    state_rank = {"act_now": 0, "watch": 1, "on_track": 2}
    return (move_rank.get(item.get("movement"), 9),
            state_rank.get(item.get("feasibility_state"), 9),
            item.get("committed_time") or "")


# ── Operator plan correction (retime → re-project) ─────────────────────────────
def apply_operator_plan(vessels, commitment_id, new_eta=None, new_etd=None, new_berth=None):
    """Return a deep copy of vessels with the operator's working-plan retime
    applied to one commitment. Pure; mutates nothing. No external write-back."""
    vs = copy.deepcopy(vessels or [])
    for v in vs:
        if v.get("id") != commitment_id:
            continue
        if new_eta:
            v["eta"] = new_eta
        if new_etd:
            v["etd"] = new_etd
        if new_berth is not None and new_berth != "":
            v["berth_id"] = new_berth
        break
    return vs


# ── Top-level block builder ────────────────────────────────────────────────────
def build_beta12_block(vessels, berths, conflicts, now, shadow_fn=None,
                       freshness=None, working_plan=False, plan_label=None,
                       plan_note=None):
    commitments = build_commitments(vessels, berths, conflicts, now)
    cards = build_decision_support_cards(commitments, conflicts, vessels, now, shadow_fn)
    commitments.sort(key=_sort_key)
    cards.sort(key=_sort_key)
    counts = {"on_track": 0, "watch": 0, "act_now": 0}
    for cm in commitments:
        counts[cm["feasibility_state"]] = counts.get(cm["feasibility_state"], 0) + 1
    fresh = freshness or _default_freshness()
    stale, stale_sources = _freshness_stale(fresh, now)
    return {
        "generated_at": _fmt(now),
        "commitments": commitments,
        "state_counts": counts,
        "state_labels": OP_LABEL,
        "decision_support_cards": cards,
        "evidence_freshness": fresh,
        "freshness_stale": stale,
        "stale_sources": stale_sources,
        "stale_warning": ("Demo data freshness warning: schedule/AIS source appears stale."
                          if stale else None),
        "planning_thresholds_mins": OPTION_LEAD_TIME_MINS,
        "assumption_note": ASSUMPTION_NOTE,
        "working_plan": bool(working_plan),
        "plan_label": plan_label,
        "plan_note": plan_note,
        "frame": "The machine detects and projects. The Harbour Master proposes and decides.",
    }
