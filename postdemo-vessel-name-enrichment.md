# Post-Demo Remediation: Vessel Name Enrichment

**Status:** Deferred — frozen for Beta 10 demo stability  
**Date logged:** 2026-04-22  
**Severity:** Cosmetic (P3)  
**Affected area:** All ports using AISStream live data

---

## Issue Summary

Some berthed vessels display as `VESSEL-<MMSI>` (e.g. VESSEL-636018036) instead of their proper vessel name. This affects the Pilotage table, Guidance rail, Decision cards, and Vessel roster — every surface that renders `v["name"]`.

Simulated inbound vessels are not affected because their names are hardcoded in the `_INBOUND_NAMES` pool.

## Root Cause

AIS uses two message types. Position reports (Type 1/2/3) carry MMSI, location, and speed but no vessel name. Static data messages (Type 5/24) carry the vessel name, IMO, and dimensions but only broadcast every 6 minutes.

When AISStream receives a position report for a vessel whose static data message has not yet arrived, the scraper falls back to `VESSEL-{mmsi}` as the display name (`aisstream_scraper.py`, line 244).

This is a genuine AIS data-quality condition, not a bug in Horizon's rendering or logic.

## Preferred Fix (Post-Demo)

**Option B — MST name enrichment at read time.**

In `aisstream_scraper.get_vessels_in_port()`, when a vessel's resolved name matches the `VESSEL-{mmsi}` pattern, check the MST cache for a record with the same MMSI that has a real name. Use it if found; keep the MMSI fallback if not.

This is approximately 5 lines of read-only code in one function in one file. No state mutation, no logic change, no architectural impact.

If MST is also unavailable, the MMSI fallback remains — which is operationally correct and preferable to displaying an incorrect name.

## Why Deferred

- The issue is cosmetic, not functional — no logic depends on vessel display names.
- The platform is in demo freeze for Beta 10.
- Any code change requires a fresh deploy cycle, adding unnecessary risk.
- The condition is narratable as real-world AIS behaviour, which it is.
