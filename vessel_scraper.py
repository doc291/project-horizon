"""
Project Horizon — Vessel Movement Scraper
Scrapes public ship movements pages for configured ports.
Currently supports the Ports Victoria HTML format (Melbourne).

Filtering rules (applied before returning):
  1. Exclude vessels with status "departed"
  2. Exclude vessels with ETA/ETD more than vessel_ingest_window_hours from now
  3. Sort remaining by ETA/ETD ascending
  4. Truncate to profile["max_vessels"] entries

Cache: 30 minutes in memory per vessel_data_url.
"""

import logging
import re
import threading
import time
from datetime import datetime, timedelta, timezone

log = logging.getLogger("horizon.vessel_scraper")

# ── In-memory cache ───────────────────────────────────────────────────────────
_cache: dict = {}        # url -> {"vessels": [...], "fetched_at": float, "scraped_at": str}
_fail_cache: dict = {}   # url -> failed_at (monotonic) — suppresses retries after scrape failure
_cache_lock  = threading.Lock()
CACHE_TTL_SECS = 1800    # 30 minutes (successful fetch)
FAIL_TTL_SECS  = 1800    # 30 minutes — don't retry a failed scrape target

# ── Column mapping for Ports Victoria Melbourne HTML table ─────────────────────
# TODO: verify column order against live page before deploying to production.
# Update this dict if the column order changes without touching the parsing logic.
PV_COLUMN_MAP = {
    "vessel_name":  0,   # Vessel Name
    "voyage_type":  1,   # Voyage Type (Arrival / Departure)
    "eta_etd":      2,   # ETA / ETD (local time, various formats)
    "berth":        3,   # Berth / Terminal
    "status":       4,   # Status (Expected / Berthed / Sailed / etc.)
}

# Browser User-Agent to avoid bot blocks
_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)

# Datetime formats used by Ports Victoria (try in order)
_DATE_FORMATS = [
    "%d/%m/%Y %H:%M",
    "%d/%m/%Y %I:%M %p",
    "%d %b %Y %H:%M",
    "%d %b %Y",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M",
]


def _parse_pv_datetime(s: str, reference_tz=timezone.utc) -> datetime | None:
    """Try multiple date formats, return UTC datetime or None."""
    s = s.strip()
    if not s:
        return None
    for fmt in _DATE_FORMATS:
        try:
            dt = datetime.strptime(s, fmt)
            # Assume local time if no tzinfo — treat as UTC for now
            # (Melbourne AEDT = UTC+11, AEST = UTC+10; close enough for a demo)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    # Try ISO format as last resort
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None


def _normalise_voyage_type(raw: str) -> str:
    raw = raw.strip().lower()
    if "arriv" in raw or "inbound" in raw:
        return "arrival"
    if "depart" in raw or "outbound" in raw or "sail" in raw:
        return "departure"
    return raw


def _normalise_status(raw: str) -> str:
    raw = raw.strip().lower()
    if "berthed" in raw or "alongside" in raw:
        return "berthed"
    if "sail" in raw or "departed" in raw:
        return "departed"
    if "expect" in raw or "scheduled" in raw:
        return "scheduled"
    if "anchor" in raw:
        return "at_anchor"
    return raw or "scheduled"


def _scrape_ports_victoria(url: str, profile: dict, now: datetime) -> list:
    """
    Fetch and parse the Ports Victoria ship movements HTML table.
    Returns raw (unfiltered) list of vessel dicts.
    Raises on any HTTP or parse error.
    """
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        raise RuntimeError("beautifulsoup4 not installed — cannot scrape Ports Victoria page")

    import urllib.request

    req = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
    with urllib.request.urlopen(req, timeout=15) as resp:
        html = resp.read().decode("utf-8", errors="replace")

    soup  = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if not table:
        # Try finding a div-based table structure some PV pages use
        table = soup.find("div", class_=re.compile(r"ship.movement|vessel.movement|movement.table", re.I))
    if not table:
        raise ValueError("No vessel movements table found on page — structure may have changed")

    rows    = table.find_all("tr")
    vessels = []
    col_map = PV_COLUMN_MAP

    for row in rows[1:]:   # skip header row
        cells = row.find_all(["td", "th"])
        if len(cells) < max(col_map.values()) + 1:
            continue

        def cell(key):
            idx = col_map.get(key)
            return cells[idx].get_text(strip=True) if idx is not None and idx < len(cells) else ""

        name        = cell("vessel_name")
        voyage_raw  = cell("voyage_type")
        eta_raw     = cell("eta_etd")
        berth_raw   = cell("berth")
        status_raw  = cell("status")

        if not name:
            continue

        eta_etd  = _parse_pv_datetime(eta_raw)
        voy_type = _normalise_voyage_type(voyage_raw)
        status   = _normalise_status(status_raw)

        # Build a minimal vessel dict compatible with server.py expectations
        vid = "PV-" + re.sub(r"[^A-Z0-9]", "", name.upper())[:12]
        vessels.append({
            "id":             vid,
            "name":           name,
            "imo":            "",
            "vessel_type":    "",
            "flag":           "",
            "loa":            0.0,
            "draught":        0.0,
            "cargo_type":     "",
            "status":         status,
            "berth_id":       None,
            "berth_name":     berth_raw,
            "eta":            eta_etd.strftime("%Y-%m-%dT%H:%M:%SZ") if eta_etd else None,
            "etd":            None,
            "ata":            None,
            "atd":            None,
            "voyage_type":    voy_type,
            "pilotage_required": True,
            "towage_required":   True,
            "agent":          "",
            "notes":          "",
            "port_profile":   profile.get("short_name", ""),
            "_raw_eta_etd":   eta_raw,
        })

    log.info("Scraped %d raw vessels from %s", len(vessels), url)
    return vessels


def _apply_filters(vessels: list, profile: dict, now: datetime) -> list:
    """
    Apply the three-stage filter:
    1. Drop departed vessels
    2. Drop vessels outside the ingest window
    3. Sort and cap at max_vessels
    """
    window_hours = profile.get("vessel_ingest_window_hours", 72)
    max_v        = profile.get("max_vessels", 30)
    cutoff       = now + timedelta(hours=window_hours)

    filtered = []
    for v in vessels:
        # Stage 1: exclude departed
        if v.get("status") == "departed":
            continue

        # Stage 2: exclude outside window
        eta_s = v.get("eta")
        if eta_s:
            try:
                eta_dt = datetime.fromisoformat(eta_s.replace("Z", "+00:00"))
                if eta_dt > cutoff:
                    continue
            except ValueError:
                pass   # If we can't parse, keep it

        filtered.append(v)

    # Stage 3: sort by ETA ascending, cap
    def _sort_key(v):
        try:
            return datetime.fromisoformat((v.get("eta") or "9999-01-01T00:00:00Z").replace("Z", "+00:00"))
        except ValueError:
            return datetime.max.replace(tzinfo=timezone.utc)

    filtered.sort(key=_sort_key)
    capped = filtered[:max_v]

    log.info("Vessel filter: %d raw → %d after filters (window=%dh, cap=%d)",
             len(vessels), len(capped), window_hours, max_v)
    return capped


def fetch_vessel_movements(profile: dict, now: datetime = None) -> dict:
    """
    Public API — returns:
        {
          "vessels":             [...],   # filtered list of vessel dicts
          "using_live_data":     bool,
          "scraped_at":          str | None,
          "vessel_count":        int,
        }
    Returns using_live_data=False with empty vessels list on simulated profile
    or any scrape failure — caller in server.py must fall back to make_vessels().
    """
    if now is None:
        now = datetime.now(tz=timezone.utc).replace(microsecond=0)

    source = profile.get("vessel_data_source", "simulated")
    url    = profile.get("vessel_data_url")

    # ── Simulated profile — skip scraping ────────────────────────────────────
    if source == "simulated" or not url:
        return {
            "vessels":         [],
            "using_live_data": False,
            "scraped_at":      None,
            "vessel_count":    0,
        }

    # ── Check cache ───────────────────────────────────────────────────────────
    with _cache_lock:
        cached = _cache.get(url)
        if cached and (time.monotonic() - cached["fetched_at"]) < CACHE_TTL_SECS:
            log.debug("Vessel cache hit for %s (%d vessels)", url, len(cached["vessels"]))
            return {
                "vessels":         cached["vessels"],
                "using_live_data": True,
                "scraped_at":      cached["scraped_at"],
                "vessel_count":    len(cached["vessels"]),
            }

        # Failure cooldown — avoid hammering an unavailable endpoint every request
        failed_at = _fail_cache.get(url)
        if failed_at and (time.monotonic() - failed_at) < FAIL_TTL_SECS:
            log.debug("Vessel scrape failure cooldown active for %s — skipping", url)
            return {
                "vessels":         [],
                "using_live_data": False,
                "scraped_at":      None,
                "vessel_count":    0,
            }

    # ── Live scrape ───────────────────────────────────────────────────────────
    try:
        raw      = _scrape_ports_victoria(url, profile, now)
        filtered = _apply_filters(raw, profile, now)
        scraped_at = now.strftime("%Y-%m-%dT%H:%M:%SZ")

        with _cache_lock:
            _cache[url] = {
                "vessels":    filtered,
                "fetched_at": time.monotonic(),
                "scraped_at": scraped_at,
            }
            _fail_cache.pop(url, None)   # clear any prior failure

        return {
            "vessels":         filtered,
            "using_live_data": True,
            "scraped_at":      scraped_at,
            "vessel_count":    len(filtered),
        }

    except Exception as exc:
        log.error("Vessel scrape failed for %s: %s — falling back to simulation", url, exc)
        with _cache_lock:
            _fail_cache[url] = time.monotonic()
        return {
            "vessels":         [],
            "using_live_data": False,
            "scraped_at":      None,
            "vessel_count":    0,
        }
