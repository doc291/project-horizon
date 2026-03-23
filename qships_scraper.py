#!/usr/bin/env python3
"""
QShips Scraper — Project Horizon Beta 6
Scrapes live vessel movement data from the QShips public website for the Port of Brisbane.
Writes results to qships_data.json in the same directory as this file.

Strategy:
  1. Try requests + BeautifulSoup (no browser required — works on Railway)
  2. Fall back to Playwright headless browser if requests approach gets no table rows

Usage:
    python3 qships_scraper.py

Dependencies:
    beautifulsoup4 (pip install beautifulsoup4)
    requests (stdlib urllib fallback if requests not installed)
    playwright (optional — pip install playwright && playwright install chromium)
"""

import hashlib
import json
import logging
import re
import sys
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [qships] %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger("qships")

OUTPUT_FILE = Path(__file__).parent / "qships_data.json"

QSHIPS_URL   = "https://qships.tmr.qld.gov.au/webx/"
USER_AGENT   = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

# AEST is UTC+10, no DST
AEST_OFFSET = timedelta(hours=10)

# Draught estimates by vessel type (QShips public data doesn't include draught)
DRAUGHT_ESTIMATES = {
    "Tanker":           11.0,
    "Bulk Carrier":     10.5,
    "Container":        11.5,
    "Vehicles Carrier": 8.0,
    "General Cargo":    7.5,
    "Passenger":        7.0,
}
DRAUGHT_DEFAULT = 9.0

# Terminal name mapping from berth/location substrings
TERMINAL_MAP = [
    ("Fisherman Island",    "Fisherman Islands Terminal"),
    ("DBCT",                "Dalrymple Bay Coal Terminal"),
    ("BP",                  "BP Terminal"),
    ("Pinkenba",            "Pinkenba Terminal"),
]

# QShips status → Horizon VesselStatus mapping
QSTATUS_MAP = {
    "PLAN": "scheduled",
    "SCHD": "scheduled",
    "CONF": "confirmed",
    "ACTV": "berthed",
    "COMP": "berthed",
    "RELS": "departed",
    "INVC": "departed",
}


# ── Field transformation helpers ──────────────────────────────────────────────

def _parse_aest_to_utc(raw: str):
    raw = raw.strip()
    if not raw:
        return None
    for fmt in ("%d-%m-%y %H:%M", "%d-%m-%Y %H:%M", "%d/%m/%Y %H:%M", "%d/%m/%y %H:%M"):
        try:
            dt_aest = datetime.strptime(raw, fmt)
            dt_utc = dt_aest - AEST_OFFSET
            return dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            pass
    return None


def _stable_id(name: str, eta_raw: str) -> str:
    return hashlib.md5(f"{name}-{eta_raw}".encode()).hexdigest()[:12]


def _draught_for_type(vessel_type: str) -> float:
    for key, val in DRAUGHT_ESTIMATES.items():
        if key.lower() in vessel_type.lower():
            return val
    return DRAUGHT_DEFAULT


def _terminal_for_location(location: str) -> str:
    for fragment, terminal in TERMINAL_MAP:
        if fragment.lower() in location.lower():
            return terminal
    return "Brisbane Port"


def _parse_loa(raw: str) -> float:
    cleaned = re.sub(r"[^\d.]", "", raw)
    try:
        return float(cleaned)
    except (ValueError, TypeError):
        return 0.0


def _map_status(qstatus: str, direction: str) -> str:
    base = QSTATUS_MAP.get(qstatus.upper(), "scheduled")
    if base == "berthed" and direction in ("DEP", "REM"):
        return "departed"
    return base


def _is_at_risk(qstatus: str, eta_utc_str, now_utc: datetime) -> bool:
    if qstatus.upper() not in ("PLAN", "SCHD"):
        return False
    if not eta_utc_str:
        return False
    try:
        eta_dt = datetime.fromisoformat(eta_utc_str.replace("Z", "+00:00"))
        return 0 <= (eta_dt - now_utc).total_seconds() / 3600 <= 6
    except (ValueError, TypeError):
        return False


def _parse_rows(rows) -> list:
    movements = []
    for row in rows:
        cells = row.find_all(["td", "th"])
        if len(cells) < 2:
            continue
        texts = [c.get_text(strip=True) for c in cells]
        if len(texts) < 10:
            continue
        if texts[0].upper() in ("JOB TYPE", "TYPE", "MOVEMENT", "JOB"):
            continue
        try:
            movements.append({
                "job_type":      texts[0].strip(),
                "ship":          texts[1].strip(),
                "ship_type":     texts[2].strip(),
                "loa_raw":       texts[3].strip(),
                "agency":        texts[4].strip(),
                "start_time":    texts[5].strip(),
                "end_time":      texts[6].strip(),
                "from_location": texts[7].strip(),
                "to_location":   texts[8].strip(),
                "status_raw":    texts[9].strip(),
                "last_port":     texts[10].strip() if len(texts) > 10 else "",
                "next_port":     texts[11].strip() if len(texts) > 11 else "",
                "voyage_number": texts[12].strip() if len(texts) > 12 else "",
            })
        except (IndexError, AttributeError):
            continue
    return movements


def _transform_movements(movements: list, now_utc: datetime) -> list:
    vessels = []
    for m in movements:
        name      = m["ship"]
        eta_raw   = m["start_time"]
        etd_raw   = m["end_time"]
        direction = m["job_type"].upper()[:3]
        qstatus   = m["status_raw"].upper()
        vessel_type = m["ship_type"]
        loa       = _parse_loa(m["loa_raw"])
        eta_utc   = _parse_aest_to_utc(eta_raw)
        etd_utc   = _parse_aest_to_utc(etd_raw)
        status    = _map_status(qstatus, direction)
        at_risk   = _is_at_risk(qstatus, eta_utc, now_utc)
        if at_risk:
            status = "at_risk"
        if status == "departed":
            continue
        draught  = _draught_for_type(vessel_type)
        vid      = _stable_id(name, eta_raw)
        berth_id = m["to_location"] if direction in ("ARR", "EXT") else m["from_location"]
        vessels.append({
            "id":                vid,
            "name":              name,
            "imo":               "unknown",
            "vessel_type":       vessel_type,
            "flag":              "unknown",
            "loa":               loa,
            "draught":           draught,
            "cargo_type":        vessel_type,
            "status":            status,
            "berth_id":          berth_id,
            "eta":               eta_utc or "",
            "etd":               etd_utc or "",
            "ata":               eta_utc if status == "berthed" else None,
            "atd":               None,
            "pilotage_required": True,
            "towage_required":   loa > 150,
            "agent":             m["agency"],
            "notes":             "Draught estimated — QShips public data",
            "qships_status":     qstatus,
            "movement_direction": direction,
            "from_location":     m["from_location"],
            "last_port":         m["last_port"],
            "next_port":         m["next_port"],
            "voyage_number":     m["voyage_number"],
        })
    return vessels


def _build_berths(vessels: list) -> list:
    berth_ids = set(v["berth_id"] for v in vessels if v.get("berth_id"))
    berths = []
    for bid in sorted(berth_ids):
        if not bid:
            continue
        occupying = [v for v in vessels if v.get("berth_id") == bid and v["status"] == "berthed"]
        reserved  = [v for v in vessels if v.get("berth_id") == bid
                     and v["status"] in ("confirmed", "scheduled", "at_risk")]
        if occupying:
            status = "occupied"
            readiness = max((v["etd"] for v in occupying if v.get("etd")), default=None)
        elif reserved:
            status = "reserved"
            readiness = None
        else:
            status = "available"
            readiness = None
        berths.append({
            "id":             bid,
            "name":           bid,
            "terminal":       _terminal_for_location(bid),
            "max_loa":        350.0,
            "max_draught":    14.5,
            "lat_depth_m":    13.0,
            "status":         status,
            "readiness_time": readiness,
            "crane_count":    0,
            "notes":          None,
        })
    return berths


# ── Strategy 1: requests (no browser) ─────────────────────────────────────────

def _scrape_with_requests(soup_parser) -> list:
    """
    Attempt to fetch QShips using requests (or urllib) and parse the HTML directly.
    Returns list of raw movement dicts, or empty list if the approach fails.
    """
    try:
        import requests as req_lib
        log.info("Fetching QShips with requests")
        session = req_lib.Session()
        session.headers.update({"User-Agent": USER_AGENT})
        # Initial page load
        resp = session.get(QSHIPS_URL, timeout=30)
        resp.raise_for_status()
        html = resp.text
    except ImportError:
        log.info("requests not available, using urllib")
        try:
            req = urllib.request.Request(QSHIPS_URL, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=30) as r:
                html = r.read().decode("utf-8", errors="replace")
        except Exception as e:
            log.warning("urllib fetch failed: %s", e)
            return []
    except Exception as e:
        log.warning("requests fetch failed: %s", e)
        return []

    try:
        from bs4 import BeautifulSoup
    except ImportError:
        log.error("beautifulsoup4 not installed")
        return []

    soup = BeautifulSoup(html, "html.parser")

    # Look for any table on the page
    tables = soup.find_all("table")
    log.info("requests approach: found %d tables", len(tables))

    all_rows = []
    for table in tables:
        tbody = table.find("tbody")
        rows  = tbody.find_all("tr") if tbody else table.find_all("tr")[1:]
        parsed = _parse_rows(rows)
        if parsed:
            all_rows.extend(parsed)
            log.info("Parsed %d rows from table", len(parsed))

    return all_rows


# ── Strategy 2: Playwright (full headless browser) ────────────────────────────

def _scrape_with_playwright() -> list:
    """
    Full Playwright headless browser scrape.
    Falls back when requests approach returns no rows (JS-rendered content).
    """
    try:
        from playwright.sync_api import sync_playwright
        from bs4 import BeautifulSoup
    except ImportError as e:
        log.error("Playwright not available: %s", e)
        return []

    log.info("Falling back to Playwright headless browser")
    all_rows = []

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage"],
            )
            context = browser.new_context(user_agent=USER_AGENT)
            page = context.new_page()

            log.info("Playwright: navigating to QShips")
            page.goto(QSHIPS_URL, wait_until="networkidle", timeout=60000)

            # Click Ship Movements tab
            try:
                page.get_by_text("Ship Movements").first.click()
                page.wait_for_load_state("networkidle", timeout=15000)
            except Exception as e:
                log.warning("Could not click Ship Movements tab: %s", e)

            # Select Brisbane from port dropdown
            for sel in ["select[name*='port']", "select[id*='port']", "#portSelect"]:
                try:
                    page.select_option(sel, label="Brisbane", timeout=3000)
                    page.wait_for_load_state("networkidle", timeout=10000)
                    break
                except Exception:
                    pass

            # Click Next 7 Days filter
            for label in ["Next 7 Days", "7 Days", "Next 7"]:
                try:
                    page.get_by_text(label).first.click(timeout=3000)
                    page.wait_for_load_state("networkidle", timeout=10000)
                    break
                except Exception:
                    pass

            # Set Show entries to maximum
            for count in [500, 200, 100]:
                for sel in ["select[name='DataTables_Table_0_length']",
                            "select[name*='length']",
                            ".dataTables_length select"]:
                    try:
                        page.select_option(sel, str(count), timeout=3000)
                        page.wait_for_load_state("networkidle", timeout=10000)
                        break
                    except Exception:
                        pass
                break

            page_num = 1
            while True:
                html = page.content()
                soup = BeautifulSoup(html, "html.parser")
                table = (soup.find("table", id="DataTables_Table_0")
                         or soup.find("table", class_=re.compile("dataTable|movements", re.I))
                         or soup.find("table"))
                if not table:
                    log.warning("No table on page %d", page_num)
                    break
                tbody = table.find("tbody")
                rows  = tbody.find_all("tr") if tbody else table.find_all("tr")[1:]
                page_rows = _parse_rows(rows)
                all_rows.extend(page_rows)
                log.info("Page %d: %d rows", page_num, len(page_rows))
                try:
                    nxt = page.locator("#DataTables_Table_0_next:not(.disabled)").first
                    cls = nxt.get_attribute("class", timeout=2000) or ""
                    if nxt.is_visible(timeout=2000) and "disabled" not in cls:
                        nxt.click()
                        page.wait_for_load_state("networkidle", timeout=10000)
                        page_num += 1
                    else:
                        break
                except Exception:
                    break

            browser.close()
    except Exception as e:
        log.error("Playwright scrape failed: %s", e, exc_info=True)

    return all_rows


# ── Main entry point ───────────────────────────────────────────────────────────

def run_scrape() -> bool:
    """
    Run scrape. Returns True on success, False on failure.
    Never overwrites qships_data.json with a failed/empty result.
    """
    log.info("Starting QShips scrape — Port of Brisbane")
    now_utc = datetime.now(tz=timezone.utc).replace(microsecond=0)

    # Try lightweight requests approach first
    raw_rows = _scrape_with_requests(None)

    # If requests returned nothing, try Playwright
    if not raw_rows:
        log.info("requests approach returned no rows — trying Playwright")
        raw_rows = _scrape_with_playwright()

    if not raw_rows:
        log.error("Both scrape strategies returned no rows — aborting")
        return False

    vessels = _transform_movements(raw_rows, now_utc)
    berths  = _build_berths(vessels)

    log.info("Transformed: %d vessels, %d berths", len(vessels), len(berths))

    if not vessels:
        log.error("No vessels after transformation — aborting write")
        return False

    output = {
        "scraped_at":    now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "scrape_source": "QShips — Maritime Safety Queensland — Public Data",
        "port":          "Brisbane",
        "port_name":     "Port of Brisbane",
        "data_type":     "live",
        "vessel_count":  len(vessels),
        "berth_count":   len(berths),
        "vessels":       vessels,
        "berths":        berths,
    }

    OUTPUT_FILE.write_text(json.dumps(output, indent=2, default=str), encoding="utf-8")
    log.info("Written to %s", OUTPUT_FILE)
    return True


if __name__ == "__main__":
    success = run_scrape()
    sys.exit(0 if success else 1)
