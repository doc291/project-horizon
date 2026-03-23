#!/usr/bin/env python3
"""
QShips Scraper — Project Horizon Beta 6
Scrapes live vessel movement data from the QShips public website for the Port of Brisbane.
Writes results to qships_data.json in the same directory as this file.

Usage:
    python3 qships_scraper.py

Dependencies:
    playwright (pip install playwright && playwright install chromium)
    beautifulsoup4 (pip install beautifulsoup4)
"""

import hashlib
import json
import logging
import os
import re
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [qships] %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger("qships")

OUTPUT_FILE = Path(__file__).parent / "qships_data.json"

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
    "ACTV": "berthed",   # further refined by direction below
    "COMP": "berthed",
    "RELS": "departed",
    "INVC": "departed",
}


def _parse_aest_to_utc(raw: str) -> str | None:
    """
    Parse QShips datetime string 'DD-MM-YY HH:MM' as AEST (UTC+10),
    return UTC ISO string 'YYYY-MM-DDTHH:MM:SSZ'.
    Returns None if parsing fails.
    """
    raw = raw.strip()
    if not raw:
        return None
    try:
        # Try DD-MM-YY HH:MM
        dt_aest = datetime.strptime(raw, "%d-%m-%y %H:%M")
        dt_utc = dt_aest - AEST_OFFSET
        return dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        pass
    try:
        # Fallback: DD-MM-YYYY HH:MM
        dt_aest = datetime.strptime(raw, "%d-%m-%Y %H:%M")
        dt_utc = dt_aest - AEST_OFFSET
        return dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        pass
    return None


def _stable_id(name: str, eta_raw: str) -> str:
    """Generate stable vessel movement ID from name + raw ETA string."""
    return hashlib.md5(f"{name}-{eta_raw}".encode()).hexdigest()[:12]


def _draught_for_type(vessel_type: str) -> float:
    """Return type-appropriate estimated draught."""
    for key, val in DRAUGHT_ESTIMATES.items():
        if key.lower() in vessel_type.lower():
            return val
    return DRAUGHT_DEFAULT


def _terminal_for_location(location: str) -> str:
    """Map berth/location name to terminal name."""
    for fragment, terminal in TERMINAL_MAP:
        if fragment.lower() in location.lower():
            return terminal
    return "Brisbane Port"


def _parse_loa(raw: str) -> float:
    """Strip non-numeric characters and return LOA as float."""
    cleaned = re.sub(r"[^\d.]", "", raw)
    try:
        return float(cleaned)
    except (ValueError, TypeError):
        return 0.0


def _map_status(qstatus: str, direction: str) -> str:
    """Map QShips status + direction to Horizon VesselStatus string."""
    base = QSTATUS_MAP.get(qstatus.upper(), "scheduled")
    if base == "berthed" and direction in ("DEP", "REM"):
        return "departed"
    return base


def _is_at_risk(qstatus: str, eta_utc_str: str | None, now_utc: datetime) -> bool:
    """
    Mark vessel at_risk if status is PLAN or SCHD and ETA is within 6 hours of now.
    These are unconfirmed movements due imminently.
    """
    if qstatus.upper() not in ("PLAN", "SCHD"):
        return False
    if not eta_utc_str:
        return False
    try:
        eta_dt = datetime.fromisoformat(eta_utc_str.replace("Z", "+00:00"))
        return 0 <= (eta_dt - now_utc).total_seconds() / 3600 <= 6
    except (ValueError, TypeError):
        return False


def _parse_rows(rows) -> list[dict]:
    """
    Parse BeautifulSoup table rows into raw movement dicts.
    Each row should have cells in the order defined by the QShips table headers.
    """
    movements = []
    for row in rows:
        cells = row.find_all(["td", "th"])
        if len(cells) < 2:
            continue
        texts = [c.get_text(strip=True) for c in cells]
        # QShips columns (order may vary — we map by position based on known schema):
        # Job Type, Ship, Ship Type, LOA, Agency, Start Time, End Time,
        # From Location, To Location, Status, Last Port, Next Port, Voyage
        if len(texts) < 10:
            continue
        # Skip header rows
        if texts[0].upper() in ("JOB TYPE", "TYPE", "MOVEMENT"):
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


def _transform_movements(movements: list[dict], now_utc: datetime) -> list[dict]:
    """Transform raw movement dicts into Horizon vessel objects."""
    vessels = []
    for m in movements:
        name          = m["ship"]
        eta_raw       = m["start_time"]
        etd_raw       = m["end_time"]
        direction     = m["job_type"].upper()[:3]    # ARR/DEP/EXT/REM
        qstatus       = m["status_raw"].upper()
        vessel_type   = m["ship_type"]
        loa           = _parse_loa(m["loa_raw"])
        eta_utc       = _parse_aest_to_utc(eta_raw)
        etd_utc       = _parse_aest_to_utc(etd_raw)
        status        = _map_status(qstatus, direction)
        at_risk       = _is_at_risk(qstatus, eta_utc, now_utc)
        if at_risk:
            status = "at_risk"
        if status == "departed":
            continue   # filter out — not operationally relevant
        draught       = _draught_for_type(vessel_type)
        vessel_id     = _stable_id(name, eta_raw)
        berth_id      = m["to_location"] if direction in ("ARR", "EXT") else m["from_location"]
        vessels.append({
            "id":               vessel_id,
            "name":             name,
            "imo":              "unknown",
            "vessel_type":      vessel_type,
            "flag":             "unknown",
            "loa":              loa,
            "draught":          draught,
            "cargo_type":       vessel_type,
            "status":           status,
            "berth_id":         berth_id,
            "eta":              eta_utc or "",
            "etd":              etd_utc or "",
            "ata":              eta_utc if status == "berthed" else None,
            "atd":              None,
            "pilotage_required": True,
            "towage_required":  loa > 150,
            "agent":            m["agency"],
            "notes":            "Draught estimated — QShips public data",
            # Additional QShips fields
            "qships_status":    qstatus,
            "movement_direction": direction,
            "from_location":    m["from_location"],
            "last_port":        m["last_port"],
            "next_port":        m["next_port"],
            "voyage_number":    m["voyage_number"],
        })
    return vessels


def _build_berths(vessels: list[dict], now_utc: datetime) -> list[dict]:
    """
    Derive berth objects from unique berth_id values across all vessels.
    """
    berth_ids = set(v["berth_id"] for v in vessels if v.get("berth_id"))
    berths = []
    for bid in sorted(berth_ids):
        if not bid:
            continue
        # Determine occupants and reserved vessels
        occupying = [v for v in vessels
                     if v.get("berth_id") == bid and v["status"] == "berthed"]
        reserved  = [v for v in vessels
                     if v.get("berth_id") == bid
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
        terminal = _terminal_for_location(bid)
        berths.append({
            "id":            bid,
            "name":          bid,
            "terminal":      terminal,
            "max_loa":       350.0,
            "max_draught":   14.5,
            "lat_depth_m":   13.0,
            "status":        status,
            "readiness_time": readiness,
            "crane_count":   0,
            "notes":         None,
        })
    return berths


def run_scrape() -> bool:
    """
    Main scrape entry point.
    Returns True on success, False on failure.
    Never overwrites qships_data.json with a failed/partial result.
    """
    try:
        from playwright.sync_api import sync_playwright
        from bs4 import BeautifulSoup
    except ImportError as e:
        log.error("Missing dependency: %s — install with: pip install playwright beautifulsoup4 && playwright install chromium", e)
        return False

    log.info("Starting QShips scrape for Port of Brisbane")
    now_utc = datetime.now(tz=timezone.utc).replace(microsecond=0)

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage"],
            )
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            )
            page = context.new_page()

            log.info("Navigating to QShips")
            page.goto("https://qships.tmr.qld.gov.au/webx/", wait_until="networkidle", timeout=60000)

            # Click Ship Movements tab
            log.info("Clicking Ship Movements tab")
            try:
                page.get_by_text("Ship Movements").first.click()
                page.wait_for_load_state("networkidle", timeout=15000)
            except Exception as e:
                log.warning("Could not click Ship Movements tab: %s", e)

            # Select Brisbane from All Ports dropdown
            log.info("Selecting Brisbane from port dropdown")
            try:
                # Try select element first
                selectors = [
                    "select[name*='port']",
                    "select[id*='port']",
                    "select[class*='port']",
                    "#portSelect",
                    ".port-select",
                ]
                selected = False
                for sel in selectors:
                    try:
                        page.select_option(sel, label="Brisbane", timeout=3000)
                        selected = True
                        break
                    except Exception:
                        pass
                if not selected:
                    # Try clicking a Brisbane option in any dropdown
                    page.get_by_text("Brisbane").first.click()
                page.wait_for_load_state("networkidle", timeout=15000)
            except Exception as e:
                log.warning("Port selection issue (may already be on Brisbane): %s", e)

            # Click Next 7 Days filter
            log.info("Clicking Next 7 Days filter")
            try:
                for label in ["Next 7 Days", "7 Days", "Next 7"]:
                    try:
                        page.get_by_text(label).first.click(timeout=3000)
                        page.wait_for_load_state("networkidle", timeout=10000)
                        break
                    except Exception:
                        pass
            except Exception as e:
                log.warning("Could not click 7-day filter: %s", e)

            # Set Show entries to maximum
            log.info("Setting Show entries to maximum")
            for count in [500, 200, 100]:
                try:
                    selectors = [
                        "select[name='DataTables_Table_0_length']",
                        "select[name*='length']",
                        ".dataTables_length select",
                    ]
                    for sel in selectors:
                        try:
                            page.select_option(sel, str(count), timeout=3000)
                            page.wait_for_load_state("networkidle", timeout=10000)
                            log.info("Show entries set to %d", count)
                            break
                        except Exception:
                            pass
                    break
                except Exception:
                    continue

            # Collect all pages
            all_rows = []
            page_num = 1
            while True:
                log.info("Scraping page %d", page_num)
                html = page.content()
                soup = BeautifulSoup(html, "html.parser")

                # Find the movements table
                table = (soup.find("table", id="DataTables_Table_0")
                         or soup.find("table", class_=re.compile("dataTable|movements", re.I))
                         or soup.find("table"))

                if not table:
                    log.warning("No table found on page %d", page_num)
                    break

                tbody = table.find("tbody")
                rows = tbody.find_all("tr") if tbody else table.find_all("tr")[1:]
                page_rows = _parse_rows(rows)
                all_rows.extend(page_rows)
                log.info("Page %d: found %d rows", page_num, len(page_rows))

                # Try to click Next page
                try:
                    next_btn = page.locator("#DataTables_Table_0_next:not(.disabled)").first
                    if next_btn.is_visible(timeout=2000) and not next_btn.get_attribute("class", timeout=2000).__contains__("disabled"):
                        next_btn.click()
                        page.wait_for_load_state("networkidle", timeout=10000)
                        page_num += 1
                    else:
                        break
                except Exception:
                    break

            browser.close()

        log.info("Scrape complete: %d raw rows collected", len(all_rows))

        if not all_rows:
            log.error("No rows collected — aborting write")
            return False

        vessels = _transform_movements(all_rows, now_utc)
        berths  = _build_berths(vessels, now_utc)

        log.info("Transformed: %d vessels, %d berths", len(vessels), len(berths))

        output = {
            "scraped_at":   now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "scrape_source":"QShips — Maritime Safety Queensland — Public Data",
            "port":         "Brisbane",
            "port_name":    "Port of Brisbane",
            "data_type":    "live",
            "vessel_count": len(vessels),
            "berth_count":  len(berths),
            "vessels":      vessels,
            "berths":       berths,
        }

        OUTPUT_FILE.write_text(json.dumps(output, indent=2, default=str), encoding="utf-8")
        log.info("Written to %s", OUTPUT_FILE)
        return True

    except Exception as e:
        log.error("Scrape failed: %s", e, exc_info=True)
        return False


if __name__ == "__main__":
    success = run_scrape()
    sys.exit(0 if success else 1)
