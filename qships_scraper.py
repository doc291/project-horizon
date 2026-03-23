"""
qships_scraper.py — Project Horizon Beta 6

Scrapes live vessel movements from the QShips public JSON API (GetDataX).
No browser/Playwright required — plain requests + JSON parsing.

API discovered via browser DevTools:
  POST https://qships.tmr.qld.gov.au/webx/services/wxdata.svc/GetDataX
  reportCode: MSQ-WEB-0001, filterName: "Next 7 days", DOMAIN_ID: -1
"""

import json
import hashlib
import logging
import threading
from datetime import datetime, timezone, timedelta
from pathlib import Path

log = logging.getLogger(__name__)

OUTPUT_FILE = Path(__file__).parent / "qships_data.json"
DEBUG_FILE  = Path(__file__).parent / "qships_debug.json"

BASE_URL    = "https://qships.tmr.qld.gov.au"
MAIN_URL    = f"{BASE_URL}/webx/"
API_URL     = f"{BASE_URL}/webx/services/wxdata.svc/GetDataX"

USER_AGENT  = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

AEST = timezone(timedelta(hours=10))

API_PAYLOAD = {
    "token": None,
    "reportCode": "MSQ-WEB-0001",
    "dataSource": None,
    "filterName": "Next 7 days",
    "parameters": [
        {
            "__type": "ParameterValueDTO:#WebX.Core.DTO",
            "sName": "DOMAIN_ID",
            "iValueType": 0,
            "aoValues": [{"Value": -1}]
        }
    ],
    "metaVersion": 0,
}

VESSEL_TYPE_DRAUGHT = {
    "tanker":       11.0,
    "bulk carrier": 10.5,
    "container":    11.5,
    "general cargo": 9.0,
    "ro-ro":         8.5,
    "passenger":     8.0,
    "tug":           4.0,
    "barge":         3.5,
}

QSTATUS_MAP = {
    "PLAN": "scheduled",
    "SCHD": "scheduled",
    "CONF": "confirmed",
    "ACTV": "berthed",
    "COMP": "berthed",
    "RELS": "departed",
    "INVC": "departed",
}

_scrape_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _stable_id(name: str, eta_raw: str) -> str:
    return hashlib.md5(f"{name}-{eta_raw}".encode()).hexdigest()[:12]


def _parse_aest_to_utc(date_str: str):
    """Parse multiple common date formats from QShips and convert AEST -> UTC."""
    if not date_str:
        return None
    date_str = date_str.strip()

    # .NET /Date(ms)/ format
    if date_str.startswith("/Date("):
        try:
            ms = int(date_str[6:date_str.index(")")])
            dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
            return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        except Exception:
            pass

    formats = [
        "%d-%m-%y %H:%M",
        "%d/%m/%Y %H:%M",
        "%d-%m-%Y %H:%M",
        "%d/%m/%y %H:%M",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=AEST)
            return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            continue

    log.warning("Could not parse date: %r", date_str)
    return None


def _map_status(raw: str) -> str:
    if not raw:
        return "scheduled"
    return QSTATUS_MAP.get(raw.strip().upper(), "scheduled")


def _est_draught(vessel_type: str) -> float:
    if not vessel_type:
        return 9.0
    vt = vessel_type.lower()
    for key, val in VESSEL_TYPE_DRAUGHT.items():
        if key in vt:
            return val
    return 9.0


def _is_at_risk(eta_utc) -> bool:
    if not eta_utc:
        return False
    try:
        eta = datetime.strptime(eta_utc, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        diff = abs((eta - datetime.now(timezone.utc)).total_seconds())
        return diff < 7200
    except Exception:
        return False


def _find_col(row: dict, *candidates) -> str:
    """Case-insensitive key lookup across candidate column names."""
    row_lower = {k.lower(): v for k, v in row.items()}
    for c in candidates:
        val = row_lower.get(c.lower())
        if val is not None:
            return str(val).strip()
    return ""


# ---------------------------------------------------------------------------
# API scraper
# ---------------------------------------------------------------------------

def _scrape_with_api() -> list:
    """
    POST to QShips GetDataX API and return a list of raw row dicts.
    """
    try:
        import requests as req_lib
    except ImportError:
        log.error("requests library not available")
        return []

    session = req_lib.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    # Step 1: GET main page to acquire ASP.NET session cookie
    try:
        log.info("Fetching QShips main page to establish session...")
        r = session.get(MAIN_URL, timeout=30)
        log.info("Main page HTTP %d  (cookies: %s)",
                 r.status_code, list(session.cookies.keys()))
    except Exception as exc:
        log.warning("Main page GET failed (will try API anyway): %s", exc)

    # Step 2: POST to GetDataX
    session.headers.update({
        "Accept":           "application/json, text/javascript, */*; q=0.01",
        "Content-Type":     "application/json; charset=UTF-8",
        "Origin":           BASE_URL,
        "Referer":          MAIN_URL,
        "X-Requested-With": "XMLHttpRequest",
    })

    try:
        log.info("Calling QShips GetDataX API...")
        resp = session.post(
            API_URL,
            data=json.dumps(API_PAYLOAD),
            timeout=45,
        )
        log.info("GetDataX HTTP %d  (%.1f KB)",
                 resp.status_code, len(resp.content) / 1024)
        resp.raise_for_status()
    except Exception as exc:
        log.error("GetDataX API request failed: %s", exc)
        return []

    # Step 3: Parse JSON
    try:
        payload = resp.json()
    except Exception as exc:
        log.error("JSON decode failed: %s", exc)
        log.debug("Raw response (first 500): %s", resp.text[:500])
        return []

    # Step 4: Unwrap WCF envelope — handles 'd' wrapper and GetDataXResult
    result = payload
    for key in ("d", "GetDataXResult"):
        if isinstance(payload, dict) and key in payload:
            result = payload[key]
            break

    # Step 5: Extract columns + rows
    # Response structure: d.Tables = [ { Columns: [...], Rows: [...] }, ... ]
    columns  = []
    rows_raw = []

    def _extract_table(table: dict):
        """Pull columns + rows out of one WebX DataTableDTO object.

        Structure:
          table["MetaData"]["Columns"]  — column definitions
          table["Data"]                 — list of row value-arrays
        """
        t_cols = []

        # Columns live inside MetaData
        meta     = table.get("MetaData") or {}
        col_data = meta.get("Columns") or meta.get("columns") or []
        for col in col_data:
            if isinstance(col, dict):
                name = (col.get("sName") or col.get("Name") or
                        col.get("name") or col.get("ColumnName") or "")
                t_cols.append(str(name))
            elif isinstance(col, str):
                t_cols.append(col)

        # Rows live in Data (each row is a value-array)
        t_rows   = []
        row_data = table.get("Data") or table.get("data") or []
        for row in row_data:
            if isinstance(row, list):
                if t_cols:
                    t_rows.append(dict(zip(t_cols, row)))
                else:
                    t_rows.append({str(i): v for i, v in enumerate(row)})
            elif isinstance(row, dict):
                # Already a named dict — use as-is
                t_rows.append(row)
        return t_cols, t_rows

    if isinstance(result, dict):
        tables = result.get("Tables") or result.get("tables") or []
        if tables:
            # Multi-table response — take first table with rows
            for table in tables:
                t_cols, t_rows = _extract_table(table)
                if t_rows:
                    columns  = t_cols
                    rows_raw = t_rows
                    break
            if not rows_raw and tables:
                # No table had rows — still capture columns from first table for debug
                columns, _ = _extract_table(tables[0])
        else:
            # Flat structure — Columns/Rows directly on result
            columns, rows_raw = _extract_table(result)

    elif isinstance(result, list):
        rows_raw = [r for r in result if isinstance(r, dict)]

    log.info("Extracted %d raw rows from API response", len(rows_raw))
    if rows_raw:
        log.debug("Sample row keys: %s", list(rows_raw[0].keys()))

    # Save compact debug summary — always valid JSON, never truncated mid-object
    try:
        tables = result.get("Tables") or [] if isinstance(result, dict) else []
        first_table_keys = list(tables[0].keys()) if tables else []
        # Peek at all values of the first table object (key + type + length if iterable)
        first_table_peek = {}
        if tables:
            for k, v in tables[0].items():
                if isinstance(v, list):
                    first_table_peek[k] = f"list({len(v)})"
                elif isinstance(v, dict):
                    first_table_peek[k] = f"dict({list(v.keys())})"
                else:
                    first_table_peek[k] = repr(v)[:120]
        debug_summary = {
            "http_status":        resp.status_code,
            "response_bytes":     len(resp.content),
            "top_level_keys":     list(payload.keys()) if isinstance(payload, dict) else str(type(payload)),
            "result_keys":        list(result.keys()) if isinstance(result, dict) else None,
            "table_count":        len(tables),
            "first_table_keys":   first_table_keys,
            "first_table_peek":   first_table_peek,
            "columns":            columns,
            "row_count":          len(rows_raw),
            "sample_rows":        rows_raw[:3],
        }
        DEBUG_FILE.write_text(json.dumps(debug_summary, indent=2, default=str), encoding="utf-8")
        log.info("Debug summary saved to %s (%d cols, %d rows)", DEBUG_FILE, len(columns), len(rows_raw))
    except Exception as e:
        log.warning("Could not save debug summary: %s", e)

    return rows_raw


# ---------------------------------------------------------------------------
# Data transformation
# ---------------------------------------------------------------------------

COMPLETED_STATUSES = {"RELS", "INVC", "COMP"}

def _transform_movements(raw_rows: list, now_utc: datetime) -> list:
    vessels = []
    seen    = set()
    window  = timedelta(days=7)

    for row in raw_rows:
        # ── Skip completed movements — they bloat the list and tank performance
        status_code = _find_col(row, "STATUS_TYPE_CODE", "STATUS", "Status")
        if status_code.upper() in COMPLETED_STATUSES:
            continue

        name = _find_col(
            row,
            "VESSEL_NAME", "VesselName", "Vessel", "VESSEL",
            "SHIP_NAME", "ShipName",
        )
        if not name:
            continue

        # ── Time: QShips uses START_TIME in /Date(ms+tz)/ format
        eta_raw = _find_col(row, "START_TIME", "ETA", "ATA", "ARRIVAL_DATE", "Arrival")

        # ── Skip movements outside the next 7-day window
        eta_utc = _parse_aest_to_utc(eta_raw) if eta_raw else None
        if eta_utc:
            try:
                eta_dt = datetime.strptime(eta_utc, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
                if eta_dt < now_utc - timedelta(hours=2) or eta_dt > now_utc + window:
                    continue
            except Exception:
                pass

        # ── Vessel type
        vtype = _find_col(
            row,
            "MSQ_SHIP_TYPE", "VESSEL_TYPE", "VesselType",
            "Ship_Type", "ShipType",
        )

        # ── Berth: for arrivals use TO_LOCATION, for departures use FROM_LOCATION
        job = _find_col(row, "JOB_TYPE_CODE", "JOB_TYPE")
        if job.upper() in ("ARR", "EXT"):
            berth = _find_col(row, "TO_LOCATION_NAME", "BERTH", "BERTH_NAME", "Terminal")
        else:
            berth = _find_col(row, "FROM_LOCATION_NAME", "BERTH", "BERTH_NAME", "Terminal")

        # Skip movements that are purely sea/anchorage with no port berth
        if berth.upper() in ("SEA", ""):
            berth = _find_col(row, "TO_LOCATION_NAME", "FROM_LOCATION_NAME") or "TBD"

        loa = _find_col(row, "LOA", "Loa", "LENGTH", "Length")

        vid = _stable_id(name, eta_raw)
        if vid in seen:
            continue
        seen.add(vid)

        draught = _est_draught(vtype)
        try:
            loa_m = float(loa) if loa else None
        except ValueError:
            loa_m = None

        vessels.append({
            "id":          vid,
            "name":        name,
            "type":        vtype or "Unknown",
            "status":      _map_status(status_code),
            "eta":         eta_utc or now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "berth":       berth or "TBD",
            "draught":     draught,
            "loa":         loa_m,
            "at_risk":     _is_at_risk(eta_utc),
            "data_source": "live",
        })

    log.info("_transform_movements: %d raw rows -> %d active vessels", len(raw_rows), len(vessels))
    return vessels


def _build_berths(vessels: list) -> list:
    berth_map = {}
    for v in vessels:
        b = v.get("berth", "TBD")
        if not b or b == "TBD":
            continue
        if b not in berth_map:
            berth_map[b] = {
                "id":      f"b-{hashlib.md5(b.encode()).hexdigest()[:6]}",
                "name":    b,
                "vessels": [],
            }
        berth_map[b]["vessels"].append(v["id"])
    return list(berth_map.values())


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run_scrape() -> bool:
    """
    Run a QShips scrape.  Returns True on success, False on failure.
    Never overwrites good data with empty/failed results.
    """
    with _scrape_lock:
        now_utc = datetime.now(timezone.utc)

        raw_rows = _scrape_with_api()

        if not raw_rows:
            log.error("QShips API returned no rows — scrape failed")
            return False

        vessels = _transform_movements(raw_rows, now_utc)

        if not vessels:
            log.warning(
                "API returned %d rows but none mapped to valid Brisbane vessels. "
                "Check qships_debug.json for raw column names.",
                len(raw_rows),
            )
            return False

        berths = _build_berths(vessels)

        output = {
            "scraped_at":    now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "scrape_source": "qships-api",
            "port_name":     "Port of Brisbane",
            "vessels":       vessels,
            "berths":        berths,
        }

        OUTPUT_FILE.write_text(json.dumps(output, indent=2), encoding="utf-8")
        log.info(
            "Scrape complete — %d vessels, %d berths -> %s",
            len(vessels), len(berths), OUTPUT_FILE,
        )
        return True


# ---------------------------------------------------------------------------
# CLI test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )
    success = run_scrape()
    if success:
        data = json.loads(OUTPUT_FILE.read_text())
        print(f"\nScrape succeeded — {len(data['vessels'])} vessels")
        for v in data["vessels"][:5]:
            print(f"  {v['name']:30s}  {v['berth']:20s}  {v['status']:12s}  {v['eta']}")
    else:
        print("\nScrape FAILED — check logs above and qships_debug.json if it exists")
        if DEBUG_FILE.exists():
            raw = json.loads(DEBUG_FILE.read_text())
            print("Debug file top-level keys:",
                  list(raw.keys()) if isinstance(raw, dict) else type(raw))
        sys.exit(1)
