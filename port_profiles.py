"""
Project Horizon — Port Profile System
Defines configuration for each supported port. Active profile is selected via
the HORIZON_PORT environment variable (default: BRISBANE).
"""

PORT_PROFILES = {

    # ── Port of Brisbane ──────────────────────────────────────────────────────
    "BRISBANE": {
        "display_name":                 "Port of Brisbane",
        "short_name":                   "Brisbane",
        "timezone":                     "Australia/Brisbane",
        "lat":                          -27.3818,
        "lon":                          153.1653,
        "bom_station_id":               "IDO71004",   # Brisbane Bar tidal gauge
        "bom_tide_url":                 "http://www.bom.gov.au/fwo/IDO71004/IDO71004.xml",
        "tidal_mean_m":                 1.40,         # Mean tide level (cosine fallback)
        "tidal_amp_m":                  0.90,         # HW~2.3m, LW~0.5m
        "vessel_data_url":              "https://www.ports.com.au/port/port-of-brisbane/ship-movements/",
        "vessel_data_source":           "ports_victoria_html",
        "vessel_ingest_window_hours":   72,
        "max_vessels":                  30,
        "channel_depth_m":              14.0,
        "ukc_minimum_m":                0.5,
        "ukc_dukc_threshold_draught_m": 11.6,
        "max_vessel_loa_m":             300,
        "pilot_boarding_point":         "Brisbane Bar",
        "compulsory_pilotage_loa_m":    35,
        "wind_limit_berthing_knots":    25,
        "wind_limit_critical_knots":    40,
        "bridge_restrictions":          [],
        "harbour_master":               "Port of Brisbane",
        "vts_callsign":                 "Brisbane VTS",
        "vts_channel":                  "VHF 12",
        "currency":                     "AUD",
        "cost_per_hour_delay":          3800,
        "port_geo": {
            "center": {"lat": -27.383, "lon": 153.173},
            "zoom": 13,
            "berths": {
                "B01": {"lat": -27.3828, "lon": 153.1488, "terminal": "DP World Berth 5",    "heading": 10,  "depth_m": 13.5},
                "B02": {"lat": -27.3838, "lon": 153.1558, "terminal": "Patrick Berth 9",      "heading": 10,  "depth_m": 12.0},
                "B03": {"lat": -27.3845, "lon": 153.1628, "terminal": "Hutchison Berth 11",   "heading": 10,  "depth_m": 10.2},
                "B04": {"lat": -27.3820, "lon": 153.1418, "terminal": "AAT Berth 2",          "heading": 10,  "depth_m": 13.0},
                "B05": {"lat": -27.3858, "lon": 153.1368, "terminal": "Fisherman Island 3",   "heading": 190, "depth_m": 11.5},
                "B06": {"lat": -27.4672, "lon": 153.1435, "terminal": "Ampol Lytton",         "heading": 90,  "depth_m":  8.8},
            },
            "anchorage": {"lat": -27.352, "lon": 153.253, "radius_km": 2.5, "label": "Brisbane Bar Anchorage"},
            "pilot_boarding_ground": {"lat": -27.360, "lon": 153.218, "label": "Brisbane Bar Pilot Station"},
            "channel_waypoints": [
                {"lat": -27.352, "lon": 153.246}, {"lat": -27.357, "lon": 153.228},
                {"lat": -27.362, "lon": 153.212}, {"lat": -27.367, "lon": 153.198},
                {"lat": -27.372, "lon": 153.186}, {"lat": -27.374, "lon": 153.175},
                {"lat": -27.370, "lon": 153.161},
            ],
        },
    },

    # ── Port of Melbourne ─────────────────────────────────────────────────────
    # Values sourced from Ports Victoria Port Information Guide, 6th Ed, Dec 2025
    "MELBOURNE": {
        "display_name":                 "Port of Melbourne",
        "short_name":                   "Melbourne",
        "timezone":                     "Australia/Melbourne",
        "lat":                          -37.8224,
        "lon":                          144.9231,
        "bom_station_id":               "IDO71001",   # Williamstown tidal gauge
        "bom_tide_url":                 "http://www.bom.gov.au/fwo/IDO71001/IDO71001.xml",
        "tidal_mean_m":                 0.45,         # Mean tide level (cosine fallback)
        "tidal_amp_m":                  0.25,         # HW~0.7m, LW~0.2m — Port Phillip small range
        "vessel_data_url":              "https://www.ports.vic.gov.au/port-of-melbourne/shipping/ship-movements",
        "vessel_data_source":           "ports_victoria_html",
        "vessel_ingest_window_hours":   72,
        "max_vessels":                  30,
        "channel_depth_m":              14.0,
        "ukc_minimum_m":                0.5,
        "ukc_dukc_threshold_draught_m": 11.6,
        "max_vessel_loa_m":             340,
        "hat_m":                        1.04,
        "mhhw_m":                       0.9,
        "tidal_surge_positive_m":       0.4,
        "tidal_surge_negative_m":       0.2,
        "pilot_boarding_point":         "5 NM SW Point Lonsdale",
        "compulsory_pilotage_loa_m":    35,           # VPC HMD Ed. 13.1 §2.1
        "wind_limit_berthing_knots":    25,           # HMD Ed. 13.1 §3.20 — no new berthing
        "wind_limit_critical_knots":    35,           # HMD Ed. 13.1 §3.20 — engines standby
        "west_gate_bridge_air_draft_m": 50.0,         # 50m clearance at MHWS
        "bridge_restrictions": [
            {
                "name":             "West Gate Bridge",
                "max_air_draught_m": 50.1,
                "absolute_limit":   True,
                "notes":            "No transit permitted above 50.1m at any state of tide",
            },
            {
                "name":                     "Bolte Bridge",
                "max_air_draught_m":        28.2,
                "clearance_required_above_m": 24.36,
                "clearance_notice_hours":   24,
                "clearance_contact":        "CityLink Operations +61 3 9674 2001",
                "notes":                    "Vessels >24.36m air draught must obtain CityLink permission 24h prior",
            },
            {
                "name":             "Shepherd Bridge (Maribyrnong)",
                "max_air_draught_m": 4.74,
                "absolute_limit":   True,
                "notes":            "Highly restrictive — container/bulker vessels cannot transit",
            },
        ],
        "priority_rules": [
            "Emergency vessels",
            "Tidal/navigational constraint movements",
            "Cruise ships (inbound and outbound)",
            "Vessel ready to depart occupying berth with labour waiting",
            "Inward bound cleared by Quarantine with labour waiting",
            "Outward bound vessels",
            "Inward bound without labour waiting",
            "Shifting without labour waiting (vacated berth not immediately required)",
            "Shifting without power unless fouling a berth",
        ],
        "harbour_master":               "Andrew Hays — Ports Victoria",
        "vts_callsign":                 "Melbourne VTS",
        "vts_channel":                  "VHF 12",
        "pocc_address":                 "331-337 Lorimer Street, Port Melbourne VIC 3207",
        "emergency_vts_tel":            "+61 3 9644 9777",
        "currency":                     "AUD",
        "cost_per_hour_delay":          4200,
        "holden_dock_ukc_minimum_m":    1.0,
        "dukc_submission_arrival_hours":  (12, 24),
        "dukc_submission_departure_hours": 6,
        "port_geo": {
            "center": {"lat": -38.05, "lon": 144.90},
            "zoom": 10,
            "berths": {
                "B01": {"lat": -37.8428, "lon": 144.9248, "terminal": "Webb Dock 1 East",  "heading": 355, "depth_m": 13.5},
                "B02": {"lat": -37.8182, "lon": 144.9308, "terminal": "Swanson East 3",    "heading": 270, "depth_m": 13.0},
                "B03": {"lat": -37.8442, "lon": 144.9258, "terminal": "Webb Dock 4 East",  "heading": 355, "depth_m": 13.5},
                "B04": {"lat": -37.8195, "lon": 144.9268, "terminal": "Swanson West 2",    "heading": 270, "depth_m": 12.5},
                "B05": {"lat": -37.8435, "lon": 144.9252, "terminal": "Webb Dock 2 East",  "heading": 355, "depth_m": 13.5},
                "B06": {"lat": -37.8258, "lon": 144.9178, "terminal": "Appleton Dock F",   "heading": 90,  "depth_m": 10.0},
            },
            "anchorage": {
                "lat": -38.10, "lon": 144.76,
                "radius_km": 6.0,
                "label": "Port Phillip Bay Anchorage",
            },
            "pilot_boarding_ground": {
                "lat": -38.305, "lon": 144.545,
                "label": "Pilot Boarding Ground (5 NM SW Point Lonsdale)",
            },
            "channel_waypoints": [
                {"lat": -38.295, "lon": 144.615},
                {"lat": -38.245, "lon": 144.660},
                {"lat": -38.160, "lon": 144.710},
                {"lat": -38.060, "lon": 144.755},
                {"lat": -37.960, "lon": 144.815},
                {"lat": -37.880, "lon": 144.870},
                {"lat": -37.840, "lon": 144.910},
                {"lat": -37.822, "lon": 144.923},
            ],
        },
    },
    # ── Port of Darwin ────────────────────────────────────────────────────────
    # Values sourced from Darwin Port Handbook 2026 (Darwin Port Corporation)
    "DARWIN": {
        "display_name":                 "Port of Darwin",
        "short_name":                   "Darwin",
        "timezone":                     "Australia/Darwin",
        "lat":                          -12.4700,
        "lon":                          130.8450,
        "bom_station_id":               "IDO71013",   # Darwin tidal gauge
        "bom_tide_url":                 "http://www.bom.gov.au/fwo/IDO71013/IDO71013.xml",
        "tidal_mean_m":                 3.80,         # MSL above chart datum — Darwin extreme tidal range
        "tidal_amp_m":                  3.50,         # Half spring range (~7.0m springs) — largest tidal range in Aus
        "vessel_data_url":              None,
        "vessel_data_source":           "static_roster",
        "vessel_ingest_window_hours":   72,
        "max_vessels":                  30,
        "channel_depth_m":              13.0,         # Outer channel (East Arm Approach)
        "ukc_minimum_m":                1.5,          # Inner harbour UKC (Handbook 2026, p.14)
        "ukc_outer_harbour_m":          2.0,          # Outer Harbour Area A UKC (Handbook 2026, p.14)
        "ukc_msb_fairway_m":            1.0,          # MSB Fairway Zone G UKC (Handbook 2026, p.14)
        "ukc_creek_m":                  0.5,          # Hudson/Sadgroves Creek UKC (Handbook 2026, p.14)
        "ukc_dukc_threshold_draught_m": 10.5,
        "max_vessel_loa_m":             300,
        "pilot_boarding_point":         "Darwin Harbour Entrance",
        "compulsory_pilotage_loa_m":    50,           # Compulsory ≥50m LOA (Darwin Port Handbook 2026, p.13)
        "wind_limit_berthing_knots":    30,           # Reasonable default — wind limits in Port Notice PN014
        "wind_limit_critical_knots":    40,           # Reasonable default — wind limits in Port Notice PN014
        "bridge_restrictions":          [],           # No bridge air-draft restrictions at Darwin
        "cyclone_season_months":        [11, 12, 1, 2, 3, 4],  # November–April (Handbook 2026, p.18)
        "harbour_master":               "Darwin Port Corporation",
        "vts_callsign":                 "Darwin Port",
        "vts_channel":                  "VHF 10",     # Darwin Port working channel (Handbook 2026, p.15-16)
        "vts_distress_channel":         "VHF 16",
        "notice_of_arrival_hours":      24,           # 24h prior notice required (Handbook 2026, p.12)
        "speed_zones": {
            "Zone A (Outer Harbour)":   16,           # kt (Handbook 2026, p.15)
            "Zones B & C":              12,
            "Zone D":                   10,
            "Zones E, F, G (Inner)":     8,
        },
        "tug_companies":                ["Coastal Tug & Barge Pty Ltd", "Svitzer Australia Pty Ltd"],
        "currency":                     "AUD",
        "cost_per_hour_delay":          3500,
        "port_geo": {
            "center": {"lat": -12.470, "lon": 130.845},
            "zoom": 12,
            "berths": {
                # Key berth areas mapped from Darwin Port Handbook 2026 (pp.7-11)
                "B01": {"lat": -12.4678, "lon": 130.8432, "terminal": "Darwin Marine Supply Base (DMSB)", "heading": 180, "depth_m":  8.5},
                "B02": {"lat": -12.4730, "lon": 130.8562, "terminal": "East Arm Wharf Berth 1",           "heading": 190, "depth_m": 13.2},
                "B03": {"lat": -12.4750, "lon": 130.8575, "terminal": "East Arm Wharf Berth 3",           "heading": 190, "depth_m": 12.5},
                "B04": {"lat": -12.4595, "lon": 130.8648, "terminal": "HCK Berths (Hudson Creek)",        "heading": 270, "depth_m":  8.0},
                "B05": {"lat": -12.4700, "lon": 130.8510, "terminal": "Sea Swift Berth 1 (SSB1)",         "heading": 180, "depth_m":  7.0},
                "B06": {"lat": -12.4715, "lon": 130.8525, "terminal": "Sea Swift Berth 3 / Paspaley",     "heading": 180, "depth_m":  7.0},
            },
            "anchorage": {
                "lat": -12.390, "lon": 130.760,
                "radius_km": 4.0,
                "label": "Darwin Outer Anchorage",
            },
            "pilot_boarding_ground": {
                "lat": -12.530, "lon": 130.820,
                "label": "Darwin Harbour Pilot Boarding Ground",
            },
            "channel_waypoints": [
                {"lat": -12.530, "lon": 130.820},
                {"lat": -12.515, "lon": 130.828},
                {"lat": -12.500, "lon": 130.835},
                {"lat": -12.490, "lon": 130.840},
                {"lat": -12.480, "lon": 130.843},
                {"lat": -12.470, "lon": 130.845},
            ],
        },
    },

}


def get_profile(port_id: str) -> dict:
    """Return the port profile for the given ID, defaulting to BRISBANE."""
    return PORT_PROFILES.get(port_id.upper(), PORT_PROFILES["BRISBANE"])


def list_profiles() -> list:
    """Return all profiles as a list of {id, display_name, short_name} dicts."""
    return [
        {"id": k, "display_name": v["display_name"], "short_name": v["short_name"]}
        for k, v in PORT_PROFILES.items()
    ]
