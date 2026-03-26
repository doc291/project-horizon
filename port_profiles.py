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
                "B01": {"lat": -27.368, "lon": 153.150, "terminal": "North Terminal", "heading": 350},
                "B02": {"lat": -27.369, "lon": 153.161, "terminal": "North Terminal", "heading": 350},
                "B03": {"lat": -27.370, "lon": 153.172, "terminal": "North Terminal", "heading": 350},
                "B04": {"lat": -27.397, "lon": 153.157, "terminal": "South Terminal", "heading": 170},
                "B05": {"lat": -27.398, "lon": 153.167, "terminal": "South Terminal", "heading": 170},
                "B06": {"lat": -27.399, "lon": 153.177, "terminal": "South Terminal", "heading": 170},
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
        "compulsory_pilotage_loa_m":    35,
        "wind_limit_berthing_knots":    35,   # stop cargo ops
        "wind_limit_disconnect_knots":  40,   # disconnect arms
        "wind_limit_critical_knots":    45,
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
                # Swanson Dock / Appleton Dock / Webb Dock approximate positions
                "B01": {"lat": -37.8178, "lon": 144.9255, "terminal": "Swanson Dock East",  "heading": 270},
                "B02": {"lat": -37.8192, "lon": 144.9215, "terminal": "Swanson Dock West",  "heading": 270},
                "B03": {"lat": -37.8225, "lon": 144.9165, "terminal": "Appleton Dock",      "heading": 90},
                "B04": {"lat": -37.8385, "lon": 144.9205, "terminal": "Webb Dock North",    "heading": 10},
                "B05": {"lat": -37.8420, "lon": 144.9195, "terminal": "Webb Dock South",    "heading": 10},
                "B06": {"lat": -37.8475, "lon": 144.9235, "terminal": "Station Pier",       "heading": 180},
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
