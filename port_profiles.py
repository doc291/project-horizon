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
        "unloco":                       "AUBNE",
        "mst_port_id":                  108,
        "lat":                          -27.3818,
        "lon":                          153.1653,
        "bom_station_id":               "IDO71004",   # Brisbane Bar tidal gauge
        "bom_tide_url":                 "https://www.bom.gov.au/fwo/IDO71004/IDO71004.xml",
        "tidal_mean_m":                 1.40,         # Mean tide level (cosine fallback)
        "tidal_amp_m":                  0.90,         # HW~2.3m, LW~0.5m
        "vessel_data_url":              None,
        "vessel_data_source":           "mst",
        "vessel_ingest_window_hours":   72,
        "max_vessels":                  30,
        "channel_depth_m":              14.0,
        "ukc_minimum_m":                0.5,
        "ukc_dukc_threshold_draught_m": 11.6,
        "max_vessel_loa_m":             300,
        "pilot_boarding_point":         "Brisbane Bar",
        "compulsory_pilotage_loa_m":    35,
        "pilots": ["PILOT_BNE_PSP_01", "PILOT_BNE_PSP_02", "PILOT_BNE_PSP_03", "PILOT_BNE_PSP_04"],
        "pilot_stations": ["Brisbane Bar Pilot Station", "Spitfire Channel Anchorage"],
        "tugs": [
            {"name": "SVR Brisbane",  "bollard_pull_t": 70},
            {"name": "SVR Apex",      "bollard_pull_t": 65},
            {"name": "TKY Vigilant",  "bollard_pull_t": 72},
            {"name": "TKY Resolute",  "bollard_pull_t": 68},
            {"name": "TKY Hawk",      "bollard_pull_t": 55},
        ],
        "mooring_gangs": [
            {"name": "Alpha Gang",   "linesmen": 4},
            {"name": "Bravo Gang",   "linesmen": 4},
            {"name": "Charlie Gang", "linesmen": 3},
            {"name": "Delta Gang",   "linesmen": 4},
            {"name": "Echo Gang",    "linesmen": 3},
        ],
        "wind_limit_berthing_knots":    25,
        "wind_limit_critical_knots":    40,
        "bridge_restrictions":          [],
        # Simulation scenario — vessel/berth counts for this port
        "sim_vessel_count": 13,
        # sim_berth_slots: None → use server.py defaults (B01-B06, 4/6 occupied)
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

        # ── Towage demand model (R4.1 — REPRESENTATIVE / PLACEHOLDER) ─────────
        # Estimated tug-demand bands for Horizon Beta 12 capacity coordination
        # and conflict detection. NOT a towage requirement. NOT a substitute
        # for Brisbane's Harbour Master's Directions. Authoritative minimum
        # towage requirements live in:
        #   Brisbane Port Procedures and Information for Shipping,
        #   Section 8: Harbour Towage and Support Vessel Procedures
        #   (Maritime Safety Queensland, October 2024 edition).
        # Values below are placeholders calibrated from the Australian Port
        # Towage Requirements research (Beta 12) and the existing `loa > 200`
        # demo behaviour. They MUST be reviewed by Brisbane towage operations
        # and the Regional Harbour Master (MSQ) before any operational use.
        # Wind / berth / beam modifiers and pilot/HMR discretion are NOT
        # modelled — those are the pilot and Harbour Master's call. R4.1
        # adds data only; no code consumes this field yet (wired in R4.3+).
        "towage_rule": {
            "status":         "PLACEHOLDER — operator-review-required",
            "model":          "representative_demand",  # never "operational_rule" until V1
            "source":         "Beta 12 representative demand model; not HMD-derived",
            "ui_label":       "estimated tug demand",
            "ui_disclaimer":  "Estimated demand only. Actual towage requirement set by pilot and Harbour Master per published Directions.",
            "bands": [
                {"loa_max": 100, "n_tugs": 0},   # ~Coastal traders / small general cargo
                {"loa_max": 180, "n_tugs": 1},   # ~Handysize / small Panamax
                {"loa_max": 240, "n_tugs": 2},   # ~Panamax to Post Panamax
                {"loa_max": 300, "n_tugs": 3},   # ~Larger Post Panamax containers
                {"loa_max": 999, "n_tugs": 4},   # Catch-all for the largest visitors
            ],
            "vessel_type_floor": {
                "Car carrier":  2,   # High windage (PCTC) — research §7.3
                "Tanker":       2,   # Brisbane crude/clean berths
                "LNG carrier":  3,
            },
            "vessel_name_override":  {},
            "loa_epsilon_m":         0.5,  # AIS rounding tolerance — admits 199.5–200.0 m hairline cases
            "review_required_by": [
                "Brisbane towage operations",
                "Regional Harbour Master, Maritime Safety Queensland",
            ],
            "v1_target": "Replace with per-berth HMD-derived matrix (MSQ Section 8). See LENS-CONTRACT.md §5.3.",
        },
    },

    # ── Port of Melbourne ─────────────────────────────────────────────────────
    # Values sourced from Ports Victoria Port Information Guide, 6th Ed, Dec 2025
    "MELBOURNE": {
        "display_name":                 "Port of Melbourne",
        "short_name":                   "Melbourne",
        "timezone":                     "Australia/Melbourne",
        "unloco":                       "AUMEL",
        "mst_port_id":                  293,
        "lat":                          -37.8224,
        "lon":                          144.9231,
        "bom_station_id":               "IDO71001",   # Williamstown tidal gauge
        "bom_tide_url":                 "https://www.bom.gov.au/fwo/IDO71001/IDO71001.xml",
        "tidal_mean_m":                 0.45,         # Mean tide level (cosine fallback)
        "tidal_amp_m":                  0.25,         # HW~0.7m, LW~0.2m — Port Phillip small range
        "vessel_data_url":              None,
        "vessel_data_source":           "mst",
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
        "pilots": ["PILOT_MEL_PSP_01", "PILOT_MEL_PSP_02", "PILOT_MEL_PSP_03", "PILOT_MEL_PSP_04"],
        "pilot_stations": ["Point Lonsdale Pilot Station", "South Channel Anchorage"],
        "tugs": [
            {"name": "SVR Apex",    "bollard_pull_t": 72},
            {"name": "SVR Mercury", "bollard_pull_t": 65},
            {"name": "SVR Orion",   "bollard_pull_t": 80},
            {"name": "SVR Atlas",   "bollard_pull_t": 58},
            {"name": "SVR Titan",   "bollard_pull_t": 70},
        ],
        "mooring_gangs": [
            {"name": "Gang 1", "linesmen": 4},
            {"name": "Gang 2", "linesmen": 4},
            {"name": "Gang 3", "linesmen": 3},
            {"name": "Gang 4", "linesmen": 4},
            {"name": "Gang 5", "linesmen": 3},
        ],
        "wind_limit_berthing_knots":    25,           # HMD Ed. 13.1 §3.20 — no new berthing
        "wind_limit_critical_knots":    35,           # HMD Ed. 13.1 §3.20 — engines standby
        # Simulation scenario — 9 vessels, Melbourne-specific berth occupancy (3/6)
        "sim_vessel_count": 9,
        "sim_berth_slots": [
            # (id, max_loa, max_draught, status, cranes, ready_offset_h)
            ("B01", 350, 14.0, "occupied",     4,  4),
            ("B02", 300, 13.0, "occupied",     4,  8),
            ("B03", 280, 12.5, "available",    2,  None),
            ("B04", 320, 14.0, "reserved",     3,  3),
            ("B05", 260, 11.5, "available",    2,  None),
            ("B06", 220, 10.0, "maintenance",  0,  22),
        ],
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

        # ── Towage demand model (R4.1 — REPRESENTATIVE / PLACEHOLDER) ─────────
        # Estimated tug-demand bands for Horizon Beta 12 capacity coordination
        # and conflict detection. NOT a towage requirement. NOT a substitute
        # for the Port of Melbourne Harbour Master's Directions. Authoritative
        # minimum towage requirements live in:
        #   Ports Victoria Harbour Master's Directions, Edition 13.1,
        #   September 2023, Section 3.21 ("Towage and minimum requirements")
        #   plus the berth-specific Minimum Towage Table referenced therein.
        #   The HMD is berth-specific (Swanson Dock, Webb Dock, Appleton Dock,
        #   Gellibrand Pier, Station Pier, Holden Dock, river berths) and
        #   uses beam thresholds (Post Panamax >32.5 m beam, Bosphorus Max
        #   >45.6 m beam) and air-draught constraints (Bolte Bridge) that
        #   are NOT modelled here. The research note explicitly records that
        #   the Melbourne HMD §3.21 table was not fully extractable from the
        #   PDF; this placeholder approximates demand only.
        # Values below are placeholders calibrated from the Australian Port
        # Towage Requirements research (Beta 12). They MUST be reviewed by
        # Melbourne towage operations and the Harbour Master (Ports Victoria)
        # before any operational use. Wind / berth / beam modifiers and
        # pilot/HMR discretion are NOT modelled. R4.1 adds data only; no
        # code consumes this field yet (wired in R4.3+).
        "towage_rule": {
            "status":         "PLACEHOLDER — operator-review-required",
            "model":          "representative_demand",
            "source":         "Beta 12 representative demand model; not HMD-derived",
            "ui_label":       "estimated tug demand",
            "ui_disclaimer":  "Estimated demand only. Actual towage requirement set by pilot and Harbour Master per published Directions.",
            "bands": [
                {"loa_max": 120, "n_tugs": 0},   # ~Coastal traders
                {"loa_max": 170, "n_tugs": 1},   # ~Small general cargo
                {"loa_max": 250, "n_tugs": 2},   # ~Panamax / containers (Swanson + Webb)
                {"loa_max": 310, "n_tugs": 3},   # ~Post Panamax (HMD §3.16.15-3.16.17)
                {"loa_max": 999, "n_tugs": 4},   # Bosphorus Max (HMD definition; individual assessment)
            ],
            "vessel_type_floor": {
                "Car carrier":  2,   # PCTC — high windage; research §1 / §7.3
                "Tanker":       2,   # Port Phillip petroleum berths
                "LNG carrier":  3,
                "LPG carrier":  2,
            },
            "vessel_name_override":  {},
            "loa_epsilon_m":         0.5,  # AIS rounding tolerance — admits BYD ZHENGZHOU-class (199.9 m)
            "review_required_by": [
                "Port of Melbourne towage operations",
                "Harbour Master, Ports Victoria",
            ],
            "v1_target": "Replace with HMD §3.21 berth-specific Minimum Towage Table; add beam tier (Post Panamax / Bosphorus Max) and Bolte Bridge air-draught modifier. See LENS-CONTRACT.md §5.3.",
        },
    },
    # ── Port of Darwin ────────────────────────────────────────────────────────
    # Values sourced from Darwin Port Handbook 2026 (Darwin Port Corporation)
    "DARWIN": {
        "display_name":                 "Port of Darwin",
        "short_name":                   "Darwin",
        "timezone":                     "Australia/Darwin",
        "unloco":                       "AUDRW",
        "mst_port_id":                  3870,
        "lat":                          -12.4700,
        "lon":                          130.8450,
        "bom_station_id":               "IDO71013",   # Darwin tidal gauge
        "bom_tide_url":                 "https://www.bom.gov.au/fwo/IDO71013/IDO71013.xml",
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
        "pilots": ["PILOT_DRW_PSP_01", "PILOT_DRW_PSP_02", "PILOT_DRW_PSP_03", "PILOT_DRW_PSP_04"],
        "pilot_stations": ["Darwin Harbour Pilot Boarding Ground", "Darwin Outer Anchorage"],
        "tugs": [
            {"name": "CTB Endeavour", "bollard_pull_t": 60},
            {"name": "CTB Pioneer",   "bollard_pull_t": 55},
            {"name": "SVR Kestrel",   "bollard_pull_t": 65},
            {"name": "SVR Falcon",    "bollard_pull_t": 58},
        ],
        "mooring_gangs": [
            {"name": "Darwin Alpha",   "linesmen": 4},
            {"name": "Darwin Bravo",   "linesmen": 3},
            {"name": "Darwin Charlie", "linesmen": 3},
            {"name": "Darwin Delta",   "linesmen": 4},
        ],
        "wind_limit_berthing_knots":    30,           # Reasonable default — wind limits in Port Notice PN014
        "wind_limit_critical_knots":    40,           # Reasonable default — wind limits in Port Notice PN014
        "bridge_restrictions":          [],           # No bridge air-draft restrictions at Darwin
        # Demo simulation lock — Darwin is a low-traffic port; live AIS often
        # returns a vessel mix that does not produce a decision card. Forcing
        # the simulation path keeps Darwin demo behaviour deterministic so the
        # existing simulated conflict scenarios (V007 ETA variance, V005/V007
        # B04 berth overlap) reliably surface. Per-port flag — does not affect
        # Brisbane / Melbourne / Geelong.
        "demo_force_simulation":        True,
        # Demo slot pinning — Darwin's roster is realistic and contains many
        # small offshore-supply vessels (<100 m LOA). The berth_overlap
        # generator requires ≥100 m vessels (see server.py detect_conflicts).
        # The Decisions panel filters on signal_type=='CONFLICT' AND
        # decision_support — both only set by berth_overlap. Pin V005 and V007
        # (both at B04 in the slot table, with overlapping windows) to two
        # ≥100 m roster entries that fit B04 (max LOA 160 m, max draught 8.5 m):
        #   slot 4 (V005) → ACHILLES SEA      (104 m, 6.8 m draught)
        #   slot 6 (V007) → Forever Assurance (119 m, 7.5 m draught)
        # This guarantees the deterministic B04 berth_overlap CONFLICT with
        # populated decision_support, so the Decisions panel is non-empty.
        # No-op for any port without this field.
        "sim_pinned_vessels": {
            4: "ACHILLES SEA",
            6: "Forever Assurance",
        },
        # Simulation scenario — 7 vessels, Darwin-specific berth occupancy (2/4)
        "sim_vessel_count": 7,
        "sim_berth_slots": [
            # (id, max_loa, max_draught, status, cranes, ready_offset_h)
            ("B01", 240, 10.5, "occupied",     2,  8),
            ("B02", 200, 10.0, "occupied",     2,  12),
            ("B03", 180,  9.5, "available",    1,  None),
            ("B04", 160,  8.5, "maintenance",  0,  24),
        ],
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

        # ── Towage demand model (R4.1 — REPRESENTATIVE / PLACEHOLDER) ─────────
        # Estimated tug-demand bands for Horizon Beta 12 capacity coordination
        # and conflict detection. NOT a towage requirement. NOT a substitute
        # for the Darwin Regional Harbour Master's directions. Authoritative
        # minimum towage requirements live in:
        #   Darwin Port Handbook (November 2023), Harbour Towage section, and
        #   Northern Territory Government Regional Harbour Master directions.
        #   Specific LNG terminal towage at Bladin Point is governed by the
        #   INPEX Onshore Terminal Regulations.
        #   Darwin does not publish a single berth-by-berth matrix; current
        #   towage is by port notice plus Harbour Master assessment, with
        #   significant weight given to tidal range (~8 m springs) and
        #   monsoonal weather (cyclone season Nov–Apr).
        # Values below are placeholders calibrated from the Australian Port
        # Towage Requirements research (Beta 12). They MUST be reviewed by
        # Darwin Port towage operations and the NT Regional Harbour Master
        # before any operational use. Tidal / weather / berth modifiers and
        # pilot discretion are NOT modelled. R4.1 adds data only; no code
        # consumes this field yet (wired in R4.3+).
        "towage_rule": {
            "status":         "PLACEHOLDER — operator-review-required",
            "model":          "representative_demand",
            "source":         "Beta 12 representative demand model; not HMR-derived",
            "ui_label":       "estimated tug demand",
            "ui_disclaimer":  "Estimated demand only. Actual towage requirement set by pilot and Harbour Master per Darwin Port directions.",
            "bands": [
                {"loa_max": 100, "n_tugs": 0},   # ~Offshore support vessels, smaller craft
                {"loa_max": 190, "n_tugs": 1},   # ~General cargo / smaller bulk at East Arm
                {"loa_max": 260, "n_tugs": 2},   # ~Larger bulk / tankers / cruise at Fort Hill
                {"loa_max": 999, "n_tugs": 3},   # ~Largest cruise (up to 350 m at Fort Hill)
            ],
            "vessel_type_floor": {
                "LNG carrier":  3,   # Bladin Point INPEX terminal expectations
                "Tanker":       2,
                "Car carrier":  2,
            },
            "vessel_name_override":  {},
            "loa_epsilon_m":         0.5,
            "review_required_by": [
                "Darwin Port towage operations",
                "Regional Harbour Master, Northern Territory Government",
            ],
            "v1_target": "Replace with NT RHM directions + INPEX terminal rules; add tidal-state and cyclone-season modifiers. See LENS-CONTRACT.md §5.3.",
        },
    },

    # ── Port of Geelong ───────────────────────────────────────────────────────
    # Values sourced from GeelongPort Berth Specifications, Ports Victoria
    # Port Information Guide 2020 (Geelong), and BOM/AHS tidal data.
    "GEELONG": {
        "display_name":                 "Port of Geelong",
        "short_name":                   "Geelong",
        "timezone":                     "Australia/Melbourne",
        "unloco":                       "AUGEX",
        "mst_port_id":                  180,
        "lat":                          -38.128,
        "lon":                          144.352,
        "bom_station_id":               "IDO71001",   # Williamstown — nearest Port Phillip gauge
        "bom_tide_url":                 "https://www.bom.gov.au/fwo/IDO71001/IDO71001.xml",
        "tidal_mean_m":                 0.42,         # Port Phillip Bay small range
        "tidal_amp_m":                  0.22,         # HW ~0.65m, LW ~0.20m
        "vessel_data_url":              None,
        "vessel_data_source":           "mst",
        "vessel_ingest_window_hours":   72,
        "max_vessels":                  20,
        "channel_depth_m":              12.3,         # Geelong Channel (4 channels, 120m wide)
        "ukc_minimum_m":                0.5,
        "ukc_dukc_threshold_draught_m": 10.0,         # DUKC applies above 10m draught
        "max_vessel_loa_m":             250,          # Geelong channel/berth LOA constraint
        "hat_m":                        0.80,
        "mhhw_m":                       0.65,
        "tidal_surge_positive_m":       0.3,
        "tidal_surge_negative_m":       0.2,
        "pilot_boarding_point":         "5 NM SW Point Lonsdale",
        "compulsory_pilotage_loa_m":    35,           # Victorian Ports — same threshold
        "pilots": ["PILOT_GEX_PSP_01", "PILOT_GEX_PSP_02", "PILOT_GEX_PSP_03", "PILOT_GEX_PSP_04"],
        "pilot_stations": ["Point Lonsdale Pilot Station"],
        "tugs": [
            {"name": "SVR Hawk",    "bollard_pull_t": 65},
            {"name": "SVR Condor",  "bollard_pull_t": 58},
            {"name": "SVR Falcon",  "bollard_pull_t": 72},
        ],
        "mooring_gangs": [
            {"name": "Gang 1", "linesmen": 4},
            {"name": "Gang 2", "linesmen": 4},
            {"name": "Gang 3", "linesmen": 3},
        ],
        "wind_limit_berthing_knots":    22,
        "wind_limit_critical_knots":    32,
        "sim_vessel_count": 7,
        "sim_berth_slots": [
            # (id, max_loa, max_draught, status, cranes, ready_offset_h)
            ("B01", 200, 11.0, "occupied",   2,  6),
            ("B02", 200, 11.0, "available",  2,  None),
            ("B03", 170, 11.0, "occupied",   2,  10),
            ("B04", 190, 11.0, "reserved",   1,  3),
            ("B05", 190, 12.3, "occupied",   0,  8),
            ("B06", 190, 12.3, "available",  0,  None),
        ],
        "dukc_submission_arrival_hours":  (12, 24),
        "dukc_submission_departure_hours": 6,
        "harbour_master":               "Ports Victoria — Harbour Master Geelong",
        "vts_callsign":                 "Melbourne VTS",
        "vts_channel":                  "VHF 12",
        "pocc_address":                 "Corio Quay Road, North Shore VIC 3214",
        "emergency_vts_tel":            "+61 3 9644 9777",
        "currency":                     "AUD",
        "cost_per_hour_delay":          3200,
        "dukc_submission_arrival_hours":  (12, 24),
        "dukc_submission_departure_hours": 6,
        "port_geo": {
            "center": {"lat": -38.11, "lon": 144.35},
            "zoom": 11,
            "berths": {
                "B01": {"lat": -38.139, "lon": 144.350, "terminal": "Corio Quay North 1",  "heading": 270, "depth_m": 11.0},
                "B02": {"lat": -38.142, "lon": 144.352, "terminal": "Corio Quay North 2",  "heading": 270, "depth_m": 11.0},
                "B03": {"lat": -38.144, "lon": 144.354, "terminal": "Corio Quay North 3",  "heading": 270, "depth_m": 11.0},
                "B04": {"lat": -38.148, "lon": 144.358, "terminal": "Corio Quay South 1",  "heading": 90,  "depth_m": 11.0},
                "B05": {"lat": -38.127, "lon": 144.344, "terminal": "Lascelles Wharf 1",   "heading": 180, "depth_m": 12.3},
                "B06": {"lat": -38.124, "lon": 144.341, "terminal": "Lascelles Wharf 2",   "heading": 180, "depth_m": 12.3},
            },
            "anchorage": {
                "lat": -38.05, "lon": 144.42,
                "radius_km": 4.0,
                "label": "Corio Bay Anchorage",
            },
            "pilot_boarding_ground": {
                "lat": -38.305, "lon": 144.545,
                "label": "Pilot Boarding Ground (5 NM SW Point Lonsdale)",
            },
            "channel_waypoints": [
                {"lat": -38.295, "lon": 144.615},   # The Rip entry
                {"lat": -38.240, "lon": 144.630},
                {"lat": -38.175, "lon": 144.580},
                {"lat": -38.140, "lon": 144.520},   # Branch from South Channel
                {"lat": -38.095, "lon": 144.450},   # Geelong Channel
                {"lat": -38.068, "lon": 144.395},
                {"lat": -38.060, "lon": 144.365},   # Corio Bay approach
                {"lat": -38.128, "lon": 144.352},   # Corio Quay
            ],
        },

        # ── Towage demand model (R4.1 — REPRESENTATIVE / PLACEHOLDER) ─────────
        # Estimated tug-demand bands for Horizon Beta 12 capacity coordination
        # and conflict detection. NOT a towage requirement. NOT a substitute
        # for the Geelong Harbour Master's Directions. Authoritative minimum
        # towage requirements live in:
        #   Ports Victoria Harbour Master's Directions 2020 (Geelong),
        #   Section 4.6 "Towage and wind speeds" — Minimum Towage Table,
        #   referenced in the Port Information Guide 2020.
        #   The HMD baseline is "steady winds up to 15 knots and a fully
        #   manoeuvrable vessel"; conditions above that require risk
        #   assessment and may add tugs. Tanker / OBO vessels at non-tanker
        #   berths have specific additional requirements (HMD §9.5). None
        #   of these modifiers are encoded here.
        # Values below are placeholders calibrated from the Australian Port
        # Towage Requirements research (Beta 12). They MUST be reviewed by
        # Geelong towage operations and the Harbour Master (Ports Victoria)
        # before any operational use. R4.1 adds data only; no code consumes
        # this field yet (wired in R4.3+).
        "towage_rule": {
            "status":         "PLACEHOLDER — operator-review-required",
            "model":          "representative_demand",
            "source":         "Beta 12 representative demand model; not HMD-derived",
            "ui_label":       "estimated tug demand",
            "ui_disclaimer":  "Estimated demand only. Actual towage requirement set by pilot and Harbour Master per published Directions.",
            "bands": [
                {"loa_max": 90,  "n_tugs": 0},   # ~Coastal small craft
                {"loa_max": 160, "n_tugs": 1},   # ~Medium vessels (research §3)
                {"loa_max": 230, "n_tugs": 2},   # ~Panamax bulk carriers
                {"loa_max": 270, "n_tugs": 3},   # ~Capesize / larger tankers at Refinery Pier
                {"loa_max": 999, "n_tugs": 4},   # Catch-all (rare at Geelong)
            ],
            "vessel_type_floor": {
                "Car carrier":  2,   # High windage; PCTC visits do occur at Geelong
                "Tanker":       2,   # Refinery Pier / petroleum exposure
                # Geelong does not currently handle LNG (research §3) — no LNG floor.
            },
            "vessel_name_override":  {},
            "loa_epsilon_m":         0.5,
            "review_required_by": [
                "Geelong towage operations",
                "Harbour Master, Ports Victoria",
            ],
            "v1_target": "Replace with HMD §4.6 Minimum Towage Table; add wind escalation above 15 kt baseline (HMD-mandated risk assessment). See LENS-CONTRACT.md §5.3.",
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
