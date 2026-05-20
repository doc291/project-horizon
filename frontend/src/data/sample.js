// Horizon V1 — M0 sample data
// Shape: ViewSummary per HORIZON_V1_API_ADAPTER_DESIGN_NOTE_v0.1.md §2.2 + §17.2.
// DEMO ONLY — none of this data is from a live source. The shape is deliberately
// the target view-model shape (NOT the raw /api/summary shape, NOT the
// prototype data.js shape) so M0 components are pre-positioned for the M1
// adapter swap without rework.

const NOW = new Date('2026-05-20T04:00:00Z'); // == 14:00 local AEST for Brisbane

function plusMinutes(d, m) {
  return new Date(d.getTime() + m * 60_000);
}

function isoOf(d) {
  return d.toISOString();
}

function localShort(d, tz = 'Australia/Brisbane') {
  return new Intl.DateTimeFormat('en-AU', {
    hour12: false,
    hour: '2-digit',
    minute: '2-digit',
    timeZone: tz,
  }).format(d);
}

// ── Conditions ───────────────────────────────────────────────────────────
const conditions = {
  rating: 'GOOD',
  windSpeedKts: 14,
  windBearingDeg: 67,
  windDirLabel: 'ENE',
  windBeaufort: 4,
  swellHeightM: 1.2,
  swellPeriodS: 8,
  swellDirLabel: 'E',
  visibilityNm: 6.5,
  visibilityKm: 12.0, // computed: 6.5 * 1.852 ≈ 12.04
  pressureHpa: 1013,
  tideHeightM: 1.84,
  tideState: 'rising',
  tideNextTime: '17:42',
  tideNextLabel: 'HW',
  tideMinM: 0.3,
  tideMaxM: 2.4,
  ukcM: 1.4,
  ukcStatus: 'ok',
  weatherSource: 'simulation',
};

// ── Vessels ──────────────────────────────────────────────────────────────
// Subset of the prototype demo set, normalised to ViewSummary vessel shape.
// Each vessel labelled (demo data) at display time.
const vessels = [
  {
    vesselId: 'V001', name: 'MV PACIFIC VOYAGER', imo: '9438762',
    type: 'Container', status: 'confirmed', source: 'sim',
    etaIso: isoOf(plusMinutes(NOW, 30)),  etaLocalShort: localShort(plusMinutes(NOW, 30)),
    etdIso: null, etdLocalShort: null,
    ataIso: null, atdIso: null,
    berth: 'B04', berthName: 'Berth 4 — Container',
    loaM: 294, draftM: 9.8, cargo: 'Containers',
    pilotageRequired: true, towageRequired: true,
    riskScore: 78, riskLevel: 'high', riskFactors: ['Berth conflict'],
    agent: 'Inchcape', flag: 'SG',
    lat: -27.32, lon: 153.24, notes: null,
  },
  {
    vesselId: 'V002', name: 'MV BRISBANE STAR', imo: '9711238',
    type: 'Bulk Carrier', status: 'berthed', source: 'sim',
    etaIso: null, etdIso: isoOf(plusMinutes(NOW, 240)),
    etdLocalShort: localShort(plusMinutes(NOW, 240)),
    ataIso: isoOf(plusMinutes(NOW, -360)), atdIso: null,
    berth: 'B04', berthName: 'Berth 4 — Container',
    loaM: 228, draftM: 12.4, cargo: 'Coal',
    pilotageRequired: true, towageRequired: true,
    riskScore: 62, riskLevel: 'medium', riskFactors: ['Tug reallocation'],
    agent: 'GAC', flag: 'PA', lat: -27.31, lon: 153.18, notes: null,
  },
  {
    vesselId: 'V003', name: 'MV CORAL SEA', imo: '9854411',
    type: 'LNG Carrier', status: 'scheduled', source: 'sim',
    etaIso: isoOf(plusMinutes(NOW, 165)), etaLocalShort: localShort(plusMinutes(NOW, 165)),
    etdIso: null, ataIso: null, atdIso: null,
    berth: 'B07', berthName: 'Berth 7 — LNG',
    loaM: 295, draftM: 11.6, cargo: 'LNG',
    pilotageRequired: true, towageRequired: true,
    riskScore: 71, riskLevel: 'high', riskFactors: ['Pilot rotation gap'],
    agent: 'Wilhelmsen', flag: 'MH', lat: -27.40, lon: 153.20, notes: null,
  },
];

// ── Conflicts — one per Canon §4 lifecycle state (DEMO only) ─────────────
// _demoState marks the state each conflict illustrates for the
// DesignVerificationSwatch in LeftPanel. None of these represent real
// operator actions, real audit entries, or any backend state.
const conflicts = [
  {
    conflictId: 'demo-rec', type: 'berth_overlap', signalType: 'CONFLICT',
    severity: 'CRITICAL',
    title: 'Berth overlap: MV PACIFIC VOYAGER vs MV BRISBANE STAR',
    description: '(Demo) Berth 4 double-booked 14:00–18:00.',
    vesselIds: ['V001', 'V002'], vesselNames: ['MV PACIFIC VOYAGER', 'MV BRISBANE STAR'],
    berth: 'B04', berthName: 'Berth 4 — Container',
    conflictTimeIso: isoOf(plusMinutes(NOW, 30)),
    conflictTimeShort: localShort(plusMinutes(NOW, 30)),
    decisionDeadlineIso: isoOf(plusMinutes(NOW, 154)),
    decisionDeadlineMinutes: 154,
    recommendedOptionId: 'B', confidence: 'high',
    options: [], // DSW deferred to M3+
    cascade: null, // Canon §8.8 — never synthesised
    dataSource: 'simulated',
    _demoState: 'RECOMMENDED',
  },
  {
    conflictId: 'demo-ack', type: 'berth_not_ready', signalType: 'WARNING',
    severity: 'HIGH', title: 'Berth not ready: MV WATTLE BAY',
    description: '(Demo) Operator acknowledged 13:42.',
    vesselIds: ['V004'], vesselNames: ['MV WATTLE BAY'],
    berth: 'B02', berthName: 'Berth 2 — Liquid',
    conflictTimeIso: isoOf(plusMinutes(NOW, 70)),
    conflictTimeShort: localShort(plusMinutes(NOW, 70)),
    decisionDeadlineIso: isoOf(plusMinutes(NOW, 200)),
    decisionDeadlineMinutes: 200,
    recommendedOptionId: 'A', confidence: 'medium',
    options: [], cascade: null, dataSource: 'simulated',
    _demoState: 'ACKNOWLEDGED',
  },
  {
    conflictId: 'demo-com', type: 'tug_double_book', signalType: 'CONFLICT',
    severity: 'MEDIUM', title: 'Tug double-book: Pacific Sentinel',
    description: '(Demo) Resolution committed — tug reallocated.',
    vesselIds: ['V005'], vesselNames: ['MV CALEDONIA'],
    berth: null, berthName: null,
    conflictTimeIso: isoOf(plusMinutes(NOW, -45)),
    conflictTimeShort: localShort(plusMinutes(NOW, -45)),
    decisionDeadlineIso: isoOf(plusMinutes(NOW, -10)),
    decisionDeadlineMinutes: -10,
    recommendedOptionId: 'A', confidence: 'high',
    options: [], cascade: null, dataSource: 'simulated',
    _demoState: 'COMMITTED',
  },
  {
    conflictId: 'demo-def', type: 'pilotage_window', signalType: 'WARNING',
    severity: 'MEDIUM', title: 'Short pilotage notice: MV CAPE BYRON',
    description: '(Demo) Deferred by HM — reason logged.',
    vesselIds: ['V006'], vesselNames: ['MV CAPE BYRON'],
    berth: null, berthName: null,
    conflictTimeIso: isoOf(plusMinutes(NOW, 525)),
    conflictTimeShort: localShort(plusMinutes(NOW, 525)),
    decisionDeadlineIso: isoOf(plusMinutes(NOW, 600)),
    decisionDeadlineMinutes: 600,
    recommendedOptionId: 'B', confidence: 'medium',
    options: [], cascade: null, dataSource: 'simulated',
    _demoState: 'DEFERRED',
  },
  {
    conflictId: 'demo-ovr', type: 'berth_overlap', signalType: 'CONFLICT',
    severity: 'HIGH', title: 'Berth overlap: HM override applied',
    description: '(Demo) HM committed alternative option C.',
    vesselIds: ['V007'], vesselNames: ['MV WHITSUNDAY'],
    berth: 'B05', berthName: 'Berth 5 — Container',
    conflictTimeIso: isoOf(plusMinutes(NOW, -90)),
    conflictTimeShort: localShort(plusMinutes(NOW, -90)),
    decisionDeadlineIso: isoOf(plusMinutes(NOW, -30)),
    decisionDeadlineMinutes: -30,
    recommendedOptionId: 'A', confidence: 'high',
    options: [], cascade: null, dataSource: 'simulated',
    _demoState: 'OVERRIDDEN',
  },
  {
    conflictId: 'demo-esc', type: 'berth_overlap', signalType: 'CONFLICT',
    severity: 'CRITICAL', title: 'Escalation: VTSO → HM',
    description: '(Demo) Escalated to Harbour Master for authority decision.',
    vesselIds: ['V008'], vesselNames: ['MV SOLANDER'],
    berth: 'B08', berthName: 'Berth 8 — Liquid',
    conflictTimeIso: isoOf(plusMinutes(NOW, 420)),
    conflictTimeShort: localShort(plusMinutes(NOW, 420)),
    decisionDeadlineIso: isoOf(plusMinutes(NOW, 90)),
    decisionDeadlineMinutes: 90,
    recommendedOptionId: 'A', confidence: 'medium',
    options: [], cascade: null, dataSource: 'simulated',
    _demoState: 'ESCALATED',
  },
  {
    conflictId: 'demo-exp', type: 'pilotage_window', signalType: 'WARNING',
    severity: 'MEDIUM', title: 'Pilotage window expired: MV HERVEY',
    description: '(Demo) Deadline passed without operator engagement.',
    vesselIds: ['V010'], vesselNames: ['MV HERVEY'],
    berth: null, berthName: null,
    conflictTimeIso: isoOf(plusMinutes(NOW, -180)),
    conflictTimeShort: localShort(plusMinutes(NOW, -180)),
    decisionDeadlineIso: isoOf(plusMinutes(NOW, -120)),
    decisionDeadlineMinutes: -120,
    recommendedOptionId: null, confidence: 'medium',
    options: [], cascade: null, dataSource: 'simulated',
    _demoState: 'EXPIRED',
  },
];

// ── Dashboard / port_status / etd_risk ───────────────────────────────────
const portStatus = {
  berthsOccupied: 7, berthsAvailable: 1, berthsTotal: 8,
  vesselsInPort: 18, vesselsExpected24h: 6, vesselsDeparting24h: 3,
  activeConflicts: conflicts.length, criticalConflicts: 2,
  pilotsAvailable: 3, tugsAvailable: 4,
};

const dashboardMetrics = {
  berthUtilisationPct: 88,
  forecastUtilisation48h: 74,
  onTimeDeparturePct: 92,
  avgDwellHours: 18.4,
  vesselsAtRisk: 2,
  activeConflicts: conflicts.length,
  criticalConflicts: 2,
  pilotOps12h: 5,
  tugOps12h: 6,
  vesselsInPort: 18,
  vesselsExpected24h: 6,
};

const etdRisk = [
  { vesselId: 'V001', vesselName: 'MV PACIFIC VOYAGER', riskScore: 78, riskLevel: 'high',   riskFactors: ['Berth 4 conflict'],    delta: null },
  { vesselId: 'V003', vesselName: 'MV CORAL SEA',       riskScore: 71, riskLevel: 'high',   riskFactors: ['Pilot rotation gap'],  delta: null },
  { vesselId: 'V002', vesselName: 'MV BRISBANE STAR',   riskScore: 62, riskLevel: 'medium', riskFactors: ['Tug reallocation'],    delta: null },
];

// ── Export ───────────────────────────────────────────────────────────────
export const SAMPLE = {
  timestamp: NOW,
  generatedAtIso: isoOf(NOW),
  portId: 'BRISBANE',
  portName: 'Port of Brisbane',
  portTimezone: 'Australia/Brisbane',
  portStatus,
  conditions,
  vessels,
  berths: [],          // Gantt segments rendered later; M0 placeholder
  conflicts,
  guidance: [],        // M2+ — left-rail alert list deferred
  pilotage: [],        // M3+ — Pilotage tab deferred
  towage: [],
  dashboardMetrics,
  etdRisk,
  liveness: { vessel: false, weather: false, tide: false, isLive: false },
  dataSource: 'Simulation',
  availablePorts: ['BRISBANE', 'MELBOURNE', 'GEELONG', 'DARWIN'],
  missingDomains: ['shiftLog', 'auditLog', 'cascade'],
};
