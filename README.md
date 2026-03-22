# Project Horizon — Release 1

Predictive maritime coordination platform. Provides a shared forward view of vessel movements, berth readiness, pilotage, towage, conflicts, and operational guidance for port coordinators.

## Running the app

**Requirements:** Python 3.8+ (zero external dependencies)

```bash
python3 server.py
```

Then open **http://localhost:8000** in your browser.

## What Release 1 includes

**Berth Timeline (48h Gantt)**
- All 6 berths across North and South terminals
- Colour-coded vessel blocks: berthed (green), confirmed (blue), scheduled (grey), at risk (orange)
- Dashed conflict outline on vessels involved in a detected conflict
- "Now" cursor line; hover any vessel block for a detail tooltip

**Operational Guidance (left panel)**
- Prioritised guidance items: critical → high → medium → info
- Derived from conflicts plus proactive upcoming-event alerts
- Click any item to expand resolution options

**Conflict Detection (right panel)**
- Five conflict types: berth overlap, berth not ready, pilotage window, tug contention, ETA variance
- Each conflict shows severity, affected vessels/berths, description, and resolution options
- Sorted by severity then time

**Vessel Table**
- Full vessel roster with status, ETA/ETD, LOA, cargo, berth assignment, agent

**Auto-refresh:** every 60 seconds (or click Refresh in the header)

## File structure

```
Project Horizon/
├── server.py        # Zero-dependency Python 3 server + all backend logic
├── index.html       # Single-file frontend (no build step required)
│
└── backend/         # Reference FastAPI implementation (requires fastapi + uvicorn)
    ├── main.py
    ├── models.py
    ├── mock_data.py
    ├── conflict_engine.py
    └── guidance_engine.py
```

## API

| Route | Description |
|-------|-------------|
| `GET /` | The coordination dashboard |
| `GET /api/summary` | Full operational snapshot (JSON) |
| `GET /health` | Health check |

## Next steps (Release 2 candidates)

- Live AIS integration (vessel positions + ETA updates)
- Berth assignment workflow (drag-and-drop reassignment)
- Pilotage scheduling interface
- Multi-port support
- User accounts and role-based views (harbour master vs. pilot coordinator vs. terminal)
- Notification / alert dispatch (email, SMS)
