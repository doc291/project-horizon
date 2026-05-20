import React from 'react';
import { Card } from '../components/Card.jsx';
import { Pill } from '../components/Pill.jsx';
import { SAMPLE } from '../data/sample.js';

// LeftPanel — Canon §1.1.2 + §4.4.
// M0 contents:
//   1. (M0 placeholder) card explaining what arrives in later milestones
//   2. DesignVerificationSwatch — clearly labelled DESIGN VERIFICATION · DEMO ONLY
//      per Plan v0.2 §6.1. Inert, no interaction, no implied audit state.

export function LeftPanel() {
  return (
    <aside className="hz-panel hz-panel-left">
      <Card>
        <div className="hz-placeholder">
          <Pill tone="info" variant="outline">M0 PLACEHOLDER</Pill>
          <h3>Operator alerts &amp; decision card</h3>
          <p>
            Arrive in later milestones. Active alerts list, the
            Active Decision card, and the DSW are deferred to M2+ per
            <code>HORIZON_V1_EXECUTION_PLAN_v0.1.md</code> §11.
          </p>
        </div>
      </Card>
      <DesignVerificationSwatch />
    </aside>
  );
}

// ── DesignVerificationSwatch ────────────────────────────────────────────
// Static card displaying each Canon §4 lifecycle state purely for visual
// verification of glyph + colour + pill treatments during M0 review.
//
// MUST NOT be read as operator functionality. Renders "Example only" on
// every row. Carries no countdown timers, no audit chip, no chain status,
// no interaction. Removed or relegated to a /dev route in M1+ once real
// lifecycle surfaces ship.

const LIFECYCLE_ROWS = [
  { name: 'RECOMMENDED',   tone: 'info',      glyph: '◆', glyphClass: 'hz-glyph-recommended'  },
  { name: 'ACKNOWLEDGED',  tone: 'success',   glyph: '●', glyphClass: 'hz-glyph-acknowledged' },
  { name: 'COMMITTED',     tone: 'success',   glyph: '■', glyphClass: 'hz-glyph-committed'    },
  { name: 'DEFERRED',      tone: 'warning',   glyph: '⧈', glyphClass: 'hz-glyph-deferred'     },
  { name: 'OVERRIDDEN',    tone: 'override',  glyph: '◇', glyphClass: 'hz-glyph-overridden'   },
  { name: 'ESCALATED',     tone: 'escalated', glyph: '⇡', glyphClass: 'hz-glyph-escalated'    },
  { name: 'EXPIRED',       tone: 'muted',     glyph: '○', glyphClass: 'hz-glyph-expired'      },
];

function DesignVerificationSwatch() {
  // For each lifecycle row, pick the matching demo conflict (if present in
  // the SAMPLE fixture) just to anchor the row label visually. The example
  // text is static "Example only" — no real conflict semantics are exposed.
  return (
    <Card className="hz-design-verification">
      <div className="hz-design-header">
        DESIGN VERIFICATION · DEMO ONLY
      </div>
      <div className="hz-design-sub">
        Visual verification of Canon §4 lifecycle states for M0 review.
        Not an operator surface. None of these states represent real
        recommendations, real operator actions, or real audit chain
        entries. Remove or relegate to a /dev route in M1+.
      </div>
      {LIFECYCLE_ROWS.map((row) => (
        <div className="hz-design-row" key={row.name}>
          <Pill tone={row.tone} variant="tinted">{row.name}</Pill>
          <span className={`hz-glyph ${row.glyphClass}`}>{row.glyph}</span>
          <span className="hz-design-row-example">Example only — no real lifecycle state</span>
          <span className="hz-design-row-name" />
        </div>
      ))}
      <div className="hz-design-sub" style={{ marginTop: 'var(--s-3)', marginBottom: 0 }}>
        Sample fixture also includes one demo conflict per state in
        <code>sample.js → conflicts[]</code> for downstream visual checks
        (count: {SAMPLE.conflicts.length}). None are wired to real actions.
      </div>
    </Card>
  );
}
