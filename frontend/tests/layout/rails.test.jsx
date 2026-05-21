// rails.test.jsx — Left + Right rail visual-remediation invariants.
//
// Asserts the M2 visual-remediation slice has moved the operator
// alerts / decision card placeholder content from the LEFT rail to
// the RIGHT rail (per Operational UX Direction PR #58 §3.1 — right
// rail is the long-term coordination / action surface).
//
// No interactive behaviour is added by the slice; these are
// content-placement assertions.

import React from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { describe, it, expect } from 'vitest';
import { LeftPanel } from '../../src/layout/LeftPanel.jsx';
import { RightPanel } from '../../src/layout/RightPanel.jsx';

function render(node) {
  return renderToStaticMarkup(node);
}

describe('Left rail — supporting context only after remediation', () => {
  const html = render(<LeftPanel />);

  it('carries "Supporting context" heading', () => {
    expect(html).toMatch(/Supporting context/);
    expect(html).toMatch(/SUPPORTING CONTEXT/); // pill label
  });

  it('does NOT contain the operator alerts / decision card placeholder', () => {
    expect(html).not.toMatch(/Operator alerts/);
    expect(html).not.toMatch(/decision card/i);
    expect(html).not.toMatch(/Active Decision/);
    expect(html).not.toMatch(/ACTION &/);
  });

  it('does NOT contain operator-action affordances', () => {
    expect(html).not.toMatch(/<button\b/);
    expect(html).not.toMatch(/<input\b/);
    expect(html).not.toMatch(/<form\b/);
    expect(html).not.toMatch(/\bACK\b/);
    expect(html).not.toMatch(/\bCOMMIT\b/);
    expect(html).not.toMatch(/\bDEFER\b/);
    expect(html).not.toMatch(/\bOVERRIDE\b/);
    expect(html).not.toMatch(/\bESCALATE\b/);
    expect(html).not.toMatch(/\bAcknowledge\b/);
  });

  it('does NOT contain a port selector UI control', () => {
    expect(html).not.toMatch(/<select\b/);
    expect(html).not.toMatch(/<option\b/);
    expect(html).not.toMatch(/Select port|Switch port|Change port/i);
  });

  it('uses the hz-panel-left class', () => {
    expect(html).toMatch(/class="[^"]*hz-panel-left/);
  });
});

describe('Right rail — action and coordination surface after remediation', () => {
  const html = render(<RightPanel />);

  it('carries the ACTION & COORDINATION label and the decision card placeholder heading', () => {
    expect(html).toMatch(/ACTION &amp; COORDINATION|ACTION & COORDINATION/);
    expect(html).toMatch(/Operator alerts/);
    expect(html).toMatch(/decision card/i);
  });

  it('says explicitly that this is the long-term action and coordination surface', () => {
    expect(html).toMatch(/long-term action and coordination surface/);
  });

  it('does NOT carry the old M0 "Vessel roster & audit log" placeholder text', () => {
    expect(html).not.toMatch(/Vessel roster &amp; audit log/);
    expect(html).not.toMatch(/Vessel roster & audit log/);
    expect(html).not.toMatch(/M0 PLACEHOLDER/);
  });

  it('explicitly states operator actions are NOT yet implemented (M3+ scope)', () => {
    expect(html).toMatch(/Not yet implemented/);
    expect(html).toMatch(/M3\+ scope/);
  });

  it('does NOT introduce write actions or operator-action affordances', () => {
    expect(html).not.toMatch(/<button\b/);
    expect(html).not.toMatch(/<input\b/);
    expect(html).not.toMatch(/<form\b/);
    expect(html).not.toMatch(/<select\b/);
  });

  it('does NOT contain a port selector UI control', () => {
    // The placeholder copy legitimately lists the captured-fixture
    // port names ("Brisbane / Melbourne / Geelong / Darwin snapshots")
    // — that is documentation copy, not a port-switching UI control.
    // The gate is the absence of <select>/<option> elements and any
    // "Select / Switch / Change port" UI labels.
    expect(html).not.toMatch(/<select\b/);
    expect(html).not.toMatch(/<option\b/);
    expect(html).not.toMatch(/Select port|Switch port|Change port/i);
  });

  it('uses the hz-panel-right class', () => {
    expect(html).toMatch(/class="[^"]*hz-panel-right/);
  });
});

describe('Combined rails — placement invariant (decision card lives on the right)', () => {
  const leftHtml  = render(<LeftPanel />);
  const rightHtml = render(<RightPanel />);

  it('"Operator alerts" appears in the RIGHT rail only, not the LEFT', () => {
    expect(rightHtml).toMatch(/Operator alerts/);
    expect(leftHtml).not.toMatch(/Operator alerts/);
  });

  it('"ACTION &amp; COORDINATION" appears in the RIGHT rail only', () => {
    expect(rightHtml).toMatch(/ACTION &amp; COORDINATION|ACTION & COORDINATION/);
    expect(leftHtml).not.toMatch(/ACTION &amp; COORDINATION|ACTION & COORDINATION/);
  });

  it('"Supporting context" appears in the LEFT rail only', () => {
    expect(leftHtml).toMatch(/Supporting context/);
    expect(rightHtml).not.toMatch(/Supporting context/);
  });
});
