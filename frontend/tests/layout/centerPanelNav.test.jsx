// centerPanelNav.test.jsx — M2 Centre Panel Navigation (final slice).
//
// CenterPanelNav is a controlled component: { activeTab, onChange }.
// These tests verify markup-level invariants via
// react-dom/server.renderToStaticMarkup. They do NOT exercise the
// click handler (that requires DOM); the component contract is that
// the wrapper carries data-active-tab and each tab carries
// aria-selected matching the prop.

import React from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { describe, it, expect } from 'vitest';
import {
  CenterPanelNav,
  CENTRE_PANEL_TABS,
} from '../../src/layout/CenterPanelNav.jsx';

function render(node) {
  return renderToStaticMarkup(node);
}

describe('CenterPanelNav — controlled view-tab strip (M2 final slice)', () => {
  it('renders all six centre-spine tabs', () => {
    const html = render(<CenterPanelNav activeTab="dashboard" onChange={() => {}} />);
    expect(CENTRE_PANEL_TABS.length).toBe(6);
    for (const tab of CENTRE_PANEL_TABS) {
      expect(html).toContain(tab.label);
    }
  });

  it('exposes the active tab key on data-active-tab and aria-selected', () => {
    for (const tab of CENTRE_PANEL_TABS) {
      const html = render(<CenterPanelNav activeTab={tab.key} onChange={() => {}} />);
      expect(html).toMatch(new RegExp(`data-active-tab="${tab.key}"`));
      // Exactly one button has aria-selected="true"; the others false.
      const trues = (html.match(/aria-selected="true"/g) || []).length;
      const falses = (html.match(/aria-selected="false"/g) || []).length;
      expect(trues).toBe(1);
      expect(falses).toBe(5);
    }
  });

  it('marks the active tab with the active CSS class', () => {
    const html = render(<CenterPanelNav activeTab="vts" onChange={() => {}} />);
    expect(html).toMatch(/hz-center-nav-tab-active/);
  });

  it('falls back to dashboard when activeTab is unknown / missing', () => {
    const html1 = render(<CenterPanelNav activeTab="not-a-tab" onChange={() => {}} />);
    expect(html1).toMatch(/data-active-tab="dashboard"/);
    const html2 = render(<CenterPanelNav activeTab={undefined} onChange={() => {}} />);
    expect(html2).toMatch(/data-active-tab="dashboard"/);
  });

  it('uses role="tablist" / role="tab" for ARIA navigation semantics', () => {
    const html = render(<CenterPanelNav activeTab="dashboard" onChange={() => {}} />);
    expect(html).toMatch(/role="tablist"/);
    expect((html.match(/role="tab"/g) || []).length).toBe(6);
  });

  it('does NOT introduce a port selector or any port-switching affordance', () => {
    const html = render(<CenterPanelNav activeTab="dashboard" onChange={() => {}} />);
    // No labels suggesting cross-port navigation.
    expect(html).not.toMatch(/\bPort\b/);
    expect(html).not.toMatch(/Brisbane|Melbourne|Geelong|Darwin/);
    expect(html).not.toMatch(/Select port|Switch port|Change port/i);
    expect(html).not.toMatch(/<select\b/);
    expect(html).not.toMatch(/<option\b/);
  });

  it('renders <button type="button"> elements for tab triggers (not links / form submits)', () => {
    const html = render(<CenterPanelNav activeTab="dashboard" onChange={() => {}} />);
    const buttonCount = (html.match(/<button[^>]*type="button"/g) || []).length;
    expect(buttonCount).toBe(6);
    // No anchor-based navigation (no URL routing).
    expect(html).not.toMatch(/<a\b/);
    // No form-submit semantics.
    expect(html).not.toMatch(/type="submit"/);
  });

  it('only exposes view tabs — not operator-action affordances', () => {
    const html = render(<CenterPanelNav activeTab="dashboard" onChange={() => {}} />);
    expect(html).not.toMatch(/\bACK\b/);
    expect(html).not.toMatch(/\bCOMMIT\b/);
    expect(html).not.toMatch(/\bDEFER\b/);
    expect(html).not.toMatch(/\bOVERRIDE\b/);
    expect(html).not.toMatch(/\bESCALATE\b/);
    expect(html).not.toMatch(/\bAcknowledge\b/);
  });

  it('CENTRE_PANEL_TABS is frozen and ordered Dashboard, Berth Timeline, Shift Log, VTS, Pilotage, Performance', () => {
    expect(Object.isFrozen(CENTRE_PANEL_TABS)).toBe(true);
    expect(CENTRE_PANEL_TABS.map(t => t.key)).toEqual([
      'dashboard',
      'berth-timeline',
      'shift-log',
      'vts',
      'pilotage',
      'performance',
    ]);
  });
});
