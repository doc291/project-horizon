// horizonHeader.test.jsx — Logo hygiene assertions for the header.
//
// After the M2 hygiene slice, the header must display the Horizon
// brand logo (/logo.png) and the AMS Group co-brand logo (/ams-logo.png)
// as <img> elements, with appropriate alt text. The previous
// placeholder text "HORIZON" wordmark and bordered "AMS Group" cobrand
// chip are no longer present.

import React from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { describe, it, expect } from 'vitest';
import { adaptSummary } from '../../src/api/adapters/summaryAdapter.js';
import { HorizonHeader } from '../../src/layout/HorizonHeader.jsx';
import { brisbaneBusy } from '../fixtures.js';

function render(node) {
  return renderToStaticMarkup(node);
}

describe('HorizonHeader — logo hygiene (M2 logo correction slice)', () => {
  const summary = adaptSummary(brisbaneBusy);

  it('renders without throwing', () => {
    expect(() =>
      render(<HorizonHeader summary={summary} lastUpdated={null} isStale={false} />)
    ).not.toThrow();
  });

  it('displays the Horizon brand logo as an <img> served from /logo.png', () => {
    const html = render(
      <HorizonHeader summary={summary} lastUpdated={null} isStale={false} />
    );
    expect(html).toMatch(/<img[^>]*src="\/logo\.png"[^>]*alt="Horizon"[^>]*>/);
    expect(html).toMatch(/class="[^"]*hz-brand-logo/);
  });

  it('displays the AMS Group cobrand logo as an <img> served from /ams-logo.png', () => {
    const html = render(
      <HorizonHeader summary={summary} lastUpdated={null} isStale={false} />
    );
    expect(html).toMatch(/<img[^>]*src="\/ams-logo\.png"[^>]*alt="AMS Group"[^>]*>/);
    expect(html).toMatch(/class="[^"]*hz-cobrand-logo/);
  });

  it('no longer renders the placeholder "HORIZON" wordmark text or anchor icon', () => {
    const html = render(
      <HorizonHeader summary={summary} lastUpdated={null} isStale={false} />
    );
    // Pre-slice header had <span class="hz-brand-wordmark">HORIZON</span>.
    expect(html).not.toMatch(/class="hz-brand-wordmark"/);
    // Pre-slice header used the anchor Icon as a placeholder mark.
    // The Icon component renders an <svg> with an aria-label of the icon name.
    expect(html).not.toMatch(/aria-label="anchor"/);
  });

  it('no longer renders the bordered "AMS Group" cobrand chip', () => {
    const html = render(
      <HorizonHeader summary={summary} lastUpdated={null} isStale={false} />
    );
    // Pre-slice header had <div class="hz-cobrand">AMS Group</div>. The
    // new className (hz-cobrand-logo) must not match the bare hz-cobrand.
    expect(html).not.toMatch(/class="hz-cobrand"/);
    expect(html).not.toMatch(/class="hz-cobrand "/);
  });

  it('preserves the existing port-name slot and stat tiles (header layout unchanged)', () => {
    const html = render(
      <HorizonHeader summary={summary} lastUpdated={null} isStale={false} />
    );
    // Port name still rendered.
    expect(html).toMatch(/class="hz-brand-port"/);
    // Stat tiles still rendered.
    expect(html).toMatch(/class="hz-header-stats"/);
    // Clock still rendered.
    expect(html).toMatch(/class="hz-clock"/);
  });

  it('does NOT add any operator-action affordances or new buttons', () => {
    const html = render(
      <HorizonHeader summary={summary} lastUpdated={null} isStale={false} />
    );
    expect(html).not.toMatch(/<button\b/);
    expect(html).not.toMatch(/<input\b/);
    expect(html).not.toMatch(/<form\b/);
    expect(html).not.toMatch(/\bACK\b/);
    expect(html).not.toMatch(/\bCOMMIT\b/);
    expect(html).not.toMatch(/\bDEFER\b/);
    expect(html).not.toMatch(/\bOVERRIDE\b/);
    expect(html).not.toMatch(/\bESCALATE\b/);
  });
});
