import React from 'react';
import { Card } from './Card.jsx';
import { Pill } from './Pill.jsx';

// EmptyState — loading / error / no-data fallback that preserves the
// Horizon Dark shell aesthetic. Used by App.jsx before first fetch
// completes (Adapter Note §15.5 / §15.7).

export function EmptyState({ isLoading, error }) {
  return (
    <div style={{ padding: 'var(--s-6)', maxWidth: 540, margin: 'var(--s-6) auto' }}>
      <Card>
        <div className="hz-placeholder">
          <Pill tone="info" variant="outline">M1 SANDBOX</Pill>
          {isLoading && (
            <>
              <h3>Loading Horizon V1 sandbox…</h3>
              <p>Fetching captured operational fixture data. Should take less than a second.</p>
            </>
          )}
          {error && (
            <>
              <h3>Could not load sandbox fixtures</h3>
              <p>The static fixture endpoint did not respond.</p>
              <p style={{ marginTop: 'var(--s-3)', fontSize: 12 }}>
                <code>{String(error.message || error)}</code>
              </p>
            </>
          )}
          {!isLoading && !error && (
            <>
              <h3>No data available</h3>
              <p>The sandbox is not currently serving any fixtures.</p>
            </>
          )}
        </div>
      </Card>
    </div>
  );
}
