// horizon.js — public entry point for the V1 adapter layer.
//
// Components import only this file. Adapters in ./adapters/ are internal.
//
// M1 read-path only: no writes, no auth, no credentials, no cookies.
// fetchSummary() targets a fixture endpoint (same-origin) by default;
// the base URL is swap-in configurable via VITE_API_BASE for future
// milestones, without component code changes.
//
// Per Adapter Note §3.2: pure functions, no state, no business logic.

import { adaptSummary } from './adapters/summaryAdapter.js';

// Vite injects import.meta.env at build time. In M1, default base is
// "/fixtures" (the static fixture endpoint served by the sandbox).
// In M2+, this can be changed to "/api" or a cross-origin URL.
const API_BASE = (typeof import.meta !== 'undefined' && import.meta?.env?.VITE_API_BASE)
  ? import.meta.env.VITE_API_BASE
  : '/fixtures';

// In M1 the fixture file is brisbane-busy by default; M1.x could rotate.
const DEFAULT_FIXTURE = 'brisbane-busy.json';

/**
 * fetchSummary — single-flight read of the /api/summary equivalent.
 *
 * Returns a parsed ViewSummary on success (Adapter Note §2.2 shape).
 * Throws on HTTP failure or invalid JSON; callers handle.
 *
 * No `credentials: 'include'`, no auth header, no cookie read.
 * GET only.
 */
export async function fetchSummary({ fixture = DEFAULT_FIXTURE } = {}) {
  const url = `${API_BASE}/${fixture}`;
  const response = await fetch(url, {
    method: 'GET',
    headers: { Accept: 'application/json' },
    // Explicitly no credentials. No cookies. Read-only.
    credentials: 'omit',
  });
  if (!response.ok) {
    throw new Error(`fetchSummary: HTTP ${response.status} for ${url}`);
  }
  const raw = await response.json();
  return adaptSummary(raw);
}

// Re-export the React hook for component consumption.
export { useSummary } from '../hooks/useSummary.js';

// Internal adapter exports (for tests only — components must NOT import these directly)
export { adaptSummary };
