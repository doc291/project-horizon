// useSummary.js — polling hook for the V1 ViewSummary.
//
// M1 Implementation Plan §9.
//
// Behaviour:
//   - Initial fetch on mount
//   - Polling at `pollInterval` ms (default 30s)
//   - Pause when document.hidden; resume + refetch on visibility-resume
//   - Exponential backoff on failure: 1s, 3s, 9s; then back to pollInterval
//   - Last-known-good cache preserved across failures (data stays set;
//     `error` is the new state)
//   - Single-flight (concurrent fetches are skipped, not queued)
//   - `isStale` = now - lastUpdated > 2 * pollInterval
//
// Read-only — `fetchSummary` is GET-only with credentials: 'omit'.

import { useEffect, useState, useRef, useCallback } from 'react';
import { fetchSummary } from '../api/horizon.js';

const DEFAULT_POLL_MS = 30_000;
const BACKOFF_MS = [1_000, 3_000, 9_000];
const STALE_MULTIPLIER = 2;

export function useSummary({ pollInterval = DEFAULT_POLL_MS, fixture } = {}) {
  const [data, setData] = useState(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState(null);
  const [lastUpdated, setLastUpdated] = useState(null);

  const aliveRef = useRef(true);
  const inFlightRef = useRef(false);
  const timerRef = useRef(null);
  const backoffIdxRef = useRef(0);

  const performFetch = useCallback(async (opts = {}) => {
    if (!aliveRef.current) return;
    if (inFlightRef.current) return; // single-flight
    inFlightRef.current = true;
    try {
      const view = await fetchSummary({ fixture });
      if (!aliveRef.current) return;
      setData(view);
      setLastUpdated(new Date());
      setError(null);
      backoffIdxRef.current = 0;
      setIsLoading(false);
    } catch (err) {
      if (!aliveRef.current) return;
      setError(err);
      setIsLoading(false);
      // Schedule a backoff retry instead of the normal interval
      const idx = backoffIdxRef.current;
      if (idx < BACKOFF_MS.length) {
        const delay = BACKOFF_MS[idx];
        backoffIdxRef.current = idx + 1;
        if (timerRef.current) clearTimeout(timerRef.current);
        timerRef.current = setTimeout(performFetch, delay);
      } else {
        backoffIdxRef.current = 0;
        // fall through to normal cadence below
      }
    } finally {
      inFlightRef.current = false;
    }
  }, [fixture]);

  const scheduleNext = useCallback(() => {
    if (!aliveRef.current) return;
    if (timerRef.current) clearTimeout(timerRef.current);
    timerRef.current = setTimeout(async () => {
      if (document.hidden) {
        // skip; visibilitychange listener will trigger when visible again
        scheduleNext();
        return;
      }
      await performFetch();
      scheduleNext();
    }, pollInterval);
  }, [performFetch, pollInterval]);

  // Initial fetch + polling loop
  useEffect(() => {
    aliveRef.current = true;
    performFetch().then(() => scheduleNext());
    return () => {
      aliveRef.current = false;
      if (timerRef.current) {
        clearTimeout(timerRef.current);
        timerRef.current = null;
      }
    };
  }, [performFetch, scheduleNext]);

  // Tab visibility — resume on visible
  useEffect(() => {
    function onVisibilityChange() {
      if (!document.hidden && aliveRef.current) {
        performFetch();
      }
    }
    document.addEventListener('visibilitychange', onVisibilityChange);
    return () => {
      document.removeEventListener('visibilitychange', onVisibilityChange);
    };
  }, [performFetch]);

  const isStale = lastUpdated
    ? (Date.now() - lastUpdated.getTime() > STALE_MULTIPLIER * pollInterval)
    : false;

  const refetch = useCallback(() => performFetch(), [performFetch]);

  return { data, isLoading, error, lastUpdated, isStale, refetch };
}
