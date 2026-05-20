// fixtures.js — shared fixture loader for Vitest.
// Loads the 5 committed fixtures from frontend/public/fixtures/.
//
// Each fixture is captured-or-derived from local server.py per PR #49.

import { readFileSync } from 'node:fs';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const FIXTURES_DIR = resolve(__dirname, '..', 'public', 'fixtures');

function loadJson(filename) {
  const path = resolve(FIXTURES_DIR, filename);
  return JSON.parse(readFileSync(path, 'utf-8'));
}

export const brisbaneBusy = loadJson('brisbane-busy.json');
export const brisbaneQuiet = loadJson('brisbane-quiet.json');
export const melbourneSim = loadJson('melbourne-sim.json');
export const nullFields = loadJson('null-fields.json');
export const malformed = loadJson('malformed.json');

export const ALL_FIXTURES = {
  brisbaneBusy,
  brisbaneQuiet,
  melbourneSim,
  nullFields,
  malformed,
};
