// units.js — Adapter Note §13.4 (visibility nm ↔ km)
// Pure conversion helpers.

export const NM_TO_KM = 1.852;

export function nmToKm(nm) {
  if (typeof nm !== 'number' || !isFinite(nm)) return null;
  return nm * NM_TO_KM; // UI formats to display precision
}

export function kmToNm(km) {
  if (typeof km !== 'number' || !isFinite(km)) return null;
  return km / NM_TO_KM;
}
