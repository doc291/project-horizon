import React from 'react';

// Icon — Lucide-style inline SVGs. M0 subset (M1 expands per Canon §4.8).
// All icons inherit currentColor and accept a size prop.

const SVG_PROPS = {
  fill: 'none',
  stroke: 'currentColor',
  strokeWidth: 1.6,
  strokeLinecap: 'round',
  strokeLinejoin: 'round',
};

const PATHS = {
  anchor: (
    <>
      <circle cx="12" cy="5" r="2" />
      <path d="M12 7v15" />
      <path d="M5 17a7 7 0 0 0 14 0" />
      <path d="M3 17h4" />
      <path d="M17 17h4" />
    </>
  ),
  ship: (
    <>
      <path d="M3 18h18l-2-6H5l-2 6z" />
      <path d="M12 4v8" />
      <path d="M8 8h8" />
    </>
  ),
  wind: (
    <>
      <path d="M3 8h13a3 3 0 1 0-3-3" />
      <path d="M3 16h17a3 3 0 1 1-3 3" />
      <path d="M3 12h10" />
    </>
  ),
  waves: (
    <>
      <path d="M3 14c3-3 6 3 9 0s6 3 9 0" />
      <path d="M3 18c3-3 6 3 9 0s6 3 9 0" />
    </>
  ),
  eye: (
    <>
      <path d="M2 12s4-7 10-7 10 7 10 7-4 7-10 7-10-7-10-7z" />
      <circle cx="12" cy="12" r="3" />
    </>
  ),
  gauge: (
    <>
      <path d="M12 14l4-4" />
      <path d="M3 14a9 9 0 0 1 18 0" />
      <path d="M3 14h2" />
      <path d="M19 14h2" />
    </>
  ),
  alert: (
    <>
      <path d="M12 3L2 21h20L12 3z" />
      <path d="M12 10v5" />
      <path d="M12 18v.01" />
    </>
  ),
  clock: (
    <>
      <circle cx="12" cy="12" r="9" />
      <path d="M12 7v5l3 2" />
    </>
  ),
  thermometer: (
    <>
      <path d="M14 4a2 2 0 1 0-4 0v10a4 4 0 1 0 4 0V4z" />
    </>
  ),
  compass: (
    <>
      <circle cx="12" cy="12" r="9" />
      <path d="M12 5l3 7-3 3-3-3z" />
    </>
  ),
};

export function Icon({ name, size = 16, ...rest }) {
  const path = PATHS[name];
  if (!path) return null;
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 24 24"
      style={{ display: 'inline-block', verticalAlign: 'middle' }}
      aria-hidden="true"
      {...SVG_PROPS}
      {...rest}
    >
      {path}
    </svg>
  );
}
