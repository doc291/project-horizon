import React from 'react';

// CategoryPill — Canon §4.5. Teal-outlined label for category tags
// (BERTH, WEATHER, NAVIGATION, OPS, RESOURCES, etc.).
export function CategoryPill({ children }) {
  return <span className="hz-pill hz-pill-category">{children}</span>;
}
