import React from 'react';

// Dot — Canon §3.3. Status dot with optional pulse animation.
// Tones map to the closed severity + lifecycle palette.
export function Dot({ tone = 'info', pulse = false }) {
  const classes = ['hz-dot', `hz-dot-${tone}`];
  if (pulse) classes.push('hz-dot-pulse');
  return <span className={classes.join(' ')} aria-hidden="true" />;
}
