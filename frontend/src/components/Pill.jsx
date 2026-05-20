import React from 'react';

// Pill — Canon §3.4. Closed tone set (CRITICAL/WARNING/ADVISORY/INFO/SUCCESS
// + OVERRIDE/ESCALATED lifecycle tones + MUTED for expired). Two variants:
// tinted (default) and outline.
const TONE_CLASS = {
  critical:  'hz-pill-critical',
  warning:   'hz-pill-warning',
  advisory:  'hz-pill-advisory',
  info:      'hz-pill-info',
  success:   'hz-pill-success',
  override:  'hz-pill-override',
  escalated: 'hz-pill-escalated',
  muted:     'hz-pill-muted',
};

export function Pill({ tone = 'info', variant = 'tinted', children }) {
  const classes = ['hz-pill', TONE_CLASS[tone] || TONE_CLASS.info];
  if (variant === 'outline') classes.push('hz-pill-outline');
  return <span className={classes.join(' ')}>{children}</span>;
}
