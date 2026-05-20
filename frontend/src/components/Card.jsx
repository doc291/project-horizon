import React from 'react';

// Card — Canon §3.4. Base surface; supports critical (red left-border)
// and accent/selected (teal left-border) variants.
export function Card({ children, variant, className, ...rest }) {
  const classes = ['hz-card'];
  if (variant === 'critical') classes.push('hz-card-critical');
  if (variant === 'accent' || variant === 'selected') classes.push('hz-card-accent');
  if (className) classes.push(className);
  return (
    <div className={classes.join(' ')} {...rest}>
      {children}
    </div>
  );
}
