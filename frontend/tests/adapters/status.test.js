import { describe, it, expect } from 'vitest';
import { mapSeverity, mapConditionsRating, mapSource, mapRiskLevel } from '../../src/api/adapters/status.js';

describe('status.js — severity mapping', () => {
  it('maps known lowercase severities to uppercase', () => {
    expect(mapSeverity('critical')).toBe('CRITICAL');
    expect(mapSeverity('high')).toBe('HIGH');
    expect(mapSeverity('medium')).toBe('MEDIUM');
    expect(mapSeverity('low')).toBe('LOW');
  });
  it('falls back to INFO for unknown / non-string', () => {
    expect(mapSeverity('explosive')).toBe('INFO');
    expect(mapSeverity('')).toBe('INFO');
    expect(mapSeverity(null)).toBe('INFO');
    expect(mapSeverity(undefined)).toBe('INFO');
    expect(mapSeverity(42)).toBe('INFO');
  });
});

describe('status.js — conditions rating mapping', () => {
  it('maps known title-case ratings to uppercase', () => {
    expect(mapConditionsRating('Excellent')).toBe('EXCELLENT');
    expect(mapConditionsRating('Good')).toBe('GOOD');
    expect(mapConditionsRating('Moderate')).toBe('MODERATE');
    expect(mapConditionsRating('Poor')).toBe('POOR');
  });
  it('handles case variations gracefully', () => {
    expect(mapConditionsRating('good')).toBe('GOOD');
    expect(mapConditionsRating('GOOD')).toBe('GOOD');
  });
  it('falls back to UNKNOWN', () => {
    expect(mapConditionsRating('Foggy')).toBe('UNKNOWN');
    expect(mapConditionsRating(null)).toBe('UNKNOWN');
    expect(mapConditionsRating(undefined)).toBe('UNKNOWN');
  });
});

describe('status.js — source mapping', () => {
  it('maps known sources to display form', () => {
    expect(mapSource('ais')).toBe('AIS');
    expect(mapSource('mst')).toBe('MST');
    expect(mapSource('qships')).toBe('QShips');
    expect(mapSource('live')).toBe('Live');
    expect(mapSource('simulation')).toBe('Simulation');
    expect(mapSource('mock')).toBe('Simulation');
  });
  it('defaults to Simulation for missing / unknown', () => {
    expect(mapSource(null)).toBe('Simulation');
    expect(mapSource(undefined)).toBe('Simulation');
    expect(mapSource('')).toBe('Simulation');
    expect(mapSource('weird-source')).toBe('Simulation');
  });
});

describe('status.js — risk level mapping', () => {
  it('preserves known risk levels', () => {
    expect(mapRiskLevel('critical')).toBe('critical');
    expect(mapRiskLevel('high')).toBe('high');
    expect(mapRiskLevel('medium')).toBe('medium');
    expect(mapRiskLevel('low')).toBe('low');
  });
  it('defaults to low for unknown / non-string', () => {
    expect(mapRiskLevel('explosive')).toBe('low');
    expect(mapRiskLevel(null)).toBe('low');
    expect(mapRiskLevel(99)).toBe('low');
  });
});
