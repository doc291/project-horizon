// conditionsAdapter.js — Adapter Note §6 (weather + tides + ukc → conditions)
// Merges three raw blocks into the V1 conditions view-model.

import { mapConditionsRating, mapSource } from './status.js';
import { nmToKm } from './units.js';
import { isoToDateOrNull, localShort } from './time.js';

function safeNumber(value, fallback = null) {
  return typeof value === 'number' && isFinite(value) ? value : fallback;
}

export function adaptConditions(rawWeather, rawTides, rawUkc, portTimezone) {
  const weather = rawWeather && typeof rawWeather === 'object' ? rawWeather : {};
  const tides = rawTides && typeof rawTides === 'object' ? rawTides : {};
  const ukc = rawUkc && typeof rawUkc === 'object' ? rawUkc : {};

  const visibilityNm = safeNumber(weather.visibility_nm);
  const visibilityKm = visibilityNm !== null ? nmToKm(visibilityNm) : null;

  return {
    rating: mapConditionsRating(weather.conditions),
    windSpeedKts: safeNumber(weather.wind_speed_kts),
    windBearingDeg: safeNumber(weather.wind_direction_deg),
    windDirLabel: typeof weather.wind_direction_label === 'string' ? weather.wind_direction_label : null,
    windBeaufort: safeNumber(weather.wind_beaufort),
    swellHeightM: safeNumber(weather.swell_height_m),
    swellPeriodS: safeNumber(weather.swell_period_s),
    swellDirLabel: typeof weather.swell_direction_label === 'string' ? weather.swell_direction_label : null,
    visibilityNm,
    visibilityKm,
    pressureHpa: safeNumber(weather.pressure_hpa),
    weatherSource: mapSource(weather.source),
    tideHeightM: safeNumber(tides.current_height_m),
    tideState: typeof tides.state === 'string' ? tides.state : null,
    tideNextTimeIso: typeof tides.next_event_time === 'string' ? tides.next_event_time : null,
    tideNextTime: tides.next_event_time ? localShort(tides.next_event_time, portTimezone) : null,
    tideNextLabel: typeof tides.next_event_type === 'string' ? tides.next_event_type : null,
    tideNextHeightM: safeNumber(tides.next_event_height_m),
    tideMeanM: safeNumber(tides.mean_height_m),
    tideAmplitudeM: safeNumber(tides.amplitude_m),
    tideSource: mapSource(tides.data_source),
    ukcM: safeNumber(ukc.min_ukc_m),
    ukcStatus: typeof ukc.status === 'string' ? ukc.status : null,
  };
}
