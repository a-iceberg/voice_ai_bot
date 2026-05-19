const fs = require('fs');
const path = require('path');
const { logger } = require('./config');

const AFFILATES_PATH = path.join(__dirname, 'data', 'affilates.json');

let AFFILATES = {};
try {
  const raw = fs.readFileSync(AFFILATES_PATH, 'utf8');
  const parsed = JSON.parse(raw);
  AFFILATES = (parsed && parsed.affilates) || {};
  logger.info(`[ZONING] Loaded ${Object.keys(AFFILATES).length} affilates from ${AFFILATES_PATH}`);
} catch (e) {
  logger.error(`[ZONING] Failed to load affilates: ${e.message}`);
}

const SHORT_DISTANCE_DIRECTIONS = new Set(['Клининг']);
const MIDDLE_DISTANCE_DIRECTIONS = new Set([
  'Натяжные потолки',
  'Вскрытие замков',
  'Вскрытие и установка замков',
]);

const toRadians = (deg) => (deg * Math.PI) / 180;

function haversineKm(p1, p2) {
  const R = 6371.0088;
  const dLat = toRadians(p2[0] - p1[0]);
  const dLon = toRadians(p2[1] - p1[1]);
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRadians(p1[0])) *
      Math.cos(toRadians(p2[0])) *
      Math.sin(dLon / 2) ** 2;
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return R * c;
}

function findNearestAffilate(lat, lon) {
  let distance = Infinity;
  let affilate = '';
  for (const [aff, boundaries] of Object.entries(AFFILATES)) {
    if (!Array.isArray(boundaries) || boundaries.length === 0) continue;
    const first = boundaries[0];
    if (!Array.isArray(first) || first.length < 2) continue;
    if (Math.abs(lat - first[0]) >= 7 || Math.abs(lon - first[1]) >= 3) continue;
    for (const point of boundaries) {
      if (!Array.isArray(point) || point.length < 2) continue;
      const d = haversineKm([lat, lon], point);
      if (d < distance) {
        distance = d;
        affilate = aff;
      }
    }
  }
  return { distance, affilate };
}

function classifyZone({ affilate, distance, direction }) {
  const isMoscow = typeof affilate === 'string' && affilate.includes('Москва');

  if (isMoscow && distance > 100) return { zone: 'out_of_service' };
  if (!isMoscow && distance > 90) return { zone: 'out_of_service' };

  const dir = direction || '';
  if (isMoscow) {
    if (SHORT_DISTANCE_DIRECTIONS.has(dir) && distance > 10) return { zone: 'paid' };
    if (MIDDLE_DISTANCE_DIRECTIONS.has(dir) && distance > 30) return { zone: 'paid' };
    if (
      !SHORT_DISTANCE_DIRECTIONS.has(dir) &&
      !MIDDLE_DISTANCE_DIRECTIONS.has(dir) &&
      distance > 50
    ) {
      return { zone: 'paid' };
    }
  } else if (distance > 40) {
    return { zone: 'paid' };
  }

  return { zone: 'ok' };
}

function cityFromAffilate(affilate) {
  if (!affilate) return '';
  return String(affilate).split('_', 2)[0];
}

function affilatesCount() {
  return Object.keys(AFFILATES).length;
}

module.exports = {
  findNearestAffilate,
  classifyZone,
  cityFromAffilate,
  affilatesCount,
};