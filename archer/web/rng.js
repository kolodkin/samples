// Deterministic RNG: all gameplay randomness flows through one seeded
// mulberry32 stream so ?seed=N reproduces a run exactly.
export function createRng(seed) {
  let a = seed >>> 0;
  const random = () => {
    a |= 0; a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
  return {
    random,
    range: (min, max) => min + random() * (max - min),
    int: (min, max) => Math.floor(min + random() * (max - min + 1)),
    pick: (arr) => arr[Math.floor(random() * arr.length)],
  };
}

export function seedFromQuery(params) {
  const s = parseInt(params.get('seed'), 10);
  return Number.isFinite(s) ? s : (Date.now() & 0xffffffff);
}
