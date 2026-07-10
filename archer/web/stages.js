import * as THREE from 'three';
import { CONFIG } from './config.js';

export const STAGE_ORDER = ['forest', 'desert', 'iceberg'];

function lambert(color) { return new THREE.MeshLambertMaterial({ color }); }

// --- Terrain noise -----------------------------------------------------
// Deterministic hash-based value noise keyed on vertex position. It must
// NOT draw from the game's seeded rng stream: tests pin per-seed obstacle
// layouts (e.g. skeleton cover on seed 42), so terrain generation cannot
// shift that stream.
function hash2(ix, iz, seed) {
  let h = (ix * 374761393 + iz * 668265263 + seed * 1442695041) | 0;
  h = Math.imul(h ^ (h >>> 13), 1274126177);
  h ^= h >>> 16;
  return (h >>> 0) / 4294967296;
}

function smoothstep(a, b, x) {
  const t = Math.min(1, Math.max(0, (x - a) / (b - a)));
  return t * t * (3 - 2 * t);
}

function valueNoise(x, z, seed) {
  const ix = Math.floor(x), iz = Math.floor(z);
  const fx = smoothstep(0, 1, x - ix), fz = smoothstep(0, 1, z - iz);
  const a = hash2(ix, iz, seed), b = hash2(ix + 1, iz, seed);
  const c = hash2(ix, iz + 1, seed), d = hash2(ix + 1, iz + 1, seed);
  return a + (b - a) * fx + (c - a) * fz + (a - b - c + d) * fx * fz;
}

function fbm(x, z, seed, octaves = 3) {
  let sum = 0, amp = 1, freq = 1, norm = 0;
  for (let i = 0; i < octaves; i++) {
    sum += valueNoise(x * freq, z * freq, seed + i * 101) * amp;
    norm += amp;
    amp *= 0.5;
    freq *= 2;
  }
  return sum / norm; // 0..1
}

// Averaged-octave value noise clusters around 0.5; expand around the
// midpoint so tints and relief actually use their full range.
function spread(n, k) {
  return Math.min(1, Math.max(0, (n - 0.5) * k + 0.5));
}

// The battlefield must stay a flat y=0 plane — arrows die at y<=0.05,
// enemies walk at y=0, and melee/cover logic assume it. Obstacles reach
// |x|<=37.5, spawns z=-34, perch edge z~37, so relief inside |coord|<40 is
// capped at grass-height jitter and real hills only rise beyond the play
// area, framing the horizon inside the fog band.
const FLAT_EXTENT = 40;
const HILL_EXTENT = 58;
const MICRO_RELIEF = 0.12;

function makeGround(theme, size) {
  const seg = 128;
  const geo = new THREE.PlaneGeometry(size, size, seg, seg);
  geo.rotateX(-Math.PI / 2);
  const pos = geo.attributes.position;
  const colors = new Float32Array(pos.count * 3);
  const dark = new THREE.Color(theme.groundColors[0]);
  const light = new THREE.Color(theme.groundColors[1]);
  const col = new THREE.Color();
  const t = theme.terrain;
  for (let i = 0; i < pos.count; i++) {
    const x = pos.getX(i), z = pos.getZ(i);
    const edge = smoothstep(FLAT_EXTENT, HILL_EXTENT, Math.max(Math.abs(x), Math.abs(z)));
    const hill = spread(fbm(x * t.freq, z * t.freq, t.seed), 1.8) * t.hillHeight;
    const micro = (spread(fbm(x * 0.3, z * 0.3, t.seed + 7), 2.5) - 0.5) * 2 * MICRO_RELIEF;
    pos.setY(i, micro * (1 - edge) + hill * edge);
    // Two-tone patches around the theme ground color; hill crests pull
    // toward the light tone so the perimeter relief reads through the fog.
    const patch = spread(fbm(x * t.colorFreq, z * t.colorFreq, t.seed + 31), 2.5);
    // Per-vertex jitter mottles individual facets; the mottle visually
    // compresses with distance — a texture-gradient depth cue.
    const jitter = (hash2(Math.round(x * 4), Math.round(z * 4), t.seed + 53) - 0.5) * 0.5;
    const shade = Math.min(1, Math.max(0, patch + jitter + edge * 0.3));
    col.lerpColors(dark, light, shade);
    colors[i * 3] = col.r;
    colors[i * 3 + 1] = col.g;
    colors[i * 3 + 2] = col.b;
  }
  geo.setAttribute('color', new THREE.BufferAttribute(colors, 3));
  // flatShading lights each facet by its own normal — the faceted relief is
  // the main depth cue on an otherwise texture-less ground.
  return new THREE.Mesh(geo, new THREE.MeshLambertMaterial({ vertexColors: true, flatShading: true }));
}

// Each maker returns { mesh, radius, height } with the mesh's base at y=0.
function makeTree(rng) {
  const g = new THREE.Group();
  const trunkH = rng.range(1.2, 2.0);
  const trunk = new THREE.Mesh(new THREE.CylinderGeometry(0.25, 0.35, trunkH, 6), lambert(0x6b4a2f));
  trunk.position.y = trunkH / 2;
  g.add(trunk);
  let y = trunkH;
  for (const r of [1.5, 1.1]) {
    const cone = new THREE.Mesh(new THREE.ConeGeometry(r, 2.2, 7), lambert(0x2f6b2a));
    cone.position.y = y + 1.1;
    g.add(cone);
    y += 1.4;
  }
  return { mesh: g, radius: 1.5, height: y + 1.8 };
}

function makeDesertObstacle(rng) {
  if (rng.random() < 0.5) {
    // saguaro cactus: trunk + two arms
    const g = new THREE.Group();
    const h = rng.range(2.5, 3.5);
    const trunk = new THREE.Mesh(new THREE.CylinderGeometry(0.35, 0.4, h, 8), lambert(0x3f7d46));
    trunk.position.y = h / 2;
    g.add(trunk);
    for (const side of [-1, 1]) {
      const arm = new THREE.Mesh(new THREE.CylinderGeometry(0.22, 0.22, 1.4, 6), lambert(0x3f7d46));
      arm.position.set(side * 0.6, h * 0.6, 0);
      arm.rotation.z = side * 0.5;
      g.add(arm);
    }
    return { mesh: g, radius: 1.0, height: h };
  }
  const r = rng.range(1.2, 2.2);
  const rock = new THREE.Mesh(new THREE.DodecahedronGeometry(r, 0), lambert(0xa8825d));
  rock.position.y = r * 0.6;
  rock.scale.y = 0.7;
  const g = new THREE.Group();
  g.add(rock);
  return { mesh: g, radius: r, height: r * 1.1 };
}

function makeIcePillar(rng) {
  const h = rng.range(2.5, 4.5);
  const pillar = new THREE.Mesh(
    new THREE.CylinderGeometry(rng.range(0.6, 1.0), rng.range(1.0, 1.6), h, 6),
    new THREE.MeshLambertMaterial({ color: 0xbfe8f7, emissive: 0x224455 }),
  );
  pillar.position.y = h / 2;
  const g = new THREE.Group();
  g.add(pillar);
  return { mesh: g, radius: 1.4, height: h };
}

const THEMES = {
  forest: {
    sky: 0x87b5d4, fog: [0x87b5d4, 40, 130], ground: 0x3e7a3a,
    groundColors: [0x2e5c28, 0x579a48],
    terrain: { seed: 11, freq: 0.05, colorFreq: 0.08, hillHeight: 5 },
    sun: 0xfff4e0, sunIntensity: 1.0, ambient: 0x777788,
    obstacleCount: 26, obstacle: makeTree, perch: 0x6b6f66,
  },
  desert: {
    sky: 0xf2d9a8, fog: [0xf2d9a8, 50, 150], ground: 0xd9b36c,
    groundColors: [0xc2984e, 0xeed091],
    terrain: { seed: 22, freq: 0.035, colorFreq: 0.05, hillHeight: 5 },
    sun: 0xfff0c8, sunIntensity: 1.4, ambient: 0x998877,
    obstacleCount: 14, obstacle: makeDesertObstacle, perch: 0x96703f,
  },
  iceberg: {
    sky: 0xbfe3f2, fog: [0xbfe3f2, 35, 120], ground: 0xdef2fb,
    groundColors: [0xaed4ea, 0xf6fbff],
    terrain: { seed: 33, freq: 0.06, colorFreq: 0.07, hillHeight: 6 },
    sun: 0xe8f4ff, sunIntensity: 1.1, ambient: 0x8899aa,
    obstacleCount: 18, obstacle: makeIcePillar, perch: 0x9dbfd1,
  },
};

export function buildStage(name, rng) {
  const theme = THEMES[name];
  const group = new THREE.Group();

  const size = CONFIG.arena.size + 40;
  group.add(makeGround(theme, size));

  const { x: px, y: py, z: pz } = CONFIG.player.pos;
  // The player's elevated vantage point. The top cap is the only face the
  // player ever sees from up there, so it keeps the perch color — matching
  // the ground would make the platform invisible underfoot. Slightly
  // translucent so the battlefield stays visible through the platform edge.
  const perch = new THREE.Mesh(
    new THREE.CylinderGeometry(2.2, 3.0, py - 1, 8),
    new THREE.MeshLambertMaterial({ color: theme.perch, transparent: true, opacity: 0.65 }),
  );
  perch.position.set(px, (py - 1) / 2, pz);
  group.add(perch);

  group.add(new THREE.HemisphereLight(theme.sky, theme.ground, 0.9));
  const sun = new THREE.DirectionalLight(theme.sun, theme.sunIntensity);
  sun.position.set(20, 40, 10);
  group.add(sun);
  group.add(new THREE.AmbientLight(theme.ambient, 0.4));

  // Obstacles scattered over the battlefield, clear of the player perch.
  const obstacles = [];
  for (let i = 0; i < theme.obstacleCount; i++) {
    const x = rng.range(-36, 36);
    const z = rng.range(-30, 22);
    const { mesh, radius, height } = theme.obstacle(rng);
    mesh.position.set(x, 0, z);
    group.add(mesh);
    obstacles.push({ x, z, radius, height });
  }

  return { group, obstacles, sky: theme.sky, fog: theme.fog };
}
