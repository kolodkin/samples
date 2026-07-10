import * as THREE from 'three';
import { CONFIG } from './config.js';

export const STAGE_ORDER = ['forest', 'desert', 'iceberg'];

function lambert(color) { return new THREE.MeshLambertMaterial({ color }); }

// --- Terrain noise -----------------------------------------------------
// Deterministic hash noise keyed on vertex position — never the seeded
// game rng: tests pin per-seed obstacle layouts, and terrain must not
// shift that stream (see SPEC.md).
function hash2(ix, iz, seed) {
  let h = (ix * 374761393 + iz * 668265263 + seed * 1442695041) | 0;
  h = Math.imul(h ^ (h >>> 13), 1274126177);
  h ^= h >>> 16;
  return (h >>> 0) / 4294967296;
}

const { clamp, lerp, smoothstep } = THREE.MathUtils;

function valueNoise(x, z, seed) {
  const ix = Math.floor(x), iz = Math.floor(z);
  const fx = smoothstep(x - ix, 0, 1), fz = smoothstep(z - iz, 0, 1);
  const a = hash2(ix, iz, seed), b = hash2(ix + 1, iz, seed);
  const c = hash2(ix, iz + 1, seed), d = hash2(ix + 1, iz + 1, seed);
  return lerp(lerp(a, b, fx), lerp(c, d, fx), fz);
}

function fbm(x, z, seed) {
  let sum = 0, amp = 1, freq = 1, norm = 0;
  for (let i = 0; i < 3; i++) {
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
  return clamp((n - 0.5) * k + 0.5, 0, 1);
}

const MICRO_RELIEF = 0.12;

function makeGround(theme, size) {
  // Gameplay assumes a flat y=0 battlefield (arrows, walkers, melee/cover —
  // see SPEC.md), so relief inside the play area caps at MICRO_RELIEF and
  // real hills rise only beyond it, framed by the fog band.
  const flatExtent = CONFIG.arena.size / 2;
  const hillExtent = size / 2 - 2;
  // 96 segments keeps facets ~1.25 u — chunky enough for the low-poly look
  // and cheap enough for software renderers (the mesh is the biggest single
  // draw in the scene).
  const seg = 96;
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
    const edge = smoothstep(Math.max(Math.abs(x), Math.abs(z)), flatExtent, hillExtent);
    const hill = edge > 0 ? spread(fbm(x * t.freq, z * t.freq, t.seed), 1.8) * t.hillHeight : 0;
    const micro = edge < 1 ? (spread(fbm(x * 0.3, z * 0.3, t.seed + 7), 2.5) - 0.5) * 2 * MICRO_RELIEF : 0;
    pos.setY(i, micro * (1 - edge) + hill * edge);
    // Two-tone patches around the theme ground color; hill crests pull
    // toward the light tone so the perimeter relief reads through the fog.
    const patch = spread(fbm(x * t.colorFreq, z * t.colorFreq, t.seed + 31), 2.5);
    // Per-vertex jitter mottles individual facets; the mottle visually
    // compresses with distance — a texture-gradient depth cue.
    const jitter = (hash2(Math.round(x * 4), Math.round(z * 4), t.seed + 53) - 0.5) * 0.5;
    const shade = clamp(patch + jitter + edge * 0.3, 0, 1);
    col.lerpColors(dark, light, shade);
    col.toArray(colors, i * 3);
  }
  geo.setAttribute('color', new THREE.BufferAttribute(colors, 3));
  // flatShading lights each facet by an in-shader derived normal — the main
  // depth cue on a texture-less ground — so the stored normals (and the uvs
  // of a map-less material) are dead weight.
  geo.deleteAttribute('normal');
  geo.deleteAttribute('uv');
  return new THREE.Mesh(geo, new THREE.MeshLambertMaterial({ vertexColors: true, flatShading: true }));
}

// Terrain is deterministic per theme, so build each ground once and reuse
// it across stage loads — retries would otherwise leak ~1 MB of
// undisposed geometry each (stage groups are regenerated, not disposed).
const groundCache = new Map();

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
    sky: 0x87b5d4, fog: [0x87b5d4, 40, 130],
    groundColors: [0x2e5c28, 0x579a48],
    terrain: { seed: 11, freq: 0.05, colorFreq: 0.08, hillHeight: 5 },
    sun: 0xfff4e0, sunIntensity: 1.0, ambient: 0x777788,
    obstacleCount: 26, obstacle: makeTree, perch: 0x6b6f66,
  },
  desert: {
    sky: 0xf2d9a8, fog: [0xf2d9a8, 50, 150],
    groundColors: [0xc2984e, 0xeed091],
    terrain: { seed: 22, freq: 0.035, colorFreq: 0.05, hillHeight: 5 },
    sun: 0xfff0c8, sunIntensity: 1.4, ambient: 0x998877,
    obstacleCount: 14, obstacle: makeDesertObstacle, perch: 0x96703f,
  },
  iceberg: {
    sky: 0xbfe3f2, fog: [0xbfe3f2, 35, 120],
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
  if (!groundCache.has(name)) groundCache.set(name, makeGround(theme, size));
  group.add(groundCache.get(name));

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

  const bounce = new THREE.Color(theme.groundColors[0]).lerp(new THREE.Color(theme.groundColors[1]), 0.5);
  group.add(new THREE.HemisphereLight(theme.sky, bounce, 0.9));
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
