import * as THREE from 'three';
import { CONFIG } from './config.js';
import { hash2, fbm, spread, seedFrom, setShadows, texturedMesh } from './relief.js';

export const STAGE_ORDER = ['meadow', 'forest', 'desert', 'iceberg', 'volcano'];

const { clamp, smoothstep } = THREE.MathUtils;

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
  // depth cue on a texture-less ground — but the stored normal attribute must
  // stay: without it the ground cannot receive cast shadows (see relief.js).
  geo.deleteAttribute('uv');
  const mesh = new THREE.Mesh(geo, new THREE.MeshLambertMaterial({ vertexColors: true, flatShading: true }));
  mesh.receiveShadow = true;
  return mesh;
}

// Terrain is deterministic per theme, so build each ground once and reuse
// it across stage loads — retries would otherwise leak ~1 MB of
// undisposed geometry each (stage groups are regenerated, not disposed).
const groundCache = new Map();

// Each maker returns { mesh, radius, height } (base at y=0) plus an
// optional sway handle for wind animation. Texture seeds go through
// seedFrom() on values already drawn from the rng, so every instance gets
// its own bark/crag pattern without shifting the pinned layouts.
function makeTree(rng) {
  const g = new THREE.Group();
  const trunkH = rng.range(1.2, 2.0);
  const seed = seedFrom(trunkH);
  const trunk = texturedMesh(new THREE.CylinderGeometry(0.22, 0.4, trunkH, 7, 4), {
    dark: 0x4a3220, light: 0x7d5a38, seed, amp: 0.05, freq: 6, grainY: 0.25,
  });
  trunk.position.y = trunkH / 2;
  g.add(trunk);
  let y = trunkH;
  // Canopy cones join the wind sway (visual only — the collision cylinder
  // and cover logic stay put). Phase from trunk height so neighboring trees
  // never swing in lockstep.
  const sway = { parts: [], phase: trunkH * 37 };
  for (const r of [1.5, 1.1]) {
    // Roughened cones read as clumped boughs instead of party hats.
    const cone = texturedMesh(new THREE.ConeGeometry(r, 2.2, 7, 3), {
      dark: 0x24501f, light: 0x549540, seed: seed + Math.round(r * 100), amp: 0.18, freq: 2.5,
    });
    cone.position.y = y + 1.1;
    g.add(cone);
    sway.parts.push(cone);
    y += 1.4;
  }
  return { mesh: g, radius: 1.5, height: y + 1.8, sway };
}

// Squat roughened boulder, palette per biome (desert sandstone, meadow granite).
function makeRock(rng, dark, light, rMin = 1.2, rMax = 2.2) {
  const r = rng.range(rMin, rMax);
  const rock = texturedMesh(new THREE.DodecahedronGeometry(r, 1), {
    dark, light, seed: seedFrom(r), amp: r * 0.16, freq: 1.6,
  });
  rock.position.y = r * 0.6;
  rock.scale.y = 0.7;
  const g = new THREE.Group();
  g.add(rock);
  return { mesh: g, radius: r, height: r * 1.1 };
}

function makeDesertObstacle(rng) {
  if (rng.random() < 0.5) {
    // saguaro cactus: trunk + two arms, ribbed skin via lengthwise grain
    const g = new THREE.Group();
    const h = rng.range(2.5, 3.5);
    const seed = seedFrom(h);
    const skin = { dark: 0x2e5f36, light: 0x5c9455, amp: 0.04, freq: 7, grainY: 0.2 };
    const trunk = texturedMesh(new THREE.CylinderGeometry(0.35, 0.4, h, 8, 4), { ...skin, seed });
    trunk.position.y = h / 2;
    g.add(trunk);
    for (const side of [-1, 1]) {
      const arm = texturedMesh(new THREE.CylinderGeometry(0.22, 0.22, 1.4, 6, 3), { ...skin, seed: seed + side });
      arm.position.set(side * 0.6, h * 0.6, 0);
      arm.rotation.z = side * 0.5;
      g.add(arm);
    }
    return { mesh: g, radius: 1.0, height: h };
  }
  return makeRock(rng, 0x7d5e40, 0xc5a072);
}

function makeMeadowObstacle(rng) {
  if (rng.random() < 0.5) {
    // leafy bush: a squashed roughened ball, low cover for the gentle stage
    const r = rng.range(0.9, 1.4);
    const bush = texturedMesh(new THREE.IcosahedronGeometry(r, 1), {
      dark: 0x2f6b28, light: 0x6fae4b, seed: seedFrom(r), amp: r * 0.18, freq: 2.2,
    });
    bush.position.y = r * 0.55;
    bush.scale.y = 0.75;
    const g = new THREE.Group();
    g.add(bush);
    return { mesh: g, radius: r, height: r * 1.2 };
  }
  return makeRock(rng, 0x6a6f6a, 0xa9b0a5, 1.0, 1.8); // granite boulder
}

// Faceted spire, palette per biome (ice pillar, obsidian crag): a tapered
// six-sided column with a faint inner glow.
function makeSpire(rng, { hMin, hMax, topMin, topMax, dark, light, emissive, amp }) {
  const h = rng.range(hMin, hMax);
  const spire = texturedMesh(
    new THREE.CylinderGeometry(rng.range(topMin, topMax), rng.range(1.0, 1.6), h, 6, 4),
    { dark, light, seed: seedFrom(h), amp, freq: 2, emissive },
  );
  spire.position.y = h / 2;
  const g = new THREE.Group();
  g.add(spire);
  return { mesh: g, radius: 1.4, height: h };
}

function makeIcePillar(rng) {
  return makeSpire(rng, {
    hMin: 2.5, hMax: 4.5, topMin: 0.6, topMax: 1.0,
    dark: 0x9cc8e4, light: 0xf4fbff, emissive: 0x224455, amp: 0.14,
  });
}

function makeVolcanoObstacle(rng) {
  // Obsidian crag: dark glass, ember glow seeping from the facets.
  return makeSpire(rng, {
    hMin: 2.2, hMax: 4.2, topMin: 0.4, topMax: 0.8,
    dark: 0x17121a, light: 0x3d3242, emissive: 0x551d05, amp: 0.16,
  });
}

const THEMES = {
  meadow: {
    sky: 0x9fd4ef, fog: [0x9fd4ef, 45, 140],
    groundColors: [0x3f8a33, 0x86c455],
    terrain: { seed: 44, freq: 0.045, colorFreq: 0.07, hillHeight: 4 },
    sun: 0xfff4e0, sunIntensity: 1.5, ambient: 0x7d8b95,
    obstacleCount: 12, obstacle: makeMeadowObstacle, perch: 0x7a7f70,
  },
  forest: {
    sky: 0x87b5d4, fog: [0x87b5d4, 40, 130],
    groundColors: [0x2e5c28, 0x579a48],
    terrain: { seed: 11, freq: 0.05, colorFreq: 0.08, hillHeight: 5 },
    sun: 0xfff4e0, sunIntensity: 1.4, ambient: 0x777788,
    obstacleCount: 26, obstacle: makeTree, perch: 0x6b6f66,
  },
  desert: {
    sky: 0xf2d9a8, fog: [0xf2d9a8, 50, 150],
    groundColors: [0xc2984e, 0xeed091],
    terrain: { seed: 22, freq: 0.035, colorFreq: 0.05, hillHeight: 5 },
    sun: 0xfff0c8, sunIntensity: 1.7, ambient: 0x998877,
    obstacleCount: 14, obstacle: makeDesertObstacle, perch: 0x96703f,
  },
  iceberg: {
    sky: 0xbfe3f2, fog: [0xbfe3f2, 35, 120],
    groundColors: [0xaed4ea, 0xf6fbff],
    terrain: { seed: 33, freq: 0.06, colorFreq: 0.07, hillHeight: 6 },
    sun: 0xe8f4ff, sunIntensity: 1.5, ambient: 0x8899aa,
    obstacleCount: 18, obstacle: makeIcePillar, perch: 0x9dbfd1,
  },
  volcano: {
    // Ember dusk: the fog band glows redder than the sky, so the horizon
    // reads as a lava glow line.
    sky: 0x3a201c, fog: [0x4a2419, 30, 110],
    groundColors: [0x241d1b, 0x54402f],
    terrain: { seed: 55, freq: 0.05, colorFreq: 0.06, hillHeight: 7 },
    sun: 0xff9a52, sunIntensity: 1.2, ambient: 0x553333,
    obstacleCount: 16, obstacle: makeVolcanoObstacle, perch: 0x3a3236,
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
  perch.castShadow = true; // grounds the platform on the terrain
  group.add(perch);

  // Fill lights sit a notch below the old values: cast shadows only read if
  // the sun's contribution dominates the shadowed-side fill.
  const bounce = new THREE.Color(theme.groundColors[0]).lerp(new THREE.Color(theme.groundColors[1]), 0.5);
  group.add(new THREE.HemisphereLight(theme.sky, bounce, 0.65));
  const sun = new THREE.DirectionalLight(theme.sun, theme.sunIntensity);
  // High and to the right, barely downrange: shadows rake sideways across
  // the battlefield (fully visible from the perch) while enemies facing the
  // player keep a lit front — they must stay readable targets.
  sun.position.set(32, 42, 6);
  sun.castShadow = true;
  // 512² keeps the shadow pass affordable on software renderers (e2e runs
  // on SwiftShader); PCF softens the coarser texels and the low-poly look
  // hides the rest.
  sun.shadow.mapSize.set(512, 512);
  const sc = sun.shadow.camera;
  sc.left = -48; sc.right = 48; sc.top = 48; sc.bottom = -48; // arena + shadow reach
  sc.near = 5; sc.far = 150;
  sc.updateProjectionMatrix();
  // Flat-shaded low-poly acne is killed by a normal-space bias; a depth bias
  // this small avoids peter-panning on thin trunks and limbs.
  sun.shadow.normalBias = 0.4;
  group.add(sun, sun.target); // target defaults to the origin; must be in-scene
  group.add(new THREE.AmbientLight(theme.ambient, 0.3));

  // Visible sun: a fog-exempt disc + soft halo far along the light direction
  // (inside the camera far plane, beyond the fog band).
  const sunDir = sun.position.clone().normalize();
  const disc = new THREE.Mesh(
    new THREE.CircleGeometry(9, 24),
    new THREE.MeshBasicMaterial({ color: 0xfffbe8, fog: false }),
  );
  disc.position.copy(sunDir).multiplyScalar(230);
  disc.lookAt(px, py, pz);
  const halo = new THREE.Mesh(
    new THREE.CircleGeometry(20, 24),
    new THREE.MeshBasicMaterial({
      color: theme.sun, fog: false, transparent: true, opacity: 0.35, depthWrite: false,
    }),
  );
  halo.position.copy(sunDir).multiplyScalar(234);
  halo.lookAt(px, py, pz);
  group.add(disc, halo);

  // Obstacles scattered over the battlefield, clear of the player perch.
  const obstacles = [];
  const swayers = [];
  for (let i = 0; i < theme.obstacleCount; i++) {
    const x = rng.range(-36, 36);
    const z = rng.range(-30, 22);
    const { mesh, radius, height, sway } = theme.obstacle(rng);
    mesh.position.set(x, 0, z);
    setShadows(mesh);
    if (sway) swayers.push(sway);
    group.add(mesh);
    obstacles.push({ x, z, radius, height });
  }

  // Per-frame ambient motion, owned by the stage: gentle canopy sway on two
  // incommensurate frequencies per tree so the movement never loops visibly.
  // Amplitudes are small — wind, not a storm.
  function animate(t) {
    for (const s of swayers) {
      s.parts.forEach((part, i) => {
        part.rotation.x = Math.sin(t * 1.2 + s.phase + i * 0.7) * 0.025;
        part.rotation.z = Math.cos(t * 0.8 + s.phase * 1.7 + i * 0.7) * 0.02;
      });
    }
  }

  return { group, obstacles, animate, sky: theme.sky, fog: theme.fog };
}
