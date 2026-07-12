import * as THREE from 'three';

// Shared low-poly texturing toolkit: hash noise, vertex relief and two-tone
// mottling. The ground, props, viewmodel and monsters all use this recipe so
// the whole scene reads as one material world.
//
// All noise is deterministic hash noise keyed on vertex position — never the
// seeded game rng: tests pin per-seed obstacle layouts, and texturing must
// not shift that stream (see SPEC.md).

export function hash2(ix, iz, seed) {
  let h = (ix * 374761393 + iz * 668265263 + seed * 1442695041) | 0;
  h = Math.imul(h ^ (h >>> 13), 1274126177);
  h ^= h >>> 16;
  return (h >>> 0) / 4294967296;
}

function hash3(ix, iy, iz, seed) {
  return hash2(ix + Math.imul(iy, 7919), iz - Math.imul(iy, 104729), seed);
}

const { clamp, lerp, smoothstep } = THREE.MathUtils;

function valueNoise(x, z, seed) {
  const ix = Math.floor(x), iz = Math.floor(z);
  const fx = smoothstep(x - ix, 0, 1), fz = smoothstep(z - iz, 0, 1);
  const a = hash2(ix, iz, seed), b = hash2(ix + 1, iz, seed);
  const c = hash2(ix, iz + 1, seed), d = hash2(ix + 1, iz + 1, seed);
  return lerp(lerp(a, b, fx), lerp(c, d, fx), fz);
}

export function fbm(x, z, seed) {
  let sum = 0, amp = 1, freq = 1, norm = 0;
  for (let i = 0; i < 3; i++) {
    sum += valueNoise(x * freq, z * freq, seed + i * 101) * amp;
    norm += amp;
    amp *= 0.5;
    freq *= 2;
  }
  return sum / norm; // 0..1
}

function valueNoise3(x, y, z, seed) {
  const ix = Math.floor(x), iy = Math.floor(y), iz = Math.floor(z);
  const fx = smoothstep(x - ix, 0, 1);
  const fy = smoothstep(y - iy, 0, 1);
  const fz = smoothstep(z - iz, 0, 1);
  let n = 0;
  for (const [cy, wy] of [[iy, 1 - fy], [iy + 1, fy]]) {
    const a = hash3(ix, cy, iz, seed), b = hash3(ix + 1, cy, iz, seed);
    const c = hash3(ix, cy, iz + 1, seed), d = hash3(ix + 1, cy, iz + 1, seed);
    n += lerp(lerp(a, b, fx), lerp(c, d, fx), fz) * wy;
  }
  return n;
}

// Averaged-octave value noise clusters around 0.5; expand around the
// midpoint so tints and relief actually use their full range.
export function spread(n, k) {
  return clamp((n - 0.5) * k + 0.5, 0, 1);
}

// Craggy relief: jitter each vertex by a hash of its quantized position.
// Seam duplicates (and a cone tip's vertex fan) share a position, so they
// displace together and the mesh never tears.
export function roughen(geo, amp, seed) {
  const pos = geo.attributes.position;
  for (let i = 0; i < pos.count; i++) {
    const x = pos.getX(i), y = pos.getY(i), z = pos.getZ(i);
    const ix = Math.round(x * 40), iy = Math.round(y * 40), iz = Math.round(z * 40);
    pos.setXYZ(
      i,
      x + (hash3(ix, iy, iz, seed) - 0.5) * 2 * amp,
      y + (hash3(ix, iy, iz, seed + 1) - 0.5) * 2 * amp,
      z + (hash3(ix, iy, iz, seed + 2) - 0.5) * 2 * amp,
    );
  }
}

// Two-tone vertex mottling — the terrain's patches-plus-facet-jitter recipe,
// sampled in 3D so it wraps prop surfaces. grainY < 1 stretches the patches
// along y for a lengthwise wood-grain read on trunks and shafts.
function mottle(geo, darkIn, lightIn, seed, freq, grainY) {
  const pos = geo.attributes.position;
  const colors = new Float32Array(pos.count * 3);
  const dark = new THREE.Color(darkIn), light = new THREE.Color(lightIn);
  const col = new THREE.Color();
  for (let i = 0; i < pos.count; i++) {
    const x = pos.getX(i), y = pos.getY(i), z = pos.getZ(i);
    const patch = spread(valueNoise3(x * freq, y * freq * grainY, z * freq, seed), 2.2);
    const jitter = (hash3(Math.round(x * 25), Math.round(y * 25), Math.round(z * 25), seed + 53) - 0.5) * 0.35;
    col.lerpColors(dark, light, clamp(patch + jitter, 0, 1));
    col.toArray(colors, i * 3);
  }
  geo.setAttribute('color', new THREE.BufferAttribute(colors, 3));
}

// Roughened, mottled, flat-shaded mesh. flatShading lights each facet by an
// in-shader derived normal, so only the uvs of a map-less material are dead
// weight. The normal attribute MUST stay: the shadow-map path reads it, and
// deleting it silently kills shadow reception (three r160 Lambert).
export function texturedMesh(geo, { dark, light, seed = 0, amp = 0, freq = 3, grainY = 1, ...mat }) {
  if (amp > 0) roughen(geo, amp, seed);
  mottle(geo, dark, light, seed, freq, grainY);
  geo.deleteAttribute('uv');
  return new THREE.Mesh(geo, new THREE.MeshLambertMaterial({ vertexColors: true, flatShading: true, ...mat }));
}
