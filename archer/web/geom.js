import * as THREE from 'three';

// Closest point on segment [a,b] to point p.
export function segClosest(a, b, p) {
  const ab = new THREE.Vector3().subVectors(b, a);
  const denom = ab.lengthSq();
  const t = denom === 0 ? 0
    : Math.max(0, Math.min(1, new THREE.Vector3().subVectors(p, a).dot(ab) / denom));
  return new THREE.Vector3().copy(a).addScaledVector(ab, t);
}

// Parametric t ∈ [0,1] where segment [a,b] first hits the vertical
// cylinder of radius r around (cx, cz) spanning y ∈ [0, h], or null if it
// misses. Covers both side entry and an arcing shot dropping in through
// the top cap.
function segCylinderT(a, b, cx, cz, r, h) {
  const dx = b.x - a.x, dz = b.z - a.z;
  const fx = a.x - cx, fz = a.z - cz;
  const A = dx * dx + dz * dz;
  const C = fx * fx + fz * fz - r * r;
  let t0 = 0, t1 = 1; // sub-span of [a,b] inside the circle in XZ
  if (A > 0) {
    const B = fx * dx + fz * dz;
    const D = B * B - A * C;
    if (D < 0) return null;
    const s = Math.sqrt(D);
    t0 = Math.max(0, (-B - s) / A);
    t1 = Math.min(1, (-B + s) / A);
    if (t0 > t1) return null;
  } else if (C > 0) return null;
  if (a.y + (b.y - a.y) * t0 <= h) return t0;
  if (b.y < a.y) { // above the cap on entry: does it descend through it?
    const tc = (h - a.y) / (b.y - a.y);
    if (tc <= t1) return tc;
  }
  return null;
}

// First impact along [a,b] against obstacles ({x, z, radius, height}
// cylinders based at y=0), padded by the projectile radius; null if the
// path is clear.
export function obstacleHit(a, b, obstacles, pad) {
  let best = null;
  for (const o of obstacles) {
    const t = segCylinderT(a, b, o.x, o.z, o.radius + pad, o.height);
    if (t !== null && (best === null || t < best)) best = t;
  }
  return best === null ? null : new THREE.Vector3().lerpVectors(a, b, best);
}
