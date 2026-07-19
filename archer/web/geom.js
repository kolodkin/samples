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

// First obstacle whose padded circle blocks a walker's XZ lane
// [pos, pos + dir·len], or null. Only obstacles the walker actually moves
// toward count: while hugging a circle the position sits on (or just
// inside) the padded rim, and a grazing t=0 "hit" from that circle must
// not read as a wall across the lane.
export function firstBlockingObstacle(pos, dir, len, r, obstacles) {
  const a = new THREE.Vector3(pos.x, 0, pos.z);
  const b = new THREE.Vector3(pos.x + dir.x * len, 0, pos.z + dir.z * len);
  let best = null;
  let bestT = Infinity;
  for (const o of obstacles) {
    if (dir.x * (o.x - pos.x) + dir.z * (o.z - pos.z) <= 0) continue;
    const t = segCylinderT(a, b, o.x, o.z, o.radius + r, o.height);
    if (t !== null && t < bestT) { bestT = t; best = o; }
  }
  return best;
}

// Push a body circle of radius r at pos out of every obstacle cylinder it
// overlaps (in place, XZ only). Only the radial part of the offending step
// is cancelled, so movers slide around obstacles instead of sticking.
// Obstacles may overlap each other and a push can land inside a neighbor,
// so sweep again until a pass pushes nothing (bounded).
export function pushOutOfObstacles(pos, r, obstacles) {
  for (let pass = 0; pass < 3; pass++) {
    let pushed = false;
    for (const o of obstacles) {
      const minD = o.radius + r;
      let dx = pos.x - o.x, dz = pos.z - o.z;
      let d = Math.hypot(dx, dz);
      if (d >= minD) continue;
      if (d < 1e-6) { dx = 1; dz = 0; d = 1; } // dead center: pick a side
      pos.x = o.x + (dx / d) * minD;
      pos.z = o.z + (dz / d) * minD;
      pushed = true;
    }
    if (!pushed) return;
  }
}

// First impact along [a,b] against obstacles ({x, z, radius, height}
// cylinders based at y=0), padded by the projectile radius; null if the
// path is clear.
export function obstacleHit(a, b, obstacles, pad = 0) {
  let best = null;
  for (const o of obstacles) {
    const t = segCylinderT(a, b, o.x, o.z, o.radius + pad, o.height);
    if (t !== null && (best === null || t < best)) best = t;
  }
  return best === null ? null : new THREE.Vector3().lerpVectors(a, b, best);
}
