import * as THREE from 'three';
import { CONFIG } from './config.js';
import { segClosest, obstacleHit } from './geom.js';
import { texturedMesh } from './relief.js';

// Wooden shaft with lengthwise grain; radius/length are the caller's.
export function arrowShaft(radius, length) {
  return texturedMesh(new THREE.CylinderGeometry(radius, radius, length, 5, 4), {
    dark: 0xb09a6e, light: 0xe4d6ae, seed: 37, freq: 9, grainY: 0.2,
  });
}

// Four-sided cone + flat shading reads as a forged broadhead.
export function arrowHead(radius, length) {
  return new THREE.Mesh(
    new THREE.ConeGeometry(radius, length, 4),
    new THREE.MeshLambertMaterial({ color: 0x4a4a52, flatShading: true }),
  );
}

// Three swept-back feather vanes at 120°, running along +y toward the nock.
// Unlit and double-sided: the bright type color is the ammo ID and must stay
// readable against any stage.
export function fletching(color, length, height, shaftR) {
  const g = new THREE.Group();
  const geo = new THREE.BufferGeometry();
  geo.setAttribute('position', new THREE.BufferAttribute(new Float32Array([
    shaftR, length / 2, 0,                    // front, on the shaft
    shaftR, -length / 2, 0,                   // rear, on the shaft
    shaftR + height, -length / 4, 0,          // outer corner, swept back
  ]), 3));
  const mat = new THREE.MeshBasicMaterial({ color, side: THREE.DoubleSide });
  for (let i = 0; i < 3; i++) {
    const vane = new THREE.Mesh(geo, mat);
    vane.rotation.y = (i * 2 * Math.PI) / 3;
    g.add(vane);
  }
  return g;
}

// Projectiles of one ammo type are pixel-identical, so the geometries,
// materials and the CPU mottling pass run once per type; every shot clones
// the template — clones share geometry/material (arrows are never tinted
// per-instance), so spent arrows leak nothing.
const arrowTemplates = new Map();

function buildArrowMesh(type) {
  let t = arrowTemplates.get(type);
  if (!t) {
    t = new THREE.Group();
    // Slim proportions matching the arrow nocked on the bow viewmodel — the
    // projectile reads as a thin arrow in flight, not a fat bolt. Collision
    // stays at CONFIG.arrow.radius (gameplay tuning, not the visual).
    const shaft = arrowShaft(0.008, 0.7);
    shaft.castShadow = true; // in-flight ground shadow tracks the arc
    const tip = arrowHead(0.02, 0.08);
    tip.position.y = 0.39;
    const fletch = fletching(CONFIG.arrow.types[type].color, 0.12, 0.03, 0.008);
    fletch.position.y = -0.28;
    t.add(shaft, tip, fletch);
    arrowTemplates.set(type, t);
  }
  return t.clone();
}

// Tracer trail: the arrow's recent flight path as a line, brightest at the
// arrow and fading to black toward the tail (additive blending: black
// vertices contribute nothing). Without it a first-person shot reads as a
// shrinking dot — the arrow flies straight away from the eye, so only its
// rear cross-section is ever visible; the trail is what makes the arc read.
const TRAIL_POINTS = 24;
// One material for all trails (per-arrow color lives in the vertex colors),
// and one fade gradient per arrow type, shared by every trail of that type.
const TRAIL_MATERIAL = new THREE.LineBasicMaterial({
  vertexColors: true, blending: THREE.AdditiveBlending, transparent: true, depthWrite: false,
});
const TRAIL_FADES = new Map();

function trailFade(color) {
  let colors = TRAIL_FADES.get(color);
  if (!colors) {
    colors = new Float32Array(TRAIL_POINTS * 3);
    const c = new THREE.Color(color);
    for (let i = 0; i < TRAIL_POINTS; i++) {
      const k = 1 - i / (TRAIL_POINTS - 1);
      colors.set([c.r * k, c.g * k, c.b * k], i * 3);
    }
    TRAIL_FADES.set(color, colors);
  }
  return colors;
}

function buildTrail(color) {
  const geo = new THREE.BufferGeometry();
  geo.setAttribute('position', new THREE.BufferAttribute(new Float32Array(TRAIL_POINTS * 3), 3));
  geo.setAttribute('color', new THREE.BufferAttribute(trailFade(color), 3));
  geo.setDrawRange(0, 0);
  const line = new THREE.Line(geo, TRAIL_MATERIAL);
  line.frustumCulled = false; // positions mutate per frame; culling would lag
  return line;
}

// Seconds over which a bow-spawned arrow's visual offset blends away.
const SPAWN_BLEND = 0.12;

export class ArrowSystem {
  constructor(game) {
    this.game = game;
    this.list = [];
  }

  get count() { return this.list.length; }

  // `visualOrigin` (the nocked-arrow tip on the bow viewmodel) is where the
  // mesh appears; physics always runs on `pos` from `origin` on the aim
  // line, so where the arrow *lands* is unaffected. The gap between the two
  // blends away over SPAWN_BLEND seconds — the shot visibly leaves the bow
  // and converges onto the flight line instead of popping in at the
  // crosshair as a dot.
  fire(origin, dir, power, type, visualOrigin = null) {
    const speed = CONFIG.bow.minSpeed + (CONFIG.bow.maxSpeed - CONFIG.bow.minSpeed) * power;
    this.spawn(origin, dir.clone().multiplyScalar(speed), type, visualOrigin);
  }

  // `volley` marks a mid-air split fragment: the record its shot verdict
  // aggregates through (see resolve()), and the no-resplit flag.
  spawn(origin, vel, type, visualOrigin = null, volley = null) {
    const mesh = buildArrowMesh(type);
    mesh.position.copy(visualOrigin ?? origin); // oriented by the first update()
    const trail = buildTrail(CONFIG.arrow.types[type].color);
    this.game.scene.add(mesh, trail);
    this.list.push({
      mesh, trail, pos: origin.clone(), vel, type, age: 0, volley,
      visualOffset: (visualOrigin ?? origin).clone().sub(origin), // zero without a bow
    });
  }

  clear() {
    for (const a of [...this.list]) this.remove(a);
  }

  remove(a) {
    this.game.scene.remove(a.mesh, a.trail);
    a.trail.geometry.dispose(); // material is shared, never disposed
    this.list.splice(this.list.indexOf(a), 1);
  }

  // Append the arrow's current visual position to the head of its trail.
  pushTrail(a) {
    const geo = a.trail.geometry;
    const attr = geo.attributes.position;
    attr.array.copyWithin(3, 0, (TRAIL_POINTS - 1) * 3);
    attr.array[0] = a.mesh.position.x;
    attr.array[1] = a.mesh.position.y;
    attr.array[2] = a.mesh.position.z;
    geo.setDrawRange(0, Math.min(geo.drawRange.count + 1, TRAIL_POINTS));
    attr.needsUpdate = true;
  }

  update(dt) {
    const R = CONFIG.arrow.radius;
    for (const a of [...this.list]) {
      const prev = a.pos.clone();
      a.vel.y += CONFIG.arrow.gravity * dt;
      a.pos.addScaledVector(a.vel, dt);
      a.age += dt;
      a.mesh.position.copy(a.pos)
        .addScaledVector(a.visualOffset, Math.max(0, 1 - a.age / SPAWN_BLEND));
      a.mesh.quaternion.setFromUnitVectors(
        new THREE.Vector3(0, 1, 0), a.vel.clone().normalize(),
      );
      this.pushTrail(a);
      // Trees and similar obstacles block arrows: clip this frame's travel
      // at the first impact, so a target peeking in front of cover can
      // still be hit but anything behind it is shielded.
      const blocked = obstacleHit(prev, a.pos, this.game.obstacles, R);
      const pos = blocked ?? a.pos;

      // A lob diving back down through its type's split height fans into
      // a volley (see SPEC.md): the crossing guarantees open air below
      // for the fan to spread. Fragments never split again, and an
      // obstacle strike this frame wins — the arrow died before crossing.
      const split = CONFIG.arrow.types[a.type].split;
      if (split && !a.volley && !blocked
          && prev.y > split.height && pos.y <= split.height) {
        this.split(a, prev, split);
        continue;
      }

      // Pickups are collected by shooting them (segment check: arrows are
      // fast). Collection is neutral for auto ammo: the arrow is spent
      // without resolving as a hit or a miss.
      let consumed = false;
      if (this.game.waves) {
        for (const p of [...this.game.waves.pickups]) {
          if (segClosest(prev, pos, p.mesh.position).distanceTo(p.mesh.position)
              < CONFIG.drops.radius + R) {
            this.game.waves.collect(p);
            // Neutral for the shot verdict — but a fragment still settles
            // its volley share (a false share never flips a volley to hit).
            if (a.volley) this.resolve(a, false);
            this.remove(a);
            consumed = true;
            break;
          }
        }
      }
      if (consumed) continue;

      // Enemies: head sphere first (headshots win ties), then body sphere.
      if (this.game.enemies) {
        for (const e of this.game.enemies.list) {
          const head = this.game.enemies.headCenter(e);
          if (segClosest(prev, pos, head).distanceTo(head) < e.c.headRadius + R) {
            this.hit(e, true, a);
            consumed = true;
            break;
          }
          const body = this.game.enemies.bodyCenter(e);
          if (segClosest(prev, pos, body).distanceTo(body)
              < this.game.enemies.bodyRadius(e) + R) {
            this.hit(e, false, a);
            consumed = true;
            break;
          }
        }
      }

      // A spent arrow reports hit (damaged someone) or miss to the game so
      // auto ammo can react; a direct strike always damaged its target.
      if (consumed) { this.resolve(a, true); this.remove(a); continue; }
      if (blocked || pos.y <= 0.05 || a.age > CONFIG.arrow.lifetime) {
        // Specials detonate on whatever stopped them — on cover, the
        // splash/snowburst is the designed counter to enemies hiding
        // behind it, and one that affected anyone still counts as a hit.
        let affected = false;
        if (a.type === 'exploding') affected = this.explode(pos);
        else if (this.rollBurst(a.type)) affected = this.snowburst(pos);
        else if (blocked) this.game.effects?.burst(pos, 0x8a7a66, 8, 3);
        this.resolve(a, affected);
        this.remove(a);
      }
    }
  }

  // Every spent arrow funnels its verdict here. A lone arrow reports
  // immediately; a volley fragment instead settles its share of the split
  // shot, which reports once — a hit if ANY fragment damaged someone —
  // when its last fragment is spent, so a split stays a single shot.
  resolve(a, hit) {
    if (!a.volley) { this.game.onShotResolved?.(hit); return; }
    a.volley.hit ||= hit;
    if (--a.volley.left === 0) this.game.onShotResolved?.(a.volley.hit);
  }

  // Replace `a` with split.count fragments at the exact height crossing:
  // fragment 0 holds the parent's flight line (a lob aimed at a single
  // target still connects), the rest tilt off it around a ring.
  split(a, prev, split) {
    const at = prev.clone().lerp(a.pos, (prev.y - split.height) / (prev.y - a.pos.y));
    const speed = a.vel.length();
    const axis = a.vel.clone().normalize();
    // Tilt axis for the ring; a near-vertical dive degenerates the
    // horizontal cross product, and any horizontal direction serves then.
    const u = new THREE.Vector3().crossVectors(axis, new THREE.Vector3(0, 1, 0));
    if (u.lengthSq() < 1e-6) u.set(1, 0, 0);
    u.normalize();
    const volley = { left: split.count, hit: false };
    for (let i = 0; i < split.count; i++) {
      const dir = i === 0 ? axis.clone()
        : axis.clone().applyAxisAngle(u, split.angle)
          .applyAxisAngle(axis, (2 * Math.PI * (i - 1)) / (split.count - 1));
      this.spawn(at, dir.multiplyScalar(speed), a.type, null, volley);
    }
    this.game.effects?.burst(at, CONFIG.arrow.types[a.type].color, 14, 5);
    this.remove(a);
  }

  hit(e, isHead, arrow) {
    const t = CONFIG.arrow.types[arrow.type];
    this.game.enemies.damage(e, t.damage * (isHead ? CONFIG.arrow.headshotMult : 1), isHead);
    const alive = e.hp > 0;
    if (arrow.type === 'burning' && alive) this.game.enemies.ignite(e);
    if (arrow.type === 'lightning') this.chainLightning(e);
    // One impact effect per hit: splash, snowburst, or the plain strike.
    // A snowburst subsumes the single-target freeze — the target sits at
    // the burst's center, and damage landed above, so no self-shatter.
    if (arrow.type === 'exploding') {
      this.explode(arrow.pos);
    } else if (this.rollBurst(arrow.type)) {
      this.snowburst(arrow.pos);
    } else {
      if (arrow.type === 'freezing' && alive) this.game.enemies.freeze(e);
      this.game.effects?.burst(arrow.pos, 0xaa3333, 10, 4);
    }
  }

  // Random detonation roll for types with a `burst` config entry (today:
  // freezing's snowburst). Seeded rng, never Math.random(): the outcome
  // is gameplay-affecting and must replay identically per seed.
  rollBurst(type) {
    const b = CONFIG.arrow.types[type].burst;
    return !!b && this.game.rng.random() < b.chance;
  }

  // AoE freeze, no damage — the control counterpart of explode(), with the
  // same no-line-of-sight rule: the powder settles on enemies behind cover.
  // Returns whether anyone was frozen (counts as a hit for auto ammo: the
  // shot materially affected an enemy even though it damaged nobody).
  snowburst(pos) {
    this.game.effects?.burst(pos, 0xeaf6ff, 60, 4, { gravity: 1.2, size: 0.3, life: 1.2 });
    const r = CONFIG.arrow.types.freezing.burst.radius;
    let froze = false;
    for (const e of this.game.enemies.list) {
      if (this.game.enemies.bodyCenter(e).distanceTo(pos) < r) {
        this.game.enemies.freeze(e);
        froze = true;
      }
    }
    return froze;
  }

  // Lightning arcs on from the struck enemy through a random number of
  // extra targets: the jolt count is rolled on the seeded rng (so a run
  // replays exactly), then each jolt jumps to the nearest not-yet-struck
  // enemy within `radius` of the previous target for flat chainDamage.
  // Chains happily continue from a corpse — the bolt already reached it.
  chainLightning(first) {
    const t = CONFIG.arrow.types.lightning;
    const struck = new Set([first]);
    const points = [this.game.enemies.bodyCenter(first)];
    const jolts = this.game.rng.int(t.jolts.min, t.jolts.max);
    for (let i = 0; i < jolts; i++) {
      let next = null;
      let nextD = t.radius;
      for (const e of this.game.enemies.list) {
        if (struck.has(e)) continue;
        const d = this.game.enemies.bodyCenter(e).distanceTo(points[points.length - 1]);
        if (d < nextD) { next = e; nextD = d; }
      }
      if (!next) break;
      struck.add(next);
      points.push(this.game.enemies.bodyCenter(next));
      this.game.effects?.burst(points[points.length - 1], t.color, 8, 3);
      this.game.enemies.damage(next, t.chainDamage);
    }
    if (points.length > 1) this.game.effects?.bolt(points, t.color);
  }

  // AoE with linear falloff. Deliberately no line-of-sight check: splash
  // reaches enemies hiding behind cover (the counter to skeleton archers).
  // Returns whether anyone was damaged (a splash hit for auto ammo).
  explode(pos) {
    this.game.effects?.burst(pos, 0xffaa33, 40, 12);
    const t = CONFIG.arrow.types.exploding;
    let damaged = false;
    for (const e of [...this.game.enemies.list]) {
      const d = this.game.enemies.bodyCenter(e).distanceTo(pos);
      if (d < t.radius) {
        this.game.enemies.damage(e, t.aoeDamage * (1 - d / t.radius));
        damaged = true;
      }
    }
    return damaged;
  }
}

// Dotted arc preview shown at partial power; fades out toward max power so
// full-power shots stay skill-based.
export class TrajectoryHint {
  constructor(game) {
    this.game = game;
    this.n = 24;
    const geo = new THREE.BufferGeometry();
    geo.setAttribute('position', new THREE.BufferAttribute(new Float32Array(this.n * 3), 3));
    this.points = new THREE.Points(geo, new THREE.PointsMaterial({
      color: 0xffffff, size: 0.12, transparent: true, opacity: 0.5,
    }));
    this.points.visible = false;
    game.scene.add(this.points);
  }

  // e2e handle: world positions of the visible dots.
  snapshot() {
    if (!this.points.visible) return [];
    const attr = this.points.geometry.attributes.position;
    const n = Math.min(this.n, this.points.geometry.drawRange.count);
    const out = [];
    for (let i = 0; i < n; i++) {
      out.push({ x: attr.getX(i), y: attr.getY(i), z: attr.getZ(i) });
    }
    return out;
  }

  update(player, active) {
    const show = active && player.power < 0.85;
    this.points.visible = show;
    if (!show) return;
    const speed = CONFIG.bow.minSpeed
      + (CONFIG.bow.maxSpeed - CONFIG.bow.minSpeed) * player.power;
    const p = player.aimOrigin();
    const v = player.aimDir().multiplyScalar(speed);
    const prev = new THREE.Vector3();
    const attr = this.points.geometry.attributes.position;
    // Integrate at the arrow's own frame step (semi-implicit Euler, ~1/60 s)
    // and emit a dot every few substeps — one coarse 0.07 s step over-applies
    // gravity and the arc lands visibly short of the real arrow.
    const step = 1 / 60;
    const perDot = 4;
    let n = 0;
    let landed = false;
    for (let i = 0; i < this.n && !landed; i++) {
      prev.copy(p);
      for (let k = 0; k < perDot; k++) {
        v.y += CONFIG.arrow.gravity * step;
        p.addScaledVector(v, step);
        if (p.y <= 0.05) { landed = true; break; } // same plane arrows die on
      }
      // The preview dies where the arrow would. One obstacle check per dot
      // chord, not per substep: the arc sags ~3 cm across a chord, invisible
      // under a 0.12-size dot, and it cuts the scans 4×.
      const hit = obstacleHit(prev, p, this.game.obstacles, CONFIG.arrow.radius);
      if (hit) { p.copy(hit); landed = true; }
      attr.setXYZ(n++, p.x, Math.max(p.y, 0.05), p.z);
    }
    this.points.geometry.setDrawRange(0, n);
    attr.needsUpdate = true;
    this.points.material.opacity = 0.5 * (1 - Math.max(0, (player.power - 0.6) / 0.25));
  }
}
