import * as THREE from 'three';
import { CONFIG } from './config.js';
import { segClosest } from './geom.js';

function buildArrowMesh(type) {
  const g = new THREE.Group();
  const color = CONFIG.arrow.types[type].color;
  // Slim proportions matching the arrow nocked on the bow viewmodel — the
  // projectile reads as a thin arrow in flight, not a fat bolt. Collision
  // stays at CONFIG.arrow.radius (gameplay tuning, not the visual).
  const shaft = new THREE.Mesh(
    new THREE.CylinderGeometry(0.008, 0.008, 0.7, 5),
    new THREE.MeshLambertMaterial({ color: 0xd8c9a3 }),
  );
  const tip = new THREE.Mesh(
    new THREE.ConeGeometry(0.02, 0.08, 6),
    new THREE.MeshLambertMaterial({ color: 0x555555 }),
  );
  tip.position.y = 0.39;
  const fletch = new THREE.Mesh(
    new THREE.ConeGeometry(0.028, 0.12, 4),
    new THREE.MeshBasicMaterial({ color }),
  );
  fletch.position.y = -0.3;
  g.add(shaft, tip, fletch);
  return g;
}

// Tracer trail: the arrow's recent flight path as a line, brightest at the
// arrow and fading to black toward the tail (additive blending: black
// vertices contribute nothing). Without it a first-person shot reads as a
// shrinking dot — the arrow flies straight away from the eye, so only its
// rear cross-section is ever visible; the trail is what makes the arc read.
const TRAIL_POINTS = 24;

function buildTrail(color) {
  const geo = new THREE.BufferGeometry();
  geo.setAttribute('position', new THREE.BufferAttribute(new Float32Array(TRAIL_POINTS * 3), 3));
  const colors = new Float32Array(TRAIL_POINTS * 3);
  const c = new THREE.Color(color);
  for (let i = 0; i < TRAIL_POINTS; i++) {
    const k = 1 - i / (TRAIL_POINTS - 1);
    colors.set([c.r * k, c.g * k, c.b * k], i * 3);
  }
  geo.setAttribute('color', new THREE.BufferAttribute(colors, 3));
  geo.setDrawRange(0, 0);
  const line = new THREE.Line(geo, new THREE.LineBasicMaterial({
    vertexColors: true, blending: THREE.AdditiveBlending, transparent: true, depthWrite: false,
  }));
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
    const mesh = buildArrowMesh(type);
    mesh.position.copy(visualOrigin ?? origin);
    mesh.quaternion.setFromUnitVectors(new THREE.Vector3(0, 1, 0), dir.clone().normalize());
    const trail = buildTrail(CONFIG.arrow.types[type].color);
    this.game.scene.add(mesh, trail);
    this.list.push({
      mesh, trail, trailCount: 0, pos: origin.clone(),
      vel: dir.clone().multiplyScalar(speed), type, age: 0,
      visualOffset: visualOrigin ? visualOrigin.clone().sub(origin) : null,
    });
  }

  clear() {
    for (const a of [...this.list]) this.remove(a);
  }

  remove(a) {
    this.game.scene.remove(a.mesh, a.trail);
    a.trail.geometry.dispose();
    a.trail.material.dispose();
    this.list.splice(this.list.indexOf(a), 1);
  }

  // Append the arrow's current visual position to the head of its trail.
  pushTrail(a) {
    const attr = a.trail.geometry.attributes.position;
    attr.array.copyWithin(3, 0, (TRAIL_POINTS - 1) * 3);
    attr.array.set([a.mesh.position.x, a.mesh.position.y, a.mesh.position.z], 0);
    a.trailCount = Math.min(a.trailCount + 1, TRAIL_POINTS);
    a.trail.geometry.setDrawRange(0, a.trailCount);
    attr.needsUpdate = true;
  }

  update(dt) {
    const R = CONFIG.arrow.radius;
    for (const a of [...this.list]) {
      const prev = a.pos.clone();
      a.vel.y += CONFIG.arrow.gravity * dt;
      a.pos.addScaledVector(a.vel, dt);
      a.age += dt;
      const blend = a.visualOffset ? Math.max(0, 1 - a.age / SPAWN_BLEND) : 0;
      a.mesh.position.copy(a.pos);
      if (blend > 0) a.mesh.position.addScaledVector(a.visualOffset, blend);
      a.mesh.quaternion.setFromUnitVectors(
        new THREE.Vector3(0, 1, 0), a.vel.clone().normalize(),
      );
      this.pushTrail(a);
      const pos = a.pos;

      // Pickups are collected by shooting them (segment check: arrows are fast).
      let consumed = false;
      if (this.game.waves) {
        for (const p of [...this.game.waves.pickups]) {
          if (segClosest(prev, pos, p.mesh.position).distanceTo(p.mesh.position)
              < CONFIG.drops.radius + R) {
            this.game.waves.collect(p);
            consumed = true;
            break;
          }
        }
      }

      // Enemies: head sphere first (headshots win ties), then body sphere.
      if (!consumed && this.game.enemies) {
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

      if (consumed) { this.remove(a); continue; }
      if (pos.y <= 0.05 || a.age > CONFIG.arrow.lifetime) {
        if (a.type === 'exploding') this.explode(pos);
        this.remove(a);
      }
    }
  }

  hit(e, isHead, arrow) {
    const t = CONFIG.arrow.types[arrow.type];
    this.game.enemies.damage(e, t.damage * (isHead ? CONFIG.arrow.headshotMult : 1), isHead);
    const alive = e.hp > 0;
    if (arrow.type === 'freezing' && alive) this.game.enemies.freeze(e);
    if (arrow.type === 'burning' && alive) this.game.enemies.ignite(e);
    if (arrow.type === 'exploding') this.explode(arrow.pos);
    else this.game.effects?.burst(arrow.pos, 0xaa3333, 10, 4);
  }

  // AoE with linear falloff. Deliberately no line-of-sight check: splash
  // reaches enemies hiding behind cover (the counter to skeleton archers).
  explode(pos) {
    this.game.effects?.burst(pos, 0xffaa33, 40, 12);
    const t = CONFIG.arrow.types.exploding;
    for (const e of [...this.game.enemies.list]) {
      const d = this.game.enemies.bodyCenter(e).distanceTo(pos);
      if (d < t.radius) this.game.enemies.damage(e, t.aoeDamage * (1 - d / t.radius));
    }
  }
}

// Dotted arc preview shown at partial power; fades out toward max power so
// full-power shots stay skill-based.
export class TrajectoryHint {
  constructor(scene) {
    this.n = 24;
    const geo = new THREE.BufferGeometry();
    geo.setAttribute('position', new THREE.BufferAttribute(new Float32Array(this.n * 3), 3));
    this.points = new THREE.Points(geo, new THREE.PointsMaterial({
      color: 0xffffff, size: 0.12, transparent: true, opacity: 0.5,
    }));
    this.points.visible = false;
    scene.add(this.points);
  }

  update(player, active) {
    const show = active && player.power < 0.85;
    this.points.visible = show;
    if (!show) return;
    const speed = CONFIG.bow.minSpeed
      + (CONFIG.bow.maxSpeed - CONFIG.bow.minSpeed) * player.power;
    const p = player.aimOrigin();
    const v = player.aimDir().multiplyScalar(speed);
    const attr = this.points.geometry.attributes.position;
    const step = 0.07;
    for (let i = 0; i < this.n; i++) {
      v.y += CONFIG.arrow.gravity * step;
      p.addScaledVector(v, step);
      attr.setXYZ(i, p.x, Math.max(p.y, 0.05), p.z);
    }
    attr.needsUpdate = true;
    this.points.material.opacity = 0.5 * (1 - Math.max(0, (player.power - 0.6) / 0.25));
  }
}
