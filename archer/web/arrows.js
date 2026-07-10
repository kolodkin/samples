import * as THREE from 'three';
import { CONFIG } from './config.js';

// Closest point on segment [a,b] to point p.
function segClosest(a, b, p) {
  const ab = new THREE.Vector3().subVectors(b, a);
  const denom = ab.lengthSq();
  const t = denom === 0 ? 0
    : Math.max(0, Math.min(1, new THREE.Vector3().subVectors(p, a).dot(ab) / denom));
  return new THREE.Vector3().copy(a).addScaledVector(ab, t);
}

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

export class ArrowSystem {
  constructor(game) {
    this.game = game;
    this.list = [];
  }

  get count() { return this.list.length; }

  fire(origin, dir, power, type) {
    const speed = CONFIG.bow.minSpeed + (CONFIG.bow.maxSpeed - CONFIG.bow.minSpeed) * power;
    const mesh = buildArrowMesh(type);
    mesh.position.copy(origin);
    this.game.scene.add(mesh);
    this.list.push({ mesh, vel: dir.clone().multiplyScalar(speed), type, age: 0 });
  }

  clear() {
    for (const a of [...this.list]) this.game.scene.remove(a.mesh);
    this.list.length = 0;
  }

  remove(a) {
    this.game.scene.remove(a.mesh);
    this.list.splice(this.list.indexOf(a), 1);
  }

  update(dt) {
    const R = CONFIG.arrow.radius;
    for (const a of [...this.list]) {
      const prev = a.mesh.position.clone();
      a.vel.y += CONFIG.arrow.gravity * dt;
      a.mesh.position.addScaledVector(a.vel, dt);
      a.mesh.quaternion.setFromUnitVectors(
        new THREE.Vector3(0, 1, 0), a.vel.clone().normalize(),
      );
      a.age += dt;
      const pos = a.mesh.position;

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
    if (arrow.type === 'exploding') this.explode(arrow.mesh.position);
    else this.game.effects?.burst(arrow.mesh.position, 0xaa3333, 10, 4);
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
