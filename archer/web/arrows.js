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
  const shaft = new THREE.Mesh(
    new THREE.CylinderGeometry(0.02, 0.02, 0.7, 5),
    new THREE.MeshLambertMaterial({ color: 0xd8c9a3 }),
  );
  const tip = new THREE.Mesh(
    new THREE.ConeGeometry(0.045, 0.12, 5),
    new THREE.MeshLambertMaterial({ color: 0x555555 }),
  );
  tip.position.y = 0.41;
  const fletch = new THREE.Mesh(
    new THREE.ConeGeometry(0.06, 0.15, 4),
    new THREE.MeshBasicMaterial({ color }),
  );
  fletch.position.y = -0.32;
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
        // [task-7] explode on ground impact for exploding arrows
        this.remove(a);
      }
    }
  }

  hit(e, isHead, arrow) {
    const t = CONFIG.arrow.types[arrow.type];
    this.game.enemies.damage(e, t.damage * (isHead ? CONFIG.arrow.headshotMult : 1), isHead);
    // [task-7] arrow-type status effects + explosion
  }
}

// Dotted arc preview shown at partial draw; fades out at full draw so
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
    const show = active && player.isDrawing
      && player.drawPower > 0.05 && player.drawPower < 0.85;
    this.points.visible = show;
    if (!show) return;
    const speed = CONFIG.bow.minSpeed
      + (CONFIG.bow.maxSpeed - CONFIG.bow.minSpeed) * player.drawPower;
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
    this.points.material.opacity = 0.5 * (1 - Math.max(0, (player.drawPower - 0.6) / 0.25));
  }
}
