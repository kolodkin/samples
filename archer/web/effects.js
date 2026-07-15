import * as THREE from 'three';

// Visual-only particles. This module is exempt from the seeded-RNG rule:
// nothing here feeds back into gameplay state, so Math.random() is fine.
export class Effects {
  constructor(scene) {
    this.scene = scene;
    this.bursts = [];
    this.bolts = [];
    this.snow = null;
  }

  // Jagged lightning flash through the chain's strike points: each hop is
  // subdivided and jittered so it reads as an arc, not a laser. Additive
  // and short-lived; the fade runs in update().
  bolt(points, color) {
    const SEGS = 6;
    const verts = [];
    const p = new THREE.Vector3();
    for (let i = 0; i < points.length - 1; i++) {
      for (let k = i === 0 ? 0 : 1; k <= SEGS; k++) {
        p.copy(points[i]).lerp(points[i + 1], k / SEGS);
        if (k > 0 && k < SEGS) {
          p.x += (Math.random() - 0.5) * 0.6;
          p.y += (Math.random() - 0.5) * 0.6;
          p.z += (Math.random() - 0.5) * 0.6;
        }
        verts.push(p.x, p.y, p.z);
      }
    }
    const geo = new THREE.BufferGeometry();
    geo.setAttribute('position', new THREE.BufferAttribute(new Float32Array(verts), 3));
    const line = new THREE.Line(geo, new THREE.LineBasicMaterial({
      color, transparent: true, blending: THREE.AdditiveBlending, depthWrite: false,
    }));
    this.scene.add(line);
    this.bolts.push({ line, age: 0, life: 0.25 });
  }

  // Options tune the burst's character: debris flies fast and drops hard
  // (the defaults); snow powder is slow, big and near-buoyant.
  burst(pos, color, count = 20, speed = 8, { gravity = 9.8, size = 0.22, life = 0.8 } = {}) {
    const positions = new Float32Array(count * 3);
    const vels = [];
    for (let i = 0; i < count; i++) {
      positions.set([pos.x, pos.y, pos.z], i * 3);
      vels.push(new THREE.Vector3(
        Math.random() - 0.5, Math.random() * 0.8, Math.random() - 0.5,
      ).normalize().multiplyScalar(speed * (0.4 + Math.random() * 0.6)));
    }
    const geo = new THREE.BufferGeometry();
    geo.setAttribute('position', new THREE.BufferAttribute(positions, 3));
    const points = new THREE.Points(
      geo, new THREE.PointsMaterial({ color, size, transparent: true }),
    );
    this.scene.add(points);
    this.bursts.push({ points, vels, age: 0, life, gravity });
  }

  setSnow(on) {
    if (on === !!this.snow) return;
    if (!on) { this.scene.remove(this.snow); this.snow = null; return; }
    const count = 500;
    const positions = new Float32Array(count * 3);
    for (let i = 0; i < count; i++) {
      positions.set(
        [(Math.random() - 0.5) * 90, Math.random() * 30, (Math.random() - 0.5) * 90], i * 3,
      );
    }
    const geo = new THREE.BufferGeometry();
    geo.setAttribute('position', new THREE.BufferAttribute(positions, 3));
    this.snow = new THREE.Points(geo, new THREE.PointsMaterial({
      color: 0xffffff, size: 0.15, transparent: true, opacity: 0.8,
    }));
    this.scene.add(this.snow);
  }

  update(dt) {
    for (const b of [...this.bolts]) {
      b.age += dt;
      b.line.material.opacity = 1 - b.age / b.life;
      if (b.age >= b.life) {
        this.scene.remove(b.line);
        b.line.geometry.dispose();
        b.line.material.dispose();
        this.bolts.splice(this.bolts.indexOf(b), 1);
      }
    }
    for (const b of [...this.bursts]) {
      b.age += dt;
      const attr = b.points.geometry.attributes.position;
      const fall = b.gravity * dt;
      for (let i = 0; i < b.vels.length; i++) {
        b.vels[i].y -= fall;
        attr.setXYZ(
          i,
          attr.getX(i) + b.vels[i].x * dt,
          attr.getY(i) + b.vels[i].y * dt,
          attr.getZ(i) + b.vels[i].z * dt,
        );
      }
      attr.needsUpdate = true;
      b.points.material.opacity = 1 - b.age / b.life;
      if (b.age >= b.life) {
        this.scene.remove(b.points);
        b.points.geometry.dispose();
        b.points.material.dispose();
        this.bursts.splice(this.bursts.indexOf(b), 1);
      }
    }
    if (this.snow) {
      const attr = this.snow.geometry.attributes.position;
      for (let i = 0; i < attr.count; i++) {
        let y = attr.getY(i) - 2.5 * dt;
        if (y < 0) y = 30;
        attr.setY(i, y);
      }
      attr.needsUpdate = true;
    }
  }
}
