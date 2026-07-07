import * as THREE from 'three';

// Visual-only particles. This module is exempt from the seeded-RNG rule:
// nothing here feeds back into gameplay state, so Math.random() is fine.
export class Effects {
  constructor(scene) {
    this.scene = scene;
    this.bursts = [];
    this.snow = null;
  }

  burst(pos, color, count = 20, speed = 8) {
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
      geo, new THREE.PointsMaterial({ color, size: 0.22, transparent: true }),
    );
    this.scene.add(points);
    this.bursts.push({ points, vels, age: 0, life: 0.8 });
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
    for (const b of [...this.bursts]) {
      b.age += dt;
      const attr = b.points.geometry.attributes.position;
      for (let i = 0; i < b.vels.length; i++) {
        b.vels[i].y -= 9.8 * dt;
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
