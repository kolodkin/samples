import * as THREE from 'three';
import { CONFIG } from './config.js';

function buildBowViewmodel() {
  const g = new THREE.Group();
  const wood = new THREE.MeshLambertMaterial({ color: 0x7a5230 });
  // Limb: a half-circle arc with tips at (0, ±0.16), bulging left toward
  // the screen center; the string closes the chord between the tips.
  const limb = new THREE.Mesh(new THREE.TorusGeometry(0.16, 0.014, 6, 24, Math.PI), wood);
  limb.rotation.z = Math.PI / 2;
  g.add(limb);
  const string = new THREE.Mesh(
    new THREE.BoxGeometry(0.003, 0.32, 0.003),
    new THREE.MeshBasicMaterial({ color: 0xeeeeee }),
  );
  g.add(string);
  // Nocked arrow: shaft pointing forward (-z), sliding back with the draw.
  const shaft = new THREE.Mesh(
    new THREE.CylinderGeometry(0.006, 0.006, 0.5, 5),
    new THREE.MeshLambertMaterial({ color: 0xd8c9a3 }),
  );
  shaft.rotation.x = Math.PI / 2;
  shaft.position.z = 0.05;
  g.add(shaft);
  g.position.set(0.26, -0.22, -0.6);
  g.rotation.y = -0.3;
  return { group: g, string, shaft };
}

export class Player {
  constructor(camera) {
    this.camera = camera;
    this.hp = CONFIG.player.hp;
    this.yaw = 0;       // facing -z, toward the spawn edge
    this.pitch = 0;
    this.isDrawing = false;
    this.drawPower = 0;
    this.bow = buildBowViewmodel();
    camera.add(this.bow.group);
    camera.rotation.order = 'YXZ';
  }

  look(dx, dy) {
    this.yaw -= dx * 0.0022;
    this.pitch = Math.max(-1.4, Math.min(1.4, this.pitch - dy * 0.0022));
  }

  startDraw() {
    if (!this.isDrawing) { this.isDrawing = true; this.drawPower = 0; }
  }

  cancelDraw() { this.isDrawing = false; this.drawPower = 0; }

  releaseDraw() {
    const p = this.drawPower;
    this.cancelDraw();
    return p >= CONFIG.bow.minDrawToFire ? p : null;
  }

  takeDamage(n) { this.hp = Math.max(0, this.hp - n); }
  resetHp() { this.hp = CONFIG.player.hp; }

  aimDir() {
    return this.camera.getWorldDirection(new THREE.Vector3());
  }

  aimOrigin() {
    return this.camera.getWorldPosition(new THREE.Vector3())
      .addScaledVector(this.aimDir(), 0.7);
  }

  update(dt) {
    this.camera.rotation.set(this.pitch, this.yaw, 0);
    if (this.isDrawing) {
      this.drawPower = Math.min(1, this.drawPower + dt / CONFIG.bow.drawTime);
    }
    // Ease FOV toward zoom at full draw.
    const targetFov = this.drawPower >= 1 ? CONFIG.bow.zoomFov : CONFIG.bow.baseFov;
    this.camera.fov += (targetFov - this.camera.fov) * Math.min(1, dt * 8);
    this.camera.updateProjectionMatrix();
    // Pull the string and nocked arrow back with draw power.
    const pull = this.drawPower * 0.15;
    this.bow.string.position.x = pull;
    this.bow.string.scale.y = 1 - this.drawPower * 0.2;
    this.bow.shaft.position.z = 0.05 + pull;
  }
}
