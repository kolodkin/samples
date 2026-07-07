import * as THREE from 'three';
import { CONFIG } from './config.js';

function lambert(color) { return new THREE.MeshLambertMaterial({ color }); }

// Builders return a Group whose base sits at y=0; collision spheres are
// derived from config (bodyRadius/height/headRadius), not from the meshes.
function buildGoblin(c) {
  const g = new THREE.Group();
  const body = new THREE.Mesh(
    new THREE.CylinderGeometry(c.bodyRadius * 0.7, c.bodyRadius, c.height * 0.75, 8),
    lambert(c.color),
  );
  body.position.y = c.height * 0.375;
  const head = new THREE.Mesh(new THREE.SphereGeometry(c.headRadius, 8, 6), lambert(0x5ea34c));
  head.position.y = c.height;
  g.add(body, head);
  for (const s of [-1, 1]) {
    const ear = new THREE.Mesh(new THREE.ConeGeometry(0.07, 0.25, 4), lambert(0x5ea34c));
    ear.position.set(s * c.headRadius, c.height + 0.1, 0);
    ear.rotation.z = -s * 1.2;
    g.add(ear);
  }
  return g;
}

function buildOgre(c) {
  const g = new THREE.Group();
  const body = new THREE.Mesh(
    new THREE.CylinderGeometry(c.bodyRadius * 0.8, c.bodyRadius, c.height * 0.8, 8),
    lambert(c.color),
  );
  body.position.y = c.height * 0.4;
  const head = new THREE.Mesh(new THREE.SphereGeometry(c.headRadius, 8, 6), lambert(0x8a765f));
  head.position.y = c.height;
  g.add(body, head);
  for (const s of [-1, 1]) {
    const arm = new THREE.Mesh(new THREE.BoxGeometry(0.3, c.height * 0.6, 0.3), lambert(c.color));
    arm.position.set(s * (c.bodyRadius + 0.18), c.height * 0.5, 0);
    g.add(arm);
  }
  return g;
}

const BUILDERS = { goblin: buildGoblin, ogre: buildOgre }; // skeleton arrives in Task 6

export class EnemySystem {
  constructor(game) {
    this.game = game;
    this.list = [];
    this.projectiles = []; // skeleton arrows (Task 6)
  }

  spawn(type, x, z) {
    const c = CONFIG.enemies[type];
    const mesh = BUILDERS[type](c);
    mesh.position.set(x, 0, z);
    this.game.scene.add(mesh);
    const e = {
      type, c, mesh, hp: c.hp, state: 'advance',
      frozen: 0, burn: 0, burnSpreadTimer: 0,
      attackTimer: 0, coverTimer: 0, cover: null, hasShot: false,
      peekSide: this.game.rng.random() < 0.5 ? -1 : 1,
      bobT: this.game.rng.range(0, Math.PI * 2),
    };
    this.list.push(e);
    return e;
  }

  clear() {
    for (const e of [...this.list]) this.game.scene.remove(e.mesh);
    this.list.length = 0;
    for (const p of [...this.projectiles]) this.game.scene.remove(p.mesh);
    this.projectiles.length = 0;
  }

  bodyCenter(e) {
    return new THREE.Vector3(e.mesh.position.x, e.c.height * 0.5, e.mesh.position.z);
  }

  headCenter(e) {
    return new THREE.Vector3(e.mesh.position.x, e.c.height, e.mesh.position.z);
  }

  bodyRadius(e) { return Math.max(e.c.bodyRadius, e.c.height * 0.45); }

  setTint(e, hex) {
    e.mesh.traverse((o) => {
      if (o.isMesh && o.material.emissive) o.material.emissive.setHex(hex);
    });
  }

  damage(e, dmg, isHead = false) {
    if (e.hp <= 0) return;
    if (e.frozen > 0) { // shatter: frozen targets take bonus damage and thaw
      dmg *= CONFIG.arrow.types.freezing.shatterMult;
      e.frozen = 0;
      this.setTint(e, 0x000000);
    }
    e.hp -= dmg;
    if (e.hp <= 0) this.kill(e, isHead);
  }

  kill(e, isHead) {
    this.game.scene.remove(e.mesh);
    this.list.splice(this.list.indexOf(e), 1);
    this.game.onEnemyKilled(e, isHead);
  }

  freeze(e) {
    e.frozen = CONFIG.arrow.types.freezing.freezeTime;
    e.burn = 0; // ice quenches fire
    this.setTint(e, 0x2288aa);
  }

  ignite(e) {
    e.burn = CONFIG.arrow.types.burning.burnTime;
    this.setTint(e, 0x993300);
  }

  speedOf(e) { return e.c.speed * CONFIG.stages[this.game.stage].speedMult; }

  moveToward(e, target, dt, speed) {
    const pos = e.mesh.position;
    const dir = new THREE.Vector3(target.x - pos.x, 0, target.z - pos.z);
    const dist = dir.length();
    if (dist < 0.05) return;
    dir.normalize();
    pos.addScaledVector(dir, Math.min(dist, speed * dt));
    e.mesh.rotation.y = Math.atan2(dir.x, dir.z);
  }

  update(dt) {
    const playerPos = this.game.camera.position;
    for (const e of [...this.list]) {
      e.bobT += dt * 6;
      if (e.frozen > 0) {
        e.frozen -= dt;
        if (e.frozen <= 0) this.setTint(e, 0x000000);
        continue; // frozen solid: no movement, no attacks
      }
      if (e.burn > 0) {
        e.burn -= dt;
        this.damage(e, CONFIG.arrow.types.burning.dps * dt);
        if (e.hp <= 0) continue;
        if (e.burn <= 0) this.setTint(e, 0x000000);
        else this.spreadBurn(e, dt);
      }
      if (e.type === 'skeleton') this.updateArcher(e, dt, playerPos);
      else this.updateMelee(e, dt, playerPos);
      e.mesh.position.y = Math.abs(Math.sin(e.bobT)) * 0.07; // visual bob only
    }
    this.updateProjectiles(dt, playerPos);
  }

  spreadBurn(e, dt) {
    e.burnSpreadTimer -= dt;
    if (e.burnSpreadTimer > 0) return;
    e.burnSpreadTimer = 0.5;
    const r = CONFIG.arrow.types.burning.spreadRadius;
    for (const other of this.list) {
      if (other !== e && other.burn <= 0 && other.frozen <= 0
          && other.mesh.position.distanceTo(e.mesh.position) < r) {
        this.ignite(other);
      }
    }
  }

  updateMelee(e, dt, playerPos) {
    const pos = e.mesh.position;
    const flatDist = Math.hypot(playerPos.x - pos.x, playerPos.z - pos.z);
    e.attackTimer -= dt;
    if (flatDist <= CONFIG.attackRange + e.c.bodyRadius) {
      if (e.attackTimer <= 0) {
        e.attackTimer = e.c.attackCooldown;
        this.game.player.takeDamage(e.c.damage);
        this.game.onPlayerHit();
      }
      return;
    }
    // Advance with a slight weave (goblins zigzag; ogres lumber straight).
    const dir = new THREE.Vector3(playerPos.x - pos.x, 0, playerPos.z - pos.z).normalize();
    if (e.type === 'goblin') {
      const perp = new THREE.Vector3(-dir.z, 0, dir.x);
      dir.addScaledVector(perp, Math.sin(e.bobT * 0.9) * 0.5).normalize();
    }
    pos.addScaledVector(dir, this.speedOf(e) * dt);
    e.mesh.rotation.y = Math.atan2(dir.x, dir.z);
  }

  updateArcher(e, dt, playerPos) {} // Task 6
  updateProjectiles(dt, playerPos) {} // Task 6
}
