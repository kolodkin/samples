import * as THREE from 'three';
import { CONFIG } from './config.js';
import { segClosest, obstacleHit } from './geom.js';
import { setShadows } from './relief.js';

function lambert(color) { return new THREE.MeshLambertMaterial({ color }); }

// Builders return a Group whose base sits at y=0; collision spheres are
// derived from config (bodyRadius/height/headRadius), not from the meshes.
// Deliberately smooth flat tints (no relief-mottle treatment): monsters must
// pop against the textured terrain and props, not blend into them.
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

function buildSkeleton(c) {
  const g = new THREE.Group();
  const body = new THREE.Mesh(
    new THREE.CylinderGeometry(0.22, 0.3, c.height * 0.8, 6), lambert(c.color),
  );
  body.position.y = c.height * 0.4;
  const head = new THREE.Mesh(new THREE.SphereGeometry(c.headRadius, 8, 6), lambert(0xe8e4d8));
  head.position.y = c.height;
  const bow = new THREE.Mesh(
    new THREE.TorusGeometry(0.3, 0.03, 5, 16, Math.PI), lambert(0x6b4a2f),
  );
  bow.position.set(0.35, c.height * 0.65, 0.1);
  bow.rotation.y = Math.PI / 2;
  g.add(body, head, bow);
  return g;
}

const BUILDERS = { goblin: buildGoblin, ogre: buildOgre, skeleton: buildSkeleton };

const PROJ_GRAVITY = 4; // m/s² drop on skeleton projectiles (shoot() compensates)
const PROJ_RADIUS = 0.09; // skeleton projectile: mesh size and collision pad

export class EnemySystem {
  constructor(game) {
    this.game = game;
    this.list = [];
    this.projectiles = []; // skeleton arrows (Task 6)
  }

  spawn(type, x, z, inert = false) {
    const c = CONFIG.enemies[type];
    const mesh = BUILDERS[type](c);
    mesh.position.set(x, 0, z);
    setShadows(mesh);
    this.game.scene.add(mesh);
    const e = {
      type, c, mesh, hp: c.hp, state: 'advance', inert,
      frozen: 0, burn: 0, burnSpreadTimer: 0,
      coverTimer: 0, cover: null, hasShot: false,
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
    this.despawn(e);
    this.game.onEnemyKilled(e, isHead);
  }

  // Silent removal: no score, no drops (spent melee attackers, resets).
  despawn(e) {
    this.game.scene.remove(e.mesh);
    this.list.splice(this.list.indexOf(e), 1);
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
      if (!e.inert) { // inert: e2e target dummies with the AI switched off
        if (e.type === 'skeleton') this.updateArcher(e, dt, playerPos);
        else this.updateMelee(e, dt, playerPos);
        if (!this.list.includes(e)) continue; // spent itself on a melee hit
      }
      e.mesh.position.y = Math.abs(Math.sin(e.bobT)) * 0.07; // visual bob only
    }
    this.updateProjectiles(dt, playerPos);
  }

  spreadBurn(e, dt) {
    e.burnSpreadTimer -= dt;
    if (e.burnSpreadTimer > 0) return;
    e.burnSpreadTimer = 0.5;
    for (const other of this.ignitable(e)) this.ignite(other);
  }

  // Neighbors the fire can still catch: inside the spread radius, not
  // already burning, not frozen (ice quenches fire). The one definition of
  // "will burning pay off?" — the actual spread above and smart auto's
  // pick (main.js autoType) must never disagree.
  ignitable(e) {
    const r = CONFIG.arrow.types.burning.spreadRadius;
    return this.list.filter((other) => other !== e && other.burn <= 0
      && other.frozen <= 0
      && other.mesh.position.distanceTo(e.mesh.position) < r);
  }

  // Enemies within r of e, e itself included (smart auto's cluster check).
  packSize(e, r) {
    return this.list.filter(
      (other) => other.mesh.position.distanceTo(e.mesh.position) < r,
    ).length;
  }

  // The enemy under the crosshair: nearest along the aim ray, matched in
  // the XZ plane only — pitch is arc compensation on long shots, so it
  // must never unselect a target. The slack is a generous targeting cone
  // for smart auto, not a hitbox.
  aimedFrom(origin, dir, slack = 1.5) {
    const flat = Math.hypot(dir.x, dir.z);
    if (flat < 1e-6) return null; // aiming straight up or down
    const dx = dir.x / flat, dz = dir.z / flat;
    let best = null;
    let bestT = Infinity;
    for (const e of this.list) {
      const ex = e.mesh.position.x - origin.x, ez = e.mesh.position.z - origin.z;
      const t = ex * dx + ez * dz;
      if (t <= 0 || t >= bestT) continue;
      if (Math.abs(ex * dz - ez * dx) < e.c.bodyRadius + slack) {
        best = e;
        bestT = t;
      }
    }
    return best;
  }

  updateMelee(e, dt, playerPos) {
    const pos = e.mesh.position;
    const flatDist = Math.hypot(playerPos.x - pos.x, playerPos.z - pos.z);
    if (flatDist <= CONFIG.attackRange + e.c.bodyRadius) {
      // One strike and the monster is spent: it lands its hit and vanishes.
      this.game.player.takeDamage(e.c.damage);
      this.despawn(e);
      this.game.onPlayerHit();
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

  updateArcher(e, dt, playerPos) {
    const pos = e.mesh.position;
    const dist = Math.hypot(playerPos.x - pos.x, playerPos.z - pos.z);
    if (e.state === 'advance') {
      e.cover = dist > e.c.range ? null : this.pickCover(e, playerPos);
      // Engage once in range with workable cover or a line of fire; keep
      // closing in otherwise — camping blind would stall the wave (see SPEC.md).
      if (!e.cover && (dist > e.c.range || !this.hasLineOfFire(e, e.mesh.position, playerPos))) {
        this.moveToward(e, playerPos, dt, this.speedOf(e));
        return;
      }
      e.state = 'cover';
      e.coverTimer = e.c.hideTime * 0.5; // first hide is short: pressure early
    }
    const speed = this.speedOf(e);
    if (e.state === 'cover') {
      if (e.cover) this.moveToward(e, this.coverPoint(e.cover, playerPos), dt, speed);
      e.coverTimer -= dt;
      if (e.coverTimer <= 0) { e.state = 'peek'; e.coverTimer = e.c.peekTime; e.hasShot = false; }
    } else if (e.state === 'peek') {
      if (e.cover) this.moveToward(e, this.peekPoint(e.cover, e.peekSide, playerPos), dt, speed);
      if (!e.hasShot && e.coverTimer <= e.c.peekTime * 0.5) {
        this.shoot(e, playerPos);
        e.hasShot = true;
      }
      e.coverTimer -= dt;
      if (e.coverTimer <= 0) { e.state = 'cover'; e.coverTimer = e.c.hideTime; }
    }
  }

  // Nearest obstacle roughly on the line between this archer and the player
  // that leaves at least one peek side with a line of fire (neighbors can
  // bury both — see SPEC.md); accepting a cover commits e.peekSide to its
  // exposed side.
  pickCover(e, playerPos) {
    const pos = e.mesh.position;
    let best = null;
    let bestD = 18;
    for (const o of this.game.obstacles) {
      const d = Math.hypot(o.x - pos.x, o.z - pos.z);
      if (d >= bestD) continue;
      const toObstacle = new THREE.Vector2(o.x - playerPos.x, o.z - playerPos.z);
      const toArcher = new THREE.Vector2(pos.x - playerPos.x, pos.z - playerPos.z);
      if (toObstacle.length() >= toArcher.length()) continue; // must shield the archer
      if (toObstacle.normalize().dot(toArcher.normalize()) < 0.7) continue;
      const side = this.exposedPeekSide(e, o, playerPos);
      if (side === 0) continue; // every peek is buried behind neighbors
      best = o;
      bestD = d;
      e.peekSide = side;
    }
    return best;
  }

  // First peek side of `cover` (preferring the archer's preset one) whose
  // peek point still sees the player; 0 when both are buried.
  exposedPeekSide(e, cover, playerPos) {
    for (const side of [e.peekSide, -e.peekSide]) {
      if (this.hasLineOfFire(e, this.peekPoint(cover, side, playerPos), playerPos)) return side;
    }
    return 0;
  }

  // The one "can a shot get through from here" test for cover decisions:
  // sight line from the archer's head over `point` to the player.
  hasLineOfFire(e, point, playerPos) {
    const head = new THREE.Vector3(point.x, e.c.height, point.z);
    return !obstacleHit(head, playerPos, this.game.obstacles);
  }

  coverPoint(cover, playerPos) {
    const away = new THREE.Vector3(cover.x - playerPos.x, 0, cover.z - playerPos.z).normalize();
    return new THREE.Vector3(cover.x, 0, cover.z).addScaledVector(away, cover.radius + 0.7);
  }

  peekPoint(cover, side, playerPos) {
    const c = this.coverPoint(cover, playerPos);
    const away = new THREE.Vector3(cover.x - playerPos.x, 0, cover.z - playerPos.z).normalize();
    const perp = new THREE.Vector3(-away.z, 0, away.x);
    return c.addScaledVector(perp, side * (cover.radius + 0.5));
  }

  shoot(e, playerPos) {
    const from = this.headCenter(e);
    const dir = new THREE.Vector3().subVectors(playerPos, from);
    const dist = dir.length();
    dir.normalize();
    // Compensate the projectile's drop (see PROJ_GRAVITY in updateProjectiles).
    const tof = dist / e.c.projectileSpeed;
    dir.y += 0.5 * PROJ_GRAVITY * tof * tof / dist;
    const rng = this.game.rng;
    dir.x += rng.range(-e.c.spread, e.c.spread);
    dir.y += rng.range(-e.c.spread, e.c.spread);
    dir.z += rng.range(-e.c.spread, e.c.spread);
    dir.normalize();
    const mesh = new THREE.Mesh(
      new THREE.SphereGeometry(PROJ_RADIUS, 6, 5),
      new THREE.MeshBasicMaterial({ color: 0x332222 }),
    );
    mesh.castShadow = true; // the racing ground shadow telegraphs the arc
    mesh.position.copy(from);
    this.game.scene.add(mesh);
    this.projectiles.push({ mesh, vel: dir.multiplyScalar(e.c.projectileSpeed), age: 0 });
  }

  updateProjectiles(dt, playerPos) {
    for (const p of [...this.projectiles]) {
      p.vel.y -= PROJ_GRAVITY * dt; // gentle drop so long shots arc
      const prev = p.mesh.position.clone();
      p.mesh.position.addScaledVector(p.vel, dt);
      p.age += dt;
      let dead = false;
      // Cover works both ways: obstacles eat skeleton shots too. Clip the
      // frame's travel at the impact so a shot can't reach the player
      // through a tree it crossed mid-frame.
      const blocked = obstacleHit(prev, p.mesh.position, this.game.obstacles, PROJ_RADIUS);
      // Segment-vs-sphere like player arrows: on slow machines a frame's
      // travel exceeds the 0.9 m hit sphere, and a point test tunnels.
      if (segClosest(prev, blocked ?? p.mesh.position, playerPos)
          .distanceTo(playerPos) < 0.9) {
        this.game.player.takeDamage(CONFIG.enemies.skeleton.damage);
        this.game.onPlayerHit();
        dead = true;
      } else if (blocked || p.mesh.position.y < 0 || p.age > 5) {
        dead = true;
      }
      if (dead) {
        this.game.scene.remove(p.mesh);
        this.projectiles.splice(this.projectiles.indexOf(p), 1);
      }
    }
  }
}
