import * as THREE from 'three';
import { CONFIG } from './config.js';
import { segClosest, obstacleHit, pushOutOfObstacles, firstBlockingObstacle } from './geom.js';
import { setShadows } from './relief.js';
import { buildEnemyModel } from './models.js';

const perpXZ = (v) => new THREE.Vector3(-v.z, 0, v.x);

const PROJ_GRAVITY = 4; // m/s² drop on skeleton projectiles (shoot() compensates)
const PROJ_RADIUS = 0.09; // skeleton projectile: mesh size and collision pad

// Walker steering (steerAround): lane lookahead (m); clearance margin over
// the push-out resting distance (pushOutOfObstacles parks a hugging walker
// at exactly obstacle.radius + bodyRadius); outward bias that keeps a
// detour from grazing back into the circle it rounds; how long (s) the
// lane must stay clear before the committed detour side is forgotten.
const STEER_LOOKAHEAD = 4;
const STEER_CLEARANCE = 0.1;
const STEER_OUT_BIAS = 0.25;
const STEER_CLEAR_TIME = 0.5;
// Wedge-pin escape (see update()): a walker counts as pinned when its net
// displacement over a STUCK_WINDOW falls under STUCK_RATIO of the distance
// it meant to walk in it; it then flips its detour side and tries the
// other way around. Windowed on purpose — a pinned walker still creeps
// free for single clean frames, which a per-frame test keeps resetting on.
const STEER_STUCK_WINDOW = 1.0;
const STEER_STUCK_RATIO = 0.4; // wall-following nets well above this; shuttles net below
// After a stuck flip the flipped side is preferred on re-commits for a few
// seconds: the escape crosses clear ground, the commitment expires, and a
// fresh nearest-tangent pick would send the walker straight back into the
// wedge it just escaped — a limit cycle through the flip.
const STEER_PREFER_TIME = 4;
// Cul-de-sac escalation: pinned with walls on BOTH sides (three-tree
// pocket), flipping just pins the other way — after RETREAT_AFTER
// consecutive stuck windows the walker backs straight out of the pocket
// mouth for RETREAT_TIME before re-approaching on the preferred side.
const STEER_RETREAT_AFTER = 2;
const STEER_RETREAT_TIME = 0.8;

// Cover acceptance margin (m): a peek line that only grazes a neighbor
// obstacle is no cover — the walker never occupies the exact peek point,
// so a zero-margin line leaves the archer camping effectively buried.
const PEEK_LOS_PAD = 0.3;

export class EnemySystem {
  constructor(game) {
    this.game = game;
    this.list = [];
    this.projectiles = []; // skeleton arrows (Task 6)
  }

  spawn(type, x, z, inert = false) {
    const c = CONFIG.enemies[type];
    const mesh = buildEnemyModel(type, c);
    mesh.position.set(x, 0, z);
    setShadows(mesh);
    this.game.scene.add(mesh);
    const e = {
      type, c, mesh, hp: c.hp, state: 'advance', inert,
      frozen: 0, burn: 0, burnSpreadTimer: 0,
      coverTimer: 0, cover: null, hasShot: false,
      detourSide: 0, laneClear: 0, sidePrefer: 0, sidePreferT: 0,
      stuckStreak: 0, retreatT: 0,
      windowT: 0, wantedAcc: 0, anchor: new THREE.Vector3(x, 0, z),
      peekSide: this.game.rng.random() < 0.5 ? -1 : 1,
      bobT: this.game.rng.range(0, Math.PI * 2),
      walkAmp: 0, aimBlend: 0, moved: 0,
    };
    // Desync animation cycles across a wave without an extra rng draw.
    mesh.userData.anim.mixer.update(e.bobT);
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

  // No pathfinding: the slide push-out alone deadlocks in the concave
  // pocket between adjacent obstacles, so a walker whose lane is blocked
  // follows the blocking obstacle's tangent instead; detourSide sticks so
  // a wall is followed to its end (see SPEC.md). The side only expires
  // after the lane has stayed clear for STEER_CLEAR_TIME: mid-detour the
  // lane samples clear for single frames when the wall's next obstacle
  // sits just past the lookahead, and re-deciding the side on each such
  // frame flip-flops a walker in place between two trees forever (a
  // deadlock reproduced on live forest layouts at 60 Hz steps — see the
  // trap regression tests).
  steerAround(e, dir, dist, dt) {
    const pos = e.mesh.position;
    const o = firstBlockingObstacle(
      pos, dir, Math.min(STEER_LOOKAHEAD, dist),
      e.c.bodyRadius + STEER_CLEARANCE, this.game.obstacles,
    );
    if (!o) {
      e.laneClear += dt;
      if (e.laneClear >= STEER_CLEAR_TIME) e.detourSide = 0;
      return dir;
    }
    e.laneClear = 0;
    const radial = new THREE.Vector3(pos.x - o.x, 0, pos.z - o.z).normalize();
    if (!e.detourSide) { // turn toward the nearer tangent, then commit
      e.detourSide = e.sidePreferT > 0 ? e.sidePrefer
        : (radial.x * dir.z - radial.z * dir.x >= 0 ? 1 : -1);
    }
    return perpXZ(radial).multiplyScalar(e.detourSide)
      .addScaledVector(radial, STEER_OUT_BIAS).normalize();
  }

  moveToward(e, target, dt, speed) {
    const pos = e.mesh.position;
    const dir = new THREE.Vector3(target.x - pos.x, 0, target.z - pos.z);
    const dist = dir.length();
    if (dist < 0.05) return;
    dir.normalize();
    let step;
    if (e.retreatT > 0) { // backing out of a cul-de-sac (see update())
      e.retreatT -= dt;
      step = dir.clone().negate();
    } else {
      step = this.steerAround(e, dir, dist, dt);
    }
    const len = Math.min(dist, speed * dt);
    pos.addScaledVector(step, len);
    e.moved += len; // feeds the walk cycle (animateRig)
    e.mesh.rotation.y = Math.atan2(step.x, step.z);
  }

  update(dt) {
    const playerPos = this.game.camera.position;
    for (const e of [...this.list]) {
      e.bobT += dt * 6;
      e.moved = 0;
      if (e.frozen > 0) {
        e.frozen -= dt;
        if (e.frozen <= 0) this.setTint(e, 0x000000);
        continue; // frozen solid: no movement, no attacks — pose holds mid-stride
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
        // Walkers cannot clip through obstacles; the footprint is the
        // body circle (not the fatter bodyRadius() arrow hit-sphere).
        pushOutOfObstacles(e.mesh.position, e.c.bodyRadius, this.game.obstacles);
        // Wedge-pin escape: a committed tangent can drive into a hugged
        // neighbor the desired lane never crosses — the push-out cancels
        // the step and the walker jitters in place forever (the second
        // live-layout deadlock; see the trap regression tests). Wanting
        // to walk but getting nowhere is the one reliable signature, so
        // a walker pinned through a STEER_STUCK_WINDOW rounds the other
        // way.
        e.sidePreferT = Math.max(0, e.sidePreferT - dt);
        e.wantedAcc += e.moved;
        e.windowT += dt;
        if (e.windowT >= STEER_STUCK_WINDOW) {
          const net = e.anchor.distanceTo(e.mesh.position);
          if (e.detourSide && e.wantedAcc > 0.1 && net < e.wantedAcc * STEER_STUCK_RATIO) {
            e.stuckStreak += 1;
            e.detourSide = -e.detourSide;
            e.laneClear = 0;
            e.sidePrefer = e.detourSide;
            e.sidePreferT = STEER_PREFER_TIME;
            if (e.stuckStreak >= STEER_RETREAT_AFTER) {
              e.retreatT = STEER_RETREAT_TIME;
              e.stuckStreak = 0;
            }
          } else {
            e.stuckStreak = 0;
          }
          e.anchor.copy(e.mesh.position);
          e.windowT = 0;
          e.wantedAcc = 0;
        }
      }
      this.animateRig(e, dt, playerPos);
    }
    this.updateProjectiles(dt, playerPos);
  }

  // Drive the character's AnimationMixer (models.js): walking and idle
  // crossfade on an eased weight, and the walk clip's timeScale is set
  // from the distance actually covered this frame, so footfalls track
  // ground speed and feet never slide. Frozen enemies skip this entirely
  // (update() continues before it), holding their pose mid-stride.
  // Archers square up toward the player while peeking — moveToward faces
  // the strafe direction, which reads wrong with a drawn crossbow.
  animateRig(e, dt, playerPos) {
    const anim = e.mesh.userData.anim;
    const ease = Math.min(1, dt * 8);
    e.walkAmp += ((e.moved > 1e-4 ? 1 : 0) - e.walkAmp) * ease;
    anim.walk.setEffectiveWeight(e.walkAmp);
    anim.idle.setEffectiveWeight(1 - e.walkAmp);
    if (dt > 0 && e.moved > 0) {
      anim.walk.setEffectiveTimeScale((e.moved / dt) / anim.walkRef);
    }
    if (e.type === 'skeleton') {
      e.aimBlend += ((e.state === 'peek' ? 1 : 0) - e.aimBlend) * Math.min(1, dt * 6);
      if (e.aimBlend > 0.02) {
        const pos = e.mesh.position;
        const yaw = Math.atan2(playerPos.x - pos.x, playerPos.z - pos.z);
        let d = yaw - e.mesh.rotation.y;
        d -= Math.round(d / (2 * Math.PI)) * 2 * Math.PI; // shortest arc
        e.mesh.rotation.y += d * e.aimBlend * ease;
      }
    }
    anim.mixer.update(dt);
  }

  spreadBurn(e, dt) {
    e.burnSpreadTimer -= dt;
    if (e.burnSpreadTimer > 0) return;
    e.burnSpreadTimer = 0.5;
    for (const other of this.ignitable(e)) this.ignite(other);
  }

  // Neighbors the fire can still catch: inside the spread radius, not
  // already burning, not frozen (ice quenches fire).
  ignitable(e) {
    const r = CONFIG.arrow.types.burning.spreadRadius;
    return this.list.filter((other) => other !== e && other.burn <= 0
      && other.frozen <= 0
      && other.mesh.position.distanceTo(e.mesh.position) < r);
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
    // The weave is a perpendicular offset on the target point, so
    // moveToward stays the one walking (and steering) primitive.
    const target = new THREE.Vector3(playerPos.x, 0, playerPos.z);
    if (e.type === 'goblin') {
      const dir = new THREE.Vector3(playerPos.x - pos.x, 0, playerPos.z - pos.z).normalize();
      target.addScaledVector(perpXZ(dir), Math.sin(e.bobT * 0.9) * 0.3 * flatDist);
    }
    this.moveToward(e, target, dt, this.speedOf(e));
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
      if (e.coverTimer <= 0) {
        e.state = 'peek';
        e.coverTimer = e.c.peekTime;
        e.hasShot = false;
        // Wind up the Throw clip now so the release lands mid-peek, when
        // shoot() actually looses the bolt (at peekTime * 0.5).
        e.mesh.userData.anim.playAttack?.();
      }
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
  // peek point still sees the player with PEEK_LOS_PAD to spare; 0 when
  // both are buried.
  exposedPeekSide(e, cover, playerPos) {
    for (const side of [e.peekSide, -e.peekSide]) {
      if (this.hasLineOfFire(e, this.peekPoint(cover, side, playerPos), playerPos,
        PEEK_LOS_PAD)) return side;
    }
    return 0;
  }

  // The one "can a shot get through from here" test for cover decisions:
  // sight line from the archer's head over `point` to the player.
  hasLineOfFire(e, point, playerPos, pad = 0) {
    const head = new THREE.Vector3(point.x, e.c.height, point.z);
    return !obstacleHit(head, playerPos, this.game.obstacles, pad);
  }

  coverPoint(cover, playerPos) {
    const away = new THREE.Vector3(cover.x - playerPos.x, 0, cover.z - playerPos.z).normalize();
    return new THREE.Vector3(cover.x, 0, cover.z).addScaledVector(away, cover.radius + 0.7);
  }

  peekPoint(cover, side, playerPos) {
    const c = this.coverPoint(cover, playerPos);
    const away = new THREE.Vector3(cover.x - playerPos.x, 0, cover.z - playerPos.z).normalize();
    return c.addScaledVector(perpXZ(away), side * (cover.radius + 0.5));
  }

  // Where a shot leaves the model: the crossbow riding the hand socket
  // (models.js stashes it as `muzzle`), in the pose the mixer last set —
  // mid-Throw when shoot() fires. Falls back to the head sphere if a
  // future archer model ships without a hand socket.
  muzzlePoint(e) {
    const m = e.mesh.userData.muzzle;
    return m ? m.getWorldPosition(new THREE.Vector3()) : this.headCenter(e);
  }

  shoot(e, playerPos) {
    const from = this.muzzlePoint(e);
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
    this.projectiles.push({
      mesh, vel: dir.multiplyScalar(e.c.projectileSpeed), age: 0,
      spawn: from.clone(), // e2e: bolts are too fast to observe at frame 0
    });
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
