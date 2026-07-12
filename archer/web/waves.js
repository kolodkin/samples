import * as THREE from 'three';
import { CONFIG } from './config.js';

const DROP_TYPES = ['exploding', 'freezing', 'burning'];
const DROP_COLORS = { exploding: 0xff7733, freezing: 0x66ddff, burning: 0xff4422, heal: 0xff3366 };

const glowMaterial = (color) => new THREE.MeshLambertMaterial({
  color, emissive: color, emissiveIntensity: 0.5,
});

// Heal potion: a stubby corked flask, so it reads as a bottle at a glance
// rather than another ammo octahedron.
function buildPotionMesh() {
  const g = new THREE.Group();
  const glass = glowMaterial(DROP_COLORS.heal);
  const r = CONFIG.drops.radius;
  const body = new THREE.Mesh(new THREE.SphereGeometry(r * 0.5, 10, 8), glass);
  const neck = new THREE.Mesh(new THREE.CylinderGeometry(r * 0.15, r * 0.15, r * 0.35, 8), glass);
  neck.position.y = r * 0.55;
  const cork = new THREE.Mesh(
    new THREE.CylinderGeometry(r * 0.18, r * 0.18, r * 0.12, 8),
    new THREE.MeshLambertMaterial({ color: 0x9a7b4f }),
  );
  cork.position.y = r * 0.75;
  g.add(body, neck, cork);
  return g;
}

export class WaveManager {
  constructor(game) {
    this.game = game;
    this.pickups = [];
    this.waveIndex = 0; // 1-based once a wave starts
    this.pending = [];  // enemy types queued to spawn this wave
    this.spawnTimer = 0;
    this.betweenTimer = 0;
    this.state = 'idle';
  }

  startWave(n) {
    this.waveIndex = n;
    const mix = CONFIG.stages[this.game.stage].waves[n - 1];
    this.pending = [];
    for (const [type, count] of Object.entries(mix)) {
      for (let i = 0; i < count; i++) this.pending.push(type);
    }
    // Fisher-Yates on the seeded rng so spawn order interleaves types.
    for (let i = this.pending.length - 1; i > 0; i--) {
      const j = Math.floor(this.game.rng.random() * (i + 1));
      [this.pending[i], this.pending[j]] = [this.pending[j], this.pending[i]];
    }
    this.spawnTimer = 0;
    this.state = 'spawning';
    this.game.syncUI();
  }

  skipToWave(n) {
    this.game.enemies.clear();
    this.pending = [];
    this.startWave(n);
  }

  update(dt) {
    if (this.state === 'spawning') {
      this.spawnTimer -= dt;
      if (this.spawnTimer <= 0 && this.pending.length) {
        const type = this.pending.pop();
        const x = this.game.rng.range(-CONFIG.arena.spawnXSpread, CONFIG.arena.spawnXSpread);
        const z = CONFIG.arena.spawnZ + this.game.rng.range(-2, 2);
        this.game.enemies.spawn(type, x, z);
        this.spawnTimer = CONFIG.waves.spawnInterval;
      }
      if (!this.pending.length) this.state = 'fighting';
    } else if (this.state === 'fighting') {
      if (this.game.enemies.list.length === 0) {
        this.state = 'cleared';
        this.betweenTimer = CONFIG.waves.clearDelay;
      }
    } else if (this.state === 'cleared') {
      this.betweenTimer -= dt;
      if (this.betweenTimer <= 0) {
        if (this.waveIndex >= CONFIG.waves.perStage) this.game.onStageCleared();
        else this.startWave(this.waveIndex + 1);
      }
    }
    this.updatePickups(dt);
  }

  onEnemyKilled(e) {
    if (this.game.rng.random() < CONFIG.drops.chance) this.dropPickup(e);
  }

  dropPickup(e) {
    const type = this.game.rng.random() < CONFIG.drops.heal.chance
      ? 'heal' : this.game.rng.pick(DROP_TYPES);
    const mesh = type === 'heal' ? buildPotionMesh() : new THREE.Mesh(
      new THREE.OctahedronGeometry(CONFIG.drops.radius * 0.7, 0),
      glowMaterial(DROP_COLORS[type]),
    );
    mesh.position.set(e.mesh.position.x, 1.1, e.mesh.position.z);
    mesh.traverse((o) => { if (o.isMesh) o.castShadow = true; }); // floats: shadow anchors it
    this.game.scene.add(mesh);
    this.pickups.push({ mesh, type, age: 0 });
  }

  collect(p) {
    if (p.type === 'heal') {
      this.game.player.heal(CONFIG.drops.heal.amount);
    } else {
      const n = this.game.rng.int(CONFIG.drops.min, CONFIG.drops.max);
      this.game.stats.ammo[p.type] += n;
    }
    this.game.effects?.burst(p.mesh.position, DROP_COLORS[p.type], 16, 5);
    this.removePickup(p);
    this.game.syncUI();
  }

  removePickup(p) {
    this.game.scene.remove(p.mesh);
    this.pickups.splice(this.pickups.indexOf(p), 1);
  }

  clearPickups() {
    for (const p of [...this.pickups]) this.removePickup(p);
  }

  updatePickups(dt) {
    for (const p of [...this.pickups]) {
      p.age += dt;
      p.mesh.rotation.y += dt * 2;
      p.mesh.position.y = 1.1 + Math.sin(p.age * 3) * 0.15;
      if (p.age > CONFIG.drops.lifetime) this.removePickup(p);
    }
  }
}
