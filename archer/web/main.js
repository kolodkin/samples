import * as THREE from 'three';
import { CONFIG } from './config.js';
import { createRng, seedFromQuery } from './rng.js';
import { buildStage, STAGE_ORDER } from './stages.js';
import { Player } from './player.js';
import { ArrowSystem, TrajectoryHint } from './arrows.js';
import { EnemySystem } from './enemies.js';
// [task-7] effects import
// [task-8] waves import
// [task-10] ui imports

const params = new URLSearchParams(location.search);
const canvas = document.getElementById('game');

let renderer;
try {
  renderer = new THREE.WebGLRenderer({ canvas, antialias: true });
} catch (err) {
  document.getElementById('fallback').hidden = false;
  throw err;
}
renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));

const scene = new THREE.Scene();
const SKY_FALLBACK = 0x87b5d4;
scene.background = new THREE.Color(SKY_FALLBACK);
const camera = new THREE.PerspectiveCamera(CONFIG.bow.baseFov, 1, 0.1, 300);
const { x: px, y: py, z: pz } = CONFIG.player.pos;
camera.position.set(px, py, pz);
scene.add(camera); // camera-attached meshes (bow viewmodel) must render

// Shared context threaded through every system. Systems never import each
// other; they reach siblings through `game`.
const game = {
  scene, camera, params,
  rng: createRng(seedFromQuery(params)),
  stats: {
    score: 0,
    ammo: { exploding: 0, freezing: 0, burning: 0 },
    selected: 'normal',
  },
  screen: 'title',
  obstacles: [],
  syncUI: () => {}, // replaced by the UI task
};
let stageHandle = null;
function loadStage(index) {
  if (stageHandle) scene.remove(stageHandle.group);
  const name = STAGE_ORDER[index];
  stageHandle = buildStage(name, game.rng);
  scene.add(stageHandle.group);
  scene.background = new THREE.Color(stageHandle.sky);
  const [fogColor, near, far] = stageHandle.fog;
  scene.fog = new THREE.Fog(fogColor, near, far);
  game.stage = name;
  game.stageIndex = index;
  game.obstacles = stageHandle.obstacles;
}
const initialStage = Math.max(0, STAGE_ORDER.indexOf(params.get('stage') || 'forest'));
loadStage(initialStage);
game.player = new Player(camera);
game.arrows = new ArrowSystem(game);
const trajectoryHint = new TrajectoryHint(scene);
function fireArrow(power) {
  const type = game.stats.selected;
  if (type !== 'normal') {
    if (game.stats.ammo[type] <= 0) return; // no ammo: the release fizzles
    game.stats.ammo[type] -= 1;
  }
  game.arrows.fire(game.player.aimOrigin(), game.player.aimDir(), power, type);
  game.syncUI();
}
game.enemies = new EnemySystem(game);
game.onEnemyKilled = (e, isHead) => {
  game.stats.score += e.c.score + (isHead ? CONFIG.headshotBonus : 0);
  game.waves?.onEnemyKilled(e);
  game.syncUI();
};
game.onPlayerHit = () => {
  flashDamage();
  game.syncUI();
  if (game.player.hp <= 0) gameOver();
};
function flashDamage() {
  const el = document.getElementById('flash');
  el.classList.remove('on');
  void el.offsetWidth; // restart the CSS animation
  el.classList.add('on');
}
function gameOver() {
  game.screen = 'gameOver';
  // [task-9] persist best on game over
  document.exitPointerLock?.();
  game.syncUI();
}
// [task-7] effects setup
// [task-8] wave manager setup
// [task-9] progression (start/stage-clear/game-over/retry)
// [task-10] ui wiring

function resize() {
  const w = window.innerWidth, h = window.innerHeight;
  renderer.setSize(w, h, false);
  camera.aspect = w / h;
  camera.updateProjectionMatrix();
}
window.addEventListener('resize', resize);
resize();

let wasLocked = false;
document.addEventListener('pointerlockchange', () => {
  const locked = document.pointerLockElement === canvas;
  if (wasLocked && !locked && game.screen === 'playing') {
    game.player.cancelDraw();
    // [task-10] pause on pointer-lock loss
  }
  wasLocked = locked;
});
canvas.addEventListener('click', () => {
  if (game.screen === 'playing' && document.pointerLockElement !== canvas) {
    canvas.requestPointerLock()?.catch(() => {}); // headless/e2e: lock may be denied
  }
});
document.addEventListener('mousemove', (e) => {
  if (document.pointerLockElement === canvas && game.screen === 'playing') {
    game.player.look(e.movementX, e.movementY);
  }
});
document.addEventListener('mousedown', (e) => {
  if (e.button === 0 && game.screen === 'playing') game.player.startDraw();
});
document.addEventListener('mouseup', (e) => {
  if (e.button !== 0 || game.screen !== 'playing') return;
  const power = game.player.releaseDraw();
  if (power !== null) fireArrow(power);
});

let last = performance.now();
let framesRendered = 0;
function tick(now) {
  const dt = Math.min(0.05, (now - last) / 1000);
  last = now;
  if (game.screen === 'playing') {
    game.player.update(dt);
    game.arrows.update(dt);
    trajectoryHint.update(game.player, true);
    game.enemies.update(dt);
    // [task-8] waves.update(dt)
  }
  // [task-7] effects.update(dt)
  document.documentElement.style.setProperty('--draw', game.player.drawPower.toFixed(3));
  renderer.render(scene, camera);
  framesRendered++;
  if (framesRendered === 1) window.__ARCHER.ready = true;
}
renderer.setAnimationLoop(tick);

// Deterministic e2e handle (pcl-viewer's window.__PCL pattern).
window.__ARCHER = {
  ready: false,
  get state() {
    return {
      screen: game.screen,
      score: game.stats.score,
      ammo: { ...game.stats.ammo },
      selected: game.stats.selected,
      stage: game.stage,
      obstacles: game.obstacles,
      hp: game.player.hp,
      drawPower: game.player.drawPower,
      arrowCount: game.arrows.count,
      enemyCount: game.enemies.list.length,
      enemies: game.enemies.list.map((e) => ({
        type: e.type, x: e.mesh.position.x, z: e.mesh.position.z,
        hp: e.hp, state: e.state, frozen: e.frozen > 0, burning: e.burn > 0,
        hasCover: !!e.cover,
      })),
      // [task-8] wave/pickup state
    };
  },
  // Test helper: fire at a world point with ballistic gravity compensation.
  // Does NOT consume ammo (input-path firing does).
  fireAt(x, y, z, type = 'normal', power = 1) {
    const origin = camera.getWorldPosition(new THREE.Vector3());
    const target = new THREE.Vector3(x, y, z);
    const dist = origin.distanceTo(target);
    const speed = CONFIG.bow.minSpeed + (CONFIG.bow.maxSpeed - CONFIG.bow.minSpeed) * power;
    const dir = target.sub(origin).normalize();
    const tof = dist / speed;
    dir.y += 0.5 * -CONFIG.arrow.gravity * tof * tof / dist;
    dir.normalize();
    game.arrows.fire(origin.addScaledVector(dir, 0.7), dir, power, type);
  },
  spawnEnemy: (type, x, z) => { game.enemies.spawn(type, x, z); },
  setPlayerHp: (n) => { game.player.hp = n; game.syncUI(); },
  // [task-8] setDropChance, killAll, skipToWave
  // [task-9] start, nextStage, retryStage
  // e2e helper: count pixels that differ from the sky background.
  visiblePixelCount() {
    const gl = renderer.getContext();
    const w = renderer.domElement.width, h = renderer.domElement.height;
    const buf = new Uint8Array(w * h * 4);
    gl.readPixels(0, 0, w, h, gl.RGBA, gl.UNSIGNED_BYTE, buf);
    const sky = new THREE.Color(scene.background);
    const br = sky.r * 255, bg = sky.g * 255, bb = sky.b * 255;
    let n = 0;
    for (let i = 0; i < buf.length; i += 4) {
      if (Math.abs(buf[i] - br) + Math.abs(buf[i + 1] - bg) + Math.abs(buf[i + 2] - bb) > 24) n++;
    }
    return n;
  },
};

// Boot: tests (and impatient humans) skip the title screen.
if (params.get('autostart') === '1') {
  game.screen = 'playing'; // [task-9] replaced by startGame()
  game.syncUI();
}
