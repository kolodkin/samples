import * as THREE from 'three';
import { CONFIG } from './config.js';
import { createRng, seedFromQuery } from './rng.js';
import { buildStage, STAGE_ORDER } from './stages.js';
import { Player } from './player.js';
import { ArrowSystem, TrajectoryHint } from './arrows.js';
import { EnemySystem } from './enemies.js';
import { Effects } from './effects.js';
import { WaveManager } from './waves.js';
import { createStore, initUI } from './ui.js';

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
  touchMode: matchMedia('(pointer: coarse)').matches,
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
  // Optional-chained: the boot-time call runs before game.effects exists
  // (the setup block re-applies snow for that first load).
  game.effects?.setSnow(name === 'iceberg');
}
const initialStage = Math.max(0, STAGE_ORDER.indexOf(params.get('stage') || 'forest'));
loadStage(initialStage);
game.player = new Player(camera);
game.arrows = new ArrowSystem(game);
const trajectoryHint = new TrajectoryHint(scene);
function fireArrow() {
  if (game.screen !== 'playing') return; // one gate for every fire path
  if (!game.player.canShoot()) return; // still re-nocking from the last shot
  const type = game.stats.selected;
  if (type !== 'normal') {
    if (game.stats.ammo[type] <= 0) return; // no ammo: the shot fizzles
    game.stats.ammo[type] -= 1;
    if (game.stats.ammo[type] === 0) game.stats.selected = 'normal'; // quiver empty: fall back to the basic arrow
  }
  game.arrows.fire(game.player.aimOrigin(), game.player.aimDir(), game.player.power, type);
  game.player.shoot(); // string snap + reload animation, and the shot gate
  game.syncUI();
}
function adjustPower(dir) {
  game.player.adjustPower(dir);
  game.syncUI();
}
game.enemies = new EnemySystem(game);
game.onEnemyKilled = (e, isHead) => {
  const now = performance.now();
  const chained = now - (game.lastKillAt ?? -Infinity) < CONFIG.multiKillWindow * 1000;
  game.combo = chained ? game.combo + 1 : 0;
  game.lastKillAt = now;
  game.stats.score += e.c.score
    + (isHead ? CONFIG.headshotBonus : 0)
    + game.combo * CONFIG.multiKillBonus;
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
  saveBest();
  document.exitPointerLock?.();
  game.syncUI();
}
game.effects = new Effects(scene);
game.effects.setSnow(game.stage === 'iceberg');
const ARROW_ORDER = ['normal', 'exploding', 'freezing', 'burning'];
const TYPE_KEYS = { Digit1: 'normal', Digit2: 'exploding', Digit3: 'freezing', Digit4: 'burning' };
// +/- keys mirror the HUD power buttons: under pointer lock the cursor is
// captive, so desktop players adjust power from the keyboard.
const POWER_KEYS = { Equal: 1, NumpadAdd: 1, Minus: -1, NumpadSubtract: -1 };
document.addEventListener('keydown', (e) => {
  if (game.screen !== 'playing') return;
  const type = TYPE_KEYS[e.code];
  if (type) { game.stats.selected = type; game.syncUI(); }
  const dir = POWER_KEYS[e.code];
  if (dir) adjustPower(dir);
});
document.addEventListener('wheel', (e) => {
  if (game.screen !== 'playing') return;
  const i = ARROW_ORDER.indexOf(game.stats.selected);
  game.stats.selected = ARROW_ORDER[(i + (e.deltaY > 0 ? 1 : -1) + 4) % 4];
  game.syncUI();
});
game.waves = new WaveManager(game);
const BEST_KEY = 'archer.best';
function loadBest() {
  try { return JSON.parse(localStorage.getItem(BEST_KEY)) || { score: 0, stage: 0 }; }
  catch { return { score: 0, stage: 0 }; }
}
function saveBest() {
  const best = loadBest();
  best.score = Math.max(best.score, game.stats.score);
  best.stage = Math.max(best.stage, game.stageIndex + 1);
  localStorage.setItem(BEST_KEY, JSON.stringify(best));
}

function startGame(stageIndex) {
  game.enemies.clear();
  game.arrows.clear();
  game.waves.clearPickups();
  game.waves.state = 'idle';
  loadStage(stageIndex);
  game.player.resetHp();
  game.stageInventory = { ...game.stats.ammo }; // retry restores this snapshot
  game.screen = 'playing';
  if (params.get('waves') !== '0') game.waves.startWave(1);
  game.syncUI();
}
function nextStage() { startGame(game.stageIndex + 1); }
function retryStage() {
  game.stats.ammo = { ...game.stageInventory };
  startGame(game.stageIndex);
}
game.onStageCleared = () => {
  saveBest();
  game.screen = game.stageIndex >= STAGE_ORDER.length - 1 ? 'victory' : 'stageClear';
  document.exitPointerLock?.();
  game.syncUI();
};
const store = createStore({});
game.syncUI = () => store.set({
  screen: game.screen,
  hp: game.player.hp,
  maxHp: CONFIG.player.hp,
  score: game.stats.score,
  ammo: { ...game.stats.ammo },
  selected: game.stats.selected,
  power: game.player.power,
  wave: game.waves.waveIndex,
  totalWaves: CONFIG.waves.perStage,
  stage: game.stage,
  best: loadBest(),
  touch: game.touchMode,
});
initUI(store, {
  start: () => startGame(initialStage),
  resume: () => { game.screen = 'playing'; game.syncUI(); },
  next: () => nextStage(),
  retry: () => retryStage(),
  restart: () => { game.stats.score = 0; startGame(0); }, // fresh run after victory
  pause: () => {
    if (game.screen !== 'playing') return;
    game.screen = 'paused';
    game.syncUI();
  },
  select: (type) => { game.stats.selected = type; game.syncUI(); },
  fire: fireArrow,
  power: (dir) => { if (game.screen === 'playing') adjustPower(dir); },
});
game.syncUI();

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
    game.screen = 'paused';
    game.syncUI();
  }
  wasLocked = locked;
});
canvas.addEventListener('click', () => {
  if (game.touchMode) return; // touch aims by dragging; no pointer lock
  if (game.screen === 'playing' && document.pointerLockElement !== canvas) {
    canvas.requestPointerLock()?.catch(() => {}); // headless/e2e: lock may be denied
  }
});
document.addEventListener('mousemove', (e) => {
  if (document.pointerLockElement === canvas && game.screen === 'playing') {
    game.player.look(e.movementX, e.movementY);
  }
});
// Shot on click. Only clicks that land on the canvas fire — HUD buttons
// (quiver, power, pause) sit in #ui and keep their clicks to themselves.
// Under pointer lock every mouse event targets the canvas, so this always
// fires while locked.
document.addEventListener('mousedown', (e) => {
  if (e.button === 0 && e.target === canvas) fireArrow();
});

// Touch: drag on the canvas to aim; a tap (short press, negligible travel)
// shoots, as does the HUD fire button (see ui.js). No pointer lock on
// touch — deltas come from tracking one finger by identifier, so a second
// finger on the fire button never steers.
document.addEventListener('touchstart', () => {
  if (!game.touchMode) { game.touchMode = true; game.syncUI(); }
}, { capture: true, passive: true });
let lookTouch = null; // { id, x, y, drift, t0 } of the finger that owns the camera
canvas.addEventListener('touchstart', (e) => {
  if (game.screen !== 'playing' || lookTouch) return;
  const t = e.changedTouches[0];
  lookTouch = { id: t.identifier, x: t.clientX, y: t.clientY, drift: 0, t0: performance.now() };
  e.preventDefault(); // no scroll/zoom, and no synthetic mouse click-fire
}, { passive: false });
canvas.addEventListener('touchmove', (e) => {
  if (!lookTouch || game.screen !== 'playing') return;
  for (const t of e.changedTouches) {
    if (t.identifier !== lookTouch.id) continue;
    const k = CONFIG.touch.lookScale;
    const dx = t.clientX - lookTouch.x, dy = t.clientY - lookTouch.y;
    game.player.look(dx * k, dy * k);
    lookTouch.drift += Math.hypot(dx, dy);
    lookTouch.x = t.clientX;
    lookTouch.y = t.clientY;
  }
  e.preventDefault();
}, { passive: false });
canvas.addEventListener('touchend', (e) => {
  for (const t of e.changedTouches) {
    if (!lookTouch || t.identifier !== lookTouch.id) continue;
    // Tap-to-shoot: anywhere on the canvas counts — HUD controls sit above
    // it and keep their taps to themselves. Real drags blow the drift
    // budget and only aim; held presses time out.
    if (lookTouch.drift < CONFIG.touch.tapMaxDrift
        && performance.now() - lookTouch.t0 < CONFIG.touch.tapMaxMs) fireArrow();
    lookTouch = null;
  }
});
canvas.addEventListener('touchcancel', (e) => {
  for (const t of e.changedTouches) {
    if (lookTouch && t.identifier === lookTouch.id) lookTouch = null; // never a tap
  }
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
    game.waves.update(dt);
  }
  game.effects.update(dt);
  document.documentElement.style.setProperty('--power', game.player.power.toFixed(3));
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
      best: loadBest(),
      ammo: { ...game.stats.ammo },
      selected: game.stats.selected,
      stage: game.stage,
      obstacles: game.obstacles,
      hp: game.player.hp,
      yaw: game.player.yaw,
      pitch: game.player.pitch,
      touch: game.touchMode,
      power: game.player.power,
      nocked: game.player.bow.nocked.visible,
      canShoot: game.player.canShoot(),
      bowX: game.player.bow.group.position.x,
      arrowCount: game.arrows.count,
      enemyCount: game.enemies.list.length,
      enemies: game.enemies.list.map((e) => ({
        type: e.type, x: e.mesh.position.x, z: e.mesh.position.z,
        hp: e.hp, state: e.state, frozen: e.frozen > 0, burning: e.burn > 0,
        hasCover: !!e.cover,
      })),
      wave: game.waves.waveIndex,
      waveState: game.waves.state,
      pickupCount: game.waves.pickups.length,
      pickups: game.waves.pickups.map((p) => ({
        type: p.type, x: p.mesh.position.x, y: p.mesh.position.y, z: p.mesh.position.z,
      })),
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
  spawnEnemy: (type, x, z, inert = false) => { game.enemies.spawn(type, x, z, inert); },
  setPlayerHp: (n) => { game.player.hp = n; game.syncUI(); },
  giveAmmo: (type, n) => { game.stats.ammo[type] += n; game.syncUI(); },
  setDropChance: (c) => { CONFIG.drops.chance = c; },
  setHealChance: (c) => { CONFIG.drops.heal.chance = c; },
  killAll: () => {
    for (const e of [...game.enemies.list]) game.enemies.damage(e, 1e9);
  },
  skipToWave: (n) => { game.waves.skipToWave(n); },
  start: (i = 0) => startGame(i),
  nextStage: () => nextStage(),
  retryStage: () => retryStage(),
  // e2e helper: RGB of the canvas pixel at fractional coords (origin top-left).
  // Renders first: the drawing buffer is cleared between frames, so reading
  // outside the animation loop would see black.
  pixelAt(fx, fy) {
    renderer.render(scene, camera);
    const gl = renderer.getContext();
    const w = renderer.domElement.width, h = renderer.domElement.height;
    const buf = new Uint8Array(4);
    const x = Math.round(fx * (w - 1));
    const y = Math.round((1 - fy) * (h - 1)); // readPixels origin is bottom-left
    gl.readPixels(x, y, 1, 1, gl.RGBA, gl.UNSIGNED_BYTE, buf);
    return [buf[0], buf[1], buf[2]];
  },
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
if (params.get('autostart') === '1') startGame(initialStage);
