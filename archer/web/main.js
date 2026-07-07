import * as THREE from 'three';
import { CONFIG } from './config.js';
import { createRng, seedFromQuery } from './rng.js';
import { buildStage, STAGE_ORDER } from './stages.js';
import { Player } from './player.js';
// [task-4] arrows import
// [task-5] enemies import
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
// [task-4] arrow system setup
function fireArrow(power) {} // stub; Task 4 replaces the body
// [task-5] enemy system setup + kill/hit callbacks
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
    // [task-4] arrows.update(dt)
    // [task-5] enemies.update(dt)
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
      // [task-4] arrowCount state
      // [task-5] enemies state
      // [task-8] wave/pickup state
    };
  },
  // [task-4] fireAt
  // [task-5] spawnEnemy, setPlayerHp
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
