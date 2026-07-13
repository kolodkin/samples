import * as THREE from 'three';
import { CONFIG } from './config.js';

// Monster radar in the HUD's top-left corner: a player-centered minimap,
// rotated so "up" is the aim direction. Enemies inside CONFIG.radar.range
// appear at scale; farther contacts pin to the rim at half opacity, so an
// incoming wave reads as a ring of bearings before it closes. Redrawn
// imperatively every frame (like the --power CSS variable) — enemy motion
// is far too chatty for the UI store. The canvas lives in the Hud component
// (ui.js) and its dish chrome (dark disc, gold rim) and display size are
// .radar CSS; the canvas draws only the moving marks, in a fixed logical
// space scaled to whatever size the stylesheet set.

const SIZE = 132; // logical drawing space (a viewBox; CSS owns display size)
const RIM = 6;    // blips clamp this far inside the edge

// Per-type blip style, derived once from config: mesh color as CSS, radius
// from body size so bigger monsters draw bigger dots.
const BLIP = Object.fromEntries(Object.entries(CONFIG.enemies).map(([type, c]) => [
  type, { color: `#${new THREE.Color(c.color).getHexString()}`, radius: 2 + c.bodyRadius * 2.5 },
]));

// Blip positions in radar px relative to the center (+x right, +y down,
// up = player facing); `clamped` marks out-of-range contacts pinned to
// the rim. Exposed on __ARCHER.state for the e2e tests.
export function radarBlips(game) {
  const { x: px, z: pz } = game.camera.position;
  const yaw = game.player.yaw;
  const sin = Math.sin(yaw), cos = Math.cos(yaw);
  const R = SIZE / 2 - RIM;
  const scale = R / CONFIG.radar.range;
  return game.enemies.list.map((e) => {
    const dx = e.mesh.position.x - px;
    const dz = e.mesh.position.z - pz;
    // Camera yaw=0 faces -z: right = (cos, -sin), forward = (-sin, -cos).
    // Screen y grows downward, so y = -(forward · d).
    let x = (cos * dx - sin * dz) * scale;
    let y = (sin * dx + cos * dz) * scale;
    const d = Math.hypot(x, y);
    const clamped = d > R;
    if (clamped) { x *= R / d; y *= R / d; }
    return { type: e.type, x, y, clamped };
  });
}

export function updateRadar(game) {
  const canvas = document.getElementById('radar');
  if (!canvas) return; // HUD not mounted (title screen)
  const dpr = Math.min(window.devicePixelRatio, 2);
  const w = Math.round(canvas.clientWidth * dpr);
  if (canvas.width !== w) canvas.width = canvas.height = w;
  const k = w / SIZE;
  const ctx = canvas.getContext('2d');
  ctx.setTransform(k, 0, 0, k, w / 2, w / 2);
  ctx.clearRect(-SIZE / 2, -SIZE / 2, SIZE, SIZE);
  const R = SIZE / 2 - RIM;
  // Half-range ring and bearing cross, faint.
  ctx.strokeStyle = 'rgba(255, 255, 255, 0.12)';
  ctx.lineWidth = 1;
  ctx.beginPath();
  ctx.arc(0, 0, R / 2, 0, Math.PI * 2);
  ctx.stroke();
  ctx.beginPath();
  ctx.moveTo(-R, 0); ctx.lineTo(R, 0);
  ctx.moveTo(0, -R); ctx.lineTo(0, R);
  ctx.stroke();
  for (const b of radarBlips(game)) {
    ctx.globalAlpha = b.clamped ? 0.5 : 1;
    ctx.fillStyle = BLIP[b.type].color;
    ctx.beginPath();
    ctx.arc(b.x, b.y, BLIP[b.type].radius, 0, Math.PI * 2);
    ctx.fill();
  }
  ctx.globalAlpha = 1;
  // The player: a gold wedge pointing up (the aim direction).
  ctx.fillStyle = '#ffd257';
  ctx.beginPath();
  ctx.moveTo(0, -6);
  ctx.lineTo(4.5, 5);
  ctx.lineTo(-4.5, 5);
  ctx.closePath();
  ctx.fill();
}
