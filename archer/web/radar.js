import { CONFIG } from './config.js';

// Monster radar in the HUD's top-left corner: a player-centered minimap,
// rotated so "up" is the aim direction. Enemies inside CONFIG.radar.range
// appear at scale; farther contacts pin to the rim at half opacity, so an
// incoming wave reads as a ring of bearings before it closes. Redrawn
// imperatively every frame (like the --power CSS variable) — enemy motion
// is far too chatty for the UI store. The canvas itself lives in the Hud
// component (ui.js), so it mounts and unmounts with the rest of the HUD.

const SIZE = 132; // CSS px; the backing store is 2x for crispness
const DPR = 2;
const RIM = 6;    // blips clamp this far inside the edge

const BLIP_RADIUS = { goblin: 3, skeleton: 3, ogre: 4.5 };

function cssColor(hex) {
  return `#${hex.toString(16).padStart(6, '0')}`;
}

export class Radar {
  constructor(game) {
    this.game = game;
  }

  // Blip positions in radar px relative to the center (+x right, +y down,
  // up = player facing); `clamped` marks out-of-range contacts pinned to
  // the rim. Exposed on __ARCHER.state for the e2e tests.
  blips() {
    const { x: px, z: pz } = CONFIG.player.pos;
    const yaw = this.game.player.yaw;
    const sin = Math.sin(yaw), cos = Math.cos(yaw);
    const R = SIZE / 2 - RIM;
    const scale = R / CONFIG.radar.range;
    return this.game.enemies.list.map((e) => {
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

  update() {
    const canvas = document.getElementById('radar');
    if (!canvas) return; // HUD not mounted (title screen)
    if (canvas.width !== SIZE * DPR) canvas.width = canvas.height = SIZE * DPR;
    const ctx = canvas.getContext('2d');
    ctx.setTransform(DPR, 0, 0, DPR, (SIZE / 2) * DPR, (SIZE / 2) * DPR);
    ctx.clearRect(-SIZE / 2, -SIZE / 2, SIZE, SIZE);
    const R = SIZE / 2 - RIM;
    // Dish + rim, matching the HUD's dark-panel / gold-accent look.
    ctx.beginPath();
    ctx.arc(0, 0, SIZE / 2 - 1.5, 0, Math.PI * 2);
    ctx.fillStyle = 'rgba(0, 0, 0, 0.45)';
    ctx.fill();
    ctx.strokeStyle = 'rgba(255, 210, 87, 0.3)';
    ctx.lineWidth = 2;
    ctx.stroke();
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
    // Enemy blips, tinted like their meshes so the radar and the field agree.
    for (const b of this.blips()) {
      ctx.globalAlpha = b.clamped ? 0.5 : 1;
      ctx.fillStyle = cssColor(CONFIG.enemies[b.type].color);
      ctx.beginPath();
      ctx.arc(b.x, b.y, BLIP_RADIUS[b.type], 0, Math.PI * 2);
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
}
