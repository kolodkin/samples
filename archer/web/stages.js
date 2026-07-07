import * as THREE from 'three';
import { CONFIG } from './config.js';

export const STAGE_ORDER = ['forest', 'desert', 'iceberg'];

function lambert(color) { return new THREE.MeshLambertMaterial({ color }); }

// Each maker returns { mesh, radius, height } with the mesh's base at y=0.
function makeTree(rng) {
  const g = new THREE.Group();
  const trunkH = rng.range(1.2, 2.0);
  const trunk = new THREE.Mesh(new THREE.CylinderGeometry(0.25, 0.35, trunkH, 6), lambert(0x6b4a2f));
  trunk.position.y = trunkH / 2;
  g.add(trunk);
  let y = trunkH;
  for (const r of [1.5, 1.1]) {
    const cone = new THREE.Mesh(new THREE.ConeGeometry(r, 2.2, 7), lambert(0x2f6b2a));
    cone.position.y = y + 1.1;
    g.add(cone);
    y += 1.4;
  }
  return { mesh: g, radius: 1.5, height: y + 1.8 };
}

function makeDesertObstacle(rng) {
  if (rng.random() < 0.5) {
    // saguaro cactus: trunk + two arms
    const g = new THREE.Group();
    const h = rng.range(2.5, 3.5);
    const trunk = new THREE.Mesh(new THREE.CylinderGeometry(0.35, 0.4, h, 8), lambert(0x3f7d46));
    trunk.position.y = h / 2;
    g.add(trunk);
    for (const side of [-1, 1]) {
      const arm = new THREE.Mesh(new THREE.CylinderGeometry(0.22, 0.22, 1.4, 6), lambert(0x3f7d46));
      arm.position.set(side * 0.6, h * 0.6, 0);
      arm.rotation.z = side * 0.5;
      g.add(arm);
    }
    return { mesh: g, radius: 1.0, height: h };
  }
  const r = rng.range(1.2, 2.2);
  const rock = new THREE.Mesh(new THREE.DodecahedronGeometry(r, 0), lambert(0xa8825d));
  rock.position.y = r * 0.6;
  rock.scale.y = 0.7;
  const g = new THREE.Group();
  g.add(rock);
  return { mesh: g, radius: r, height: r * 1.1 };
}

function makeIcePillar(rng) {
  const h = rng.range(2.5, 4.5);
  const pillar = new THREE.Mesh(
    new THREE.CylinderGeometry(rng.range(0.6, 1.0), rng.range(1.0, 1.6), h, 6),
    new THREE.MeshLambertMaterial({ color: 0xbfe8f7, emissive: 0x224455 }),
  );
  pillar.position.y = h / 2;
  const g = new THREE.Group();
  g.add(pillar);
  return { mesh: g, radius: 1.4, height: h };
}

const THEMES = {
  forest: {
    sky: 0x87b5d4, fog: [0x87b5d4, 40, 130], ground: 0x3e7a3a,
    sun: 0xfff4e0, sunIntensity: 1.0, ambient: 0x777788,
    obstacleCount: 26, obstacle: makeTree, perch: 0x6b6f66,
  },
  desert: {
    sky: 0xf2d9a8, fog: [0xf2d9a8, 50, 150], ground: 0xd9b36c,
    sun: 0xfff0c8, sunIntensity: 1.4, ambient: 0x998877,
    obstacleCount: 14, obstacle: makeDesertObstacle, perch: 0xc2955a,
  },
  iceberg: {
    sky: 0xbfe3f2, fog: [0xbfe3f2, 35, 120], ground: 0xdef2fb,
    sun: 0xe8f4ff, sunIntensity: 1.1, ambient: 0x8899aa,
    obstacleCount: 18, obstacle: makeIcePillar, perch: 0xcfe8f5,
  },
};

export function buildStage(name, rng) {
  const theme = THEMES[name];
  const group = new THREE.Group();

  const size = CONFIG.arena.size + 40;
  const ground = new THREE.Mesh(new THREE.PlaneGeometry(size, size), lambert(theme.ground));
  ground.rotation.x = -Math.PI / 2;
  group.add(ground);

  // The player's elevated vantage point.
  const { x: px, y: py, z: pz } = CONFIG.player.pos;
  const perch = new THREE.Mesh(new THREE.CylinderGeometry(2.2, 3.0, py - 1, 8), lambert(theme.perch));
  perch.position.set(px, (py - 1) / 2, pz);
  group.add(perch);

  group.add(new THREE.HemisphereLight(theme.sky, theme.ground, 0.9));
  const sun = new THREE.DirectionalLight(theme.sun, theme.sunIntensity);
  sun.position.set(20, 40, 10);
  group.add(sun);
  group.add(new THREE.AmbientLight(theme.ambient, 0.4));

  // Obstacles scattered over the battlefield, clear of the player perch.
  const obstacles = [];
  for (let i = 0; i < theme.obstacleCount; i++) {
    const x = rng.range(-36, 36);
    const z = rng.range(-30, 22);
    const { mesh, radius, height } = theme.obstacle(rng);
    mesh.position.set(x, 0, z);
    group.add(mesh);
    obstacles.push({ x, z, radius, height });
  }

  return { group, obstacles, sky: theme.sky, fog: theme.fog };
}
