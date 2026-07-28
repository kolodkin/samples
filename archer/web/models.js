import * as THREE from 'three';

// Articulated enemy figures. Each builder returns a Group whose base sits
// at y=0 with the visual head centered at y=c.height — the collision
// contract in enemies.js stays config-derived (bodyRadius/height/
// headRadius), never measured from these meshes. Limbs are pivot Groups
// parked at the hip/shoulder joints; EnemySystem.animateRig() swings
// rig.legL/legR/armL/armR.rotation.x every frame. Deliberately smooth flat
// tints (no relief-mottle treatment): monsters must pop against the
// textured terrain and props, not blend into them (see SPEC.md).

function lambert(color) { return new THREE.MeshLambertMaterial({ color }); }

function part(geo, color, x = 0, y = 0, z = 0) {
  const m = new THREE.Mesh(geo, lambert(color));
  m.position.set(x, y, z);
  return m;
}

// A limb hangs from its joint: the group sits at the joint so rotating it
// swings the whole limb; the tapered segment and the hand/foot cap extend
// downward. `tilt` is a static outward lean (rotation.z) that keeps arms
// clear of wide torsos — the walk swing only ever touches rotation.x.
function limb({ x, y, len, r, color, cap = 0, capColor = color, tilt = 0 }) {
  const g = new THREE.Group();
  g.position.set(x, y, 0);
  g.rotation.z = tilt;
  g.add(part(new THREE.CylinderGeometry(r, r * 0.75, len, 6), color, 0, -len / 2, 0));
  if (cap) g.add(part(new THREE.SphereGeometry(cap, 6, 5), capColor, 0, -len, 0));
  return g;
}

function eyes(head, { x, y, z, r, color = 0x1c140c }) {
  for (const s of [-1, 1]) head.add(part(new THREE.SphereGeometry(r, 5, 4), color, s * x, y, z));
}

// rig tuning: swing = leg amplitude (rad), armFactor = arm counter-swing
// fraction, stride = metres per full cycle (phase advances by distance
// covered, so feet never slide), bob = footfall bounce height, aimPose =
// arms-forward rotation blended in while an archer peeks.
function buildGoblin(c) {
  const skin = 0x5ea34c;
  const g = new THREE.Group();
  const legL = limb({ x: -0.18, y: 0.5, len: 0.46, r: 0.09, color: 0x3f6b33, cap: 0.09, capColor: skin });
  const legR = limb({ x: 0.18, y: 0.5, len: 0.46, r: 0.09, color: 0x3f6b33, cap: 0.09, capColor: skin });
  const torso = part(new THREE.SphereGeometry(0.42, 8, 6), c.color, 0, 0.74, 0);
  torso.scale.set(1, 0.92, 0.85); // pot-bellied tunic
  const armL = limb({ x: -0.46, y: 0.98, len: 0.52, r: 0.075, color: skin, cap: 0.08, tilt: 0.18 });
  const armR = limb({ x: 0.46, y: 0.98, len: 0.52, r: 0.075, color: skin, cap: 0.08, tilt: -0.18 });
  const club = part(new THREE.CylinderGeometry(0.09, 0.04, 0.5, 6), 0x6b4a2f, 0, -0.5, 0.18);
  club.rotation.x = -0.7;
  armR.add(club);
  const head = new THREE.Group();
  head.position.y = c.height;
  head.add(part(new THREE.SphereGeometry(c.headRadius, 8, 6), skin));
  for (const s of [-1, 1]) {
    const ear = part(new THREE.ConeGeometry(0.07, 0.25, 4), skin, s * c.headRadius, 0.1, 0);
    ear.rotation.z = -s * 1.2;
    head.add(ear);
  }
  eyes(head, { x: 0.11, y: 0.03, z: c.headRadius * 0.82, r: 0.045 });
  const nose = part(new THREE.ConeGeometry(0.05, 0.14, 4), skin, 0, -0.04, c.headRadius);
  nose.rotation.x = Math.PI / 2;
  head.add(nose);
  g.add(legL, legR, torso, armL, armR, head);
  g.userData.rig = { legL, legR, armL, armR, swing: 0.85, armFactor: 0.7, stride: 1.1, bob: 0.06 };
  return g;
}

function buildSkeleton(c) {
  const bone = 0xe8e4d8;
  const g = new THREE.Group();
  const legL = limb({ x: -0.13, y: 0.84, len: 0.8, r: 0.055, color: bone, cap: 0.055 });
  const legR = limb({ x: 0.13, y: 0.84, len: 0.8, r: 0.055, color: bone, cap: 0.055 });
  const pelvis = part(new THREE.BoxGeometry(0.34, 0.14, 0.2), c.color, 0, 0.88, 0);
  const spine = part(new THREE.CylinderGeometry(0.04, 0.04, 0.34, 5), c.color, 0, 1.08, 0);
  const ribs = part(new THREE.SphereGeometry(0.22, 8, 6), c.color, 0, 1.3, 0);
  ribs.scale.set(1, 1.05, 0.72);
  g.add(pelvis, spine, ribs);
  for (const y of [1.24, 1.36]) { // rib rings over the cage
    const ring = part(new THREE.TorusGeometry(0.22, 0.018, 4, 10), 0xb9b3a4, 0, y, 0);
    ring.rotation.x = Math.PI / 2;
    ring.scale.y = 0.75; // local y maps to world z after the flip
    g.add(ring);
  }
  const armL = limb({ x: -0.31, y: 1.44, len: 0.62, r: 0.05, color: bone, cap: 0.05, tilt: 0.12 });
  const armR = limb({ x: 0.31, y: 1.44, len: 0.62, r: 0.05, color: bone, cap: 0.05, tilt: -0.12 });
  for (const arm of [armL, armR]) arm.add(part(new THREE.SphereGeometry(0.07, 5, 4), bone));
  // The bow rides the left hand, so the peek aim pose raises it. Held flat
  // at the hip at rest; upright and facing the player once the arm levels.
  const bow = new THREE.Group();
  bow.position.set(0, -0.62, 0.02);
  bow.rotation.x = Math.PI / 2;
  const stave = part(new THREE.TorusGeometry(0.3, 0.03, 5, 16, Math.PI), 0x6b4a2f);
  const string = part(new THREE.CylinderGeometry(0.008, 0.008, 0.6, 4), 0xd9d4c5);
  string.rotation.z = Math.PI / 2;
  bow.add(stave, string);
  armL.add(bow);
  const head = new THREE.Group();
  head.position.y = c.height;
  head.add(part(new THREE.SphereGeometry(c.headRadius, 8, 6), bone));
  eyes(head, { x: 0.1, y: 0.04, z: c.headRadius * 0.78, r: 0.055, color: 0x14100c });
  head.add(part(new THREE.BoxGeometry(0.16, 0.09, 0.12), bone, 0, -0.24, 0.09));
  g.add(legL, legR, armL, armR, head);
  g.userData.rig = { legL, legR, armL, armR, swing: 0.7, armFactor: 0.55, stride: 1.4, bob: 0.045,
                     aimPose: -1.3 };
  return g;
}

function buildOgre(c) {
  const skin = 0x8a765f;
  const g = new THREE.Group();
  const legL = limb({ x: -0.42, y: 1.15, len: 1.1, r: 0.23, color: 0x63523f, cap: 0.2, capColor: 0x63523f });
  const legR = limb({ x: 0.42, y: 1.15, len: 1.1, r: 0.23, color: 0x63523f, cap: 0.2, capColor: 0x63523f });
  const torso = part(new THREE.SphereGeometry(0.82, 9, 7), c.color, 0, 1.68, 0);
  torso.scale.set(1.05, 0.95, 0.8); // barrel chest
  const armL = limb({ x: -0.95, y: 2.1, len: 1.15, r: 0.19, color: skin, cap: 0.22, tilt: 0.12 });
  const armR = limb({ x: 0.95, y: 2.1, len: 1.15, r: 0.19, color: skin, cap: 0.22, tilt: -0.12 });
  for (const arm of [armL, armR]) arm.add(part(new THREE.SphereGeometry(0.26, 6, 5), c.color));
  const head = new THREE.Group();
  head.position.y = c.height;
  head.add(part(new THREE.SphereGeometry(c.headRadius, 8, 6), skin));
  eyes(head, { x: 0.15, y: 0.06, z: c.headRadius * 0.8, r: 0.06 });
  for (const s of [-1, 1]) { // underbite tusks
    head.add(part(new THREE.ConeGeometry(0.05, 0.16, 4), 0xe8e4d8, s * 0.15, -0.24, c.headRadius * 0.66));
  }
  g.add(legL, legR, torso, armL, armR, head);
  g.userData.rig = { legL, legR, armL, armR, swing: 0.5, armFactor: 0.55, stride: 2.2, bob: 0.05 };
  return g;
}

const BUILDERS = { goblin: buildGoblin, skeleton: buildSkeleton, ogre: buildOgre };

export function buildEnemyModel(type, c) { return BUILDERS[type](c); }
