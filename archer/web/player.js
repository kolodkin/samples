import * as THREE from 'three';
import { CONFIG } from './config.js';

function buildBowViewmodel() {
  const g = new THREE.Group();
  const wood = new THREE.MeshLambertMaterial({ color: 0x7a5230 });
  // Limb + string live in their own group, rotated so the bow's plane
  // contains the aim line: the limb bulges downrange (toward the target)
  // and the string sits on the archer's side, like a bow actually held.
  const limbGroup = new THREE.Group();
  const limb = new THREE.Mesh(new THREE.TorusGeometry(0.16, 0.014, 6, 24, Math.PI), wood);
  limb.rotation.z = Math.PI / 2; // tips at (0, ±0.16), bulge -x (pre-rotation)
  limbGroup.add(limb);
  // String: two segments pinned at the limb tips, meeting at the nock —
  // they fold into a V as the draw pulls the nock back (see updateString).
  const stringMat = new THREE.MeshBasicMaterial({ color: 0xeeeeee });
  const segGeo = new THREE.BoxGeometry(0.003, 1, 0.003); // unit length, scaled per frame
  const stringTop = new THREE.Mesh(segGeo, stringMat);
  const stringBottom = new THREE.Mesh(segGeo, stringMat);
  limbGroup.add(stringTop, stringBottom);
  limbGroup.rotation.y = -Math.PI / 2 + 0.25; // bulge → forward, slight angle so the curve reads
  g.add(limbGroup);
  // Nocked arrow: shaft + head along the aim line. The nock sits on the
  // string (z=0) and the head protrudes past the bow's front (-0.16) —
  // even at full draw, when the whole arrow slides back with the string.
  const nocked = new THREE.Group();
  const shaft = new THREE.Mesh(
    new THREE.CylinderGeometry(0.006, 0.006, 0.36, 5),
    new THREE.MeshLambertMaterial({ color: 0xd8c9a3 }),
  );
  shaft.rotation.x = Math.PI / 2;
  shaft.position.z = -0.18;
  nocked.add(shaft);
  const tip = new THREE.Mesh(
    new THREE.ConeGeometry(0.016, 0.05, 6),
    new THREE.MeshLambertMaterial({ color: 0x555555 }),
  );
  tip.rotation.x = -Math.PI / 2; // cone points downrange
  tip.position.z = -0.385;
  nocked.add(tip);
  // Fletching: feather vanes at the nock, tapering forward from the string.
  const fletch = new THREE.Mesh(
    new THREE.ConeGeometry(0.02, 0.09, 4),
    new THREE.MeshLambertMaterial({ color: 0xcc4444 }),
  );
  fletch.rotation.x = -Math.PI / 2;
  fletch.position.z = -0.045;
  nocked.add(fletch);
  g.add(nocked);
  g.position.set(0.26, -0.22, -0.6);
  return { group: g, stringTop, stringBottom, nocked };
}

const STRING_TIP_Y = 0.16; // limb tip height (torus radius)

export class Player {
  constructor(camera) {
    this.camera = camera;
    this.hp = CONFIG.player.hp;
    this.yaw = 0;       // facing -z, toward the spawn edge
    this.pitch = 0;
    this.power = CONFIG.bow.power.initial;
    this.shootT = Infinity; // seconds since the last shot; Infinity = at rest
    this.bow = buildBowViewmodel();
    camera.add(this.bow.group);
    camera.rotation.order = 'YXZ';
  }

  // A shot is allowed only once the fresh arrow is nocked and drawn.
  canShoot() {
    const { snap, reload, nock } = CONFIG.bow.shot;
    return this.shootT >= snap + reload + nock;
  }

  shoot() { this.shootT = 0; }

  look(dx, dy) {
    this.yaw -= dx * 0.0022;
    this.pitch = Math.max(-1.4, Math.min(1.4, this.pitch - dy * 0.0022));
  }

  adjustPower(dir) {
    const { min, max, step } = CONFIG.bow.power;
    // Snap to the step grid so repeated float adds never drift off the stops.
    const stepped = Math.round((this.power + dir * step) / step) * step;
    this.power = Math.min(max, Math.max(min, Math.round(stepped * 100) / 100));
  }

  takeDamage(n) { this.hp = Math.max(0, this.hp - n); }
  heal(n) { this.hp = Math.min(CONFIG.player.hp, this.hp + n); }
  resetHp() { this.hp = CONFIG.player.hp; }

  aimDir() {
    return this.camera.getWorldDirection(new THREE.Vector3());
  }

  aimOrigin() {
    return this.camera.getWorldPosition(new THREE.Vector3())
      .addScaledVector(this.aimDir(), 0.7);
  }

  update(dt) {
    this.camera.rotation.set(this.pitch, this.yaw, 0);
    // Ease FOV toward zoom at max power.
    const targetFov = this.power >= 1 ? CONFIG.bow.zoomFov : CONFIG.bow.baseFov;
    this.camera.fov += (targetFov - this.camera.fov) * Math.min(1, dt * 8);
    this.camera.updateProjectionMatrix();
    // The FOV is vertical, so portrait screens get a narrow horizontal
    // frustum — slide the bow inward (and shrink it a touch) so it stays
    // fully in frame without dominating the view.
    const halfW = Math.tan(THREE.MathUtils.degToRad(this.camera.fov) / 2)
      * 0.6 * this.camera.aspect;
    this.bow.group.position.x = Math.min(0.26, Math.max(0.08, halfW - 0.16));
    this.bow.group.scale.setScalar(Math.min(1, 0.7 + this.camera.aspect * 0.25));
    // Release animation: as the shot leaves, the string snaps forward and
    // the nocked arrow vanishes (it became the projectile); the bow sits
    // empty for a beat, then a new arrow appears and rides the string back
    // out to the persistent power draw, ready for the next shot.
    this.shootT += dt;
    const { snap, reload, nock } = CONFIG.bow.shot;
    const t = this.shootT;
    let draw = this.power;
    if (t < snap) draw = this.power * (1 - t / snap);
    else if (t < snap + reload) draw = 0;
    else if (t < snap + reload + nock) draw = this.power * ((t - snap - reload) / nock);
    this.bow.nocked.visible = t >= snap + reload;
    // Pull the string and nocked arrow back with the draw. Each string
    // half runs from its limb tip to the nock at (pull, 0), forming a V.
    const pull = draw * 0.15;
    for (const [seg, side] of [[this.bow.stringTop, 1], [this.bow.stringBottom, -1]]) {
      const v = new THREE.Vector3(pull, -side * STRING_TIP_Y, 0); // tip → nock
      seg.scale.y = v.length();
      seg.position.set(pull / 2, side * STRING_TIP_Y / 2, 0);
      seg.quaternion.setFromUnitVectors(new THREE.Vector3(0, 1, 0), v.normalize());
    }
    this.bow.nocked.position.z = pull;
  }
}
