import * as THREE from 'three';
import { GLTFLoader } from './vendor/loaders/GLTFLoader.js';
import { clone as cloneSkinned } from './vendor/utils/SkeletonUtils.js';

// Animated CC0 enemy characters (see assets/CREDITS.txt): the goblin
// (Tribal) and ogre (Orc) are Quaternius "Ultimate Monsters" characters
// with their animations baked in; the skeleton archer is KayKit's
// Skeleton_Minion with a crossbow socketed into its hand bone, animated
// by the shared KayKit Rig_Medium clip library. Everything loads once at module init
// (top-level await, before the first frame and before __ARCHER.ready), so
// buildEnemyModel() stays synchronous for spawns and e2e hooks.
//
// The collision contract in enemies.js is untouched: hit spheres come from
// config (bodyRadius/height/headRadius); `height` below scales each model
// so its visual head sits at the config head sphere (y = c.height).

const loader = new GLTFLoader();
const load = (url) => new Promise((resolve, reject) => loader.load(url, resolve, undefined, reject));

const [goblinGltf, ogreGltf, skeletonGltf, moveGltf, generalGltf, crossbowGltf] =
  await Promise.all([
    load('./assets/goblin.glb'),
    load('./assets/ogre.glb'),
    load('./assets/skeleton.glb'),
    load('./assets/skeleton_anim_movement.glb'),
    load('./assets/skeleton_anim_general.glb'),
    load('./assets/crossbow.glb'),
  ]);

// The crossbow rides the skeleton's hand socket in the template, so every
// clone carries it and hand-bone animation (walk sway, the Throw attack)
// moves the weapon for free. The socket is authored as "handslot.r", but
// GLTFLoader sanitizes node names (dots are reserved in track paths), so
// match loosely instead of by exact name.
let handSlot = null;
skeletonGltf.scene.traverse((o) => { if (/^handslot.?r$/i.test(o.name)) handSlot = o; });
handSlot.add(crossbowGltf.scene);

// walkRef: ground speed (m/s) the Walk clip reads naturally at, at this
// model's final scale — animateRig sets timeScale = actual speed / walkRef
// so footfalls track the ground and never slide. attack is optional
// (LoopOnce, triggered by EnemySystem when an archer starts its peek).
// Standing height of the model as RENDERED: the highest skinned world-space
// vertex of the rest pose. Box3.setFromObject ignores skinning, and these
// rigs carry most of their size in bone transforms — the Quaternius figures
// draw ~2-2.6× taller than their raw geometry box, so a box-derived scale
// blows the model up past the config hit spheres and aimed headshots whiff.
// One CPU pass over a few thousand vertices, once per template at load.
function standingHeight(scene) {
  scene.updateMatrixWorld(true);
  const v = new THREE.Vector3();
  let top = 0; // floor at y=0: below-ground bind-pose parts must not deflate the scale
  scene.traverse((o) => {
    if (!o.isMesh) return;
    for (let i = 0; i < o.geometry.attributes.position.count; i++) {
      o.getVertexPosition(i, v).applyMatrix4(o.matrixWorld);
      if (v.y > top) top = v.y;
    }
  });
  return top;
}

function template(gltf, { height, face = 0, clips = gltf.animations, walk, idle, attack, walkRef }) {
  return {
    scene: gltf.scene, clips, walk, idle, attack, walkRef, face,
    scale: height / standingHeight(gltf.scene),
  };
}

const TEMPLATES = {
  goblin: template(goblinGltf, {
    // Goblins rush at ~4.4-6.3 m/s — that's a sprint, so their movement
    // clip is Run, not Walk.
    height: 1.7, walk: 'Run', idle: 'Idle', walkRef: 2.3,
  }),
  ogre: template(ogreGltf, {
    // Big but not kaiju: the face lands in the lower half of the config
    // head sphere (2.05..2.95), so aiming at what you see still headshots.
    height: 2.6, walk: 'Walk', idle: 'Idle', walkRef: 0.86,
  }),
  skeleton: template(skeletonGltf, {
    // Chibi proportions + a hunched idle: scale past the nominal 1.6 m so
    // the posed skull sits at the config head sphere (y = c.height).
    height: 2.0, walk: 'Walking_A', idle: 'Idle_B', attack: 'Throw', walkRef: 1.9,
    clips: [...moveGltf.animations, ...generalGltf.animations],
  }),
};

export function buildEnemyModel(type, c) { // eslint-disable-line no-unused-vars
  const t = TEMPLATES[type];
  const inst = cloneSkinned(t.scene);
  inst.scale.setScalar(t.scale);
  inst.rotation.y = t.face;
  // Clone materials per spawn: freeze/burn tints (EnemySystem.setTint sets
  // material.emissive) must never leak between enemies sharing a template.
  inst.traverse((o) => { if (o.isMesh) o.material = o.material.clone(); });
  const wrapper = new THREE.Group();
  wrapper.add(inst);

  const mixer = new THREE.AnimationMixer(inst);
  const action = (name) => mixer.clipAction(THREE.AnimationClip.findByName(t.clips, name));
  const walk = action(t.walk).play();
  walk.setEffectiveWeight(0); // stand at spawn; animateRig blends walking in
  const idle = action(t.idle).play();
  const anim = { mixer, walk, idle, walkRef: t.walkRef, attack: null };
  if (t.attack) {
    const attack = action(t.attack);
    attack.setLoop(THREE.LoopOnce, 1);
    attack.clampWhenFinished = true; // hold the last pose, then fade out
    mixer.addEventListener('finished', (ev) => { if (ev.action === attack) attack.fadeOut(0.2); });
    anim.attack = attack;
    anim.playAttack = () => attack.reset().setEffectiveWeight(1).fadeIn(0.1).play();
  }
  wrapper.userData.anim = anim;
  return wrapper;
}
