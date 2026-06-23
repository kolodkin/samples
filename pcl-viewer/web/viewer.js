// three.js viewer: owns all 3D state behind a small imperative handle.
// Knows nothing about Preact. Exposes window.__PCL for deterministic e2e.
import * as THREE from 'three';
import { OrbitControls } from 'three/addons/controls/OrbitControls.js';
import { PCDLoader } from 'three/addons/loaders/PCDLoader.js';

const BG = 0x101418;
const FLAT_COLOR = 0x66ccff;

// Render each point either as a lit sphere impostor ("ball", the default) or as
// the plain flat square sprite ("square", three.js's stock point look). Both go
// through PointsMaterial.onBeforeCompile so the size slider, size attenuation,
// and per-vertex color ramps keep working untouched; a `uBall` uniform (1/0)
// flips the fragment behavior at runtime with no recompile. For the ball: clip
// the quad to a circle, rebuild a hemisphere normal from gl_PointCoord, and
// shade it (ambient + diffuse + a tight specular highlight) so every point reads
// as a tiny 3D ball. The light is fixed in view space, so the highlights stay
// put as the cloud orbits — like a studio key light on the lens.
function installPointShapeShading(material) {
  material.userData.ballUniform = { value: 1 }; // 1 = ball, 0 = square
  material.onBeforeCompile = (shader) => {
    shader.uniforms.uBall = material.userData.ballUniform;
    shader.fragmentShader = 'uniform float uBall;\n' + shader.fragmentShader.replace(
      '#include <color_fragment>',
      `#include <color_fragment>
      if (uBall > 0.5) {
        // gl_PointCoord is top-left origin; map to [-1,1] and flip Y so the
        // light reads as coming from above-front. Outside the unit disc -> clip.
        vec2 impostorUv = vec2(1.0, -1.0) * (2.0 * gl_PointCoord - 1.0);
        float impostorR2 = dot(impostorUv, impostorUv);
        if (impostorR2 > 1.0) discard;
        vec3 impostorN = vec3(impostorUv, sqrt(1.0 - impostorR2));
        vec3 impostorL = normalize(vec3(0.35, 0.55, 0.75));
        float impostorDiff = max(dot(impostorN, impostorL), 0.0);
        float impostorSpec = pow(max(
          dot(reflect(-impostorL, impostorN), vec3(0.0, 0.0, 1.0)), 0.0), 24.0);
        float impostorShade = 0.35 + 0.75 * impostorDiff;
        diffuseColor.rgb = diffuseColor.rgb * impostorShade + vec3(impostorSpec) * 0.5;
      }`,
    );
  };
}

export function createViewer(canvas, { modelUrl = './models/kitti-velodyne-000000.pcd' } = {}) {
  const renderer = new THREE.WebGLRenderer({
    canvas,
    antialias: true,
    preserveDrawingBuffer: true, // lets e2e read pixels at any time
  });
  renderer.setPixelRatio(window.devicePixelRatio || 1);

  const scene = new THREE.Scene();
  scene.background = new THREE.Color(BG);

  const camera = new THREE.PerspectiveCamera(45, 1, 0.001, 1000);
  const controls = new OrbitControls(camera, renderer.domElement);
  controls.enableDamping = true;

  let points = null;
  let colorBuffers = {}; // mode name -> Float32Array of per-point ramp colors
  let sceneRadius = 1; // robust horizontal radius of the scan, in normalized units

  const state = {
    ready: false,
    pointCount: 0,
    settings: { pointSize: 0.004, colorMode: 'height', pointShape: 'ball' },
    framesRendered: 0,
  };
  window.__PCL = state;

  function resize() {
    const w = canvas.clientWidth || canvas.parentElement.clientWidth || 800;
    const h = canvas.clientHeight || canvas.parentElement.clientHeight || 600;
    renderer.setSize(w, h, false);
    camera.aspect = w / h;
    camera.updateProjectionMatrix();
  }
  window.addEventListener('resize', resize);

  function frameCamera() {
    const box = new THREE.Box3().setFromObject(points);
    const center = box.getCenter(new THREE.Vector3());
    const radius = sceneRadius; // ignore far stray returns so the scene fills the view
    // Axis convention after normalizeGeometry: +X is forward (down the road),
    // +Y is up, +Z is right, and the sensor sits at the cloud center. Pull the
    // eye up and behind the sensor and aim it forward at the road ahead, an
    // elevated chase view that pulls back so the whole scene reads at once (the
    // sensor blind-spot ring sits in the foreground). The eye sits above the
    // target, so the view still looks down even with a level aim point.
    // Offsets are round multiples of 0.1 so that, at sceneRadius 0.5, the eye
    // and target land on clean values in the HUD readout.
    const eye = center.clone().add(new THREE.Vector3(-1.4, 1.2, 0).multiplyScalar(radius));
    const look = center.clone().add(new THREE.Vector3(0.8, 0, 0).multiplyScalar(radius));
    camera.position.copy(eye);
    controls.target.copy(look);
    camera.near = radius / 100;
    camera.far = radius * 100;
    camera.updateProjectionMatrix();
    controls.update();
  }

  // Map a per-point scalar field onto the blue->red HSL ramp. Robust 2nd..98th
  // percentile clamp so a handful of outliers don't compress the whole ramp
  // into one hue (low values stay blue, high values climb through green to red).
  function rampColors(values) {
    const n = values.length;
    const sorted = Float32Array.from(values).sort();
    const min = sorted[Math.floor(n * 0.02)];
    const span = (sorted[Math.floor(n * 0.98)] - min) || 1;
    const colors = new Float32Array(n * 3);
    const c = new THREE.Color();
    for (let i = 0; i < n; i++) {
      const t = Math.min(1, Math.max(0, (values[i] - min) / span));
      c.setHSL(0.7 - 0.7 * t, 0.9, 0.5); // blue (low) -> red (high)
      colors[i * 3] = c.r;
      colors[i * 3 + 1] = c.g;
      colors[i * 3 + 2] = c.b;
    }
    return colors;
  }

  // Precompute a ramp buffer for every scalar mode the cloud can supply:
  // height (y), radial distance from the sensor (origin), and laser intensity
  // (only if the source PCD carried an `intensity` field).
  function computeColorBuffers(geometry) {
    const pos = geometry.getAttribute('position');
    const n = pos.count;
    const height = new Float32Array(n);
    const distance = new Float32Array(n);
    for (let i = 0; i < n; i++) {
      height[i] = pos.getY(i);
      distance[i] = Math.hypot(pos.getX(i), pos.getY(i), pos.getZ(i));
    }
    const buffers = { height: rampColors(height), distance: rampColors(distance) };
    const intensity = geometry.getAttribute('intensity');
    if (intensity) {
      const vals = Float32Array.from({ length: n }, (_, i) => intensity.getX(i));
      buffers.intensity = rampColors(vals);
    }
    return buffers;
  }

  function applyColorMode(mode) {
    if (!points) return;
    const buffer = mode === 'flat' ? null : colorBuffers[mode];
    // A scalar mode the cloud can't supply (e.g. intensity-free data) falls
    // back to flat shading rather than rendering nothing.
    state.settings.colorMode = mode === 'flat' || buffer ? mode : 'flat';
    if (buffer) {
      points.geometry.setAttribute('color', new THREE.BufferAttribute(buffer, 3));
      points.material.vertexColors = true;
      points.material.color.set(0xffffff);
    } else {
      points.geometry.deleteAttribute('color');
      points.material.vertexColors = false;
      points.material.color.set(FLAT_COLOR);
    }
    points.material.needsUpdate = true;
  }

  // Normalize an arbitrary cloud into the viewer's working frame: KITTI scans
  // are z-up and ~80 m across, so rotate them y-up, center on the origin and
  // scale to ~unit size. This keeps the camera framing and the point-size
  // slider range meaningful regardless of the source dataset's units.
  function normalizeGeometry(geom) {
    geom.rotateX(-Math.PI / 2); // z-up (vehicle frame) -> three.js y-up
    geom.computeBoundingBox();
    const center = geom.boundingBox.getCenter(new THREE.Vector3());
    geom.translate(-center.x, -center.y, -center.z);
    // Scale by a robust horizontal radius (90th percentile of distance from the
    // sensor) rather than the absolute max, so the dense street scene fills the
    // frame instead of being shrunk by a few 80 m stray returns.
    const pos = geom.getAttribute('position');
    const radii = Float32Array.from(
      { length: pos.count }, (_, i) => Math.hypot(pos.getX(i), pos.getZ(i))).sort();
    const r = radii[Math.floor(pos.count * 0.9)] || 1;
    const s = 0.5 / r; // characteristic radius -> 0.5 units
    geom.scale(s, s, s);
    sceneRadius = 0.5;
  }

  const loader = new PCDLoader();
  loader.load(modelUrl, (loaded) => {
    points = loaded;
    normalizeGeometry(points.geometry);
    points.material = new THREE.PointsMaterial({
      size: state.settings.pointSize,
      color: FLAT_COLOR,
      sizeAttenuation: true,
    });
    installPointShapeShading(points.material); // ball (default) or square sprite
    scene.add(points);
    colorBuffers = computeColorBuffers(points.geometry);
    applyColorMode(state.settings.colorMode); // height ramp by default
    state.pointCount = points.geometry.getAttribute('position').count;
    resize();
    frameCamera();
    state.ready = true;
  });

  let raf = 0;
  function tick() {
    raf = requestAnimationFrame(tick);
    controls.update();
    renderer.render(scene, camera);
    state.framesRendered++;
  }
  resize();
  tick();

  const handle = {
    setPointSize(n) {
      state.settings.pointSize = n;
      if (points) { points.material.size = n; points.material.needsUpdate = true; }
    },
    setColorMode(mode) { applyColorMode(mode); },
    setPointShape(shape) {
      state.settings.pointShape = shape === 'square' ? 'square' : 'ball';
      if (points) {
        points.material.userData.ballUniform.value =
          state.settings.pointShape === 'ball' ? 1 : 0;
      }
    },
    resetCamera() { if (points) frameCamera(); },
    getStats() {
      const e = camera.position, t = controls.target;
      const vec = (v) => ({ x: v.x, y: v.y, z: v.z }); // snapshot, don't leak the live Vector3
      return {
        pointCount: state.pointCount,
        cameraDistance: e.distanceTo(t),
        eye: vec(e),
        target: vec(t),
      };
    },
    // e2e helper: count non-background pixels in the rendered frame.
    visiblePixelCount() {
      const gl = renderer.getContext();
      const w = renderer.domElement.width;
      const h = renderer.domElement.height;
      const buf = new Uint8Array(w * h * 4);
      gl.readPixels(0, 0, w, h, gl.RGBA, gl.UNSIGNED_BYTE, buf);
      const br = ((BG >> 16) & 255), bg = ((BG >> 8) & 255), bb = BG & 255;
      let n = 0;
      for (let i = 0; i < buf.length; i += 4) {
        if (Math.abs(buf[i] - br) + Math.abs(buf[i + 1] - bg) + Math.abs(buf[i + 2] - bb) > 24) n++;
      }
      return n;
    },
    dispose() {
      cancelAnimationFrame(raf);
      window.removeEventListener('resize', resize);
      controls.dispose();
      if (points) {
        points.geometry.dispose();
        points.material.dispose();
      }
      renderer.dispose();
    },
  };
  state.handle = handle; // expose for e2e
  return handle;
}
