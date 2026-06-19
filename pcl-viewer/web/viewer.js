// three.js viewer: owns all 3D state behind a small imperative handle.
// Knows nothing about Preact. Exposes window.__PCL for deterministic e2e.
import * as THREE from 'three';
import { OrbitControls } from 'three/addons/controls/OrbitControls.js';
import { PCDLoader } from 'three/addons/loaders/PCDLoader.js';

const BG = 0x101418;
const FLAT_COLOR = 0x66ccff;

export function createViewer(canvas, { modelUrl = './models/Zaghetto.pcd' } = {}) {
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
  let baseColors = null; // Float32Array of height-mapped colors
  const helpers = new THREE.Group();
  helpers.visible = false;
  scene.add(helpers);

  const state = {
    ready: false,
    pointCount: 0,
    settings: { pointSize: 0.004, colorMode: 'height', helpers: false },
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
    const size = box.getSize(new THREE.Vector3());
    const center = box.getCenter(new THREE.Vector3());
    const radius = Math.max(size.x, size.y, size.z);
    controls.target.copy(center);
    // Mostly front-on (this is a single-viewpoint 2.5D scan, so a steep angle
    // just shows its thin edge) with a slight tilt from the upper-right so the
    // surface relief reads as 3D instead of a flat silhouette.
    const dir = new THREE.Vector3(0.28, 0.18, 1).normalize();
    camera.position.copy(center).add(dir.multiplyScalar(radius * 2.2));
    camera.near = radius / 100;
    camera.far = radius * 100;
    camera.updateProjectionMatrix();
    controls.update();
  }

  function computeHeightColors(geometry) {
    const pos = geometry.getAttribute('position');
    const box = new THREE.Box3().setFromBufferAttribute(pos);
    const minY = box.min.y;
    const span = box.max.y - box.min.y || 1;
    const colors = new Float32Array(pos.count * 3);
    const c = new THREE.Color();
    for (let i = 0; i < pos.count; i++) {
      const t = (pos.getY(i) - minY) / span; // color along the vertical axis
      c.setHSL(0.7 - 0.7 * t, 0.9, 0.5); // blue (low) -> red (high)
      colors[i * 3] = c.r;
      colors[i * 3 + 1] = c.g;
      colors[i * 3 + 2] = c.b;
    }
    return colors;
  }

  function applyColorMode(mode) {
    if (!points) return;
    state.settings.colorMode = mode;
    if (mode === 'height') {
      points.geometry.setAttribute(
        'color', new THREE.BufferAttribute(baseColors, 3));
      points.material.vertexColors = true;
      points.material.color.set(0xffffff);
    } else {
      points.geometry.deleteAttribute('color');
      points.material.vertexColors = false;
      points.material.color.set(FLAT_COLOR);
    }
    points.material.needsUpdate = true;
  }

  function buildHelpers() {
    helpers.clear();
    const box = new THREE.Box3().setFromObject(points);
    helpers.add(new THREE.Box3Helper(box, 0xffaa00));
    const size = box.getSize(new THREE.Vector3()).length();
    helpers.add(new THREE.AxesHelper(size * 0.5));
  }

  const loader = new PCDLoader();
  loader.load(modelUrl, (loaded) => {
    points = loaded;
    points.material = new THREE.PointsMaterial({
      size: state.settings.pointSize,
      color: FLAT_COLOR,
      sizeAttenuation: true,
    });
    scene.add(points);
    baseColors = computeHeightColors(points.geometry);
    applyColorMode(state.settings.colorMode); // height ramp by default
    state.pointCount = points.geometry.getAttribute('position').count;
    buildHelpers();
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
    resetCamera() { if (points) frameCamera(); },
    toggleHelpers(on) { helpers.visible = on; state.settings.helpers = on; },
    getStats() {
      return {
        pointCount: state.pointCount,
        cameraDistance: camera.position.distanceTo(controls.target),
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
