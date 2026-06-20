// three.js viewer: owns all 3D state behind a small imperative handle.
// Knows nothing about Preact. Exposes window.__PCL for deterministic e2e.
import * as THREE from 'three';
import { OrbitControls } from 'three/addons/controls/OrbitControls.js';
import { PCDLoader } from 'three/addons/loaders/PCDLoader.js';
import { DRACOLoader } from 'three/addons/loaders/DRACOLoader.js';
import { CITY_URL, PCL_URL, MOVIE_COUNT, frameUrl } from './config.js';

const BG = 0x101418;
const FLAT_COLOR = 0x66ccff;
const MOVIE_FPS = 10;

export function createViewer(canvas) {
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

  const pcdLoader = new PCDLoader();
  const dracoLoader = new DRACOLoader();
  dracoLoader.setDecoderPath('./vendor/draco/');
  dracoLoader.setDecoderConfig({ type: 'wasm' });
  dracoLoader.preload(); // warm the WASM worker at startup so the first movie
                         // frame doesn't pay cold-compile cost mid-playback

  let points = null;       // the live THREE.Points object
  let baseColors = null;   // Float32Array height-mapped colors for the current geometry
  let sceneRadius = 0.5;   // normalized robust radius

  // Movie state
  let movie = null;        // { frames: [{geometry, colors}], timer, index }
  let loadToken = 0;       // increments on each loadScene to cancel stale async loads

  const state = {
    ready: false,
    scene: null,
    pointCount: 0,
    frameIndex: 0,
    frameCount: 0,
    playing: false,
    loading: false,
    loadProgress: { loaded: 0, total: 0 },
    error: null,
    settings: { pointSize: 0.004, colorMode: 'height' },
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
    const radius = sceneRadius;
    const eye = center.clone().add(new THREE.Vector3(-1.4, 1.17, 0).multiplyScalar(radius));
    const look = center.clone().add(new THREE.Vector3(0.8, -0.1, 0).multiplyScalar(radius));
    camera.position.copy(eye);
    controls.target.copy(look);
    camera.near = radius / 100;
    camera.far = radius * 100;
    camera.updateProjectionMatrix();
    controls.update();
  }

  function computeHeightColors(geometry) {
    const pos = geometry.getAttribute('position');
    const ys = Float32Array.from({ length: pos.count }, (_, i) => pos.getY(i)).sort();
    const minY = ys[Math.floor(pos.count * 0.02)];
    const span = (ys[Math.floor(pos.count * 0.98)] - minY) || 1;
    const colors = new Float32Array(pos.count * 3);
    const c = new THREE.Color();
    for (let i = 0; i < pos.count; i++) {
      const t = Math.min(1, Math.max(0, (pos.getY(i) - minY) / span));
      c.setHSL(0.7 - 0.7 * t, 0.9, 0.5);
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
      points.geometry.setAttribute('color', new THREE.BufferAttribute(baseColors, 3));
      points.material.vertexColors = true;
      points.material.color.set(0xffffff);
    } else {
      points.geometry.deleteAttribute('color');
      points.material.vertexColors = false;
      points.material.color.set(FLAT_COLOR);
    }
    points.material.needsUpdate = true;
  }

  // Compute a normalization transform (rotation handled by caller) for a geometry:
  // center + scale so the robust horizontal radius maps to 0.5 units. Returns the
  // {center, scale} so a movie can reuse one transform across all frames.
  function computeTransform(geom) {
    geom.computeBoundingBox();
    const center = geom.boundingBox.getCenter(new THREE.Vector3());
    const pos = geom.getAttribute('position');
    const radii = Float32Array.from(
      { length: pos.count }, (_, i) => Math.hypot(pos.getX(i) - center.x, pos.getZ(i) - center.z)).sort();
    const r = radii[Math.floor(pos.count * 0.9)] || 1;
    return { center, scale: 0.5 / r };
  }

  function applyTransform(geom, t) {
    geom.translate(-t.center.x, -t.center.y, -t.center.z);
    geom.scale(t.scale, t.scale, t.scale);
  }

  // Build / swap the live Points object from a normalized geometry + its colors.
  function installGeometry(geometry, colors) {
    if (!points) {
      points = new THREE.Points(
        geometry,
        new THREE.PointsMaterial({ size: state.settings.pointSize, color: FLAT_COLOR, sizeAttenuation: true }),
      );
      scene.add(points);
    } else {
      points.geometry = geometry;
    }
    baseColors = colors;
    applyColorMode(state.settings.colorMode);
    state.pointCount = geometry.getAttribute('position').count;
  }

  function stopMovie() {
    if (movie && movie.timer) clearInterval(movie.timer);
    if (movie) {
      for (const f of movie.frames) f.geometry.dispose();
    }
    movie = null;
    state.playing = false;
    state.frameCount = 0;
    state.frameIndex = 0;
  }

  function teardownScene() {
    const wasMovie = movie !== null;
    stopMovie();
    if (points) {
      // Movie frames are disposed by stopMovie(); for static scenes dispose here.
      if (!wasMovie) points.geometry.dispose();
      scene.remove(points);
      points.material.dispose();
      points = null;
    }
    baseColors = null;
  }

  // Load a single static cloud (city / table). z-up (KITTI) and arbitrary clouds
  // both get rotated to y-up then normalized.
  async function loadStatic(url, { rotate = true } = {}) {
    const token = loadToken;
    const geom = await new Promise((res, rej) => pcdLoader.load(url, (p) => res(p.geometry), undefined, rej));
    if (token !== loadToken) { geom.dispose(); return; } // superseded
    if (rotate) geom.rotateX(-Math.PI / 2);
    const t = computeTransform(geom);
    applyTransform(geom, t);
    sceneRadius = 0.5;
    installGeometry(geom, computeHeightColors(geom));
    resize();
    frameCamera();
    state.ready = true;
  }

  function dracoLoad(url) {
    return new Promise((res, rej) => dracoLoader.load(url, (g) => res(g), undefined, rej));
  }

  async function loadMovie(count) {
    const token = loadToken;
    state.loading = true;
    state.loadProgress = { loaded: 0, total: count };
    const frames = [];
    let transform = null;
    for (let i = 0; i < count; i++) {
      let geom;
      try {
        geom = await dracoLoad(frameUrl(i));
      } catch (e) {
        if (token !== loadToken) return;
        state.loading = false;
        state.error = `Failed to load movie frame ${i}: ${e.message || e}`;
        for (const f of frames) f.geometry.dispose();
        return;
      }
      if (token !== loadToken) { geom.dispose(); return; }
      geom.rotateX(-Math.PI / 2);
      if (!transform) transform = computeTransform(geom);
      applyTransform(geom, transform);
      frames.push({ geometry: geom, colors: computeHeightColors(geom) });
      state.loadProgress = { loaded: i + 1, total: count };
    }
    if (token !== loadToken) { for (const f of frames) f.geometry.dispose(); return; }
    sceneRadius = 0.5;
    movie = { frames, timer: null, index: 0 };
    state.frameCount = frames.length;
    state.frameIndex = 0;
    installGeometry(frames[0].geometry, frames[0].colors);
    resize();
    frameCamera();
    state.loading = false;
    state.ready = true;
    play();
  }

  function showFrame(i) {
    if (!movie) return;
    movie.index = i;
    state.frameIndex = i;
    installGeometry(movie.frames[i].geometry, movie.frames[i].colors);
  }

  function play() {
    if (!movie || movie.timer) return;
    state.playing = true;
    movie.timer = setInterval(() => {
      showFrame((movie.index + 1) % movie.frames.length);
    }, 1000 / MOVIE_FPS);
  }

  function pause() {
    if (!movie || !movie.timer) return;
    clearInterval(movie.timer);
    movie.timer = null;
    state.playing = false;
  }

  async function loadScene(id) {
    loadToken++;
    state.ready = false;
    state.error = null;
    state.scene = id;
    teardownScene();
    if (id === 'city') await loadStatic(CITY_URL, { rotate: true });
    else if (id === 'table') await loadStatic(PCL_URL, { rotate: true });
    else if (id === 'movie') await loadMovie(MOVIE_COUNT);
  }

  let raf = 0;
  function tick() {
    raf = requestAnimationFrame(tick);
    controls.update();
    renderer.render(scene, camera);
    state.framesRendered++;
  }
  resize();
  tick();
  loadScene('city');

  const handle = {
    loadScene,
    play,
    pause,
    setPointSize(n) {
      state.settings.pointSize = n;
      if (points) { points.material.size = n; points.material.needsUpdate = true; }
    },
    setColorMode(mode) { applyColorMode(mode); },
    resetCamera() { if (points) frameCamera(); },
    getStats() {
      const e = camera.position, t = controls.target;
      const vec = (v) => ({ x: v.x, y: v.y, z: v.z });
      return {
        pointCount: state.pointCount,
        cameraDistance: e.distanceTo(t),
        eye: vec(e),
        target: vec(t),
        scene: state.scene,
        frameIndex: state.frameIndex,
        frameCount: state.frameCount,
        playing: state.playing,
        loading: state.loading,
        loadProgress: state.loadProgress,
        error: state.error,
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
      teardownScene();
      dracoLoader.dispose();
      renderer.dispose();
    },
  };
  state.handle = handle; // expose for e2e
  return handle;
}
