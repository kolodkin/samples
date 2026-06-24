// three.js viewer: owns all 3D state behind a small imperative handle.
// Knows nothing about Preact. Exposes window.__PCL for deterministic e2e.
import * as THREE from 'three';
import { OrbitControls } from 'three/addons/controls/OrbitControls.js';
import { PCDLoader } from 'three/addons/loaders/PCDLoader.js';
import { DRACOLoader } from 'three/addons/loaders/DRACOLoader.js';
import { CITY_URL, PCL_URL, MOVIE_COUNT, frameUrl } from './config.js';

const BG = 0x101418;
const FLAT_COLOR = 0x66ccff;
const MOVIE_FPS = 15;
const MOVIE_DECODE_CONCURRENCY = 4; // frames decoded in flight via the worker queue

// Per-scene normalization + framing. KITTI clouds (city, movie) are z-up sensor
// frames spread across a wide ground plane: rotate to y-up, scale by a robust
// *horizontal* radius so the dense scene fills the view, and frame from a low
// chase camera looking down the road. The PCL face scan is a compact object that
// is already y-up and faces +Z (nose protruding toward +Z, crown at +Y): skip the
// rotation, scale by a robust *3D* radius so the whole head fits, and frame it
// front-on. `radial` selects the scale metric; `camera` selects the framing.
const PROFILES = {
  kitti:  { upRotate: true,  radial: false, camera: 'chase' },
  object: { upRotate: false, radial: true,  camera: 'front' },
};
const SCENE_PROFILE = { city: PROFILES.kitti, face: PROFILES.object, movie: PROFILES.kitti };

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
  let colorBuffers = {};   // mode name -> Float32Array of per-point ramp colors
  let sceneRadius = 0.5;   // normalized robust radius of the scan
  let activeProfile = PROFILES.kitti; // normalization/framing for the live scene

  // Movie state
  let movie = null;        // { frames: [{geometry, buffers}], timer, index }
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
    settings: { pointSize: 0.004, colorMode: 'distance', pointShape: 'ball' },
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

  function frameCamera(profile = activeProfile) {
    const box = new THREE.Box3().setFromObject(points);
    const center = box.getCenter(new THREE.Vector3());
    const radius = sceneRadius; // ignore far stray returns so the scene fills the view
    let eye, look;
    if (profile.camera === 'front') {
      // The face is centered, y-up, and faces +Z (nose toward +Z, crown at +Y).
      // View it front-on from +Z, lifted slightly and pushed a touch to the right
      // for a flattering near-frontal portrait; aim at the box center (just above
      // the nose, around the eyes). Pulled back so the whole head fits the 45° FOV.
      eye = center.clone().add(new THREE.Vector3(0.5, 0.35, 2.3).multiplyScalar(radius));
      look = center.clone();
    } else {
      // Axis convention after normalization: +X is forward (down the road),
      // +Y is up, +Z is right, and the sensor sits at the cloud center. Pull the
      // eye up and behind the sensor and aim it forward at the road ahead, an
      // elevated chase view that pulls back so the whole scene reads at once (the
      // sensor blind-spot ring sits in the foreground). The eye sits above the
      // target, so the view still looks down even with a level aim point.
      // Offsets are round multiples of 0.1 so that, at sceneRadius 0.5, the eye
      // and target land on clean values in the HUD readout.
      eye = center.clone().add(new THREE.Vector3(-1.4, 1.2, 0).multiplyScalar(radius));
      look = center.clone().add(new THREE.Vector3(0.8, 0, 0).multiplyScalar(radius));
    }
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
  // (only if the source carried an `intensity` field — Draco movie frames are
  // positions-only, so they get height + distance, and intensity falls back).
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

  // Normalize a static cloud into the viewer's working frame: rotate z-up
  // (KITTI vehicle frame) to three.js y-up, center on the origin, and scale by a
  // robust horizontal radius (90th percentile of distance from the sensor) so
  // the dense scene fills the frame instead of being shrunk by a few stray
  // returns. Keeps camera framing and the point-size slider meaningful across
  // datasets.
  function normalizeGeometry(geom, profile) {
    if (profile.upRotate) geom.rotateX(-Math.PI / 2); // z-up sensor frame -> y-up
    geom.computeBoundingBox();
    const center = geom.boundingBox.getCenter(new THREE.Vector3());
    geom.translate(-center.x, -center.y, -center.z);
    const pos = geom.getAttribute('position');
    // KITTI scans fill a wide ground plane, so scale by a robust *horizontal*
    // (x,z) radius; a compact object scales by a robust *3D* radius so its full
    // vertical extent fits the frame.
    const radii = Float32Array.from(
      { length: pos.count }, (_, i) => profile.radial
        ? Math.hypot(pos.getX(i), pos.getY(i), pos.getZ(i))
        : Math.hypot(pos.getX(i), pos.getZ(i))).sort();
    const r = radii[Math.floor(pos.count * 0.9)] || 1;
    geom.scale(0.5 / r, 0.5 / r, 0.5 / r);
    sceneRadius = 0.5;
  }

  // Movie frames must share ONE transform (computed from frame 0) so the world
  // stays put and only the moving objects/ego flow between frames — per-frame
  // normalization would make everything pulse. `shared` is computed on the first
  // frame and reused for the rest.
  function normalizeMovieFrame(geom, shared) {
    geom.rotateX(-Math.PI / 2);
    if (!shared.ready) {
      geom.computeBoundingBox();
      shared.center = geom.boundingBox.getCenter(new THREE.Vector3());
      geom.translate(-shared.center.x, -shared.center.y, -shared.center.z);
      const pos = geom.getAttribute('position');
      const radii = Float32Array.from(
        { length: pos.count }, (_, i) => Math.hypot(pos.getX(i), pos.getZ(i))).sort();
      const r = radii[Math.floor(pos.count * 0.9)] || 1;
      shared.scale = 0.5 / r;
      geom.scale(shared.scale, shared.scale, shared.scale);
      shared.ready = true;
    } else {
      geom.translate(-shared.center.x, -shared.center.y, -shared.center.z);
      geom.scale(shared.scale, shared.scale, shared.scale);
    }
    sceneRadius = 0.5;
  }

  function makeMaterial() {
    const m = new THREE.PointsMaterial({
      size: state.settings.pointSize,
      color: FLAT_COLOR,
      sizeAttenuation: true,
    });
    installPointShapeShading(m); // ball (default) or square sprite
    m.userData.ballUniform.value = state.settings.pointShape === 'ball' ? 1 : 0;
    return m;
  }

  // Install (or swap to) a normalized geometry + its precomputed ramp buffers.
  // The material persists across movie frame swaps so point size/shape stick.
  function installGeometry(geometry, buffers) {
    if (!points) {
      points = new THREE.Points(geometry, makeMaterial());
      scene.add(points);
    } else {
      points.geometry = geometry;
    }
    colorBuffers = buffers;
    applyColorMode(state.settings.colorMode);
    state.pointCount = geometry.getAttribute('position').count;
  }

  function stopMovie() {
    if (movie) {
      if (movie.timer) clearInterval(movie.timer);
      for (const f of movie.frames) if (f) f.geometry.dispose();
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
      // Movie frame geometries are disposed by stopMovie(); for static scenes
      // the current geometry is owned here.
      if (!wasMovie) points.geometry.dispose();
      scene.remove(points);
      points.material.dispose();
      points = null;
    }
    colorBuffers = {};
  }

  async function loadStatic(url, profile) {
    const token = loadToken;
    const loaded = await new Promise((res, rej) => pcdLoader.load(url, (p) => res(p), undefined, rej));
    if (token !== loadToken) { loaded.geometry.dispose(); loaded.material.dispose(); return; }
    loaded.material.dispose(); // we build our own (shape-shaded) material
    const geom = loaded.geometry;
    normalizeGeometry(geom, profile);
    installGeometry(geom, computeColorBuffers(geom));
    resize();
    frameCamera(profile);
    state.ready = true;
  }

  function dracoLoad(url) {
    return new Promise((res, rej) => dracoLoader.load(url, (g) => res(g), undefined, rej));
  }

  // Decode one frame off the queue: Draco decode (in DRACOLoader's WASM worker) →
  // normalize against the shared transform → precompute color buffers. Returns the
  // slot, or null if the load was cancelled (stale token).
  async function decodeFrame(i, shared, token) {
    const geom = await dracoLoad(frameUrl(i));
    if (token !== loadToken) { geom.dispose(); return null; }
    normalizeMovieFrame(geom, shared);
    return { geometry: geom, buffers: computeColorBuffers(geom) };
  }

  // Stream the movie: decode frame 0 first (it defines the shared normalization
  // transform), start playback immediately, then fill the remaining slots through a
  // bounded-concurrency worker queue while playback runs (hold-on-stall covers any
  // frame the playhead reaches before it is decoded).
  async function loadMovie(count) {
    const token = loadToken;
    state.loading = true;
    state.loadProgress = { loaded: 0, total: count };
    const frames = new Array(count).fill(null);
    const shared = { ready: false };

    let first;
    try {
      first = await decodeFrame(0, shared, token);
    } catch (e) {
      if (token !== loadToken) return;
      state.loading = false;
      state.error = `Failed to load movie frame 0: ${e.message || e}`;
      return;
    }
    if (token !== loadToken) { if (first) first.geometry.dispose(); return; }
    frames[0] = first;
    movie = { frames, timer: null, index: 0, failed: new Set() };
    state.frameCount = count;
    state.frameIndex = 0;
    state.loadProgress = { loaded: 1, total: count };
    installGeometry(first.geometry, first.buffers);
    resize();
    frameCamera();
    state.ready = true;
    play();

    // Worker queue: N concurrent workers pull the next undecoded index off a shared
    // cursor until the movie is fully loaded (or the load is cancelled).
    let next = 1;
    let decoded = 1;
    const worker = async () => {
      while (next < count) {
        const i = next++;
        let slot;
        try {
          slot = await decodeFrame(i, shared, token);
        } catch (e) {
          if (token !== loadToken) return;
          // One frame failing shouldn't kill the whole stream: mark it so
          // playback skips it, surface the error, and keep decoding the rest.
          movie.failed.add(i);
          state.error = `Failed to load movie frame ${i}: ${e.message || e}`;
          continue;
        }
        if (token !== loadToken) { if (slot) slot.geometry.dispose(); return; }
        frames[i] = slot;
        decoded++;
        state.loadProgress = { loaded: decoded, total: count };
      }
    };
    const workers = [];
    for (let w = 0; w < Math.min(MOVIE_DECODE_CONCURRENCY, count - 1); w++) {
      workers.push(worker());
    }
    await Promise.all(workers);
    if (token === loadToken) state.loading = false;
  }

  function showFrame(i) {
    if (!movie || !movie.frames[i]) return;
    movie.index = i;
    state.frameIndex = i;
    installGeometry(movie.frames[i].geometry, movie.frames[i].buffers);
  }

  function play() {
    if (!movie || movie.timer) return;
    state.playing = true;
    movie.timer = setInterval(() => {
      const count = movie.frames.length;
      let next = (movie.index + 1) % count;
      // Skip frames that permanently failed to decode (rare network errors)...
      let guard = 0;
      while (!movie.frames[next] && movie.failed.has(next) && guard < count) {
        next = (next + 1) % count;
        guard++;
      }
      // ...but hold-on-stall for ones the queue simply hasn't reached yet: stay on
      // the current frame rather than skipping or erroring.
      if (movie.frames[next]) showFrame(next);
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
    // Clear any in-flight movie load's progress: switching scenes bumps
    // loadToken (cancelling the movie's decode workers), but the superseded load
    // bails out via its stale-token guards without ever resetting these, and the
    // static loaders never touch them — so a half-streamed movie would otherwise
    // leave the HUD's "Loading X / Y…" spinner stuck on forever over the new scene.
    state.loading = false;
    state.loadProgress = { loaded: 0, total: 0 };
    state.scene = id;
    activeProfile = SCENE_PROFILE[id] || PROFILES.kitti;
    teardownScene();
    if (id === 'city') await loadStatic(CITY_URL, activeProfile);
    else if (id === 'face') await loadStatic(PCL_URL, activeProfile);
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
        ready: state.ready,
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
