// three.js viewer: owns all 3D state behind a small imperative handle.
// Knows nothing about Preact. Exposes window.__PCL for deterministic e2e.
import * as THREE from 'three';
import { OrbitControls } from 'three/addons/controls/OrbitControls.js';
import { PCDLoader } from 'three/addons/loaders/PCDLoader.js';
import { PLYLoader } from 'three/addons/loaders/PLYLoader.js';
import { DRACOLoader } from 'three/addons/loaders/DRACOLoader.js';
import {
  LUCY_URL, MOVIE_COUNT, frameUrl,
  SEG_MOVIE_COUNT, SEG_BOXES_URL, segFrameUrl,
} from './config.js';
import { COLOR_MODES } from './colorModes.js';

const BG = 0x101418;
const FLAT_COLOR = 0x66ccff;
const MOVIE_FPS = 15;
const MOVIE_DECODE_CONCURRENCY = 4; // frames decoded in flight via the worker queue

// Modes the cloud offers are derived from the ramp/class buffers it actually
// carries: "flat" is always available, the rest only when their buffer exists
// (so "by class" appears just on the seg scene, "by intensity" only where the
// source had an intensity field). Order follows COLOR_MODES.
const offeredModes = (buffers) =>
  COLOR_MODES.filter((m) => m.id === 'flat' || buffers[m.id]).map((m) => m.id);

// Scenes listed here reset the color mode to the given value each time they load:
// the seg scene is *about* its per-point classes, so it always opens "by class".
// Scenes not listed keep whatever mode is active, falling back to flat (via
// applyColorMode) if the new cloud can't supply it (e.g. carrying seg's "by class"
// onto the movie, which has no class buffer).
const SCENE_DEFAULT_COLOR = { seg: 'class' };

// Per-scene normalization + framing. KITTI clouds (movie, seg) are z-up
// sensor frames spread over a wide ground plane: rotate to y-up, scale by a robust
// *horizontal* radius so the dense scene fills the view, and frame from a low
// chase camera looking down the road. The Lucy statue is a compact object already
// in a y-up frame: skip the rotation, scale by a robust bounding extent so the
// tall figure fits the view, and frame it front-on, upright. `kind` selects the
// behavior in normalizeGeometry/frameCamera.
const PROFILES = { kitti: { kind: 'kitti' }, object: { kind: 'object' } };
const SCENE_PROFILE = {
  lucy: PROFILES.object, movie: PROFILES.kitti, seg: PROFILES.kitti,
};

// SemanticKITTI 19-class learning palette, indexed by class id (0 = unlabeled).
const SEG_PALETTE = [
  0x202830, 0x6496F5, 0x64E6F5, 0x1E3C96, 0x501EB4, 0x0000FF, 0xFF1E1E,
  0xFF28C8, 0x961E5A, 0xFF00FF, 0xFF96FF, 0x4B004B, 0xAF004B, 0xFFC800,
  0xFF7832, 0x00AF00, 0x873C00, 0x96F050, 0xFFF096, 0xFF0000,
].map((hex) => new THREE.Color(hex));

// Render each point either as a lit sphere impostor ("ball", the default) or as
// the plain flat square sprite ("square", three.js's stock point look). Both go
// through PointsMaterial.onBeforeCompile so the size slider, size attenuation,
// and per-vertex color ramps keep working untouched; a `uBall` uniform (1/0)
// flips the fragment behavior at runtime with no recompile. For the ball: clip
// the quad to a circle, rebuild a hemisphere normal from gl_PointCoord, and
// shade it (ambient + diffuse + a tight specular highlight) so every point reads
// as a tiny 3D ball. The light is fixed in view space, so the highlights stay
// put as the cloud orbits — like a studio key light on the lens.
// Per-point range/class filtering rides the same custom shader. Each geometry
// carries an `aHide` attribute (1 = filtered out, 0 = shown) computed on the CPU
// in applyFilter; the vertex stage forwards it and the fragment stage discards
// hidden points. The attribute defaults to 0 when absent, so a geometry without
// it (or before applyFilter runs) shows every point — filtering fails open.
function installPointShapeShading(material) {
  material.userData.ballUniform = { value: 1 }; // 1 = ball, 0 = square
  material.onBeforeCompile = (shader) => {
    shader.uniforms.uBall = material.userData.ballUniform;
    shader.vertexShader = 'attribute float aHide;\nvarying float vHide;\n' +
      shader.vertexShader.replace(
        '#include <begin_vertex>',
        '#include <begin_vertex>\n  vHide = aHide;');
    shader.fragmentShader = 'uniform float uBall;\nvarying float vHide;\n' +
      shader.fragmentShader.replace(
      '#include <color_fragment>',
      `#include <color_fragment>
      if (vHide > 0.5) discard;
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

export function createViewer(canvas, { onColorState } = {}) {
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
  const plyLoader = new PLYLoader();
  const dracoLoader = new DRACOLoader();
  dracoLoader.setDecoderPath('./vendor/draco/');
  dracoLoader.setDecoderConfig({ type: 'wasm' });
  dracoLoader.preload(); // warm the WASM worker at startup so the first movie
                         // frame doesn't pay cold-compile cost mid-playback

  let points = null;       // the live THREE.Points object
  let colorBuffers = {};   // mode name -> Float32Array of per-point ramp colors
  let currentScalars = null; // { height, distance, intensity?, classId? } raw per-point fields
  let sceneRadius = 0.5;   // normalized robust radius of the scan
  let activeProfile = PROFILES.kitti; // normalization/framing for the live scene

  // Movie state
  let movie = null;        // { frames: [{geometry, buffers}], timer, index, onFrame, shared }
  let loadToken = 0;       // increments on each loadScene to cancel stale async loads

  // Seg scene state: per-instance 3D boxes drawn over the movie points.
  let boxGroup = null;     // THREE.Group of per-instance LineSegments
  let segBoxes = null;     // parsed boxes.json: { "000000": [ {cls,center,size}, ... ] }
  let showBoxes = true;

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
    boxCount: 0,
    colorModes: ['flat'], // modes the live cloud supports (drives the UI dropdown)
    visibleCount: 0,      // points passing the active filters (== pointCount when none)
    scalarRanges: {},     // field -> { min, max } over the live cloud (UI hints)
    settings: {
      pointSize: 0.004, colorMode: 'distance', pointShape: 'ball', showBoxes: true,
      // Range filters per scalar field; null bound = unbounded (the default, so
      // nothing is filtered). hiddenClasses lists class ids toggled off via the
      // seg legend. Both persist across frames/scenes and are applied in applyFilter.
      filters: {
        height: { min: null, max: null },
        distance: { min: null, max: null },
        intensity: { min: null, max: null },
      },
      hiddenClasses: [],
    },
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
    if (profile.kind === 'object') {
      // The statue is centered and y-up. View it front-on and upright from a
      // slightly elevated three-quarter angle, pulled well back so the full
      // height (taller than the sceneRadius the cloud is scaled to) fits the 45°
      // FOV. Aim at the box center so the figure sits squarely in frame.
      eye = center.clone().add(new THREE.Vector3(1.2, 0.6, 2.8).multiplyScalar(radius));
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

  // Histogram-equalize a scalar field to a roughly uniform [0,1] distribution via
  // its empirical CDF: each value maps to the fraction of points at or below it.
  // LiDAR intensity is heavily clumped (most returns dark, a sparse bright tail),
  // so a plain linear ramp wastes most of the color range on a narrow band. Mapping
  // through the CDF spreads the histogram evenly across the ramp, pulling faint
  // structure (lane paint, signs, foliage) out of the murk. Ties share one
  // equalized value (the CDF at the top of the run) so the mapping stays monotonic.
  // See https://en.wikipedia.org/wiki/Histogram_equalization. Returns a fresh array;
  // the caller keeps the raw field untouched for filtering and the HUD range hints.
  function equalizeHistogram(values) {
    const n = values.length;
    const eq = new Float32Array(n);
    if (!n) return eq;
    const order = Array.from({ length: n }, (_, i) => i).sort((a, b) => values[a] - values[b]);
    let i = 0;
    while (i < n) {
      let j = i;
      while (j + 1 < n && values[order[j + 1]] === values[order[i]]) j++;
      const cdf = (j + 1) / n; // CDF at the top of this run of equal values
      for (let k = i; k <= j; k++) eq[order[k]] = cdf;
      i = j + 1;
    }
    return eq;
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

  // Intensity gets its own "hot" (thermal) colormap instead of the shared blue->red
  // ramp: only the lowest returns read as cool purple, and the bulk of the
  // histogram-equalized range climbs through magenta -> red -> orange -> yellow to
  // near-white, so the cloud is mostly bright and reflective surfaces pop. Stops are
  // placed so purple occupies just the bottom ~15% of the range and everything above
  // stays warm and bright. Same robust 2nd..98th percentile clamp as rampColors so a
  // few outliers don't compress the map; linear RGB interpolation between stops.
  const HOT_STOPS = [
    { t: 0.00, c: [0.28, 0.05, 0.42] }, // deep purple  (low intensity only)
    { t: 0.15, c: [0.70, 0.09, 0.42] }, // magenta
    { t: 0.35, c: [0.95, 0.24, 0.16] }, // red
    { t: 0.60, c: [1.00, 0.52, 0.06] }, // orange
    { t: 0.82, c: [1.00, 0.82, 0.26] }, // yellow
    { t: 1.00, c: [1.00, 0.98, 0.88] }, // near-white   (high intensity)
  ];
  function hotRampColors(values) {
    const n = values.length;
    const sorted = Float32Array.from(values).sort();
    const min = sorted[Math.floor(n * 0.02)];
    const span = (sorted[Math.floor(n * 0.98)] - min) || 1;
    const colors = new Float32Array(n * 3);
    for (let i = 0; i < n; i++) {
      const t = Math.min(1, Math.max(0, (values[i] - min) / span));
      let s = 0;
      while (s < HOT_STOPS.length - 2 && t > HOT_STOPS[s + 1].t) s++;
      const a = HOT_STOPS[s], b = HOT_STOPS[s + 1];
      const u = (t - a.t) / (b.t - a.t || 1);
      colors[i * 3]     = a.c[0] + (b.c[0] - a.c[0]) * u;
      colors[i * 3 + 1] = a.c[1] + (b.c[1] - a.c[1]) * u;
      colors[i * 3 + 2] = a.c[2] + (b.c[2] - a.c[2]) * u;
    }
    return colors;
  }

  // Precompute, for every scalar mode the cloud can supply, both a ramp color
  // buffer (for "color by") and the raw per-point values (for range filtering):
  // height (y), radial distance from the sensor (origin), and intensity. Movie/seg
  // Draco frames pack scalars into the color attribute, so an explicit per-scene
  // channel map (`opts.classChannel`, `opts.intensityChannel`) says which channel
  // carries what; static clouds pass no map and fall back to native attributes.
  // Returns { colors, scalars }: colors keyed by mode id (drives offeredModes/the
  // color dropdown), scalars the raw fields the filter reads.
  function computeColorBuffers(geometry, opts = {}) {
    const pos = geometry.getAttribute('position');
    const n = pos.count;
    const height = new Float32Array(n);
    const distance = new Float32Array(n);
    for (let i = 0; i < n; i++) {
      height[i] = pos.getY(i);
      distance[i] = Math.hypot(pos.getX(i), pos.getY(i), pos.getZ(i));
    }
    const colors = { height: rampColors(height), distance: rampColors(distance) };
    const scalars = { height, distance };
    const color = geometry.getAttribute('color');

    // Intensity: a movie channel if the scene maps one, else a native PCD
    // `intensity` field. Raw channel bytes are fine — rampColors clamps relatively.
    let intensity;
    if (opts.intensityChannel != null && color) {
      const ch = opts.intensityChannel;
      intensity = Float32Array.from({ length: n }, (_, i) => color.getComponent(i, ch));
    } else {
      const attr = geometry.getAttribute('intensity');
      if (attr) intensity = Float32Array.from({ length: n }, (_, i) => attr.getX(i));
    }
    // Color "by intensity" off a histogram-equalized copy (eq_i) precomputed here
    // at load, never by mutating the raw field: the equalized values drive the hot
    // colormap (so the clumped LiDAR histogram spreads evenly — mostly bright, only
    // the low tail purple), while raw intensity stays in scalars for range filtering
    // and the HUD's 0–255 hints. eq_i is already a uniform [0,1] CDF, so the map's
    // percentile clamp is a near-identity.
    if (intensity) {
      const eqIntensity = equalizeHistogram(intensity);
      colors.intensity = hotRampColors(eqIntensity);
      scalars.intensity = intensity;
    }

    // Class: the seg scene packs the per-point id (raw integer) in a color channel;
    // map each id through the fixed palette. DRACOLoader hands the color attribute
    // back as raw, un-normalized floats, so the channel value already IS the id.
    if (opts.classChannel != null && color) {
      const ch = opts.classChannel;
      const out = new Float32Array(n * 3);
      const classId = new Float32Array(n);
      for (let i = 0; i < n; i++) {
        const id = Math.round(color.getComponent(i, ch));
        classId[i] = id;
        const c = SEG_PALETTE[id] || SEG_PALETTE[0];
        out[i * 3] = c.r; out[i * 3 + 1] = c.g; out[i * 3 + 2] = c.b;
      }
      colors.class = out;
      scalars.classId = classId;
    }
    return { colors, scalars };
  }

  // Push the applied mode + available modes to the UI, but only when either
  // actually changes. `state.colorModes` is a stable array per scene (set once at
  // load), so the per-frame applyColorMode during movie playback is a no-op here.
  let lastMode, lastModes;
  function notifyColorState() {
    if (state.settings.colorMode === lastMode && state.colorModes === lastModes) return;
    lastMode = state.settings.colorMode;
    lastModes = state.colorModes;
    if (onColorState) onColorState({ mode: lastMode, modes: lastModes });
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
    notifyColorState();
  }

  // Normalize a static cloud into the viewer's working frame: center on the
  // origin and scale to a normalized radius, so camera framing and the point-size
  // slider stay meaningful across datasets. The KITTI profile also rotates z-up
  // (vehicle frame) to three.js y-up and scales by a robust *horizontal* radius
  // (90th percentile of distance from the sensor) so the dense ground scene fills
  // the frame. The object profile (Lucy) is already y-up, so it skips the rotation
  // and instead scales by a robust *bounding extent* (98th-percentile L∞ radius)
  // so the tall figure fits the cube rather than being dominated by its height.
  function normalizeGeometry(geom, profile) {
    if (profile.kind !== 'object') geom.rotateX(-Math.PI / 2); // z-up -> y-up
    geom.computeBoundingBox();
    const center = geom.boundingBox.getCenter(new THREE.Vector3());
    geom.translate(-center.x, -center.y, -center.z);
    const pos = geom.getAttribute('position');
    const metric = profile.kind === 'object'
      ? (i) => Math.max(Math.abs(pos.getX(i)), Math.abs(pos.getY(i)), Math.abs(pos.getZ(i)))
      : (i) => Math.hypot(pos.getX(i), pos.getZ(i));
    const pct = profile.kind === 'object' ? 0.98 : 0.9;
    const radii = Float32Array.from({ length: pos.count }, (_, i) => metric(i)).sort();
    const r = radii[Math.floor(pos.count * pct)] || 1;
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

  // True when a scalar value falls outside an inclusive [min, max] range; a null
  // bound is treated as unbounded on that side (so the default null/null passes
  // everything).
  function outOfRange(v, range) {
    return (range.min != null && v < range.min) || (range.max != null && v > range.max);
  }

  // Record the min/max of each filterable scalar over the live cloud so the UI can
  // hint the achievable range (and so callers know what to type). Cheap O(n) pass,
  // run once per geometry install rather than per filter change.
  function computeScalarRanges() {
    const ranges = {};
    for (const key of ['height', 'distance', 'intensity']) {
      const a = currentScalars && currentScalars[key];
      if (!a || !a.length) continue;
      let mn = Infinity, mx = -Infinity;
      for (let i = 0; i < a.length; i++) {
        if (a[i] < mn) mn = a[i];
        if (a[i] > mx) mx = a[i];
      }
      ranges[key] = { min: mn, max: mx };
    }
    state.scalarRanges = ranges;
  }

  // Recompute the per-point `aHide` attribute from the active filters and push it
  // onto the live geometry. A point is hidden if any ranged field falls outside
  // its range or its class is toggled off. Runs on every geometry install (so each
  // movie frame is filtered) and whenever a filter setting changes.
  function applyFilter() {
    if (!points || !currentScalars) return;
    const geom = points.geometry;
    const n = geom.getAttribute('position').count;
    const { height, distance, intensity, classId } = currentScalars;
    const f = state.settings.filters;
    const hidden = state.settings.hiddenClasses;
    const hiddenSet = hidden.length ? new Set(hidden) : null;
    const hide = new Float32Array(n);
    let visible = 0;
    for (let i = 0; i < n; i++) {
      const out =
        (height && outOfRange(height[i], f.height)) ||
        (distance && outOfRange(distance[i], f.distance)) ||
        (intensity && outOfRange(intensity[i], f.intensity)) ||
        (hiddenSet && classId && hiddenSet.has(classId[i]));
      hide[i] = out ? 1 : 0;
      if (!out) visible++;
    }
    geom.setAttribute('aHide', new THREE.BufferAttribute(hide, 1));
    state.visibleCount = visible;
  }

  // Install (or swap to) a normalized geometry + its precomputed ramp buffers.
  // The material persists across movie frame swaps so point size/shape stick.
  function installGeometry(geometry, colors, scalars) {
    if (!points) {
      points = new THREE.Points(geometry, makeMaterial());
      scene.add(points);
    } else {
      points.geometry = geometry;
    }
    colorBuffers = colors;
    currentScalars = scalars;
    applyColorMode(state.settings.colorMode);
    computeScalarRanges();
    applyFilter();
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
    disposeBoxGroup();
    segBoxes = null;
    state.boxCount = 0;
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
    currentScalars = null;
  }

  // Load a static cloud's geometry, picking the loader by file extension. PCD
  // comes back as a Points (keep its geometry, drop its material — we build our
  // own shape-shaded one); PLY comes back as a BufferGeometry that may be indexed
  // as a mesh, so strip the index to render one point per unique vertex rather
  // than one per face corner.
  function loadGeometry(url) {
    if (/\.ply(\?|$)/i.test(url)) {
      return new Promise((res, rej) => plyLoader.load(url, (g) => {
        g.setIndex(null);
        res(g);
      }, undefined, rej));
    }
    return new Promise((res, rej) => pcdLoader.load(url, (p) => {
      p.material.dispose();
      res(p.geometry);
    }, undefined, rej));
  }

  async function loadStatic(url, profile) {
    const token = loadToken;
    const geom = await loadGeometry(url);
    if (token !== loadToken) { geom.dispose(); return; }
    normalizeGeometry(geom, profile);
    const { colors, scalars } = computeColorBuffers(geom);
    state.colorModes = offeredModes(colors); // once per scene, before install
    installGeometry(geom, colors, scalars);
    resize();
    frameCamera(profile);
    state.ready = true;
  }

  function dracoLoad(url) {
    // Decode via decodeDracoFile (not .load(), which hardcodes SRGBColorSpace) with
    // a non-sRGB colorspace, so DRACOLoader SKIPS its convertSRGBToLinear pass over
    // the color attribute. The seg scene smuggles the per-point class id in that
    // attribute's red channel as a raw integer; the sRGB→linear curve would mangle
    // any id > 1, so it must pass through untouched.
    return fetch(url)
      .then((r) => r.arrayBuffer())
      .then((buf) => new Promise((res, rej) => {
        dracoLoader.decodeDracoFile(buf, res, null, null, THREE.LinearSRGBColorSpace).catch(rej);
      }));
  }

  // Decode one frame off the queue: Draco decode (in DRACOLoader's WASM worker) →
  // normalize against the shared transform → precompute color buffers. Returns the
  // slot, or null if the load was cancelled (stale token).
  async function decodeFrame(i, shared, token, urlFn, colorChannels) {
    const geom = await dracoLoad(urlFn(i));
    if (token !== loadToken) { geom.dispose(); return null; }
    normalizeMovieFrame(geom, shared);
    const { colors, scalars } = computeColorBuffers(geom, colorChannels);
    return { geometry: geom, colors, scalars };
  }

  // Transform an axis-aligned source-frame (z-up, metres) box through the same
  // rotate→translate→scale normalization applied to the movie points, then return
  // its 12-edge wireframe as LineSegments colored by class.
  function buildBoxLines(box, shared) {
    const [cx, cy, cz] = box.center;
    const [sx, sy, sz] = box.size;
    const hx = sx / 2, hy = sy / 2, hz = sz / 2;
    const corners = [];
    for (const dx of [-hx, hx]) for (const dy of [-hy, hy]) for (const dz of [-hz, hz]) {
      const v = new THREE.Vector3(cx + dx, cy + dy, cz + dz);
      v.applyAxisAngle(new THREE.Vector3(1, 0, 0), -Math.PI / 2); // z-up -> y-up
      v.sub(shared.center).multiplyScalar(shared.scale);
      corners.push(v);
    }
    // corners ordered by (dx,dy,dz) bits; edges connect corners differing in one bit.
    const E = [[0,1],[0,2],[0,4],[1,3],[1,5],[2,3],[2,6],[3,7],[4,5],[4,6],[5,7],[6,7]];
    const positions = new Float32Array(E.length * 2 * 3);
    let k = 0;
    for (const [a, b] of E) {
      positions[k++] = corners[a].x; positions[k++] = corners[a].y; positions[k++] = corners[a].z;
      positions[k++] = corners[b].x; positions[k++] = corners[b].y; positions[k++] = corners[b].z;
    }
    const g = new THREE.BufferGeometry();
    g.setAttribute('position', new THREE.BufferAttribute(positions, 3));
    const color = SEG_PALETTE[box.cls] || SEG_PALETTE[0];
    return new THREE.LineSegments(g, new THREE.LineBasicMaterial({ color }));
  }

  function disposeBoxGroup() {
    if (!boxGroup) return;
    scene.remove(boxGroup);
    boxGroup.traverse((o) => {
      if (o.geometry) o.geometry.dispose();
      if (o.material) o.material.dispose();
    });
    boxGroup = null;
  }

  // Rebuild the box group for the given frame from segBoxes + the shared transform.
  function updateBoxes(frameIndex, shared) {
    disposeBoxGroup();
    boxGroup = new THREE.Group();
    boxGroup.visible = showBoxes;
    const list = (segBoxes && segBoxes[String(frameIndex).padStart(6, '0')]) || [];
    for (const box of list) boxGroup.add(buildBoxLines(box, shared));
    state.boxCount = boxGroup.children.length;
    scene.add(boxGroup);
  }

  // Stream the movie: decode frame 0 first (it defines the shared normalization
  // transform), start playback immediately, then fill the remaining slots through a
  // bounded-concurrency worker queue while playback runs (hold-on-stall covers any
  // frame the playhead reaches before it is decoded).
  async function loadMovie(count, opts = {}) {
    const urlFn = opts.urlFn || frameUrl;
    const token = loadToken;
    state.loading = true;
    state.loadProgress = { loaded: 0, total: count };
    const frames = new Array(count).fill(null);
    const shared = { ready: false };

    let first;
    try {
      first = await decodeFrame(0, shared, token, urlFn, opts.colorChannels);
    } catch (e) {
      if (token !== loadToken) return;
      state.loading = false;
      state.error = `Failed to load movie frame 0: ${e.message || e}`;
      return;
    }
    if (token !== loadToken) { if (first) first.geometry.dispose(); return; }
    frames[0] = first;
    movie = { frames, timer: null, index: 0, failed: new Set(), onFrame: opts.onFrame, shared };
    state.frameCount = count;
    state.frameIndex = 0;
    state.loadProgress = { loaded: 1, total: count };
    state.colorModes = offeredModes(first.colors); // every frame shares these
    installGeometry(first.geometry, first.colors, first.scalars);
    if (movie.onFrame) movie.onFrame(0, shared);
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
          slot = await decodeFrame(i, shared, token, urlFn, opts.colorChannels);
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

  // The seg scene is a movie with per-point class colors plus per-frame 3D boxes:
  // fetch boxes.json up front, then stream frames from the seg dataset, rebuilding
  // the box group each time the displayed frame changes (via loadMovie's onFrame).
  async function loadSegMovie(count) {
    const token = loadToken;
    // "By class" is the seg scene's default, applied in loadScene via
    // SCENE_DEFAULT_COLOR (frame 0 installs with it).
    try {
      const resp = await fetch(SEG_BOXES_URL);
      segBoxes = resp.ok ? await resp.json() : null;
    } catch { segBoxes = null; }
    if (token !== loadToken) return;
    await loadMovie(count, {
      urlFn: segFrameUrl,
      onFrame: (i, shared) => updateBoxes(i, shared),
      colorChannels: { classChannel: 0, intensityChannel: 1 },
    });
  }

  function showFrame(i) {
    if (!movie || !movie.frames[i]) return;
    movie.index = i;
    state.frameIndex = i;
    installGeometry(movie.frames[i].geometry, movie.frames[i].colors, movie.frames[i].scalars);
    if (movie.onFrame) movie.onFrame(i, movie.shared);
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

  // Step `delta` frames from the current one. Stepping is a paused-scrub gesture,
  // so it always pauses first (otherwise the playback timer would overwrite the
  // step on its next tick). Frames the worker queue hasn't decoded yet are
  // skipped in the step direction so the playhead always lands on a real frame;
  // if the edge is reached with nothing decoded beyond, it holds.
  function step(delta) {
    if (!movie) return;
    pause();
    const count = movie.frames.length;
    const dir = delta < 0 ? -1 : 1;
    let remaining = Math.abs(delta) || 1;
    let target = movie.index;
    while (remaining > 0) {
      let next = target + dir;
      while (next >= 0 && next < count && !movie.frames[next]) next += dir;
      if (next < 0 || next >= count) break; // no decoded frame left this way
      target = next;
      remaining--;
    }
    showFrame(target);
  }

  // Jump to an arbitrary frame (the frame-select slider). Pauses like step, and
  // if the exact frame isn't decoded yet it snaps to the nearest decoded one so a
  // drag never lands on a blank frame.
  function seek(i) {
    if (!movie) return;
    pause();
    const count = movie.frames.length;
    let target = Math.max(0, Math.min(count - 1, Math.round(i)));
    if (!movie.frames[target]) {
      let found = -1;
      for (let d = 1; d < count && found < 0; d++) {
        if (target - d >= 0 && movie.frames[target - d]) found = target - d;
        else if (target + d < count && movie.frames[target + d]) found = target + d;
      }
      if (found < 0) return;
      target = found;
    }
    showFrame(target);
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
    // Scenes with a forced default (seg → "by class") reset the mode before the
    // first frame installs; others carry the current mode into the new scene.
    if (SCENE_DEFAULT_COLOR[id]) state.settings.colorMode = SCENE_DEFAULT_COLOR[id];
    activeProfile = SCENE_PROFILE[id] || PROFILES.kitti;
    teardownScene();
    if (id === 'lucy') await loadStatic(LUCY_URL, activeProfile);
    else if (id === 'movie') await loadMovie(MOVIE_COUNT, { colorChannels: { intensityChannel: 1 } });
    else if (id === 'seg') await loadSegMovie(SEG_MOVIE_COUNT);
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
  loadScene('movie');

  const handle = {
    loadScene,
    play,
    pause,
    step,
    seek,
    setPointSize(n) {
      state.settings.pointSize = n;
      if (points) { points.material.size = n; points.material.needsUpdate = true; }
    },
    setColorMode(mode) { applyColorMode(mode); },
    // Set one field's range filter. `range` carries min/max; pass null (or omit) a
    // bound to leave it unbounded. Reapplies immediately to the live cloud.
    setFilter(field, range = {}) {
      const f = state.settings.filters[field];
      if (!f) return;
      f.min = range.min == null ? null : range.min;
      f.max = range.max == null ? null : range.max;
      applyFilter();
    },
    // Toggle a class id in/out of the cloud (driven by the seg legend).
    setClassHidden(id, hidden) {
      const set = new Set(state.settings.hiddenClasses);
      if (hidden) set.add(id); else set.delete(id);
      state.settings.hiddenClasses = [...set].sort((a, b) => a - b);
      applyFilter();
    },
    // Clear every range filter and un-hide all classes.
    resetFilters() {
      for (const k of Object.keys(state.settings.filters)) {
        state.settings.filters[k] = { min: null, max: null };
      }
      state.settings.hiddenClasses = [];
      applyFilter();
    },
    setShowBoxes(on) {
      showBoxes = !!on;
      state.settings.showBoxes = showBoxes;
      if (boxGroup) boxGroup.visible = showBoxes;
    },
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
        visibleCount: state.visibleCount,     // points passing the active filters
        scalarRanges: state.scalarRanges,     // field -> {min,max} over the live cloud
        filters: state.settings.filters,      // active range filters
        hiddenClasses: state.settings.hiddenClasses, // class ids toggled off
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
        boxCount: state.boxCount,
        colorMode: state.settings.colorMode, // the mode actually applied (post-fallback)
        colorModes: state.colorModes,         // modes the live cloud supports
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
    // e2e helper: count saturated (chromatic) pixels — max−min channel spread.
    // The seg palette is vivid (car blue, road magenta, building yellow), so a
    // working "by class" render has many; the near-grey fallback color has none.
    colorfulPixelCount(threshold = 40) {
      const gl = renderer.getContext();
      const w = renderer.domElement.width;
      const h = renderer.domElement.height;
      const buf = new Uint8Array(w * h * 4);
      gl.readPixels(0, 0, w, h, gl.RGBA, gl.UNSIGNED_BYTE, buf);
      let n = 0;
      for (let i = 0; i < buf.length; i += 4) {
        const max = Math.max(buf[i], buf[i + 1], buf[i + 2]);
        const min = Math.min(buf[i], buf[i + 1], buf[i + 2]);
        if (max - min > threshold) n++;
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
