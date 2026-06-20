# PCL Viewer Scene Modes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a **Scene** drop-down to the PCL viewer that switches between KITTI city view (static), a PCL table scene (static, hot-linked), and a KITTI movie (multi-frame Draco-compressed LiDAR playback fetched from a Hugging Face dataset).

**Architecture:** Keep the existing boundary — `viewer.js` owns all three.js state behind an imperative handle; `app.js` is pure Preact UI. Generalize the viewer from one hard-coded model to a `loadScene(id)` dispatcher (`loadStatic` for city/table via `PCDLoader`, `loadMovie` for the movie via `DRACOLoader`). Movie frame data lives on HF (no git bloat) and is decoded in-browser with a vendored Draco WASM decoder.

**Tech Stack:** three.js 0.160 (`PCDLoader`, `DRACOLoader`), Preact + htm (no-build ESM), Python `http.server`, pytest + Playwright (Chromium), Python `numpy` + `DracoPy` + `huggingface_hub` for the one-shot generator.

---

## Background the engineer needs

- The project is a **no-build** ESM app under `pcl-viewer/web/` served by `pcl-viewer/serve.py`. Bare imports (`three`, `three/addons/`, …) resolve via the import map in `web/index.html` to files under `web/vendor/`, which are **gitignored** and produced by `pcl-viewer/vendor.sh` from unpkg.
- Tests are **Playwright e2e only** (no JS unit runner). `pcl-viewer/conftest.py` vendors libs then starts `serve.py` on a free port per test. `window.__PCL` exposes viewer state for deterministic assertions.
- **The viewer colors points by height (Y) or a flat color — intensity is never used.** Therefore movie frames carry **positions only** (XYZ); we drop the KITTI reflectance channel. This simplifies Draco encoding.
- KITTI raw is **CC BY-NC-SA 3.0** (redistribution permitted with attribution + non-commercial + share-alike). The HF dataset must carry the same license + attribution.
- All paths below are relative to the repo root `/home/user/samples` unless noted. Work happens under `pcl-viewer/`.

## File structure (created / modified)

- Create `pcl-viewer/web/config.js` — scene URLs + counts, with query-param/`window.__PCL_CONFIG` overrides.
- Modify `pcl-viewer/web/viewer.js` — scene loader (`loadScene`, `loadStatic`, `loadMovie`), Draco, playback, extended `getStats`.
- Modify `pcl-viewer/web/app.js` — Scene `<select>`, movie controls (play/pause, loading/error, frame HUD).
- Modify `pcl-viewer/vendor.sh` — also vendor the Draco decoder into `web/vendor/draco/`.
- Create `pcl-viewer/scripts/build_movie_dataset.py` — one-shot: download drive 0005 → decimate → Draco-encode → upload HF dataset.
- Create `pcl-viewer/tests/fixtures/build_fixtures.py` — one-shot: build tiny `.drc` movie fixtures from the committed KITTI frame.
- Create `pcl-viewer/tests/fixtures/movie/000000.drc`…`000003.drc` — committed offline movie fixtures (tiny).
- Modify `pcl-viewer/conftest.py` — copy `tests/fixtures/` into `web/fixtures/` (gitignored) so the test server serves them.
- Modify `pcl-viewer/tests/test_e2e.py` — scene tests.
- Modify `pcl-viewer/.gitignore` or root `.gitignore` — ignore `pcl-viewer/web/fixtures/` and `pcl-viewer/web/vendor/draco/` (latter already covered by `web/vendor/`).
- Modify `pcl-viewer/README.md` and `pcl-viewer/SPEC.md` — document scenes + attribution.
- Modify `pcl-viewer/pyproject.toml` — add `numpy`, `DracoPy`, `huggingface_hub` to a `gen` dependency group (generator/fixtures only).

---

## Task 1: Vendor the Draco decoder

**Files:**
- Modify: `pcl-viewer/vendor.sh`

- [ ] **Step 1: Add Draco decoder fetches to `vendor.sh`**

Open `pcl-viewer/vendor.sh`. After the existing `mkdir -p "$DEST/addons/controls" "$DEST/addons/loaders"` line, add a draco dir; and after the `PCDLoader.js` fetch line, add the `DRACOLoader` addon + decoder files. The final fetch block should read:

```bash
mkdir -p "$DEST/addons/controls" "$DEST/addons/loaders" "$DEST/draco"

fetch() { # url dest
  if [ -f "$2" ]; then echo "have   $2"; return; fi
  echo "fetch  $2"
  curl -fsSL "$1" -o "$2.tmp" && mv "$2.tmp" "$2"
}

fetch "$BASE/three@$THREE/build/three.module.js"                     "$DEST/three.module.js"
fetch "$BASE/three@$THREE/examples/jsm/controls/OrbitControls.js"    "$DEST/addons/controls/OrbitControls.js"
fetch "$BASE/three@$THREE/examples/jsm/loaders/PCDLoader.js"         "$DEST/addons/loaders/PCDLoader.js"
fetch "$BASE/three@$THREE/examples/jsm/loaders/DRACOLoader.js"       "$DEST/addons/loaders/DRACOLoader.js"
fetch "$BASE/three@$THREE/examples/jsm/libs/draco/draco_wasm_wrapper.js" "$DEST/draco/draco_wasm_wrapper.js"
fetch "$BASE/three@$THREE/examples/jsm/libs/draco/draco_decoder.wasm"    "$DEST/draco/draco_decoder.wasm"
fetch "$BASE/three@$THREE/examples/jsm/libs/draco/draco_decoder.js"      "$DEST/draco/draco_decoder.js"
fetch "$BASE/preact@$PREACT/dist/preact.module.js"                   "$DEST/preact.module.js"
fetch "$BASE/preact@$PREACT/hooks/dist/hooks.module.js"              "$DEST/preact-hooks.module.js"
fetch "$BASE/htm@$HTM/dist/htm.module.js"                            "$DEST/htm.module.js"
```

- [ ] **Step 2: Run vendor.sh and verify the decoder landed**

Run: `cd pcl-viewer && bash vendor.sh && ls -la web/vendor/draco/ web/vendor/addons/loaders/DRACOLoader.js`
Expected: `draco_wasm_wrapper.js`, `draco_decoder.wasm`, `draco_decoder.js`, and `DRACOLoader.js` all present and non-empty.

- [ ] **Step 3: Commit**

```bash
cd /home/user/samples
git add pcl-viewer/vendor.sh
git commit -m "build(pcl-viewer): vendor Draco decoder for movie playback"
```

---

## Task 2: Config module

**Files:**
- Create: `pcl-viewer/web/config.js`

- [ ] **Step 1: Create `web/config.js`**

```js
// Scene sources + movie parameters. Overridable (in priority order) by URL query
// params, then window.__PCL_CONFIG (set before module load), then these defaults.
// The overrides let e2e point scenes at local fixtures served by the test server.
const params = new URLSearchParams(location.search);
const cfg = (typeof window !== 'undefined' && window.__PCL_CONFIG) || {};

const pick = (key, def) => params.get(key) ?? cfg[key] ?? def;

export const CITY_URL = './models/kitti-velodyne-000000.pcd';

export const PCL_URL = pick(
  'pclUrl',
  'https://raw.githubusercontent.com/PointCloudLibrary/data/master/tutorials/table_scene_lms400.pcd',
);

export const MOVIE_BASE = pick(
  'movieBase',
  'https://huggingface.co/datasets/kolodkin/pcl-viewer-kitti-movie/resolve/main/',
);

export const MOVIE_COUNT = parseInt(pick('movieCount', '154'), 10);

// Frame URL helper: MOVIE_BASE + zero-padded index + .drc
export const frameUrl = (i) => `${MOVIE_BASE}${String(i).padStart(6, '0')}.drc`;
```

- [ ] **Step 2: Commit**

```bash
cd /home/user/samples
git add pcl-viewer/web/config.js
git commit -m "feat(pcl-viewer): add scene config module"
```

---

## Task 3: Refactor viewer.js to a scene loader (city + static URL)

This generalizes the viewer without changing default behavior: it still shows the KITTI city frame on load, but via `loadScene('city')`. Adds `loadStatic(url)` and extends `getStats`.

**Files:**
- Modify: `pcl-viewer/web/viewer.js`
- Test: `pcl-viewer/tests/test_e2e.py`

- [ ] **Step 1: Add the scene-default assertion to the existing test**

In `pcl-viewer/tests/test_e2e.py`, extend `test_point_cloud_loads_and_renders` to also assert the default scene id. Add after the existing `point_count` assertion:

```python
    assert page.evaluate("() => window.__PCL.scene") == "city"
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cd pcl-viewer && uv run --group dev pytest tests/test_e2e.py::test_point_cloud_loads_and_renders -v`
Expected: FAIL — `window.__PCL.scene` is `undefined` (not yet set).

- [ ] **Step 3: Rewrite `web/viewer.js` with the scene loader**

Replace the entire contents of `pcl-viewer/web/viewer.js` with:

```js
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

  let points = null;       // the live THREE.Points object
  let baseColors = null;   // Float32Array height-mapped colors for the current geometry
  let sceneRadius = 0.5;   // normalized robust radius

  // Movie state
  let movie = null;        // { frames: [{geometry, colors}], timer, transform }
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

  // Build the live Points object from a normalized geometry + its colors.
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
    stopMovie();
    if (points) {
      // dispose current geometry only if it isn't a movie frame (movie disposes its own)
      points.geometry.dispose();
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
  state.handle = handle;
  return handle;
}
```

- [ ] **Step 4: Run the full existing suite to confirm no regressions**

Run: `cd pcl-viewer && uv run --group dev pytest tests/test_e2e.py -v`
Expected: all existing tests PASS, including the new `scene == "city"` assertion. (The viewer now loads via `loadScene('city')`; behavior is identical.)

- [ ] **Step 5: Commit**

```bash
cd /home/user/samples
git add pcl-viewer/web/viewer.js pcl-viewer/tests/test_e2e.py
git commit -m "refactor(pcl-viewer): scene loader (city + static URL) behind loadScene"
```

---

## Task 4: Movie fixtures + conftest wiring

Build tiny committed `.drc` fixtures so the movie path is testable offline, and make the test server serve them.

**Files:**
- Modify: `pcl-viewer/pyproject.toml`
- Create: `pcl-viewer/tests/fixtures/build_fixtures.py`
- Create: `pcl-viewer/tests/fixtures/movie/000000.drc` … `000003.drc` (generated)
- Modify: `pcl-viewer/conftest.py`
- Modify: root `.gitignore`

- [ ] **Step 1: Add a `gen` dependency group**

In `pcl-viewer/pyproject.toml`, add (or extend) a dependency group used only for generation/fixtures:

```toml
[dependency-groups]
gen = ["numpy>=1.26", "DracoPy>=1.4.0", "huggingface_hub>=0.23"]
```

(If a `[dependency-groups]` table already exists, add the `gen` line to it. Keep the existing `dev` group untouched.)

- [ ] **Step 2: Write the fixture builder**

Create `pcl-viewer/tests/fixtures/build_fixtures.py`:

```python
"""Build tiny Draco movie fixtures from the committed KITTI frame.

Run once (commit the outputs): produces 4 heavily-decimated .drc frames with a
small synthetic translation between them, so the e2e movie test has a real, light,
offline sequence. Requires the `gen` group: `uv run --group gen python tests/fixtures/build_fixtures.py`.
"""
from __future__ import annotations

import struct
from pathlib import Path

import numpy as np
import DracoPy

HERE = Path(__file__).resolve().parent
SRC = HERE.parent.parent / "web" / "models" / "kitti-velodyne-000000.pcd"
OUT = HERE / "movie"


def read_binary_pcd_xyz(path: Path) -> np.ndarray:
    """Minimal binary-PCD reader for FIELDS x y z intensity (float32)."""
    data = path.read_bytes()
    header_end = data.index(b"DATA binary\n") + len(b"DATA binary\n")
    header = data[:header_end].decode("ascii", "replace")
    count = next(int(line.split()[1]) for line in header.splitlines() if line.startswith("POINTS"))
    body = np.frombuffer(data[header_end:header_end + count * 16], dtype=np.float32).reshape(-1, 4)
    return body[:, :3].copy()


def voxel_downsample(pts: np.ndarray, voxel: float) -> np.ndarray:
    keys = np.floor(pts / voxel).astype(np.int64)
    _, idx = np.unique(keys, axis=0, return_index=True)
    return pts[np.sort(idx)]


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    xyz = read_binary_pcd_xyz(SRC)
    base = voxel_downsample(xyz, 0.8)  # ~a few thousand points -> tiny .drc
    for i in range(4):
        frame = base + np.array([i * 0.5, 0.0, 0.0], dtype=np.float32)  # roll forward
        buf = DracoPy.encode(frame.astype(np.float32), quantization_bits=14)
        (OUT / f"{i:06d}.drc").write_bytes(buf)
        print(f"wrote {OUT / f'{i:06d}.drc'}  ({len(buf)} bytes, {len(frame)} pts)")


if __name__ == "__main__":
    main()
```

- [ ] **Step 3: Generate the fixtures**

Run: `cd pcl-viewer && uv run --group gen python tests/fixtures/build_fixtures.py`
Expected: prints 4 lines; `ls tests/fixtures/movie/` shows `000000.drc`…`000003.drc`, each a few KB.

- [ ] **Step 4: Make conftest serve fixtures + ignore generated dir**

In `pcl-viewer/conftest.py`, find the fixture that ensures vendoring / starts the server. Add a step that copies `tests/fixtures/` into `web/fixtures/` before the server starts. Add this helper and call it from the existing autouse/server fixture (adapt names to the file — the key is it runs once before `serve.py`):

```python
import shutil
from pathlib import Path

def _stage_fixtures() -> None:
    here = Path(__file__).resolve().parent
    src = here / "tests" / "fixtures"
    dst = here / "web" / "fixtures"
    if dst.exists():
        shutil.rmtree(dst)
    shutil.copytree(src, dst, ignore=shutil.ignore_patterns("*.py", "__pycache__"))
```

Call `_stage_fixtures()` in the same place `vendor.sh` is ensured (before the server is started). After staging, the browser can fetch `/fixtures/movie/000000.drc` and `/fixtures/table-via-model` paths from the test server.

- [ ] **Step 5: Ignore the generated served dir**

Append to root `.gitignore`:

```
# pcl-viewer test fixtures staged into the served web/ dir
pcl-viewer/web/fixtures/
```

- [ ] **Step 6: Commit**

```bash
cd /home/user/samples
git add pcl-viewer/pyproject.toml pcl-viewer/tests/fixtures/build_fixtures.py \
        pcl-viewer/tests/fixtures/movie/000000.drc pcl-viewer/tests/fixtures/movie/000001.drc \
        pcl-viewer/tests/fixtures/movie/000002.drc pcl-viewer/tests/fixtures/movie/000003.drc \
        pcl-viewer/conftest.py .gitignore
git commit -m "test(pcl-viewer): offline Draco movie fixtures + conftest staging"
```

---

## Task 5: app.js — Scene drop-down + movie controls

**Files:**
- Modify: `pcl-viewer/web/app.js`
- Modify: `pcl-viewer/web/styles.css` (small additions)

- [ ] **Step 1: Rewrite `web/app.js`**

Replace the entire contents of `pcl-viewer/web/app.js` with:

```js
// Preact UI (no JSX build — htm). Owns UI state and drives the viewer handle.
import { h, render } from 'preact';
import { useEffect, useRef, useState } from 'preact/hooks';
import htm from 'htm';
import { createViewer } from './viewer.js';

const html = htm.bind(h);

const SCENES = [
  { id: 'city', label: 'KITTI city view' },
  { id: 'table', label: 'PCL table scene' },
  { id: 'movie', label: 'KITTI movie' },
];

function App() {
  const canvasRef = useRef(null);
  const viewerRef = useRef(null);
  const [pointSize, setPointSize] = useState(0.004);
  const [colorMode, setColorMode] = useState('height');
  const [sceneId, setSceneId] = useState('city');
  const [menuOpen, setMenuOpen] = useState(false);
  const origin = { x: 0, y: 0, z: 0 };
  const [stats, setStats] = useState({
    pointCount: 0, fps: 0, cameraDistance: 0, eye: origin, target: origin,
    scene: 'city', frameIndex: 0, frameCount: 0, playing: false,
    loading: false, loadProgress: { loaded: 0, total: 0 }, error: null,
  });

  useEffect(() => {
    const viewer = createViewer(canvasRef.current);
    viewerRef.current = viewer;

    let frames = 0, last = performance.now(), fps = 0, raf = 0;
    const loop = () => {
      raf = requestAnimationFrame(loop);
      frames++;
      const now = performance.now();
      if (now - last >= 500) {
        fps = Math.round((frames * 1000) / (now - last));
        frames = 0; last = now;
        const s = viewer.getStats();
        setStats({ ...s, fps });
      }
    };
    loop();
    return () => { cancelAnimationFrame(raf); viewer.dispose(); };
  }, []);

  const onSize = (e) => {
    const v = parseFloat(e.target.value);
    setPointSize(v); viewerRef.current.setPointSize(v);
  };
  const onColor = (e) => {
    setColorMode(e.target.value); viewerRef.current.setColorMode(e.target.value);
  };
  const onScene = (e) => {
    const id = e.target.value;
    setSceneId(id); viewerRef.current.loadScene(id);
  };
  const onPlayPause = () => {
    if (stats.playing) viewerRef.current.pause(); else viewerRef.current.play();
  };
  const onReset = () => viewerRef.current.resetCamera();
  const fmt = (v) => `${v.x.toFixed(2)}  ${v.y.toFixed(2)}  ${v.z.toFixed(2)}`;

  const isMovie = sceneId === 'movie';
  const loadingText = stats.loading
    ? `Loading ${stats.loadProgress.loaded} / ${stats.loadProgress.total}…`
    : null;

  return html`
    <canvas id="scene" ref=${canvasRef}></canvas>

    <button class="menu-toggle" data-testid="menu-toggle"
            aria-label=${menuOpen ? 'Close controls' : 'Open controls'}
            onClick=${() => setMenuOpen((o) => !o)}>${menuOpen ? '✕' : '☰'}</button>

    ${menuOpen && html`
      <div class="backdrop" data-testid="backdrop" onClick=${() => setMenuOpen(false)}></div>
      <div class="panel controls" data-testid="controls">
        <h1>PCL Viewer</h1>
        <label>Scene</label>
        <select data-testid="scene" value=${sceneId} onChange=${onScene}>
          ${SCENES.map((s) => html`<option value=${s.id}>${s.label}</option>`)}
        </select>
        ${isMovie && html`
          <div class="row">
            <button data-testid="play-pause" onClick=${onPlayPause}>
              ${stats.playing ? 'Pause' : 'Play'}
            </button>
          </div>
        `}
        <label>Point size: ${pointSize.toFixed(3)}</label>
        <input type="range" min="0.002" max="0.05" step="0.001"
               value=${pointSize} data-testid="point-size" onInput=${onSize} />
        <label>Color mode</label>
        <select data-testid="color-mode" value=${colorMode} onChange=${onColor}>
          <option value="flat">Flat</option>
          <option value="height">By height</option>
        </select>
        <div class="row">
          <button data-testid="reset" onClick=${onReset}>Reset camera</button>
        </div>
      </div>
    `}

    <div class="panel hud" data-testid="stats">
      <div><b data-testid="point-count">${stats.pointCount.toLocaleString()}</b> pts
           · <b>${stats.fps}</b> fps · d <b>${stats.cameraDistance.toFixed(2)}</b></div>
      ${isMovie && stats.frameCount > 0 && html`
        <div>frame <b data-testid="frame-index">${stats.frameIndex + 1}</b> / ${stats.frameCount}</div>`}
      ${loadingText && html`<div data-testid="loading">${loadingText}</div>`}
      ${stats.error && html`<div class="err" data-testid="error">${stats.error}</div>`}
      <div class="vec">eye <b data-testid="cam-eye">${fmt(stats.eye)}</b></div>
      <div class="vec">tgt <b data-testid="cam-target">${fmt(stats.target)}</b></div>
    </div>
  `;
}

render(html`<${App} />`, document.getElementById('app'));
```

- [ ] **Step 2: Add minimal styles for the error line**

Append to `pcl-viewer/web/styles.css`:

```css
.hud .err { color: #ff8080; max-width: 18rem; }
```

- [ ] **Step 3: Sanity-run the existing suite (controls moved, testids preserved)**

Run: `cd pcl-viewer && uv run --group dev pytest tests/test_e2e.py -v`
Expected: all existing tests still PASS (the `scene`, `color-mode`, `point-size`, `reset`, `menu-toggle`, `backdrop` testids are unchanged; a new `scene` select was added above them).

- [ ] **Step 4: Commit**

```bash
cd /home/user/samples
git add pcl-viewer/web/app.js pcl-viewer/web/styles.css
git commit -m "feat(pcl-viewer): scene drop-down + movie play/pause and loading HUD"
```

---

## Task 6: e2e tests for the new scenes

**Files:**
- Modify: `pcl-viewer/tests/test_e2e.py`

- [ ] **Step 1: Add the static-URL (table) scene test**

Append to `pcl-viewer/tests/test_e2e.py`. This exercises the `loadStatic`-from-URL path offline by overriding `pclUrl` to the locally-served KITTI model:

```python
def test_static_scene_from_url(server_url, page):
    # Point the "table" scene at the locally-served model so the static URL path
    # is tested offline (same loadStatic code path as the real PCL table scene).
    page.goto(server_url + "/?pclUrl=/models/kitti-velodyne-000000.pcd")
    _wait_ready(page)
    page.get_by_test_id("menu-toggle").click()
    page.get_by_test_id("scene").select_option("table")
    page.wait_for_function("() => window.__PCL.scene === 'table' && window.__PCL.ready === true",
                           timeout=20000)
    visible = page.evaluate("() => window.__PCL.handle.visiblePixelCount()")
    assert visible > 1000
```

- [ ] **Step 2: Add the movie playback + play/pause test**

```python
def test_movie_scene_plays_and_pauses(server_url, page):
    page.goto(server_url + "/?movieBase=/fixtures/movie/&movieCount=4")
    _wait_ready(page)
    page.get_by_test_id("menu-toggle").click()
    page.get_by_test_id("scene").select_option("movie")
    page.wait_for_function(
        "() => window.__PCL.scene === 'movie' && window.__PCL.ready === true && window.__PCL.frameCount === 4",
        timeout=30000)
    # It auto-plays: the frame index advances.
    page.wait_for_function(
        "() => window.__PCL.playing === true")
    start = page.evaluate("() => window.__PCL.frameIndex")
    page.wait_for_function(f"() => window.__PCL.frameIndex !== {start}", timeout=5000)
    # The cloud renders.
    assert page.evaluate("() => window.__PCL.handle.visiblePixelCount()") > 500
    # Pause stops advancement.
    page.get_by_test_id("play-pause").click()
    page.wait_for_function("() => window.__PCL.playing === false")
    frozen = page.evaluate("() => window.__PCL.frameIndex")
    page.wait_for_timeout(700)
    assert page.evaluate("() => window.__PCL.frameIndex") == frozen
```

- [ ] **Step 3: Add the clean scene-switch test (no leaked timer)**

```python
def test_scene_switch_back_stops_movie(server_url, page):
    page.goto(server_url + "/?movieBase=/fixtures/movie/&movieCount=4")
    _wait_ready(page)
    page.get_by_test_id("menu-toggle").click()
    page.get_by_test_id("scene").select_option("movie")
    page.wait_for_function("() => window.__PCL.scene === 'movie' && window.__PCL.ready === true")
    page.get_by_test_id("scene").select_option("city")
    page.wait_for_function("() => window.__PCL.scene === 'city' && window.__PCL.ready === true",
                           timeout=20000)
    # Movie timer torn down: not playing, frameCount reset.
    assert page.evaluate("() => window.__PCL.playing") is False
    assert page.evaluate("() => window.__PCL.frameCount") == 0
    assert page.evaluate("() => window.__PCL.pointCount") == 115385
```

- [ ] **Step 4: Run the full suite**

Run: `cd pcl-viewer && uv run --group dev pytest tests/test_e2e.py -v`
Expected: all tests PASS (existing + 3 new).

- [ ] **Step 5: Commit**

```bash
cd /home/user/samples
git add pcl-viewer/tests/test_e2e.py
git commit -m "test(pcl-viewer): e2e for static URL + movie playback + scene switch"
```

---

## Task 7: The one-shot movie dataset generator

This produces the real movie and publishes it to HF. It is **not** wired into CI and is run manually (Task 8).

**Files:**
- Create: `pcl-viewer/scripts/build_movie_dataset.py`

- [ ] **Step 1: Write the generator**

Create `pcl-viewer/scripts/build_movie_dataset.py`:

```python
"""One-shot: build the KITTI movie HF dataset.

Downloads KITTI raw drive 0005, voxel-downsamples each Velodyne frame to ~30k
points, Draco-encodes (positions only, 14-bit quantization), and uploads the
.drc frames + a CC BY-NC-SA dataset card to a Hugging Face dataset.

Run: HF_TOKEN must be set.
  uv run --group gen python scripts/build_movie_dataset.py --repo-id kolodkin/pcl-viewer-kitti-movie
"""
from __future__ import annotations

import argparse
import io
import os
import zipfile
from pathlib import Path

import numpy as np
import requests
import DracoPy
from huggingface_hub import HfApi

DRIVE_ZIP = (
    "https://s3.eu-central-1.amazonaws.com/avg-kitti/raw_data/"
    "2011_09_26_drive_0005/2011_09_26_drive_0005_sync.zip"
)
VELO_DIR = "2011_09_26/2011_09_26_drive_0005_sync/velodyne_points/data/"

CARD = """\
---
license: cc-by-nc-sa-3.0
task_categories:
  - other
tags:
  - point-cloud
  - lidar
  - kitti
  - draco
---

# pcl-viewer KITTI movie

Draco-compressed, downsampled LiDAR frames for the
[pcl-viewer](https://github.com/kolodkin/samples) demo's "KITTI movie" scene.

**This is a derivative work.** Each `NNNNNN.drc` is one Velodyne sweep from
**KITTI raw drive `2011_09_26_drive_0005`**, voxel-downsampled to ~30k points
(positions only) and Draco-encoded (14-bit position quantization).

## Attribution & license

Source: the KITTI dataset, **CC BY-NC-SA 3.0**. This derivative is released under
the **same license (CC BY-NC-SA 3.0)** per the ShareAlike term.

> A. Geiger, P. Lenz, C. Stiller, R. Urtasun. *Vision meets Robotics: The KITTI
> Dataset.* International Journal of Robotics Research (IJRR), 2013.
>
> A. Geiger, P. Lenz, R. Urtasun. *Are we ready for Autonomous Driving? The KITTI
> Vision Benchmark Suite.* CVPR, 2012.

Non-commercial use only.
"""


def voxel_downsample(pts: np.ndarray, voxel: float) -> np.ndarray:
    keys = np.floor(pts / voxel).astype(np.int64)
    _, idx = np.unique(keys, axis=0, return_index=True)
    return pts[np.sort(idx)]


def downsample_to_target(pts: np.ndarray, target: int = 30000) -> np.ndarray:
    """Pick a voxel size that lands near `target` points (a few bisection steps)."""
    lo, hi = 0.05, 2.0
    out = pts
    for _ in range(12):
        mid = (lo + hi) / 2
        out = voxel_downsample(pts, mid)
        if len(out) > target:
            lo = mid
        else:
            hi = mid
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo-id", default="kolodkin/pcl-viewer-kitti-movie")
    ap.add_argument("--out", default="/tmp/kitti-movie")
    ap.add_argument("--limit", type=int, default=0, help="cap frame count (0 = all)")
    ap.add_argument("--no-upload", action="store_true")
    args = ap.parse_args()

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    print(f"Downloading {DRIVE_ZIP} …")
    resp = requests.get(DRIVE_ZIP, timeout=600)
    resp.raise_for_status()
    zf = zipfile.ZipFile(io.BytesIO(resp.content))
    bins = sorted(n for n in zf.namelist() if n.startswith(VELO_DIR) and n.endswith(".bin"))
    if args.limit:
        bins = bins[: args.limit]
    print(f"{len(bins)} velodyne frames")

    count = 0
    for i, name in enumerate(bins):
        raw = np.frombuffer(zf.read(name), dtype=np.float32).reshape(-1, 4)
        xyz = downsample_to_target(raw[:, :3].copy(), 30000)
        buf = DracoPy.encode(xyz.astype(np.float32), quantization_bits=14)
        (out / f"{i:06d}.drc").write_bytes(buf)
        count += 1
        if i % 20 == 0:
            print(f"  frame {i}: {len(xyz)} pts -> {len(buf)} bytes")
    (out / "README.md").write_text(CARD)
    print(f"wrote {count} frames to {out}")

    if args.no_upload:
        return
    token = os.environ["HF_TOKEN"]
    api = HfApi(token=token)
    api.create_repo(args.repo_id, repo_type="dataset", exist_ok=True, private=False)
    api.upload_folder(folder_path=str(out), repo_id=args.repo_id, repo_type="dataset")
    print(f"uploaded to https://huggingface.co/datasets/{args.repo_id}  (frames: {count})")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Add `requests` to the `gen` group**

In `pcl-viewer/pyproject.toml`, update the `gen` group to include `requests`:

```toml
[dependency-groups]
gen = ["numpy>=1.26", "DracoPy>=1.4.0", "huggingface_hub>=0.23", "requests>=2.31"]
```

- [ ] **Step 3: Smoke-test the generator logic without uploading (small limit)**

Run: `cd pcl-viewer && uv run --group gen python scripts/build_movie_dataset.py --limit 3 --no-upload --out /tmp/kitti-movie-smoke`
Expected: downloads the drive, writes `000000.drc`…`000002.drc` + `README.md` to `/tmp/kitti-movie-smoke`; each `.drc` ~60–90 KB.

- [ ] **Step 4: Commit**

```bash
cd /home/user/samples
git add pcl-viewer/scripts/build_movie_dataset.py pcl-viewer/pyproject.toml
git commit -m "feat(pcl-viewer): one-shot KITTI movie HF dataset generator"
```

---

## Task 8: Publish the HF dataset and pin the frame count

**Files:**
- Modify: `pcl-viewer/web/config.js` (only if the real frame count differs from 154)

- [ ] **Step 1: Run the full generator (download + encode + upload)**

Run: `cd pcl-viewer && uv run --group gen python scripts/build_movie_dataset.py --repo-id kolodkin/pcl-viewer-kitti-movie`
Expected: ends with `uploaded to https://huggingface.co/datasets/kolodkin/pcl-viewer-kitti-movie (frames: N)`. Note `N`.

- [ ] **Step 2: Verify the dataset is public, has the frames + license**

Run: `curl -s "https://huggingface.co/api/datasets/kolodkin/pcl-viewer-kitti-movie" | python3 -c "import sys,json;d=json.load(sys.stdin);print('private',d.get('private'),'card',d.get('cardData',{}).get('license'))"`
Then: `curl -sI "https://huggingface.co/datasets/kolodkin/pcl-viewer-kitti-movie/resolve/main/000000.drc" -H "Origin: https://kolodkin.github.io" | grep -i "access-control-allow-origin"`
Expected: `private False`, `card cc-by-nc-sa-3.0`, and a reflected `access-control-allow-origin` header.

- [ ] **Step 3: Set `MOVIE_COUNT` to the real frame count**

If `N` from Step 1 is not 154, update the default in `pcl-viewer/web/config.js`:

```js
export const MOVIE_COUNT = parseInt(pick('movieCount', 'N'), 10);  // replace N
```

- [ ] **Step 4: Manually verify the live movie in a browser (optional but recommended)**

Run: `cd pcl-viewer && PORT=8123 bash pcl-viewer.sh` then open `http://127.0.0.1:8123/`, open controls, select **KITTI movie**. Expected: a loading counter, then a looping driving sequence. Ctrl-C to stop.

- [ ] **Step 5: Commit (if config changed)**

```bash
cd /home/user/samples
git add pcl-viewer/web/config.js
git commit -m "chore(pcl-viewer): pin KITTI movie frame count"
```

---

## Task 9: Docs — README + SPEC + attribution

**Files:**
- Modify: `pcl-viewer/README.md`
- Modify: `pcl-viewer/SPEC.md`

- [ ] **Step 1: Update README description**

Replace the description paragraph in `pcl-viewer/README.md` so it mentions the three scenes (keep the exact README convention: setext title, one paragraph, one bash block). New body:

```markdown
PCL Viewer — three.js + Preact
---

A no-build, ES-module point cloud viewer with a **Scene** selector: a 360°
street-level KITTI LiDAR scan (`kitti-velodyne-000000.pcd`, 115k points), a PCL
`table_scene_lms400` cloud hot-linked from the PointCloudLibrary data repo, and a
**KITTI movie** — a Draco-compressed multi-frame LiDAR sequence streamed at
runtime from a Hugging Face dataset and decoded in-browser. It renders with
three.js `OrbitControls`, wraps the scene in a Preact UI (scene selector,
play/pause, point-size, color modes, live stats), loads frontend deps as vendored
ES modules via an import map (no bundler), is served by a small Python
`http.server`, and is verified end-to-end with Python Playwright.

```bash
./pcl-viewer.sh
```
```

- [ ] **Step 2: Add a Scenes + attribution section to SPEC.md**

Append to `pcl-viewer/SPEC.md`:

```markdown
## Scenes

| Scene            | Source                                                            | Transport            |
|------------------|------------------------------------------------------------------|----------------------|
| KITTI city view  | `web/models/kitti-velodyne-000000.pcd` (committed)               | same-origin PCD      |
| PCL table scene  | `PointCloudLibrary/data` `table_scene_lms400.pcd`                 | raw.githubusercontent (CORS) |
| KITTI movie      | `kolodkin/pcl-viewer-kitti-movie` (HF dataset)                    | HF resolve (CORS), Draco `.drc` |

`web/config.js` holds the scene URLs and the movie frame count, each overridable
via `?pclUrl=`, `?movieBase=`, `?movieCount=` (used by e2e to point at local
fixtures). The movie shares one normalization transform (computed from frame 0)
so points don't pulse; it plays at 10 fps and loops, with play/pause.

### Movie pipeline (`scripts/build_movie_dataset.py`, one-shot)

KITTI raw drive `2011_09_26_drive_0005` → per-frame Velodyne `.bin` →
voxel-downsample to ~30k points (positions only; the viewer colors by height/flat,
so reflectance is dropped) → Draco encode (14-bit position quantization, ~60–90 KB
/frame) → upload to the HF dataset with a dataset card. Frame data is **not** in
git; the browser fetches `.drc` at runtime and decodes with a vendored Draco WASM
decoder (`web/vendor/draco/`, fetched by `vendor.sh`).

### Licensing

KITTI is **CC BY-NC-SA 3.0**. Both the committed city frame and the derived movie
dataset retain that license with attribution (Geiger et al., IJRR 2013 / CVPR
2012); the HF dataset card carries `license: cc-by-nc-sa-3.0` and the citation per
the BY + SA terms. The PCL table scene is **BSD-3-Clause** (PointCloudLibrary).
```

- [ ] **Step 3: Commit**

```bash
cd /home/user/samples
git add pcl-viewer/README.md pcl-viewer/SPEC.md
git commit -m "docs(pcl-viewer): document scenes, movie pipeline, attribution"
```

---

## Task 10: Final verification

- [ ] **Step 1: Clean vendor + full test run**

Run: `cd pcl-viewer && bash vendor.sh && uv run --group dev pytest -v`
Expected: every test PASSES (city, controls, static URL scene, movie playback, scene switch, screenshot, camera readout).

- [ ] **Step 2: Confirm no frame data was committed**

Run: `cd /home/user/samples && git status --porcelain && git ls-files pcl-viewer/web | grep -E "fixtures|vendor" || echo "no vendor/fixtures tracked under web/"`
Expected: clean working tree; nothing under `web/vendor/` or `web/fixtures/` tracked (only `tests/fixtures/movie/*.drc` are tracked, which is intended).

- [ ] **Step 3: Push the branch**

```bash
cd /home/user/samples
git push -u origin claude/pcl-viewer-url-jemzbd
```

---

## Self-review notes (for the author)

- **Spec coverage:** scene drop-down (Task 5), city/table/movie (Tasks 3,5,7,8), Draco + vendored decoder (Tasks 1,3), HF hosting + license card (Tasks 7,8), no git bloat (Task 10 step 2), offline e2e (Tasks 4,6), config overrides (Task 2), README/SPEC + attribution (Task 9). ✅
- **Intensity:** dropped everywhere (viewer colors by height/flat) — consistent between generator, fixtures, and viewer. The spec's "8-bit intensity" note is superseded; positions-only is simpler and sufficient.
- **Naming consistency:** `loadScene`/`loadStatic`/`loadMovie`/`play`/`pause`, `frameUrl`, `MOVIE_COUNT`, `MOVIE_BASE`, `PCL_URL`, `CITY_URL`, and the `data-testid`s (`scene`, `play-pause`, `frame-index`, `loading`, `error`) are used identically across viewer.js, app.js, config.js, and tests.
- **DRACOLoader returns a `BufferGeometry`** (positions only); we wrap it in `THREE.Points`. City/table use `PCDLoader` (`.geometry`).
```
