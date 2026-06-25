# KITTI-seg Scene Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `seg` scene to pcl-viewer: a streamed SemanticKITTI movie where every point is colored by its semantic class ("by class" mode) and each object instance is wrapped in a 3D bounding box, all served from a `seg/` folder of the shared `kolodkin/pcl-viewer-kitti-movie` HF dataset.

**Architecture:** A one-shot Python builder downloads a SemanticKITTI sequence slice, packs each point's learning-class id into the Draco **color attribute** (so it survives Draco's point reordering — verified), derives one axis-aligned box per instance into a `boxes.json`, and uploads both under `seg/`. The viewer gains a `seg` scene that decodes class ids from the color attribute, colors points via a fixed 20-class palette, and renders a per-frame box group. The Preact UI gains the scene, a "By class" mode, a class legend, and a "show boxes" toggle.

**Tech Stack:** Python (numpy, DracoPy, huggingface_hub, requests), three.js (DRACOLoader, LineSegments, Box3), Preact/htm, pytest-playwright.

---

## Shared conventions (both builders + viewer MUST agree)

**Learning class ids:** 0 = unlabeled/ignore, 1..19 = the standard SemanticKITTI 19-class learning set. One `uint8` per point.

**Class → palette (RGB hex), index = learning id 0..19:**
```
0  unlabeled     0x202830   (muted gray-blue)
1  car           0x6496F5
2  bicycle        0x64E6F5
3  motorcycle     0x1E3C96
4  truck          0x501EB4
5  other-vehicle  0x0000FF
6  person         0xFF1E1E
7  bicyclist      0xFF28C8
8  motorcyclist   0x961E5A
9  road           0xFF00FF
10 parking        0xFF96FF
11 sidewalk       0x4B004B
12 other-ground   0xAF004B
13 building       0xFFC800
14 fence          0xFF7832
15 vegetation     0x00AF00
16 trunk          0x873C00
17 terrain        0x96F050
18 pole           0xFFF096
19 traffic-sign   0xFF0000
```

**Thing classes (get boxes):** learning ids 1..8. Other classes are "stuff" — no boxes.

**Draco encoding:** positions `float32` + a `colors` `uint8` array shape `(N,3)` where `colors[:,0] = class_id`, `colors[:,1:] = 0`. 14-bit position quantization (as the geometry movie). On decode, three.js exposes a normalized `color` attribute; recover `classId = Math.round(color.getX(i) * 255)`.

**boxes.json schema** — keyed by zero-padded frame string, boxes in the **source KITTI Velodyne frame** (metres, z-up, sensor at origin):
```json
{ "000000": [ {"cls": 1, "center": [x, y, z], "size": [sx, sy, sz]}, ... ], "000001": [ ... ] }
```
The viewer applies the same rotate→translate→scale normalization to box corners that it applies to points.

---

## File Structure

- **Create** `pcl-viewer/scripts/build_seg_dataset.py` — one-shot SemanticKITTI → `seg/` builder.
- **Create** `pcl-viewer/tests/fixtures/build_seg_fixtures.py` — tiny offline seg fixtures builder.
- **Create** `pcl-viewer/tests/fixtures/seg/000000.drc … 000003.drc` + `tests/fixtures/seg/boxes.json` (committed outputs).
- **Modify** `pcl-viewer/web/config.js` — seg scene URLs + count + helpers.
- **Modify** `pcl-viewer/web/viewer.js` — palette, class color buffer, `loadSegMovie`, box group, `setShowBoxes`, stats.
- **Modify** `pcl-viewer/web/app.js` — `seg` scene, "By class" option, legend, "show boxes" toggle.
- **Modify** `pcl-viewer/web/styles.css` — legend + toggle styling.
- **Modify** `pcl-viewer/tests/test_e2e.py` — seg scene e2e.
- **Modify** `pcl-viewer/README.md`, `pcl-viewer/SPEC.md` — document the scene.

---

## Task 1: config.js — seg scene parameters

**Files:**
- Modify: `pcl-viewer/web/config.js`

- [ ] **Step 1: Add seg config after the `frameUrl` helper**

Append to `web/config.js` (after line 24):
```javascript

export const SEG_MOVIE_BASE = pick(
  'segMovieBase',
  'https://huggingface.co/datasets/kolodkin/pcl-viewer-kitti-movie/resolve/main/seg/',
);

export const SEG_MOVIE_COUNT = parseInt(pick('segMovieCount', '150'), 10);

export const SEG_BOXES_URL = pick('segBoxesUrl', `${SEG_MOVIE_BASE}boxes.json`);

// Seg frame URL helper: SEG_MOVIE_BASE + zero-padded index + .drc
export const segFrameUrl = (i) => `${SEG_MOVIE_BASE}${String(i).padStart(6, '0')}.drc`;
```

- [ ] **Step 2: Syntax-check**

Run: `node --check web/config.js`
Expected: no output (exit 0).

- [ ] **Step 3: Commit**

```bash
git add pcl-viewer/web/config.js
git commit -m "feat(pcl-viewer): add seg scene config (base, count, boxes url)"
```

---

## Task 2: Seg fixtures builder + committed fixtures

The viewer e2e for the seg scene needs tiny, offline `.drc` frames that carry a class color attribute, plus a `boxes.json`. Build them from the committed city PCD with synthetic classes/instances.

**Files:**
- Create: `pcl-viewer/tests/fixtures/build_seg_fixtures.py`
- Create (outputs): `pcl-viewer/tests/fixtures/seg/000000.drc`…`000003.drc`, `pcl-viewer/tests/fixtures/seg/boxes.json`

- [ ] **Step 1: Write the fixtures builder**

Create `tests/fixtures/build_seg_fixtures.py`:
```python
"""Build tiny seg movie fixtures from the committed KITTI frame.

Run once (commit the outputs): produces 4 heavily-decimated .drc frames whose
per-point COLOR attribute red channel carries a synthetic learning-class id, plus
a boxes.json with one moving box per frame. Gives the seg e2e a real, light,
offline sequence with both per-point classes and per-frame boxes.

    uv run --group gen python tests/fixtures/build_seg_fixtures.py
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import DracoPy

HERE = Path(__file__).resolve().parent
SRC = HERE.parent.parent / "web" / "models" / "kitti-velodyne-000000.pcd"
OUT = HERE / "seg"


def read_binary_pcd_xyz(path: Path) -> np.ndarray:
    data = path.read_bytes()
    marker = b"DATA binary\n"
    header_end = data.index(marker) + len(marker)
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
    base = voxel_downsample(xyz, 0.8)
    # Synthetic per-point class: ground-ish (low z) -> road(9); high -> building(13);
    # a small cluster near +x -> car(1) so "By class" shows >=3 colors.
    boxes = {}
    for i in range(4):
        frame = base + np.array([i * 0.5, 0.0, 0.0], dtype=np.float32)
        cls = np.full(len(frame), 9, dtype=np.uint8)            # road default
        cls[frame[:, 2] > np.median(frame[:, 2])] = 13          # building
        near = (np.abs(frame[:, 0] - (3.0 + i * 0.5)) < 1.5) & (np.abs(frame[:, 1]) < 1.5)
        cls[near] = 1                                           # car
        colors = np.zeros((len(frame), 3), dtype=np.uint8)
        colors[:, 0] = cls
        buf = DracoPy.encode(frame.astype(np.float32), colors=colors, quantization_bits=14)
        (OUT / f"{i:06d}.drc").write_bytes(buf)
        # One car box that rolls forward with the frame, in the source velodyne frame.
        boxes[f"{i:06d}"] = [
            {"cls": 1, "center": [3.0 + i * 0.5, 0.0, -1.0], "size": [4.0, 2.0, 1.6]}
        ]
        print(f"wrote {OUT / f'{i:06d}.drc'}  ({len(buf)} bytes, {len(frame)} pts)")
    (OUT / "boxes.json").write_text(json.dumps(boxes))
    print(f"wrote {OUT / 'boxes.json'} ({len(boxes)} frames)")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Run it to produce committed fixtures**

Run: `uv run --group gen python tests/fixtures/build_seg_fixtures.py`
Expected: prints 4 `wrote …drc` lines and a `wrote …boxes.json` line; `tests/fixtures/seg/` now has 4 `.drc` + `boxes.json`.

- [ ] **Step 3: Verify the color attribute round-trips the class ids**

Run:
```bash
uv run --group gen python -c "import DracoPy,numpy as np; m=DracoPy.decode(open('tests/fixtures/seg/000000.drc','rb').read()); c=np.asarray(m.colors)[:,0]; print('classes present:', sorted(set(c.tolist())))"
```
Expected: `classes present: [1, 9, 13]` (order/extra may vary, but must include 1, 9, 13).

- [ ] **Step 4: Commit**

```bash
git add pcl-viewer/tests/fixtures/build_seg_fixtures.py pcl-viewer/tests/fixtures/seg/
git commit -m "test(pcl-viewer): add offline seg movie fixtures (class colors + boxes)"
```

---

## Task 3: viewer.js — class palette + "by class" color buffer

**Files:**
- Modify: `pcl-viewer/web/viewer.js` (constants near line 9; `computeColorBuffers` lines 148–164)

- [ ] **Step 1: Add the palette constant**

After line 11 (`const MOVIE_FPS = 15;`) add:
```javascript
// SemanticKITTI 19-class learning palette, indexed by class id (0 = unlabeled).
const SEG_PALETTE = [
  0x202830, 0x6496F5, 0x64E6F5, 0x1E3C96, 0x501EB4, 0x0000FF, 0xFF1E1E,
  0xFF28C8, 0x961E5A, 0xFF00FF, 0xFF96FF, 0x4B004B, 0xAF004B, 0xFFC800,
  0xFF7832, 0x00AF00, 0x873C00, 0x96F050, 0xFFF096, 0xFF0000,
].map((hex) => new THREE.Color(hex));
const SEG_THING_CLASSES = new Set([1, 2, 3, 4, 5, 6, 7, 8]);
```

- [ ] **Step 2: Extend `computeColorBuffers` to build a `class` buffer from the color attribute**

Replace the body of `computeColorBuffers` (lines 148–164) with:
```javascript
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
  // Draco color attribute carries the per-point class id in its (normalized) red
  // channel; map each id through the fixed palette to build the "by class" buffer.
  const klass = geometry.getAttribute('color');
  if (klass) {
    const out = new Float32Array(n * 3);
    for (let i = 0; i < n; i++) {
      const id = Math.round(klass.getX(i) * 255);
      const c = SEG_PALETTE[id] || SEG_PALETTE[0];
      out[i * 3] = c.r; out[i * 3 + 1] = c.g; out[i * 3 + 2] = c.b;
    }
    buffers.class = out;
  }
  return buffers;
}
```

(No code reads `buffers.class` until `applyColorMode('class')` is called — `applyColorMode` already does `colorBuffers[mode]` lookup with flat fallback, so `'class'` works with no further change there. Verify by reading lines 166–182.)

- [ ] **Step 3: Run existing suite to confirm no regression**

Run: `PLAYWRIGHT_BROWSERS_PATH=/opt/pw-browsers uv run --group dev --group gen pytest -q`
Expected: all existing tests pass (17 passed).

- [ ] **Step 4: Commit**

```bash
git add pcl-viewer/web/viewer.js
git commit -m "feat(pcl-viewer): build by-class color buffer from Draco class attribute"
```

---

## Task 4: viewer.js — box group + `loadSegMovie` + handle wiring

**Files:**
- Modify: `pcl-viewer/web/viewer.js` (imports line 7; `loadScene` lines 442–458; handle lines 471–530; `teardownScene` ~line 263; state lines 78–90)

- [ ] **Step 1: Import seg config**

Replace the config import on line 7:
```javascript
import { CITY_URL, PCL_URL, MOVIE_COUNT, frameUrl } from './config.js';
```
with:
```javascript
import {
  CITY_URL, PCL_URL, MOVIE_COUNT, frameUrl,
  SEG_MOVIE_COUNT, SEG_BOXES_URL, segFrameUrl,
} from './config.js';
```

- [ ] **Step 2: Add box-group state and a builder near the movie helpers**

Add these module-level `let`s next to the existing `movie`/`points` declarations (find `let points` / `let movie`, add alongside):
```javascript
let boxGroup = null;     // THREE.Group of per-instance LineSegments for the seg scene
let segBoxes = null;     // parsed boxes.json: { "000000": [ {cls,center,size}, ... ] }
let showBoxes = true;
```

Add this helper (place it just above `loadMovie`):
```javascript
// Transform an axis-aligned source-frame box through the same rotate→translate→
// scale normalization applied to the movie points, then return its 12-edge
// LineSegments colored by class. `shared` is the movie's normalization transform.
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
  // corners are ordered by (dx,dy,dz) bits; edges connect corners differing in one bit.
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

// Rebuild the box group for the given frame index from segBoxes + shared transform.
function updateBoxes(frameIndex, shared) {
  if (boxGroup) { scene.remove(boxGroup); boxGroup.traverse((o) => { if (o.geometry) o.geometry.dispose(); if (o.material) o.material.dispose(); }); }
  boxGroup = new THREE.Group();
  boxGroup.visible = showBoxes;
  const list = (segBoxes && segBoxes[String(frameIndex).padStart(6, '0')]) || [];
  for (const box of list) boxGroup.add(buildBoxLines(box, shared));
  state.boxCount = boxGroup.children.length;
  scene.add(boxGroup);
}
```

- [ ] **Step 3: Add `loadSegMovie` (a movie variant that fetches boxes and updates them per frame)**

Add `loadSegMovie` directly after `loadMovie`. It mirrors `loadMovie` but (a) decodes via `segFrameUrl`, (b) fetches `SEG_BOXES_URL` once before playback, and (c) the render loop calls `updateBoxes` when the displayed frame changes. To keep `loadMovie`'s render loop untouched, the simplest hook is: store `shared` on the module and have the existing frame-advance path call `updateBoxes` only when `state.scene === 'seg'`.

Implement as follows. First, capture `shared` for the seg scene by adding a module `let segShared = null;`. Then add:
```javascript
async function loadSegMovie(count) {
  const token = loadToken;
  // Fetch boxes alongside frame 0; tolerate a missing/!ok boxes.json (no boxes).
  try {
    const resp = await fetch(SEG_BOXES_URL);
    segBoxes = resp.ok ? await resp.json() : null;
  } catch { segBoxes = null; }
  if (token !== loadToken) return;
  await loadMovie(count, (shared, frameIndex) => {
    segShared = shared;
    updateBoxes(frameIndex, shared);
  });
}
```

Then make `loadMovie` accept an optional `onFrame(shared, index)` callback. In `loadMovie`, thread `shared` out of `decodeFrame` (it already populates `shared`), and call `onFrame` when frame 0 installs and whenever the playhead changes. Concretely:

1. Change the signature `async function loadMovie(count)` → `async function loadMovie(count, onFrame)`.
2. After `installGeometry(first.geometry, first.buffers);` (frame 0), add: `if (onFrame) onFrame(shared, 0);`
3. Find the playback advance code (where `state.frameIndex` is updated and the next frame's geometry is installed during `play()`/the timer). Store `onFrame` on the `movie` object (`movie = { frames, timer: null, index: 0, failed: new Set(), onFrame };`) and, at the point a new frame is shown, add: `if (movie.onFrame) movie.onFrame(shared, movie.index);`

> Read lines 308–410 (the `play`/timer/`showFrame` path) to place the single `movie.onFrame(...)` call at the spot where the visible frame changes. There is exactly one place geometry is swapped for the current `movie.index`; add the call right after it.

- [ ] **Step 4: Dispatch the seg scene in `loadScene`**

In `loadScene` (lines 455–457), add a branch after the `movie` branch:
```javascript
  else if (id === 'movie') await loadMovie(MOVIE_COUNT);
  else if (id === 'seg') await loadSegMovie(SEG_MOVIE_COUNT);
```

- [ ] **Step 5: Tear down boxes on scene switch**

In `teardownScene` (~line 263), add at the top:
```javascript
  if (boxGroup) {
    scene.remove(boxGroup);
    boxGroup.traverse((o) => { if (o.geometry) o.geometry.dispose(); if (o.material) o.material.dispose(); });
    boxGroup = null;
  }
  segBoxes = null; segShared = null; state.boxCount = 0;
```

- [ ] **Step 6: Add `boxCount`/`showBoxes` to state and `setShowBoxes` to the handle**

In the `state` object (lines 78–90) add: `boxCount: 0,` and within `settings`: `showBoxes: true,`.

In the handle (after `setColorMode`), add:
```javascript
  setShowBoxes(on) {
    showBoxes = !!on;
    state.settings.showBoxes = showBoxes;
    if (boxGroup) boxGroup.visible = showBoxes;
  },
```

Add `boxCount: state.boxCount,` to the `getStats()` return object.

- [ ] **Step 7: Syntax-check + run suite**

Run: `node --check web/viewer.js && PLAYWRIGHT_BROWSERS_PATH=/opt/pw-browsers uv run --group dev --group gen pytest -q`
Expected: existing 17 tests still pass.

- [ ] **Step 8: Commit**

```bash
git add pcl-viewer/web/viewer.js
git commit -m "feat(pcl-viewer): seg movie scene with per-frame 3D bounding boxes"
```

---

## Task 5: app.js + styles.css — scene, By-class mode, legend, boxes toggle

**Files:**
- Modify: `pcl-viewer/web/app.js` (SCENES line 9; color-mode select lines 157–163; conditional UI region; onScene lines 65–68)
- Modify: `pcl-viewer/web/styles.css`

- [ ] **Step 1: Add the seg scene**

In `SCENES` (lines 9–13) add a fourth entry:
```javascript
  { id: 'seg', label: 'KITTI seg' },
```

- [ ] **Step 2: Add the "By class" color option**

In the color-mode `<select>` (lines 157–163) add as the first option after `Flat`:
```javascript
      <option value="class">By class</option>
```

- [ ] **Step 3: Default to "By class" when entering seg, and track the seg flag**

Replace `onScene` (lines 65–68) with:
```javascript
  const onScene = (e) => {
    const id = e.target.value;
    setSceneId(id);
    viewerRef.current.loadScene(id);
    if (id === 'seg') { setColorMode('class'); viewerRef.current.setColorMode('class'); }
  };
```

Add near `isMovie` (line 96):
```javascript
  const isSeg = sceneId === 'seg';
```

- [ ] **Step 4: Add the boxes toggle + legend (seg only)**

Add a `showBoxes` state near the other `useState`s (e.g. after line 19):
```javascript
  const [showBoxes, setShowBoxes] = useState(true);
```

Add a handler near `onColor`:
```javascript
  const onToggleBoxes = (e) => {
    setShowBoxes(e.target.checked);
    viewerRef.current.setShowBoxes(e.target.checked);
  };
```

In the controls JSX, add (gated on `isSeg`), after the color-mode select:
```javascript
        ${isSeg && html`
          <label class="row">
            <input type="checkbox" data-testid="show-boxes"
                   checked=${showBoxes} onChange=${onToggleBoxes} />
            Show boxes
          </label>
          <div class="legend" data-testid="legend">
            ${SEG_LEGEND.map((c) => html`
              <span class="legend-item">
                <span class="swatch" style=${`background:#${c.hex}`}></span>${c.name}
              </span>`)}
          </div>
        `}
```

Add the legend data near `SCENES` (top of `app.js`):
```javascript
const SEG_LEGEND = [
  { name: 'car', hex: '6496F5' }, { name: 'person', hex: 'FF1E1E' },
  { name: 'road', hex: 'FF00FF' }, { name: 'sidewalk', hex: '4B004B' },
  { name: 'building', hex: 'FFC800' }, { name: 'vegetation', hex: '00AF00' },
  { name: 'pole', hex: 'FFF096' }, { name: 'traffic-sign', hex: 'FF0000' },
];
```

- [ ] **Step 5: Style the legend**

Append to `web/styles.css`:
```css
.legend { display: flex; flex-wrap: wrap; gap: 4px 10px; margin-top: 6px; font-size: 11px; }
.legend-item { display: inline-flex; align-items: center; gap: 4px; }
.legend .swatch { width: 10px; height: 10px; border-radius: 2px; display: inline-block; }
```

- [ ] **Step 6: Syntax-check + run suite**

Run: `node --check web/app.js && PLAYWRIGHT_BROWSERS_PATH=/opt/pw-browsers uv run --group dev --group gen pytest -q`
Expected: existing 17 tests pass.

- [ ] **Step 7: Commit**

```bash
git add pcl-viewer/web/app.js pcl-viewer/web/styles.css
git commit -m "feat(pcl-viewer): seg scene UI — By class mode, legend, boxes toggle"
```

---

## Task 6: e2e test for the seg scene

**Files:**
- Modify: `pcl-viewer/tests/test_e2e.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_e2e.py` (model on `test_movie_scene_plays_and_pauses`, lines 146–167). The fixtures are staged at `/fixtures/seg/` by conftest's `_stage_fixtures`.
```python
def test_seg_scene_classes_and_boxes(server_url, page):
    page.goto(
        server_url
        + "/?segMovieBase=/fixtures/seg/&segMovieCount=4&segBoxesUrl=/fixtures/seg/boxes.json"
    )
    _wait_ready(page)
    page.get_by_test_id("menu-toggle").click()
    page.get_by_test_id("scene").select_option("seg")
    page.wait_for_function(
        "() => window.__PCL.scene === 'seg' && window.__PCL.ready === true"
        " && window.__PCL.frameCount === 4",
        timeout=60000,
    )
    # By-class coloring is selected automatically and renders pixels.
    page.wait_for_function("() => window.__PCL.settings.colorMode === 'class'")
    assert page.evaluate("() => window.__PCL.handle.visiblePixelCount()") > 500
    # Boxes render (the fixture has one car box per frame).
    page.wait_for_function("() => window.__PCL.handle.getStats().boxCount >= 1", timeout=5000)
    # The toggle hides them.
    page.get_by_test_id("show-boxes").click()
    page.wait_for_function("() => window.__PCL.settings.showBoxes === false")
    # Legend is shown for the seg scene.
    assert page.get_by_test_id("legend").is_visible()
```

- [ ] **Step 2: Run it**

Run: `PLAYWRIGHT_BROWSERS_PATH=/opt/pw-browsers uv run --group dev --group gen pytest tests/test_e2e.py::test_seg_scene_classes_and_boxes -q`
Expected: PASS. If it fails on `boxCount`, re-read Task 4 Step 3 placement of the `movie.onFrame` call.

- [ ] **Step 3: Run the full suite**

Run: `PLAYWRIGHT_BROWSERS_PATH=/opt/pw-browsers uv run --group dev --group gen pytest -q`
Expected: 18 passed.

- [ ] **Step 4: Commit**

```bash
git add pcl-viewer/tests/test_e2e.py
git commit -m "test(pcl-viewer): e2e for seg scene class coloring + boxes"
```

---

## Task 7: build_seg_dataset.py — the real SemanticKITTI builder (one-shot)

This is a one-shot generator (not unit-tested against the live download). It must be runnable with `--limit` and `--no-upload` for a small dry run.

**Files:**
- Create: `pcl-viewer/scripts/build_seg_dataset.py`

- [ ] **Step 1: Write the builder**

Create `scripts/build_seg_dataset.py`:
```python
"""One-shot: build the SemanticKITTI 'seg' movie under the shared HF dataset.

Downloads one SemanticKITTI sequence slice (KITTI Odometry velodyne + label
files), remaps each point to the 19-class learning set, joint voxel-downsamples
to ~30k points carrying the class, derives one axis-aligned 3D box per thing
instance, Draco-encodes positions with the class id packed into the color
attribute (red channel), writes boxes.json, and uploads everything under seg/ in
kolodkin/pcl-viewer-kitti-movie alongside the existing geometry/ movie.

Inputs (set SEMANTIC_KITTI_DIR to a local SemanticKITTI 'dataset/sequences' tree,
or pass --velodyne-dir / --label-dir):
  <seq>/velodyne/NNNNNN.bin   float32 [x y z remission]
  <seq>/labels/NNNNNN.label   uint32  (low16 = class, high16 = instance)

Run (HF_TOKEN must be set to upload):
  uv run --group gen python scripts/build_seg_dataset.py --seq 08 --start 0 --limit 150
  uv run --group gen python scripts/build_seg_dataset.py --seq 08 --limit 4 --no-upload
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import numpy as np
import DracoPy
from huggingface_hub import HfApi

REPO_ID = "kolodkin/pcl-viewer-kitti-movie"
TARGET_POINTS = 30000
QUANT_BITS = 14
THING_CLASSES = {1, 2, 3, 4, 5, 6, 7, 8}

# SemanticKITTI raw label id -> 19-class learning id (the official learning_map).
LEARNING_MAP = {
    0: 0, 1: 0, 10: 1, 11: 2, 13: 5, 15: 3, 16: 5, 18: 4, 20: 5, 30: 6, 31: 7,
    32: 8, 40: 9, 44: 10, 48: 11, 49: 12, 50: 13, 51: 14, 52: 0, 60: 9, 70: 15,
    71: 16, 72: 17, 80: 18, 81: 19, 99: 0, 252: 1, 253: 7, 254: 6, 255: 8,
    256: 5, 257: 5, 258: 4, 259: 5,
}

CARD = """\
---
license: cc-by-nc-sa-3.0
task_categories:
  - other
tags: [point-cloud, lidar, kitti, semantic-kitti, draco]
---

# pcl-viewer KITTI movies

Draco-compressed LiDAR frames for the
[pcl-viewer](https://github.com/kolodkin/samples) demo, in two folders:

- **`geometry/`** — positions-only sweeps from KITTI raw drive `2011_09_26_drive_0005`.
- **`seg/`** — SemanticKITTI sequence slice with a per-point **class id** packed in
  the Draco color attribute, plus `boxes.json` (one axis-aligned 3D box per thing
  instance per frame).

## Attribution & license

Source: KITTI / SemanticKITTI, **CC BY-NC-SA 3.0**; these derivatives keep the
same license (ShareAlike). Non-commercial use only.

> Geiger et al., *Vision meets Robotics: The KITTI Dataset*, IJRR 2013.
> Behley et al., *SemanticKITTI: A Dataset for Semantic Scene Understanding of
> LiDAR Sequences*, ICCV 2019.
"""

ANNOTATIONS = """\
# Annotations — seg/

Each `NNNNNN.drc` is a SemanticKITTI sweep, voxel-downsampled to ~{target:,}
points (positions only), Draco-encoded with the per-point **19-class learning id**
stored in the color attribute's red channel ({bits}-bit positions). `boxes.json`
maps each frame to a list of axis-aligned 3D boxes (one per thing instance):
`{{ "NNNNNN": [ {{"cls": id, "center": [x,y,z], "size": [sx,sy,sz]}} ] }}`, in the
source Velodyne frame (metres, z-up, sensor at origin).

Sequence: **{seq}**, frames {start}…{last}. License: see `../README.md`.
"""


def voxel_downsample_idx(pts: np.ndarray, voxel: float) -> np.ndarray:
    keys = np.floor(pts / voxel).astype(np.int64)
    _, idx = np.unique(keys, axis=0, return_index=True)
    return np.sort(idx)


def downsample_idx_to_target(pts: np.ndarray, target: int) -> np.ndarray:
    lo, hi = 0.05, 2.0
    idx = np.arange(len(pts))
    for _ in range(12):
        mid = (lo + hi) / 2
        idx = voxel_downsample_idx(pts, mid)
        if len(idx) > target:
            lo = mid
        else:
            hi = mid
    return idx


def remap_classes(raw: np.ndarray) -> np.ndarray:
    out = np.zeros_like(raw, dtype=np.uint8)
    for k, v in LEARNING_MAP.items():
        out[raw == k] = v
    return out


def derive_boxes(xyz: np.ndarray, cls: np.ndarray, inst: np.ndarray) -> list[dict]:
    boxes = []
    things = np.isin(cls, list(THING_CLASSES))
    if not things.any():
        return boxes
    keys = inst.astype(np.int64) * 100 + cls.astype(np.int64)
    for key in np.unique(keys[things]):
        m = (keys == key) & things
        if m.sum() < 10:
            continue
        pts = xyz[m]
        lo, hi = pts.min(0), pts.max(0)
        center = ((lo + hi) / 2).tolist()
        size = (hi - lo).tolist()
        boxes.append({"cls": int(cls[m][0]), "center": center, "size": size})
    return boxes


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo-id", default=REPO_ID)
    ap.add_argument("--seq", default="08")
    ap.add_argument("--start", type=int, default=0)
    ap.add_argument("--limit", type=int, default=150)
    ap.add_argument("--velodyne-dir")
    ap.add_argument("--label-dir")
    ap.add_argument("--out", default="/tmp/kitti-seg")
    ap.add_argument("--no-upload", action="store_true")
    args = ap.parse_args()

    root = os.environ.get("SEMANTIC_KITTI_DIR", "")
    velo_dir = Path(args.velodyne_dir or f"{root}/{args.seq}/velodyne")
    label_dir = Path(args.label_dir or f"{root}/{args.seq}/labels")
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    bins = sorted(velo_dir.glob("*.bin"))[args.start : args.start + args.limit]
    if not bins:
        raise SystemExit(f"no .bin frames under {velo_dir}")
    print(f"{len(bins)} frames from {velo_dir}")

    boxes_all = {}
    for i, binpath in enumerate(bins):
        raw = np.fromfile(binpath, dtype=np.float32).reshape(-1, 4)
        xyz = raw[:, :3]
        lab = np.fromfile(label_dir / f"{binpath.stem}.label", dtype=np.uint32)
        cls = remap_classes(lab & 0xFFFF)
        inst = (lab >> 16).astype(np.uint32)
        idx = downsample_idx_to_target(xyz.copy(), TARGET_POINTS)
        xyz_d, cls_d, inst_d = xyz[idx], cls[idx], inst[idx]
        colors = np.zeros((len(xyz_d), 3), dtype=np.uint8)
        colors[:, 0] = cls_d
        buf = DracoPy.encode(xyz_d.astype(np.float32), colors=colors, quantization_bits=QUANT_BITS)
        (out / f"{i:06d}.drc").write_bytes(buf)
        boxes_all[f"{i:06d}"] = derive_boxes(xyz_d, cls_d, inst_d)
        if i % 20 == 0:
            print(f"  frame {i}: {len(xyz_d)} pts, {len(boxes_all[f'{i:06d}'])} boxes -> {len(buf)} bytes")

    (out / "boxes.json").write_text(json.dumps(boxes_all))
    (out / "annotations.md").write_text(
        ANNOTATIONS.format(
            target=TARGET_POINTS, bits=QUANT_BITS, seq=args.seq,
            start=args.start, last=args.start + len(bins) - 1,
        )
    )
    print(f"wrote {len(bins)} frames + boxes.json to {out}")

    if args.no_upload:
        return
    api = HfApi(token=os.environ["HF_TOKEN"])
    api.create_repo(args.repo_id, repo_type="dataset", exist_ok=True, private=False)
    api.upload_folder(folder_path=str(out), path_in_repo="seg", repo_id=args.repo_id, repo_type="dataset")
    api.upload_file(path_or_fileobj=CARD.encode(), path_in_repo="README.md",
                    repo_id=args.repo_id, repo_type="dataset")
    print(f"uploaded seg/ ({len(bins)} frames) to https://huggingface.co/datasets/{args.repo_id}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Syntax-check**

Run: `uv run --group gen python -c "import ast; ast.parse(open('scripts/build_seg_dataset.py').read()); print('ok')"`
Expected: `ok`.

- [ ] **Step 3: Commit**

```bash
git add pcl-viewer/scripts/build_seg_dataset.py
git commit -m "feat(pcl-viewer): one-shot SemanticKITTI seg dataset builder"
```

> **Data acquisition note (manual, outside this plan):** running the builder for
> real needs the SemanticKITTI velodyne + label files for the chosen sequence
> (`SEMANTIC_KITTI_DIR` pointing at a `dataset/sequences` tree). These are large
> (KITTI Odometry velodyne ~80 GB; SemanticKITTI labels ~3 GB). The actual
> download + upload is run by the maintainer with `HF_TOKEN`; this plan stops at a
> committed, syntax-valid builder + offline-tested viewer.

---

## Task 8: Docs

**Files:**
- Modify: `pcl-viewer/README.md`, `pcl-viewer/SPEC.md`

- [ ] **Step 1: README — add the seg scene to the description**

In `README.md`, extend the scene description paragraph to mention the new **KITTI seg** scene (a SemanticKITTI movie colored per-point by class with 3D bounding boxes per object). Keep it to the existing one-paragraph style.

- [ ] **Step 2: SPEC — document palette, encoding, boxes, scene table**

In `SPEC.md`:
- Add a **kitti-seg** row to the Scenes table: source `kolodkin/pcl-viewer-kitti-movie` `seg/`, transport HF resolve, Draco `.drc` + `boxes.json`.
- Add a short section: class id packed in the Draco color attribute (red channel), the 19-class learning palette, "by class" mode, axis-aligned boxes derived from instance ids (thing classes 1–8), and box normalization matching the point transform.
- Note the shared dataset now has `geometry/` and `seg/` folders and a single card.

- [ ] **Step 3: Commit**

```bash
git add pcl-viewer/README.md pcl-viewer/SPEC.md
git commit -m "docs(pcl-viewer): document the seg scene and shared dataset layout"
```

---

## Post-implementation (maintainer, manual)

1. Run `build_seg_dataset.py` with `HF_TOKEN` + SemanticKITTI data to populate `seg/`.
2. Verify the deployed viewer's `seg` scene loads from HF.
3. Once `geometry/` is confirmed live, run `scripts/restructure_dataset.py --delete-root` to drop the root frames.

---

## Self-Review

- **Spec coverage:** shared dataset `geometry/`+`seg/` (Task 7/8 + already done), per-point class via Draco color (Tasks 2,3,7 — empirically verified), boxes from instance ids (Task 4,7), "by class" mode + legend + toggle (Task 5), config overrides for e2e (Task 1), offline fixtures + e2e (Tasks 2,6), licensing/card/annotations (Task 7), axis-aligned v1 / no instance coloring / no editor (out of scope, honored). ✓
- **Placeholder scan:** every code step has complete code; the one prose-only step is Task 4 Step 3's `movie.onFrame` placement, which points at exact lines to read — acceptable because it depends on reading the existing `play`/timer block. No TBD/TODO.
- **Type consistency:** `setShowBoxes`, `boxCount`, `settings.showBoxes`, `settings.colorMode === 'class'`, `buffers.class`, `segFrameUrl`, `SEG_BOXES_URL`, `SEG_MOVIE_COUNT` used identically across viewer/app/config/tests. boxes.json schema (`cls`/`center`/`size`) identical in fixtures builder, real builder, and `buildBoxLines`. ✓
