# KITTI Movie Intensity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Carry per-point LiDAR intensity through both KITTI movie datasets (`geometry/` and `seg/`) and surface "By intensity" as a color mode for those scenes in the viewer.

**Architecture:** Intensity (the velodyne `.bin` 4th float column, ~0–1) is quantized to a `uint8` and packed into the Draco color attribute's **green** channel — the same mechanism that already smuggles the seg class id in the **red** channel. The viewer reads it via an explicit per-scene channel map and builds an intensity ramp buffer, so the movie/seg scenes offer the mode the static city PCD already has.

**Tech Stack:** Python (`numpy`, `DracoPy`, `huggingface_hub`) for the build scripts + fixtures; vanilla ES-module three.js (`web/viewer.js`) for the viewer; Python Playwright for e2e.

**Conventions used everywhere below:**
- Draco color attribute channel layout — R (0) = class id (seg only; 0 for geometry), G (1) = intensity (0–255).
- Quantization: `np.clip(intensity * 255.0, 0, 255).astype(np.uint8)`.
- All commands run from `pcl-viewer/`. Python-with-deps commands use `uv run --group gen`.

---

### Task 1: Geometry build script encodes intensity

**Files:**
- Modify: `pcl-viewer/scripts/build_movie_dataset.py`
- Test: `pcl-viewer/tests/test_build_movie.py` (create)

- [ ] **Step 1: Write the failing test**

Create `pcl-viewer/tests/test_build_movie.py`:

```python
"""Offline round-trip test for the geometry movie encoder: intensity must survive
into the Draco color attribute's green channel. Needs the `gen` deps; run with
`uv run --group gen pytest tests/test_build_movie.py`."""
import importlib.util
from pathlib import Path

import pytest

pytest.importorskip("DracoPy")
pytest.importorskip("requests")
pytest.importorskip("huggingface_hub")
import DracoPy  # noqa: E402
import numpy as np  # noqa: E402

HERE = Path(__file__).resolve().parent
PCD = HERE.parent / "web" / "models" / "kitti-velodyne-000000.pcd"


def _load_script():
    path = HERE.parent / "scripts" / "build_movie_dataset.py"
    spec = importlib.util.spec_from_file_location("build_movie_dataset", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _read_pcd(path):
    data = path.read_bytes()
    marker = b"DATA binary\n"
    end = data.index(marker) + len(marker)
    header = data[:end].decode("ascii", "replace")
    count = next(int(l.split()[1]) for l in header.splitlines() if l.startswith("POINTS"))
    return np.frombuffer(data[end:end + count * 16], dtype=np.float32).reshape(-1, 4)


def test_encode_frame_round_trips_intensity():
    mod = _load_script()
    raw = _read_pcd(PCD)
    idx = mod.downsample_idx_to_target(raw[:, :3].copy(), 3000)
    xyz, inten = raw[idx, :3], raw[idx, 3]
    buf = mod.encode_frame(xyz, inten)
    out = DracoPy.decode(buf)
    colors = np.asarray(out.colors)
    assert colors.shape[0] == len(idx)
    expected = np.clip(inten * 255.0, 0, 255).astype(np.uint8)
    # Green channel carries intensity; allow ±1 for Draco's color quantization.
    assert np.abs(colors[:, 1].astype(int) - expected.astype(int)).max() <= 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd pcl-viewer && uv run --group gen --group dev pytest tests/test_build_movie.py -v`
Expected: FAIL with `AttributeError: module 'build_movie_dataset' has no attribute 'downsample_idx_to_target'` (and `encode_frame`). (Both groups are needed: `gen` for DracoPy/numpy and the script's imports, `dev` for pytest.)

- [ ] **Step 3: Refactor the script to index-based downsampling + an `encode_frame` helper**

In `pcl-viewer/scripts/build_movie_dataset.py`, replace the two downsample helpers:

```python
def voxel_downsample_idx(pts: np.ndarray, voxel: float) -> np.ndarray:
    keys = np.floor(pts / voxel).astype(np.int64)
    _, idx = np.unique(keys, axis=0, return_index=True)
    return np.sort(idx)


def downsample_idx_to_target(pts: np.ndarray, target: int = 30000) -> np.ndarray:
    """Pick a voxel size landing near `target` points; return the kept indices."""
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


def encode_frame(xyz: np.ndarray, intensity: np.ndarray) -> bytes:
    """Draco-encode positions + per-point intensity in the color green channel."""
    colors = np.zeros((len(xyz), 3), dtype=np.uint8)
    colors[:, 1] = np.clip(intensity * 255.0, 0, 255).astype(np.uint8)
    return DracoPy.encode(xyz.astype(np.float32), colors=colors,
                          quantization_bits=QUANT_BITS)
```

(Delete the old `voxel_downsample` and `downsample_to_target` functions.)

Then change the per-frame loop in `main()` (the `for i, name in enumerate(bins):` block) from:

```python
        raw = np.frombuffer(zf.read(name), dtype=np.float32).reshape(-1, 4)
        xyz = downsample_to_target(raw[:, :3].copy(), TARGET_POINTS)
        buf = DracoPy.encode(xyz.astype(np.float32), quantization_bits=QUANT_BITS)
        (out / f"{i:06d}.drc").write_bytes(buf)
        count += 1
        if i % 20 == 0:
            print(f"  frame {i}: {len(xyz)} pts -> {len(buf)} bytes")
```

to:

```python
        raw = np.frombuffer(zf.read(name), dtype=np.float32).reshape(-1, 4)
        idx = downsample_idx_to_target(raw[:, :3].copy(), TARGET_POINTS)
        xyz, inten = raw[idx, :3], raw[idx, 3]
        buf = encode_frame(xyz, inten)
        (out / f"{i:06d}.drc").write_bytes(buf)
        count += 1
        if i % 20 == 0:
            print(f"  frame {i}: {len(xyz)} pts -> {len(buf)} bytes")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd pcl-viewer && uv run --group gen --group dev pytest tests/test_build_movie.py -v`
Expected: PASS.

- [ ] **Step 5: Update the dataset-card / annotations text in the same file**

In the `ANNOTATIONS` string, replace the "Format:" bullet:

```
- Format: Draco-encoded point **positions only** (`x y z`), {bits}-bit position
  quantization. No color, intensity, normals, or per-point labels.
```

with:

```
- Format: Draco-encoded point positions (`x y z`, {bits}-bit quantization) plus a
  per-point **intensity** (laser reflectance) packed into the Draco color
  attribute's green channel as a 0–255 byte. No normals or per-point labels.
```

In the `CARD` string, change:

```
**KITTI raw drive `2011_09_26_drive_0005`**, voxel-downsampled to ~30k points
(positions only) and Draco-encoded (14-bit position quantization).
```

to:

```
**KITTI raw drive `2011_09_26_drive_0005`**, voxel-downsampled to ~30k points
(positions + per-point intensity) and Draco-encoded (14-bit position quantization).
```

- [ ] **Step 6: Commit**

```bash
cd pcl-viewer
git add scripts/build_movie_dataset.py tests/test_build_movie.py
git commit -m "geometry movie: encode per-point intensity into Draco green channel"
```

---

### Task 2: Seg build script encodes intensity

**Files:**
- Modify: `pcl-viewer/scripts/build_seg_dataset.py`

- [ ] **Step 1: Carry intensity through `process()` into the green channel**

In `pcl-viewer/scripts/build_seg_dataset.py`, inside `process()`'s per-frame loop, change:

```python
        raw = np.fromfile(binpath, dtype=np.float32).reshape(-1, 4)
        xyz = raw[:, :3]
        lab = np.fromfile(label_dir / f"{binpath.stem}.label", dtype=np.uint32)
        cls = remap_classes(lab & 0xFFFF)
        inst = (lab >> 16).astype(np.uint32)
        idx = downsample_idx_to_target(xyz.copy(), TARGET_POINTS)
        xyz_d, cls_d, inst_d = xyz[idx], cls[idx], inst[idx]
        colors = np.zeros((len(xyz_d), 3), dtype=np.uint8)
        colors[:, 0] = cls_d
        buf = DracoPy.encode(xyz_d.astype(np.float32), colors=colors,
                             quantization_bits=QUANT_BITS)
```

to:

```python
        raw = np.fromfile(binpath, dtype=np.float32).reshape(-1, 4)
        xyz = raw[:, :3]
        intensity = raw[:, 3]
        lab = np.fromfile(label_dir / f"{binpath.stem}.label", dtype=np.uint32)
        cls = remap_classes(lab & 0xFFFF)
        inst = (lab >> 16).astype(np.uint32)
        idx = downsample_idx_to_target(xyz.copy(), TARGET_POINTS)
        xyz_d, cls_d, inst_d = xyz[idx], cls[idx], inst[idx]
        colors = np.zeros((len(xyz_d), 3), dtype=np.uint8)
        colors[:, 0] = cls_d
        colors[:, 1] = np.clip(intensity[idx] * 255.0, 0, 255).astype(np.uint8)
        buf = DracoPy.encode(xyz_d.astype(np.float32), colors=colors,
                             quantization_bits=QUANT_BITS)
```

- [ ] **Step 2: Update the card / annotations text in the same file**

In the `CARD` string, change:

```
- **`geometry/`** — positions-only sweeps from KITTI raw drive `2011_09_26_drive_0005`.
- **`seg/`** — SemanticKITTI sequence slice with a per-point **class id** packed in
  the Draco color attribute, plus `boxes.json` (one axis-aligned 3D box per thing
  instance per frame).
```

to:

```
- **`geometry/`** — sweeps from KITTI raw drive `2011_09_26_drive_0005`, positions
  plus per-point intensity (Draco color green channel).
- **`seg/`** — SemanticKITTI sequence slice with a per-point **class id** (Draco
  color red channel) and **intensity** (green channel), plus `boxes.json` (one
  axis-aligned 3D box per thing instance per frame).
```

In the `ANNOTATIONS` string, change:

```
Each `NNNNNN.drc` is a SemanticKITTI sweep, voxel-downsampled to ~{target:,}
points (positions only), Draco-encoded with the per-point **19-class learning id**
stored in the color attribute's red channel ({bits}-bit positions). `boxes.json`
```

to:

```
Each `NNNNNN.drc` is a SemanticKITTI sweep, voxel-downsampled to ~{target:,}
points, Draco-encoded ({bits}-bit positions) with the per-point **19-class
learning id** in the color attribute's red channel and per-point **intensity**
(laser reflectance, 0–255) in the green channel. `boxes.json`
```

- [ ] **Step 3: Sanity-check the file imports/parses**

Run: `cd pcl-viewer && uv run --group gen python -c "import importlib.util,pathlib; p=pathlib.Path('scripts/build_seg_dataset.py'); s=importlib.util.spec_from_file_location('b',p); m=importlib.util.module_from_spec(s); s.loader.exec_module(m); print('ok')"`
Expected: prints `ok`.

- [ ] **Step 4: Commit**

```bash
cd pcl-viewer
git add scripts/build_seg_dataset.py
git commit -m "seg movie: encode per-point intensity into Draco green channel"
```

---

### Task 3: Viewer reads intensity from the movie color attribute

**Files:**
- Modify: `pcl-viewer/web/viewer.js`

- [ ] **Step 1: Make `computeColorBuffers` take an explicit channel map**

Replace the whole `computeColorBuffers` function (currently the block starting `function computeColorBuffers(geometry) {` and ending at its closing `}` before `notifyColorState`) with:

```js
  // Precompute a ramp buffer for every scalar mode the cloud can supply: height
  // (y), radial distance from the sensor (origin), and intensity. Movie/seg Draco
  // frames pack scalars into the color attribute, so an explicit per-scene channel
  // map (`opts.classChannel`, `opts.intensityChannel`) says which channel carries
  // what; static clouds pass no map and fall back to native attributes.
  function computeColorBuffers(geometry, opts = {}) {
    const pos = geometry.getAttribute('position');
    const n = pos.count;
    const height = new Float32Array(n);
    const distance = new Float32Array(n);
    for (let i = 0; i < n; i++) {
      height[i] = pos.getY(i);
      distance[i] = Math.hypot(pos.getX(i), pos.getY(i), pos.getZ(i));
    }
    const buffers = { height: rampColors(height), distance: rampColors(distance) };
    const color = geometry.getAttribute('color');

    // Intensity: a movie channel if the scene maps one, else a native PCD field
    // (the city scene). Raw channel bytes are fine — rampColors clamps relatively.
    if (opts.intensityChannel != null && color) {
      const ch = opts.intensityChannel;
      const vals = Float32Array.from({ length: n }, (_, i) => color.getComponent(i, ch));
      buffers.intensity = rampColors(vals);
    } else {
      const intensity = geometry.getAttribute('intensity');
      if (intensity) {
        const vals = Float32Array.from({ length: n }, (_, i) => intensity.getX(i));
        buffers.intensity = rampColors(vals);
      }
    }

    // Class: the seg scene packs the per-point id (raw integer) in a color channel;
    // map each id through the fixed palette. DRACOLoader hands the color attribute
    // back as raw, un-normalized floats, so the channel value already IS the id.
    if (opts.classChannel != null && color) {
      const ch = opts.classChannel;
      const out = new Float32Array(n * 3);
      for (let i = 0; i < n; i++) {
        const id = Math.round(color.getComponent(i, ch));
        const c = SEG_PALETTE[id] || SEG_PALETTE[0];
        out[i * 3] = c.r; out[i * 3 + 1] = c.g; out[i * 3 + 2] = c.b;
      }
      buffers.class = out;
    }
    return buffers;
  }
```

- [ ] **Step 2: Thread the channel map through `decodeFrame`**

Change the `decodeFrame` signature and its `computeColorBuffers` call from:

```js
  async function decodeFrame(i, shared, token, urlFn) {
    const geom = await dracoLoad(urlFn(i));
    if (token !== loadToken) { geom.dispose(); return null; }
    normalizeMovieFrame(geom, shared);
    return { geometry: geom, buffers: computeColorBuffers(geom) };
  }
```

to:

```js
  async function decodeFrame(i, shared, token, urlFn, colorChannels) {
    const geom = await dracoLoad(urlFn(i));
    if (token !== loadToken) { geom.dispose(); return null; }
    normalizeMovieFrame(geom, shared);
    return { geometry: geom, buffers: computeColorBuffers(geom, colorChannels) };
  }
```

- [ ] **Step 3: Pass `opts.colorChannels` from `loadMovie` into both `decodeFrame` calls**

In `loadMovie`, change the frame-0 decode:

```js
      first = await decodeFrame(0, shared, token, urlFn);
```

to:

```js
      first = await decodeFrame(0, shared, token, urlFn, opts.colorChannels);
```

and the worker-queue decode:

```js
          slot = await decodeFrame(i, shared, token, urlFn);
```

to:

```js
          slot = await decodeFrame(i, shared, token, urlFn, opts.colorChannels);
```

- [ ] **Step 4: Set the channel map at the two movie call sites**

In `loadScene`, change the geometry-movie branch:

```js
    else if (id === 'movie') await loadMovie(MOVIE_COUNT);
```

to:

```js
    else if (id === 'movie') await loadMovie(MOVIE_COUNT, { colorChannels: { intensityChannel: 1 } });
```

In `loadSegMovie`, change:

```js
    await loadMovie(count, {
      urlFn: segFrameUrl,
      onFrame: (i, shared) => updateBoxes(i, shared),
    });
```

to:

```js
    await loadMovie(count, {
      urlFn: segFrameUrl,
      onFrame: (i, shared) => updateBoxes(i, shared),
      colorChannels: { classChannel: 0, intensityChannel: 1 },
    });
```

- [ ] **Step 5: Commit**

```bash
cd pcl-viewer
git add web/viewer.js
git commit -m "viewer: read movie/seg intensity from the Draco color green channel"
```

(The viewer is verified by the e2e suite in Task 6; fixtures must carry intensity first — Tasks 4 and 5.)

---

### Task 4: Regenerate the geometry movie fixture with intensity

**Files:**
- Modify: `pcl-viewer/tests/fixtures/build_fixtures.py`
- Regenerate (binary, committed): `pcl-viewer/tests/fixtures/movie/000000.drc` … `000003.drc`

- [ ] **Step 1: Update the fixture builder to carry intensity**

In `pcl-viewer/tests/fixtures/build_fixtures.py`, replace `read_binary_pcd_xyz` and `voxel_downsample` with index-aware versions:

```python
def read_binary_pcd(path: Path) -> np.ndarray:
    """Minimal binary-PCD reader for FIELDS x y z intensity (float32) -> (N,4)."""
    data = path.read_bytes()
    marker = b"DATA binary\n"
    header_end = data.index(marker) + len(marker)
    header = data[:header_end].decode("ascii", "replace")
    count = next(int(line.split()[1]) for line in header.splitlines() if line.startswith("POINTS"))
    return np.frombuffer(data[header_end:header_end + count * 16], dtype=np.float32).reshape(-1, 4)


def voxel_downsample_idx(pts: np.ndarray, voxel: float) -> np.ndarray:
    keys = np.floor(pts / voxel).astype(np.int64)
    _, idx = np.unique(keys, axis=0, return_index=True)
    return np.sort(idx)
```

Then replace the movie-building part of `main()`:

```python
    OUT.mkdir(parents=True, exist_ok=True)
    xyz = read_binary_pcd_xyz(SRC)
    base = voxel_downsample(xyz, 0.8)  # ~a few thousand points -> tiny .drc
    for i in range(4):
        frame = base + np.array([i * 0.5, 0.0, 0.0], dtype=np.float32)  # roll forward
        buf = DracoPy.encode(frame.astype(np.float32), quantization_bits=14)
        (OUT / f"{i:06d}.drc").write_bytes(buf)
        print(f"wrote {OUT / f'{i:06d}.drc'}  ({len(buf)} bytes, {len(frame)} pts)")
    build_lucy_fixture()
```

with:

```python
    OUT.mkdir(parents=True, exist_ok=True)
    data = read_binary_pcd(SRC)                 # (N,4): xyz + intensity
    idx = voxel_downsample_idx(data[:, :3], 0.8)  # ~a few thousand points -> tiny .drc
    base = data[idx]
    for i in range(4):
        frame = base.copy()
        frame[:, 0] += i * 0.5                   # roll forward in x
        colors = np.zeros((len(frame), 3), dtype=np.uint8)
        colors[:, 1] = np.clip(frame[:, 3] * 255.0, 0, 255).astype(np.uint8)
        buf = DracoPy.encode(frame[:, :3].astype(np.float32), colors=colors,
                             quantization_bits=14)
        (OUT / f"{i:06d}.drc").write_bytes(buf)
        print(f"wrote {OUT / f'{i:06d}.drc'}  ({len(buf)} bytes, {len(frame)} pts)")
    build_lucy_fixture()
```

- [ ] **Step 2: Regenerate the committed fixtures**

Run: `cd pcl-viewer && uv run --group gen python tests/fixtures/build_fixtures.py`
Expected: prints four `wrote .../movie/00000N.drc` lines (and the Lucy line).

- [ ] **Step 3: Verify the green channel now carries intensity**

Run:
```bash
cd pcl-viewer && uv run --group gen python -c "
import DracoPy, numpy as np
m = DracoPy.decode(open('tests/fixtures/movie/000000.drc','rb').read())
c = np.asarray(m.colors)
print('has_color', c.shape, 'green_nonzero', int((c[:,1]>0).sum()), 'green_max', int(c[:,1].max()))
"
```
Expected: `has_color (M, 3)`, `green_nonzero` > 0, `green_max` > 0.

- [ ] **Step 4: Commit**

```bash
cd pcl-viewer
git add tests/fixtures/build_fixtures.py tests/fixtures/movie/000000.drc \
        tests/fixtures/movie/000001.drc tests/fixtures/movie/000002.drc \
        tests/fixtures/movie/000003.drc
git commit -m "test fixtures: geometry movie frames carry real intensity"
```

---

### Task 5: Inject synthetic intensity into the seg fixture

The committed seg fixtures are normally sliced from a ~6 GB processed dataset that
is not available offline, so we re-encode the existing committed `.drc` frames in
place, adding a deterministic synthetic intensity (derived from radial distance) to
the green channel while preserving the class id in the red channel. Real intensity
arrives when the user re-runs `build_seg_dataset.py --process`.

**Files:**
- Modify: `pcl-viewer/tests/fixtures/build_seg_fixtures.py`
- Regenerate (binary, committed): `pcl-viewer/tests/fixtures/seg/000000.drc` … `000003.drc`

- [ ] **Step 1: Add an `--inject-intensity` mode to the seg fixture builder**

In `pcl-viewer/tests/fixtures/build_seg_fixtures.py`, add these imports near the top (the file currently imports `argparse, json, shutil`):

```python
import numpy as np
import DracoPy
```

Add this function above `main()`:

```python
def inject_intensity(out: Path) -> None:
    """Re-encode each committed seg .drc in place, adding a deterministic synthetic
    intensity (normalized radial distance) to the color green channel. The class id
    in the red channel is preserved. Use offline when the real ~6 GB processed
    source isn't available; real intensity comes from build_seg_dataset.py --process."""
    for drc in sorted(out.glob("*.drc")):
        m = DracoPy.decode(drc.read_bytes())
        pts = np.asarray(m.points, dtype=np.float32)
        colors = np.asarray(m.colors, dtype=np.uint8).copy()
        d = np.linalg.norm(pts, axis=1)
        span = (d.max() - d.min()) or 1.0
        colors[:, 1] = np.clip((d - d.min()) / span * 255.0, 0, 255).astype(np.uint8)
        buf = DracoPy.encode(pts, colors=colors, quantization_bits=14)
        drc.write_bytes(buf)
        print(f"injected intensity -> {drc} ({len(buf)} bytes, "
              f"green_max={int(colors[:,1].max())})")
```

Add an `--inject-intensity` flag and branch in `main()`. Change the argument parsing block:

```python
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", default="/tmp/kitti-seg",
                    help="processed seg dataset dir (build_seg_dataset.py --out)")
    ap.add_argument("--frames", type=int, default=4, help="how many frames to keep")
    args = ap.parse_args()

    src = Path(args.src)
```

to:

```python
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", default="/tmp/kitti-seg",
                    help="processed seg dataset dir (build_seg_dataset.py --out)")
    ap.add_argument("--frames", type=int, default=4, help="how many frames to keep")
    ap.add_argument("--inject-intensity", action="store_true",
                    help="re-encode the committed fixtures in place with synthetic "
                         "intensity (offline; no processed source needed)")
    args = ap.parse_args()

    if args.inject_intensity:
        inject_intensity(OUT)
        return

    src = Path(args.src)
```

- [ ] **Step 2: Run the injection over the committed fixtures**

Run: `cd pcl-viewer && uv run --group gen python tests/fixtures/build_seg_fixtures.py --inject-intensity`
Expected: four `injected intensity -> .../seg/00000N.drc` lines, each with `green_max` > 0.

- [ ] **Step 3: Verify class (red) preserved and intensity (green) present**

Run:
```bash
cd pcl-viewer && uv run --group gen python -c "
import DracoPy, numpy as np
m = DracoPy.decode(open('tests/fixtures/seg/000000.drc','rb').read())
c = np.asarray(m.colors)
print('red_unique', np.unique(c[:,0])[:8], 'green_nonzero', int((c[:,1]>0).sum()), 'green_max', int(c[:,1].max()))
"
```
Expected: `red_unique` shows several distinct class ids (not just `[0]`); `green_nonzero` > 0; `green_max` > 0.

- [ ] **Step 4: Commit**

```bash
cd pcl-viewer
git add tests/fixtures/build_seg_fixtures.py tests/fixtures/seg/000000.drc \
        tests/fixtures/seg/000001.drc tests/fixtures/seg/000002.drc \
        tests/fixtures/seg/000003.drc
git commit -m "test fixtures: seg movie frames carry synthetic intensity (offline)"
```

---

### Task 6: Update e2e assertions and run the suite

**Files:**
- Modify: `pcl-viewer/tests/test_e2e.py`

- [ ] **Step 1: Update the seg color-mode expectation and add a movie-scene check**

In `pcl-viewer/tests/test_e2e.py`, in `test_color_modes_match_scene`, change the seg block:

```python
    # The seg Draco frames carry per-point classes but no intensity, so the set
    # flips: "class" appears, "intensity" drops.
    page.get_by_test_id("scene").select_option("seg")
    page.wait_for_function(
        "() => window.__PCL.scene === 'seg' && window.__PCL.ready === true"
        " && window.__PCL.frameCount === 4",
        timeout=60000,
    )
    page.wait_for_function(
        "() => JSON.stringify(window.__PCL.handle.getStats().colorModes)"
        " === JSON.stringify(['flat','class','height','distance'])")
    assert _color_mode_options(page) == ["flat", "class", "height", "distance"]
```

to:

```python
    # The seg Draco frames carry per-point classes AND intensity, so both "class"
    # and "intensity" join the scalar ramps.
    page.get_by_test_id("scene").select_option("seg")
    page.wait_for_function(
        "() => window.__PCL.scene === 'seg' && window.__PCL.ready === true"
        " && window.__PCL.frameCount === 4",
        timeout=60000,
    )
    page.wait_for_function(
        "() => JSON.stringify(window.__PCL.handle.getStats().colorModes)"
        " === JSON.stringify(['flat','class','height','distance','intensity'])")
    assert _color_mode_options(page) == ["flat", "class", "height", "distance", "intensity"]
```

Then, immediately after that seg block and before the "Switching back to a scene
without class" comment, insert a geometry-movie check:

```python
    # The geometry movie now carries intensity too (positions + green-channel
    # reflectance), so it offers the intensity ramp but no class.
    page.get_by_test_id("scene").select_option("movie")
    page.wait_for_function(
        "() => window.__PCL.scene === 'movie' && window.__PCL.ready === true",
        timeout=60000,
    )
    page.wait_for_function(
        "() => JSON.stringify(window.__PCL.handle.getStats().colorModes)"
        " === JSON.stringify(['flat','height','distance','intensity'])")
    assert _color_mode_options(page) == ["flat", "height", "distance", "intensity"]
```

- [ ] **Step 2: Run the full e2e suite**

Run: `cd pcl-viewer && uv run --group gen --group dev pytest -v`
Expected: all tests pass, including `test_color_modes_match_scene`, `test_color_mode_toggle`, and `test_build_movie.py` (the `gen` group makes the build test run rather than skip). The `dev` group supplies pytest + pytest-playwright; Chromium is preinstalled in this environment. The `conftest.py` session fixtures vendor the JS libs and stage `tests/fixtures/` into `web/fixtures/` automatically.

- [ ] **Step 3: Commit**

```bash
cd pcl-viewer
git add tests/test_e2e.py
git commit -m "e2e: assert movie/seg scenes offer the intensity color mode"
```

---

### Task 7: Update SPEC.md

**Files:**
- Modify: `pcl-viewer/SPEC.md`

- [ ] **Step 1: Update the offered-modes summary**

In `pcl-viewer/SPEC.md`, change the line:

```
order). The result per scene: **city** flat/height/distance/intensity, **Lucy** and
**movie** flat/height/distance, **seg** flat/class/height/distance.
```

to:

```
order). The result per scene: **city** and **movie** flat/height/distance/intensity,
**Lucy** flat/height/distance, **seg** flat/class/height/distance/intensity.
```

Also update the nearby `computeColorBuffers` description line:

```
cloud carries — every cloud gets `height` and `distance`, the city PCD adds
`intensity` (its `intensity` field), and the seg Draco frames add `class` (the
per-point id smuggled in the color attribute).
```

to:

```
cloud carries — every cloud gets `height` and `distance`, the city PCD adds
`intensity` from its `intensity` field, the movie/seg Draco frames add `intensity`
from the color attribute's green channel, and the seg frames add `class` (the
per-point id in the color attribute's red channel).
```

- [ ] **Step 2: Update the intensity fallback note**

In `pcl-viewer/SPEC.md`, change the line:

```
unchanged (Lucy carries no `intensity`, so that mode falls back to flat).
```

to:

```
unchanged (Lucy carries no `intensity`, so that mode falls back to flat; the
movie and seg scenes now do carry it).
```

- [ ] **Step 3: Verify no stale "positions only" / "no intensity" claims remain for the movies**

Run: `cd pcl-viewer && grep -n -i "positions only\|position-only\|no intensity\|intensity.*falls back\|flat/height/distance," SPEC.md`
Expected: no remaining line describes a movie/seg scene as positions-only or lacking intensity. (Lucy may still legitimately read `flat/height/distance`.)

- [ ] **Step 4: Commit**

```bash
cd pcl-viewer
git add SPEC.md
git commit -m "docs: SPEC reflects movie/seg intensity color mode"
```

---

## Notes for the implementer

- **What is NOT in this plan:** actually downloading KITTI / SemanticKITTI and
  uploading the rebuilt datasets to Hugging Face. That requires `HF_TOKEN` and
  multi-GB bandwidth and is run by the user with:
  - `uv run --group gen python scripts/build_movie_dataset.py --repo-id kolodkin/pcl-viewer-kitti-movie`
  - `uv run --group gen python scripts/build_seg_dataset.py --seq 00 --limit 150`
- **Why synthetic seg-fixture intensity (Task 5):** the real seg fixture source is
  the ~6 GB processed dataset, unavailable offline. The synthetic green channel
  exists only to exercise the by-intensity code path in the offline e2e; the
  shipped `seg/` dataset gets genuine intensity from Task 2's `--process` run.
- **Push at the end:** `git push -u origin claude/pcl-viewer-intensity-85xdd1`.
