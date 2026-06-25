# PCL Viewer — Technical Notes

## ESM, no build
Frontend uses native `import` with an import map in `index.html`. Bare specifiers
(`three`, `three/addons/`, `preact`, `preact/hooks`, `htm`) resolve to files under
`web/vendor/`, populated by `vendor.sh` from pinned unpkg builds:
three 0.160.0, preact 10.19.3, htm 3.1.1. `web/vendor/` is gitignored and
regenerated on demand, so e2e runs offline (the browser fetches libs from the local
Python server, never a CDN at test time). Preact renders via `htm` — no JSX/transpile.

## Component boundaries
- `viewer.js` — `createViewer(canvas)` returns an imperative handle
  (`loadScene`, `play`, `pause`, `setPointSize`, `setColorMode`,
  `setPointShape`, `resetCamera`, `getStats`, `visiblePixelCount`, `dispose`).
  Owns all three.js state. No Preact.
- `app.js` — pure Preact UI; owns control state and drives the handle. No three.js
  internals.
- `serve.py` — `ThreadingHTTPServer` serving `web/` with JS/`.pcd` MIME types and
  `Cache-Control: no-store`.

## Sample asset
`web/models/kitti-velodyne-000000.pcd` — frame `000000` of the KITTI raw Velodyne
data: a single 360° street-level LiDAR scan (binary PCD, `FIELDS x y z intensity`,
115,385 points, ~1.8 MB). It is a *city viewpoint* cloud — one sensor sweep showing
the road as concentric scan rings with parked cars, walls, poles and vegetation
rising out of it. It comes from the KITTI dataset and is licensed
**CC BY-NC-SA 3.0** — *not* permissively licensed like the rest of the project; see
`web/models/ATTRIBUTION.md` for the source, license terms, and citation.

KITTI uses a z-up vehicle frame in metres, so on load the viewer reorients it
(z-up → three.js y-up), centers it, and scales by a **robust** horizontal radius
(90th percentile of distance from the sensor, not the absolute max) so the dense
scene fills the view instead of being shrunk by a few 80 m stray returns. The
**default** "by distance" mode colors per-vertex by radial range from the sensor
(blue → red), with the range clamped to the 2nd–98th percentile so the near rings
read blue and the far returns climb through to red, lighting up the concentric scan
rings. The same ramp (and the same robust percentile clamp) also drives "by height"
(along the vertical axis, so ground reads blue and cars/walls climb to red) and "by
intensity" (the PCD's per-point laser reflectance, which picks out road markings and
signs); "flat" mode (a single material color) is a toggle. The ramp buffers are precomputed once per cloud and swapped on the geometry,
and a scalar mode the source lacks (e.g. an intensity-free PCD) falls back to flat.
The camera sits low and forward-facing — just above the sensor's forward (+X) axis,
looking down the road — so the scan reads like an onboard driving view: ground
rings sweep to the horizon and cars/walls/poles stand up along the street.

## Point shape (ball vs. square)
Points render as lit sphere impostors by default, with the older flat square
sprite selectable via the **Point shape** control. `PointsMaterial.onBeforeCompile`
injects a snippet after `<color_fragment>`, gated on a `uBall` uniform (1 = ball,
0 = square): when on, it clips each point quad to a circle (`discard` outside the
unit disc), reconstructs a hemisphere normal from `gl_PointCoord`, and shades it
with ambient + diffuse + a tight specular highlight, so every point reads as a
tiny 3D ball; when off, the stock square sprite is left untouched. The light is
fixed in **view space**, so highlights stay put as the cloud orbits. The toggle
flips the uniform at runtime — no shader recompile. Going through `onBeforeCompile`
(rather than a bespoke `ShaderMaterial`) keeps the size slider, `sizeAttenuation`,
and per-vertex color ramps working unchanged — the shading multiplies into
`diffuseColor`, whatever its source.

## Controls
| Control       | Effect                                              |
|---------------|-----------------------------------------------------|
| Point shape   | lit sphere impostor ("ball", default) vs. flat square sprite |
| Point size    | `PointsMaterial.size` (0.002–0.05)                  |
| Color mode    | flat vs. per-vertex ramp by height / distance / intensity, or palette by class (seg scene) |
| Movie (movie + seg scenes) | play/pause, frame step/seek (the sequence loops continuously) |
| Show boxes (seg scene only) | toggle the per-instance 3D bounding boxes |
| Reset camera  | re-frames the low forward-facing view down the road  |
| Stats overlay | point count, rolling FPS, camera distance, box count |

## e2e strategy
`conftest.py` ensures vendoring, then starts `serve.py` on a free port per test.
`test_e2e.py` (pytest-playwright, Chromium) waits on `window.__PCL.ready`, asserts
the point count and that non-background pixels were drawn
(`__PCL.handle.visiblePixelCount()` via `gl.readPixels`, enabled by the renderer's
`preserveDrawingBuffer`), exercises each control through the `window.__PCL.settings`
hook, and captures a screenshot (compatible with `/e2e-screenshots-report`).

## Scenes

| Scene            | Source                                                | Transport                       |
|------------------|-------------------------------------------------------|---------------------------------|
| KITTI city view  | `web/models/kitti-velodyne-000000.pcd` (committed)    | same-origin PCD                 |
| PCL table scene  | `PointCloudLibrary/data` `table_scene_lms400.pcd`     | raw.githubusercontent (CORS)    |
| KITTI movie      | `kolodkin/pcl-viewer-kitti-movie` `geometry/` (HF)    | HF resolve (CORS), Draco `.drc` |
| KITTI seg        | `kolodkin/pcl-viewer-kitti-movie` `seg/` (HF)         | HF resolve (CORS), Draco `.drc` + `boxes.json` |

The shared HF dataset holds both movies under sibling folders — `geometry/`
(positions-only, from KITTI raw drive 0005) and `seg/` (SemanticKITTI, with
per-point classes + `boxes.json`) — under one CC BY-NC-SA card.

`web/config.js` holds the scene URLs and frame counts, each overridable via
`?pclUrl=`, `?movieBase=`, `?movieCount=`, `?segMovieBase=`, `?segMovieCount=`,
`?segBoxesUrl=` (used by e2e to point at local fixtures). `viewer.js` exposes `loadScene(id)` — `loadStatic` (PCDLoader) for
city/table, `loadMovie` (DRACOLoader) for the movie. The movie **streams**:
frame 0 is decoded first (it defines the shared normalization transform every
other frame reuses, so points don't pulse), then playback starts immediately
while the remaining frames decode through a **bounded-concurrency worker queue**
(`MOVIE_DECODE_CONCURRENCY = 4` in flight — DRACOLoader's WASM worker does the
decode, the queue just keeps several requests outstanding instead of one). It
plays at 15 fps and loops continuously, with play/pause; if the playhead reaches
a frame the queue hasn't decoded yet it **holds** the current frame (no skip)
rather than stalling. The Draco WASM decoder is vendored into `web/vendor/draco/`
by `vendor.sh` and preloaded at startup, so playback and the offline e2e need no
CDN.

### Movie pipeline (`scripts/build_movie_dataset.py`, one-shot)

KITTI raw drive `2011_09_26_drive_0005` → per-frame Velodyne `.bin` →
voxel-downsample to ~30k points (positions only; the viewer colors by
height/flat, so reflectance is dropped) → Draco encode (14-bit position
quantization, ~73 KB/frame, 154 frames) → upload to the HF dataset with a dataset
card (`README.md`) and an `annotations.md` noting the frames are **geometry only**
(positions, no KITTI object/semantic labels). Frame data is **not** in git; the
browser fetches `.drc` at runtime. Tiny
committed fixtures (`tests/fixtures/movie/*.drc`, built by
`tests/fixtures/build_fixtures.py`) drive the offline movie e2e — conftest stages
them into `web/fixtures/`.

### Seg scene (`loadSegMovie` + `scripts/build_seg_dataset.py`, one-shot)

The seg scene reuses the streaming `loadMovie` path (a `urlFn`/`onFrame` options
pair) but adds per-point **classes** and per-frame **3D boxes**:

- **Class encoding.** Draco can't carry a side array (it may reorder/dedup points
  on decode), so each point's **19-class learning id** is packed into the Draco
  **color attribute's red channel** (`colors[:,0] = class_id`). It rides glued to
  its point through decode; three.js exposes a normalized `color` attribute and
  the viewer recovers `id = round(color.r * 255)`. `computeColorBuffers` maps each
  id through `SEG_PALETTE` (the SemanticKITTI 19-class colors) into a `class`
  buffer; "By class" is just another `applyColorMode` entry, so clouds without the
  attribute fall back to flat. `loadSegMovie` defaults the mode to `class`.
- **Boxes.** `boxes.json` (`{ "NNNNNN": [ {cls, center, size} ] }`, fetched once)
  holds one **axis-aligned** box per thing instance (learning classes 1–8),
  derived at build time from the SemanticKITTI instance ids (high 16 bits of the
  `.label`). `onFrame` rebuilds a `LineSegments` box group each time the displayed
  frame changes, transforming each box through the **same** rotate→translate→scale
  normalization applied to the points (`buildBoxLines`), colored by class. A
  **Show boxes** toggle flips `boxGroup.visible`.
- **Pipeline.** `build_seg_dataset.py` runs in three selectable stages
  (`--download` / `--process` / `--upload`; no flag = all three). **download**
  streams the SemanticKITTI archive (a split `tar.zst`) and extracts matched
  velodyne `.bin` + `.label` pairs for one sequence's first N frames — the two
  live in separate, randomly-ordered regions, so it keeps streaming until it holds
  all N of both (~6 GB for 150 frames). **process** remaps to learning ids → joint
  voxel-downsample to ~30k (class carried, not averaged) → derive boxes → Draco
  encode (class in color, 14-bit positions) → write `boxes.json`. **upload** pushes
  `seg/` to HF. The live scene is sequence **00**, frames 0–149. Offline fixtures
  (`tests/fixtures/seg/`, built by `build_seg_fixtures.py`) drive the seg e2e.

### Licensing

KITTI / SemanticKITTI are **CC BY-NC-SA 3.0**. The committed city frame and both
derived movies (`geometry/` and `seg/`) retain that license with attribution
(Geiger et al., IJRR 2013 / CVPR 2012; Behley et al., ICCV 2019 for the seg
labels); the single HF dataset card declares `license: cc-by-nc-sa-3.0` and
carries the citations per the BY + SA terms. The PCL table scene is
**BSD-3-Clause** (PointCloudLibrary).

## Run
- Viewer: `./pcl-viewer.sh` (set `PORT` to override 8000).
- Tests: `uv run --group dev playwright install chromium` once, then
  `uv run --group dev pytest`.
- Regenerate the movie dataset (one-shot, needs `HF_TOKEN` with write on the
  dataset): `uv run --group gen python scripts/build_movie_dataset.py`.
- Regenerate the seg dataset (needs `HF_TOKEN`): `uv run --group gen python
  scripts/build_seg_dataset.py --seq 00 --limit 150` downloads, processes, and
  uploads. Use `--download` / `--process` / `--upload` to run a single stage, or
  set `SEMANTIC_KITTI_DIR` to a local `dataset/sequences` tree to skip the
  download and `--process` straight from disk.
