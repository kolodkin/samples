# PCL Viewer — Technical Notes

## ESM, no build
Frontend uses native `import` with an import map in `index.html`. Bare specifiers
(`three`, `three/addons/`, `preact`, `preact/hooks`, `htm`, `hyparquet`) resolve to
files under `web/vendor/`, populated by `vendor.sh` from pinned builds:
three 0.160.0, preact 10.19.3, htm 3.1.1 (unpkg), and hyparquet 1.26.1 (the
self-contained esm.sh bundle — unpkg serves hyparquet as un-bundled `src/*.js`).
`web/vendor/` is gitignored and
regenerated on demand, so e2e runs offline (the browser fetches libs from the local
Python server, never a CDN at test time). Preact renders via `htm` — no JSX/transpile.

## Component boundaries
- `viewer.js` — `createViewer(canvas, { onColorState })` returns an imperative
  handle (`loadScene`, `loadFile`, `play`, `pause`, `setPointSize`, `setColorMode`,
  `setPointShape`, `setFilter`, `setClassHidden`, `resetFilters`, `resetCamera`,
  `getStats`, `visiblePixelCount`, `dispose`).
  Owns all three.js state. No Preact. The optional `onColorState` callback pushes
  color-mode changes back to the UI (see "Scene-dependent color modes").
- `loaders.js` — `parseLocalFile(file)` parses a user-supplied `.pcd`/`.csv`/
  `.parquet` into a plain `{positions, intensity, classIds, classNames}` (see
  "Loading local files"). No three.js scene state; uses `PCDLoader`/`hyparquet`
  purely as parsers.
- `app.js` — pure Preact UI; owns control state and drives the handle. No three.js
  internals.
- `colorModes.js` — shared, ordered `COLOR_MODES` list (`{id, label}`) imported by
  both `viewer.js` and `app.js`.
- `serve.py` — `ThreadingHTTPServer` serving `web/` with JS/`.pcd` MIME types and
  `Cache-Control: no-store`.

## Sample asset (movie-fixture source)
`web/models/kitti-velodyne-000000.pcd` — frame `000000` of the KITTI raw Velodyne
data: a single 360° street-level LiDAR scan (binary PCD, `FIELDS x y z intensity`,
115,385 points, ~1.8 MB). It comes from the KITTI dataset and is licensed
**CC BY-NC-SA 3.0** — *not* permissively licensed like the rest of the project; see
`web/models/ATTRIBUTION.md` for the source, license terms, and citation. It is no
longer a selectable scene; it is retained as the **source for the offline movie
fixtures** — `tests/fixtures/build_fixtures.py` decimates it into the tiny
`tests/fixtures/movie/*.drc` frames the e2e plays without a network.

The same z-up→y-up reorientation, robust-radius normalization, and percentile-clamped
ramp coloring it once demonstrated now drive the live KITTI scenes (movie + seg);
see "Per-scene normalization profiles" below.

## Scene-dependent color modes
The color-mode dropdown lists only the modes the live scene can actually supply,
rather than a fixed five. `web/colorModes.js` is the single source of truth — an
ordered `COLOR_MODES` list of `{id, label}`, imported by both the viewer (ids) and
the UI (labels). `computeColorBuffers` builds a ramp/class buffer for each field the
cloud carries — every cloud gets `height` and `distance`, the movie/seg Draco frames
add `intensity` from the color attribute's green channel, and the seg frames add
`class` (the per-point id in the color attribute's red channel). At **scene load**
(in `loadStatic` / `loadMovie`, not per movie frame) the offered set is derived once
as `flat` plus whichever buffers exist (`offeredModes` → `state.colorModes`, in
`COLOR_MODES` order). The result per scene: **movie**
flat/height/distance/intensity, **Lucy** flat/height/distance, **seg**
flat/class/height/distance/intensity. (`computeColorBuffers` also reads `intensity`
from a native PCD `intensity` field when a cloud carries one — the retired city PCD
was the last to use that path; no current scene does.)

**Intensity is histogram-equalized for display.** Raw LiDAR intensity is heavily
clumped (most returns dark, a sparse bright tail), so a plain linear ramp wastes
most of the color range on a narrow band. At scene load `computeColorBuffers`
precomputes a per-point equalized copy (`eq_i`) via the field's empirical CDF — each
value maps to the fraction of points at or below it (ties share the CDF at the top of
their run, keeping the mapping monotonic) — and the **colormap draws off `eq_i`**,
spreading the histogram evenly so faint structure (lane paint, signs) emerges. The raw
field is never mutated: `scalars.intensity` keeps the original values so the **range
filter** and the HUD's 0–255 hints stay in true intensity units. See
<https://en.wikipedia.org/wiki/Histogram_equalization>.

Intensity uses its own **"hot" (thermal) colormap** (`hotRampColors`) rather than the
shared blue→red `rampColors`: only the lowest returns read as cool **purple**, and the
bulk of the equalized range climbs magenta→red→orange→yellow→near-white, so the cloud
is mostly bright and reflective surfaces pop. The stops place purple in just the bottom
~15% of the range; height/distance keep the blue→red ramp.

Color state is **pushed** to the UI rather than polled: `createViewer` takes an
`onColorState({mode, modes})` callback that `applyColorMode` fires whenever the
applied mode or the offered set changes (guarded against the per-frame movie
re-installs, since `state.colorModes` is a stable array per scene). `app.js` holds
`colorMode`/`colorModes` as local state updated by that callback and renders exactly
those `<option>`s — so the dropdown follows the viewer immediately, including the
`applyColorMode` flat-fallback that backstops a *scene switch* stranding the current
mode (e.g. leaving seg's "by class" for the movie, which has no class buffer). Per-scene **default** modes
live in one `SCENE_DEFAULT_COLOR` map in the viewer (`{ seg: 'class' }`, applied in
`loadScene`); scenes not listed carry the current mode into the new scene.
`getStats()` also exposes `colorMode`/`colorModes` for e2e introspection.
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

## Point filters (range + class)
Two filter kinds clip which points draw, sharing one mechanism: a per-point
`aHide` vertex attribute (1 = filtered out) that the same `onBeforeCompile` shader
forwards to the fragment stage and `discard`s. The attribute defaults to 0 when
absent, so filtering **fails open** — a geometry without it shows every point.

- **Range filters** clip by a scalar field — **height**, **distance**, or
  **intensity** (offered only where the cloud supplies it, gated on the same
  offered-modes set as the color dropdown). **Distance** is the *absolute* radial
  distance from the sensor origin in the cloud's own (pre-normalization) units:
  the normalize step stashes it on the geometry as `aDistance` **before** the
  recenter+scale, so coring by distance acts on real metric distance rather than
  distance-from-the-bbox-center in normalized units (the height ramp stays in the
  normalized frame). Each is a `min`/`max` pair; a blank
  bound is unbounded (the default `null`/`null` passes everything, so **max = ∞**
  out of the box). Each bound carries **−/+ stepper buttons** sized to that field's
  data range (a "nice" 1/2/5×10^k step from `niceStep`, ≈5 for 0–255 intensity,
  ≈0.005 for the tiny normalized height span); from an empty bound the first click
  materializes a full-range handle that clips nothing, then steps inward, staying
  clamped to the step-aligned data envelope. `computeColorBuffers` now returns `{colors, scalars}` — the
  ramp colors as before, plus the raw per-point values the filter reads — and the
  data's live min/max per field (`state.scalarRanges`) is shown as input
  placeholders.
- **Class filters** make the class legend tickable: each swatch is a toggle button
  that adds/removes its class id from `state.settings.hiddenClasses`. Hidden classes
  drop out of the cloud; clicking again brings them back. The seg scene's ids map to
  `SEG_PALETTE` indices; a loaded file with a class column gets the same treatment,
  its ids being the enumeration index from its dynamic palette (both supply a
  `scalars.classId`, so one filter path serves both).

`applyFilter` recomputes `aHide` from the active filters whenever a setting changes
and on every geometry install (so each movie frame is re-filtered against the live
settings), tracking the survivor count in `state.visibleCount`. **Reset filters**
clears every range and un-hides all classes. Filters and hidden classes **persist**
across frames and scenes. `getStats()` exposes `visibleCount`, `scalarRanges`,
`filters`, and `hiddenClasses` for e2e introspection.

## Controls
| Control       | Effect                                              |
|---------------|-----------------------------------------------------|
| Point shape   | lit sphere impostor ("ball", default) vs. flat square sprite |
| Point size    | `PointsMaterial.size` (0.002–0.05)                  |
| Color mode    | flat vs. per-vertex ramp by height / distance (plus by-intensity where a source carries it), or palette by class (seg scene) |
| Point filters | clip the cloud by height / distance / intensity range (min/max, blank = ∞) |
| Classes (seg scene + loaded files with a class column) | click a legend swatch to filter that class in/out |
| Movie (movie + seg scenes) | play/pause, frame step/seek (the sequence loops continuously) |
| Show boxes (seg scene only) | toggle the per-instance 3D bounding boxes |
| Load PCL…     | load a local `.pcd`/`.csv`/`.parquet` cloud (see "Loading local files") |
| Reset camera  | re-frames the low forward-facing view down the road  |
| Stats overlay | point count, rolling FPS, camera distance, box count |

The color-mode dropdown lists only the modes the live scene supplies — see
"Scene-dependent color modes".

## e2e strategy
`conftest.py` ensures vendoring, then starts `serve.py` on a free port per test.
`test_e2e.py` (pytest-playwright, Chromium) waits on `window.__PCL.ready`, asserts
the point count and that non-background pixels were drawn
(`__PCL.handle.visiblePixelCount()` via `gl.readPixels`, enabled by the renderer's
`preserveDrawingBuffer`), exercises each control through the `window.__PCL.settings`
hook, and captures a screenshot (compatible with `/e2e-screenshots-report`).

## Scenes

Scenes are offered in this order (the first, **KITTI movie**, is the default):

| Scene            | Source                                                | Transport                       |
|------------------|-------------------------------------------------------|---------------------------------|
| KITTI movie      | `kolodkin/pcl-viewer-kitti-movie` `geometry/` (HF)    | HF resolve (CORS), Draco `.drc` |
| KITTI seg        | `kolodkin/pcl-viewer-kitti-movie` `seg/` (HF)         | HF resolve (CORS), Draco `.drc` + `boxes.json` |
| Stanford Lucy    | three.js repo `Lucy100k.ply` (50k-vertex binary PLY)  | raw.githubusercontent (CORS)    |

The shared HF dataset holds both movies under sibling folders — `geometry/`
(positions-only, from KITTI raw drive 0005) and `seg/` (SemanticKITTI, with
per-point classes + `boxes.json`) — under one CC BY-NC-SA card.

### Loading local files (Load PCL)
Beyond the three remote scenes, a **Load PCL…** button opens the OS file picker
(a hidden `<input type="file" accept=".pcd,.csv,.parquet">`) and loads a local
cloud as a transient `file` scene via `viewer.loadFile(file)`. Parsing lives in
`web/loaders.js` (`parseLocalFile`), dispatched by extension:

- **`.pcd`** — three's `PCDLoader` as a pure parser; takes `position` plus, when
  the header declares it, the `intensity` attribute. Standard PCD fields are
  numeric, so this path carries no class.
- **`.csv`** — text parse. Delimiter auto-detected among `, ; \t`; blank lines and
  `#` comments skipped. A header row (first three cells not all numeric) maps
  columns by name; otherwise columns are positional.
- **`.parquet`** — `hyparquet.parquetReadObjects` over an in-memory `AsyncBuffer`
  (uncompressed + snappy only; other codecs surface a clear error). Columns map by
  name, else positionally in file order.

**Schema.** Tabular inputs follow `x,y,z` plus optional `i` (numeric intensity)
and `c` (per-point class **string**): `x,y,z` / `x,y,z,i` / `x,y,z,c` /
`x,y,z,i,c`. Header names recognized: `x`/`y`/`z`, intensity (`i`, `intensity`,
`reflectance`), class (`c`, `class`, `label`, `category`, `cls`). With no header,
columns are inferred by count — 3 ⇒ `xyz`, 4 ⇒ `xyz` + (4th column numeric ⇒
intensity, else class), 5+ ⇒ `x,y,z,i,c` (extras ignored). Rows with a non-finite
`x`/`y`/`z` are skipped; an empty/short/garbage file sets `state.error` and the
viewer stays usable.

**Class enumeration + palette.** The class strings are enumerated in first-seen
order into `classIds` + `classNames`; the viewer builds a dynamic vivid palette
(`generateClassPalette`, golden-angle HSL) — the seg scene's fixed SemanticKITTI
palette doesn't apply to arbitrary labels — and `computeColorBuffers` maps each id
through it into the `class` buffer (the loaded path passes ids/colors directly
rather than smuggling them through a Draco color channel like seg), and also fills
`scalars.classId` so the loaded file's legend is **tickable** like the seg scene's —
the same class-filter path serves both. A `name → swatch` legend
(`state.classLegend`, surfaced via `getStats`) renders in the panel, reusing the seg
scene's tickable `legend` markup (class id = enumeration index). Loading a file
resets the range filters and hidden classes so the new cloud starts unfiltered.
"By class" is the default mode when a file carries classes; otherwise the active
scalar mode carries over (flat fallback as usual). Loaded clouds use the **object** normalization profile (no z-up rotation,
centered, framed front-on). Point size is **auto-scaled to the cloud's density** on
load (`autoPointSize`, ∝ 1/√count, clamped 0.004–0.02) so a sparse file reads
clearly instead of vanishing at the dense-scan default; the size slider follows via
`getStats`, and the user can still adjust it. The Scene dropdown gains a temporary
entry showing the file name while loaded; switching to a built-in scene clears it.

### Per-scene normalization profiles
`viewer.js` keys a small **profile** off the scene id. The KITTI clouds (movie,
seg) are z-up sensor frames spread over a wide ground plane: rotate z-up → y-up,
scale by a robust *horizontal* (x,z) radius, and frame from the low forward-facing
chase camera (the default described above). **Lucy** is a compact object already in
a y-up frame: it skips the rotation, scales by a robust *bounding extent* (98th
-percentile L∞ radius) so the tall figure fits the cube rather than being dominated
by its height, and is framed **front-on and upright** from a slightly elevated
three-quarter angle. Both profiles center on the origin and normalize to
`sceneRadius 0.5`, so the point-size slider and the height/distance ramps work
unchanged (Lucy carries no `intensity`, so that mode falls back to flat; the
movie and seg scenes now do carry it).

`web/config.js` holds the scene URLs and frame counts, each overridable via
`?lucyUrl=`, `?movieBase=`, `?movieCount=`, `?segMovieBase=`, `?segMovieCount=`,
`?segBoxesUrl=` (used by e2e to point at local fixtures). `viewer.js` exposes
`loadScene(id)`; `loadStatic` loads Lucy, picking the loader by
file extension (`PCDLoader` for `.pcd`, `PLYLoader` for `.ply` — the PLY mesh's face
index is stripped so its unique vertices render as points), `loadMovie` (DRACOLoader)
drives the movie, and `loadSegMovie` adds the seg scene's classes + boxes. The movie **streams**:
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
voxel-downsample to ~30k points (positions + per-point intensity, the latter packed
into the Draco color attribute's green channel) → Draco encode (14-bit position
quantization, 154 frames) → upload to the HF dataset with a dataset
card (`README.md`) and an `annotations.md` noting the frames carry positions +
intensity (no KITTI object/semantic labels). Frame data is **not** in git; the
browser fetches `.drc` at runtime. Tiny
committed fixtures (`tests/fixtures/movie/*.drc` for the movie, and
`tests/fixtures/lucy/lucy_fixture.ply` — a small indexed-mesh PLY exercising the
PLY/object path for the Lucy scene), built by `tests/fixtures/build_fixtures.py`,
drive the offline e2e — conftest stages them into `web/fixtures/`. The real Lucy
cloud is hot-linked, so it is not fetched in tests. The **Load PCL** fixtures
(`tests/fixtures/file/cloud.{csv,pcd,parquet}` + `cloud_noheader.csv`, one cloud in
every format/schema, built by `build_file_fixtures.py`) are uploaded straight
through the file input by `test_load_pcl.py`, exercising each loader and schema
variant offline.

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
- **Intensity encoding.** The same color attribute's **green channel** carries the
  per-point intensity (`colors[:,1] = intensity*255`), for both the geometry and
  seg movies. The viewer is told which channel holds what via a per-scene
  `colorChannels` map (`{classChannel, intensityChannel}`) threaded through
  `loadMovie` → `decodeFrame` → `computeColorBuffers`, so "By intensity" is offered
  on both the movie and seg scenes.
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
  `seg/` to HF. The live scene is sequence **00**, frames 0–149. The offline seg
  fixture (`tests/fixtures/seg/`) is the **first few real processed frames** sliced
  out by `build_seg_fixtures.py`, so the e2e and screenshot report render at the
  same density/classes/boxes as the live scene, just with a handful of frames.

### Licensing

KITTI / SemanticKITTI are **CC BY-NC-SA 3.0**. The committed KITTI frame (retained
as the movie-fixture source) and both
derived movies (`geometry/` and `seg/`) retain that license with attribution
(Geiger et al., IJRR 2013 / CVPR 2012; Behley et al., ICCV 2019 for the seg
labels); the single HF dataset card declares `license: cc-by-nc-sa-3.0` and
carries the citations per the BY + SA terms. **Stanford Lucy** is from the Stanford
3D Scanning Repository (Stanford Computer Graphics Laboratory); per the repository's
terms it is used with attribution and is hot-linked at runtime (the `Lucy100k.ply`
decimation shipped in the three.js examples), not committed.

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
