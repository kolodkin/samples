# pcl-viewer: scene mode drop-down (city view + table scene + KITTI movie)

## Goal

Add a **Scene** drop-down to the PCL viewer that switches between three point
clouds, so the viewer demonstrates more than one static scan:

1. **KITTI city view** (default) — the current single static LiDAR scan.
2. **PCL table scene** — a visually distinct structured indoor cloud.
3. **KITTI movie** — a real multi-frame LiDAR sequence played back as an
   animation (looped, with play/pause).

The headline constraints, settled during brainstorming:

- The movie must be **real multi-frame playback**, not a camera fly-through.
- **No git bloat** — frame data must not be committed to the repo.
- Movie frames are **fetched at runtime from a URL** (the viewer's original
  intent), so the data must be served with cross-origin (CORS) headers.

## Background: why these sources

There is no public, CORS-fetchable, browser-light, *long* point-cloud movie that
can simply be hot-linked. An exhaustive sweep established this:

- The only same-source-as-the-current-demo sequence
  (`Qjizhi/kitti-velodyne-viewer`, `velodyne_pcd/`) has just **6 frames** — too
  short.
- A systematic scan of ~78 Hugging Face datasets found only one genuine dynamic
  movie with individually-fetchable frames — the 8i *longdress*/*loot* sequences
  (`Shivamkak/voxelized_pc_data`, 300 frames each) — but each frame is **~17 MB**
  (≈800k points), i.e. multiple GB to stream. Too heavy for the web.
- Every other LiDAR sequence (SemanticKITTI, KITTI odometry, nuScenes, Waymo, …)
  is packaged as multi-GB archives, account-gated, and/or stored in formats that
  need heavy toolkits to decode — not individually hot-linkable.

The resolution per source:

- **KITTI city view** — already in the project
  (`web/models/kitti-velodyne-000000.pcd`, committed). Unchanged. Ultimately
  sourced from `raw.githubusercontent.com/Qjizhi/kitti-velodyne-viewer`.
- **PCL table scene** — `table_scene_lms400.pcd` (~5.6 MB) fetched **live** from
  `raw.githubusercontent.com/PointCloudLibrary/data` (BSD-3-Clause, serves with
  `access-control-allow-origin: *` — verified). No hosting needed.
- **KITTI movie** — KITTI raw is only distributed as per-drive ZIP archives
  (drive 0005 = 646 MB, ~153 frames, `.bin`) with no per-frame URLs and no CORS,
  so a browser cannot consume it directly. We therefore extract, **decimate**,
  and **re-host** the frames. KITTI raw is **CC BY-NC-SA 3.0**, which explicitly
  permits redistribution (with attribution + non-commercial + share-alike); the
  project already redistributes one KITTI frame, so this is the same posture.
  Frames are hosted on a **new free public Hugging Face dataset** and fetched at
  runtime (HF `resolve` URLs serve with CORS — verified). This keeps the data out
  of git entirely.

### Why not Waymo

Waymo Open Dataset is freely available for research but is the wrong fit: its
license **prohibits redistribution** (so we cannot re-host frames the way
CC BY-NC-SA lets us for KITTI), it is account-gated (cannot be auto-downloaded
here), and its LiDAR is stored as range images in TFRecord protobufs requiring
the TensorFlow `waymo_open_dataset` toolkit to decode. KITTI's redistributable
license is exactly what makes this feature legally clean.

## Architecture

The existing component boundary is preserved: `viewer.js` owns all three.js
state behind an imperative handle and knows nothing about Preact; `app.js` is
pure Preact UI driving that handle.

### Data flow

```
app.js (Scene <select>)  ->  viewer.loadScene(sceneId)
                                 |
   city  -> loadStatic('./models/kitti-velodyne-000000.pcd')   [same-origin]
   table -> loadStatic(PCL_URL)                                 [raw.githubusercontent, CORS]
   movie -> loadMovie(MOVIE_BASE, MOVIE_COUNT)                  [HF resolve, CORS, .drc]
```

### `viewer.js` changes

Generalize from a single hard-coded model to a small scene loader:

- `loadScene(id)` — tears down the current scene (stop movie timer, dispose
  geometries/materials), then dispatches to `loadStatic` or `loadMovie`.
- `loadStatic(url)` — fetch + `PCDLoader.parse`, normalize, frame the camera,
  compute height colors. (City and table share this path; both are `.pcd`.)
- `loadMovie(baseUrl, count)` — preload all frames (fetch `NNNNNN.drc`, decode
  with three.js `DRACOLoader` → `BufferGeometry`), with a progress callback for
  the loading UI. The Draco WASM decoder is created once and reused for every
  frame. Frames share **one transform** (rotation z-up→y-up, sensor at origin,
  **scale computed once from frame 0**) so points don't pulse and the camera
  stays put while the world flows past. A `setInterval`/raf-driven timer
  (~10 fps) swaps the displayed geometry; per-frame height colors are precomputed
  so the color toggle keeps working. `play()` / `pause()` / `isPlaying`.
- **Loaders:** city/table use `PCDLoader`; the movie uses `DRACOLoader`. The
  Draco decoder (`draco_decoder.wasm` + `draco_wasm_wrapper.js`, or three's
  `examples/jsm/libs/draco/`) is **vendored by `vendor.sh`** like the other ESM
  assets, so e2e stays offline and there is no CDN dependency at runtime.
- Existing handle methods (`setPointSize`, `setColorMode`, `resetCamera`,
  `getStats`, `visiblePixelCount`, `dispose`) work across all scenes.
- `window.__PCL` state extended with `scene`, `frameIndex`, `frameCount`,
  `playing`, `loading`, `error` for deterministic e2e.

### `app.js` changes

- A **Scene** `<select>` at the top of the controls modal (KITTI city / PCL table
  / KITTI movie).
- Movie-only controls appear when the movie scene is active: a **play/pause**
  button and a small **loading / progress** line (and an error line on fetch
  failure). On error the viewer keeps the previously loaded scene.
- HUD gains a frame index `n / count` while the movie plays.
- Existing point-size, color-mode, reset controls are unchanged and apply to
  whatever scene is loaded.

### Movie generation + hosting (one-shot, out of band)

A generator script (committed; **not run in CI**, run once to publish):
`pcl-viewer/scripts/build_movie_dataset.py`

1. Download KITTI raw drive 0005 sync zip from the public S3 mirror.
2. Extract `velodyne_points/data/*.bin`, parse float32 `x,y,z,reflectance`.
3. Voxel-grid downsample each frame to ~30k points.
4. **Draco-encode** each frame (e.g. Python `DracoPy`) with **14-bit position**
   quantization (~1 cm over the scene, below the LiDAR's own noise → effectively
   lossless vs. the decimated frame) and **8-bit intensity** → `NNNNNN.drc`
   (~60–90 KB/frame; ≈10–13 MB for ~150 frames, roughly 4× smaller than gzipped
   PCD). Draco output is already compressed — not gzipped again.
5. Create/upload to a new public HF dataset (`kolodkin/pcl-viewer-kitti-movie`)
   using `HF_TOKEN`, including a dataset card and `license: cc-by-nc-sa-3.0`.

The repo keeps only the script + a config constant with the dataset base URL and
frame count. No frame data in git.

### Configuration

`viewer.js` (or a tiny `config.js`) exposes:

- `MOVIE_BASE` — default `https://huggingface.co/datasets/kolodkin/pcl-viewer-kitti-movie/resolve/main/`
- `MOVIE_COUNT` — number of frames (~150)
- `PCL_URL` — the table-scene URL

All overridable via `window.__PCL_CONFIG` (set before module load) and/or
`?movieBase=` / `?pclUrl=` query params, so e2e can point them at local fixtures.

## Licensing / attribution

- **HF dataset** carries `license: cc-by-nc-sa-3.0` in its card metadata, plus an
  attribution + citation block (KITTI raw: Geiger, Lenz, Stiller, Urtasun,
  *Vision meets Robotics: The KITTI Dataset*, IJRR 2013; benchmark suite: Geiger,
  Lenz, Urtasun, CVPR 2012) and a note that it is a **downsampled derivative** of
  KITTI raw drive 0005, linking the original (BY + SA obligations).
- **Viewer/SPEC** keep the existing KITTI CC BY-NC-SA attribution and add a
  BSD-3-Clause note for the PCL table scene.

## Error handling

- A failed movie/table fetch (offline, CORS, 404) shows an on-screen error line
  and keeps the currently-loaded scene (city view always available locally).
- Per-frame movie fetch failures abort the preload with a clear message rather
  than playing a partial/jittery sequence.

## Testing (offline)

- City-view tests unchanged.
- **PCL table** and **movie** e2e run **offline** by overriding `MOVIE_BASE` /
  `PCL_URL` to the local test server, pointing at tiny committed fixtures:
  - a small `table` fixture PCD, and
  - a short fixture movie sequence (a handful of heavily-decimated `.drc`
    frames, a few hundred KB total) under `tests/fixtures/`.
- Assertions: scene loads and renders (`visiblePixelCount`), movie `frameIndex`
  advances over time, play/pause toggles `playing`, switching scenes is clean
  (no leaked timers/geometry), and the Scene `<select>` drives the change.
- Screenshot capture step retained.

## Out of scope / non-goals

- No bundler / build step changes (stays no-build ESM).
- No frame scrubber or playback-speed control (play/pause + loop only).
- Committing frame data (full-res or compressed) to git — movie data lives on HF.

## Open questions (resolved)

- Movie meaning → real multi-frame playback. ✅
- Hosting → free public HF dataset, runtime fetch (no git bloat). ✅
- Source → KITTI raw drive 0005 (redistributable; downloadable here). ✅
- Second-mode scope → three scenes ("Both"). ✅
- Compression → **Draco** (`.drc`), decoded in-browser via vendored
  `DRACOLoader` WASM (~4× smaller than gzipped PCD). ✅
