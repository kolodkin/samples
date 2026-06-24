# pcl-viewer "kitti-seg" scene — design

**Date:** 2026-06-24
**Status:** approved (design), pending implementation plan
**Scope:** a new annotated movie scene in the existing `pcl-viewer` project.

## Goal

Add a fourth scene, **kitti-seg**, to the pcl-viewer: a streamed KITTI LiDAR
*movie* where every point carries a **semantic class** (per-point
classification) and each object instance is wrapped in a **3D bounding box**
(object detection). The existing "KITTI movie" scene stays geometry-only and
untouched; kitti-seg is the labeled counterpart.

The demo thus shows both halves of LiDAR perception on the same sequence:
- **Classification** — points colored by SemanticKITTI class via a fixed palette.
- **Object detection** — a 3D box per instance, updated each frame.

## Data source

**SemanticKITTI**, which extends the KITTI Odometry benchmark with dense
per-point labels. Each scan is the KITTI Odometry `velodyne/*.bin`
(`x y z remission`, ~120k pts); the matching `labels/*.label` stores one
`uint32` per point — **lower 16 bits = semantic class, upper 16 bits = a
temporally-consistent instance id**. Annotated sequences are 00–10.

We take a **~150-frame slice** of one annotated sequence, chosen at build time
for object density (cars / cyclists / pedestrians visible), to parallel the
existing 154-frame movie. The exact sequence/range is a build-script choice, not
a user-facing parameter.

Why not the alternatives:
- *KITTI raw + tracklets* (the current movie's drive 0005): gives boxes but no
  per-point labels.
- *KITTI-360*: native oriented boxes + dense labels, but a much heavier dataset
  that diverges from the existing KITTI-Velodyne pipeline. Rejected as YAGNI.

New HF dataset: **`kolodkin/pcl-viewer-kitti-seg`**, mirroring the existing
`kolodkin/pcl-viewer-kitti-movie` layout.

### Licensing

SemanticKITTI / KITTI are **CC BY-NC-SA 3.0**. The derived dataset keeps that
license with attribution; the HF card declares `cc-by-nc-sa-3.0` and carries the
SemanticKITTI (Behley et al., ICCV 2019) and KITTI (Geiger et al.) citations.
Unlike the geometry-only movie, this dataset's `annotations.md` documents that
labels **are** included (per-point class + per-frame boxes).

## How labels ride the streaming pipeline

The hard constraint: **Draco may reorder/dedup points on decode**, so any
per-point attribute must travel *inside* the Draco point, not in a side array.

**Decision — class packed into the Draco color attribute.** `DracoPy.encode`
already accepts `colors`; we store the **raw class id in one channel** (e.g. the
red byte = class id 0–255) rather than the resolved palette RGB. Keeping the id
(not the color) lets the viewer drive both the per-point palette *and* the legend
and box-by-class colors from a single source of truth, and lets the palette
change without rebuilding the dataset. The class stays glued to its point through
decode. No new attribute machinery, and the existing streaming/worker-queue path
is reused as-is.

**Boxes** are precomputed at build time and shipped as a single small
`boxes.json` fetched once — not per point, so they don't need the Draco channel.

Rejected alternative: a parallel per-frame `.lbl` binary alongside positions-only
Draco. The decoded-position order is not guaranteed to match the label array, so
labels could silently misalign. Rejected.

## Build pipeline — `scripts/build_seg_dataset.py` (one-shot)

1. Download KITTI Odometry `velodyne` + SemanticKITTI `labels` for the chosen
   sequence/range.
2. Per frame: split each `.label` into class (low 16) and instance (high 16).
3. **Joint voxel-downsample** positions to ~30k points, carrying each surviving
   point's class (nearest-point label — labels are not averaged).
4. **Derive boxes:** group the downsampled points by instance id (thing classes
   only), and emit one 3D box per instance: center, size, class. v1 uses an
   **axis-aligned** box in the reoriented frame; oriented-via-PCA heading is a
   later nicety.
5. **Draco-encode** positions with the per-point **class as the color
   attribute** (14-bit position quantization, as today).
6. Write `boxes.json` — `{ "NNNNNN": [ {cls, center:[x,y,z], size:[x,y,z]}, … ] }`.
7. Upload `.drc` frames + `boxes.json` + dataset card (`README.md`) +
   `annotations.md` to the HF dataset.

## Viewer changes — `viewer.js`

- `loadMovie` reads the Draco **color attribute** as the per-point class and
  builds the "by class" color buffer from the fixed SemanticKITTI palette.
- New color mode **"by class"** (palette lookup, not a ramp). Existing
  height/distance/intensity/flat modes are unaffected; "by class" only carries
  data on the seg scene and otherwise falls back like any missing scalar.
- A per-frame **box group** (`LineSegments`) rebuilt/swapped as each frame's
  `boxes.json` entry is applied, colored by class, toggleable via the handle.
- Handle/`getStats` expose box count and current mode so e2e can assert them.

## UI changes — `app.js`

- Add the **kitti-seg** scene to the scene selector.
- Add **"by class"** to the color-mode control (default mode for the seg scene).
- A small **class legend** (color ⇢ class name) shown for the seg scene.
- A **"show boxes"** toggle (seg scene only).

## Config — `web/config.js`

Add the seg scene's `segMovieBase`, `segBoxesUrl`, and `segMovieCount`, each
`?`-overridable (and via `window.__PCL_CONFIG`) exactly like the existing movie
params, so e2e points them at local fixtures.

## Tests

- New committed fixtures: a few Draco frames **with the class color attribute**
  plus a tiny `boxes.json`, built by an extension of
  `tests/fixtures/build_fixtures.py`; conftest stages them into `web/fixtures/`.
- e2e (`test_e2e.py`): select the kitti-seg scene, assert "by class" coloring
  draws non-background pixels (`visiblePixelCount`), and assert the box group
  renders (box count > 0 via the stats hook). Stays offline — no CDN.

## Docs

- `README.md`: add the kitti-seg scene to the description and scene table.
- `SPEC.md`: document the palette, the class-as-color encoding, box derivation
  from instance ids, and the new licensing/citation note.

## Out of scope (v1)

- Oriented (heading-aware) boxes — axis-aligned only for now.
- Per-point *instance* coloring (we color by class; instances are used only to
  build boxes).
- Editing/labeling UI — playback and visualization only.
