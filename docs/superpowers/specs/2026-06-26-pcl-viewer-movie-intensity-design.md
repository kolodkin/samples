# pcl-viewer: intensity in the KITTI movies — design

**Date:** 2026-06-26
**Status:** approved for planning

## Goal

The two KITTI movie scenes in pcl-viewer (`geometry/` and `seg/`) currently
discard the LiDAR intensity (reflectance) channel at dataset-build time, so the
viewer cannot offer "By intensity" for them (only the static city PCD can).
Add per-point intensity to both movie datasets and surface it as a color mode in
the viewer, matching the city scene.

## Scope

In scope:
- Both build scripts encode intensity into the movie frames.
- The viewer decodes intensity from the movie frames and offers the mode.
- Test fixtures + e2e assertions updated.
- Dataset cards, `annotations.md` text, and `SPEC.md` updated.

Out of scope (the user runs these with `HF_TOKEN` + bandwidth):
- The actual download → process → upload of the real datasets.

## Encoding convention

KITTI raw and SemanticKITTI velodyne `.bin` files store reflectance as the 4th
`float32` column, range ~0–1. Quantize to `uint8` (`clip(intensity * 255, 0,
255)`) and pack it into the Draco **color attribute's green channel** — the same
mechanism already used to smuggle the class id in the red channel. DracoPy's
`encode` exposes `colors` (not arbitrary generic attributes), so a color channel
is the practical carrier, and it keeps the two movies consistent.

Per-scene channel layout of the Draco color attribute:

| Scene             | R (channel 0) | G (channel 1) |
|-------------------|---------------|---------------|
| `geometry/` movie | 0             | intensity     |
| `seg/` movie      | class id      | intensity     |

The city PCD is untouched — it keeps its native `intensity` PCD field.

## Build-script changes

### `scripts/build_movie_dataset.py`
- `downsample_to_target` currently returns *points*, which loses alignment with
  intensity. Refactor to an index-returning form (mirror the seg script's
  `voxel_downsample_idx` / `downsample_idx_to_target`) so intensity stays
  point-aligned.
- Keep `intensity = raw[:, 3]`, index it by the downsample indices, quantize to
  `uint8`, place in `colors[:, 1]`; encode with `DracoPy.encode(xyz,
  colors=colors, quantization_bits=QUANT_BITS)`.
- Update `CARD` / `ANNOTATIONS` text: no longer "positions only"; document the
  intensity-in-green-channel layout.

### `scripts/build_seg_dataset.py`
- Keep `intensity = raw[:, 3]`, index by the existing `idx`, quantize to `uint8`,
  place in `colors[:, 1]` (red channel `colors[:, 0]` still holds the class id).
- Update `CARD` / `ANNOTATIONS` text to mention the intensity channel.

Both scripts upload to the same HF repo and each defines its own `CARD`; update
both so the dataset card is consistent whichever script last wrote it.

## Viewer changes (`web/viewer.js`)

- `computeColorBuffers(geometry, opts = {})` takes an explicit channel map rather
  than sniffing the color attribute:
  - `opts.classChannel` (default null): if set and a `color` attribute exists,
    build the `class` buffer from that channel (existing palette logic).
  - `opts.intensityChannel` (default null): if set and a `color` attribute
    exists, build the `intensity` buffer from that channel (`value / 255` →
    `rampColors`, which already does robust percentile clamping).
  - If neither is set, fall back to the existing native `intensity` *attribute*
    path (the city PCD).
- Thread the map through the movie loaders: `loadMovie(count, { colorChannels })`
  → `decodeFrame(..., colorChannels)` → `computeColorBuffers(geom, colorChannels)`.
  - geometry movie call site: `{ intensityChannel: 1 }`.
  - `loadSegMovie` → `loadMovie(..., { colorChannels: { classChannel: 0,
    intensityChannel: 1 } })`.
- The Draco decode already uses `LinearSRGBColorSpace` so raw color values pass
  through un-mangled; intensity rides the same path, no change needed there.

Resulting offered modes:
- **movie:** `flat / height / distance / intensity`
- **seg:** `flat / class / height / distance / intensity`
- **city:** unchanged (`flat / height / distance / intensity`)
- **Lucy:** unchanged (`flat / height / distance`)

## Fixtures & tests

- `tests/fixtures/build_fixtures.py` (geometry movie fixture): the committed city
  PCD carries real intensity, so read the 4th column and encode it into the green
  channel — the regenerated fixtures carry genuine intensity. Regenerate and
  commit the 4 `.drc` frames.
- `tests/fixtures/build_seg_fixtures.py`: the seg fixture is normally sliced from
  a ~6 GB processed dataset, which is not available offline. Add a small offline
  re-encoder step that decodes each committed seg `.drc` frame and injects a
  **deterministic synthetic** intensity into the green channel (red/class channel
  preserved), then re-encodes. This exercises the "by intensity" path offline.
  Real seg intensity arrives when the user re-runs `build_seg_dataset.py
  --process`. The synthetic nature is documented in the fixture script.
- `tests/test_e2e.py`: update the seg color-mode assertion from
  `['flat','class','height','distance']` to
  `['flat','class','height','distance','intensity']` (both the `wait_for_function`
  and the `_color_mode_options` assert). City and movie assertions already include
  intensity / are unaffected.

Open risk: regenerating `.drc` fixtures requires DracoPy (the `gen` dependency
group). If it cannot run in the implementation environment, fixture regeneration
falls to the user; the plan should verify DracoPy availability early.

## Docs

- `SPEC.md`: update the offered-modes summary (movie/seg now include intensity)
  and the note that movie intensity "falls back to flat".
- `README.md`: line 19 already reads "by-intensity where the source carries it",
  which stays accurate — no change.
- Dataset `annotations.md` text is generated by the build scripts (covered above).

## Testing strategy

- Run the existing Playwright e2e suite (`pcl-viewer/tests/test_e2e.py`) against
  the regenerated fixtures; the color-mode-per-scene test now asserts intensity on
  the seg scene and exercises the by-intensity render path on the movie scenes.
- `test_server.py` unaffected.
