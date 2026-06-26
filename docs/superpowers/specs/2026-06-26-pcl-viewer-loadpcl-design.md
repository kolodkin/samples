# PCL Viewer — "Load PCL" local file menu

Date: 2026-06-26
Scope: `pcl-viewer/` (no-build ESM viewer: three.js + Preact)

## Goal

Let the user load their **own** point clouds from local files via a "Load PCL"
control, in addition to the three built-in remote scenes (movie, seg, lucy).

Accepted formats: **`.pcd`**, **`.csv`**, **`.parquet`**.

Column schema for tabular inputs (csv / parquet):

```
x,y,z
x,y,z,i        # i = intensity (numeric)
x,y,z,c        # c = class (string enumeration)
x,y,z,i,c
```

`c` is a per-point class label as a string; distinct strings form an enumeration
that drives the "By class" color mode and a legend.

## Decisions (defaults; interactive approval unavailable in this session)

1. **Parquet parsing** — vendor a self-contained ESM build of
   [`hyparquet`](https://github.com/hyparam/hyparquet) (pure JS, no WASM,
   bundles snappy) into `web/vendor/hyparquet.module.js`, consistent with how
   three/preact/htm are vendored. Source: the esm.sh self-contained bundle
   `https://esm.sh/hyparquet@1.26.1/es2022/hyparquet.bundle.mjs` (no external
   imports — works offline once vendored). Uncompressed + snappy Parquet is
   supported; other codecs (zstd/gzip/brotli) are out of scope and surface a
   clear error.
2. **Schema detection** — header names when a header row/columns exist
   (`x,y,z,i,c`, plus aliases `intensity`, `class`/`label`/`category`/`cls`),
   otherwise positional inference by column count + value type (4th column:
   numeric ⇒ intensity, non-numeric ⇒ class).
3. **Class coloring** — build a dynamic palette: collect distinct class strings
   in first-seen order, assign each a generated distinct color (golden-angle
   HSL), and render a `name → swatch` legend in the controls panel.
4. **UX** — a dedicated **"Load PCL…"** button in the controls panel opens the
   OS file picker (hidden `<input type="file">`). On success the cloud loads as
   a transient `file` scene; the Scene dropdown gains a temporary entry showing
   the loaded filename (so the dropdown reflects current state and the user can
   still switch to a built-in scene). Switching away discards the loaded cloud
   (no persistence — files are local and re-pickable).

## Architecture

### New module: `web/loaders.js`

Pure parsing, no three.js scene state. Exports:

- `parseLocalFile(file): Promise<ParsedCloud>` — dispatch by extension.
  - `.pcd` → `PCDLoader.parse(arrayBuffer)`; take `position` + (if present)
    `intensity` attributes. No class (standard PCD fields are numeric).
  - `.csv` → text parse (below).
  - `.parquet` → `parquetReadObjects` over an in-memory AsyncBuffer.
  - Unknown extension → throws a clear error.

`ParsedCloud` shape:

```js
{
  positions: Float32Array,   // length n*3, interleaved xyz
  intensity: Float32Array | null,   // length n
  classIds:  Uint16Array  | null,   // length n, index into classNames
  classNames: string[]    | null,   // enum: id -> label
}
```

Internal helpers:

- `parseCsv(text)` → builds column arrays, detects header + delimiter
  (`,` `;` `\t`), maps columns to the schema, enumerates class strings.
- `parseParquet(arrayBuffer)` → rows → columns → same mapping.
- `mapColumns(names)` → `{x,y,z,i,c}` indices from header names (or `-1`).
- `enumerate(stringValues)` → `{classIds, classNames}` (first-seen order).
- Positional fallback shared by csv/parquet when names don't resolve: by count
  (3=xyz, 4=xyz+i|c by type, 5=xyz,i,c).

### `web/viewer.js` changes

- New profile `loaded` (alias of the existing `object` profile: no z-up→y-up
  rotation, center, scale by robust 98th-pct extent, three-quarter framing).
  Arbitrary clouds are treated as compact objects in their own frame.
- `computeColorBuffers(geometry, opts)` extended: accept `opts.classIds`
  (Int array) + `opts.classColors` (`THREE.Color[]` by id) and build the
  `class` ramp buffer directly (the loaded path doesn't smuggle ids through a
  Draco color channel like seg does).
- `generateClassPalette(n)` → `THREE.Color[]` via golden-angle HSL.
- `handle.loadFile(file)`:
  1. `loadToken++`, `teardownScene()`, `state.scene='file'`, clear errors.
  2. `const parsed = await parseLocalFile(file)` (stale-token guarded).
  3. Build `BufferGeometry` (position; native `intensity` attribute if present
     so the existing intensity branch picks it up).
  4. Normalize with the `loaded` profile.
  5. `computeColorBuffers` with class ids/colors when present.
  6. `state.colorModes = offeredModes(buffers)`; default mode = `class` if class
     present else carry current (falls back to flat as usual).
  7. `installGeometry`, `resize`, `frameCamera`, `state.ready = true`.
  8. Set `state.fileName`, `state.classLegend` (`[{name, hex}]`).
- `state` gains `fileName` and `classLegend`; `getStats()` returns both.
- Errors (parse failure, unsupported codec, empty/short rows) set `state.error`
  and leave the previous scene's teardown done but `ready=false` with a message.

### `web/app.js` changes

- Hidden `<input type="file" accept=".pcd,.csv,.parquet" data-testid="file-input">`
  (always rendered, `display:none`), `onChange` → read `files[0]`, call
  `viewer.loadFile(file)`, set local `sceneId='file'`.
- "Load PCL…" button `data-testid="load-file"` in the panel → triggers the
  input's `click()`.
- Scene `<select>` options = built-in scenes, plus a temporary
  `{ id:'file', label: fileName }` entry while `sceneId==='file'`, so the
  dropdown shows the loaded file and selecting another scene calls `loadScene`.
- Legend block renders when `isSeg` (existing fixed legend) **or** when
  `stats.classLegend?.length` (dynamic loaded-file legend), reusing
  `data-testid="legend"`.
- HUD shows the loaded filename when `sceneId==='file'`.

### `vendor.sh` change

Add a fetch for `web/vendor/hyparquet.module.js` from the esm.sh bundle URL.
Import-mapped in `index.html` as `"hyparquet"`.

### `index.html` change

Add `"hyparquet": "./vendor/hyparquet.module.js"` to the import map.

## Error handling

- Unsupported extension / parquet codec / malformed rows → `state.error` with a
  human-readable message; viewer stays usable (no crash), boot/HUD shows it.
- Empty file or < 3 numeric columns → error "Need at least x,y,z columns".
- A row with non-finite x/y/z is skipped; if all are skipped → error.

## Testing

- **JS unit-ish via e2e**: drive `viewer.loadFile` through the hidden file input
  with Playwright `set_input_files` over committed fixtures.
- New fixtures in `tests/fixtures/file/`:
  - `cloud.csv` — header `x,y,z,i,c` with a few classes.
  - `cloud_noheader.csv` — positional, 5 columns.
  - `cloud.parquet` — same data (generated via pyarrow in `build_fixtures.py`,
    committed).
  - `cloud.pcd` — small ASCII PCD with `x y z intensity`.
- Tests assert: file loads (`ready`, `scene==='file'`, `pointCount` matches),
  renders (`visiblePixelCount`), class mode offered + colorful pixels for the
  class case, intensity offered when present, dynamic legend visible, and a
  clear error for an unsupported/garbage file.
- Existing tests must stay green (legend testid reused, color-mode derivation
  unchanged for built-in scenes).

## Out of scope (YAGNI)

- Orientation/up-axis toggle for loaded clouds (assume object frame).
- Streaming/animated multi-frame local files.
- Parquet codecs beyond uncompressed/snappy.
- Drag-and-drop (button + picker only; easy to add later).
- Persisting loaded files across scene switches.

## Docs

Update `pcl-viewer/README.md` (one line on loading local files) and `SPEC.md`
(formats, schema, detection rules) per project conventions.
