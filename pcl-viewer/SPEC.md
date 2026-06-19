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
  (`setPointSize`, `setColorMode`, `resetCamera`, `toggleHelpers`, `getStats`,
  `visiblePixelCount`, `dispose`). Owns all three.js state. No Preact.
- `app.js` — pure Preact UI; owns control state and drives the handle. No three.js
  internals.
- `serve.py` — `ThreadingHTTPServer` serving `web/` with JS/`.pcd` MIME types and
  `Cache-Control: no-store`.

## Sample asset
`web/models/Zaghetto.pcd` — the canonical three.js `PCDLoader` demo asset (binary
PCD, `FIELDS x y z`, 59,750 points, ~704 KB). MIT-licensed via three.js. Since it
carries no color, "by height" mode computes per-vertex colors from the Z coordinate
(blue → red); "flat" uses a single material color.

## Controls
| Control       | Effect                                              |
|---------------|-----------------------------------------------------|
| Point size    | `PointsMaterial.size` (0.002–0.05)                  |
| Color mode    | flat material color vs. per-vertex color-by-height  |
| Show helpers  | toggles `Box3Helper` + `AxesHelper`                 |
| Reset camera  | re-frames the camera to the cloud's bounding box    |
| Stats overlay | point count, rolling FPS, camera distance           |

## e2e strategy
`conftest.py` ensures vendoring, then starts `serve.py` on a free port per test.
`test_e2e.py` (pytest-playwright, Chromium) waits on `window.__PCL.ready`, asserts
the point count and that non-background pixels were drawn
(`__PCL.handle.visiblePixelCount()` via `gl.readPixels`, enabled by the renderer's
`preserveDrawingBuffer`), exercises each control through the `window.__PCL.settings`
hook, and captures a screenshot (compatible with `/e2e-screenshots-report`).

## Run
- Viewer: `./pcl-viewer.sh` (set `PORT` to override 8000).
- Tests: `uv run --group dev playwright install chromium` once, then
  `uv run --group dev pytest`.
