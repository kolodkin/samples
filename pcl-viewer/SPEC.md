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
  (`setPointSize`, `setColorMode`, `resetCamera`, `getStats`,
  `visiblePixelCount`, `dispose`). Owns all three.js state. No Preact.
- `app.js` — pure Preact UI; owns control state and drives the handle. No three.js
  internals.
- `serve.py` — `ThreadingHTTPServer` serving `web/` with JS/`.pcd` MIME types and
  `Cache-Control: no-store`.

## Sample asset
`web/models/kitti-velodyne-000000.pcd` — frame `000000` of the KITTI raw Velodyne
data: a single 360° street-level LiDAR scan (binary PCD, `FIELDS x y z intensity`,
115,385 points, ~1.8 MB). It is a *city viewpoint* cloud — one sensor sweep showing
the road as concentric scan rings with parked cars, walls, poles and vegetation
rising out of it. Source: the KITTI dataset (Geiger et al., CVPR 2012), via the
`Qjizhi/kitti-velodyne-viewer` repo's pre-converted PCDs. KITTI is licensed
**CC BY-NC-SA 3.0** (non-commercial, attribution, share-alike) — fine for this demo,
but note it is *not* permissively licensed like the rest of the project.

KITTI uses a z-up vehicle frame in metres, so on load the viewer reorients it
(z-up → three.js y-up), centers it, and scales by a **robust** horizontal radius
(90th percentile of distance from the sensor, not the absolute max) so the dense
scene fills the view instead of being shrunk by a few 80 m stray returns. The
**default** "by height" mode colors per-vertex along the vertical axis (blue → red),
with the range clamped to the 2nd–98th height percentile so ground reads blue and
cars/walls climb through to red. The same ramp (and the same robust percentile
clamp) also drives "by distance" (radial range from the sensor, which lights up the
concentric scan rings) and "by intensity" (the PCD's per-point laser reflectance,
which picks out road markings and signs); "flat" mode (a single material color) is a
toggle. The ramp buffers are precomputed once per cloud and swapped on the geometry,
and a scalar mode the source lacks (e.g. an intensity-free PCD) falls back to flat.
The camera sits low and forward-facing — just above the sensor's forward (+X) axis,
looking down the road — so the scan reads like an onboard driving view: ground
rings sweep to the horizon and cars/walls/poles stand up along the street.

## Controls
| Control       | Effect                                              |
|---------------|-----------------------------------------------------|
| Point size    | `PointsMaterial.size` (0.002–0.05)                  |
| Color mode    | flat vs. per-vertex ramp by height / distance / intensity |
| Reset camera  | re-frames the low forward-facing view down the road  |
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
