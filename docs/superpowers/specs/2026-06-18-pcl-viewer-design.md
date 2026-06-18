# PCL Viewer — three.js + Preact — Design

**Date:** 2026-06-18
**Status:** Approved (pending spec review)

## Overview

A standalone example project, `pcl-viewer/`, demonstrating a **no-build,
ESM-only** browser point cloud viewer. It loads a real point cloud
(`Zaghetto.pcd`) with three.js's `PCDLoader`, renders it with
`OrbitControls`, and wraps the scene in a small **Preact** UI (control panel +
stats overlay). The static assets are served by a tiny **Python `http.server`**
wrapper, and the whole flow is verified with **Python Playwright** e2e tests.

The sample demonstrates: native ES module `import` via import maps (no
bundler), three.js loader/controls usage, Preact rendering without a JSX build
step (via `htm`), a minimal static file server in pure Python, and deterministic
browser e2e testing.

## Goals / Non-goals

**Goals**
- Authentic "JavaScript module import" pattern: import maps + bare specifiers,
  zero build tooling.
- Load and render a real, externally-sourced point cloud asset.
- Full-featured viewer: orbit controls, point-size + color controls, live stats,
  reset + helper toggles.
- Reliable, deterministic, **offline-capable** Playwright e2e tests.

**Non-goals**
- No bundler, transpiler, or npm install of the frontend (vendored ESM only).
- No server-side point cloud processing — the Python server is a static file
  server only.
- No support for arbitrary file uploads or multiple point clouds (single bundled
  asset).

## Bundled asset

- **File:** `Zaghetto.pcd` — the canonical three.js `PCDLoader` demo asset
  (a scanned figure), ~704 KB **binary** PCD.
- **Source:** three.js repository, `examples/models/pcd/binary/Zaghetto.pcd`.
- **License:** MIT (covered by three.js's repository-wide MIT license).
- The header declares `x y z` only (no RGB), so the "color by height" mode
  computes per-vertex colors from the Z coordinate; "flat" mode uses a single
  material color. (Header to be confirmed during implementation; if RGB is
  present the loader's colors are used for a third "native" mode — optional.)
- Committed into the repo at `pcl-viewer/web/models/Zaghetto.pcd`.

## ESM strategy — vendored import maps

- `index.html` declares an **import map** mapping bare specifiers to **local**
  vendored files:
  - `three` → `./vendor/three.module.js`
  - `three/addons/` → `./vendor/addons/`
  - `preact` → `./vendor/preact.module.js`
  - `htm/preact` → `./vendor/htm-preact.module.js`
- `vendor.sh` downloads these from a **pinned CDN** (esm.sh, pinned versions)
  into `web/vendor/`. The runner (`pcl-viewer.sh`) invokes `vendor.sh` (a no-op
  if vendor files already exist).
- `web/vendor/` is **gitignored** — not committed. Reproducible via `vendor.sh`.
- Rationale: real import-map / no-build pattern, while keeping the repo small
  and making Playwright e2e **offline-capable** (browser loads libs from the
  local Python server, not a CDN at test time).

## Project structure

```
pcl-viewer/
├── web/
│   ├── index.html        # import map + #app mount point
│   ├── app.js            # Preact root: Viewer + ControlPanel + StatsOverlay (htm)
│   ├── viewer.js         # three.js scene/camera/renderer/controls/PCDLoader/helpers
│   ├── styles.css
│   ├── models/Zaghetto.pcd   # bundled sample (committed, MIT)
│   └── vendor/           # gitignored; populated by vendor.sh
├── tests/
│   └── test_e2e.py       # Playwright e2e
├── conftest.py           # pytest fixture: start/stop server on a free port
├── serve.py              # http.server wrapper: MIME for .pcd/.js/.mjs, no-cache
├── vendor.sh             # downloads pinned three/preact/htm into web/vendor/
├── pcl-viewer.sh         # runner: vendor.sh + serve.py + print URL
├── pyproject.toml        # dev deps: pytest, pytest-playwright
├── README.md             # setext title + one paragraph + ./pcl-viewer.sh
└── SPEC.md               # ESM rationale, controls catalog, e2e strategy
```

## Component boundaries

- **`viewer.js`** — exports `createViewer(canvas)` returning an imperative
  handle: `setPointSize(n)`, `setColorMode(mode)`, `resetCamera()`,
  `toggleHelpers(on)`, `getStats()`, `dispose()`. Owns all three.js state
  (scene, camera, renderer, OrbitControls, the loaded `Points`, helpers, render
  loop, FPS measurement). Knows nothing about Preact.
- **`app.js`** — pure Preact via `htm` (no JSX/build). Owns UI state, renders
  the control panel and stats overlay, and calls the viewer handle. Knows
  nothing about three.js internals. Polls `getStats()` on an animation/interval
  tick to update the overlay.
- **`serve.py`** — `http.server.ThreadingHTTPServer` + a
  `SimpleHTTPRequestHandler` subclass serving `web/` with correct MIME types for
  `.pcd` (application/octet-stream), `.js`/`.mjs` (text/javascript) and
  `Cache-Control: no-store`. Accepts `--port` and `--directory`; binds to
  `127.0.0.1`.

## Test hook

`viewer.js`/`app.js` expose a small `window.__PCL` object for deterministic
e2e assertions:
- `ready` (bool) — true once the PCD is loaded and first frame rendered.
- `pointCount` (int).
- `settings` — `{ pointSize, colorMode, helpers }`, kept in sync with UI.

This lets Playwright assert state changes precisely instead of pixel-guessing.

## Features

1. **Orbit controls** — `OrbitControls` for rotate/pan/zoom.
2. **Point size + color** — Preact slider drives `points.material.size`;
   color-mode toggle switches between flat material color and per-vertex
   color-by-height (computed from Z).
3. **Stats overlay** — point count, live FPS (rolling average), camera distance.
4. **Reset + helpers** — reset-camera button; toggle for `BoxHelper`
   (bounding box) + `AxesHelper`.

## e2e test flow

1. `conftest.py` fixture picks a free port, launches `serve.py` as a subprocess,
   waits for it to accept connections, yields the base URL, tears it down after.
   (Fixture also ensures `vendor.sh` has run.)
2. `test_e2e.py` (pytest-playwright, Chromium):
   - Navigate to the base URL.
   - Wait for `window.__PCL.ready === true`.
   - Assert `window.__PCL.pointCount > 0`.
   - Assert the canvas is non-blank (non-uniform pixels via a small JS readback
     or screenshot heuristic).
   - Move the point-size slider; assert `window.__PCL.settings.pointSize`
     reflects the new value.
   - Toggle color mode; assert `settings.colorMode` changed.
   - Click reset-camera; assert no errors and viewer still ready.
   - Toggle helpers; assert `settings.helpers` changed.
   - Capture a screenshot (compatible with the `/e2e-screenshots-report` skill).

## Runner (`pcl-viewer.sh`)

- `cd`s to its own directory.
- Runs `vendor.sh` (idempotent).
- Starts `serve.py` and prints the local URL.
- Mirrors the repo convention of a single user-facing shell entry point.
- `PYTHON="${PYTHON:-uv run python}"` for dual-mode (monorepo or standalone).

## README / docs convention

- `README.md`: setext title (`---` underline), one concise paragraph, a single
  bash code block running `./pcl-viewer.sh`. No extra sections.
- `SPEC.md`: ESM/import-map rationale, controls catalog, e2e strategy, asset
  provenance/license.

## Open questions / risks

- **`vendor.sh` network at setup time** — requires reachable esm.sh once to
  populate `web/vendor/`. Acceptable; documented in SPEC.md.
- **Playwright browser install** — `playwright install chromium` is required
  before running e2e; documented and/or attempted by the runner.
- **PCD header** — confirm `Zaghetto.pcd` fields during implementation; adjust
  color-mode logic if RGB is present.
