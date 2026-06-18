# PCL Viewer (three.js + Preact) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `pcl-viewer/`, a no-build ESM browser point-cloud viewer that loads `Zaghetto.pcd` with three.js `PCDLoader`, renders it with `OrbitControls` inside a Preact UI, served by a Python `http.server` wrapper and verified with Python Playwright.

**Architecture:** Static frontend in `web/` using native ES module `import` via an import map pointing at **locally vendored** three/preact/htm files (offline-capable). `viewer.js` owns all three.js state behind an imperative handle; `app.js` is pure Preact (via `htm`, no JSX build) that drives that handle and renders controls/stats. `serve.py` serves `web/` with correct MIME types. A `window.__PCL` test hook exposes `ready`, `pointCount`, `settings`, and a pixel-content check for deterministic e2e.

**Tech Stack:** three.js 0.160.0, preact 10.19.3, htm 3.1.1 (all vendored ESM); Python 3.10+ `http.server`; pytest + pytest-playwright (Chromium); `uv run`.

---

## Verified facts (do not re-derive)

- `Zaghetto.pcd`: binary PCD, header `FIELDS x y z` (no RGB), `POINTS 59750`, ~721 KB. Color-by-height is computed from the Z coordinate.
- Vendored files and their internal bare imports (all resolved by the import map):
  - `three.module.js` (unpkg `three@0.160.0/build/three.module.js`) — no imports.
  - `examples/jsm/controls/OrbitControls.js` — `} from 'three';`
  - `examples/jsm/loaders/PCDLoader.js` — `} from 'three';`
  - `preact.module.js` (`preact@10.19.3/dist/preact.module.js`) — no imports.
  - `hooks.module.js` (`preact@10.19.3/hooks/dist/hooks.module.js`) — `from"preact"`.
  - `htm.module.js` (`htm@3.1.1/dist/htm.module.js`) — no imports.
- Import map (in `index.html`):
  ```json
  {
    "imports": {
      "three": "./vendor/three.module.js",
      "three/addons/": "./vendor/addons/",
      "preact": "./vendor/preact.module.js",
      "preact/hooks": "./vendor/preact-hooks.module.js",
      "htm": "./vendor/htm.module.js"
    }
  }
  ```

## File structure

```
pcl-viewer/
├── web/
│   ├── index.html        # import map + #app mount + module script
│   ├── app.js            # Preact root (htm): ControlPanel + StatsOverlay, drives viewer handle
│   ├── viewer.js         # three.js: scene/camera/renderer/OrbitControls/PCDLoader/helpers + window.__PCL
│   ├── styles.css
│   ├── models/Zaghetto.pcd   # committed sample (MIT, three.js)
│   └── vendor/           # gitignored; populated by vendor.sh
├── tests/
│   └── test_e2e.py       # Playwright e2e
├── conftest.py           # pytest fixtures: ensure vendored, start/stop server on free port
├── serve.py              # http.server wrapper (MIME + no-store), CLI --port/--directory
├── vendor.sh             # downloads pinned three/preact/htm into web/vendor/
├── pcl-viewer.sh         # runner: vendor.sh + serve.py + print URL
├── pyproject.toml        # standalone project: pytest + pytest-playwright
├── README.md
└── SPEC.md
```

---

## Task 1: Project scaffold + Python static server (TDD)

**Files:**
- Create: `pcl-viewer/serve.py`
- Create: `pcl-viewer/web/index.html` (temporary minimal page for this task)
- Create: `pcl-viewer/pyproject.toml`
- Create: `pcl-viewer/conftest.py`
- Test: `pcl-viewer/tests/test_server.py`

- [ ] **Step 1: Create the project pyproject.toml**

Create `pcl-viewer/pyproject.toml`:

```toml
[project]
name = "pcl-viewer"
version = "0.1.0"
description = "No-build ESM point cloud viewer (three.js + Preact) with Playwright e2e."
requires-python = ">=3.10"
dependencies = []

[dependency-groups]
dev = [
    "pytest>=8",
    "pytest-playwright>=0.5",
]
```

- [ ] **Step 2: Create a temporary minimal index.html**

Create `pcl-viewer/web/index.html` (replaced in Task 3):

```html
<!doctype html>
<html lang="en">
  <head><meta charset="utf-8" /><title>PCL Viewer</title></head>
  <body><div id="app">scaffold</div></body>
</html>
```

- [ ] **Step 3: Write the failing server test**

Create `pcl-viewer/tests/test_server.py`:

```python
import urllib.request


def test_serves_index_html(server_url):
    with urllib.request.urlopen(server_url + "/") as resp:
        body = resp.read().decode()
        assert resp.status == 200
        assert "<div id=\"app\">" in body


def test_serves_js_with_module_mime(server_url, tmp_path):
    # A .js file under web/ must be served with a JS MIME so <script type=module> loads.
    with urllib.request.urlopen(server_url + "/app.js") as resp:
        assert resp.status == 200
        assert "javascript" in resp.headers.get("Content-Type", "")


def test_serves_pcd_as_octet_stream(server_url):
    with urllib.request.urlopen(server_url + "/models/Zaghetto.pcd") as resp:
        assert resp.status == 200
        assert resp.headers.get("Content-Type") == "application/octet-stream"
```

Note: `app.js` and `models/Zaghetto.pcd` are added in later tasks; these two assertions will pass once those files exist. For this task, only `test_serves_index_html` must pass — run just that one in Step 6.

- [ ] **Step 4: Implement serve.py**

Create `pcl-viewer/serve.py`:

```python
"""Minimal static file server for the PCL viewer.

Serves the web/ directory with correct MIME types for ES modules and .pcd
assets, and disables caching so edits show up immediately.
"""
from __future__ import annotations

import argparse
import os
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer


class ViewerHandler(SimpleHTTPRequestHandler):
    extensions_map = {
        **SimpleHTTPRequestHandler.extensions_map,
        ".js": "text/javascript",
        ".mjs": "text/javascript",
        ".pcd": "application/octet-stream",
        ".css": "text/css",
        ".html": "text/html",
    }

    def end_headers(self):
        self.send_header("Cache-Control", "no-store")
        super().end_headers()

    def log_message(self, *args):  # keep test output quiet
        pass


def make_server(directory: str, port: int) -> ThreadingHTTPServer:
    handler = partial(ViewerHandler, directory=directory)
    return ThreadingHTTPServer(("127.0.0.1", port), handler)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    here = os.path.dirname(os.path.abspath(__file__))
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--directory", default=os.path.join(here, "web"))
    args = parser.parse_args()
    httpd = make_server(args.directory, args.port)
    host, port = httpd.server_address
    print(f"PCL viewer serving {args.directory} at http://{host}:{port}/")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        httpd.shutdown()


if __name__ == "__main__":
    main()
```

- [ ] **Step 5: Implement the server fixture in conftest.py**

Create `pcl-viewer/conftest.py`:

```python
"""Pytest fixtures: ensure vendored libs exist, run the static server."""
from __future__ import annotations

import os
import socket
import subprocess
import threading
import time
import urllib.request

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
WEB = os.path.join(HERE, "web")
VENDOR = os.path.join(WEB, "vendor")


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="session", autouse=True)
def vendored():
    """Ensure web/vendor/ is populated (runs vendor.sh once if missing)."""
    sentinel = os.path.join(VENDOR, "three.module.js")
    if not os.path.exists(sentinel):
        subprocess.run(["bash", os.path.join(HERE, "vendor.sh")], check=True)
    assert os.path.exists(sentinel), "vendor.sh did not populate web/vendor/"


@pytest.fixture()
def server_url(vendored):
    from serve import make_server  # imported here so HERE is on sys.path

    port = _free_port()
    httpd = make_server(WEB, port)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{port}"
    # wait until it accepts a connection
    for _ in range(50):
        try:
            urllib.request.urlopen(base + "/", timeout=0.2)
            break
        except OSError:
            time.sleep(0.05)
    try:
        yield base
    finally:
        httpd.shutdown()
        thread.join(timeout=2)
```

Add `pcl-viewer/` to `sys.path` for the `from serve import ...` line by creating `pcl-viewer/pytest.ini`:

Create `pcl-viewer/pytest.ini`:

```ini
[pytest]
pythonpath = .
```

- [ ] **Step 6: Run the index test to verify it passes**

Run: `cd pcl-viewer && uv run --group dev pytest tests/test_server.py::test_serves_index_html -v`
Expected: PASS. (vendor.sh does not exist yet — see Task 2; if the `vendored` autouse fixture fails because `vendor.sh` is missing, temporarily skip it by running with `-p no:cacheprovider` is NOT enough; instead implement Task 2 first if the runner blocks. The autouse fixture will error without vendor.sh.)

> Execution note: Task 1 and Task 2 are tightly coupled (the autouse `vendored` fixture needs `vendor.sh`). If executing strictly task-by-task, create `vendor.sh` (Task 2 Step 1) before running this step.

- [ ] **Step 7: Commit**

```bash
git add pcl-viewer/serve.py pcl-viewer/web/index.html pcl-viewer/pyproject.toml \
        pcl-viewer/conftest.py pcl-viewer/pytest.ini pcl-viewer/tests/test_server.py
git commit -m "feat(pcl-viewer): scaffold project and static file server"
```

---

## Task 2: Vendor script + gitignore

**Files:**
- Create: `pcl-viewer/vendor.sh`
- Modify: `.gitignore` (repo root)

- [ ] **Step 1: Create vendor.sh**

Create `pcl-viewer/vendor.sh`:

```bash
#!/usr/bin/env bash
# Download pinned ESM builds of three.js, preact, and htm into web/vendor/.
# Idempotent: skips files that already exist. Required once (needs network).
set -euo pipefail
cd "$(dirname "$0")"

THREE=0.160.0
PREACT=10.19.3
HTM=3.1.1
BASE=https://unpkg.com
DEST=web/vendor

mkdir -p "$DEST/addons/controls" "$DEST/addons/loaders"

fetch() { # url dest
  if [ -f "$2" ]; then echo "have   $2"; return; fi
  echo "fetch  $2"
  curl -fsSL "$1" -o "$2"
}

fetch "$BASE/three@$THREE/build/three.module.js"                     "$DEST/three.module.js"
fetch "$BASE/three@$THREE/examples/jsm/controls/OrbitControls.js"    "$DEST/addons/controls/OrbitControls.js"
fetch "$BASE/three@$THREE/examples/jsm/loaders/PCDLoader.js"         "$DEST/addons/loaders/PCDLoader.js"
fetch "$BASE/preact@$PREACT/dist/preact.module.js"                   "$DEST/preact.module.js"
fetch "$BASE/preact@$PREACT/hooks/dist/hooks.module.js"              "$DEST/preact-hooks.module.js"
fetch "$BASE/htm@$HTM/dist/htm.module.js"                            "$DEST/htm.module.js"

echo "vendored into $DEST"
```

- [ ] **Step 2: Make it executable**

Run: `chmod +x pcl-viewer/vendor.sh`

- [ ] **Step 3: Run it and verify the files land**

Run: `cd pcl-viewer && bash vendor.sh && ls web/vendor web/vendor/addons/controls web/vendor/addons/loaders`
Expected: lists `three.module.js`, `preact.module.js`, `preact-hooks.module.js`, `htm.module.js`, `addons/controls/OrbitControls.js`, `addons/loaders/PCDLoader.js`.

- [ ] **Step 4: Ignore the vendored directory**

Append to the repo-root `.gitignore`:

```
# pcl-viewer vendored ESM libs (fetched by pcl-viewer/vendor.sh)
pcl-viewer/web/vendor/
```

- [ ] **Step 5: Verify it is ignored**

Run: `git check-ignore pcl-viewer/web/vendor/three.module.js`
Expected: prints the path (it is ignored).

- [ ] **Step 6: Commit**

```bash
git add pcl-viewer/vendor.sh .gitignore
git commit -m "feat(pcl-viewer): add vendor.sh to fetch pinned ESM libs"
```

---

## Task 3: three.js viewer module + import-map page

**Files:**
- Create: `pcl-viewer/web/viewer.js`
- Replace: `pcl-viewer/web/index.html`
- Create: `pcl-viewer/web/styles.css`
- Create: `pcl-viewer/web/models/Zaghetto.pcd` (downloaded)

- [ ] **Step 1: Download the bundled point cloud asset**

Run:
```bash
mkdir -p pcl-viewer/web/models
curl -fsSL https://raw.githubusercontent.com/mrdoob/three.js/dev/examples/models/pcd/binary/Zaghetto.pcd \
  -o pcl-viewer/web/models/Zaghetto.pcd
head -c 64 pcl-viewer/web/models/Zaghetto.pcd
```
Expected: starts with `# .PCD v0.7 - Point Cloud Data file format`.

- [ ] **Step 2: Write viewer.js**

Create `pcl-viewer/web/viewer.js`:

```javascript
// three.js viewer: owns all 3D state behind a small imperative handle.
// Knows nothing about Preact. Exposes window.__PCL for deterministic e2e.
import * as THREE from 'three';
import { OrbitControls } from 'three/addons/controls/OrbitControls.js';
import { PCDLoader } from 'three/addons/loaders/PCDLoader.js';

const BG = 0x101418;
const FLAT_COLOR = 0x66ccff;

export function createViewer(canvas, { modelUrl = './models/Zaghetto.pcd' } = {}) {
  const renderer = new THREE.WebGLRenderer({
    canvas,
    antialias: true,
    preserveDrawingBuffer: true, // lets e2e read pixels at any time
  });
  renderer.setPixelRatio(window.devicePixelRatio || 1);

  const scene = new THREE.Scene();
  scene.background = new THREE.Color(BG);

  const camera = new THREE.PerspectiveCamera(45, 1, 0.001, 1000);
  const controls = new OrbitControls(camera, renderer.domElement);
  controls.enableDamping = true;

  let points = null;
  let baseColors = null; // Float32Array of height-mapped colors
  const helpers = new THREE.Group();
  helpers.visible = false;
  scene.add(helpers);

  const state = {
    ready: false,
    pointCount: 0,
    settings: { pointSize: 0.01, colorMode: 'flat', helpers: false },
    framesRendered: 0,
  };
  window.__PCL = state;

  function resize() {
    const w = canvas.clientWidth || canvas.parentElement.clientWidth || 800;
    const h = canvas.clientHeight || canvas.parentElement.clientHeight || 600;
    renderer.setSize(w, h, false);
    camera.aspect = w / h;
    camera.updateProjectionMatrix();
  }
  window.addEventListener('resize', resize);

  function frameCamera() {
    const box = new THREE.Box3().setFromObject(points);
    const size = box.getSize(new THREE.Vector3());
    const center = box.getCenter(new THREE.Vector3());
    const radius = Math.max(size.x, size.y, size.z);
    controls.target.copy(center);
    camera.position.copy(center).add(new THREE.Vector3(0, 0, radius * 2.2));
    camera.near = radius / 100;
    camera.far = radius * 100;
    camera.updateProjectionMatrix();
    controls.update();
  }

  function computeHeightColors(geometry) {
    const pos = geometry.getAttribute('position');
    const box = new THREE.Box3().setFromBufferAttribute(pos);
    const minZ = box.min.z;
    const span = box.max.z - box.min.z || 1;
    const colors = new Float32Array(pos.count * 3);
    const c = new THREE.Color();
    for (let i = 0; i < pos.count; i++) {
      const t = (pos.getZ(i) - minZ) / span;
      c.setHSL(0.7 - 0.7 * t, 0.9, 0.5); // blue (low) -> red (high)
      colors[i * 3] = c.r;
      colors[i * 3 + 1] = c.g;
      colors[i * 3 + 2] = c.b;
    }
    return colors;
  }

  function applyColorMode(mode) {
    if (!points) return;
    state.settings.colorMode = mode;
    if (mode === 'height') {
      points.geometry.setAttribute(
        'color', new THREE.BufferAttribute(baseColors, 3));
      points.material.vertexColors = true;
      points.material.color.set(0xffffff);
    } else {
      points.geometry.deleteAttribute('color');
      points.material.vertexColors = false;
      points.material.color.set(FLAT_COLOR);
    }
    points.material.needsUpdate = true;
  }

  function buildHelpers() {
    helpers.clear();
    const box = new THREE.Box3().setFromObject(points);
    helpers.add(new THREE.Box3Helper(box, 0xffaa00));
    const size = box.getSize(new THREE.Vector3()).length();
    helpers.add(new THREE.AxesHelper(size * 0.5));
  }

  const loader = new PCDLoader();
  loader.load(modelUrl, (loaded) => {
    points = loaded;
    points.material = new THREE.PointsMaterial({
      size: state.settings.pointSize,
      color: FLAT_COLOR,
      sizeAttenuation: true,
    });
    scene.add(points);
    baseColors = computeHeightColors(points.geometry);
    state.pointCount = points.geometry.getAttribute('position').count;
    buildHelpers();
    resize();
    frameCamera();
    state.ready = true;
  });

  let raf = 0;
  function tick() {
    raf = requestAnimationFrame(tick);
    controls.update();
    renderer.render(scene, camera);
    state.framesRendered++;
  }
  resize();
  tick();

  const handle = {
    setPointSize(n) {
      state.settings.pointSize = n;
      if (points) { points.material.size = n; points.material.needsUpdate = true; }
    },
    setColorMode(mode) { applyColorMode(mode); },
    resetCamera() { if (points) frameCamera(); },
    toggleHelpers(on) { helpers.visible = on; state.settings.helpers = on; },
    getStats() {
      return {
        pointCount: state.pointCount,
        cameraDistance: camera.position.distanceTo(controls.target),
      };
    },
    // e2e helper: count non-background pixels in the rendered frame.
    visiblePixelCount() {
      const gl = renderer.getContext();
      const w = renderer.domElement.width;
      const h = renderer.domElement.height;
      const buf = new Uint8Array(w * h * 4);
      gl.readPixels(0, 0, w, h, gl.RGBA, gl.UNSIGNED_BYTE, buf);
      const br = ((BG >> 16) & 255), bg = ((BG >> 8) & 255), bb = BG & 255;
      let n = 0;
      for (let i = 0; i < buf.length; i += 4) {
        if (Math.abs(buf[i] - br) + Math.abs(buf[i + 1] - bg) + Math.abs(buf[i + 2] - bb) > 24) n++;
      }
      return n;
    },
    dispose() {
      cancelAnimationFrame(raf);
      window.removeEventListener('resize', resize);
      renderer.dispose();
    },
  };
  state.handle = handle; // expose for e2e
  return handle;
}
```

- [ ] **Step 3: Write styles.css**

Create `pcl-viewer/web/styles.css`:

```css
* { box-sizing: border-box; }
html, body { margin: 0; height: 100%; background: #101418; color: #e6edf3;
  font: 14px/1.4 system-ui, sans-serif; }
#app { position: relative; height: 100vh; }
#scene { display: block; width: 100%; height: 100%; }
.panel { position: absolute; background: rgba(20, 26, 33, 0.82);
  border: 1px solid #2a3340; border-radius: 8px; padding: 12px 14px; }
.controls { top: 12px; left: 12px; width: 220px; }
.controls h1 { font-size: 14px; margin: 0 0 10px; }
.controls label { display: block; margin: 10px 0 4px; font-size: 12px; color: #9fb0c0; }
.controls input[type=range], .controls select { width: 100%; }
.controls .row { display: flex; gap: 8px; margin-top: 12px; }
.controls button { flex: 1; cursor: pointer; background: #1f6feb; color: #fff;
  border: 0; border-radius: 6px; padding: 7px; }
.controls .toggle { display: flex; align-items: center; gap: 8px; margin-top: 12px; }
.stats { top: 12px; right: 12px; font-variant-numeric: tabular-nums; }
.stats div { margin: 2px 0; }
.stats b { color: #58a6ff; }
```

- [ ] **Step 4: Replace index.html with the real page**

Replace the contents of `pcl-viewer/web/index.html`:

```html
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>PCL Viewer — three.js + Preact</title>
    <link rel="stylesheet" href="./styles.css" />
    <script type="importmap">
      {
        "imports": {
          "three": "./vendor/three.module.js",
          "three/addons/": "./vendor/addons/",
          "preact": "./vendor/preact.module.js",
          "preact/hooks": "./vendor/preact-hooks.module.js",
          "htm": "./vendor/htm.module.js"
        }
      }
    </script>
  </head>
  <body>
    <div id="app"></div>
    <script type="module" src="./app.js"></script>
  </body>
</html>
```

- [ ] **Step 5: Commit**

```bash
git add pcl-viewer/web/viewer.js pcl-viewer/web/styles.css \
        pcl-viewer/web/index.html pcl-viewer/web/models/Zaghetto.pcd
git commit -m "feat(pcl-viewer): three.js viewer module, page, styles, and PCD asset"
```

---

## Task 4: Preact UI (app.js)

**Files:**
- Create: `pcl-viewer/web/app.js`

- [ ] **Step 1: Write app.js**

Create `pcl-viewer/web/app.js`:

```javascript
// Preact UI (no JSX build — htm). Owns UI state and drives the viewer handle.
import { h, render } from 'preact';
import { useEffect, useRef, useState } from 'preact/hooks';
import htm from 'htm';
import { createViewer } from './viewer.js';

const html = htm.bind(h);

function App() {
  const canvasRef = useRef(null);
  const viewerRef = useRef(null);
  const [pointSize, setPointSize] = useState(0.01);
  const [colorMode, setColorMode] = useState('flat');
  const [showHelpers, setShowHelpers] = useState(false);
  const [stats, setStats] = useState({ pointCount: 0, fps: 0, cameraDistance: 0 });

  useEffect(() => {
    const viewer = createViewer(canvasRef.current);
    viewerRef.current = viewer;

    let frames = 0, last = performance.now(), fps = 0, raf = 0;
    const loop = () => {
      raf = requestAnimationFrame(loop);
      frames++;
      const now = performance.now();
      if (now - last >= 500) {
        fps = Math.round((frames * 1000) / (now - last));
        frames = 0; last = now;
        const s = viewer.getStats();
        setStats({ pointCount: s.pointCount, fps, cameraDistance: s.cameraDistance });
      }
    };
    loop();
    return () => { cancelAnimationFrame(raf); viewer.dispose(); };
  }, []);

  const onSize = (e) => {
    const v = parseFloat(e.target.value);
    setPointSize(v); viewerRef.current.setPointSize(v);
  };
  const onColor = (e) => {
    setColorMode(e.target.value); viewerRef.current.setColorMode(e.target.value);
  };
  const onHelpers = (e) => {
    setShowHelpers(e.target.checked); viewerRef.current.toggleHelpers(e.target.checked);
  };
  const onReset = () => viewerRef.current.resetCamera();

  return html`
    <canvas id="scene" ref=${canvasRef}></canvas>
    <div class="panel controls">
      <h1>PCL Viewer</h1>
      <label>Point size: ${pointSize.toFixed(3)}</label>
      <input type="range" min="0.002" max="0.05" step="0.001"
             value=${pointSize} data-testid="point-size" onInput=${onSize} />
      <label>Color mode</label>
      <select data-testid="color-mode" value=${colorMode} onChange=${onColor}>
        <option value="flat">Flat</option>
        <option value="height">By height</option>
      </select>
      <div class="toggle">
        <input type="checkbox" id="helpers" data-testid="helpers"
               checked=${showHelpers} onChange=${onHelpers} />
        <label for="helpers" style="margin:0">Show box + axes</label>
      </div>
      <div class="row">
        <button data-testid="reset" onClick=${onReset}>Reset camera</button>
      </div>
    </div>
    <div class="panel stats" data-testid="stats">
      <div>Points: <b data-testid="point-count">${stats.pointCount.toLocaleString()}</b></div>
      <div>FPS: <b>${stats.fps}</b></div>
      <div>Camera dist: <b>${stats.cameraDistance.toFixed(2)}</b></div>
    </div>
  `;
}

render(html`<${App} />`, document.getElementById('app'));
```

- [ ] **Step 2: Manual smoke check (optional, if a browser is available)**

Run: `cd pcl-viewer && bash vendor.sh && uv run python serve.py --port 8011` then open `http://127.0.0.1:8011/`. Expected: point cloud renders, controls work. Ctrl-C to stop. (The Playwright test in Task 5 is the authoritative check.)

- [ ] **Step 3: Commit**

```bash
git add pcl-viewer/web/app.js
git commit -m "feat(pcl-viewer): Preact control panel and stats overlay"
```

---

## Task 5: Playwright e2e test

**Files:**
- Create: `pcl-viewer/tests/test_e2e.py`

- [ ] **Step 1: Ensure the Chromium browser is installed**

Run: `cd pcl-viewer && uv run --group dev playwright install chromium`
Expected: downloads/installs Chromium (no error). Run once per environment.

- [ ] **Step 2: Write the e2e test**

Create `pcl-viewer/tests/test_e2e.py`:

```python
"""End-to-end test of the PCL viewer using Playwright (Chromium)."""
from playwright.sync_api import expect


def _wait_ready(page):
    page.wait_for_function("() => window.__PCL && window.__PCL.ready === true",
                           timeout=20000)


def test_point_cloud_loads_and_renders(server_url, page):
    page.goto(server_url + "/")
    _wait_ready(page)

    point_count = page.evaluate("() => window.__PCL.pointCount")
    assert point_count == 59750

    # Stats overlay reflects the loaded cloud.
    expect(page.get_by_test_id("point-count")).to_have_text("59,750")

    # The canvas actually drew the cloud (non-background pixels present).
    visible = page.evaluate("() => window.__PCL.handle.visiblePixelCount()")
    assert visible > 1000


def test_point_size_control(server_url, page):
    page.goto(server_url + "/")
    _wait_ready(page)
    slider = page.get_by_test_id("point-size")
    slider.evaluate(
        "el => { el.value = '0.05'; el.dispatchEvent(new Event('input', {bubbles:true})); }")
    page.wait_for_function("() => Math.abs(window.__PCL.settings.pointSize - 0.05) < 1e-6")


def test_color_mode_toggle(server_url, page):
    page.goto(server_url + "/")
    _wait_ready(page)
    page.get_by_test_id("color-mode").select_option("height")
    page.wait_for_function("() => window.__PCL.settings.colorMode === 'height'")


def test_helpers_toggle(server_url, page):
    page.goto(server_url + "/")
    _wait_ready(page)
    page.get_by_test_id("helpers").check()
    page.wait_for_function("() => window.__PCL.settings.helpers === true")


def test_reset_camera(server_url, page):
    page.goto(server_url + "/")
    _wait_ready(page)
    # Orbit far away via wheel, then reset and confirm still ready & rendering.
    before = page.evaluate("() => window.__PCL.framesRendered")
    page.get_by_test_id("reset").click()
    page.wait_for_function(f"() => window.__PCL.framesRendered > {before}")
    assert page.evaluate("() => window.__PCL.ready") is True


def test_screenshot_capture(server_url, page, tmp_path):
    page.goto(server_url + "/")
    _wait_ready(page)
    page.wait_for_timeout(300)  # let a few frames render
    out = tmp_path / "pcl-viewer.png"
    page.screenshot(path=str(out))
    assert out.stat().st_size > 5000
```

- [ ] **Step 3: Run the full test suite**

Run: `cd pcl-viewer && uv run --group dev pytest -v`
Expected: all tests in `test_server.py` and `test_e2e.py` PASS. (The `vendored` autouse fixture runs `vendor.sh` if needed; Chromium must be installed from Step 1.)

- [ ] **Step 4: Commit**

```bash
git add pcl-viewer/tests/test_e2e.py
git commit -m "test(pcl-viewer): Playwright e2e covering load, controls, and render"
```

---

## Task 6: Runner script

**Files:**
- Create: `pcl-viewer/pcl-viewer.sh`

- [ ] **Step 1: Write pcl-viewer.sh**

Create `pcl-viewer/pcl-viewer.sh`:

```bash
#!/usr/bin/env bash
# User-facing entry point: vendor ESM libs (once), then serve the viewer.
set -euo pipefail
cd "$(dirname "$0")"

PYTHON="${PYTHON:-uv run python}"
PORT="${PORT:-8000}"

bash vendor.sh
echo "Open http://127.0.0.1:${PORT}/ in a browser (Ctrl-C to stop)."
exec $PYTHON serve.py --port "$PORT"
```

- [ ] **Step 2: Make it executable**

Run: `chmod +x pcl-viewer/pcl-viewer.sh`

- [ ] **Step 3: Smoke-check it starts**

Run: `cd pcl-viewer && timeout 4 ./pcl-viewer.sh || true`
Expected: prints the vendoring lines and `... serving ... at http://127.0.0.1:8000/`, then exits on timeout.

- [ ] **Step 4: Commit**

```bash
git add pcl-viewer/pcl-viewer.sh
git commit -m "feat(pcl-viewer): add ./pcl-viewer.sh runner"
```

---

## Task 7: README + SPEC docs

**Files:**
- Create: `pcl-viewer/README.md`
- Create: `pcl-viewer/SPEC.md`

- [ ] **Step 1: Write README.md (follows the repo's README convention)**

Create `pcl-viewer/README.md`:

```markdown
PCL Viewer — three.js + Preact
---

A no-build, ES-module point cloud viewer: it loads the MIT-licensed `Zaghetto.pcd`
sample with three.js's `PCDLoader`, renders it with `OrbitControls`, and wraps the
scene in a Preact UI (point-size slider, flat/by-height color modes, a live stats
overlay, reset-camera, and box/axes helpers). Frontend dependencies are vendored
ES modules loaded via an import map — no bundler — served by a small Python
`http.server` and verified end-to-end with Python Playwright.

\```bash
./pcl-viewer.sh
\```
```

(Use real backticks for the code fence — the `\`` above are escaped only for this plan.)

- [ ] **Step 2: Write SPEC.md**

Create `pcl-viewer/SPEC.md`:

```markdown
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
```

- [ ] **Step 3: Commit**

```bash
git add pcl-viewer/README.md pcl-viewer/SPEC.md
git commit -m "docs(pcl-viewer): README and SPEC"
```

---

## Task 8: Final verification + push

- [ ] **Step 1: Clean-room test run**

Run:
```bash
cd pcl-viewer
rm -rf web/vendor
uv run --group dev playwright install chromium
uv run --group dev pytest -v
```
Expected: `vendor.sh` repopulates `web/vendor/`, then every test passes.

- [ ] **Step 2: Confirm vendored libs and the screenshot are not committed**

Run: `git status --porcelain pcl-viewer/web/vendor` and `git ls-files pcl-viewer/web/vendor`
Expected: both empty (nothing tracked or pending under `web/vendor/`).

- [ ] **Step 3: Push the branch**

Run: `git push -u origin claude/pcl-viewer-threejs-preact-knwkgb`
Expected: branch pushed. (Do NOT open a PR unless the user asks.)

---

## Self-review notes

- **Spec coverage:** data source (Task 3 asset) ✓, vendored ESM import map (Tasks 2–3) ✓, orbit controls (Task 3) ✓, point size + color (Tasks 3–4) ✓, stats overlay (Task 4) ✓, reset + helpers (Tasks 3–4) ✓, Python server (Task 1) ✓, `window.__PCL` hook (Task 3) ✓, Playwright e2e flow (Task 5) ✓, runner (Task 6) ✓, README/SPEC convention (Task 7) ✓.
- **Type/name consistency:** handle methods (`setPointSize`, `setColorMode`, `resetCamera`, `toggleHelpers`, `getStats`, `visiblePixelCount`, `dispose`) are defined in `viewer.js` (Task 3) and called identically in `app.js` (Task 4) and `test_e2e.py` (Task 5). `window.__PCL` fields (`ready`, `pointCount`, `settings.{pointSize,colorMode,helpers}`, `framesRendered`, `handle`) are set in Task 3 and read in Task 5. `data-testid`s (`point-size`, `color-mode`, `helpers`, `reset`, `stats`, `point-count`) match between Task 4 and Task 5.
- **Coupling note:** Task 1's autouse `vendored` fixture needs `vendor.sh` (Task 2). Flagged in Task 1 Step 6 — create `vendor.sh` first if executing strictly in order.
```
