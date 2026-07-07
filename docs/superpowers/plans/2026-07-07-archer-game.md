# Archer Game Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `archer/`, a standalone no-build three.js browser game: a stationary first-person archer defends against waves of low-poly fantasy enemies across forest → desert → iceberg stages, with charged ballistic shots and exploding/freezing/burning arrow drops.

**Architecture:** Vendored ES modules (pcl-viewer is the reference stack — `vendor.sh` + import map + `serve.py`), a flat set of focused game modules composed by `main.js`'s frame loop around a shared `game` context object, a Preact+htm UI overlay driven by a tiny observable store, and a `window.__ARCHER` test handle with a seeded RNG for deterministic Python Playwright e2e tests.

**Tech Stack:** three.js 0.160.0, Preact 10.19.3 + htm 3.1.1 (all vendored ESM, no bundler), Python 3.10+ stdlib `http.server`, pytest + pytest-playwright.

**Spec:** `docs/superpowers/specs/2026-07-07-archer-game-design.md`

## Global Constraints

- Vendored pins (copied from pcl-viewer): `three@0.160.0`, `preact@10.19.3`, `htm@3.1.1`, fetched from `https://unpkg.com` by `archer/vendor.sh` into `archer/web/vendor/` (gitignored).
- No bundler, no npm, no TypeScript: plain browser ES modules loaded via an import map.
- No physics engine; arrows integrate gravity manually, collisions are sphere-vs-sphere distance checks.
- No external asset files: all meshes built from three.js primitives.
- All gameplay randomness flows through ONE seeded mulberry32 RNG (`rng.js`), seedable via `?seed=N`.
- All tuning constants live in `web/config.js` — no magic numbers in game modules.
- Test handle is `window.__ARCHER`; UI elements carry `data-testid` attributes.
- Python: `requires-python = ">=3.10"`; dev deps only `pytest>=8`, `pytest-playwright>=0.5`.
- Every shell script starts with `#!/usr/bin/env bash` + `set -euo pipefail` and `cd "$(dirname "$0")"`.
- README.md must follow the repo convention exactly: setext title, one paragraph, one bash block (see CLAUDE.md).
- Run tests from the `archer/` directory: `uv run --group dev pytest tests/ -x -q` (add `--browser chromium` implicitly via pytest-playwright defaults).
- Commit messages end with the two Co-Authored-By/Claude-Session trailer lines used by this session's git configuration (see repository instructions); the `git commit` blocks below omit them for brevity.

---

### Task 1: Project scaffold — serving, vendoring, boot render, test infra

**Files:**
- Create: `archer/archer.sh`, `archer/vendor.sh`, `archer/serve.py`, `archer/pyproject.toml`, `archer/pytest.ini`, `archer/conftest.py`
- Create: `archer/web/index.html`, `archer/web/styles.css`, `archer/web/config.js`, `archer/web/rng.js`, `archer/web/main.js`
- Create: `archer/tests/test_e2e.py`
- Modify: `.gitignore` (repo root)

**Interfaces:**
- Consumes: nothing (first task).
- Produces:
  - `web/config.js`: `export const CONFIG` — full tuning object (verbatim below); later tasks read `CONFIG.player`, `CONFIG.bow`, `CONFIG.arrow`, `CONFIG.enemies`, `CONFIG.drops`, `CONFIG.waves`, `CONFIG.stages`, `CONFIG.arena`, `CONFIG.attackRange`, `CONFIG.headshotBonus`.
  - `web/rng.js`: `createRng(seed)` → `{ random(), range(min,max), int(min,max), pick(arr) }`; `seedFromQuery(params)` → integer.
  - `web/main.js`: owns `renderer`, `scene`, `camera`, the `game` context object (`{ scene, camera, params, rng, stats, screen, syncUI }`), the frame loop, and `window.__ARCHER` (`ready`, `state` getter, `visiblePixelCount()`). Later tasks extend `game` and `__ARCHER` — anchor comments `// [task-N]` mark insertion points.
  - `conftest.py`: session fixtures `vendored`, `browser_type_launch_args`, and per-test `server_url` (serves `web/` on a free port).
- Test: `archer/tests/test_e2e.py`

- [ ] **Step 1: Add gitignore entries**

Append to the repo root `.gitignore`:

```gitignore

# archer vendored ESM libs (fetched by archer/vendor.sh)
archer/web/vendor/

# archer Playwright test output
archer/test-results/
```

- [ ] **Step 2: Write the failing e2e boot test**

Create `archer/tests/test_e2e.py`:

```python
"""End-to-end tests for the archer game (Playwright, Chromium)."""
from playwright.sync_api import expect

# Deterministic, menu-skipping boot used by most tests.
BOOT = "/?autostart=1&seed=42"


def _wait_ready(page):
    page.wait_for_function(
        "() => window.__ARCHER && window.__ARCHER.ready === true", timeout=30000
    )


def test_boot_renders(server_url, page):
    page.goto(server_url + BOOT)
    _wait_ready(page)
    state = page.evaluate("() => window.__ARCHER.state")
    assert state["screen"] == "playing"
    # The canvas actually drew something (sky background is excluded).
    assert page.evaluate("() => window.__ARCHER.visiblePixelCount()") > 500
```

- [ ] **Step 3: Run the test to verify it fails**

```bash
cd archer && uv run --group dev pytest tests/test_e2e.py -x -q
```

Expected: FAIL (no `conftest.py`/fixtures yet — collection error). After the remaining files below exist, the failure becomes a Playwright timeout waiting for `__ARCHER`.

- [ ] **Step 4: Write vendor.sh**

Create `archer/vendor.sh` (mode 755):

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

mkdir -p "$DEST"

fetch() { # url dest
  if [ -f "$2" ]; then echo "have   $2"; return; fi
  echo "fetch  $2"
  curl -fsSL "$1" -o "$2.tmp" && mv "$2.tmp" "$2"
}

fetch "$BASE/three@$THREE/build/three.module.js"        "$DEST/three.module.js"
fetch "$BASE/preact@$PREACT/dist/preact.module.js"      "$DEST/preact.module.js"
fetch "$BASE/preact@$PREACT/hooks/dist/hooks.module.js" "$DEST/preact-hooks.module.js"
fetch "$BASE/htm@$HTM/dist/htm.module.js"               "$DEST/htm.module.js"

echo "vendored into $DEST"
```

- [ ] **Step 5: Write serve.py**

Create `archer/serve.py` (same pattern as `pcl-viewer/serve.py`):

```python
"""Minimal static file server for the archer game.

Serves the web/ directory with correct MIME types for ES modules and
disables caching so edits show up immediately.
"""
from __future__ import annotations

import argparse
import os
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer


class GameHandler(SimpleHTTPRequestHandler):
    extensions_map = {
        **SimpleHTTPRequestHandler.extensions_map,
        ".js": "text/javascript",
        ".mjs": "text/javascript",
        ".css": "text/css",
        ".html": "text/html",
    }

    def end_headers(self):
        self.send_header("Cache-Control", "no-store")
        super().end_headers()

    def log_message(self, *args):  # keep test output quiet
        pass


def make_server(directory: str, port: int) -> ThreadingHTTPServer:
    handler = partial(GameHandler, directory=directory)
    return ThreadingHTTPServer(("127.0.0.1", port), handler)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    here = os.path.dirname(os.path.abspath(__file__))
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--directory", default=os.path.join(here, "web"))
    args = parser.parse_args()
    httpd = make_server(args.directory, args.port)
    host, port = httpd.server_address
    print(f"archer serving {args.directory} at http://{host}:{port}/")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        httpd.shutdown()


if __name__ == "__main__":
    main()
```

- [ ] **Step 6: Write archer.sh**

Create `archer/archer.sh` (mode 755):

```bash
#!/usr/bin/env bash
# User-facing entry point: vendor ESM libs (once), then serve the game.
set -euo pipefail
cd "$(dirname "$0")"

PYTHON="${PYTHON:-uv run python}"
PORT="${PORT:-8000}"

bash vendor.sh
echo "Open http://127.0.0.1:${PORT}/ in a browser (Ctrl-C to stop)."
exec $PYTHON serve.py --port "$PORT"
```

- [ ] **Step 7: Write pyproject.toml and pytest.ini**

Create `archer/pyproject.toml`:

```toml
[project]
name = "archer"
version = "0.1.0"
description = "No-build three.js wave-defense archery game with Playwright e2e."
requires-python = ">=3.10"
dependencies = []

[dependency-groups]
dev = [
    "pytest>=8",
    "pytest-playwright>=0.5",
]
```

Create `archer/pytest.ini`:

```ini
[pytest]
pythonpath = .
```

- [ ] **Step 8: Write conftest.py**

Create `archer/conftest.py` (trimmed copy of `pcl-viewer/conftest.py` — no fixture staging, no modal-closing fixture):

```python
"""Pytest fixtures: ensure vendored libs exist, run the static server."""
from __future__ import annotations

import glob
import os
import socket
import subprocess
import threading
import time
import urllib.request
from urllib.error import HTTPError, URLError

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


@pytest.fixture(scope="session")
def browser_type_launch_args(browser_type_launch_args):
    """Use Playwright's own bundled Chromium when present; otherwise fall back
    to a pre-provisioned Chromium under PLAYWRIGHT_BROWSERS_PATH (CI/web-session
    images ship a pinned Chromium at a different revision than the installed
    Playwright expects)."""
    args = dict(browser_type_launch_args)
    if "executable_path" in args:
        return args
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            bundled = p.chromium.executable_path
        if os.path.exists(bundled):
            return args
    except Exception:
        pass
    base = os.environ.get("PLAYWRIGHT_BROWSERS_PATH", "/opt/pw-browsers")
    found = sorted(glob.glob(os.path.join(base, "chromium-*", "chrome-linux", "chrome")))
    if found:
        args["executable_path"] = found[-1]
    return args


@pytest.fixture()
def server_url(vendored):
    from serve import make_server  # imported here so HERE is on sys.path

    port = _free_port()
    httpd = make_server(WEB, port)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{port}"
    for _ in range(50):
        try:
            urllib.request.urlopen(base + "/", timeout=0.2)
            break
        except HTTPError:
            break  # server is up; an HTTP status still means it's listening
        except (URLError, OSError):
            time.sleep(0.05)
    try:
        yield base
    finally:
        httpd.shutdown()
        thread.join(timeout=2)
```

- [ ] **Step 9: Write index.html and styles.css**

Create `archer/web/index.html`:

```html
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Archer — three.js wave defense</title>
    <link rel="stylesheet" href="./styles.css" />
    <script type="importmap">
      {
        "imports": {
          "three": "./vendor/three.module.js",
          "preact": "./vendor/preact.module.js",
          "preact/hooks": "./vendor/preact-hooks.module.js",
          "htm": "./vendor/htm.module.js"
        }
      }
    </script>
  </head>
  <body>
    <canvas id="game"></canvas>
    <div id="ui"></div>
    <div id="flash"></div>
    <div id="fallback" hidden>
      <p>Archer needs WebGL, which this browser could not provide.</p>
    </div>
    <script type="module" src="./main.js"></script>
  </body>
</html>
```

Create `archer/web/styles.css` (minimal for now; the HUD task extends it):

```css
:root { --draw: 0; }
html, body { margin: 0; height: 100%; overflow: hidden; background: #000; }
#game { position: fixed; inset: 0; width: 100%; height: 100%; display: block; }
#ui { position: fixed; inset: 0; pointer-events: none; font-family: system-ui, sans-serif; }
#flash { position: fixed; inset: 0; pointer-events: none; background: transparent; }
#fallback { position: fixed; inset: 0; display: grid; place-items: center;
  color: #eee; font-family: system-ui, sans-serif; }
#fallback[hidden] { display: none; }
```

- [ ] **Step 10: Write config.js**

Create `archer/web/config.js`:

```js
// All gameplay tuning in one place. Values are read at use time, so the
// test handle may patch them mid-run (e.g. drop chance).
export const CONFIG = {
  arena: { size: 80, spawnZ: -34, spawnXSpread: 28 },
  player: { hp: 100, pos: { x: 0, y: 3.2, z: 34 } },
  bow: {
    drawTime: 1.0,       // seconds to full draw
    minSpeed: 14,
    maxSpeed: 55,
    minDrawToFire: 0.15, // releases below this power are cancelled
    baseFov: 70,
    zoomFov: 62,         // FOV eased in at full draw
  },
  arrow: {
    gravity: -20,
    lifetime: 6,
    radius: 0.12,
    headshotMult: 2,
    types: {
      normal:    { damage: 34, color: 0xd8c9a3 },
      exploding: { damage: 20, color: 0xff7733, radius: 5, aoeDamage: 55 },
      freezing:  { damage: 18, color: 0x66ddff, freezeTime: 3, shatterMult: 2 },
      burning:   { damage: 15, color: 0xff4422, dps: 9, burnTime: 4, spreadRadius: 2.5 },
    },
  },
  drops: { chance: 0.2, min: 3, max: 5, lifetime: 20, radius: 0.6 },
  enemies: {
    goblin:   { hp: 40,  speed: 5.5, damage: 10, attackCooldown: 1.2, score: 100,
                bodyRadius: 0.55, height: 1.3, headRadius: 0.28, color: 0x4a8f3c },
    skeleton: { hp: 60,  speed: 4.0, damage: 8, score: 150, range: 26,
                peekTime: 1.4, hideTime: 2.0, projectileSpeed: 20, spread: 0.06,
                bodyRadius: 0.5, height: 1.6, headRadius: 0.26, color: 0xd9d4c5 },
    ogre:     { hp: 220, speed: 1.9, damage: 25, attackCooldown: 1.8, score: 400,
                bodyRadius: 1.0, height: 2.5, headRadius: 0.45, color: 0x7a6652 },
  },
  attackRange: 2.2,      // melee reach measured from the player
  headshotBonus: 50,     // extra score on a headshot kill
  multiKillBonus: 25,    // extra score per combo step (kills ≤1.5 s apart)
  multiKillWindow: 1.5,  // seconds between kills to sustain a combo
  waves: {
    perStage: 5,
    clearDelay: 2.0,     // seconds after a cleared wave before the next
    spawnInterval: 0.8,  // stagger between spawns within a wave
  },
  stages: {
    forest:  { speedMult: 1.0, waves: [
      { goblin: 4 },
      { goblin: 6 },
      { goblin: 5, skeleton: 2 },
      { goblin: 6, skeleton: 3 },
      { goblin: 6, skeleton: 3, ogre: 1 },
    ] },
    desert:  { speedMult: 1.15, waves: [
      { goblin: 6, skeleton: 2 },
      { goblin: 8, skeleton: 2 },
      { goblin: 8, skeleton: 3, ogre: 1 },
      { goblin: 10, skeleton: 3, ogre: 1 },
      { goblin: 10, skeleton: 4, ogre: 2 },
    ] },
    iceberg: { speedMult: 1.25, waves: [
      { goblin: 6, skeleton: 3, ogre: 1 },
      { goblin: 8, skeleton: 3, ogre: 1 },
      { goblin: 8, skeleton: 4, ogre: 2 },
      { goblin: 10, skeleton: 4, ogre: 2 },
      { goblin: 12, skeleton: 5, ogre: 3 },
    ] },
  },
};
```

- [ ] **Step 11: Write rng.js**

Create `archer/web/rng.js`:

```js
// Deterministic RNG: all gameplay randomness flows through one seeded
// mulberry32 stream so ?seed=N reproduces a run exactly.
export function createRng(seed) {
  let a = seed >>> 0;
  const random = () => {
    a |= 0; a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
  return {
    random,
    range: (min, max) => min + random() * (max - min),
    int: (min, max) => Math.floor(min + random() * (max - min + 1)),
    pick: (arr) => arr[Math.floor(random() * arr.length)],
  };
}

export function seedFromQuery(params) {
  const s = parseInt(params.get('seed'), 10);
  return Number.isFinite(s) ? s : (Date.now() & 0xffffffff);
}
```

- [ ] **Step 12: Write the boot main.js**

Create `archer/web/main.js`. Anchor comments (`// [task-N] ...`) mark where later tasks insert code — keep them.

```js
import * as THREE from 'three';
import { CONFIG } from './config.js';
import { createRng, seedFromQuery } from './rng.js';
// [task-2] stage imports
// [task-3] player import
// [task-4] arrows import
// [task-5] enemies import
// [task-7] effects import
// [task-8] waves import
// [task-10] ui imports

const params = new URLSearchParams(location.search);
const canvas = document.getElementById('game');

let renderer;
try {
  renderer = new THREE.WebGLRenderer({ canvas, antialias: true });
} catch (err) {
  document.getElementById('fallback').hidden = false;
  throw err;
}
renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));

const scene = new THREE.Scene();
const SKY_FALLBACK = 0x87b5d4;
scene.background = new THREE.Color(SKY_FALLBACK);
const camera = new THREE.PerspectiveCamera(CONFIG.bow.baseFov, 1, 0.1, 300);
const { x: px, y: py, z: pz } = CONFIG.player.pos;
camera.position.set(px, py, pz);
scene.add(camera); // camera-attached meshes (bow viewmodel) must render

// Shared context threaded through every system. Systems never import each
// other; they reach siblings through `game`.
const game = {
  scene, camera, params,
  rng: createRng(seedFromQuery(params)),
  stats: {
    score: 0,
    ammo: { exploding: 0, freezing: 0, burning: 0 },
    selected: 'normal',
  },
  screen: 'title',
  obstacles: [],
  syncUI: () => {}, // replaced by the UI task
};
// [task-2] stage loading
// [task-3] player setup
// [task-4] arrow system setup
// [task-5] enemy system setup + kill/hit callbacks
// [task-7] effects setup
// [task-8] wave manager setup
// [task-9] progression (start/stage-clear/game-over/retry)
// [task-10] ui wiring

function resize() {
  const w = window.innerWidth, h = window.innerHeight;
  renderer.setSize(w, h, false);
  camera.aspect = w / h;
  camera.updateProjectionMatrix();
}
window.addEventListener('resize', resize);
resize();

// [task-3] input handlers

let last = performance.now();
let framesRendered = 0;
function tick(now) {
  const dt = Math.min(0.05, (now - last) / 1000);
  last = now;
  if (game.screen === 'playing') {
    // [task-3] player.update(dt)
    // [task-4] arrows.update(dt)
    // [task-5] enemies.update(dt)
    // [task-8] waves.update(dt)
  }
  // [task-7] effects.update(dt)
  // [task-3] draw-ring css var
  renderer.render(scene, camera);
  framesRendered++;
  if (framesRendered === 1) window.__ARCHER.ready = true;
}
renderer.setAnimationLoop(tick);

// Deterministic e2e handle (pcl-viewer's window.__PCL pattern).
window.__ARCHER = {
  ready: false,
  get state() {
    return {
      screen: game.screen,
      score: game.stats.score,
      ammo: { ...game.stats.ammo },
      selected: game.stats.selected,
      // [task-2] stage/obstacles state
      // [task-3] hp/drawPower state
      // [task-4] arrowCount state
      // [task-5] enemies state
      // [task-8] wave/pickup state
    };
  },
  // [task-4] fireAt
  // [task-5] spawnEnemy, setPlayerHp
  // [task-8] setDropChance, killAll, skipToWave
  // [task-9] start, nextStage, retryStage
  // e2e helper: count pixels that differ from the sky background.
  visiblePixelCount() {
    const gl = renderer.getContext();
    const w = renderer.domElement.width, h = renderer.domElement.height;
    const buf = new Uint8Array(w * h * 4);
    gl.readPixels(0, 0, w, h, gl.RGBA, gl.UNSIGNED_BYTE, buf);
    const sky = new THREE.Color(scene.background);
    const br = sky.r * 255, bg = sky.g * 255, bb = sky.b * 255;
    let n = 0;
    for (let i = 0; i < buf.length; i += 4) {
      if (Math.abs(buf[i] - br) + Math.abs(buf[i + 1] - bg) + Math.abs(buf[i + 2] - bb) > 24) n++;
    }
    return n;
  },
};

// Boot: tests (and impatient humans) skip the title screen.
if (params.get('autostart') === '1') {
  game.screen = 'playing'; // [task-9] replaced by startGame()
  game.syncUI();
}
```

Note: with only a sky-colored background, `visiblePixelCount()` returns 0 — the boot test needs Task 1 to draw *something*. Add a temporary ground plane right after the `game` object definition (Task 2 deletes it when real stages land):

```js
// Temporary ground so the boot test has visible pixels; Task 2 replaces
// this with real stage building.
const tmpGround = new THREE.Mesh(
  new THREE.PlaneGeometry(120, 120),
  new THREE.MeshBasicMaterial({ color: 0x3e7a3a }),
);
tmpGround.rotation.x = -Math.PI / 2;
scene.add(tmpGround);
camera.lookAt(0, 2, 0);
```

- [ ] **Step 13: Run the boot test to verify it passes**

```bash
cd archer && uv run --group dev pytest tests/test_e2e.py -x -q
```

Expected: 1 passed. (First run fetches vendor libs and needs network.)

- [ ] **Step 14: Sanity-check the human entry point**

```bash
cd archer && (PORT=8123 ./archer.sh &) && sleep 2 && curl -s http://127.0.0.1:8123/ | grep -q '<canvas id="game">' && echo OK; pkill -f 'serve.py --port 8123'
```

Expected: `OK`.

- [ ] **Step 15: Commit**

```bash
git add .gitignore archer/
git commit -m "feat(archer): scaffold no-build three.js game with Playwright e2e boot test"
```

---

### Task 2: Stages — forest / desert / iceberg scene builders

**Files:**
- Create: `archer/web/stages.js`
- Modify: `archer/web/main.js` (replace temporary ground with stage loading)
- Test: `archer/tests/test_e2e.py`

**Interfaces:**
- Consumes: `CONFIG.arena`, `CONFIG.player.pos`, `createRng` instance via `game.rng`.
- Produces:
  - `stages.js`: `export const STAGE_ORDER = ['forest', 'desert', 'iceberg']`; `export function buildStage(name, rng)` → `{ group: THREE.Group, obstacles: Array<{x, z, radius, height}>, sky: number, fog: [color, near, far] }`.
  - `main.js`: `loadStage(index)` sets `game.stage` (name string), `game.obstacles`, swaps `stage.group` into the scene, sets `scene.background`/`scene.fog`. `__ARCHER.state` gains `stage` and `obstacles` (the array above).

- [ ] **Step 1: Write the failing tests**

Append to `archer/tests/test_e2e.py`:

```python
def test_stage_param_and_determinism(server_url, page):
    # Each named stage builds a scene with obstacles; same seed → same layout.
    page.goto(server_url + "/?autostart=1&seed=7&stage=desert")
    _wait_ready(page)
    s1 = page.evaluate("() => window.__ARCHER.state")
    assert s1["stage"] == "desert"
    assert len(s1["obstacles"]) > 5
    assert page.evaluate("() => window.__ARCHER.visiblePixelCount()") > 500

    page.goto(server_url + "/?autostart=1&seed=7&stage=desert")
    _wait_ready(page)
    s2 = page.evaluate("() => window.__ARCHER.state")
    assert s1["obstacles"] == s2["obstacles"]


def test_each_stage_builds(server_url, page):
    for name in ("forest", "desert", "iceberg"):
        page.goto(server_url + f"/?autostart=1&seed=3&stage={name}")
        _wait_ready(page)
        state = page.evaluate("() => window.__ARCHER.state")
        assert state["stage"] == name
        assert len(state["obstacles"]) > 5
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd archer && uv run --group dev pytest tests/test_e2e.py -x -q
```

Expected: FAIL — `state["stage"]` is missing (KeyError).

- [ ] **Step 3: Write stages.js**

Create `archer/web/stages.js`:

```js
import * as THREE from 'three';
import { CONFIG } from './config.js';

export const STAGE_ORDER = ['forest', 'desert', 'iceberg'];

function lambert(color) { return new THREE.MeshLambertMaterial({ color }); }

// Each maker returns { mesh, radius, height } with the mesh's base at y=0.
function makeTree(rng) {
  const g = new THREE.Group();
  const trunkH = rng.range(1.2, 2.0);
  const trunk = new THREE.Mesh(new THREE.CylinderGeometry(0.25, 0.35, trunkH, 6), lambert(0x6b4a2f));
  trunk.position.y = trunkH / 2;
  g.add(trunk);
  let y = trunkH;
  for (const r of [1.5, 1.1]) {
    const cone = new THREE.Mesh(new THREE.ConeGeometry(r, 2.2, 7), lambert(0x2f6b2a));
    cone.position.y = y + 1.1;
    g.add(cone);
    y += 1.4;
  }
  return { mesh: g, radius: 1.5, height: y + 1.8 };
}

function makeDesertObstacle(rng) {
  if (rng.random() < 0.5) {
    // saguaro cactus: trunk + two arms
    const g = new THREE.Group();
    const h = rng.range(2.5, 3.5);
    const trunk = new THREE.Mesh(new THREE.CylinderGeometry(0.35, 0.4, h, 8), lambert(0x3f7d46));
    trunk.position.y = h / 2;
    g.add(trunk);
    for (const side of [-1, 1]) {
      const arm = new THREE.Mesh(new THREE.CylinderGeometry(0.22, 0.22, 1.4, 6), lambert(0x3f7d46));
      arm.position.set(side * 0.6, h * 0.6, 0);
      arm.rotation.z = side * 0.5;
      g.add(arm);
    }
    return { mesh: g, radius: 1.0, height: h };
  }
  const r = rng.range(1.2, 2.2);
  const rock = new THREE.Mesh(new THREE.DodecahedronGeometry(r, 0), lambert(0xa8825d));
  rock.position.y = r * 0.6;
  rock.scale.y = 0.7;
  const g = new THREE.Group();
  g.add(rock);
  return { mesh: g, radius: r, height: r * 1.1 };
}

function makeIcePillar(rng) {
  const h = rng.range(2.5, 4.5);
  const pillar = new THREE.Mesh(
    new THREE.CylinderGeometry(rng.range(0.6, 1.0), rng.range(1.0, 1.6), h, 6),
    new THREE.MeshLambertMaterial({ color: 0xbfe8f7, emissive: 0x224455 }),
  );
  pillar.position.y = h / 2;
  const g = new THREE.Group();
  g.add(pillar);
  return { mesh: g, radius: 1.4, height: h };
}

const THEMES = {
  forest: {
    sky: 0x87b5d4, fog: [0x87b5d4, 40, 130], ground: 0x3e7a3a,
    sun: 0xfff4e0, sunIntensity: 1.0, ambient: 0x777788,
    obstacleCount: 26, obstacle: makeTree, perch: 0x6b6f66,
  },
  desert: {
    sky: 0xf2d9a8, fog: [0xf2d9a8, 50, 150], ground: 0xd9b36c,
    sun: 0xfff0c8, sunIntensity: 1.4, ambient: 0x998877,
    obstacleCount: 14, obstacle: makeDesertObstacle, perch: 0xc2955a,
  },
  iceberg: {
    sky: 0xbfe3f2, fog: [0xbfe3f2, 35, 120], ground: 0xdef2fb,
    sun: 0xe8f4ff, sunIntensity: 1.1, ambient: 0x8899aa,
    obstacleCount: 18, obstacle: makeIcePillar, perch: 0xcfe8f5,
  },
};

export function buildStage(name, rng) {
  const theme = THEMES[name];
  const group = new THREE.Group();

  const size = CONFIG.arena.size + 40;
  const ground = new THREE.Mesh(new THREE.PlaneGeometry(size, size), lambert(theme.ground));
  ground.rotation.x = -Math.PI / 2;
  group.add(ground);

  // The player's elevated vantage point.
  const { x: px, y: py, z: pz } = CONFIG.player.pos;
  const perch = new THREE.Mesh(new THREE.CylinderGeometry(2.2, 3.0, py - 1, 8), lambert(theme.perch));
  perch.position.set(px, (py - 1) / 2, pz);
  group.add(perch);

  group.add(new THREE.HemisphereLight(theme.sky, theme.ground, 0.9));
  const sun = new THREE.DirectionalLight(theme.sun, theme.sunIntensity);
  sun.position.set(20, 40, 10);
  group.add(sun);
  group.add(new THREE.AmbientLight(theme.ambient, 0.4));

  // Obstacles scattered over the battlefield, clear of the player perch.
  const obstacles = [];
  for (let i = 0; i < theme.obstacleCount; i++) {
    const x = rng.range(-36, 36);
    const z = rng.range(-30, 22);
    const { mesh, radius, height } = theme.obstacle(rng);
    mesh.position.set(x, 0, z);
    group.add(mesh);
    obstacles.push({ x, z, radius, height });
  }

  return { group, obstacles, sky: theme.sky, fog: theme.fog };
}
```

- [ ] **Step 4: Wire stage loading into main.js**

In `archer/web/main.js`:

Replace the line `// [task-2] stage imports` with:

```js
import { buildStage, STAGE_ORDER } from './stages.js';
```

Delete the whole "Temporary ground …" block (the `tmpGround` mesh and `camera.lookAt(0, 2, 0)` — the camera now starts level, facing −z by default).

Replace the line `// [task-2] stage loading` with:

```js
let stageHandle = null;
function loadStage(index) {
  if (stageHandle) scene.remove(stageHandle.group);
  const name = STAGE_ORDER[index];
  stageHandle = buildStage(name, game.rng);
  scene.add(stageHandle.group);
  scene.background = new THREE.Color(stageHandle.sky);
  const [fogColor, near, far] = stageHandle.fog;
  scene.fog = new THREE.Fog(fogColor, near, far);
  game.stage = name;
  game.stageIndex = index;
  game.obstacles = stageHandle.obstacles;
}
const initialStage = Math.max(0, STAGE_ORDER.indexOf(params.get('stage') || 'forest'));
loadStage(initialStage);
```

Replace the line `// [task-2] stage/obstacles state` (inside the `state` getter) with:

```js
      stage: game.stage,
      obstacles: game.obstacles,
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
cd archer && uv run --group dev pytest tests/test_e2e.py -x -q
```

Expected: 3 passed (boot test still green — the stage ground supplies its visible pixels).

- [ ] **Step 6: Commit**

```bash
git add archer/web/stages.js archer/web/main.js archer/tests/test_e2e.py
git commit -m "feat(archer): forest/desert/iceberg stage builders with seeded layouts"
```

---

### Task 3: Player — pointer-lock look, bow draw, viewmodel

**Files:**
- Create: `archer/web/player.js`
- Modify: `archer/web/main.js` (player setup, input handlers, tick, state)
- Test: `archer/tests/test_e2e.py`

**Interfaces:**
- Consumes: `CONFIG.player`, `CONFIG.bow`; `game.camera`.
- Produces:
  - `player.js`: `export class Player` with `constructor(camera)`, fields `hp`, `isDrawing`, `drawPower` (0..1), methods `look(dx, dy)`, `startDraw()`, `cancelDraw()`, `releaseDraw()` → power number or `null` (below `minDrawToFire`), `takeDamage(n)`, `aimDir()` → normalized `THREE.Vector3`, `aimOrigin()` → `THREE.Vector3`, `resetHp()`, `update(dt)`.
  - `main.js`: `game.player`; mousedown/mouseup/mousemove/pointerlock handlers; `__ARCHER.state` gains `hp` and `drawPower`. A `fireArrow(power)` stub (`function fireArrow(power) {}` at the `// [task-4] arrow system setup` anchor is NOT yet created — mouseup only calls `player.releaseDraw()` and stores the result in `game.lastReleasePower` for this task's test; Task 4 replaces that line).

- [ ] **Step 1: Write the failing test**

Append to `archer/tests/test_e2e.py`:

```python
def test_bow_draw_charges_and_releases(server_url, page):
    page.goto(server_url + BOOT)
    _wait_ready(page)
    page.mouse.move(640, 360)
    page.mouse.down()
    page.wait_for_function("() => window.__ARCHER.state.drawPower > 0.4", timeout=5000)
    power = page.evaluate("() => window.__ARCHER.state.drawPower")
    assert 0.4 < power <= 1.0
    page.mouse.up()
    assert page.evaluate("() => window.__ARCHER.state.drawPower") == 0
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd archer && uv run --group dev pytest tests/test_e2e.py::test_bow_draw_charges_and_releases -x -q
```

Expected: FAIL — timeout, `drawPower` is undefined.

- [ ] **Step 3: Write player.js**

Create `archer/web/player.js`:

```js
import * as THREE from 'three';
import { CONFIG } from './config.js';

function buildBowViewmodel() {
  const g = new THREE.Group();
  const wood = new THREE.MeshLambertMaterial({ color: 0x7a5230 });
  // Limb: a torus arc standing vertically.
  const limb = new THREE.Mesh(new THREE.TorusGeometry(0.28, 0.025, 6, 24, Math.PI * 1.5), wood);
  limb.rotation.z = Math.PI * 0.75;
  g.add(limb);
  // String: a stretched box between the limb tips; scales back with draw.
  const string = new THREE.Mesh(
    new THREE.BoxGeometry(0.004, 0.52, 0.004),
    new THREE.MeshBasicMaterial({ color: 0xeeeeee }),
  );
  string.position.x = 0.1;
  g.add(string);
  // Nocked arrow: shaft pointing forward (-z).
  const shaft = new THREE.Mesh(
    new THREE.CylinderGeometry(0.008, 0.008, 0.55, 5),
    new THREE.MeshLambertMaterial({ color: 0xd8c9a3 }),
  );
  shaft.rotation.x = Math.PI / 2;
  shaft.position.z = -0.15;
  g.add(shaft);
  g.position.set(0.28, -0.28, -0.7);
  g.rotation.y = -0.15;
  return { group: g, string, shaft };
}

export class Player {
  constructor(camera) {
    this.camera = camera;
    this.hp = CONFIG.player.hp;
    this.yaw = 0;       // facing -z, toward the spawn edge
    this.pitch = 0;
    this.isDrawing = false;
    this.drawPower = 0;
    this.bow = buildBowViewmodel();
    camera.add(this.bow.group);
    camera.rotation.order = 'YXZ';
  }

  look(dx, dy) {
    this.yaw -= dx * 0.0022;
    this.pitch = Math.max(-1.4, Math.min(1.4, this.pitch - dy * 0.0022));
  }

  startDraw() {
    if (!this.isDrawing) { this.isDrawing = true; this.drawPower = 0; }
  }

  cancelDraw() { this.isDrawing = false; this.drawPower = 0; }

  releaseDraw() {
    const p = this.drawPower;
    this.cancelDraw();
    return p >= CONFIG.bow.minDrawToFire ? p : null;
  }

  takeDamage(n) { this.hp = Math.max(0, this.hp - n); }
  resetHp() { this.hp = CONFIG.player.hp; }

  aimDir() {
    return this.camera.getWorldDirection(new THREE.Vector3());
  }

  aimOrigin() {
    return this.camera.getWorldPosition(new THREE.Vector3())
      .addScaledVector(this.aimDir(), 0.7);
  }

  update(dt) {
    this.camera.rotation.set(this.pitch, this.yaw, 0);
    if (this.isDrawing) {
      this.drawPower = Math.min(1, this.drawPower + dt / CONFIG.bow.drawTime);
    }
    // Ease FOV toward zoom at full draw.
    const targetFov = this.drawPower >= 1 ? CONFIG.bow.zoomFov : CONFIG.bow.baseFov;
    this.camera.fov += (targetFov - this.camera.fov) * Math.min(1, dt * 8);
    this.camera.updateProjectionMatrix();
    // Pull the string and nocked arrow back with draw power.
    const pull = this.drawPower * 0.18;
    this.bow.string.position.x = 0.1 + pull;
    this.bow.string.scale.y = 1 - this.drawPower * 0.25;
    this.bow.shaft.position.z = -0.15 + pull;
  }
}
```

- [ ] **Step 4: Wire the player into main.js**

In `archer/web/main.js`:

Replace `// [task-3] player import` with:

```js
import { Player } from './player.js';
```

Replace `// [task-3] player setup` with:

```js
game.player = new Player(camera);
```

Replace `// [task-3] input handlers` with:

```js
let wasLocked = false;
document.addEventListener('pointerlockchange', () => {
  const locked = document.pointerLockElement === canvas;
  if (wasLocked && !locked && game.screen === 'playing') {
    game.player.cancelDraw();
    // [task-10] pause on pointer-lock loss
  }
  wasLocked = locked;
});
canvas.addEventListener('click', () => {
  if (game.screen === 'playing' && document.pointerLockElement !== canvas) {
    canvas.requestPointerLock()?.catch(() => {}); // headless/e2e: lock may be denied
  }
});
document.addEventListener('mousemove', (e) => {
  if (document.pointerLockElement === canvas && game.screen === 'playing') {
    game.player.look(e.movementX, e.movementY);
  }
});
document.addEventListener('mousedown', (e) => {
  if (e.button === 0 && game.screen === 'playing') game.player.startDraw();
});
document.addEventListener('mouseup', (e) => {
  if (e.button !== 0 || game.screen !== 'playing') return;
  const power = game.player.releaseDraw();
  if (power !== null) fireArrow(power); // fireArrow arrives in Task 4
});
```

Replace `// [task-4] arrows import` — leave it (Task 4). Add directly under the `// [task-4] arrow system setup` anchor (keeping the anchor comment):

```js
function fireArrow(power) {} // stub; Task 4 replaces the body
```

Replace `// [task-3] player.update(dt)` with:

```js
    game.player.update(dt);
```

Replace `// [task-3] draw-ring css var` with:

```js
  document.documentElement.style.setProperty('--draw', game.player.drawPower.toFixed(3));
```

Replace `// [task-3] hp/drawPower state` (in the `state` getter) with:

```js
      hp: game.player.hp,
      drawPower: game.player.drawPower,
```

- [ ] **Step 5: Run test to verify it passes**

```bash
cd archer && uv run --group dev pytest tests/test_e2e.py -x -q
```

Expected: 4 passed.

- [ ] **Step 6: Commit**

```bash
git add archer/web/player.js archer/web/main.js archer/tests/test_e2e.py
git commit -m "feat(archer): first-person player with charged bow draw and viewmodel"
```

---

### Task 4: Arrows — ballistic projectiles, ground impact, trajectory hint

**Files:**
- Create: `archer/web/arrows.js`
- Modify: `archer/web/main.js`
- Test: `archer/tests/test_e2e.py`

**Interfaces:**
- Consumes: `CONFIG.arrow`, `CONFIG.bow`; `game.player.aimOrigin()/aimDir()`; `game.enemies` and `game.waves` are optional (`?.`-guarded) — they arrive in Tasks 5/8.
- Produces:
  - `arrows.js`: `export class ArrowSystem` with `constructor(game)`, getter `count`, `fire(origin, dir, power, type)`, `update(dt)`, `clear()`, `hit(enemy, isHead, arrow)` (this task: direct damage only; Task 7 extends it with type effects and adds `explode(pos)`). `export class TrajectoryHint` with `constructor(scene)`, `update(player, active)`.
  - `main.js`: real `fireArrow(power)` (consumes ammo for special types); `__ARCHER.fireAt(x, y, z, type='normal', power=1)` (test helper, does NOT consume ammo); `__ARCHER.state.arrowCount`.
  - Collision is segment-vs-sphere (arrow's per-frame path segment against enemy head/body spheres) — point checks would tunnel at 55 m/s.

- [ ] **Step 1: Write the failing tests**

Append to `archer/tests/test_e2e.py`:

```python
def test_arrow_flies_and_lands(server_url, page):
    page.goto(server_url + BOOT)
    _wait_ready(page)
    page.evaluate("() => window.__ARCHER.fireAt(0, 1, 0)")
    assert page.evaluate("() => window.__ARCHER.state.arrowCount") == 1
    # Gravity brings it down; ground impact removes it.
    page.wait_for_function("() => window.__ARCHER.state.arrowCount === 0", timeout=10000)


def test_mouse_release_fires_arrow(server_url, page):
    page.goto(server_url + BOOT)
    _wait_ready(page)
    page.mouse.move(640, 360)
    page.mouse.down()
    page.wait_for_function("() => window.__ARCHER.state.drawPower > 0.5", timeout=5000)
    page.mouse.up()
    page.wait_for_function("() => window.__ARCHER.state.arrowCount === 1", timeout=2000)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd archer && uv run --group dev pytest tests/test_e2e.py -x -q`
Expected: FAIL — `fireAt` is not a function.

- [ ] **Step 3: Write arrows.js**

Create `archer/web/arrows.js`:

```js
import * as THREE from 'three';
import { CONFIG } from './config.js';

// Closest point on segment [a,b] to point p.
function segClosest(a, b, p) {
  const ab = new THREE.Vector3().subVectors(b, a);
  const denom = ab.lengthSq();
  const t = denom === 0 ? 0
    : Math.max(0, Math.min(1, new THREE.Vector3().subVectors(p, a).dot(ab) / denom));
  return new THREE.Vector3().copy(a).addScaledVector(ab, t);
}

function buildArrowMesh(type) {
  const g = new THREE.Group();
  const color = CONFIG.arrow.types[type].color;
  const shaft = new THREE.Mesh(
    new THREE.CylinderGeometry(0.02, 0.02, 0.7, 5),
    new THREE.MeshLambertMaterial({ color: 0xd8c9a3 }),
  );
  const tip = new THREE.Mesh(
    new THREE.ConeGeometry(0.045, 0.12, 5),
    new THREE.MeshLambertMaterial({ color: 0x555555 }),
  );
  tip.position.y = 0.41;
  const fletch = new THREE.Mesh(
    new THREE.ConeGeometry(0.06, 0.15, 4),
    new THREE.MeshBasicMaterial({ color }),
  );
  fletch.position.y = -0.32;
  g.add(shaft, tip, fletch);
  return g;
}

export class ArrowSystem {
  constructor(game) {
    this.game = game;
    this.list = [];
  }

  get count() { return this.list.length; }

  fire(origin, dir, power, type) {
    const speed = CONFIG.bow.minSpeed + (CONFIG.bow.maxSpeed - CONFIG.bow.minSpeed) * power;
    const mesh = buildArrowMesh(type);
    mesh.position.copy(origin);
    this.game.scene.add(mesh);
    this.list.push({ mesh, vel: dir.clone().multiplyScalar(speed), type, age: 0 });
  }

  clear() {
    for (const a of [...this.list]) this.game.scene.remove(a.mesh);
    this.list.length = 0;
  }

  remove(a) {
    this.game.scene.remove(a.mesh);
    this.list.splice(this.list.indexOf(a), 1);
  }

  update(dt) {
    const R = CONFIG.arrow.radius;
    for (const a of [...this.list]) {
      const prev = a.mesh.position.clone();
      a.vel.y += CONFIG.arrow.gravity * dt;
      a.mesh.position.addScaledVector(a.vel, dt);
      a.mesh.quaternion.setFromUnitVectors(
        new THREE.Vector3(0, 1, 0), a.vel.clone().normalize(),
      );
      a.age += dt;
      const pos = a.mesh.position;

      // Pickups are collected by shooting them (segment check: arrows are fast).
      let consumed = false;
      if (this.game.waves) {
        for (const p of [...this.game.waves.pickups]) {
          if (segClosest(prev, pos, p.mesh.position).distanceTo(p.mesh.position)
              < CONFIG.drops.radius + R) {
            this.game.waves.collect(p);
            consumed = true;
            break;
          }
        }
      }

      // Enemies: head sphere first (headshots win ties), then body sphere.
      if (!consumed && this.game.enemies) {
        for (const e of this.game.enemies.list) {
          const head = this.game.enemies.headCenter(e);
          if (segClosest(prev, pos, head).distanceTo(head) < e.c.headRadius + R) {
            this.hit(e, true, a);
            consumed = true;
            break;
          }
          const body = this.game.enemies.bodyCenter(e);
          if (segClosest(prev, pos, body).distanceTo(body)
              < this.game.enemies.bodyRadius(e) + R) {
            this.hit(e, false, a);
            consumed = true;
            break;
          }
        }
      }

      if (consumed) { this.remove(a); continue; }
      if (pos.y <= 0.05 || a.age > CONFIG.arrow.lifetime) {
        // [task-7] explode on ground impact for exploding arrows
        this.remove(a);
      }
    }
  }

  hit(e, isHead, arrow) {
    const t = CONFIG.arrow.types[arrow.type];
    this.game.enemies.damage(e, t.damage * (isHead ? CONFIG.arrow.headshotMult : 1), isHead);
    // [task-7] arrow-type status effects + explosion
  }
}

// Dotted arc preview shown at partial draw; fades out at full draw so
// full-power shots stay skill-based.
export class TrajectoryHint {
  constructor(scene) {
    this.n = 24;
    const geo = new THREE.BufferGeometry();
    geo.setAttribute('position', new THREE.BufferAttribute(new Float32Array(this.n * 3), 3));
    this.points = new THREE.Points(geo, new THREE.PointsMaterial({
      color: 0xffffff, size: 0.12, transparent: true, opacity: 0.5,
    }));
    this.points.visible = false;
    scene.add(this.points);
  }

  update(player, active) {
    const show = active && player.isDrawing
      && player.drawPower > 0.05 && player.drawPower < 0.85;
    this.points.visible = show;
    if (!show) return;
    const speed = CONFIG.bow.minSpeed
      + (CONFIG.bow.maxSpeed - CONFIG.bow.minSpeed) * player.drawPower;
    const p = player.aimOrigin();
    const v = player.aimDir().multiplyScalar(speed);
    const attr = this.points.geometry.attributes.position;
    const step = 0.07;
    for (let i = 0; i < this.n; i++) {
      v.y += CONFIG.arrow.gravity * step;
      p.addScaledVector(v, step);
      attr.setXYZ(i, p.x, Math.max(p.y, 0.05), p.z);
    }
    attr.needsUpdate = true;
    this.points.material.opacity = 0.5 * (1 - Math.max(0, (player.drawPower - 0.6) / 0.25));
  }
}
```

- [ ] **Step 4: Wire arrows into main.js**

In `archer/web/main.js`:

Replace `// [task-4] arrows import` with:

```js
import { ArrowSystem, TrajectoryHint } from './arrows.js';
```

Replace the Task-3 stub block at the `// [task-4] arrow system setup` anchor (the anchor comment plus `function fireArrow(power) {} // stub; Task 4 replaces the body`) with:

```js
game.arrows = new ArrowSystem(game);
const trajectoryHint = new TrajectoryHint(scene);
function fireArrow(power) {
  const type = game.stats.selected;
  if (type !== 'normal') {
    if (game.stats.ammo[type] <= 0) return; // no ammo: the release fizzles
    game.stats.ammo[type] -= 1;
  }
  game.arrows.fire(game.player.aimOrigin(), game.player.aimDir(), power, type);
  game.syncUI();
}
```

Replace `// [task-4] arrows.update(dt)` with:

```js
    game.arrows.update(dt);
    trajectoryHint.update(game.player, true);
```

Replace `// [task-4] arrowCount state` with:

```js
      arrowCount: game.arrows.count,
```

Replace `// [task-4] fireAt` with:

```js
  // Test helper: fire at a world point with ballistic gravity compensation.
  // Does NOT consume ammo (input-path firing does).
  fireAt(x, y, z, type = 'normal', power = 1) {
    const origin = camera.getWorldPosition(new THREE.Vector3());
    const target = new THREE.Vector3(x, y, z);
    const dist = origin.distanceTo(target);
    const speed = CONFIG.bow.minSpeed + (CONFIG.bow.maxSpeed - CONFIG.bow.minSpeed) * power;
    const dir = target.sub(origin).normalize();
    const tof = dist / speed;
    dir.y += 0.5 * -CONFIG.arrow.gravity * tof * tof / dist;
    dir.normalize();
    game.arrows.fire(origin.addScaledVector(dir, 0.7), dir, power, type);
  },
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd archer && uv run --group dev pytest tests/test_e2e.py -x -q`
Expected: 6 passed.

- [ ] **Step 6: Commit**

```bash
git add archer/web/arrows.js archer/web/main.js archer/tests/test_e2e.py
git commit -m "feat(archer): ballistic arrow system with trajectory hint and fireAt test helper"
```

---

### Task 5: Melee enemies — goblin and ogre, combat resolution, score, game over

**Files:**
- Create: `archer/web/enemies.js`
- Modify: `archer/web/main.js`, `archer/web/styles.css`
- Test: `archer/tests/test_e2e.py`

**Interfaces:**
- Consumes: `CONFIG.enemies`, `CONFIG.attackRange`, `CONFIG.arrow.types` (shatter/burn constants), `CONFIG.stages[...].speedMult`; `game.rng`, `game.scene`, `game.camera.position` (player position), `game.player.takeDamage`, `game.stage`.
- Produces:
  - `enemies.js`: `export class EnemySystem` — `constructor(game)`, `list` (array of enemy records `{type, c, mesh, hp, state, frozen, burn, ...}`), `spawn(type, x, z)` → enemy, `damage(e, dmg, isHead=false)` (applies frozen-shatter multiplier, kills at ≤0), `kill(e, isHead)` (calls `game.onEnemyKilled(e, isHead)`), `freeze(e)`, `ignite(e)`, `clear()`, `bodyCenter(e)`/`headCenter(e)` → `THREE.Vector3`, `bodyRadius(e)` → number, `update(dt)`. Status handling (frozen timer halts AI; burn ticks damage and spreads) is fully implemented here — Task 7's arrows trigger it. `updateArcher`/`updateProjectiles` are empty methods overridden-in-place by Task 6's edit.
  - `main.js`: `game.enemies`; `game.onEnemyKilled(e, isHead)` (score + syncUI; `game.waves?.onEnemyKilled(e)` guarded); `game.onPlayerHit()` (damage flash + game-over check); `gameOver()`; `__ARCHER.spawnEnemy(type, x, z)`, `__ARCHER.setPlayerHp(n)`; `__ARCHER.state` gains `enemyCount` and `enemies` (array of `{type, x, z, hp, state, frozen, burning}`).
- Test note: parked enemies (spawned at z=32, inside melee reach of the player at z=34) stand still and attack — this avoids leading moving targets in tests. Tests that park enemies call `setPlayerHp(10000)` first so the player never dies mid-test.

- [ ] **Step 1: Write the failing tests**

Append to `archer/tests/test_e2e.py`:

```python
def test_arrow_kills_goblin_and_scores(server_url, page):
    page.goto(server_url + BOOT)
    _wait_ready(page)
    page.evaluate("() => window.__ARCHER.setPlayerHp(10000)")
    page.evaluate("() => window.__ARCHER.spawnEnemy('goblin', 0, 32)")  # parked at melee reach
    assert page.evaluate("() => window.__ARCHER.state.enemyCount") == 1
    # Goblin: 40 hp; normal arrow: 34 dmg → two body shots.
    page.evaluate("() => window.__ARCHER.fireAt(0, 0.65, 32)")
    page.wait_for_function("() => window.__ARCHER.state.arrowCount === 0", timeout=5000)
    page.evaluate("() => window.__ARCHER.fireAt(0, 0.65, 32)")
    page.wait_for_function("() => window.__ARCHER.state.enemyCount === 0", timeout=5000)
    assert page.evaluate("() => window.__ARCHER.state.score") == 100


def test_headshot_double_damage_and_bonus(server_url, page):
    page.goto(server_url + BOOT)
    _wait_ready(page)
    page.evaluate("() => window.__ARCHER.setPlayerHp(10000)")
    page.evaluate("() => window.__ARCHER.spawnEnemy('goblin', 0, 32)")
    # Head at y=1.3: 34*2=68 ≥ 40 hp → one-shot kill, +50 headshot bonus.
    page.evaluate("() => window.__ARCHER.fireAt(0, 1.3, 32)")
    page.wait_for_function("() => window.__ARCHER.state.enemyCount === 0", timeout=5000)
    assert page.evaluate("() => window.__ARCHER.state.score") == 150


def test_goblin_advances_and_deals_contact_damage(server_url, page):
    page.goto(server_url + BOOT)
    _wait_ready(page)
    page.evaluate("() => window.__ARCHER.spawnEnemy('goblin', 0, 10)")
    z0 = page.evaluate("() => window.__ARCHER.state.enemies[0].z")
    page.wait_for_timeout(1500)
    z1 = page.evaluate("() => window.__ARCHER.state.enemies[0].z")
    assert z1 > z0 + 3  # closing in on the player at z=34
    page.wait_for_function("() => window.__ARCHER.state.hp < 100", timeout=15000)


def test_player_death_shows_game_over(server_url, page):
    page.goto(server_url + BOOT)
    _wait_ready(page)
    page.evaluate("() => window.__ARCHER.setPlayerHp(5)")
    page.evaluate("() => window.__ARCHER.spawnEnemy('goblin', 0, 32)")
    page.wait_for_function("() => window.__ARCHER.state.screen === 'gameOver'", timeout=10000)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd archer && uv run --group dev pytest tests/test_e2e.py -x -q`
Expected: FAIL — `spawnEnemy` is not a function.

- [ ] **Step 3: Write enemies.js**

Create `archer/web/enemies.js`:

```js
import * as THREE from 'three';
import { CONFIG } from './config.js';

function lambert(color) { return new THREE.MeshLambertMaterial({ color }); }

// Builders return a Group whose base sits at y=0; collision spheres are
// derived from config (bodyRadius/height/headRadius), not from the meshes.
function buildGoblin(c) {
  const g = new THREE.Group();
  const body = new THREE.Mesh(
    new THREE.CylinderGeometry(c.bodyRadius * 0.7, c.bodyRadius, c.height * 0.75, 8),
    lambert(c.color),
  );
  body.position.y = c.height * 0.375;
  const head = new THREE.Mesh(new THREE.SphereGeometry(c.headRadius, 8, 6), lambert(0x5ea34c));
  head.position.y = c.height;
  g.add(body, head);
  for (const s of [-1, 1]) {
    const ear = new THREE.Mesh(new THREE.ConeGeometry(0.07, 0.25, 4), lambert(0x5ea34c));
    ear.position.set(s * c.headRadius, c.height + 0.1, 0);
    ear.rotation.z = -s * 1.2;
    g.add(ear);
  }
  return g;
}

function buildOgre(c) {
  const g = new THREE.Group();
  const body = new THREE.Mesh(
    new THREE.CylinderGeometry(c.bodyRadius * 0.8, c.bodyRadius, c.height * 0.8, 8),
    lambert(c.color),
  );
  body.position.y = c.height * 0.4;
  const head = new THREE.Mesh(new THREE.SphereGeometry(c.headRadius, 8, 6), lambert(0x8a765f));
  head.position.y = c.height;
  g.add(body, head);
  for (const s of [-1, 1]) {
    const arm = new THREE.Mesh(new THREE.BoxGeometry(0.3, c.height * 0.6, 0.3), lambert(c.color));
    arm.position.set(s * (c.bodyRadius + 0.18), c.height * 0.5, 0);
    g.add(arm);
  }
  return g;
}

const BUILDERS = { goblin: buildGoblin, ogre: buildOgre }; // skeleton arrives in Task 6

export class EnemySystem {
  constructor(game) {
    this.game = game;
    this.list = [];
    this.projectiles = []; // skeleton arrows (Task 6)
  }

  spawn(type, x, z) {
    const c = CONFIG.enemies[type];
    const mesh = BUILDERS[type](c);
    mesh.position.set(x, 0, z);
    this.game.scene.add(mesh);
    const e = {
      type, c, mesh, hp: c.hp, state: 'advance',
      frozen: 0, burn: 0, burnSpreadTimer: 0,
      attackTimer: 0, coverTimer: 0, cover: null, hasShot: false,
      peekSide: this.game.rng.random() < 0.5 ? -1 : 1,
      bobT: this.game.rng.range(0, Math.PI * 2),
    };
    this.list.push(e);
    return e;
  }

  clear() {
    for (const e of [...this.list]) this.game.scene.remove(e.mesh);
    this.list.length = 0;
    for (const p of [...this.projectiles]) this.game.scene.remove(p.mesh);
    this.projectiles.length = 0;
  }

  bodyCenter(e) {
    return new THREE.Vector3(e.mesh.position.x, e.c.height * 0.5, e.mesh.position.z);
  }

  headCenter(e) {
    return new THREE.Vector3(e.mesh.position.x, e.c.height, e.mesh.position.z);
  }

  bodyRadius(e) { return Math.max(e.c.bodyRadius, e.c.height * 0.45); }

  setTint(e, hex) {
    e.mesh.traverse((o) => {
      if (o.isMesh && o.material.emissive) o.material.emissive.setHex(hex);
    });
  }

  damage(e, dmg, isHead = false) {
    if (e.hp <= 0) return;
    if (e.frozen > 0) { // shatter: frozen targets take bonus damage and thaw
      dmg *= CONFIG.arrow.types.freezing.shatterMult;
      e.frozen = 0;
      this.setTint(e, 0x000000);
    }
    e.hp -= dmg;
    if (e.hp <= 0) this.kill(e, isHead);
  }

  kill(e, isHead) {
    this.game.scene.remove(e.mesh);
    this.list.splice(this.list.indexOf(e), 1);
    this.game.onEnemyKilled(e, isHead);
  }

  freeze(e) {
    e.frozen = CONFIG.arrow.types.freezing.freezeTime;
    e.burn = 0; // ice quenches fire
    this.setTint(e, 0x2288aa);
  }

  ignite(e) {
    e.burn = CONFIG.arrow.types.burning.burnTime;
    this.setTint(e, 0x993300);
  }

  speedOf(e) { return e.c.speed * CONFIG.stages[this.game.stage].speedMult; }

  moveToward(e, target, dt, speed) {
    const pos = e.mesh.position;
    const dir = new THREE.Vector3(target.x - pos.x, 0, target.z - pos.z);
    const dist = dir.length();
    if (dist < 0.05) return;
    dir.normalize();
    pos.addScaledVector(dir, Math.min(dist, speed * dt));
    e.mesh.rotation.y = Math.atan2(dir.x, dir.z);
  }

  update(dt) {
    const playerPos = this.game.camera.position;
    for (const e of [...this.list]) {
      e.bobT += dt * 6;
      if (e.frozen > 0) {
        e.frozen -= dt;
        if (e.frozen <= 0) this.setTint(e, 0x000000);
        continue; // frozen solid: no movement, no attacks
      }
      if (e.burn > 0) {
        e.burn -= dt;
        this.damage(e, CONFIG.arrow.types.burning.dps * dt);
        if (e.hp <= 0) continue;
        if (e.burn <= 0) this.setTint(e, 0x000000);
        else this.spreadBurn(e, dt);
      }
      if (e.type === 'skeleton') this.updateArcher(e, dt, playerPos);
      else this.updateMelee(e, dt, playerPos);
      e.mesh.position.y = Math.abs(Math.sin(e.bobT)) * 0.07; // visual bob only
    }
    this.updateProjectiles(dt, playerPos);
  }

  spreadBurn(e, dt) {
    e.burnSpreadTimer -= dt;
    if (e.burnSpreadTimer > 0) return;
    e.burnSpreadTimer = 0.5;
    const r = CONFIG.arrow.types.burning.spreadRadius;
    for (const other of this.list) {
      if (other !== e && other.burn <= 0 && other.frozen <= 0
          && other.mesh.position.distanceTo(e.mesh.position) < r) {
        this.ignite(other);
      }
    }
  }

  updateMelee(e, dt, playerPos) {
    const pos = e.mesh.position;
    const flatDist = Math.hypot(playerPos.x - pos.x, playerPos.z - pos.z);
    e.attackTimer -= dt;
    if (flatDist <= CONFIG.attackRange + e.c.bodyRadius) {
      if (e.attackTimer <= 0) {
        e.attackTimer = e.c.attackCooldown;
        this.game.player.takeDamage(e.c.damage);
        this.game.onPlayerHit();
      }
      return;
    }
    // Advance with a slight weave (goblins zigzag; ogres lumber straight).
    const dir = new THREE.Vector3(playerPos.x - pos.x, 0, playerPos.z - pos.z).normalize();
    if (e.type === 'goblin') {
      const perp = new THREE.Vector3(-dir.z, 0, dir.x);
      dir.addScaledVector(perp, Math.sin(e.bobT * 0.9) * 0.5).normalize();
    }
    pos.addScaledVector(dir, this.speedOf(e) * dt);
    e.mesh.rotation.y = Math.atan2(dir.x, dir.z);
  }

  updateArcher(e, dt, playerPos) {} // Task 6
  updateProjectiles(dt, playerPos) {} // Task 6
}
```

- [ ] **Step 4: Wire enemies into main.js and styles.css**

In `archer/web/main.js`:

Replace `// [task-5] enemies import` with:

```js
import { EnemySystem } from './enemies.js';
```

Replace `// [task-5] enemy system setup + kill/hit callbacks` with:

```js
game.enemies = new EnemySystem(game);
game.onEnemyKilled = (e, isHead) => {
  game.stats.score += e.c.score + (isHead ? CONFIG.headshotBonus : 0);
  game.waves?.onEnemyKilled(e);
  game.syncUI();
};
game.onPlayerHit = () => {
  flashDamage();
  game.syncUI();
  if (game.player.hp <= 0) gameOver();
};
function flashDamage() {
  const el = document.getElementById('flash');
  el.classList.remove('on');
  void el.offsetWidth; // restart the CSS animation
  el.classList.add('on');
}
function gameOver() {
  game.screen = 'gameOver';
  // [task-9] persist best on game over
  document.exitPointerLock?.();
  game.syncUI();
}
```

Replace `// [task-5] enemies.update(dt)` with:

```js
    game.enemies.update(dt);
```

Replace `// [task-5] enemies state` with:

```js
      enemyCount: game.enemies.list.length,
      enemies: game.enemies.list.map((e) => ({
        type: e.type, x: e.mesh.position.x, z: e.mesh.position.z,
        hp: e.hp, state: e.state, frozen: e.frozen > 0, burning: e.burn > 0,
        hasCover: !!e.cover,
      })),
```

Replace `// [task-5] spawnEnemy, setPlayerHp` with:

```js
  spawnEnemy: (type, x, z) => { game.enemies.spawn(type, x, z); },
  setPlayerHp: (n) => { game.player.hp = n; game.syncUI(); },
```

Append to `archer/web/styles.css`:

```css
#flash.on { animation: dmg 0.35s ease-out; }
@keyframes dmg {
  from { background: radial-gradient(ellipse at center, transparent 40%, rgba(255, 0, 0, 0.45)); }
  to { background: transparent; }
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd archer && uv run --group dev pytest tests/test_e2e.py -x -q`
Expected: 10 passed.

- [ ] **Step 6: Commit**

```bash
git add archer/web/enemies.js archer/web/main.js archer/web/styles.css archer/tests/test_e2e.py
git commit -m "feat(archer): goblin and ogre melee enemies with combat, score, and game over"
```

---

### Task 6: Skeleton archer — cover AI and enemy projectiles

**Files:**
- Modify: `archer/web/enemies.js`
- Test: `archer/tests/test_e2e.py`

**Interfaces:**
- Consumes: `CONFIG.enemies.skeleton` (`range`, `peekTime`, `hideTime`, `projectileSpeed`, `spread`, `damage`); `game.obstacles` (from `loadStage`); everything Task 5 defined.
- Produces: skeleton entry in `BUILDERS`; real `updateArcher(e, dt, playerPos)` and `updateProjectiles(dt, playerPos)` bodies; internal helpers `pickCover(e, playerPos)`, `coverPoint(cover, playerPos)`, `peekPoint(e, playerPos)`, `shoot(e, playerPos)`. Skeleton states cycle `advance → cover ↔ peek`; the enemy record's `state` field (already exposed in `__ARCHER.state.enemies`) reflects it.

- [ ] **Step 1: Write the failing tests**

Append to `archer/tests/test_e2e.py`:

```python
def _nearest_obstacle_gap(state):
    """Min distance from the first enemy to any obstacle edge."""
    e = state["enemies"][0]
    return min(
        ((o["x"] - e["x"]) ** 2 + (o["z"] - e["z"]) ** 2) ** 0.5 - o["radius"]
        for o in state["obstacles"]
    )


def test_skeleton_takes_cover_behind_obstacle(server_url, page):
    page.goto(server_url + BOOT)
    _wait_ready(page)
    page.evaluate("() => window.__ARCHER.setPlayerHp(10000)")
    # Spawn already inside firing range so it immediately seeks cover.
    page.evaluate("() => window.__ARCHER.spawnEnemy('skeleton', 0, 12)")
    # It must actually find a cover obstacle (deterministic: seed 42 layout).
    page.wait_for_function(
        "() => window.__ARCHER.state.enemies[0].hasCover", timeout=10000
    )
    page.wait_for_timeout(3000)  # let it walk to its chosen cover
    state = page.evaluate("() => window.__ARCHER.state")
    # Hugging its obstacle (cover point is edge+0.7; a peek adds ~edge+0.5
    # sideways, worst case ~2 m from the edge).
    assert _nearest_obstacle_gap(state) < 2.0


def test_skeleton_shoots_the_player(server_url, page):
    page.goto(server_url + BOOT)
    _wait_ready(page)
    page.evaluate("() => window.__ARCHER.spawnEnemy('skeleton', 0, 12)")
    # Peek/shoot cycle is ~3.4 s; several volleys land within 25 s.
    page.wait_for_function("() => window.__ARCHER.state.hp < 100", timeout=25000)
```

Note: both tests are deterministic (seed 42) — if the chosen seed leaves no cover
near (0, 12) or every shot misses, adjust the spawn point or seed once and the
test stays green forever.

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd archer && uv run --group dev pytest tests/test_e2e.py -x -q`
Expected: FAIL — `BUILDERS['skeleton']` is undefined (spawn throws).

- [ ] **Step 3: Implement the skeleton**

In `archer/web/enemies.js`:

Add after `buildOgre`:

```js
function buildSkeleton(c) {
  const g = new THREE.Group();
  const body = new THREE.Mesh(
    new THREE.CylinderGeometry(0.22, 0.3, c.height * 0.8, 6), lambert(c.color),
  );
  body.position.y = c.height * 0.4;
  const head = new THREE.Mesh(new THREE.SphereGeometry(c.headRadius, 8, 6), lambert(0xe8e4d8));
  head.position.y = c.height;
  const bow = new THREE.Mesh(
    new THREE.TorusGeometry(0.3, 0.03, 5, 16, Math.PI), lambert(0x6b4a2f),
  );
  bow.position.set(0.35, c.height * 0.65, 0.1);
  bow.rotation.y = Math.PI / 2;
  g.add(body, head, bow);
  return g;
}
```

Change the `BUILDERS` line to:

```js
const BUILDERS = { goblin: buildGoblin, ogre: buildOgre, skeleton: buildSkeleton };
```

Replace the two empty methods (`updateArcher(e, dt, playerPos) {} // Task 6` and `updateProjectiles(dt, playerPos) {} // Task 6`) with:

```js
  updateArcher(e, dt, playerPos) {
    const pos = e.mesh.position;
    const dist = Math.hypot(playerPos.x - pos.x, playerPos.z - pos.z);
    if (e.state === 'advance') {
      if (dist > e.c.range) { this.moveToward(e, playerPos, dt, this.speedOf(e)); return; }
      e.cover = this.pickCover(e, playerPos);
      e.state = 'cover';
      e.coverTimer = e.c.hideTime * 0.5; // first hide is short: pressure early
    }
    const speed = this.speedOf(e);
    if (e.state === 'cover') {
      if (e.cover) this.moveToward(e, this.coverPoint(e.cover, playerPos), dt, speed);
      e.coverTimer -= dt;
      if (e.coverTimer <= 0) { e.state = 'peek'; e.coverTimer = e.c.peekTime; e.hasShot = false; }
    } else if (e.state === 'peek') {
      if (e.cover) this.moveToward(e, this.peekPoint(e, playerPos), dt, speed);
      if (!e.hasShot && e.coverTimer <= e.c.peekTime * 0.5) {
        this.shoot(e, playerPos);
        e.hasShot = true;
      }
      e.coverTimer -= dt;
      if (e.coverTimer <= 0) { e.state = 'cover'; e.coverTimer = e.c.hideTime; }
    }
  }

  // Nearest obstacle roughly on the line between this archer and the player.
  pickCover(e, playerPos) {
    const pos = e.mesh.position;
    let best = null;
    let bestD = 18;
    for (const o of this.game.obstacles) {
      const d = Math.hypot(o.x - pos.x, o.z - pos.z);
      if (d >= bestD) continue;
      const toObstacle = new THREE.Vector2(o.x - playerPos.x, o.z - playerPos.z);
      const toArcher = new THREE.Vector2(pos.x - playerPos.x, pos.z - playerPos.z);
      if (toObstacle.length() >= toArcher.length()) continue; // must shield the archer
      if (toObstacle.normalize().dot(toArcher.normalize()) < 0.7) continue;
      best = o;
      bestD = d;
    }
    return best;
  }

  coverPoint(cover, playerPos) {
    const away = new THREE.Vector3(cover.x - playerPos.x, 0, cover.z - playerPos.z).normalize();
    return new THREE.Vector3(cover.x, 0, cover.z).addScaledVector(away, cover.radius + 0.7);
  }

  peekPoint(e, playerPos) {
    const c = this.coverPoint(e.cover, playerPos);
    const away = new THREE.Vector3(e.cover.x - playerPos.x, 0, e.cover.z - playerPos.z).normalize();
    const perp = new THREE.Vector3(-away.z, 0, away.x);
    return c.addScaledVector(perp, e.peekSide * (e.cover.radius + 0.5));
  }

  shoot(e, playerPos) {
    const from = this.headCenter(e);
    const dir = new THREE.Vector3().subVectors(playerPos, from).normalize();
    const rng = this.game.rng;
    dir.x += rng.range(-e.c.spread, e.c.spread);
    dir.y += rng.range(-e.c.spread, e.c.spread);
    dir.z += rng.range(-e.c.spread, e.c.spread);
    dir.normalize();
    const mesh = new THREE.Mesh(
      new THREE.SphereGeometry(0.09, 6, 5),
      new THREE.MeshBasicMaterial({ color: 0x332222 }),
    );
    mesh.position.copy(from);
    this.game.scene.add(mesh);
    this.projectiles.push({ mesh, vel: dir.multiplyScalar(e.c.projectileSpeed), age: 0 });
  }

  updateProjectiles(dt, playerPos) {
    for (const p of [...this.projectiles]) {
      p.vel.y -= 4 * dt; // gentle drop so long shots arc
      p.mesh.position.addScaledVector(p.vel, dt);
      p.age += dt;
      let dead = false;
      if (p.mesh.position.distanceTo(playerPos) < 0.9) {
        this.game.player.takeDamage(CONFIG.enemies.skeleton.damage);
        this.game.onPlayerHit();
        dead = true;
      } else if (p.mesh.position.y < 0 || p.age > 5) {
        dead = true;
      }
      if (dead) {
        this.game.scene.remove(p.mesh);
        this.projectiles.splice(this.projectiles.indexOf(p), 1);
      }
    }
  }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd archer && uv run --group dev pytest tests/test_e2e.py -x -q`
Expected: 12 passed. If `test_skeleton_shoots_the_player` times out, the seed's spread rolls all miss — verify a hit lands by rerunning with `?seed=43` in the test URL, then keep whichever seed passes (deterministic thereafter).

- [ ] **Step 5: Commit**

```bash
git add archer/web/enemies.js archer/tests/test_e2e.py
git commit -m "feat(archer): skeleton archer with cover-seeking AI and enemy projectiles"
```

---

### Task 7: Special arrows — explosion, freeze, burn, particles, type selection

**Files:**
- Create: `archer/web/effects.js`
- Modify: `archer/web/arrows.js`, `archer/web/main.js`
- Test: `archer/tests/test_e2e.py`

**Interfaces:**
- Consumes: `EnemySystem.damage/freeze/ignite/bodyCenter` (Task 5 — status logic already implemented there); `CONFIG.arrow.types`.
- Produces:
  - `effects.js`: `export class Effects` — `constructor(scene)`, `burst(pos, color, count=20, speed=8)`, `setSnow(on)`, `update(dt)`. Particles are visual-only, so plain `Math.random()` is allowed here (the seeded-RNG rule covers gameplay state only — document this in the file header comment).
  - `arrows.js`: `hit()` gains type effects; new `explode(pos)` (AoE with linear falloff; no line-of-sight check, so splash reaches enemies behind cover by design); ground impact triggers `explode` for exploding arrows.
  - `main.js`: `game.effects`; snow enabled on the iceberg stage; keys 1–4 + mouse wheel select `game.stats.selected`; `__ARCHER.giveAmmo(type, n)`.

- [ ] **Step 1: Write the failing tests**

Append to `archer/tests/test_e2e.py`:

```python
def test_exploding_arrow_splashes_the_group(server_url, page):
    page.goto(server_url + BOOT)
    _wait_ready(page)
    page.evaluate("() => window.__ARCHER.setPlayerHp(10000)")
    for x in (-1.2, 0, 1.2):
        page.evaluate(f"() => window.__ARCHER.spawnEnemy('goblin', {x}, 32)")
    page.evaluate("() => window.__ARCHER.fireAt(0, 0.65, 32, 'exploding')")
    page.wait_for_function("() => window.__ARCHER.state.arrowCount === 0", timeout=5000)
    # Direct target dies; every survivor was splashed (hp below the 40 max).
    state = page.evaluate("() => window.__ARCHER.state")
    assert state["enemyCount"] < 3
    assert all(e["hp"] < 40 for e in state["enemies"])


def test_freezing_arrow_stops_attacks_then_thaws(server_url, page):
    page.goto(server_url + BOOT)
    _wait_ready(page)
    page.evaluate("() => window.__ARCHER.setPlayerHp(10000)")
    page.evaluate("() => window.__ARCHER.spawnEnemy('ogre', 0, 32)")
    page.evaluate("() => window.__ARCHER.fireAt(0, 1.25, 32, 'freezing')")
    page.wait_for_function(
        "() => window.__ARCHER.state.enemies[0] && window.__ARCHER.state.enemies[0].frozen",
        timeout=5000,
    )
    hp0 = page.evaluate("() => window.__ARCHER.state.hp")
    page.wait_for_timeout(1500)  # frozen: no attacks land
    assert page.evaluate("() => window.__ARCHER.state.hp") == hp0
    page.wait_for_function(
        "() => !window.__ARCHER.state.enemies[0].frozen", timeout=5000
    )  # thaws after freezeTime


def test_burning_arrow_ticks_and_spreads(server_url, page):
    page.goto(server_url + BOOT)
    _wait_ready(page)
    page.evaluate("() => window.__ARCHER.setPlayerHp(10000)")
    page.evaluate("() => window.__ARCHER.spawnEnemy('ogre', 0, 32)")
    page.evaluate("() => window.__ARCHER.spawnEnemy('ogre', 1.8, 32)")
    page.evaluate("() => window.__ARCHER.fireAt(0, 1.25, 32, 'burning')")
    page.wait_for_function(
        "() => window.__ARCHER.state.enemies[0] && window.__ARCHER.state.enemies[0].burning",
        timeout=5000,
    )
    hp0 = page.evaluate("() => window.__ARCHER.state.enemies[0].hp")
    page.wait_for_timeout(2000)
    hp1 = page.evaluate("() => window.__ARCHER.state.enemies[0].hp")
    assert hp1 < hp0  # damage over time with no further arrows
    # Fire spreads to the adjacent ogre (within spreadRadius).
    page.wait_for_function(
        "() => window.__ARCHER.state.enemies.length > 1 && window.__ARCHER.state.enemies[1].burning",
        timeout=5000,
    )


def test_special_ammo_is_consumed_and_gated(server_url, page):
    page.goto(server_url + BOOT)
    _wait_ready(page)
    page.keyboard.press("Digit3")
    assert page.evaluate("() => window.__ARCHER.state.selected") == "freezing"
    # No ammo: a full-draw release fizzles.
    page.mouse.move(640, 360)
    page.mouse.down()
    page.wait_for_function("() => window.__ARCHER.state.drawPower >= 1", timeout=5000)
    page.mouse.up()
    assert page.evaluate("() => window.__ARCHER.state.arrowCount") == 0
    # With ammo: fires and decrements.
    page.evaluate("() => window.__ARCHER.giveAmmo('freezing', 2)")
    page.mouse.down()
    page.wait_for_function("() => window.__ARCHER.state.drawPower >= 1", timeout=5000)
    page.mouse.up()
    page.wait_for_function("() => window.__ARCHER.state.arrowCount === 1", timeout=2000)
    assert page.evaluate("() => window.__ARCHER.state.ammo.freezing") == 1
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd archer && uv run --group dev pytest tests/test_e2e.py -x -q`
Expected: FAIL — exploding arrow does plain damage (no splash), `giveAmmo` undefined.

- [ ] **Step 3: Write effects.js**

Create `archer/web/effects.js`:

```js
import * as THREE from 'three';

// Visual-only particles. This module is exempt from the seeded-RNG rule:
// nothing here feeds back into gameplay state, so Math.random() is fine.
export class Effects {
  constructor(scene) {
    this.scene = scene;
    this.bursts = [];
    this.snow = null;
  }

  burst(pos, color, count = 20, speed = 8) {
    const positions = new Float32Array(count * 3);
    const vels = [];
    for (let i = 0; i < count; i++) {
      positions.set([pos.x, pos.y, pos.z], i * 3);
      vels.push(new THREE.Vector3(
        Math.random() - 0.5, Math.random() * 0.8, Math.random() - 0.5,
      ).normalize().multiplyScalar(speed * (0.4 + Math.random() * 0.6)));
    }
    const geo = new THREE.BufferGeometry();
    geo.setAttribute('position', new THREE.BufferAttribute(positions, 3));
    const points = new THREE.Points(
      geo, new THREE.PointsMaterial({ color, size: 0.22, transparent: true }),
    );
    this.scene.add(points);
    this.bursts.push({ points, vels, age: 0, life: 0.8 });
  }

  setSnow(on) {
    if (on === !!this.snow) return;
    if (!on) { this.scene.remove(this.snow); this.snow = null; return; }
    const count = 500;
    const positions = new Float32Array(count * 3);
    for (let i = 0; i < count; i++) {
      positions.set(
        [(Math.random() - 0.5) * 90, Math.random() * 30, (Math.random() - 0.5) * 90], i * 3,
      );
    }
    const geo = new THREE.BufferGeometry();
    geo.setAttribute('position', new THREE.BufferAttribute(positions, 3));
    this.snow = new THREE.Points(geo, new THREE.PointsMaterial({
      color: 0xffffff, size: 0.15, transparent: true, opacity: 0.8,
    }));
    this.scene.add(this.snow);
  }

  update(dt) {
    for (const b of [...this.bursts]) {
      b.age += dt;
      const attr = b.points.geometry.attributes.position;
      for (let i = 0; i < b.vels.length; i++) {
        b.vels[i].y -= 9.8 * dt;
        attr.setXYZ(
          i,
          attr.getX(i) + b.vels[i].x * dt,
          attr.getY(i) + b.vels[i].y * dt,
          attr.getZ(i) + b.vels[i].z * dt,
        );
      }
      attr.needsUpdate = true;
      b.points.material.opacity = 1 - b.age / b.life;
      if (b.age >= b.life) {
        this.scene.remove(b.points);
        b.points.geometry.dispose();
        b.points.material.dispose();
        this.bursts.splice(this.bursts.indexOf(b), 1);
      }
    }
    if (this.snow) {
      const attr = this.snow.geometry.attributes.position;
      for (let i = 0; i < attr.count; i++) {
        let y = attr.getY(i) - 2.5 * dt;
        if (y < 0) y = 30;
        attr.setY(i, y);
      }
      attr.needsUpdate = true;
    }
  }
}
```

- [ ] **Step 4: Extend arrows.js with type effects**

In `archer/web/arrows.js`:

Replace the whole `hit` method (including its `// [task-7]` comment) with:

```js
  hit(e, isHead, arrow) {
    const t = CONFIG.arrow.types[arrow.type];
    this.game.enemies.damage(e, t.damage * (isHead ? CONFIG.arrow.headshotMult : 1), isHead);
    const alive = e.hp > 0;
    if (arrow.type === 'freezing' && alive) this.game.enemies.freeze(e);
    if (arrow.type === 'burning' && alive) this.game.enemies.ignite(e);
    if (arrow.type === 'exploding') this.explode(arrow.mesh.position);
    else this.game.effects?.burst(arrow.mesh.position, 0xaa3333, 10, 4);
  }

  // AoE with linear falloff. Deliberately no line-of-sight check: splash
  // reaches enemies hiding behind cover (the counter to skeleton archers).
  explode(pos) {
    this.game.effects?.burst(pos, 0xffaa33, 40, 12);
    const t = CONFIG.arrow.types.exploding;
    for (const e of [...this.game.enemies.list]) {
      const d = this.game.enemies.bodyCenter(e).distanceTo(pos);
      if (d < t.radius) this.game.enemies.damage(e, t.aoeDamage * (1 - d / t.radius));
    }
  }
```

In the `update` method, replace the ground-impact block:

```js
      if (consumed) { this.remove(a); continue; }
      if (pos.y <= 0.05 || a.age > CONFIG.arrow.lifetime) {
        // [task-7] explode on ground impact for exploding arrows
        this.remove(a);
      }
```

with:

```js
      if (consumed) { this.remove(a); continue; }
      if (pos.y <= 0.05 || a.age > CONFIG.arrow.lifetime) {
        if (a.type === 'exploding') this.explode(pos);
        this.remove(a);
      }
```

- [ ] **Step 5: Wire effects and type selection into main.js**

In `archer/web/main.js`:

Replace `// [task-7] effects import` with:

```js
import { Effects } from './effects.js';
```

Replace `// [task-7] effects setup` with:

```js
game.effects = new Effects(scene);
game.effects.setSnow(game.stage === 'iceberg');
const ARROW_ORDER = ['normal', 'exploding', 'freezing', 'burning'];
const TYPE_KEYS = { Digit1: 'normal', Digit2: 'exploding', Digit3: 'freezing', Digit4: 'burning' };
document.addEventListener('keydown', (e) => {
  if (game.screen !== 'playing') return;
  const type = TYPE_KEYS[e.code];
  if (type) { game.stats.selected = type; game.syncUI(); }
});
document.addEventListener('wheel', (e) => {
  if (game.screen !== 'playing') return;
  const i = ARROW_ORDER.indexOf(game.stats.selected);
  game.stats.selected = ARROW_ORDER[(i + (e.deltaY > 0 ? 1 : -1) + 4) % 4];
  game.syncUI();
});
```

In `loadStage`, add as the last line of the function body:

```js
  game.effects?.setSnow(name === 'iceberg');
```

(Optional-chained because the boot-time `loadStage` call runs before `game.effects` exists; the explicit `setSnow` in the setup block covers that first load.)

Replace `// [task-7] effects.update(dt)` with:

```js
  game.effects.update(dt);
```

Replace `// [task-8] setDropChance, killAll, skipToWave` with (giveAmmo lands now; the anchor keeps its Task-8 items):

```js
  giveAmmo: (type, n) => { game.stats.ammo[type] += n; game.syncUI(); },
  // [task-8] setDropChance, killAll, skipToWave
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `cd archer && uv run --group dev pytest tests/test_e2e.py -x -q`
Expected: 16 passed.

- [ ] **Step 7: Commit**

```bash
git add archer/web/effects.js archer/web/arrows.js archer/web/main.js archer/tests/test_e2e.py
git commit -m "feat(archer): exploding/freezing/burning arrows with particles and type selection"
```

---

### Task 8: Waves and pickups — spawn sequencing, drops, shoot-to-collect

**Files:**
- Create: `archer/web/waves.js`
- Modify: `archer/web/main.js`
- Test: `archer/tests/test_e2e.py` (also updates the `BOOT` constant)

**Interfaces:**
- Consumes: `CONFIG.waves`, `CONFIG.stages[...].waves`, `CONFIG.drops`, `CONFIG.arena`; `game.enemies.spawn/list/clear`, `game.rng`, `game.stats.ammo`, `game.effects`, `game.onStageCleared` (stub this task; real progression in Task 9). `ArrowSystem.update` already collects pickups via the `game.waves?.pickups`/`collect(p)` guard written in Task 4.
- Produces:
  - `waves.js`: `export class WaveManager` — `constructor(game)`, fields `waveIndex` (1-based once started), `state` (`'idle' | 'spawning' | 'fighting' | 'cleared'`), `pickups` (array of `{mesh, type, age}`); methods `startWave(n)`, `skipToWave(n)` (clears the battlefield first), `update(dt)`, `onEnemyKilled(e)` (rolls drops), `collect(p)` (grants `rng.int(drops.min, drops.max)` ammo), `clearPickups()`.
  - `main.js`: `game.waves`; autostart starts wave 1 unless `?waves=0`; `__ARCHER.setDropChance(c)`, `__ARCHER.killAll()`, `__ARCHER.skipToWave(n)`; state gains `wave`, `waveState`, `pickupCount`, `pickups` (`[{type, x, y, z}]`).
  - Wave-clear detection is computed, not counted: a wave is done when `pending` is empty AND `game.enemies.list` is empty — so test-spawned or test-killed enemies can't corrupt the accounting.
- Test note: `BOOT` gains `&waves=0` so all earlier single-enemy combat tests keep an empty battlefield; wave tests use their own URL without it.

- [ ] **Step 1: Update BOOT and write the failing tests**

In `archer/tests/test_e2e.py`, change the `BOOT` constant to:

```python
# Deterministic, menu-skipping boot with wave spawning disabled — combat
# tests spawn their own enemies on an empty battlefield.
BOOT = "/?autostart=1&seed=42&waves=0"
```

Append:

```python
def test_wave_one_spawns_forest_mix(server_url, page):
    page.goto(server_url + "/?autostart=1&seed=42")  # waves ON
    _wait_ready(page)
    # Forest wave 1 = 4 goblins, staggered by spawnInterval.
    page.wait_for_function("() => window.__ARCHER.state.enemyCount === 4", timeout=15000)
    assert page.evaluate("() => window.__ARCHER.state.wave") == 1
    types = page.evaluate("() => window.__ARCHER.state.enemies.map(e => e.type)")
    assert types == ["goblin"] * 4


def test_skip_to_wave(server_url, page):
    page.goto(server_url + "/?autostart=1&seed=42")
    _wait_ready(page)
    page.evaluate("() => window.__ARCHER.skipToWave(3)")
    # Forest wave 3 = 5 goblins + 2 skeletons.
    page.wait_for_function("() => window.__ARCHER.state.enemyCount === 7", timeout=15000)
    assert page.evaluate("() => window.__ARCHER.state.wave") == 3


def test_drops_spawn_and_are_shot_to_collect(server_url, page):
    page.goto(server_url + BOOT)
    _wait_ready(page)
    page.evaluate("() => window.__ARCHER.setPlayerHp(10000)")
    page.evaluate("() => window.__ARCHER.setDropChance(1)")
    page.evaluate("() => window.__ARCHER.spawnEnemy('goblin', 0, 32)")
    page.evaluate("() => window.__ARCHER.killAll()")
    page.wait_for_function("() => window.__ARCHER.state.pickupCount === 1", timeout=2000)
    pickup = page.evaluate("() => window.__ARCHER.state.pickups[0]")
    ammo0 = page.evaluate("(t) => window.__ARCHER.state.ammo[t]", pickup["type"])
    page.evaluate(
        "(p) => window.__ARCHER.fireAt(p.x, p.y, p.z)",
        {"x": pickup["x"], "y": pickup["y"], "z": pickup["z"]},
    )
    page.wait_for_function("() => window.__ARCHER.state.pickupCount === 0", timeout=5000)
    ammo1 = page.evaluate("(t) => window.__ARCHER.state.ammo[t]", pickup["type"])
    assert 3 <= ammo1 - ammo0 <= 5
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd archer && uv run --group dev pytest tests/test_e2e.py -x -q`
Expected: FAIL — no wave spawning, `skipToWave`/`setDropChance`/`killAll` undefined.

- [ ] **Step 3: Write waves.js**

Create `archer/web/waves.js`:

```js
import * as THREE from 'three';
import { CONFIG } from './config.js';

const DROP_TYPES = ['exploding', 'freezing', 'burning'];
const DROP_COLORS = { exploding: 0xff7733, freezing: 0x66ddff, burning: 0xff4422 };

export class WaveManager {
  constructor(game) {
    this.game = game;
    this.pickups = [];
    this.waveIndex = 0; // 1-based once a wave starts
    this.pending = [];  // enemy types queued to spawn this wave
    this.spawnTimer = 0;
    this.betweenTimer = 0;
    this.state = 'idle';
  }

  startWave(n) {
    this.waveIndex = n;
    const mix = CONFIG.stages[this.game.stage].waves[n - 1];
    this.pending = [];
    for (const [type, count] of Object.entries(mix)) {
      for (let i = 0; i < count; i++) this.pending.push(type);
    }
    // Fisher-Yates on the seeded rng so spawn order interleaves types.
    for (let i = this.pending.length - 1; i > 0; i--) {
      const j = Math.floor(this.game.rng.random() * (i + 1));
      [this.pending[i], this.pending[j]] = [this.pending[j], this.pending[i]];
    }
    this.spawnTimer = 0;
    this.state = 'spawning';
    this.game.syncUI();
  }

  skipToWave(n) {
    this.game.enemies.clear();
    this.pending = [];
    this.startWave(n);
  }

  update(dt) {
    if (this.state === 'spawning') {
      this.spawnTimer -= dt;
      if (this.spawnTimer <= 0 && this.pending.length) {
        const type = this.pending.pop();
        const x = this.game.rng.range(-CONFIG.arena.spawnXSpread, CONFIG.arena.spawnXSpread);
        const z = CONFIG.arena.spawnZ + this.game.rng.range(-2, 2);
        this.game.enemies.spawn(type, x, z);
        this.spawnTimer = CONFIG.waves.spawnInterval;
      }
      if (!this.pending.length) this.state = 'fighting';
    } else if (this.state === 'fighting') {
      if (this.game.enemies.list.length === 0) {
        this.state = 'cleared';
        this.betweenTimer = CONFIG.waves.clearDelay;
      }
    } else if (this.state === 'cleared') {
      this.betweenTimer -= dt;
      if (this.betweenTimer <= 0) {
        if (this.waveIndex >= CONFIG.waves.perStage) this.game.onStageCleared();
        else this.startWave(this.waveIndex + 1);
      }
    }
    this.updatePickups(dt);
  }

  onEnemyKilled(e) {
    if (this.game.rng.random() < CONFIG.drops.chance) this.dropPickup(e);
  }

  dropPickup(e) {
    const type = this.game.rng.pick(DROP_TYPES);
    const mesh = new THREE.Mesh(
      new THREE.OctahedronGeometry(CONFIG.drops.radius * 0.7, 0),
      new THREE.MeshLambertMaterial({
        color: DROP_COLORS[type], emissive: DROP_COLORS[type], emissiveIntensity: 0.5,
      }),
    );
    mesh.position.set(e.mesh.position.x, 1.1, e.mesh.position.z);
    this.game.scene.add(mesh);
    this.pickups.push({ mesh, type, age: 0 });
  }

  collect(p) {
    const n = this.game.rng.int(CONFIG.drops.min, CONFIG.drops.max);
    this.game.stats.ammo[p.type] += n;
    this.game.effects?.burst(p.mesh.position, DROP_COLORS[p.type], 16, 5);
    this.removePickup(p);
    this.game.syncUI();
  }

  removePickup(p) {
    this.game.scene.remove(p.mesh);
    this.pickups.splice(this.pickups.indexOf(p), 1);
  }

  clearPickups() {
    for (const p of [...this.pickups]) this.removePickup(p);
  }

  updatePickups(dt) {
    for (const p of [...this.pickups]) {
      p.age += dt;
      p.mesh.rotation.y += dt * 2;
      p.mesh.position.y = 1.1 + Math.sin(p.age * 3) * 0.15;
      if (p.age > CONFIG.drops.lifetime) this.removePickup(p);
    }
  }
}
```

- [ ] **Step 4: Wire waves into main.js**

In `archer/web/main.js`:

Replace `// [task-8] waves import` with:

```js
import { WaveManager } from './waves.js';
```

Replace `// [task-8] wave manager setup` with:

```js
game.waves = new WaveManager(game);
game.onStageCleared = () => { // stub; Task 9 adds real progression
  game.screen = 'stageClear';
  game.syncUI();
};
```

Replace `// [task-8] waves.update(dt)` with:

```js
    game.waves.update(dt);
```

Replace `// [task-8] wave/pickup state` with:

```js
      wave: game.waves.waveIndex,
      waveState: game.waves.state,
      pickupCount: game.waves.pickups.length,
      pickups: game.waves.pickups.map((p) => ({
        type: p.type, x: p.mesh.position.x, y: p.mesh.position.y, z: p.mesh.position.z,
      })),
```

Replace `// [task-8] setDropChance, killAll, skipToWave` with:

```js
  setDropChance: (c) => { CONFIG.drops.chance = c; },
  killAll: () => {
    for (const e of [...game.enemies.list]) game.enemies.damage(e, 1e9);
  },
  skipToWave: (n) => { game.waves.skipToWave(n); },
```

In the autostart block at the bottom, replace:

```js
if (params.get('autostart') === '1') {
  game.screen = 'playing'; // [task-9] replaced by startGame()
  game.syncUI();
}
```

with:

```js
if (params.get('autostart') === '1') {
  game.screen = 'playing'; // [task-9] replaced by startGame()
  if (params.get('waves') !== '0') game.waves.startWave(1);
  game.syncUI();
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd archer && uv run --group dev pytest tests/test_e2e.py -x -q`
Expected: 19 passed.

- [ ] **Step 6: Commit**

```bash
git add archer/web/waves.js archer/web/main.js archer/tests/test_e2e.py
git commit -m "feat(archer): wave spawning and shoot-to-collect ammo drops"
```

---

### Task 9: Progression — stage advance, retry, victory, localStorage best

**Files:**
- Modify: `archer/web/main.js`
- Test: `archer/tests/test_e2e.py`

**Interfaces:**
- Consumes: `loadStage(index)`, `game.waves.startWave/clearPickups`, `game.enemies.clear()`, `game.arrows.clear()`, `game.player.resetHp()`, `STAGE_ORDER`.
- Produces (all in `main.js`):
  - `startGame(stageIndex)` — clears battlefield (enemies, arrows, pickups, `waves.state = 'idle'`), reloads the stage, refills HP, snapshots `game.stageInventory = {...game.stats.ammo}`, sets `screen='playing'`, starts wave 1 unless `?waves=0`.
  - `game.onStageCleared` (real): saves best, `screen = 'stageClear'`, or `'victory'` on the last stage.
  - `nextStage()`, `retryStage()` (restores the ammo snapshot), `gameOver()` gains `saveBest()`.
  - `loadBest()/saveBest()` on localStorage key `'archer.best'` → `{score, stage}` (stage = highest 1-based stage reached).
  - `__ARCHER.start(i)`, `__ARCHER.nextStage()`, `__ARCHER.retryStage()`; state gains `best`.
  - Autostart now calls `startGame(initialStage)`.

- [ ] **Step 1: Write the failing tests**

Append to `archer/tests/test_e2e.py`:

```python
def test_stage_clear_advances_to_desert(server_url, page):
    page.goto(server_url + "/?autostart=1&seed=42")
    _wait_ready(page)
    page.evaluate("() => window.__ARCHER.setDropChance(0)")
    page.evaluate("() => window.__ARCHER.setPlayerHp(10000)")
    page.evaluate("() => window.__ARCHER.skipToWave(5)")
    # Kill every wave-5 enemy as it spawns until the wave is done.
    page.wait_for_function(
        """() => {
          window.__ARCHER.killAll();
          return window.__ARCHER.state.screen === 'stageClear';
        }""",
        timeout=30000,
    )
    page.evaluate("() => window.__ARCHER.nextStage()")
    state = page.evaluate("() => window.__ARCHER.state")
    assert state["stage"] == "desert"
    assert state["screen"] == "playing"
    assert state["hp"] == 100  # HP refills between stages


def test_retry_restores_stage_start_inventory(server_url, page):
    page.goto(server_url + BOOT)
    _wait_ready(page)
    # Ammo gained mid-stage is lost on retry (snapshot from stage start = 0).
    page.evaluate("() => window.__ARCHER.giveAmmo('burning', 7)")
    page.evaluate("() => window.__ARCHER.setPlayerHp(1)")
    page.evaluate("() => window.__ARCHER.spawnEnemy('goblin', 0, 32)")
    page.wait_for_function("() => window.__ARCHER.state.screen === 'gameOver'", timeout=10000)
    page.evaluate("() => window.__ARCHER.retryStage()")
    state = page.evaluate("() => window.__ARCHER.state")
    assert state["screen"] == "playing"
    assert state["hp"] == 100
    assert state["ammo"]["burning"] == 0
    assert state["enemyCount"] == 0  # battlefield cleared


def test_multikill_combo_bonus(server_url, page):
    page.goto(server_url + BOOT)
    _wait_ready(page)
    page.evaluate("() => window.__ARCHER.setPlayerHp(10000)")
    page.evaluate("() => window.__ARCHER.setDropChance(0)")
    page.evaluate("() => window.__ARCHER.spawnEnemy('goblin', -1, 32)")
    page.evaluate("() => window.__ARCHER.spawnEnemy('goblin', 1, 32)")
    page.evaluate("() => window.__ARCHER.killAll()")  # same-frame kills chain a combo
    # 100 + (100 + 25 combo bonus on the second kill)
    page.wait_for_function("() => window.__ARCHER.state.score === 225", timeout=5000)


def test_best_score_persists_across_reloads(server_url, page):
    page.goto(server_url + BOOT)
    _wait_ready(page)
    page.evaluate("() => window.__ARCHER.setPlayerHp(10000)")
    page.evaluate("() => window.__ARCHER.spawnEnemy('goblin', 0, 32)")
    page.evaluate("() => window.__ARCHER.fireAt(0, 1.3, 32)")  # headshot: 150 points
    page.wait_for_function("() => window.__ARCHER.state.score === 150", timeout=5000)
    page.evaluate("() => window.__ARCHER.setPlayerHp(1)")
    page.evaluate("() => window.__ARCHER.spawnEnemy('goblin', 0, 32)")
    page.wait_for_function("() => window.__ARCHER.state.screen === 'gameOver'", timeout=10000)
    page.goto(server_url + BOOT)  # fresh page, same origin → same localStorage
    _wait_ready(page)
    assert page.evaluate("() => window.__ARCHER.state.best.score") >= 150
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd archer && uv run --group dev pytest tests/test_e2e.py -x -q`
Expected: FAIL — `nextStage`/`retryStage` undefined, `state.best` missing.

- [ ] **Step 3: Implement progression in main.js**

Replace `// [task-9] progression (start/stage-clear/game-over/retry)` with:

```js
const BEST_KEY = 'archer.best';
function loadBest() {
  try { return JSON.parse(localStorage.getItem(BEST_KEY)) || { score: 0, stage: 0 }; }
  catch { return { score: 0, stage: 0 }; }
}
function saveBest() {
  const best = loadBest();
  best.score = Math.max(best.score, game.stats.score);
  best.stage = Math.max(best.stage, game.stageIndex + 1);
  localStorage.setItem(BEST_KEY, JSON.stringify(best));
}

function startGame(stageIndex) {
  game.enemies.clear();
  game.arrows.clear();
  game.waves.clearPickups();
  game.waves.state = 'idle';
  loadStage(stageIndex);
  game.player.resetHp();
  game.stageInventory = { ...game.stats.ammo }; // retry restores this snapshot
  game.screen = 'playing';
  if (params.get('waves') !== '0') game.waves.startWave(1);
  game.syncUI();
}
function nextStage() { startGame(game.stageIndex + 1); }
function retryStage() {
  game.stats.ammo = { ...game.stageInventory };
  startGame(game.stageIndex);
}
game.onStageCleared = () => {
  saveBest();
  game.screen = game.stageIndex >= STAGE_ORDER.length - 1 ? 'victory' : 'stageClear';
  document.exitPointerLock?.();
  game.syncUI();
};
```

Delete the Task-8 stub (`game.onStageCleared = () => { // stub; Task 9 adds real progression` … `};`) from the wave-manager setup block.

In `gameOver()`, replace the line `// [task-9] persist best on game over` with:

```js
  saveBest();
```

Replace the whole `game.onEnemyKilled = ...` assignment (from Task 5) with a
combo-aware version — kills within `CONFIG.multiKillWindow` seconds of each
other chain a multi-kill bonus:

```js
game.onEnemyKilled = (e, isHead) => {
  const now = performance.now();
  const chained = now - (game.lastKillAt ?? -Infinity) < CONFIG.multiKillWindow * 1000;
  game.combo = chained ? game.combo + 1 : 0;
  game.lastKillAt = now;
  game.stats.score += e.c.score
    + (isHead ? CONFIG.headshotBonus : 0)
    + game.combo * CONFIG.multiKillBonus;
  game.waves?.onEnemyKilled(e);
  game.syncUI();
};
```

Replace `// [task-9] start, nextStage, retryStage` with:

```js
  start: (i = 0) => startGame(i),
  nextStage: () => nextStage(),
  retryStage: () => retryStage(),
```

Add to the `state` getter, after the `score:` line:

```js
      best: loadBest(),
```

Replace the autostart block:

```js
if (params.get('autostart') === '1') {
  game.screen = 'playing'; // [task-9] replaced by startGame()
  if (params.get('waves') !== '0') game.waves.startWave(1);
  game.syncUI();
}
```

with:

```js
if (params.get('autostart') === '1') startGame(initialStage);
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd archer && uv run --group dev pytest tests/test_e2e.py -x -q`
Expected: 23 passed.

- [ ] **Step 5: Commit**

```bash
git add archer/web/main.js archer/tests/test_e2e.py
git commit -m "feat(archer): stage progression, retry with inventory snapshot, localStorage best"
```

---

### Task 10: UI — Preact screens and HUD

**Files:**
- Create: `archer/web/ui.js`
- Modify: `archer/web/main.js`, `archer/web/styles.css`
- Test: `archer/tests/test_e2e.py`

**Interfaces:**
- Consumes: vendored `preact`, `preact/hooks`, `htm`; every game-state field pushed by `syncUI` (see store shape below); progression functions from Task 9.
- Produces:
  - `ui.js`: `export function createStore(initial)` → `{ get(), set(patch), subscribe(fn) }`; `export function initUI(store, actions)` renders the Preact app into `#ui`. `actions = { start, resume, next, retry, restart }`. Store shape: `{ screen, hp, maxHp, score, ammo, selected, wave, totalWaves, stage, best }`.
  - `main.js`: real `game.syncUI` (pushes the store shape above); `initUI` wiring; pause on pointer-lock loss (`screen = 'paused'`).
  - Testids: `title-screen`, `start-btn`, `pause-screen`, `resume-btn`, `stageclear-screen`, `next-btn`, `victory-screen`, `restart-btn`, `gameover-screen`, `retry-btn`, `hud`, `hp-text`, `score`, `wave`, `slot-<type>`, `ammo-<type>`, `best`.
  - The draw-power ring stays OUTSIDE Preact: it reads the `--draw` CSS variable that `main.js` already sets each frame (Task 3).

- [ ] **Step 1: Write the failing tests**

Append to `archer/tests/test_e2e.py`:

```python
def test_title_screen_and_start_button(server_url, page):
    page.goto(server_url + "/?seed=1&waves=0")  # no autostart: land on the title
    _wait_ready(page)
    expect(page.get_by_test_id("title-screen")).to_be_visible()
    page.get_by_test_id("start-btn").click()
    page.wait_for_function("() => window.__ARCHER.state.screen === 'playing'", timeout=5000)
    expect(page.get_by_test_id("hud")).to_be_visible()


def test_hud_reflects_score_ammo_and_selection(server_url, page):
    page.goto(server_url + BOOT)
    _wait_ready(page)
    page.evaluate("() => window.__ARCHER.setPlayerHp(10000)")
    page.evaluate("() => window.__ARCHER.spawnEnemy('goblin', 0, 32)")
    page.evaluate("() => window.__ARCHER.fireAt(0, 1.3, 32)")  # headshot kill: 150
    page.wait_for_function("() => window.__ARCHER.state.score === 150", timeout=5000)
    expect(page.get_by_test_id("score")).to_have_text("150")
    page.evaluate("() => window.__ARCHER.giveAmmo('freezing', 4)")
    expect(page.get_by_test_id("ammo-freezing")).to_have_text("4")
    page.keyboard.press("Digit3")
    expect(page.get_by_test_id("slot-freezing")).to_have_class(re.compile(r"\bactive\b"))
    expect(page.get_by_test_id("wave")).to_contain_text("forest")


def test_game_over_screen_retry_button(server_url, page):
    page.goto(server_url + BOOT)
    _wait_ready(page)
    page.evaluate("() => window.__ARCHER.setPlayerHp(1)")
    page.evaluate("() => window.__ARCHER.spawnEnemy('goblin', 0, 32)")
    page.wait_for_function("() => window.__ARCHER.state.screen === 'gameOver'", timeout=10000)
    expect(page.get_by_test_id("gameover-screen")).to_be_visible()
    page.get_by_test_id("retry-btn").click()
    page.wait_for_function("() => window.__ARCHER.state.screen === 'playing'", timeout=5000)
    assert page.evaluate("() => window.__ARCHER.state.hp") == 100
```

Add `import re` at the top of the file (below the existing import).

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd archer && uv run --group dev pytest tests/test_e2e.py -x -q`
Expected: FAIL — `title-screen` testid never appears.

- [ ] **Step 3: Write ui.js**

Create `archer/web/ui.js`:

```js
import { h, render } from 'preact';
import { useEffect, useState } from 'preact/hooks';
import htm from 'htm';

const html = htm.bind(h);

export function createStore(initial) {
  let state = { ...initial };
  const subs = new Set();
  return {
    get: () => state,
    set(patch) {
      state = { ...state, ...patch };
      subs.forEach((fn) => fn(state));
    },
    subscribe(fn) {
      subs.add(fn);
      return () => subs.delete(fn);
    },
  };
}

function useStore(store) {
  const [s, setS] = useState(store.get());
  useEffect(() => store.subscribe(setS), [store]);
  return s;
}

const SLOTS = [
  ['normal', 'Normal', '1'],
  ['exploding', 'Explode', '2'],
  ['freezing', 'Freeze', '3'],
  ['burning', 'Burn', '4'],
];

function Hud({ s }) {
  return html`
    <div class="hud" data-testid="hud">
      <div class="top-left">
        <div class="hp-bar"><div class="hp-fill" style="width:${(100 * s.hp) / s.maxHp}%" /></div>
        <div class="hp-text" data-testid="hp-text">${s.hp} HP</div>
      </div>
      <div class="top-right">
        <div class="score" data-testid="score">${s.score}</div>
        <div class="wave" data-testid="wave">${s.stage} — wave ${s.wave}/${s.totalWaves}</div>
      </div>
      <div class="quiver">
        ${SLOTS.map(([type, label, key]) => html`
          <div
            class="slot ${s.selected === type ? 'active' : ''} ${type !== 'normal' && !s.ammo[type] ? 'empty' : ''}"
            data-testid="slot-${type}"
          >
            <span class="key">${key}</span>
            <span class="label">${label}</span>
            <span class="count" data-testid="ammo-${type}">${type === 'normal' ? '∞' : s.ammo[type]}</span>
          </div>
        `)}
      </div>
      <div class="crosshair"><div class="draw-ring" /></div>
    </div>`;
}

function Screen({ testid, title, children }) {
  return html`
    <div class="screen" data-testid=${testid}>
      <h1>${title}</h1>
      ${children}
    </div>`;
}

function Screens({ s, actions }) {
  if (s.screen === 'playing') return null;
  if (s.screen === 'title') {
    return html`
      <${Screen} testid="title-screen" title="ARCHER">
        <p>Hold to draw, release to loose. Keys 1–4 switch arrows.</p>
        <p data-testid="best">Best: ${s.best.score} pts, stage ${s.best.stage}/3</p>
        <button data-testid="start-btn" onClick=${actions.start}>Start</button>
      <//>`;
  }
  if (s.screen === 'paused') {
    return html`
      <${Screen} testid="pause-screen" title="Paused">
        <button data-testid="resume-btn" onClick=${actions.resume}>Resume</button>
      <//>`;
  }
  if (s.screen === 'stageClear') {
    return html`
      <${Screen} testid="stageclear-screen" title="Stage cleared!">
        <p>Score: ${s.score}</p>
        <button data-testid="next-btn" onClick=${actions.next}>Next stage</button>
      <//>`;
  }
  if (s.screen === 'victory') {
    return html`
      <${Screen} testid="victory-screen" title="All three lands defended!">
        <p>Final score: ${s.score}</p>
        <button data-testid="restart-btn" onClick=${actions.restart}>Play again</button>
      <//>`;
  }
  return html`
    <${Screen} testid="gameover-screen" title="You fell.">
      <p>Score: ${s.score}</p>
      <button data-testid="retry-btn" onClick=${actions.retry}>Retry stage</button>
    <//>`;
}

function App({ store, actions }) {
  const s = useStore(store);
  if (!s.screen) return null; // before the first syncUI
  return html`
    <div class="ui-root">
      ${s.screen !== 'title' && html`<${Hud} s=${s} />`}
      <${Screens} s=${s} actions=${actions} />
    </div>`;
}

export function initUI(store, actions) {
  render(html`<${App} store=${store} actions=${actions} />`, document.getElementById('ui'));
}
```

- [ ] **Step 4: Wire the UI into main.js**

In `archer/web/main.js`:

Replace `// [task-10] ui imports` with:

```js
import { createStore, initUI } from './ui.js';
```

Replace `// [task-10] ui wiring` with:

```js
const store = createStore({});
game.syncUI = () => store.set({
  screen: game.screen,
  hp: game.player.hp,
  maxHp: CONFIG.player.hp,
  score: game.stats.score,
  ammo: { ...game.stats.ammo },
  selected: game.stats.selected,
  wave: game.waves.waveIndex,
  totalWaves: CONFIG.waves.perStage,
  stage: game.stage,
  best: loadBest(),
});
initUI(store, {
  start: () => startGame(initialStage),
  resume: () => { game.screen = 'playing'; game.syncUI(); },
  next: () => nextStage(),
  retry: () => retryStage(),
  restart: () => { game.stats.score = 0; startGame(0); }, // fresh run after victory
});
game.syncUI();
```

Replace `// [task-10] pause on pointer-lock loss` (inside the `pointerlockchange` handler) with:

```js
    game.screen = 'paused';
    game.syncUI();
```

- [ ] **Step 5: Style the HUD and screens**

Append to `archer/web/styles.css`:

```css
.ui-root { width: 100%; height: 100%; }

.screen {
  position: absolute; inset: 0; display: grid; place-items: center;
  align-content: center; gap: 12px; text-align: center;
  background: rgba(10, 14, 20, 0.65); color: #f2ede2; pointer-events: auto;
}
.screen h1 { font-size: 56px; letter-spacing: 0.12em; margin: 0; }
.screen p { margin: 0; opacity: 0.85; }
.screen button {
  font-size: 20px; padding: 10px 34px; margin-top: 10px; cursor: pointer;
  background: #ffd257; border: none; border-radius: 6px; font-weight: 700;
}
.screen button:hover { background: #ffe28a; }

.hud { position: absolute; inset: 0; color: #f2ede2;
  text-shadow: 0 1px 2px rgba(0, 0, 0, 0.6); }
.top-left { position: absolute; top: 16px; left: 16px; }
.hp-bar { width: 220px; height: 14px; background: rgba(0, 0, 0, 0.45);
  border-radius: 7px; overflow: hidden; }
.hp-fill { height: 100%; background: linear-gradient(90deg, #d64545, #6fbf4a);
  transition: width 0.2s; }
.hp-text { margin-top: 4px; font-size: 13px; }
.top-right { position: absolute; top: 16px; right: 16px; text-align: right; }
.score { font-size: 30px; font-weight: 700; }
.wave { font-size: 14px; opacity: 0.9; }

.quiver { position: absolute; bottom: 16px; left: 50%; transform: translateX(-50%);
  display: flex; gap: 8px; }
.slot { display: flex; gap: 6px; align-items: baseline; padding: 6px 12px;
  background: rgba(0, 0, 0, 0.45); border-radius: 6px; font-size: 14px;
  border: 1px solid transparent; }
.slot.active { border-color: #ffd257; background: rgba(60, 45, 0, 0.6); }
.slot.empty { opacity: 0.45; }
.slot .key { font-weight: 700; color: #ffd257; }
.slot .count { font-variant-numeric: tabular-nums; }

.crosshair { position: absolute; left: 50%; top: 50%; width: 8px; height: 8px;
  transform: translate(-50%, -50%); }
.crosshair::before { content: ''; position: absolute; inset: 0;
  border-radius: 50%; background: rgba(255, 255, 255, 0.9); }
.draw-ring { position: absolute; inset: -14px; border-radius: 50%;
  background: conic-gradient(#ffd257 calc(var(--draw) * 360deg), rgba(255, 255, 255, 0.15) 0);
  -webkit-mask: radial-gradient(circle, transparent 58%, #000 60%);
  mask: radial-gradient(circle, transparent 58%, #000 60%); }
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `cd archer && uv run --group dev pytest tests/test_e2e.py -x -q`
Expected: 26 passed.

- [ ] **Step 7: Commit**

```bash
git add archer/web/ui.js archer/web/main.js archer/web/styles.css archer/tests/test_e2e.py
git commit -m "feat(archer): Preact HUD, title/pause/clear/victory/game-over screens"
```

---

### Task 11: README, SPEC, and final verification

**Files:**
- Create: `archer/README.md`, `archer/SPEC.md`

**Interfaces:**
- Consumes: everything — this task verifies the whole game.
- Produces: user-facing docs following the repo conventions (CLAUDE.md).

- [ ] **Step 1: Write README.md**

Create `archer/README.md` (repo convention: setext title, ONE paragraph, one bash block — no extra sections):

````markdown
Archer — three.js Wave Defense
---

A no-build, ES-module 3D archery game: you are a stationary first-person
archer on a raised outcrop, holding the line against five escalating waves
per stage across three low-poly arenas — forest, desert, and iceberg. Hold
the mouse to draw (a dotted arc previews partial-power shots), release to
loose a gravity-obeying arrow; headshots deal double damage. Goblins rush
you in weaving lines, ogres soak arrows, and skeleton archers duck behind
trees and pillars, peeking out to return fire. Kills drop exploding,
freezing, and burning arrow pickups that you collect by shooting them and
switch between with keys 1–4. Runs entirely from vendored ESM builds of
three.js and Preact served by a small Python `http.server`; every run is
reproducible via a seeded RNG (`?seed=N`) and the game is verified
end-to-end with Python Playwright.

```bash
./archer.sh
```
````

- [ ] **Step 2: Write SPEC.md**

Create `archer/SPEC.md`:

```markdown
Archer — technical spec
---

Design doc: `docs/superpowers/specs/2026-07-07-archer-game-design.md`.

## Controls

| Input | Action |
|-------|--------|
| Mouse move (pointer lock) | Aim |
| Hold / release left mouse | Draw (power charges ~1 s) / loose arrow |
| Keys 1–4, mouse wheel | Select arrow type |
| Esc (exits pointer lock) | Pause |

## Arrow types

| Type | Ammo | Damage | Effect |
|------|------|--------|--------|
| Normal | ∞ | 34 | — |
| Exploding | drops | 20 + AoE 55, r=5, linear falloff | Splash ignores cover (no LOS check) |
| Freezing | drops | 18 | Freeze 3 s; next hit ×2 (shatter) |
| Burning | drops | 15 | 9 dps for 4 s; spreads within 2.5 m |

Headshots ×2 damage, +50 score. Multi-kill combo: +25 score per extra kill
landed within 1.5 s of the previous one. Drops: 20% of kills, +3–5 arrows
of a random special type, collected by shooting the floating pickup.

## Enemies

| Enemy | HP | Speed | Behavior |
|-------|----|-------|----------|
| Goblin | 40 | 5.5 | Weaving melee rush, 10 dmg per 1.2 s |
| Skeleton archer | 60 | 4.0 | Advances to 26 m, hides behind the nearest obstacle on the player line, peeks 1.4 s to shoot (8 dmg), hides 2 s |
| Ogre | 220 | 1.9 | Slow tank, 25 dmg per 1.8 s |

Cover selection: nearest obstacle within 18 m of the archer whose direction
from the player is within ~45° of the archer's and closer to the player than
the archer is; cover point = obstacle edge + 0.7 m on the far side; peek
point offsets sideways by the obstacle radius + 0.5 m.

## Stages and waves

Five waves per stage; enemy mixes and per-stage speed multipliers live in
`web/config.js` (`CONFIG.stages`). Forest (26 trees, dense cover) →
desert (14 cacti/rocks, long sightlines, ×1.15 speed) → iceberg (18 ice
pillars, snow particles, ×1.25 speed). HP refills between stages; special
ammo carries over. Death → retry the same stage with the ammo held at its
start. Best score/stage persist in `localStorage['archer.best']`.

## Determinism and e2e

All gameplay randomness flows through one seeded mulberry32 stream
(`web/rng.js`, `?seed=N`); particles are visual-only and exempt.
`window.__ARCHER` exposes `ready`, a `state` snapshot (screen, hp, score,
wave, enemies, pickups, obstacles, best) and test hooks: `fireAt(x, y, z,
type?, power?)` (gravity-compensated), `spawnEnemy(type, x, z)`,
`skipToWave(n)`, `killAll()`, `giveAmmo(type, n)`, `setDropChance(c)`,
`setPlayerHp(n)`, `start(i)`, `nextStage()`, `retryStage()`,
`visiblePixelCount()`. Tests park enemies at melee reach (z=32) to avoid
leading moving targets, and boot with `?autostart=1&seed=42&waves=0` for a
clean battlefield. Arrow collision is segment-vs-sphere per frame (no
tunneling at 55 m/s). Enemy melee ignores the perch elevation (attacks
reach the player from the perch base by design).

## Known simplifications

- No player movement; no sound; no mobile controls (out of scope, v1).
- Stage geometry is regenerated (not disposed) on stage change — a small,
  bounded leak over a 3-stage run.
- Enemy projectiles use point (not segment) collision: at 20 m/s vs a
  0.9 m player radius they cannot tunnel.
```

- [ ] **Step 3: Run the FULL suite**

```bash
cd archer && uv run --group dev pytest tests/ -q
```

Expected: 26 passed, no skips.

- [ ] **Step 4: Manual smoke test**

```bash
cd archer && PORT=8123 ./archer.sh
```

Open http://127.0.0.1:8123/ — verify: title screen → Start → pointer lock →
draw/release fires an arrow with the dotted arc hint → goblins advance and
die → HUD updates → Esc pauses. Then Ctrl-C the server. If a browser isn't
available in this environment, verify via a longer Playwright run instead:

```bash
cd archer && uv run --group dev pytest tests/test_e2e.py::test_wave_one_spawns_forest_mix -q
```

- [ ] **Step 5: Commit**

```bash
git add archer/README.md archer/SPEC.md
git commit -m "docs(archer): README and technical SPEC"
```

---

## Execution Notes

- Tasks are strictly ordered; each leaves the full suite green.
- Playwright determinism: every gameplay assertion runs against `?seed=42`
  (or another pinned seed). If a physics-tuning assertion fails, the failure
  reproduces identically every run — fix the aim point or seed once, never
  chase flakes.
- The pcl-viewer project is the reference for any infra question not covered
  here (vendoring, MIME types, fixture patterns, CI-friendly Chromium
  fallback).
