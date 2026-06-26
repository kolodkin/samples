PCL Viewer — three.js + Preact
---

A no-build, ES-module point cloud viewer with a **Scene** selector: a
**KITTI movie** — a Draco-compressed multi-frame LiDAR sequence streamed at
runtime from a Hugging Face dataset and decoded in-browser through a
bounded-concurrency worker queue, so playback starts on the first frame and runs
at ~15 fps while the rest decode in the background — a **KITTI seg** scene — a
SemanticKITTI movie (from the same dataset's `seg/` folder) that colors every
point by its semantic class and draws a 3D bounding box around each object
instance — and **Stanford Lucy**, the famous winged-angel statue (50k-vertex
binary PLY, framed upright and front-on) hot-linked from the three.js model repo.
Each point renders as
a lit sphere impostor (a round, shaded "3D ball", with the older flat square
sprite still selectable) under `OrbitControls`, wrapped in a Preact UI (scene
selector, movie play/pause, point-shape and point-size controls, a
color-mode picker that lists only the modes the current scene can supply
(flat/by-height/by-distance always, plus by-intensity where the source carries it
and by-class on the seg scene), a class legend and
box toggle for the seg scene, a live stats overlay, and
reset-camera). Frontend dependencies are vendored ES modules loaded via an import
map — no bundler — served by a small Python `http.server` and verified end-to-end
with Python Playwright.

```bash
./pcl-viewer.sh
```
