PCL Viewer — three.js + Preact
---

A no-build, ES-module point cloud viewer with a **Scene** selector: a 360°
street-level KITTI LiDAR scan (`kitti-velodyne-000000.pcd`, 115k points), a PCL
`table_scene_lms400` cloud hot-linked from the PointCloudLibrary data repo, and a
**KITTI movie** — a Draco-compressed multi-frame LiDAR sequence streamed at
runtime from a Hugging Face dataset and decoded in-browser. Each point renders as
a lit sphere impostor (a round, shaded "3D ball", with the older flat square
sprite still selectable) under `OrbitControls`, wrapped in a Preact UI (scene
selector, movie play/pause, point-shape and point-size controls,
flat/by-height/by-distance/by-intensity color modes, a live stats overlay, and
reset-camera). Frontend dependencies are vendored ES modules loaded via an import
map — no bundler — served by a small Python `http.server` and verified end-to-end
with Python Playwright.

```bash
./pcl-viewer.sh
```
