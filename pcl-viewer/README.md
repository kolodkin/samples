PCL Viewer — three.js + Preact
---

A no-build, ES-module point cloud viewer: it loads a single 360° street-level
LiDAR scan from the KITTI dataset (`kitti-velodyne-000000.pcd`, 115k points) with
three.js's `PCDLoader`, reorients and normalizes it, renders each point as a
lit sphere impostor (a round, shaded "3D ball", with the older flat square
sprite still selectable) under `OrbitControls`, and wraps the scene in a Preact
UI (point-shape and point-size controls, flat/by-height/by-distance/by-intensity
color modes, a live stats overlay, and reset-camera).
Frontend dependencies are vendored ES modules loaded via an import map
— no bundler — served by a small Python `http.server` and verified end-to-end
with Python Playwright.

```bash
./pcl-viewer.sh
```
