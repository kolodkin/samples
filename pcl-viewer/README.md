PCL Viewer — three.js + Preact
---

A no-build, ES-module point cloud viewer: it loads a single 360° street-level
LiDAR scan from the KITTI dataset (`kitti-velodyne-000000.pcd`, 115k points) with
three.js's `PCDLoader`, reorients and normalizes it, renders it with
`OrbitControls`, and wraps the scene in a Preact UI (point-size slider,
flat/by-height color modes, a live stats overlay, and reset-camera).
Frontend dependencies are vendored ES modules loaded via an import map
— no bundler — served by a small Python `http.server` and verified end-to-end
with Python Playwright.

```bash
./pcl-viewer.sh
```
