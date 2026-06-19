PCL Viewer — three.js + Preact
---

A no-build, ES-module point cloud viewer: it loads the MIT-licensed `Zaghetto.pcd`
sample with three.js's `PCDLoader`, renders it with `OrbitControls`, and wraps the
scene in a Preact UI (point-size slider, flat/by-height color modes, a live stats
overlay, reset-camera, and box/axes helpers). Frontend dependencies are vendored
ES modules loaded via an import map — no bundler — served by a small Python
`http.server` and verified end-to-end with Python Playwright.

```bash
./pcl-viewer.sh
```
