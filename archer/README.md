Archer — three.js Wave Defense
---

A no-build, ES-module 3D archery game: a stationary first-person archer
holds the line against five escalating waves per stage across three
low-poly arenas — forest, desert, and iceberg. Hold the mouse (or the
on-screen 🏹 button on touch) to draw, release to loose a gravity-obeying
arrow; goblins rush, ogres tank, skeleton archers shoot back from cover,
and kills drop exploding, freezing, and burning arrow pickups. Runs
entirely from vendored ESM builds of three.js and Preact served by a small
Python `http.server`; every run is reproducible via a seeded RNG
(`?seed=N`) and verified end-to-end with Python Playwright.

```bash
./archer.sh
```
