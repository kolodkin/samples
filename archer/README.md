Archer — three.js Wave Defense
---

A no-build, ES-module 3D archery game: a stationary first-person archer
holds the line against five escalating waves per stage across three
low-poly arenas — forest, desert, and iceberg. Click while aiming under
pointer lock (or tap the 🏹 button on touch) to loose a gravity-obeying
arrow, with shot power set by the +/− buttons on the left edge of the
screen; goblins rush, ogres tank, skeleton archers shoot back from cover,
and kills drop exploding, freezing, and burning arrow pickups that fire
automatically, strongest first. Runs
entirely from vendored ESM builds of three.js and Preact served by a small
Python `http.server`; every run is reproducible via a seeded RNG
(`?seed=N`) and verified end-to-end with Python Playwright.

```bash
./archer.sh
```
