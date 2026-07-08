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
switch between with keys 1–4 or a tap on the quiver. Plays on touch
devices too: drag to aim, hold the on-screen 🏹 button to draw, release to
fire. Runs entirely from vendored ESM builds of
three.js and Preact served by a small Python `http.server`; every run is
reproducible via a seeded RNG (`?seed=N`) and the game is verified
end-to-end with Python Playwright.

```bash
./archer.sh
```
