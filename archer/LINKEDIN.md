LinkedIn Post — Archer
---

Draft post announcing the archer game. Replace `[link]` with the live game URL before posting.

---

After playing with point clouds in three.js, I had to try the fun side of 3D — so I built a simple first-person archery game. 🏹

You hold the line against waves of goblins, ogres and skeleton archers across five low-poly arenas: meadow, forest, desert, iceberg and a volcano finale. Arrows obey gravity, and kills drop special pickups — exploding, lightning, freezing, and a burning arrow that splits mid-dive into a volley of fire arrows. 🔥

It's a 2-minute game. Give it a go and let me know what you think:
👉 [link]

The backbone is deliberately simple:
• one static page, no build step, no bundler
• three.js for the 3D, a small Preact SPA for the UI
• plain ES modules with vendored dependencies
• seeded RNG, so every run is reproducible — the e2e tests literally play the game with Playwright

It turned out way more fun than I expected. If you write software for a living, I strongly recommend building a tiny game at least once — it exercises muscles that regular app work never touches.

Want a write-up on how it's built? Comment "details" 👇

#threejs #gamedev #webdev #javascript #preact
